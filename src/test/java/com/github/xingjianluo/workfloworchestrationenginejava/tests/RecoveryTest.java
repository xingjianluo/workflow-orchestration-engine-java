package com.github.xingjianluo.workfloworchestrationenginejava.tests;

import com.github.xingjianluo.workfloworchestrationenginejava.tests.support.MutableClock;
import com.github.xingjianluo.workfloworchestrationenginejava.tests.support.WorkflowHarness;
import com.github.xingjianluo.workfloworchestrationenginejava.workflow.config.WorkflowProperties;
import com.github.xingjianluo.workfloworchestrationenginejava.workflow.engine.DefinitionValidator;
import com.github.xingjianluo.workfloworchestrationenginejava.workflow.engine.WorkflowEngine;
import com.github.xingjianluo.workfloworchestrationenginejava.workflow.model.NodeLease;
import com.github.xingjianluo.workfloworchestrationenginejava.workflow.model.NodeStatus;
import com.github.xingjianluo.workfloworchestrationenginejava.workflow.model.WorkflowInstance;
import com.github.xingjianluo.workfloworchestrationenginejava.workflow.model.WorkflowStatus;
import com.github.xingjianluo.workfloworchestrationenginejava.workflow.store.FileWorkflowStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static com.github.xingjianluo.workfloworchestrationenginejava.tests.support.WorkflowHarness.definition;
import static com.github.xingjianluo.workfloworchestrationenginejava.tests.support.WorkflowHarness.node;
import static org.junit.jupiter.api.Assertions.*;

/**
 * 进程恢复：用同一数据目录创建"新进程"的引擎实例模拟重启。
 * 覆盖：过期租约回收、短暂重启（租约未过期）不双跑、全部状态从磁盘恢复。
 */
class RecoveryTest {

    @TempDir
    Path tempDir;

    private WorkflowEngine newEngine(MutableClock clock, WorkflowProperties props) {
        FileWorkflowStore store = new FileWorkflowStore(tempDir, clock);
        return new WorkflowEngine(store, new DefinitionValidator(), clock, props);
    }

    @Test
    void expiredLeaseIsReclaimedOnRestartAndWorkContinues() {
        MutableClock clock = new MutableClock();
        WorkflowProperties props = new WorkflowProperties();
        props.setBaseDir(tempDir);
        props.setLeaseTtlMillis(1_000);
        props.setDefaultMaxAttempts(3);

        // 旧进程：提交并领取 a 的租约，执行中"宕机"，没有任何回传
        WorkflowEngine oldEngine = newEngine(clock, props);
        String id = oldEngine.submit("rec-1", definition("wf",
                node("a", "noop"), node("b", "noop", "a"))).getInstanceId();
        NodeLease oldLease = oldEngine.tryAcquireLease().orElseThrow();
        assertEquals(NodeStatus.RUNNING, oldEngine.get(id).getStates().get("a").getStatus());
        String oldOwner = oldEngine.getOwnerId();

        // 时间推进超过 ttl 后"新进程"启动
        clock.advance(5_000);
        WorkflowEngine newEngine = newEngine(clock, props);
        assertNotEquals(oldOwner, newEngine.getOwnerId());

        List<String> recovered = newEngine.recoverExpiredLeases();
        assertTrue(recovered.contains(id + "/a"), "过期租约必须被回收");
        assertEquals(NodeStatus.PENDING, newEngine.get(id).getStates().get("a").getStatus(),
                "回收后节点回到 PENDING，租约归属清空");
        assertNull(newEngine.get(id).getStates().get("a").getLeaseToken());

        // 新进程可以正常继续执行，流程最终完成
        NodeLease newLease = newEngine.tryAcquireLease().orElseThrow();
        assertEquals("a", newLease.getNodeId());
        assertNotEquals(oldLease.getToken(), newLease.getToken());
        newEngine.completeSuccess(newLease);
        NodeLease b = newEngine.tryAcquireLease().orElseThrow();
        newEngine.completeSuccess(b);
        assertEquals(WorkflowStatus.SUCCEEDED, newEngine.get(id).getStatus());
        // 旧进程的回传在新进程必须作废（跨重启 token 不匹配）
        assertEquals(com.github.xingjianluo.workfloworchestrationenginejava.workflow.model.CompletionOutcome.LEASE_LOST,
                newEngine.completeSuccess(oldLease));
        WorkflowInstance wf = newEngine.get(id);
        assertEquals(WorkflowStatus.SUCCEEDED, wf.getStatus(), "过期 token 回传不得改变已完成状态");
    }

    @Test
    void quickRestartWithinLeaseTtlDoesNotDoubleRun() {
        MutableClock clock = new MutableClock();
        WorkflowProperties props = new WorkflowProperties();
        props.setBaseDir(tempDir);
        props.setLeaseTtlMillis(10_000);

        WorkflowEngine oldEngine = newEngine(clock, props);
        String id = oldEngine.submit("rec-2", definition("wf", node("a", "noop"))).getInstanceId();
        NodeLease lease = oldEngine.tryAcquireLease().orElseThrow();

        // 只过了 1s（远小于 ttl=10s）就重启
        clock.advance(1_000);
        WorkflowEngine newEngine = newEngine(clock, props);
        List<String> recovered = newEngine.recoverExpiredLeases();
        assertTrue(recovered.isEmpty(), "租约仍有效，重启不得接管，避免双跑");
        assertTrue(newEngine.tryAcquireLease().isEmpty(), "扫描也不能抢占仍有效的租约");
        assertEquals(NodeStatus.RUNNING, newEngine.get(id).getStates().get("a").getStatus());
        assertEquals(lease.getToken(), newEngine.get(id).getStates().get("a").getLeaseToken());

        // 租约自然到期后（原执行线程确未回来），下一轮扫描安全接管
        clock.advance(9_001);
        assertTrue(newEngine.tryAcquireLease().isPresent(), "租约到期后允许接管");
    }

    @Test
    void allStateSurvivesRestartIncludingBackoffAndFailure() {
        MutableClock clock = new MutableClock();
        WorkflowProperties props = new WorkflowProperties();
        props.setBaseDir(tempDir);
        props.setLeaseTtlMillis(1_000);
        props.setBaseBackoffMillis(100);

        WorkflowEngine e1 = newEngine(clock, props);
        String id = e1.submit("rec-3", definition("wf",
                node("a", "noop"), node("b", "noop", "a"))).getInstanceId();
        NodeLease l = e1.tryAcquireLease().orElseThrow();
        e1.completeFailure(l, "transient");
        assertEquals(NodeStatus.WAITING, e1.get(id).getStates().get("a").getStatus());
        long nextRetryAt = e1.get(id).getStates().get("a").getNextRetryAt();
        long version = e1.get(id).getVersion();

        // 重启：WAITING、退避时刻、attempts、version 全部从磁盘恢复
        WorkflowEngine e2 = newEngine(clock, props);
        e2.recoverExpiredLeases();
        WorkflowInstance restored = e2.get(id);
        assertEquals(NodeStatus.WAITING, restored.getStates().get("a").getStatus());
        assertEquals(1, restored.getStates().get("a").getAttempts());
        assertEquals(nextRetryAt, restored.getStates().get("a").getNextRetryAt());
        assertEquals(version, restored.getVersion());
        assertTrue(e2.tryAcquireLease().isEmpty(), "退避未到，重启后同样不派发");
        clock.advance(101);
        NodeLease l2 = e2.tryAcquireLease().orElseThrow();
        assertEquals(2, l2.getAttempt(), "attempts 跨重启持续累计");
    }
}
