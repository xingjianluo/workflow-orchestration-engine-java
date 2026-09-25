package com.github.xingjianluo.workfloworchestrationenginejava.tests;

import com.github.xingjianluo.workfloworchestrationenginejava.tests.support.MutableClock;
import com.github.xingjianluo.workfloworchestrationenginejava.tests.support.WorkflowHarness;
import com.github.xingjianluo.workfloworchestrationenginejava.workflow.error.ErrorCode;
import com.github.xingjianluo.workfloworchestrationenginejava.workflow.error.WorkflowException;
import com.github.xingjianluo.workfloworchestrationenginejava.workflow.model.NodeLease;
import com.github.xingjianluo.workfloworchestrationenginejava.workflow.model.NodeStatus;
import com.github.xingjianluo.workfloworchestrationenginejava.workflow.model.WorkflowInstance;
import com.github.xingjianluo.workfloworchestrationenginejava.workflow.model.WorkflowStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static com.github.xingjianluo.workfloworchestrationenginejava.tests.support.WorkflowHarness.definition;
import static com.github.xingjianluo.workfloworchestrationenginejava.tests.support.WorkflowHarness.node;
import static org.junit.jupiter.api.Assertions.*;

/** 人工干预：暂停/恢复/取消、取消幂等、非法转换、终态拒绝、过期版本拒绝，错误码均可区分。 */
class InterventionTest {

    @TempDir
    Path tempDir;
    private WorkflowHarness harness;

    @BeforeEach
    void setUp() {
        harness = new WorkflowHarness(tempDir, new MutableClock());
    }

    @Test
    void pausePreventsDispatchAndResumeContinues() {
        String id = harness.engine.submit("iv-1", definition("wf",
                node("a", "noop"), node("b", "noop", "a"))).getInstanceId();

        WorkflowInstance paused = harness.engine.pause(id, harness.engine.get(id).getVersion());
        assertEquals(WorkflowStatus.PAUSED, paused.getStatus());
        assertTrue(harness.engine.tryAcquireLease().isEmpty(), "暂停期间不得派发任何节点");

        // 已暂停再暂停：非法状态转换
        WorkflowException doublePause = assertThrows(WorkflowException.class,
                () -> harness.engine.pause(id, paused.getVersion()));
        assertEquals(ErrorCode.ILLEGAL_TRANSITION, doublePause.getErrorCode());

        WorkflowInstance resumed = harness.engine.resume(id, paused.getVersion());
        assertEquals(WorkflowStatus.RUNNING, resumed.getStatus());
        NodeLease lease = harness.engine.tryAcquireLease().orElseThrow();
        assertEquals("a", lease.getNodeId());
        harness.dump("恢复后正常派发", id);

        // 对 RUNNING 直接 resume 是非法转换（先取最新版本，避免过期版本先触发 STALE_INTERVENTION）
        long currentVersion = harness.engine.get(id).getVersion();
        WorkflowException badResume = assertThrows(WorkflowException.class,
                () -> harness.engine.resume(id, currentVersion));
        assertEquals(ErrorCode.ILLEGAL_TRANSITION, badResume.getErrorCode());
    }

    @Test
    void cancelIsIdempotentButRejectedOnOtherTerminalStates() {
        String id = harness.engine.submit("iv-2", definition("wf",
                node("a", "noop"), node("b", "noop", "a"))).getInstanceId();

        WorkflowInstance c1 = harness.engine.cancel(id, 0L);
        assertEquals(WorkflowStatus.CANCELLED, c1.getStatus());
        assertEquals(NodeStatus.CANCELLED, harness.state(id, "a").getStatus());
        assertEquals(NodeStatus.CANCELLED, harness.state(id, "b").getStatus());

        // 再次取消：幂等成功（版本匹配时原样返回）
        WorkflowInstance c2 = assertDoesNotThrow(() -> harness.engine.cancel(id, c1.getVersion()));
        assertEquals(WorkflowStatus.CANCELLED, c2.getStatus());

        // 对已取消流程暂停/恢复：终态拒绝
        assertEquals(ErrorCode.ALREADY_TERMINAL,
                assertThrows(WorkflowException.class, () -> harness.engine.pause(id, c2.getVersion())).getErrorCode());
        assertEquals(ErrorCode.ALREADY_TERMINAL,
                assertThrows(WorkflowException.class, () -> harness.engine.resume(id, c2.getVersion())).getErrorCode());

        // 已 SUCCEEDED 的流程取消必须被拒绝（取消只对 CANCELLED 自身幂等）
        String id2 = harness.engine.submit("iv-2b", definition("wf", node("a", "noop"))).getInstanceId();
        NodeLease lease = harness.engine.tryAcquireLease().orElseThrow();
        harness.engine.completeSuccess(lease);
        assertEquals(WorkflowStatus.SUCCEEDED, harness.engine.get(id2).getStatus());
        WorkflowException cancelDone = assertThrows(WorkflowException.class,
                () -> harness.engine.cancel(id2, null));
        assertEquals(ErrorCode.ALREADY_TERMINAL, cancelDone.getErrorCode());
    }

    @Test
    void cancelReclaimsRunningNodeAndVoidsLateCompletion() {
        String id = harness.engine.submit("iv-6", definition("wf",
                node("a", "noop"), node("b", "noop", "a"))).getInstanceId();
        NodeLease lease = harness.engine.tryAcquireLease().orElseThrow();
        assertEquals(NodeStatus.RUNNING, harness.state(id, "a").getStatus());

        harness.engine.cancel(id, harness.engine.get(id).getVersion());
        assertEquals(NodeStatus.CANCELLED, harness.state(id, "a").getStatus(), "在执行节点也被取消并回收租约");
        assertEquals(NodeStatus.CANCELLED, harness.state(id, "b").getStatus());
        assertNull(harness.state(id, "a").getLeaseToken());

        assertEquals(com.github.xingjianluo.workfloworchestrationenginejava.workflow.model.CompletionOutcome.LEASE_LOST,
                harness.engine.completeSuccess(lease), "取消后迟到回传必须作废");
        assertEquals(NodeStatus.CANCELLED, harness.state(id, "a").getStatus(), "迟到回传不得污染终态");
        assertEquals(WorkflowStatus.CANCELLED, harness.engine.get(id).getStatus());
        harness.dump("取消在执行中的流程后", id);
    }

    @Test
    void staleVersionInterventionIsRejected() {
        String id = harness.engine.submit("iv-3", definition("wf", node("a", "noop"))).getInstanceId();

        // 客户端拿着旧版本 0，但实际版本已经因为暂停->恢复推进到 2
        WorkflowInstance p = harness.engine.pause(id, 0L);
        harness.engine.resume(id, p.getVersion());

        WorkflowException stale = assertThrows(WorkflowException.class,
                () -> harness.engine.pause(id, 0L));
        assertEquals(ErrorCode.STALE_INTERVENTION, stale.getErrorCode());
        assertTrue(stale.getMessage().contains("expected=0"));
        assertTrue(stale.getMessage().contains("actual=2"));

        // 版本正确时操作成功 —— 证明拒绝原因确实是版本过期而非其它
        WorkflowInstance ok = harness.engine.pause(id, 2L);
        assertEquals(WorkflowStatus.PAUSED, ok.getStatus());
        harness.dump("过期干预被拒绝后，正确版本可操作", id);
    }

    @Test
    void pauseFreezesRunningNodeAndDiscardsLateCompletion() {
        String id = harness.engine.submit("iv-5", definition("wf",
                node("a", "noop"), node("b", "noop", "a"))).getInstanceId();
        NodeLease lease = harness.engine.tryAcquireLease().orElseThrow();
        assertEquals(NodeStatus.RUNNING, harness.state(id, "a").getStatus());

        WorkflowInstance paused = harness.engine.pause(id, harness.engine.get(id).getVersion());
        assertEquals(WorkflowStatus.PAUSED, paused.getStatus());
        assertEquals(NodeStatus.PENDING, harness.state(id, "a").getStatus(), "在执行节点被冻结回 PENDING");
        assertEquals(0, harness.state(id, "a").getAttempts(), "冻结不消耗重试额度");
        assertNull(harness.state(id, "a").getLeaseToken());

        // 执行线程迟到的成功/失败回传一律作废，暂停期间状态不演进
        assertEquals(com.github.xingjianluo.workfloworchestrationenginejava.workflow.model.CompletionOutcome.LEASE_LOST,
                harness.engine.completeSuccess(lease));
        assertEquals(com.github.xingjianluo.workfloworchestrationenginejava.workflow.model.CompletionOutcome.LEASE_LOST,
                harness.engine.completeFailure(lease, "late"));
        assertEquals(NodeStatus.PENDING, harness.state(id, "a").getStatus());
        assertTrue(harness.engine.tryAcquireLease().isEmpty(), "暂停期间仍不得重新派发");

        // 恢复后重新调度执行，attempts 从 1 重新计数
        harness.engine.resume(id, harness.engine.get(id).getVersion());
        NodeLease lease2 = harness.engine.tryAcquireLease().orElseThrow();
        assertEquals(1, lease2.getAttempt());
        assertNotEquals(lease.getToken(), lease2.getToken());
        harness.engine.completeSuccess(lease2);
        harness.dump("暂停冻结后恢复执行", id);
    }

    @Test
    void manualRetryRejectedForSucceededAndRunningNodes() {
        String id = harness.engine.submit("iv-4", definition("wf",
                node("a", "noop"), node("b", "noop", "a"))).getInstanceId();
        NodeLease a = harness.engine.tryAcquireLease().orElseThrow();
        harness.engine.completeSuccess(a);
        NodeLease b = harness.engine.tryAcquireLease().orElseThrow();
        // b RUNNING 时人工重试：非法转换
        assertEquals(ErrorCode.ILLEGAL_TRANSITION,
                assertThrows(WorkflowException.class,
                        () -> harness.engine.retryNode(id, "b", harness.engine.get(id).getVersion())).getErrorCode());
        harness.engine.completeSuccess(b);
        // b SUCCEEDED：终态拒绝
        assertEquals(ErrorCode.ALREADY_TERMINAL,
                assertThrows(WorkflowException.class,
                        () -> harness.engine.retryNode(id, "b", harness.engine.get(id).getVersion())).getErrorCode());
        // 不存在的节点
        assertEquals(ErrorCode.INSTANCE_NOT_FOUND,
                assertThrows(WorkflowException.class,
                        () -> harness.engine.retryNode(id, "ghost", harness.engine.get(id).getVersion())).getErrorCode());
    }
}
