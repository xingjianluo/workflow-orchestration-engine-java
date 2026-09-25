package com.github.xingjianluo.workfloworchestrationenginejava.tests;

import com.github.xingjianluo.workfloworchestrationenginejava.tests.support.MutableClock;
import com.github.xingjianluo.workfloworchestrationenginejava.tests.support.WorkflowHarness;
import com.github.xingjianluo.workfloworchestrationenginejava.workflow.model.CompletionOutcome;
import com.github.xingjianluo.workfloworchestrationenginejava.workflow.model.NodeLease;
import com.github.xingjianluo.workfloworchestrationenginejava.workflow.model.NodeStatus;
import com.github.xingjianluo.workfloworchestrationenginejava.workflow.model.WorkflowInstance;
import com.github.xingjianluo.workfloworchestrationenginejava.workflow.model.WorkflowStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Optional;

import static com.github.xingjianluo.workfloworchestrationenginejava.tests.support.WorkflowHarness.definition;
import static com.github.xingjianluo.workfloworchestrationenginejava.tests.support.WorkflowHarness.node;
import static com.github.xingjianluo.workfloworchestrationenginejava.tests.support.WorkflowHarness.nodeWithAttempts;
import static org.junit.jupiter.api.Assertions.*;

/** 自动重试：退避时间确定（100,200,400...）、次数到顶终态；人工重试：立即重置。 */
class RetryAndBackoffTest {

    @TempDir
    Path tempDir;
    private WorkflowHarness harness;

    @BeforeEach
    void setUp() {
        harness = new WorkflowHarness(tempDir, new MutableClock());
    }

    @Test
    void backoffIsDeterministicAndRetryCapIsHonored() {
        // 节点 maxAttempts=3：第 1、2 次失败分别退避 100ms、200ms，第 3 次失败终态
        String id = harness.engine.submit("retry-1",
                definition("wf", nodeWithAttempts("a", "flaky", 3))).getInstanceId();

        long t0 = harness.clock.nowMillis();

        NodeLease l1 = harness.engine.tryAcquireLease().orElseThrow();
        assertEquals(1, l1.getAttempt());
        assertEquals(CompletionOutcome.COMMITTED_WAIT_RETRY, harness.engine.completeFailure(l1, "e1"));
        assertEquals(NodeStatus.WAITING, harness.state(id, "a").getStatus());
        assertEquals(t0 + 100, harness.state(id, "a").getNextRetryAt(), "首次退避必须是 baseBackoff=100ms");

        // 退避未到不得派发
        harness.clock.advance(99);
        assertTrue(harness.engine.tryAcquireLease().isEmpty(), "退避还差 1ms，不得派发");
        harness.clock.advance(1);
        NodeLease l2 = harness.engine.tryAcquireLease().orElseThrow();
        assertEquals(2, l2.getAttempt());
        long t1 = harness.clock.nowMillis();
        assertEquals(CompletionOutcome.COMMITTED_WAIT_RETRY, harness.engine.completeFailure(l2, "e2"));
        assertEquals(t1 + 200, harness.state(id, "a").getNextRetryAt(), "第二次退避必须是 100*2=200ms");

        harness.clock.advance(200);
        NodeLease l3 = harness.engine.tryAcquireLease().orElseThrow();
        assertEquals(3, l3.getAttempt());
        assertEquals(CompletionOutcome.COMMITTED_TERMINAL_FAILURE, harness.engine.completeFailure(l3, "e3"));

        assertEquals(NodeStatus.FAILED, harness.state(id, "a").getStatus());
        assertEquals(3, harness.state(id, "a").getAttempts());
        assertEquals(WorkflowStatus.FAILED, harness.engine.get(id).getStatus());
        harness.dump("重试 3 次耗尽（退避 100/200ms）", id);
    }

    @Test
    void manualRetryResetsAttemptsAndRunsImmediately() {
        String id = harness.engine.submit("retry-2",
                definition("wf",
                        nodeWithAttempts("a", "flaky", 1),
                        node("b", "noop", "a"))).getInstanceId();

        NodeLease l1 = harness.engine.tryAcquireLease().orElseThrow();
        harness.engine.completeFailure(l1, "permanent");
        assertEquals(WorkflowStatus.FAILED, harness.engine.get(id).getStatus());
        assertEquals(NodeStatus.SKIPPED, harness.state(id, "b").getStatus());
        harness.dump("终态失败，等待人工重试", id);

        // 不推进时间，人工重试后应立即可以抢占
        long versionBefore = harness.engine.get(id).getVersion();
        WorkflowInstance revived = harness.engine.retryNode(id, "a", versionBefore);
        assertEquals(WorkflowStatus.RUNNING, revived.getStatus(), "人工重试使 FAILED 流程回到 RUNNING");
        assertEquals(NodeStatus.PENDING, harness.state(id, "a").getStatus());
        assertEquals(0, harness.state(id, "a").getAttempts(), "自动重试计数必须清零");

        // 注意：失败传播产生的 SKIPPED 是终态。人工重试 a 成功后，b 仍保持 SKIPPED ——
        // 若要连下游一起重跑，需要对每个目标节点分别人工重试
        NodeLease l2 = harness.engine.tryAcquireLease().orElseThrow();
        assertEquals(1, l2.getAttempt());
        harness.engine.completeSuccess(l2);
        assertEquals(NodeStatus.SUCCEEDED, harness.state(id, "a").getStatus());

        // 被连带 SKIPPED 的下游 b 允许人工重试：重新排队，且此时上游 a 已 SUCCEEDED
        harness.engine.retryNode(id, "b", harness.engine.get(id).getVersion());
        assertEquals(NodeStatus.PENDING, harness.state(id, "b").getStatus());
        NodeLease lb = harness.engine.tryAcquireLease().orElseThrow();
        assertEquals("b", lb.getNodeId());
        harness.engine.completeSuccess(lb);
        assertEquals(NodeStatus.SUCCEEDED, harness.state(id, "b").getStatus());
        assertEquals(WorkflowStatus.SUCCEEDED, harness.engine.get(id).getStatus(),
                "a 修复成功 + b 人工重试成功后，流程收敛为 SUCCEEDED");
        harness.dump("人工重试 a 与被连带跳过的 b 后流程 SUCCEEDED", id);
    }
}
