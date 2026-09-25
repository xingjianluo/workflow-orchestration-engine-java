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
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.github.xingjianluo.workfloworchestrationenginejava.tests.support.WorkflowHarness.definition;
import static com.github.xingjianluo.workfloworchestrationenginejava.tests.support.WorkflowHarness.node;
import static org.junit.jupiter.api.Assertions.*;

/** 租约一致性：并发抢占只有一方成功；有效租约不可抢；过期后可接管；失效 token 回传作废。 */
class LeaseConcurrencyTest {

    @TempDir
    Path tempDir;
    private WorkflowHarness harness;

    @BeforeEach
    void setUp() {
        harness = new WorkflowHarness(tempDir, new MutableClock());
    }

    @Test
    void onlyOneOfManyThreadsAcquiresLease() throws Exception {
        String id = harness.engine.submit("lease-1", definition("wf", node("a", "noop"))).getInstanceId();

        int threads = 24;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        Set<String> tokens = ConcurrentHashMap.newKeySet();

        for (int i = 0; i < threads; i++) {
            pool.submit(() -> {
                try {
                    start.await();
                    harness.engine.tryAcquireLease().ifPresent(l -> tokens.add(l.getToken()));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            });
        }
        start.countDown();
        assertTrue(done.await(10, TimeUnit.SECONDS));
        pool.shutdownNow();

        assertEquals(1, tokens.size(), "24 线程并发抢占同一节点，最多只能有一个租约");
        assertEquals(NodeStatus.RUNNING, harness.state(id, "a").getStatus());
        assertNotNull(harness.state(id, "a").getLeaseOwner());
        harness.dump("并发抢占后（租约归属唯一）", id);
    }

    @Test
    void validLeaseCannotBeStolen() {
        String id = harness.engine.submit("lease-2", definition("wf", node("a", "noop"))).getInstanceId();
        Optional<NodeLease> first = harness.engine.tryAcquireLease();
        assertTrue(first.isPresent());

        // 租约未过期：任何其他抢占必须空手
        Optional<NodeLease> second = harness.engine.tryAcquireLease();
        assertTrue(second.isEmpty(), "租约仍在有效期（ttl=1000ms），不得重复授予");
        assertEquals(first.get().getToken(), harness.state(id, "a").getLeaseToken());
    }

    @Test
    void expiredLeaseCanBeTakenOverAndOldCompletionIsVoided() {
        String id = harness.engine.submit("lease-3", definition("wf",
                node("a", "noop"), node("b", "noop", "a"))).getInstanceId();

        NodeLease oldLease = harness.engine.tryAcquireLease().orElseThrow();
        harness.dump("旧 owner 获得租约", id);

        // 推进时间超过 ttl，旧持有者"僵死"没有续租；新抢占应安全接管
        harness.clock.advance(1_001);
        NodeLease newLease = harness.engine.tryAcquireLease().orElseThrow();
        assertNotEquals(oldLease.getToken(), newLease.getToken(), "接管必须颁发新 token");
        assertEquals(newLease.getToken(), harness.state(id, "a").getLeaseToken());
        harness.dump("租约过期后被接管（owner 已变更）", id);

        // 旧持有者醒来回传成功/失败，都必须因 token 不匹配而作废，绝不覆盖接管者状态
        CompletionOutcome staleSuccess = harness.engine.completeSuccess(oldLease);
        assertEquals(CompletionOutcome.LEASE_LOST, staleSuccess, "旧 token 成功回传必须作废");
        assertEquals(NodeStatus.RUNNING, harness.state(id, "a").getStatus(),
                "作废回传不得把接管者的 RUNNING 改成 SUCCEEDED");
        CompletionOutcome staleFailure = harness.engine.completeFailure(oldLease, "late");
        assertEquals(CompletionOutcome.LEASE_LOST, staleFailure, "旧 token 失败回传必须作废");
        assertEquals(NodeStatus.RUNNING, harness.state(id, "a").getStatus());

        // 新持有者正常成功，节点 SUCCEEDED，随后 b 可被调度（上游成功）
        assertEquals(CompletionOutcome.COMMITTED_SUCCESS, harness.engine.completeSuccess(newLease));
        assertEquals(NodeStatus.SUCCEEDED, harness.state(id, "a").getStatus());
        NodeLease leaseB = harness.engine.tryAcquireLease().orElseThrow();
        assertEquals("b", leaseB.getNodeId(), "上游 a 成功后 b 才可被派发");
        harness.engine.completeSuccess(leaseB);
        WorkflowInstance wf = harness.engine.get(id);
        assertEquals(WorkflowStatus.SUCCEEDED, wf.getStatus());
        harness.dump("新 owner 完成后流程 SUCCEEDED", id);
    }

    @Test
    void downstreamNotExecutedWhenUpstreamFails() {
        String id = harness.engine.submit("lease-4", definition("wf",
                node("a", "noop"), node("b", "noop", "a"), node("c", "noop", "b"))).getInstanceId();

        NodeLease a = harness.engine.tryAcquireLease().orElseThrow();
        assertEquals("a", a.getNodeId());
        // a 失败 3 次（默认上限）：前两次进入退避，第三次终态失败
        for (int i = 1; i <= 2; i++) {
            assertEquals(CompletionOutcome.COMMITTED_WAIT_RETRY,
                    harness.engine.completeFailure(a, "boom-" + i), "第 " + i + " 次失败应进入退避");
            harness.clock.advance(10_000);
            Optional<NodeLease> next = harness.engine.tryAcquireLease();
            assertTrue(next.isPresent(), "退避到期后第 " + (i + 1) + " 次应能获得租约");
            a = next.get();
        }
        assertEquals(CompletionOutcome.COMMITTED_TERMINAL_FAILURE,
                harness.engine.completeFailure(a, "boom-final"), "第 3 次失败应为终态");

        assertEquals(NodeStatus.FAILED, harness.state(id, "a").getStatus());
        assertEquals(NodeStatus.SKIPPED, harness.state(id, "b").getStatus(), "上游失败，下游不得执行 -> SKIPPED");
        assertEquals(NodeStatus.SKIPPED, harness.state(id, "c").getStatus());
        assertTrue(harness.engine.tryAcquireLease().isEmpty(), "无任何可派发节点");
        WorkflowInstance wf = harness.engine.get(id);
        assertEquals(WorkflowStatus.FAILED, wf.getStatus());
        harness.dump("上游 a 失败，b/c 均不执行", id);
    }
}
