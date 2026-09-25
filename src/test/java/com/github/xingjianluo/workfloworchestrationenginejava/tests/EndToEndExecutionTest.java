package com.github.xingjianluo.workfloworchestrationenginejava.tests;

import com.github.xingjianluo.workfloworchestrationenginejava.tests.support.MutableClock;
import com.github.xingjianluo.workfloworchestrationenginejava.tests.support.WorkflowHarness;
import com.github.xingjianluo.workfloworchestrationenginejava.workflow.exec.NodeExecutor;
import com.github.xingjianluo.workfloworchestrationenginejava.workflow.model.NodeStatus;
import com.github.xingjianluo.workfloworchestrationenginejava.workflow.model.WorkflowInstance;
import com.github.xingjianluo.workfloworchestrationenginejava.workflow.model.WorkflowStatus;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static com.github.xingjianluo.workfloworchestrationenginejava.tests.support.Await.until;
import static com.github.xingjianluo.workfloworchestrationenginejava.tests.support.WorkflowHarness.definition;
import static com.github.xingjianluo.workfloworchestrationenginejava.tests.support.WorkflowHarness.node;
import static org.junit.jupiter.api.Assertions.*;

/** 端到端：真实线程池执行 + 心跳续租 + 调度循环（手动 tick 驱动，时钟手动推进退避）。 */
class EndToEndExecutionTest {

    @TempDir
    Path tempDir;

    /** 两个独立分支 + 汇聚：每个节点恰好执行一次（成功路径）。 */
    @Test
    void diamondWorkflowRunsEveryNodeExactlyOnce() {
        MutableClock clock = new MutableClock();
        WorkflowHarness h = new WorkflowHarness(tempDir, clock);
        ExecutorService pool = Executors.newFixedThreadPool(4);
        NodeExecutor executor = new NodeExecutor(h.registry, pool, h.engine, h.properties);
        Map<String, AtomicInteger> execCount = new ConcurrentHashMap<>();

        for (String n : new String[]{"a", "b", "c", "d"}) {
            execCount.put(n, new AtomicInteger());
        }
        h.register("once", ctx -> {
            // 模拟业务处理；心跳确认租约仍属于自己
            assertTrue(ctx.heartbeat(), "执行期间心跳必须成功（租约未被接管）");
            execCount.get(ctx.getNodeId()).incrementAndGet();
        });

        String id = h.engine.submit("e2e-1", definition("diamond",
                node("a", "once"),
                node("b", "once", "a"),
                node("c", "once", "a"),
                node("d", "once", "b", "c"))).getInstanceId();

        // 模拟调度器：不断抢占并提交，直到无节点可派发（含等待执行中的节点完成）
        driveUntilDone(h, executor, id, 50);

        WorkflowInstance wf = h.engine.get(id);
        h.dump("菱形流程执行完毕", id);
        assertEquals(WorkflowStatus.SUCCEEDED, wf.getStatus());
        for (String n : new String[]{"a", "b", "c", "d"}) {
            assertEquals(1, execCount.get(n).get(), "节点 " + n + " 必须恰好实际执行一次");
            assertEquals(NodeStatus.SUCCEEDED, wf.getStates().get(n).getStatus());
        }
        pool.shutdownNow();
        executor.shutdown();
    }

    /** 业务失败两次后第三次成功：退避由调度循环推进时钟模拟，最终 SUCCEEDED。 */
    @Test
    void failingNodeRetriesAndEventuallySucceeds() {
        MutableClock clock = new MutableClock();
        WorkflowHarness h = new WorkflowHarness(tempDir, clock);
        ExecutorService pool = Executors.newFixedThreadPool(2);
        NodeExecutor executor = new NodeExecutor(h.registry, pool, h.engine, h.properties);
        AtomicInteger attempts = new AtomicInteger();
        h.register("flaky", ctx -> {
            int n = attempts.incrementAndGet();
            if (n < 3) {
                throw new RuntimeException("transient-" + n);
            }
        });

        String id = h.engine.submit("e2e-2", definition("retry", node("a", "flaky"))).getInstanceId();
        driveUntilDone(h, executor, id, 100);

        assertEquals(3, attempts.get(), "必须实际执行 3 次（1 次初始 + 2 次自动重试）");
        assertEquals(WorkflowStatus.SUCCEEDED, h.engine.get(id).getStatus());
        h.dump("失败重试两次后成功", id);
        pool.shutdownNow();
        executor.shutdown();
    }

    /**
     * 模拟调度循环：抢占 -> 提交真实线程池执行；每次循环把时钟推进超过退避阈值，
     * 直到流程终态或达到最大轮次。等待每轮所有提交的任务回传完成。
     */
    private void driveUntilDone(WorkflowHarness h, NodeExecutor executor, String id, int maxRounds) {
        for (int round = 0; round < maxRounds; round++) {
            WorkflowStatus st = h.engine.get(id).getStatus();
            if (st == WorkflowStatus.SUCCEEDED || st == WorkflowStatus.FAILED
                    || st == WorkflowStatus.CANCELLED) {
                return;
            }
            boolean[] dispatched = {false};
            h.engine.tryAcquireLease().ifPresent(lease -> {
                executor.submit(lease);
                dispatched[0] = true;
            });
            if (dispatched[0]) {
                // 等待该任务回传（状态离开 RUNNING）
                until("节点执行回传", () -> {
                    NodeStatus s = h.engine.get(id).getStates().values().stream()
                            .filter(n -> n.getStatus() == NodeStatus.RUNNING).findFirst()
                            .map(n -> NodeStatus.RUNNING).orElse(null);
                    return s == null;
                }, Duration.ofSeconds(5));
            }
            // 推进退避（100/200ms）与租约
            h.clock.advance(300);
        }
        fail("调度超过最大轮次仍未终态: " + h.engine.get(id).getStatus());
    }
}
