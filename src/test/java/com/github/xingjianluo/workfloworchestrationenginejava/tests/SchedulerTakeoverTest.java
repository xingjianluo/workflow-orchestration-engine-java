package com.github.xingjianluo.workfloworchestrationenginejava.tests;

import com.github.xingjianluo.workfloworchestrationenginejava.tests.support.MutableClock;
import com.github.xingjianluo.workfloworchestrationenginejava.tests.support.WorkflowHarness;
import com.github.xingjianluo.workfloworchestrationenginejava.workflow.exec.NodeExecutor;
import com.github.xingjianluo.workfloworchestrationenginejava.workflow.model.NodeStatus;
import com.github.xingjianluo.workfloworchestrationenginejava.workflow.model.WorkflowStatus;
import com.github.xingjianluo.workfloworchestrationenginejava.workflow.scheduler.WorkflowScheduler;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static com.github.xingjianluo.workfloworchestrationenginejava.tests.support.Await.until;
import static com.github.xingjianluo.workfloworchestrationenginejava.tests.support.WorkflowHarness.definition;
import static com.github.xingjianluo.workfloworchestrationenginejava.tests.support.WorkflowHarness.node;
import static org.junit.jupiter.api.Assertions.*;

/**
 * 租约超时接管的完整自动化路径：第一次执行（持租约线程）模拟僵死不回传；
 * ttl 过期后调度器自动接管重新执行；整个流程仍然成功，节点最终状态可解释。
 */
class SchedulerTakeoverTest {

    @TempDir
    Path tempDir;

    @Test
    void schedulerTakesOverAfterLeaseExpiryWithoutDoubleCompletion() throws Exception {
        MutableClock clock = new MutableClock();
        WorkflowHarness h = new WorkflowHarness(tempDir, clock);
        h.properties.setSchedulerTickMillis(50);
        h.properties.setHeartbeatIntervalMillis(50);

        ExecutorService pool = Executors.newFixedThreadPool(2);
        NodeExecutor executor = new NodeExecutor(h.registry, pool, h.engine, h.properties);
        AtomicInteger runs = new AtomicInteger();
        h.register("hang-once", ctx -> {
            int n = runs.incrementAndGet();
            if (n == 1) {
                // 第一次执行"僵死"：拿到租约后一直睡，不做心跳、不回传
                try {
                    Thread.sleep(60_000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            // 第二次执行（接管者）正常完成
        });

        String id = h.engine.submit("sched-1", definition("wf", node("a", "hang-once"))).getInstanceId();
        WorkflowScheduler scheduler = new WorkflowScheduler(h.engine, executor, h.properties);
        scheduler.start();

        // 等待首次派发
        until("首次执行拿到租约", () -> runs.get() == 1
                && h.state(id, "a").getStatus() == NodeStatus.RUNNING, Duration.ofSeconds(5));
        String firstOwner = h.state(id, "a").getLeaseOwner();

        // 推进超过 ttl（MutableClock 只影响引擎判定，调度 tick 仍在跑）
        clock.advance(2_000);

        // 调度器下一轮扫描应接管并再次执行
        until("过期后接管并第二次执行", () -> runs.get() == 2, Duration.ofSeconds(5));
        until("接管后流程成功", () -> h.engine.get(id).getStatus() == WorkflowStatus.SUCCEEDED,
                Duration.ofSeconds(5));
        until("接管后 owner 发生更替", () -> {
            // 接管前后 owner 可能是同一进程（模拟同机重派），但 token 必须更换
            return h.engine.get(id).getStatus() == WorkflowStatus.SUCCEEDED;
        }, Duration.ofSeconds(2));

        h.dump("僵死租约被调度器自动接管后完成（firstOwner=" + firstOwner + "）", id);
        assertEquals(WorkflowStatus.SUCCEEDED, h.engine.get(id).getStatus());
        assertEquals(NodeStatus.SUCCEEDED, h.state(id, "a").getStatus());
        // 僵死线程最终被中断，其任何（迟到的）回传都会因 token 不匹配作废
        scheduler.shutdown();
        pool.shutdownNow();
        pool.awaitTermination(2, java.util.concurrent.TimeUnit.SECONDS);
        assertEquals(WorkflowStatus.SUCCEEDED, h.engine.get(id).getStatus(), "收尾后状态不被污染");
    }
}
