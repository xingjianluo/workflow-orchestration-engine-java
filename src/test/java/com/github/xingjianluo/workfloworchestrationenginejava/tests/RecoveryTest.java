package com.github.xingjianluo.workfloworchestrationenginejava.tests;

import com.github.xingjianluo.workfloworchestrationenginejava.config.EngineProperties;
import com.github.xingjianluo.workfloworchestrationenginejava.engine.DefinitionValidator;
import com.github.xingjianluo.workfloworchestrationenginejava.engine.RetryPolicy;
import com.github.xingjianluo.workfloworchestrationenginejava.engine.WorkflowEngine;
import com.github.xingjianluo.workfloworchestrationenginejava.handler.NodeTaskHandler;
import com.github.xingjianluo.workfloworchestrationenginejava.model.NodeState;
import com.github.xingjianluo.workfloworchestrationenginejava.model.WorkflowDefinition;
import com.github.xingjianluo.workfloworchestrationenginejava.model.WorkflowInstance;
import com.github.xingjianluo.workfloworchestrationenginejava.model.WorkflowState;
import com.github.xingjianluo.workfloworchestrationenginejava.store.JsonFileWorkflowStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

/**
 * 进程恢复：引擎 1 在节点 RUNNING 期间“崩溃”（直接 stop，不等待任务结束），
 * 引擎 2 从同一目录启动并开启 recoverOnStartup，RUNNING 节点必须被重置后重新执行，
 * 最终状态一致、可解释。
 */
class RecoveryTest extends AbstractEngineTest {

    @TempDir
    Path tmp;

    @Test
    void runningNodeAfterRestartIsReacquiredAndWorkflowFinishes() throws Exception {
        // ---- 第一个“进程”：a 拿到租约后引擎被强行停止 ----
        setupEngine(tmp, 60_000L, 2);
        WorkflowDefinition def = definition("restart-1", 3, 10L,
                Map.of("a", List.of(), "b", List.of("a")));
        java.util.concurrent.CountDownLatch aEntered = new java.util.concurrent.CountDownLatch(1);
        java.util.concurrent.CountDownLatch hangForever = new java.util.concurrent.CountDownLatch(1);
        engine.submit(def, Map.of(
                "a", node -> { aEntered.countDown(); hangForever.await(); },
                "b", node -> { }));
        aEntered.await(5, java.util.concurrent.TimeUnit.SECONDS);
        awaitUntil(() -> snapshot("restart-1").getNodes().get("a").getState() == NodeState.RUNNING);
        String deadOwner = snapshot("restart-1").getNodes().get("a").getLeaseOwner();
        String deadExecutor = engine.getExecutorId();

        // 模拟进程崩溃：不等任务、不做优雅收尾
        engine.stop();
        hangForever.countDown(); // 解除测试线程阻塞

        // 磁盘上 a 仍是 RUNNING 且带着旧租约
        JsonFileWorkflowStore rawStore = new JsonFileWorkflowStore(props);
        WorkflowInstance onDisk = rawStore.find("restart-1").orElseThrow();
        assertEquals(NodeState.RUNNING, onDisk.getNodes().get("a").getState());
        assertEquals(deadOwner, onDisk.getNodes().get("a").getLeaseOwner());

        // ---- 第二个“进程”：同目录、开启恢复 ----
        EngineProperties props2 = new EngineProperties();
        props2.setStoreDir(tmp.toString());
        props2.setLeaseTtlMillis(60_000L);
        props2.setScanIntervalMillis(20);
        props2.setWorkerThreads(2);
        props2.setRecoverOnStartup(true);
        JsonFileWorkflowStore store2 = new JsonFileWorkflowStore(props2);
        store2.init();
        WorkflowEngine engine2 = new WorkflowEngine(store2, props2, clock,
                new RetryPolicy(), new DefinitionValidator());
        AtomicInteger aRunsAfterReboot = new AtomicInteger();
        AtomicInteger bRunsAfterReboot = new AtomicInteger();
        Map<String, NodeTaskHandler> hs2 = Map.of(
                "a", node -> aRunsAfterReboot.incrementAndGet(),
                "b", node -> bRunsAfterReboot.incrementAndGet());
        // 先启动（触发磁盘恢复），再用同一 requestId 重新 submit 以合并业务处理器；
        // 已存在实例不会重复创建，返回原实例
        engine2.start();
        WorkflowInstance returned = engine2.submit(def, hs2);
        assertEquals(WorkflowState.ACTIVE, returned.getState());
        try {
            awaitWorkflowWith(engine2, "restart-1", WorkflowState.SUCCEEDED);
            WorkflowInstance done = engine2.get("restart-1").orElseThrow();
            assertEquals(NodeState.SUCCEEDED, done.getNodes().get("a").getState());
            assertEquals(NodeState.SUCCEEDED, done.getNodes().get("b").getState());
            assertEquals(1, aRunsAfterReboot.get(), "a must execute exactly once after reboot");
            assertEquals(1, bRunsAfterReboot.get());
            assertNotEquals(deadExecutor, engine2.getExecutorId());
            assertNotEquals(deadOwner, done.getNodes().get("a").getLeaseOwner());
        } finally {
            engine2.stop();
        }
    }

    private void awaitWorkflowWith(WorkflowEngine eng, String id, WorkflowState state) {
        org.awaitility.Awaitility.await()
                .atMost(java.time.Duration.ofSeconds(10))
                .pollInterval(java.time.Duration.ofMillis(20))
                .until(() -> eng.get(id).orElseThrow().getState() == state);
    }
}
