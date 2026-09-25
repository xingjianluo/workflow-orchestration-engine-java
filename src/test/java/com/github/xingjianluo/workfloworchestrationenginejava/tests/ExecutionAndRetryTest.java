package com.github.xingjianluo.workfloworchestrationenginejava.tests;

import com.github.xingjianluo.workfloworchestrationenginejava.handler.NodeTaskHandler;
import com.github.xingjianluo.workfloworchestrationenginejava.model.NodeState;
import com.github.xingjianluo.workfloworchestrationenginejava.model.WorkflowDefinition;
import com.github.xingjianluo.workfloworchestrationenginejava.model.WorkflowInstance;
import com.github.xingjianluo.workfloworchestrationenginejava.model.WorkflowState;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 正常执行、依赖门控、固定退避重试、重试上限与上游失败连锁。
 */
class ExecutionAndRetryTest extends AbstractEngineTest {

    @TempDir
    Path tmp;

    @BeforeEach
    void setUp() {
        setupEngine(tmp, 5_000L, 4);
    }

    @AfterEach
    void tearDown() {
        teardownEngine();
    }

    @Test
    void diamondDagRunsInDependencyOrderAndSucceeds() throws Exception {
        // a -> b,c -> d
        WorkflowDefinition def = definition("dag", 1, 0L, new java.util.LinkedHashMap<>() {{
            put("a", List.of());
            put("b", List.of("a"));
            put("c", List.of("a"));
            put("d", List.of("b", "c"));
        }});
        Map<String, Integer> finishOrder = new ConcurrentHashMap<>();
        AtomicInteger seq = new AtomicInteger();
        CountDownLatch latch = new CountDownLatch(4);
        Map<String, NodeTaskHandler> handlers = new java.util.LinkedHashMap<>();
        for (String n : def.dependencies().keySet()) {
            handlers.put(n, node -> {
                Thread.sleep(10);
                finishOrder.put(node, seq.incrementAndGet());
                latch.countDown();
            });
        }
        engine.submit(def, handlers);
        assertTrue(latch.await(10, TimeUnit.SECONDS));
        awaitWorkflow("dag", WorkflowState.SUCCEEDED);

        assertTrue(finishOrder.get("a") < finishOrder.get("b"));
        assertTrue(finishOrder.get("a") < finishOrder.get("c"));
        assertTrue(finishOrder.get("b") < finishOrder.get("d"));
        assertTrue(finishOrder.get("c") < finishOrder.get("d"));
        for (String n : List.of("a", "b", "c", "d")) {
            assertEquals(NodeState.SUCCEEDED, nodeState("dag", n));
        }
    }

    @Test
    void failedUpstreamBlocksDownstreamAndCascadesCancel() {
        // a(fails) -> b；a 永久失败后 b 必须被级联取消且从不执行
        WorkflowDefinition def = definition("blocked", 1, 0L, Map.of(
                "a", List.of(),
                "b", List.of("a")));
        AtomicInteger bRuns = new AtomicInteger();
        Map<String, NodeTaskHandler> handlers = Map.of(
                "a", node -> { throw new IllegalStateException("boom-a"); },
                "b", node -> bRuns.incrementAndGet());
        engine.submit(def, handlers);

        awaitNode("blocked", "a", NodeState.FAILED);
        awaitNode("blocked", "b", NodeState.CANCELLED);
        awaitWorkflow("blocked", WorkflowState.FAILED);
        assertEquals(0, bRuns.get(), "downstream must never execute when upstream failed");
        WorkflowInstance inst = snapshot("blocked");
        assertTrue(inst.getNodes().get("b").isCascadeCancelled());
    }

    @Test
    void retriesWithFixedBackoffThenSucceeds() {
        // 前两次失败，第三次成功；退避固定 300ms，attempts 与 nextAttemptAt 可预期
        long backoff = 300L;
        WorkflowDefinition def = definition("retry-ok", 3, backoff, Map.of("a", List.of()));
        AtomicInteger runs = new AtomicInteger();
        engine.submit(def, sameHandler(def, node -> {
            if (runs.incrementAndGet() < 3) {
                throw new RuntimeException("transient");
            }
        }));

        awaitWorkflow("retry-ok", WorkflowState.SUCCEEDED);
        assertEquals(3, runs.get());
        WorkflowInstance inst = snapshot("retry-ok");
        assertEquals(3, inst.getNodes().get("a").getAttempts());
        assertEquals(NodeState.SUCCEEDED, inst.getNodes().get("a").getState());
    }

    @Test
    void retriesExhaustedLeadsToFailedWithExpectedAttempts() {
        WorkflowDefinition def = definition("retry-exhaust", 2, 10L, Map.of("a", List.of()));
        AtomicInteger runs = new AtomicInteger();
        engine.submit(def, sameHandler(def, node -> {
            runs.incrementAndGet();
            throw new RuntimeException("always fails");
        }));
        awaitWorkflow("retry-exhaust", WorkflowState.FAILED);
        assertEquals(2, runs.get(), "must execute exactly maxAttempts times");
        WorkflowInstance inst = snapshot("retry-exhaust");
        assertEquals(2, inst.getNodes().get("a").getAttempts());
        assertEquals(NodeState.FAILED, inst.getNodes().get("a").getState());
        assertTrue(inst.getNodes().get("a").getLastError().contains("always fails"));
    }
}
