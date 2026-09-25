package com.github.xingjianluo.workfloworchestrationenginejava.tests;

import com.github.xingjianluo.workfloworchestrationenginejava.error.ErrorCode;
import com.github.xingjianluo.workfloworchestrationenginejava.error.WorkflowException;
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
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class InterventionTest extends AbstractEngineTest {

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

    private WorkflowDefinition chain(String id) {
        return definition(id, 1, 0L, new java.util.LinkedHashMap<>() {{
            put("a", List.of());
            put("b", List.of("a"));
        }});
    }

    private Map<String, NodeTaskHandler> okHandlers(WorkflowDefinition def) {
        return sameHandler(def, node -> { });
    }

    @Test
    void pauseStopsSchedulingAndResumeContinues() throws Exception {
        // 暂停时 a 尚未完成，因此 a,b 都应停在 PAUSED；恢复后链路跑完
        WorkflowDefinition def = chain("pause-resume");
        java.util.concurrent.CountDownLatch aEntered = new java.util.concurrent.CountDownLatch(1);
        java.util.concurrent.CountDownLatch releaseA = new java.util.concurrent.CountDownLatch(1);
        Map<String, NodeTaskHandler> hs = Map.of(
                "a", node -> {
                    aEntered.countDown();
                    releaseA.await();
                },
                "b", node -> { });
        engine.submit(def, hs);
        assertTrue(aEntered.await(5, java.util.concurrent.TimeUnit.SECONDS));
        engine.pause("pause-resume", 0L);
        assertEquals(WorkflowState.PAUSED, snapshot("pause-resume").getState());
        assertEquals(NodeState.PAUSED, snapshot("pause-resume").getNodes().get("b").getState());
        // 暂停期间反复扫描，b 不得被拉起
        Thread.sleep(300);
        assertEquals(NodeState.PAUSED, snapshot("pause-resume").getNodes().get("b").getState());

        releaseA.countDown();
        long v = snapshot("pause-resume").getVersion(); // pause 后 version=1
        engine.resume("pause-resume", v);
        awaitWorkflow("pause-resume", WorkflowState.SUCCEEDED);
        assertEquals(NodeState.SUCCEEDED, nodeState("pause-resume", "b"));
    }

    @Test
    void cancelIsIdempotentAndBlocksTerminalTransitions() {
        WorkflowDefinition def = chain("cancel");
        engine.submit(def, okHandlers(def));
        engine.cancel("cancel", 0L);
        assertEquals(WorkflowState.CANCELLED, snapshot("cancel").getState());
        // 再次取消：幂等成功
        engine.cancel("cancel", 1L);
        engine.cancel("cancel", 1L);

        // 终态上的其他操作必须被拒绝
        WorkflowException pause = assertThrows(WorkflowException.class,
                () -> engine.pause("cancel", 1L));
        assertEquals(ErrorCode.ALREADY_TERMINAL, pause.getErrorCode());
        WorkflowException resume = assertThrows(WorkflowException.class,
                () -> engine.resume("cancel", 1L));
        assertEquals(ErrorCode.ALREADY_TERMINAL, resume.getErrorCode());
    }

    @Test
    void illegalResumeIsRejected() {
        WorkflowDefinition def = chain("resume-illegal");
        engine.submit(def, okHandlers(def));
        WorkflowException ex = assertThrows(WorkflowException.class,
                () -> engine.resume("resume-illegal", 0L));
        assertEquals(ErrorCode.ILLEGAL_TRANSITION, ex.getErrorCode());
    }

    @Test
    void staleInterventionVersionIsRejected() {
        WorkflowDefinition def = chain("stale");
        engine.submit(def, okHandlers(def));
        engine.pause("stale", 0L); // version -> 1
        // 旧版本恢复
        WorkflowException ex = assertThrows(WorkflowException.class,
                () -> engine.resume("stale", 0L));
        assertEquals(ErrorCode.STALE_INTERVENTION, ex.getErrorCode());
        assertEquals(WorkflowState.PAUSED, snapshot("stale").getState());
    }

    @Test
    void manualRetryResetsFailedNodeAndReactivatesCascade() {
        // a 两次失败 -> FAILED，b 级联 CANCELLED；修复后人工重试 a，a/b 全部成功
        WorkflowDefinition def = definition("manual", 2, 10L, new java.util.LinkedHashMap<>() {{
            put("a", List.of());
            put("b", List.of("a"));
        }});
        AtomicInteger aRuns = new AtomicInteger();
        Map<String, NodeTaskHandler> hs = Map.of(
                "a", node -> {
                    if (aRuns.incrementAndGet() <= 2) {
                        throw new RuntimeException("persistent-bug");
                    }
                },
                "b", node -> { });
        engine.submit(def, hs);
        awaitWorkflow("manual", WorkflowState.FAILED);
        assertEquals(NodeState.FAILED, nodeState("manual", "a"));
        assertEquals(NodeState.CANCELLED, nodeState("manual", "b"));

        // b 已被级联取消（终态），对它人工重试应被拒绝，且原因可区分
        WorkflowException badTarget = assertThrows(WorkflowException.class,
                () -> engine.retryNode("manual", "b", 0L));
        assertEquals(ErrorCode.ALREADY_TERMINAL, badTarget.getErrorCode());

        long v = snapshot("manual").getVersion();
        engine.retryNode("manual", "a", v);
        awaitWorkflow("manual", WorkflowState.SUCCEEDED);
        assertEquals(NodeState.SUCCEEDED, nodeState("manual", "a"));
        assertEquals(NodeState.SUCCEEDED, nodeState("manual", "b"));
        // 人工重试给予新的完整预算：a 共执行 3 次（旧 2 + 新 1）
        assertEquals(3, aRuns.get());
        WorkflowInstance inst = snapshot("manual");
        assertEquals(1, inst.getNodes().get("a").getAttempts());
    }

    @Test
    void operationsOnUnknownWorkflowRejected() {
        WorkflowException ex = assertThrows(WorkflowException.class,
                () -> engine.pause("ghost", 0L));
        assertEquals(ErrorCode.WORKFLOW_NOT_FOUND, ex.getErrorCode());
        WorkflowException ex2 = assertThrows(WorkflowException.class,
                () -> engine.retryNode("ghost", "n", 0L));
        assertEquals(ErrorCode.WORKFLOW_NOT_FOUND, ex2.getErrorCode());
    }

    @Test
    void invalidDefinitionNeverPartiallyPersists() {
        WorkflowDefinition bad = definition("bad-def", 1, 0L, Map.of("a", List.of("ghost")));
        WorkflowException ex = assertThrows(WorkflowException.class,
                () -> engine.submit(bad, Map.of()));
        assertEquals(ErrorCode.MISSING_DEPENDENCY, ex.getErrorCode());
        assertTrue(engine.get("bad-def").isEmpty());
        // 修复后同 requestId 仍可正常提交（证明此前没有部分生效的实例）
        WorkflowDefinition good = definition("bad-def", 1, 0L, Map.of("a", List.of()));
        engine.submit(good, sameHandler(good, n -> { }));
        awaitWorkflow("bad-def", WorkflowState.SUCCEEDED);
    }
}
