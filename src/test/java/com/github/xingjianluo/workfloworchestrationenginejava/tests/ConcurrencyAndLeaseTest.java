package com.github.xingjianluo.workfloworchestrationenginejava.tests;

import com.github.xingjianluo.workfloworchestrationenginejava.error.ErrorCode;
import com.github.xingjianluo.workfloworchestrationenginejava.error.WorkflowException;
import com.github.xingjianluo.workfloworchestrationenginejava.handler.NodeTaskHandler;
import com.github.xingjianluo.workfloworchestrationenginejava.model.NodeState;
import com.github.xingjianluo.workfloworchestrationenginejava.model.WorkflowDefinition;
import com.github.xingjianluo.workfloworchestrationenginejava.model.WorkflowInstance;
import com.github.xingjianluo.workfloworchestrationenginejava.model.WorkflowState;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * 并发抢占与租约：
 * 1) 多线程重复提交同一 requestId 只产生一个实例、节点只执行一次；
 * 2) 租约过期后被安全接管，旧属主迟到结果被 CAS 拒绝，节点不双跑。
 */
class ConcurrencyAndLeaseTest extends AbstractEngineTest {

    @TempDir
    Path tmp;

    @AfterEach
    void tearDown() {
        teardownEngine();
    }

    @Test
    void duplicateSubmissionsFromMultipleThreadsCreateOneInstance() throws Exception {
        setupEngine(tmp, 5_000L, 8);
        WorkflowDefinition def = definition("dup-submit", 1, 0L,
                Map.of("a", List.of(), "b", List.of("a")));
        Map<String, AtomicInteger> runs = new ConcurrentHashMap<>();
        runs.put("a", new AtomicInteger());
        runs.put("b", new AtomicInteger());
        CountDownLatch done = new CountDownLatch(20);
        ExecutorService pool = Executors.newFixedThreadPool(20);
        for (int i = 0; i < 20; i++) {
            pool.submit(() -> {
                try {
                    Map<String, NodeTaskHandler> hs = Map.of(
                            "a", n -> runs.get("a").incrementAndGet(),
                            "b", n -> runs.get("b").incrementAndGet());
                    engine.submit(def, hs);
                } finally {
                    done.countDown();
                }
            });
        }
        assertTrue(done.await(10, TimeUnit.SECONDS));
        pool.shutdown();

        awaitWorkflow("dup-submit", WorkflowState.SUCCEEDED);
        WorkflowInstance inst = snapshot("dup-submit");
        assertEquals(1, inst.getNodes().get("a").getAttempts(), "node a must execute once");
        assertEquals(1, inst.getNodes().get("b").getAttempts(), "node b must execute once");
        assertEquals(1, runs.get("a").get());
        assertEquals(1, runs.get("b").get());
    }

    @Test
    void expiredLeaseIsTakenOverWithoutDoubleCommit() throws Exception {
        // 极短租约（100ms）：首个执行器卡住直到租约过期，被调度器接管；
        // 旧任务最终醒来并回交结果，必须被 CAS 拒绝；节点由接管者成功完成一次。
        setupEngine(tmp, 100L, 4);
        WorkflowDefinition def = definition("lease", 5, 0L, Map.of("a", List.of()));

        CountDownLatch firstEntered = new CountDownLatch(1);
        CountDownLatch letFirstFinish = new CountDownLatch(1);
        AtomicInteger successes = new AtomicInteger();
        AtomicInteger bodies = new AtomicInteger();

        NodeTaskHandler handler = node -> {
            int which = bodies.incrementAndGet();
            if (which == 1) {
                firstEntered.countDown();
                // 阻塞直到测试确认接管已发生（模拟执行器卡死/进程暂停）
                assertTrue(letFirstFinish.await(5, TimeUnit.SECONDS));
                // 旧执行器“复活”，返回成功——但租约已易主，结果应被拒绝
                return;
            }
            // 接管执行：立即成功
            successes.incrementAndGet();
        };
        engine.submit(def, Map.of("a", handler));

        // 等待第一次执行进入并获得租约
        assertTrue(firstEntered.await(5, TimeUnit.SECONDS));
        // 等待接管发生：attempts 增长到 2（首次 + 接管）
        awaitUntil(() -> snapshot("lease").getNodes().get("a").getAttempts() >= 2);
        WorkflowInstance mid = snapshot("lease");
        // 接管期间租约属主必为新的非空属主（若接管体已瞬间完成，节点已 SUCCEEDED）
        if (mid.getNodes().get("a").getState() == NodeState.RUNNING) {
            assertNotEquals(null, mid.getNodes().get("a").getLeaseOwner());
        }

        // 等接管执行成功提交
        awaitNode("lease", "a", NodeState.SUCCEEDED);
        WorkflowInstance after = snapshot("lease");
        assertEquals(NodeState.SUCCEEDED, after.getNodes().get("a").getState());
        assertEquals(2, after.getNodes().get("a").getAttempts(),
                "first attempt (stale) + one takeover attempt");
        assertEquals(1, successes.get(), "only takeover body completes the node");

        // 现在才放行旧执行器；其迟到结果必须被拒绝且状态不得翻转
        letFirstFinish.countDown();
        Thread.sleep(300);
        WorkflowInstance finalInst = snapshot("lease");
        assertEquals(NodeState.SUCCEEDED, finalInst.getNodes().get("a").getState());
        assertEquals(WorkflowState.SUCCEEDED, finalInst.getState());
        assertEquals(1, successes.get());
    }

    @Test
    void concurrentLeaseGrantsForSameNodeOnlyOneWins() throws Exception {
        // 宽租约 + 一个执行极慢的节点：调度器在租约有效期内反复扫描，不得重复授权
        setupEngine(tmp, 60_000L, 8);
        WorkflowDefinition def = definition("single", 3, 0L, Map.of("a", List.of()));
        CountDownLatch entered = new CountDownLatch(1);
        AtomicInteger bodies = new AtomicInteger();
        engine.submit(def, Map.of("a", node -> {
            bodies.incrementAndGet();
            entered.countDown();
            Thread.sleep(500);
        }));
        assertTrue(entered.await(5, TimeUnit.SECONDS));
        Thread.sleep(400); // 让扫描器多跑几轮
        assertEquals(1, bodies.get(), "valid lease must not be re-granted while unexpired");
        awaitWorkflow("single", WorkflowState.SUCCEEDED);
        assertEquals(1, snapshot("single").getNodes().get("a").getAttempts());
    }

    @Test
    void parallelIndependentNodesRunConcurrentlyButOnceEach() throws Exception {
        setupEngine(tmp, 5_000L, 8);
        Map<String, List<String>> deps = new java.util.LinkedHashMap<>();
        for (int i = 0; i < 8; i++) {
            deps.put("n" + i, List.of());
        }
        WorkflowDefinition def = definition("parallel", 1, 0L, deps);
        CountDownLatch allIn = new CountDownLatch(8);
        CountDownLatch release = new CountDownLatch(1);
        Map<String, AtomicInteger> runs = new ConcurrentHashMap<>();
        Map<String, NodeTaskHandler> hs = new ConcurrentHashMap<>();
        for (String n : deps.keySet()) {
            runs.put(n, new AtomicInteger());
            hs.put(n, node -> {
                runs.get(node).incrementAndGet();
                allIn.countDown();
                release.await();
            });
        }
        engine.submit(def, hs);
        assertTrue(allIn.await(5, TimeUnit.SECONDS), "independent nodes must run concurrently");
        release.countDown();
        awaitWorkflow("parallel", WorkflowState.SUCCEEDED);
        for (String n : deps.keySet()) {
            assertEquals(1, runs.get(n).get(), n + " executed more than once");
        }
    }
}
