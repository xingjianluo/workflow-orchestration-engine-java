package com.github.xingjianluo.workfloworchestrationenginejava.tests;

import com.github.xingjianluo.workfloworchestrationenginejava.model.NodeExecution;
import com.github.xingjianluo.workfloworchestrationenginejava.model.NodeState;
import com.github.xingjianluo.workfloworchestrationenginejava.model.WorkflowDefinition;
import com.github.xingjianluo.workfloworchestrationenginejava.model.WorkflowInstance;
import com.github.xingjianluo.workfloworchestrationenginejava.model.WorkflowState;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 执行时长预算与长耗时节点：
 * 1) 节点耗时远超租约 TTL 时，续约保证只被真正执行一次并正常成功；
 * 2) 超过自身预算的执行按既有失败/重试规则落态，耗尽后 FAILED，可人工重试；
 * 3) 超时失效被接手后，旧执行方迟到的结果不得改动已发生的状态；
 * 4) 流程级默认预算与节点级覆盖的优先级；
 * 5) 长任务不把执行资源占死，其它流程的节点照常推进；
 * 6) 运行中的节点可查询属主与剩余时间预算。
 */
class ExecutionBudgetTest extends AbstractEngineTest {

    private static final Logger log = LoggerFactory.getLogger(ExecutionBudgetTest.class);

    @TempDir
    Path tmp;

    @AfterEach
    void tearDown() {
        teardownEngine();
    }

    @Test
    void longRunningNodeBeyondLeaseTtlRunsExactlyOnce() throws Exception {
        log.info("=== 场景1：节点耗时(约1200ms)远超租约TTL(200ms)，未配置预算 -> "
                + "租约自动续约，节点只被真正执行一次并正常成功 ===");
        setupEngine(tmp, 200L, 4);
        WorkflowDefinition def = definition("long-run", 3, 0L,
                Map.of("a", List.of(), "b", List.of("a")));
        AtomicInteger aBodies = new AtomicInteger();
        AtomicInteger bBodies = new AtomicInteger();
        engine.submit(def, Map.of(
                "a", node -> {
                    aBodies.incrementAndGet();
                    log.info("[long-run] 业务节点 a 开始长时间执行 (第 {} 次进入)", aBodies.get());
                    Thread.sleep(1200L); // 远超 leaseTtl=200ms：不续约就会被误判接管
                },
                "b", node -> bBodies.incrementAndGet()));

        awaitWorkflow("long-run", WorkflowState.SUCCEEDED);
        WorkflowInstance inst = snapshot("long-run");
        assertEquals(1, aBodies.get(), "长耗时节点只能被真正执行一次");
        assertEquals(1, inst.getNodes().get("a").getAttempts(),
                "未被误判接管，尝试次数不得虚高");
        assertEquals(1, bBodies.get());
        assertEquals(1, inst.getNodes().get("b").getAttempts());
        log.info("=== 场景1通过：a attempts={}, bodies={} ===",
                inst.getNodes().get("a").getAttempts(), aBodies.get());
    }

    @Test
    void nodeExceedingOwnBudgetFailsByRuleThenManualRetrySucceeds() throws Exception {
        log.info("=== 场景2：节点级预算300ms，前两次尝试都超时 -> 按规则 RETRY_WAIT 后 FAILED；"
                + "人工重试获得新预算，第三次快速成功 ===");
        setupEngine(tmp, 5_000L, 4);
        WorkflowDefinition def = new WorkflowDefinition("budget-fail", 2, 50L,
                Map.of("a", List.of()), null, Map.of("a", 300L));
        AtomicInteger bodies = new AtomicInteger();
        engine.submit(def, Map.of("a", node -> {
            int which = bodies.incrementAndGet();
            if (which <= 2) {
                Thread.sleep(2_000L); // 超过 300ms 预算；被超时中断后回交会被 CAS 拒绝
            }
            // 第三次（人工重试后）立即成功
        }));

        // 两次尝试都超时 -> 预算耗尽 -> FAILED，流程终态 FAILED
        awaitNode("budget-fail", "a", NodeState.FAILED);
        awaitWorkflow("budget-fail", WorkflowState.FAILED);
        WorkflowInstance failed = snapshot("budget-fail");
        assertEquals(2, failed.getNodes().get("a").getAttempts(), "两次超时尝试都要计入");
        assertTrue(failed.getNodes().get("a").getLastError().contains("execution timeout"),
                "失败原因必须可解释为超时: " + failed.getNodes().get("a").getLastError());
        log.info("=== 场景2：节点按规则 FAILED, attempts=2, error={} ===",
                failed.getNodes().get("a").getLastError());

        // 人工重试：预算重置，第三次执行快速成功
        engine.retryNode("budget-fail", "a", failed.getVersion());
        awaitWorkflow("budget-fail", WorkflowState.SUCCEEDED);
        WorkflowInstance done = snapshot("budget-fail");
        assertEquals(NodeState.SUCCEEDED, done.getNodes().get("a").getState());
        assertEquals(1, done.getNodes().get("a").getAttempts(), "人工重试后尝试次数从 1 重新计");
        assertEquals(3, bodies.get(), "业务共进入三次：两次超时 + 一次成功");
        log.info("=== 场景2通过：人工重试后 SUCCEEDED, attempts=1 ===");
    }

    @Test
    void staleResultAfterTimeoutTakeoverDoesNotMutateState() throws Exception {
        log.info("=== 场景3：第一次尝试超时失效（执行方拉不动），由新尝试接手成功；"
                + "旧执行方迟到的结果被 CAS 拒绝，不得改动已发生的状态 ===");
        setupEngine(tmp, 5_000L, 4);
        WorkflowDefinition def = new WorkflowDefinition("timeout-stale", 3, 0L,
                Map.of("a", List.of()), null, Map.of("a", 300L));

        CountDownLatch firstEntered = new CountDownLatch(1);
        CountDownLatch firstReturned = new CountDownLatch(1);
        AtomicBoolean firstMayReturn = new AtomicBoolean(false);
        AtomicInteger bodies = new AtomicInteger();
        AtomicInteger successes = new AtomicInteger();

        engine.submit(def, Map.of("a", node -> {
            int which = bodies.incrementAndGet();
            if (which == 1) {
                firstEntered.countDown();
                // 模拟“拉不动”的执行方：被超时中断后仍不退出，直到测试放行
                while (!firstMayReturn.get()) {
                    try {
                        Thread.sleep(20L);
                    } catch (InterruptedException ignored) {
                        // 忽略中断，继续挂着
                    }
                }
                firstReturned.countDown();
                return; // 迟到地回交“成功”
            }
            successes.incrementAndGet(); // 接手的尝试：立即成功
        }));

        assertTrue(firstEntered.await(5, TimeUnit.SECONDS));
        // 第一次尝试 300ms 后超时 -> RETRY_WAIT -> 第二次尝试接手并成功
        awaitNode("timeout-stale", "a", NodeState.SUCCEEDED);
        awaitWorkflow("timeout-stale", WorkflowState.SUCCEEDED);
        assertEquals(2, snapshot("timeout-stale").getNodes().get("a").getAttempts(),
                "超时一次 + 接手成功一次");

        // 现在放行旧执行方：其迟到结果必须被拒绝，状态不得回退
        firstMayReturn.set(true);
        assertTrue(firstReturned.await(5, TimeUnit.SECONDS));
        Thread.sleep(300L); // 留给迟到回交与 CAS 判定的时间
        WorkflowInstance finalInst = snapshot("timeout-stale");
        assertEquals(NodeState.SUCCEEDED, finalInst.getNodes().get("a").getState(),
                "迟到结果不得把 SUCCEEDED 改回去");
        assertEquals(WorkflowState.SUCCEEDED, finalInst.getState());
        assertEquals(2, finalInst.getNodes().get("a").getAttempts(), "迟到结果不得虚增尝试次数");
        assertEquals(1, successes.get(), "只有接手的尝试真正成功");
        log.info("=== 场景3通过：迟到结果被拒绝, state=SUCCEEDED, attempts=2 ===");
    }

    @Test
    void workflowDefaultBudgetAppliesAndNodeOverrideWins() throws Exception {
        log.info("=== 场景4：流程级默认预算300ms；节点 a 覆盖为2000ms 跑500ms 成功，"
                + "节点 b 不覆盖跑500ms 超默认预算失败 ===");
        setupEngine(tmp, 5_000L, 4);
        Map<String, List<String>> deps = new LinkedHashMap<>();
        deps.put("a", List.of());
        deps.put("b", List.of());
        WorkflowDefinition def = new WorkflowDefinition("budget-mix", 1, 0L,
                deps, 300L, Map.of("a", 2_000L));
        engine.submit(def, Map.of(
                "a", node -> Thread.sleep(500L),
                "b", node -> Thread.sleep(500L)));

        awaitNode("budget-mix", "a", NodeState.SUCCEEDED);
        awaitNode("budget-mix", "b", NodeState.FAILED);
        awaitWorkflow("budget-mix", WorkflowState.FAILED);
        WorkflowInstance inst = snapshot("budget-mix");
        NodeExecution a = inst.getNodes().get("a");
        NodeExecution b = inst.getNodes().get("b");
        assertEquals(2_000L, a.getExecutionTimeoutMillis(), "节点级覆盖生效");
        assertEquals(300L, b.getExecutionTimeoutMillis(), "流程级默认生效");
        assertEquals(1, a.getAttempts());
        assertEquals(1, b.getAttempts());
        assertTrue(b.getLastError().contains("execution timeout"));
        log.info("=== 场景4通过：a(预算2000ms) SUCCEEDED, b(预算300ms) FAILED ===");
    }

    @Test
    void longTaskDoesNotBlockOtherWorkflows() throws Exception {
        log.info("=== 场景5：核心线程数=1，流程 long-flow 的节点 a 长时间执行；"
                + "流程 fast-flow 的节点 x 必须照常推进，不被顶住 ===");
        setupEngine(tmp, 300L, 1);
        CountDownLatch aEntered = new CountDownLatch(1);
        CountDownLatch releaseA = new CountDownLatch(1);
        engine.submit(definition("long-flow", 1, 0L, Map.of("a", List.of())),
                Map.of("a", node -> {
                    aEntered.countDown();
                    releaseA.await(10, TimeUnit.SECONDS);
                }));
        assertTrue(aEntered.await(5, TimeUnit.SECONDS), "长任务先占住唯一核心线程");

        engine.submit(definition("fast-flow", 1, 0L, Map.of("x", List.of())),
                Map.of("x", node -> { }));
        // 线程池弹性扩容：x 必须在 a 仍占用核心线程时完成
        awaitNode("fast-flow", "x", NodeState.SUCCEEDED);
        awaitWorkflow("fast-flow", WorkflowState.SUCCEEDED);
        assertEquals(NodeState.RUNNING, nodeState("long-flow", "a"),
                "x 完成时 a 仍在执行，证明 x 没有排队等 a");
        log.info("=== 场景5：fast-flow 在 long-flow 阻塞期间完成 ===");

        releaseA.countDown();
        awaitWorkflow("long-flow", WorkflowState.SUCCEEDED);
        assertEquals(1, snapshot("long-flow").getNodes().get("a").getAttempts());
        log.info("=== 场景5通过：两个流程都成功，长任务只执行一次 ===");
    }

    @Test
    void runningNodeExposesOwnerAndRemainingBudget() throws Exception {
        log.info("=== 场景6：运行中的节点可查询当前属主与剩余时间预算 ===");
        setupEngine(tmp, 5_000L, 4);
        WorkflowDefinition def = new WorkflowDefinition("budget-view", 1, 0L,
                Map.of("a", List.of()), null, Map.of("a", 2_000L));
        CountDownLatch aEntered = new CountDownLatch(1);
        CountDownLatch releaseA = new CountDownLatch(1);
        engine.submit(def, Map.of("a", node -> {
            aEntered.countDown();
            releaseA.await(10, TimeUnit.SECONDS);
        }));
        assertTrue(aEntered.await(5, TimeUnit.SECONDS));
        awaitNode("budget-view", "a", NodeState.RUNNING);

        NodeExecution running = snapshot("budget-view").getNodes().get("a");
        assertNotNull(running.getLeaseOwner(), "运行中必须能看到租约属主");
        assertTrue(running.getExecutionStartedAt() > 0, "必须能看到本次尝试开始时刻");
        assertEquals(running.getExecutionStartedAt() + 2_000L, running.getExecutionDeadlineAt(),
                "截止时刻 = 开始时刻 + 节点预算");
        long remaining = running.getExecutionDeadlineAt() - clock.nowMillis();
        assertTrue(remaining > 0 && remaining <= 2_000L,
                "剩余预算必须在 (0, 2000ms] 区间, actual=" + remaining);
        log.info("=== 场景6：owner={}, 剩余预算={}ms ===", running.getLeaseOwner(), remaining);

        releaseA.countDown();
        awaitWorkflow("budget-view", WorkflowState.SUCCEEDED);
        NodeExecution done = snapshot("budget-view").getNodes().get("a");
        assertEquals(0L, done.getExecutionDeadlineAt(), "完成后预算窗口清零");
        assertEquals(0L, done.getExecutionStartedAt());
        log.info("=== 场景6通过：完成后预算字段已清理 ===");
    }
}
