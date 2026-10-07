package com.github.xingjianluo.workfloworchestrationenginejava.tests;

import com.github.xingjianluo.workfloworchestrationenginejava.handler.HeartbeatCapableNodeTaskHandler;
import com.github.xingjianluo.workfloworchestrationenginejava.handler.NodeTaskHandler;
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
 * 长耗时节点与执行时间预算：
 * 1) 心跳续约：节点耗时远超租约时长，只要持续心跳就只被真正执行一次并正常成功；
 * 2) 单次执行超时上限：流程级默认 + 节点级覆盖，超时按失败/重试规则落状态；
 * 3) 超时被判失效后由后续尝试接手，旧执行迟到的结果不得改回已发生的状态；
 * 4) 长任务不阻塞同进程内其它流程的推进；
 * 5) 运行中查询可见租约属主与剩余时间预算。
 */
class LongRunningAndTimeoutTest extends AbstractEngineTest {

    private static final Logger log = LoggerFactory.getLogger(LongRunningAndTimeoutTest.class);

    @TempDir
    Path tmp;

    @AfterEach
    void tearDown() {
        teardownEngine();
    }

    private static void scenario(String name) {
        log.info("==================== 场景: {} ====================", name);
    }

    @Test
    void longRunningNodeWithHeartbeatExecutesOnceAndSucceeds() throws Exception {
        scenario("节点耗时远超租约时长，持续心跳 => 只执行一次并正常成功");
        // 租约 150ms，节点实际跑 ~1.2s（约 8 倍租约），全程心跳续约
        setupEngine(tmp, 150L, 2);
        WorkflowDefinition def = new WorkflowDefinition("long-hb", 1, 0L,
                Map.of("a", List.of(), "b", List.of("a")));
        AtomicInteger bodies = new AtomicInteger();
        AtomicInteger heartbeats = new AtomicInteger();
        AtomicInteger leaseLost = new AtomicInteger();

        HeartbeatCapableNodeTaskHandler longTask = (nodeId, ctx) -> {
            bodies.incrementAndGet();
            long end = System.currentTimeMillis() + 1200;
            while (System.currentTimeMillis() < end) {
                Thread.sleep(50);
                heartbeats.incrementAndGet();
                if (!ctx.heartbeat()) {
                    leaseLost.incrementAndGet();
                    return;
                }
            }
        };
        Map<String, NodeTaskHandler> hs = new LinkedHashMap<>();
        hs.put("a", longTask);
        hs.put("b", (NodeTaskHandler) node -> { });
        engine.submit(def, hs);

        awaitWorkflow("long-hb", WorkflowState.SUCCEEDED);
        WorkflowInstance inst = snapshot("long-hb");
        assertEquals(1, bodies.get(), "长任务业务体必须只被真正执行一次");
        assertEquals(1, inst.getNodes().get("a").getAttempts(),
                "attempts 必须与实际执行次数一致，不得因接管虚高");
        assertEquals(NodeState.SUCCEEDED, inst.getNodes().get("a").getState());
        assertEquals(NodeState.SUCCEEDED, inst.getNodes().get("b").getState());
        assertEquals(0, leaseLost.get(), "正常心跳期间不应丢失租约");
        assertTrue(heartbeats.get() > 5, "长任务期间应发生多次心跳续约");
        log.info("场景校验通过: bodies={}, attempts={}, heartbeats={}",
                bodies.get(), inst.getNodes().get("a").getAttempts(), heartbeats.get());
    }

    @Test
    void nodeExceedingOwnTimeoutFailsByRulesThenRetrySucceeds() throws Exception {
        scenario("单次执行超过节点自身上限 => 按失败/重试规则落 RETRY_WAIT，再次尝试成功");
        // 节点级上限 300ms；第一次执行阻塞（被超时判负），第二次立即成功
        setupEngine(tmp, 5_000L, 2);
        WorkflowDefinition def = new WorkflowDefinition("timeout-retry", 2, 50L,
                Map.of("a", List.of()), null, Map.of("a", 300L));
        AtomicInteger bodies = new AtomicInteger();
        CountDownLatch firstEntered = new CountDownLatch(1);
        engine.submit(def, Map.of("a", (NodeTaskHandler) node -> {
            int which = bodies.incrementAndGet();
            if (which == 1) {
                firstEntered.countDown();
                // 模拟拉不动的长任务：不响应中断，直到被超时判定后自行退出
                long deadline = System.currentTimeMillis() + 2000;
                while (System.currentTimeMillis() < deadline) {
                    try {
                        Thread.sleep(30);
                    } catch (InterruptedException ignored) {
                        // 引擎会尝试中断；本任务模拟“拉不动”，忽略之
                    }
                }
                return;
            }
            // 第二次尝试：立即成功
        }));

        assertTrue(firstEntered.await(5, TimeUnit.SECONDS));
        // 第一次尝试应被判定超时并进入 RETRY_WAIT（预算未耗尽）
        awaitUntil(() -> {
            NodeExecution a = snapshot("timeout-retry").getNodes().get("a");
            return a.getState() == NodeState.RETRY_WAIT
                    && a.getLastError() != null && a.getLastError().contains("timeout");
        });
        log.info("第一次尝试已按超时规则落入 RETRY_WAIT: lastError={}",
                snapshot("timeout-retry").getNodes().get("a").getLastError());

        awaitWorkflow("timeout-retry", WorkflowState.SUCCEEDED);
        WorkflowInstance inst = snapshot("timeout-retry");
        assertEquals(2, bodies.get(), "超时失败后应发生第二次真实执行");
        assertEquals(2, inst.getNodes().get("a").getAttempts());
        assertEquals(NodeState.SUCCEEDED, inst.getNodes().get("a").getState());
        log.info("场景校验通过: bodies={}, attempts={}", bodies.get(),
                inst.getNodes().get("a").getAttempts());
    }

    @Test
    void nodeTimeoutWithExhaustedBudgetBecomesFailedAndManualRetryRevives() throws Exception {
        scenario("超时且重试预算耗尽 => FAILED，人工重试后恢复并成功");
        setupEngine(tmp, 5_000L, 2);
        WorkflowDefinition def = new WorkflowDefinition("timeout-failed", 1, 0L,
                Map.of("a", List.of()), null, Map.of("a", 250L));
        AtomicInteger bodies = new AtomicInteger();
        AtomicBoolean blockFirst = new AtomicBoolean(true);
        engine.submit(def, Map.of("a", (NodeTaskHandler) node -> {
            bodies.incrementAndGet();
            if (blockFirst.get()) {
                long deadline = System.currentTimeMillis() + 2000;
                while (System.currentTimeMillis() < deadline) {
                    try {
                        Thread.sleep(30);
                    } catch (InterruptedException ignored) {
                    }
                }
            }
        }));

        awaitNode("timeout-failed", "a", NodeState.FAILED);
        awaitWorkflow("timeout-failed", WorkflowState.FAILED);
        WorkflowInstance failed = snapshot("timeout-failed");
        assertEquals(1, failed.getNodes().get("a").getAttempts());
        assertTrue(failed.getNodes().get("a").getLastError().contains("timeout"));
        log.info("预算耗尽后节点 FAILED: lastError={}", failed.getNodes().get("a").getLastError());

        // 人工重试：放行业务体，重置预算后应成功
        blockFirst.set(false);
        long version = failed.getVersion();
        engine.retryNode("timeout-failed", "a", version);
        awaitWorkflow("timeout-failed", WorkflowState.SUCCEEDED);
        WorkflowInstance done = snapshot("timeout-failed");
        assertEquals(1, done.getNodes().get("a").getAttempts(), "人工重试重置 attempts 后只计新预算");
        assertEquals(2, bodies.get());
        log.info("场景校验通过: 人工重试后 SUCCEEDED, bodies={}", bodies.get());
    }

    @Test
    void lateResultAfterTimeoutTakeoverDoesNotOverwriteState() throws Exception {
        scenario("超时被判失效、后续尝试已成功后，旧执行迟到的结果不得改回状态");
        setupEngine(tmp, 5_000L, 2);
        WorkflowDefinition def = new WorkflowDefinition("timeout-late", 3, 30L,
                Map.of("a", List.of()), null, Map.of("a", 200L));
        AtomicInteger bodies = new AtomicInteger();
        AtomicInteger committedSuccesses = new AtomicInteger();
        CountDownLatch firstEntered = new CountDownLatch(1);
        AtomicBoolean releaseFirst = new AtomicBoolean(false);

        engine.submit(def, Map.of("a", (NodeTaskHandler) node -> {
            int which = bodies.incrementAndGet();
            if (which == 1) {
                firstEntered.countDown();
                // 旧执行“拉不动”：等测试放行后才返回（模拟无法被中断的批处理）
                while (!releaseFirst.get()) {
                    try {
                        Thread.sleep(20);
                    } catch (InterruptedException ignored) {
                    }
                }
                return; // 迟到返回成功，必须被 CAS 拒绝
            }
            committedSuccesses.incrementAndGet();
        }));

        assertTrue(firstEntered.await(5, TimeUnit.SECONDS));
        // 等超时判定 + 后续尝试成功
        awaitNode("timeout-late", "a", NodeState.SUCCEEDED);
        WorkflowInstance mid = snapshot("timeout-late");
        assertEquals(2, mid.getNodes().get("a").getAttempts(),
                "超时尝试 + 接手尝试各计一次");
        assertEquals(1, committedSuccesses.get());

        // 放行旧执行：其迟到结果必须被拒绝，状态与尝试次数都不得变化
        releaseFirst.set(true);
        Thread.sleep(400);
        WorkflowInstance after = snapshot("timeout-late");
        assertEquals(NodeState.SUCCEEDED, after.getNodes().get("a").getState());
        assertEquals(WorkflowState.SUCCEEDED, after.getState());
        assertEquals(2, after.getNodes().get("a").getAttempts(), "迟到结果不得改变尝试次数");
        assertEquals(1, committedSuccesses.get(), "成功结果只能被提交一次");
        log.info("场景校验通过: 迟到结果被拒绝, attempts={}, successes={}",
                after.getNodes().get("a").getAttempts(), committedSuccesses.get());
    }

    @Test
    void workflowLevelTimeoutAppliesAndNodeOverrideWins() throws Exception {
        scenario("流程级默认上限生效，节点级覆盖优先");
        setupEngine(tmp, 5_000L, 3);
        // 流程级默认 250ms；a 用默认（会超时失败），b 覆盖为 3000ms（慢跑成功）
        WorkflowDefinition def = new WorkflowDefinition("timeout-override", 1, 0L,
                Map.of("a", List.of(), "b", List.of()),
                250L, Map.of("b", 3000L));
        CountDownLatch bDone = new CountDownLatch(1);
        engine.submit(def, Map.of(
                "a", (NodeTaskHandler) node -> {
                    long deadline = System.currentTimeMillis() + 1500;
                    while (System.currentTimeMillis() < deadline) {
                        try {
                            Thread.sleep(30);
                        } catch (InterruptedException ignored) {
                        }
                    }
                },
                "b", (NodeTaskHandler) node -> {
                    try {
                        Thread.sleep(600); // 超过流程级默认 250ms，但低于节点级 3000ms
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    bDone.countDown();
                }));

        awaitNode("timeout-override", "a", NodeState.FAILED);
        assertTrue(bDone.await(5, TimeUnit.SECONDS), "节点级覆盖的 b 不应被流程级默认上限误杀");
        awaitNode("timeout-override", "b", NodeState.SUCCEEDED);
        WorkflowInstance inst = snapshot("timeout-override");
        assertEquals(NodeState.FAILED, inst.getNodes().get("a").getState());
        assertTrue(inst.getNodes().get("a").getLastError().contains("timeout"));
        assertEquals(NodeState.SUCCEEDED, inst.getNodes().get("b").getState());
        assertEquals(1, inst.getNodes().get("b").getAttempts());
        log.info("场景校验通过: a=FAILED(默认上限250ms), b=SUCCEEDED(节点级3000ms)");
    }

    @Test
    void longTaskDoesNotBlockOtherWorkflows() throws Exception {
        scenario("长耗时节点不阻塞同进程内其它流程的推进");
        setupEngine(tmp, 150L, 2);
        WorkflowDefinition longDef = new WorkflowDefinition("wf-long", 1, 0L,
                Map.of("slow", List.of()));
        HeartbeatCapableNodeTaskHandler slow = (nodeId, ctx) -> {
            long end = System.currentTimeMillis() + 1000;
            while (System.currentTimeMillis() < end) {
                Thread.sleep(50);
                ctx.heartbeat();
            }
        };
        engine.submit(longDef, Map.of("slow", slow));

        WorkflowDefinition shortDef = new WorkflowDefinition("wf-short", 1, 0L,
                Map.of("quick", List.of()));
        engine.submit(shortDef, Map.of("quick", (NodeTaskHandler) node -> { }));

        // 短流程必须在长流程结束前就完成（长任务只占 1 个 worker）
        awaitWorkflow("wf-short", WorkflowState.SUCCEEDED);
        assertEquals(WorkflowState.ACTIVE, snapshot("wf-long").getState(),
                "短流程完成时长流程应仍在执行");
        awaitWorkflow("wf-long", WorkflowState.SUCCEEDED);
        log.info("场景校验通过: wf-short 在 wf-long 运行期间完成，互不被顶死");
    }

    @Test
    void runningNodeExposesOwnerAndRemainingBudget() throws Exception {
        scenario("运行中查询可见：租约属主、生效上限、剩余时间预算");
        setupEngine(tmp, 150L, 2);
        WorkflowDefinition def = new WorkflowDefinition("wf-observe", 1, 0L,
                Map.of("a", List.of()), 2000L, null);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        HeartbeatCapableNodeTaskHandler task = (nodeId, ctx) -> {
            entered.countDown();
            while (release.getCount() > 0) {
                Thread.sleep(40);
                ctx.heartbeat();
            }
        };
        engine.submit(def, Map.of("a", task));
        assertTrue(entered.await(5, TimeUnit.SECONDS));
        awaitNode("wf-observe", "a", NodeState.RUNNING);

        NodeExecution a = snapshot("wf-observe").getNodes().get("a");
        assertNotNull(a.getLeaseOwner(), "运行中必须可见租约属主");
        assertTrue(a.getExecutionStartedAt() > 0, "必须可见本次尝试开始时间");
        assertEquals(2000L, a.getExecutionTimeoutMillis(), "必须可见生效的超时上限");
        assertTrue(a.getExecutionDeadlineAt() > 0, "必须可见超时截止时间");
        long remaining = a.getExecutionDeadlineAt() - clock.nowMillis();
        assertTrue(remaining > 0 && remaining <= 2000L,
                "剩余预算应在 (0, 2000] 区间, actual=" + remaining);
        log.info("运行中观测: owner={}, startedAt={}, timeout={}ms, deadline={}, remaining={}ms",
                a.getLeaseOwner(), a.getExecutionStartedAt(), a.getExecutionTimeoutMillis(),
                a.getExecutionDeadlineAt(), remaining);

        release.countDown();
        awaitWorkflow("wf-observe", WorkflowState.SUCCEEDED);
        NodeExecution done = snapshot("wf-observe").getNodes().get("a");
        assertEquals(0L, done.getExecutionDeadlineAt(), "结束后预算字段应清空");
        log.info("场景校验通过: 运行中可观测属主与预算，结束后字段清空");
    }
}
