package com.github.xingjianluo.workfloworchestrationenginejava.tests;

import com.github.xingjianluo.workfloworchestrationenginejava.config.EngineProperties;
import com.github.xingjianluo.workfloworchestrationenginejava.engine.Clock;
import com.github.xingjianluo.workfloworchestrationenginejava.engine.DefinitionValidator;
import com.github.xingjianluo.workfloworchestrationenginejava.engine.RetryPolicy;
import com.github.xingjianluo.workfloworchestrationenginejava.engine.WorkflowEngine;
import com.github.xingjianluo.workfloworchestrationenginejava.handler.NodeTaskHandler;
import com.github.xingjianluo.workfloworchestrationenginejava.model.NodeState;
import com.github.xingjianluo.workfloworchestrationenginejava.model.WorkflowDefinition;
import com.github.xingjianluo.workfloworchestrationenginejava.model.WorkflowInstance;
import com.github.xingjianluo.workfloworchestrationenginejava.model.WorkflowState;
import com.github.xingjianluo.workfloworchestrationenginejava.store.JsonFileWorkflowStore;

import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BooleanSupplier;

import static org.awaitility.Awaitility.await;

/**
 * 测试基类：临时目录 + 可控时钟 + 短租约/短扫描间隔，引擎生命周期由子类管理。
 */
abstract class AbstractEngineTest {

    protected MutableClock clock = new MutableClock();
    protected EngineProperties props;
    protected JsonFileWorkflowStore store;
    protected WorkflowEngine engine;

    protected void setupEngine(Path tmpDir, long leaseTtl, int workers) {
        props = new EngineProperties();
        props.setStoreDir(tmpDir.toString());
        props.setLeaseTtlMillis(leaseTtl);
        props.setScanIntervalMillis(20);
        props.setWorkerThreads(workers);
        props.setRecoverOnStartup(false); // 恢复场景的测试自行控制
        store = new JsonFileWorkflowStore(props);
        store.init();
        engine = new WorkflowEngine(store, props, clock, new RetryPolicy(), new DefinitionValidator());
        engine.start();
    }

    protected void teardownEngine() {
        if (engine != null) {
            engine.stop();
        }
    }

    protected WorkflowDefinition definition(String requestId, int maxAttempts, long backoff,
                                            Map<String, List<String>> deps) {
        return new WorkflowDefinition(requestId, maxAttempts, backoff, deps);
    }

    protected Map<String, NodeTaskHandler> handlers(NodeTaskHandler all) {
        return Map.of("__all__", all);
    }

    /** 为定义中的每个节点绑定同一处理器（submit 会过滤不存在的 key） */
    protected Map<String, NodeTaskHandler> sameHandler(WorkflowDefinition def, NodeTaskHandler handler) {
        Map<String, NodeTaskHandler> map = new LinkedHashMap<>();
        for (String node : def.dependencies().keySet()) {
            map.put(node, handler);
        }
        return map;
    }

    protected WorkflowInstance snapshot(String id) {
        return engine.get(id).orElseThrow();
    }

    protected NodeState nodeState(String id, String node) {
        return snapshot(id).getNodes().get(node).getState();
    }

    protected void awaitWorkflow(String id, WorkflowState state) {
        await().atMost(Duration.ofSeconds(10)).pollInterval(Duration.ofMillis(20))
                .until(() -> snapshot(id).getState() == state);
    }

    protected void awaitNode(String id, String node, NodeState state) {
        await().atMost(Duration.ofSeconds(10)).pollInterval(Duration.ofMillis(20))
                .until(() -> nodeState(id, node) == state);
    }

    protected void awaitUntil(BooleanSupplier condition) {
        await().atMost(Duration.ofSeconds(10)).pollInterval(Duration.ofMillis(20))
                .until(condition::getAsBoolean);
    }

    /** 基于真实时间、可在真实时间之上叠加偏移的时钟（推进时线程真实休眠，调度线程能观察到） */
    static class MutableClock implements Clock {
        volatile long offset = 0L;

        @Override
        public long nowMillis() {
            return System.currentTimeMillis() + offset;
        }

        /** 向前拨动时间并真实等待，保证调度线程也能看到时间流逝 */
        void advance(long delta) {
            offset += delta;
            try {
                Thread.sleep(delta + 30);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
