package com.github.xingjianluo.workfloworchestrationenginejava.tests.support;

import com.github.xingjianluo.workfloworchestrationenginejava.workflow.config.WorkflowProperties;
import com.github.xingjianluo.workfloworchestrationenginejava.workflow.engine.DefinitionValidator;
import com.github.xingjianluo.workfloworchestrationenginejava.workflow.engine.WorkflowEngine;
import com.github.xingjianluo.workfloworchestrationenginejava.workflow.exec.TaskHandler;
import com.github.xingjianluo.workfloworchestrationenginejava.workflow.exec.TaskHandlerRegistry;
import com.github.xingjianluo.workfloworchestrationenginejava.workflow.model.NodeDefinition;
import com.github.xingjianluo.workfloworchestrationenginejava.workflow.model.NodeState;
import com.github.xingjianluo.workfloworchestrationenginejava.workflow.model.WorkflowDefinition;
import com.github.xingjianluo.workfloworchestrationenginejava.workflow.model.WorkflowInstance;
import com.github.xingjianluo.workfloworchestrationenginejava.workflow.store.FileWorkflowStore;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** 测试装配：临时目录 + 可控时钟 + 引擎 + 处理器注册表。 */
public class WorkflowHarness {

    private static final Logger log = LoggerFactory.getLogger(WorkflowHarness.class);

    public final MutableClock clock;
    public final WorkflowProperties properties;
    public final FileWorkflowStore store;
    public final TaskHandlerRegistry registry;
    public final WorkflowEngine engine;

    public WorkflowHarness(Path tempDir, MutableClock clock) {
        this.clock = clock;
        this.properties = new WorkflowProperties();
        this.properties.setBaseDir(tempDir);
        this.properties.setLeaseTtlMillis(1_000);
        this.properties.setBaseBackoffMillis(100);
        this.properties.setBackoffMultiplier(2);
        this.properties.setDefaultMaxAttempts(3);
        this.store = new FileWorkflowStore(tempDir, clock);
        this.registry = new TaskHandlerRegistry();
        this.engine = new WorkflowEngine(store, new DefinitionValidator(), clock, properties);
    }

    public void register(String type, TaskHandler handler) {
        registry.register(type, handler);
    }

    // ---- DSL ----

    public static NodeDefinition node(String id, String type, String... deps) {
        NodeDefinition n = new NodeDefinition();
        n.setId(id);
        n.setType(type);
        n.setDependsOn(deps.length == 0 ? List.of() : List.of(deps));
        return n;
    }

    public static NodeDefinition nodeWithAttempts(String id, String type, int maxAttempts, String... deps) {
        NodeDefinition n = node(id, type, deps);
        n.setMaxAttempts(maxAttempts);
        return n;
    }

    public static WorkflowDefinition definition(String name, NodeDefinition... nodes) {
        WorkflowDefinition d = new WorkflowDefinition();
        d.setName(name);
        Map<String, NodeDefinition> map = new LinkedHashMap<>();
        for (NodeDefinition n : nodes) {
            map.put(n.getId(), n);
        }
        d.setNodes(map);
        return d;
    }

    /** 打印流程整体状态、每个节点状态与租约归属、以及关键判定依据。 */
    public void dump(String title, String instanceId) {
        WorkflowInstance wf = engine.get(instanceId);
        log.info("[状态快照] {} | instance={} workflowStatus={} version={} failureReason={}",
                title, instanceId, wf.getStatus(), wf.getVersion(), wf.getFailureReason());
        for (NodeState n : wf.getStates().values()) {
            log.info("    node={:<10} status={:<9} attempts={} leaseOwner={} leaseExpiresIn={}ms nextRetryIn={}ms err={}",
                    n.getNodeId(), n.getStatus(), n.getAttempts(),
                    n.getLeaseOwner(),
                    n.getLeaseExpiresAt() == 0 ? "-" : (n.getLeaseExpiresAt() - clock.nowMillis()),
                    n.getNextRetryAt() == 0 ? "-" : (n.getNextRetryAt() - clock.nowMillis()),
                    n.getLastError());
        }
    }

    public NodeState state(String instanceId, String nodeId) {
        return engine.get(instanceId).getStates().get(nodeId);
    }
}
