package com.github.xingjianluo.workfloworchestrationenginejava.tests;

import com.github.xingjianluo.workfloworchestrationenginejava.tests.support.MutableClock;
import com.github.xingjianluo.workfloworchestrationenginejava.tests.support.WorkflowHarness;
import com.github.xingjianluo.workfloworchestrationenginejava.workflow.error.DefinitionValidationException;
import com.github.xingjianluo.workfloworchestrationenginejava.workflow.error.ErrorCode;
import com.github.xingjianluo.workfloworchestrationenginejava.workflow.model.NodeDefinition;
import com.github.xingjianluo.workfloworchestrationenginejava.workflow.model.WorkflowDefinition;
import com.github.xingjianluo.workfloworchestrationenginejava.workflow.model.WorkflowInstance;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.github.xingjianluo.workfloworchestrationenginejava.tests.support.WorkflowHarness.definition;
import static com.github.xingjianluo.workfloworchestrationenginejava.tests.support.WorkflowHarness.node;
import static org.junit.jupiter.api.Assertions.*;

/** 定义提交与依赖校验：重复节点、缺失依赖、环各自给出可区分错误码；非法定义不产生实例。 */
class DefinitionValidationTest {

    @TempDir
    Path tempDir;
    private WorkflowHarness harness;

    @BeforeEach
    void setUp() {
        harness = new WorkflowHarness(tempDir, new MutableClock());
    }

    @Test
    void rejectsDuplicateNode() {
        // 手工构造两个 id 相同的节点（绕过 map 去重，模拟来源数据重复）
        WorkflowDefinition def = new WorkflowDefinition();
        Map<String, NodeDefinition> nodes = new LinkedHashMap<>();
        nodes.put("a", node("a", "noop"));
        NodeDefinition dup = node("a", "noop");
        // LinkedHashMap 不允许重复 key，改用“node.id 与 key 冲突”的等价重复场景
        nodes.put("b", dup);
        def.setNodes(nodes);

        DefinitionValidationException ex = assertThrows(DefinitionValidationException.class,
                () -> harness.engine.submit("key-dup", def));
        assertEquals(ErrorCode.DUPLICATE_NODE, ex.getErrorCode());
        assertNull(harness.engine.findByKey("key-dup").orElse(null),
                "非法定义不得产生任何实例（不部分生效）");
    }

    @Test
    void rejectsMissingDependency() {
        WorkflowDefinition def = definition("wf",
                node("a", "noop", "missing-node"));
        DefinitionValidationException ex = assertThrows(DefinitionValidationException.class,
                () -> harness.engine.submit("key-missing", def));
        assertEquals(ErrorCode.MISSING_DEPENDENCY, ex.getErrorCode());
        assertTrue(ex.getMessage().contains("missing-node"));
    }

    @Test
    void rejectsSelfDependencyAndCycle() {
        // 自依赖即环
        DefinitionValidationException self = assertThrows(DefinitionValidationException.class,
                () -> harness.engine.submit("key-self",
                        definition("wf", node("a", "noop", "a"))));
        assertEquals(ErrorCode.CYCLIC_DEPENDENCY, self.getErrorCode());

        // a -> b -> c -> a
        DefinitionValidationException cycle = assertThrows(DefinitionValidationException.class,
                () -> harness.engine.submit("key-cycle",
                        definition("wf",
                                node("a", "noop", "c"),
                                node("b", "noop", "a"),
                                node("c", "noop", "b"))));
        assertEquals(ErrorCode.CYCLIC_DEPENDENCY, cycle.getErrorCode());
        assertTrue(cycle.getMessage().contains("检测到依赖环"));
    }

    @Test
    void invalidDefinitionNeverPartiallyTakesEffect() {
        // 环定义中的节点即便"排在前面"也不允许被查询到任何实例
        assertThrows(DefinitionValidationException.class,
                () -> harness.engine.submit("key-invalid",
                        definition("wf",
                                node("good", "noop"),
                                node("bad", "noop", "ghost"))));
        assertEquals(0, harness.store.loadAll().size(), "存储中不应有任何实例文件");
        assertNull(harness.engine.findByKey("key-invalid").orElse(null));

        // 同一 key 随后提交合法定义必须正常成功（前次失败不留残迹影响幂等）
        WorkflowInstance ok = harness.engine.submit("key-invalid",
                definition("wf2", node("good", "noop")));
        assertEquals(1, ok.getStates().size());
    }

    @Test
    void acceptsValidDag() {
        WorkflowInstance wf = harness.engine.submit("key-ok",
                definition("wf",
                        node("a", "noop"),
                        node("b", "noop", "a"),
                        node("c", "noop", "a"),
                        node("d", "noop", "b", "c")));
        assertNotNull(wf.getInstanceId());
        harness.dump("合法 DAG 提交后", wf.getInstanceId());
        assertEquals(4, wf.getStates().size());
    }
}
