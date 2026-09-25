package com.github.xingjianluo.workfloworchestrationenginejava.workflow.engine;

import com.github.xingjianluo.workfloworchestrationenginejava.workflow.error.DefinitionValidationException;
import com.github.xingjianluo.workfloworchestrationenginejava.workflow.error.ErrorCode;
import com.github.xingjianluo.workfloworchestrationenginejava.workflow.model.NodeDefinition;
import com.github.xingjianluo.workfloworchestrationenginejava.workflow.model.WorkflowDefinition;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 流程定义校验：拒绝重复节点、缺失依赖与环。
 * 纯函数式校验，不产生任何副作用 —— 调用方只有在全部校验通过后才创建实例，
 * 因此非法定义不会部分生效。
 */
public class DefinitionValidator {

    public void validate(WorkflowDefinition definition) {
        if (definition == null) {
            throw new DefinitionValidationException(ErrorCode.MISSING_DEPENDENCY, "流程定义不能为空");
        }
        Map<String, NodeDefinition> nodes = definition.getNodes();
        if (nodes == null || nodes.isEmpty()) {
            throw new DefinitionValidationException(ErrorCode.MISSING_DEPENDENCY, "流程定义必须至少包含一个节点");
        }

        // 1) 重复节点：map key 与 node.id 必须一致，且不允许出现重复 id
        Set<String> seenIds = new HashSet<>();
        for (Map.Entry<String, NodeDefinition> entry : nodes.entrySet()) {
            NodeDefinition node = entry.getValue();
            if (node == null || node.getId() == null || node.getId().isBlank()) {
                throw new DefinitionValidationException(ErrorCode.DUPLICATE_NODE,
                        "节点定义缺失或 id 为空 (key=" + entry.getKey() + ")");
            }
            if (!node.getId().equals(entry.getKey())) {
                throw new DefinitionValidationException(ErrorCode.DUPLICATE_NODE,
                        "节点 id 与定义键不一致: key=" + entry.getKey() + ", id=" + node.getId());
            }
            if (!seenIds.add(node.getId())) {
                throw new DefinitionValidationException(ErrorCode.DUPLICATE_NODE,
                        "存在重复节点: " + node.getId());
            }
        }

        // 2) 缺失依赖：依赖目标必须存在；同一节点重复声明的依赖也按缺失之外的问题收集
        for (NodeDefinition node : nodes.values()) {
            List<String> deps = node.getDependsOn();
            if (deps == null) {
                continue;
            }
            Set<String> dedup = new HashSet<>();
            for (String dep : deps) {
                if (dep == null || dep.isBlank() || !nodes.containsKey(dep)) {
                    throw new DefinitionValidationException(ErrorCode.MISSING_DEPENDENCY,
                            "节点 " + node.getId() + " 依赖了不存在的节点: " + dep);
                }
                if (!dedup.add(dep)) {
                    throw new DefinitionValidationException(ErrorCode.DUPLICATE_NODE,
                            "节点 " + node.getId() + " 重复声明依赖: " + dep);
                }
            }
        }

        // 3) 环检测：DFS 三色标记（自依赖也视为环）
        Set<String> visiting = new HashSet<>();
        Set<String> visited = new HashSet<>();
        Deque<String> path = new ArrayDeque<>();
        for (String id : nodes.keySet()) {
            if (!visited.contains(id)) {
                dfs(id, nodes, visiting, visited, path);
            }
        }
    }

    private void dfs(String id, Map<String, NodeDefinition> nodes,
                     Set<String> visiting, Set<String> visited, Deque<String> path) {
        visiting.add(id);
        path.push(id);
        List<String> deps = nodes.get(id).getDependsOn();
        if (deps != null) {
            for (String dep : new ArrayList<>(deps)) {
                if (visiting.contains(dep)) {
                    throw new DefinitionValidationException(ErrorCode.CYCLIC_DEPENDENCY,
                            "检测到依赖环，环路上的节点: " + describeCycle(dep, path));
                }
                if (!visited.contains(dep)) {
                    dfs(dep, nodes, visiting, visited, path);
                }
            }
        }
        path.pop();
        visiting.remove(id);
        visited.add(id);
    }

    private String describeCycle(String backTo, Deque<String> path) {
        List<String> cycle = new ArrayList<>();
        boolean started = false;
        for (String node : path) {
            if (node.equals(backTo)) {
                started = true;
            }
            if (started) {
                cycle.add(node);
            }
        }
        cycle.add(backTo);
        return String.join(" -> ", cycle);
    }
}
