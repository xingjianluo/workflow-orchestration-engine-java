package com.github.xingjianluo.workfloworchestrationenginejava.engine;

import com.github.xingjianluo.workfloworchestrationenginejava.error.ErrorCode;
import com.github.xingjianluo.workfloworchestrationenginejava.error.WorkflowException;
import com.github.xingjianluo.workfloworchestrationenginejava.model.WorkflowDefinition;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 流程定义静态校验：
 * <ol>
 *     <li>基础参数非法 → {@link ErrorCode#INVALID_DEFINITION}</li>
 *     <li>同一节点重复声明 → {@link ErrorCode#DUPLICATE_NODE}</li>
 *     <li>依赖指向不存在的节点 → {@link ErrorCode#MISSING_DEPENDENCY}</li>
 *     <li>依赖图成环 → {@link ErrorCode#CYCLE_DETECTED}</li>
 * </ol>
 * 全部检查在实例落盘前完成，非法定义不会部分生效。
 */
public class DefinitionValidator {

    public DefinitionValidator() {
    }

    public void validate(WorkflowDefinition definition) {
        if (definition == null) {
            throw new WorkflowException(ErrorCode.INVALID_DEFINITION, "definition is null");
        }
        if (definition.requestId() == null || definition.requestId().isBlank()) {
            throw new WorkflowException(ErrorCode.INVALID_DEFINITION, "requestId must not be blank");
        }
        if (definition.maxAttempts() < 1) {
            throw new WorkflowException(ErrorCode.INVALID_DEFINITION,
                    "maxAttempts must be >= 1 but was " + definition.maxAttempts());
        }
        if (definition.backoffMillis() < 0) {
            throw new WorkflowException(ErrorCode.INVALID_DEFINITION,
                    "backoffMillis must be >= 0 but was " + definition.backoffMillis());
        }
        Map<String, List<String>> deps = definition.dependencies();
        if (deps == null || deps.isEmpty()) {
            throw new WorkflowException(ErrorCode.INVALID_DEFINITION,
                    "dependencies must contain at least one node");
        }

        // 1) 重复节点：key 重复（LinkedHashMap 无法表达）或依赖列表内重复上游
        Set<String> declared = new HashSet<>();
        for (String node : deps.keySet()) {
            if (!declared.add(node)) {
                throw new WorkflowException(ErrorCode.DUPLICATE_NODE,
                        "duplicate node declared: " + node);
            }
        }
        for (Map.Entry<String, List<String>> entry : deps.entrySet()) {
            List<String> upstream = entry.getValue();
            if (upstream == null) {
                continue;
            }
            Set<String> seen = new HashSet<>();
            for (String dep : upstream) {
                if (!seen.add(dep)) {
                    throw new WorkflowException(ErrorCode.DUPLICATE_NODE,
                            "node '" + entry.getKey() + "' declares duplicate dependency: " + dep);
                }
            }
        }

        // 2) 缺失依赖
        for (Map.Entry<String, List<String>> entry : deps.entrySet()) {
            List<String> upstream = entry.getValue();
            if (upstream == null) {
                continue;
            }
            for (String dep : upstream) {
                if (!declared.contains(dep)) {
                    throw new WorkflowException(ErrorCode.MISSING_DEPENDENCY,
                            "node '" + entry.getKey() + "' depends on missing node: " + dep);
                }
            }
        }

        // 3) 环检测：三色 DFS（ITERATIVE 白/灰/黑）
        detectCycle(deps);
    }

    private void detectCycle(Map<String, List<String>> deps) {
        // 0=white 1=gray 2=black
        var color = new java.util.HashMap<String, Integer>();
        for (String node : deps.keySet()) {
            color.put(node, 0);
        }
        // 迭代式 DFS：栈帧保存节点与其待访问下游的游标
        for (String root : new ArrayList<>(deps.keySet())) {
            if (color.get(root) != 0) {
                continue;
            }
            Deque<Frame> stack = new ArrayDeque<>();
            stack.push(new Frame(root, 0));
            color.put(root, 1);
            while (!stack.isEmpty()) {
                Frame frame = stack.peek();
                List<String> upstream = deps.getOrDefault(frame.node, List.of());
                if (frame.cursor >= upstream.size()) {
                    color.put(frame.node, 2);
                    stack.pop();
                    continue;
                }
                String next = upstream.get(frame.cursor++);
                int c = color.get(next);
                if (c == 1) {
                    List<String> chain = new ArrayList<>();
                    for (Frame f : stack) {
                        chain.add(0, f.node);
                    }
                    chain.add(next);
                    throw new WorkflowException(ErrorCode.CYCLE_DETECTED,
                            "dependency cycle detected: " + String.join(" -> ", chain));
                }
                if (c == 0) {
                    color.put(next, 1);
                    stack.push(new Frame(next, 0));
                }
            }
        }
    }

    private static final class Frame {
        final String node;
        int cursor;

        Frame(String node, int cursor) {
            this.node = node;
            this.cursor = cursor;
        }
    }
}
