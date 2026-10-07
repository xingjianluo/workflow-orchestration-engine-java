package com.github.xingjianluo.workfloworchestrationenginejava.model;

import java.util.List;
import java.util.Map;

/**
 * 流程定义：节点及其依赖。
 *
 * @param requestId              业务请求幂等键；相同 requestId 重复提交只产生一个实例
 * @param maxAttempts            每个节点最大自动尝试次数（含首次，&gt;=1）
 * @param backoffMillis          固定退避时长（毫秒）
 * @param dependencies           nodeId -> 其直接上游 nodeId 列表
 * @param executionTimeoutMillis 流程级默认单次执行时长预算（毫秒）；
 *                               null 或 0 表示不限制（与旧版本行为一致）
 * @param nodeTimeouts           nodeId -> 节点级执行时长预算（毫秒），覆盖流程级默认；
 *                               null 表示无节点级覆盖。0 表示该节点不限制
 */
public record WorkflowDefinition(
        String requestId,
        int maxAttempts,
        long backoffMillis,
        Map<String, List<String>> dependencies,
        Long executionTimeoutMillis,
        Map<String, Long> nodeTimeouts
) {
    /** 兼容构造：不配置任何执行时长预算（行为与引入预算前完全一致） */
    public WorkflowDefinition(String requestId, int maxAttempts, long backoffMillis,
                              Map<String, List<String>> dependencies) {
        this(requestId, maxAttempts, backoffMillis, dependencies, null, null);
    }
}
