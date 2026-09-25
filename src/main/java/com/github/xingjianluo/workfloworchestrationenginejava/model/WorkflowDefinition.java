package com.github.xingjianluo.workfloworchestrationenginejava.model;

import java.util.List;
import java.util.Map;

/**
 * 流程定义：节点及其依赖。
 *
 * @param requestId     业务请求幂等键；相同 requestId 重复提交只产生一个实例
 * @param maxAttempts   每个节点最大自动尝试次数（含首次，&gt;=1）
 * @param backoffMillis 固定退避时长（毫秒）
 * @param dependencies  nodeId -> 其直接上游 nodeId 列表
 */
public record WorkflowDefinition(
        String requestId,
        int maxAttempts,
        long backoffMillis,
        Map<String, List<String>> dependencies
) {
}
