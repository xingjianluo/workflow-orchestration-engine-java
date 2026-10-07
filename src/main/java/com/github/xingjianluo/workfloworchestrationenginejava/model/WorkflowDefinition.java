package com.github.xingjianluo.workfloworchestrationenginejava.model;

import java.util.List;
import java.util.Map;

/**
 * 流程定义：节点及其依赖。
 *
 * @param requestId                 业务请求幂等键；相同 requestId 重复提交只产生一个实例
 * @param maxAttempts               每个节点最大自动尝试次数（含首次，&gt;=1）
 * @param backoffMillis             固定退避时长（毫秒）
 * @param dependencies              nodeId -> 其直接上游 nodeId 列表
 * @param executionTimeoutMillis    流程级默认单次执行超时上限（毫秒）；
 *                                  null 表示不设置（与历史行为一致：不因超时被判失败）
 * @param nodeExecutionTimeoutMillis 节点级超时覆盖：nodeId -> 上限毫秒数，优先于流程级默认
 */
public record WorkflowDefinition(
        String requestId,
        int maxAttempts,
        long backoffMillis,
        Map<String, List<String>> dependencies,
        Long executionTimeoutMillis,
        Map<String, Long> nodeExecutionTimeoutMillis
) {
    /** 兼容构造器：不配置任何超时上限，行为与引入该能力前完全一致 */
    public WorkflowDefinition(String requestId, int maxAttempts, long backoffMillis,
                              Map<String, List<String>> dependencies) {
        this(requestId, maxAttempts, backoffMillis, dependencies, null, null);
    }
}
