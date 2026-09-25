package com.github.xingjianluo.workfloworchestrationenginejava.model;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 流程实例（可变实体，持久化到 store）。
 */
public class WorkflowInstance {
    private String requestId;
    private int maxAttempts;
    private long backoffMillis;
    private WorkflowState state = WorkflowState.ACTIVE;
    /** nodeId -> 上游 nodeId 列表（依赖不可变） */
    private Map<String, java.util.List<String>> dependencies = new LinkedHashMap<>();
    /** nodeId -> 运行时状态 */
    private Map<String, NodeExecution> nodes = new LinkedHashMap<>();
    private long createdAt;
    private long updatedAt;
    /** 干预版本号：每次 pause/resume/cancel/retry 递增，过期干预携带旧版本将被拒绝 */
    private long version;

    public String getRequestId() { return requestId; }
    public void setRequestId(String requestId) { this.requestId = requestId; }
    public int getMaxAttempts() { return maxAttempts; }
    public void setMaxAttempts(int maxAttempts) { this.maxAttempts = maxAttempts; }
    public long getBackoffMillis() { return backoffMillis; }
    public void setBackoffMillis(long backoffMillis) { this.backoffMillis = backoffMillis; }
    public WorkflowState getState() { return state; }
    public void setState(WorkflowState state) { this.state = state; }
    public Map<String, java.util.List<String>> getDependencies() { return dependencies; }
    public void setDependencies(Map<String, java.util.List<String>> dependencies) { this.dependencies = dependencies; }
    public Map<String, NodeExecution> getNodes() { return nodes; }
    public void setNodes(Map<String, NodeExecution> nodes) { this.nodes = nodes; }
    public long getCreatedAt() { return createdAt; }
    public void setCreatedAt(long createdAt) { this.createdAt = createdAt; }
    public long getUpdatedAt() { return updatedAt; }
    public void setUpdatedAt(long updatedAt) { this.updatedAt = updatedAt; }
    public long getVersion() { return version; }
    public void setVersion(long version) { this.version = version; }
}
