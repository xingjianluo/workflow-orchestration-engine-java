package com.github.xingjianluo.workfloworchestrationenginejava.workflow.model;

import java.io.Serializable;
import java.util.Map;

/** 流程实例：定义 + 全部节点运行时状态 + 乐观锁版本号。 */
public class WorkflowInstance implements Serializable {
    private static final long serialVersionUID = 1L;
    private String instanceId;
    private WorkflowDefinition definition;
    private Map<String, NodeState> states;
    private WorkflowStatus status;
    private long version;
    private String failureReason;
    private long createdAt;
    private long updatedAt;

    public WorkflowInstance() {
    }

    public String getInstanceId() { return instanceId; }
    public void setInstanceId(String instanceId) { this.instanceId = instanceId; }
    public WorkflowDefinition getDefinition() { return definition; }
    public void setDefinition(WorkflowDefinition definition) { this.definition = definition; }
    public Map<String, NodeState> getStates() { return states; }
    public void setStates(Map<String, NodeState> states) { this.states = states; }
    public WorkflowStatus getStatus() { return status; }
    public void setStatus(WorkflowStatus status) { this.status = status; }
    public long getVersion() { return version; }
    public void setVersion(long version) { this.version = version; }
    public String getFailureReason() { return failureReason; }
    public void setFailureReason(String failureReason) { this.failureReason = failureReason; }
    public long getCreatedAt() { return createdAt; }
    public void setCreatedAt(long createdAt) { this.createdAt = createdAt; }
    public long getUpdatedAt() { return updatedAt; }
    public void setUpdatedAt(long updatedAt) { this.updatedAt = updatedAt; }
}
