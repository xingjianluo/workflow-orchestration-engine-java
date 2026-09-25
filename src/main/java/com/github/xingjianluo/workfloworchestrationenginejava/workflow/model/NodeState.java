package com.github.xingjianluo.workfloworchestrationenginejava.workflow.model;

import java.io.Serializable;

/** 节点的运行时状态（含租约信息与重试信息），随实例一起持久化。 */
public class NodeState implements Serializable {
    private static final long serialVersionUID = 1L;
    private String nodeId;
    private NodeStatus status;
    private int attempts;
    private String lastError;
    private long nextRetryAt;
    private String leaseOwner;
    private String leaseToken;
    private long leaseExpiresAt;
    private long updatedAt;

    public NodeState() {
    }

    public String getNodeId() { return nodeId; }
    public void setNodeId(String nodeId) { this.nodeId = nodeId; }
    public NodeStatus getStatus() { return status; }
    public void setStatus(NodeStatus status) { this.status = status; }
    public int getAttempts() { return attempts; }
    public void setAttempts(int attempts) { this.attempts = attempts; }
    public String getLastError() { return lastError; }
    public void setLastError(String lastError) { this.lastError = lastError; }
    public long getNextRetryAt() { return nextRetryAt; }
    public void setNextRetryAt(long nextRetryAt) { this.nextRetryAt = nextRetryAt; }
    public String getLeaseOwner() { return leaseOwner; }
    public void setLeaseOwner(String leaseOwner) { this.leaseOwner = leaseOwner; }
    public String getLeaseToken() { return leaseToken; }
    public void setLeaseToken(String leaseToken) { this.leaseToken = leaseToken; }
    public long getLeaseExpiresAt() { return leaseExpiresAt; }
    public void setLeaseExpiresAt(long leaseExpiresAt) { this.leaseExpiresAt = leaseExpiresAt; }
    public long getUpdatedAt() { return updatedAt; }
    public void setUpdatedAt(long updatedAt) { this.updatedAt = updatedAt; }
}
