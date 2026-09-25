package com.github.xingjianluo.workfloworchestrationenginejava.model;

/**
 * 单个节点的运行时状态（可变实体，持久化到 store）。
 */
public class NodeExecution {
    private String nodeId;
    private NodeState state = NodeState.PENDING;
    private int attempts;
    private long nextAttemptAt;
    private String lastError;
    private String leaseOwner;
    private long leaseExpiresAt;
    /** true=由上游失败级联取消（可被人工重试唤醒）；false=终态取消 */
    private boolean cascadeCancelled;

    public String getNodeId() { return nodeId; }
    public void setNodeId(String nodeId) { this.nodeId = nodeId; }
    public NodeState getState() { return state; }
    public void setState(NodeState state) { this.state = state; }
    public int getAttempts() { return attempts; }
    public void setAttempts(int attempts) { this.attempts = attempts; }
    public long getNextAttemptAt() { return nextAttemptAt; }
    public void setNextAttemptAt(long nextAttemptAt) { this.nextAttemptAt = nextAttemptAt; }
    public String getLastError() { return lastError; }
    public void setLastError(String lastError) { this.lastError = lastError; }
    public String getLeaseOwner() { return leaseOwner; }
    public void setLeaseOwner(String leaseOwner) { this.leaseOwner = leaseOwner; }
    public long getLeaseExpiresAt() { return leaseExpiresAt; }
    public void setLeaseExpiresAt(long leaseExpiresAt) { this.leaseExpiresAt = leaseExpiresAt; }
    public boolean isCascadeCancelled() { return cascadeCancelled; }
    public void setCascadeCancelled(boolean cascadeCancelled) { this.cascadeCancelled = cascadeCancelled; }
}
