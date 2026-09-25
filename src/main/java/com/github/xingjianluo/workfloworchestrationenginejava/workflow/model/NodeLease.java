package com.github.xingjianluo.workfloworchestrationenginejava.workflow.model;

/** 成功抢占到的节点租约。 */
public class NodeLease {
    private final String instanceId;
    private final String nodeId;
    private final String token;
    private final int attempt;

    public NodeLease(String instanceId, String nodeId, String token, int attempt) {
        this.instanceId = instanceId;
        this.nodeId = nodeId;
        this.token = token;
        this.attempt = attempt;
    }

    public String getInstanceId() { return instanceId; }
    public String getNodeId() { return nodeId; }
    public String getToken() { return token; }
    public int getAttempt() { return attempt; }
}
