package com.github.xingjianluo.workfloworchestrationenginejava.workflow.web.dto;

/** 人工干预请求：可携带期望版本用于过期检测；为 null 表示不校验。 */
public class InterventionRequest {
    private Long expectedVersion;
    private String nodeId;

    public Long getExpectedVersion() { return expectedVersion; }
    public void setExpectedVersion(Long expectedVersion) { this.expectedVersion = expectedVersion; }
    public String getNodeId() { return nodeId; }
    public void setNodeId(String nodeId) { this.nodeId = nodeId; }
}
