package com.github.xingjianluo.workfloworchestrationenginejava.workflow.web.dto;

import com.github.xingjianluo.workfloworchestrationenginejava.workflow.model.WorkflowDefinition;

/** 提交流程请求：幂等键 + 定义。 */
public class SubmitWorkflowRequest {
    private String idempotencyKey;
    private WorkflowDefinition definition;

    public String getIdempotencyKey() { return idempotencyKey; }
    public void setIdempotencyKey(String idempotencyKey) { this.idempotencyKey = idempotencyKey; }
    public WorkflowDefinition getDefinition() { return definition; }
    public void setDefinition(WorkflowDefinition definition) { this.definition = definition; }
}
