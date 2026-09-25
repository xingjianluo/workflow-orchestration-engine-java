package com.github.xingjianluo.workfloworchestrationenginejava.workflow.model;

/** 流程实例生命周期状态。 */
public enum WorkflowStatus {
    RUNNING,
    PAUSED,
    SUCCEEDED,
    FAILED,
    CANCELLED
}
