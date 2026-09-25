package com.github.xingjianluo.workfloworchestrationenginejava.workflow.model;

/** 节点生命周期状态。 */
public enum NodeStatus {
    PENDING,
    WAITING,
    RUNNING,
    SUCCEEDED,
    FAILED,
    SKIPPED,
    CANCELLED
}
