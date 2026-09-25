package com.github.xingjianluo.workfloworchestrationenginejava.workflow.model;

/** 节点执行结果回传后的处置结论。 */
public enum CompletionOutcome {
    COMMITTED_SUCCESS,
    COMMITTED_WAIT_RETRY,
    COMMITTED_TERMINAL_FAILURE,
    LEASE_LOST
}
