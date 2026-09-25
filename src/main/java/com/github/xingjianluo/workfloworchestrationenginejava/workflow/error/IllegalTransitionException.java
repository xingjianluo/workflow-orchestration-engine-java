package com.github.xingjianluo.workfloworchestrationenginejava.workflow.error;

/** 非法状态转换。 */
public class IllegalTransitionException extends WorkflowException {
    public IllegalTransitionException(String message) {
        super(ErrorCode.ILLEGAL_TRANSITION, message);
    }
}
