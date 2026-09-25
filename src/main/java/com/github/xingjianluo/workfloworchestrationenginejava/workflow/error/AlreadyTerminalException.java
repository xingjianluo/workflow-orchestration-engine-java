package com.github.xingjianluo.workfloworchestrationenginejava.workflow.error;

/** 操作对象已处于终态。 */
public class AlreadyTerminalException extends WorkflowException {
    public AlreadyTerminalException(String message) {
        super(ErrorCode.ALREADY_TERMINAL, message);
    }
}
