package com.github.xingjianluo.workfloworchestrationenginejava.workflow.error;

/** 所有工作流业务异常的基类，携带可区分错误码。 */
public class WorkflowException extends RuntimeException {
    private final ErrorCode errorCode;

    public WorkflowException(ErrorCode errorCode, String message) {
        super(message);
        this.errorCode = errorCode;
    }

    public ErrorCode getErrorCode() {
        return errorCode;
    }
}
