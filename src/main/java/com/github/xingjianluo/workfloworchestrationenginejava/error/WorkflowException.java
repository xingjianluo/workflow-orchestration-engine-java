package com.github.xingjianluo.workfloworchestrationenginejava.error;

/**
 * 所有业务拒绝的统一异常，携带可区分的 {@link ErrorCode}。
 */
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
