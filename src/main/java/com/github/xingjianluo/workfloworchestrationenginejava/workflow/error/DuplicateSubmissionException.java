package com.github.xingjianluo.workfloworchestrationenginejava.workflow.error;

/** 同一业务请求（幂等键）重复提交。 */
public class DuplicateSubmissionException extends WorkflowException {
    public DuplicateSubmissionException(String message) {
        super(ErrorCode.DUPLICATE_SUBMISSION, message);
    }
}
