package com.github.xingjianluo.workfloworchestrationenginejava.workflow.error;

/** 干预请求基于过期版本（乐观锁不匹配）。 */
public class StaleInterventionException extends WorkflowException {
    public StaleInterventionException(String message) {
        super(ErrorCode.STALE_INTERVENTION, message);
    }
}
