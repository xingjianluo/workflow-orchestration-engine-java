package com.github.xingjianluo.workfloworchestrationenginejava.workflow.error;

/** 流程定义非法（重复节点 / 缺失依赖 / 环）。非法定义不会部分生效。 */
public class DefinitionValidationException extends WorkflowException {
    public DefinitionValidationException(ErrorCode errorCode, String message) {
        super(errorCode, message);
    }
}
