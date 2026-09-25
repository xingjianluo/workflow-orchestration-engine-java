package com.github.xingjianluo.workfloworchestrationenginejava.workflow.exec;

/** 业务节点任务处理器，按 node type 注册。 */
@FunctionalInterface
public interface TaskHandler {
    void execute(NodeExecutionContext context) throws Exception;
}
