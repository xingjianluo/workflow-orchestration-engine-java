package com.github.xingjianluo.workfloworchestrationenginejava.handler;

/**
 * 支持心跳续约的节点业务逻辑：长耗时任务应在循环/轮询中定期调用
 * {@link NodeExecutionContext#heartbeat()}，只要还在正常干活，租约就不会因
 * “跑得久”被判定失效，节点不会被重新拉起、重复执行。
 */
@FunctionalInterface
public interface HeartbeatCapableNodeTaskHandler extends NodeTaskHandler {

    @Override
    default void execute(String nodeId) throws Exception {
        execute(nodeId, null);
    }

    void execute(String nodeId, NodeExecutionContext context) throws Exception;
}
