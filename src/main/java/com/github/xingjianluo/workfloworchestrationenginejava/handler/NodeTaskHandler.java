package com.github.xingjianluo.workfloworchestrationenginejava.handler;

/**
 * 节点业务逻辑。实现方抛出异常即视为该次尝试失败。
 */
@FunctionalInterface
public interface NodeTaskHandler {
    void execute(String nodeId) throws Exception;
}
