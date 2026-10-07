package com.github.xingjianluo.workfloworchestrationenginejava.handler;

/**
 * 单次节点执行的运行上下文（由引擎提供给支持心跳的处理器）。
 */
public interface NodeExecutionContext {

    /**
     * 续约当前租约：告知引擎“我还在正常干活”。
     * 续约受本次执行超时上限约束（不会超过 executionDeadlineAt）。
     *
     * @return true=续约成功；false=租约已丢失（被接管/取消/判超时），业务应尽快退出
     */
    boolean heartbeat();

    /**
     * 本次尝试的超时截止时间（epoch millis）；0 表示无上限。
     */
    long executionDeadlineAt();
}
