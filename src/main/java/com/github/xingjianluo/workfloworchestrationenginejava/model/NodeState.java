package com.github.xingjianluo.workfloworchestrationenginejava.model;

/**
 * 节点生命周期状态。
 */
public enum NodeState {
    /** 等待上游就绪 */
    PENDING,
    /** 已获取租约，任务正在执行 */
    RUNNING,
    /** 执行成功（终态） */
    SUCCEEDED,
    /** 执行失败，等待退避后自动重试 */
    RETRY_WAIT,
    /** 已耗尽自动重试次数，等待人工干预（终态，可被人工重试） */
    FAILED,
    /** 被人工暂停（仅 PENDING/RETRY_WAIT 可暂停） */
    PAUSED,
    /** 被取消（终态） */
    CANCELLED
}
