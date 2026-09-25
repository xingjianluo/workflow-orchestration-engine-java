package com.github.xingjianluo.workfloworchestrationenginejava.model;

/**
 * 流程实例生命周期状态。
 */
public enum WorkflowState {
    /** 可调度：不断有节点被执行 */
    ACTIVE,
    /** 人工暂停：调度器不再拉起新节点，运行中的节点跑完当前尝试 */
    PAUSED,
    /** 全部节点进入成功/取消终态（终态） */
    SUCCEEDED,
    /** 存在 FAILED 节点且无运行中节点（终态，可被人工重试重新激活） */
    FAILED,
    /** 人工取消（终态，幂等） */
    CANCELLED
}
