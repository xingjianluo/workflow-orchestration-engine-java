package com.github.xingjianluo.workfloworchestrationenginejava.error;

/**
 * 可区分的拒绝原因。
 */
public enum ErrorCode {
    /** 定义中存在重复节点 */
    DUPLICATE_NODE,
    /** 依赖指向不存在的节点 */
    MISSING_DEPENDENCY,
    /** 依赖图存在环 */
    CYCLE_DETECTED,
    /** 定义参数非法（空 requestId、maxAttempts<1 等） */
    INVALID_DEFINITION,
    /** 流程不存在 */
    WORKFLOW_NOT_FOUND,
    /** 节点不存在 */
    NODE_NOT_FOUND,
    /** 非法状态转换 */
    ILLEGAL_TRANSITION,
    /** 实例已处于终态 */
    ALREADY_TERMINAL,
    /** 干预请求携带的版本过期 */
    STALE_INTERVENTION
}
