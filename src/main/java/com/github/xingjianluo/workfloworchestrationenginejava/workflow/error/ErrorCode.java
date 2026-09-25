package com.github.xingjianluo.workfloworchestrationenginejava.workflow.error;

/** 可区分的错误码：定义校验 / 幂等 / 状态流转 / 终态 / 过期干预 各成一类。 */
public enum ErrorCode {
    DUPLICATE_NODE,
    MISSING_DEPENDENCY,
    CYCLIC_DEPENDENCY,
    DUPLICATE_SUBMISSION,
    INSTANCE_NOT_FOUND,
    ILLEGAL_TRANSITION,
    ALREADY_TERMINAL,
    STALE_INTERVENTION,
    NODE_NOT_RETRIABLE,
    UNKNOWN_NODE_TYPE
}
