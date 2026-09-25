package com.github.xingjianluo.workfloworchestrationenginejava.workflow.web;

import com.github.xingjianluo.workfloworchestrationenginejava.workflow.error.ErrorCode;
import com.github.xingjianluo.workfloworchestrationenginejava.workflow.error.WorkflowException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.Map;

/** 把业务错误码映射为可区分的 HTTP 响应。 */
@RestControllerAdvice
public class WorkflowExceptionHandler {

    @ExceptionHandler(WorkflowException.class)
    public ResponseEntity<Map<String, String>> handle(WorkflowException ex) {
        HttpStatus status = switch (ex.getErrorCode()) {
            case INSTANCE_NOT_FOUND -> HttpStatus.NOT_FOUND;
            // 409 语义最贴合：冲突（重复提交 / 非法状态转换 / 已终态 / 过期干预）
            case DUPLICATE_SUBMISSION, ILLEGAL_TRANSITION, ALREADY_TERMINAL,
                 STALE_INTERVENTION, NODE_NOT_RETRIABLE -> HttpStatus.CONFLICT;
            // 定义类问题：请求内容不合法
            case DUPLICATE_NODE, MISSING_DEPENDENCY, CYCLIC_DEPENDENCY, UNKNOWN_NODE_TYPE ->
                    HttpStatus.BAD_REQUEST;
        };
        return ResponseEntity.status(status)
                .body(Map.of("errorCode", ex.getErrorCode().name(), "message", ex.getMessage()));
    }
}
