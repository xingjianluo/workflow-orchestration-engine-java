package com.github.xingjianluo.workfloworchestrationenginejava.web;

import com.github.xingjianluo.workfloworchestrationenginejava.engine.WorkflowEngine;
import com.github.xingjianluo.workfloworchestrationenginejava.error.ErrorCode;
import com.github.xingjianluo.workfloworchestrationenginejava.error.WorkflowException;
import com.github.xingjianluo.workfloworchestrationenginejava.handler.NodeTaskHandler;
import com.github.xingjianluo.workfloworchestrationenginejava.model.WorkflowDefinition;
import com.github.xingjianluo.workfloworchestrationenginejava.model.WorkflowInstance;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 工作流 REST 接口：
 * <pre>
 * POST /api/workflows                      提交定义（按 requestId 幂等）
 * GET  /api/workflows/{requestId}          查询实例状态
 * POST /api/workflows/{id}/pause?version=  暂停
 * POST /api/workflows/{id}/resume?version= 恢复
 * POST /api/workflows/{id}/cancel?version= 取消（幂等）
 * POST /api/workflows/{id}/nodes/{nodeId}/retry?version= 人工重试
 * </pre>
 * 通过 REST 提交的节点无自定义业务处理器，统一使用 no-op 处理器（执行即成功），
 * 便于通过接口演示依赖驱动的状态流转；业务处理器可在代码中直接调用
 * {@link WorkflowEngine#submit(WorkflowDefinition, Map)} 注入。
 */
@RestController
@RequestMapping("/api/workflows")
public class WorkflowController {

    private final WorkflowEngine engine;

    public WorkflowController(WorkflowEngine engine) {
        this.engine = engine;
    }

    public record SubmitRequest(String requestId,
                                Integer maxAttempts,
                                Long backoffMillis,
                                Map<String, List<String>> dependencies,
                                Long executionTimeoutMillis,
                                Map<String, Long> nodeTimeouts) {
    }

    @PostMapping
    public ResponseEntity<WorkflowInstance> submit(@RequestBody SubmitRequest request) {
        WorkflowDefinition def = new WorkflowDefinition(
                request.requestId(),
                request.maxAttempts() == null ? 1 : request.maxAttempts(),
                request.backoffMillis() == null ? 0L : request.backoffMillis(),
                request.dependencies(),
                request.executionTimeoutMillis(),
                request.nodeTimeouts());
        Map<String, NodeTaskHandler> handlers = new LinkedHashMap<>();
        if (def.dependencies() != null) {
            for (String nodeId : def.dependencies().keySet()) {
                handlers.put(nodeId, node -> { });
            }
        }
        WorkflowInstance instance = engine.submit(def, handlers);
        return ResponseEntity.ok(instance);
    }

    @GetMapping("/{requestId}")
    public WorkflowInstance get(@PathVariable String requestId) {
        return engine.get(requestId)
                .orElseThrow(() -> new WorkflowException(ErrorCode.WORKFLOW_NOT_FOUND,
                        "workflow not found: " + requestId));
    }

    @PostMapping("/{requestId}/pause")
    public ResponseEntity<Map<String, Object>> pause(@PathVariable String requestId,
                                                     @RequestParam long version) {
        engine.pause(requestId, version);
        return ResponseEntity.ok(Map.of("accepted", true, "requestId", requestId));
    }

    @PostMapping("/{requestId}/resume")
    public ResponseEntity<Map<String, Object>> resume(@PathVariable String requestId,
                                                      @RequestParam long version) {
        engine.resume(requestId, version);
        return ResponseEntity.ok(Map.of("accepted", true, "requestId", requestId));
    }

    @PostMapping("/{requestId}/cancel")
    public ResponseEntity<Map<String, Object>> cancel(@PathVariable String requestId,
                                                      @RequestParam long version) {
        engine.cancel(requestId, version);
        return ResponseEntity.ok(Map.of("accepted", true, "requestId", requestId));
    }

    @PostMapping("/{requestId}/nodes/{nodeId}/retry")
    public ResponseEntity<Map<String, Object>> retryNode(@PathVariable String requestId,
                                                         @PathVariable String nodeId,
                                                         @RequestParam long version) {
        engine.retryNode(requestId, nodeId, version);
        return ResponseEntity.ok(Map.of("accepted", true, "requestId", requestId, "nodeId", nodeId));
    }

    @ExceptionHandler(WorkflowException.class)
    public ResponseEntity<Map<String, Object>> handle(WorkflowException ex) {
        HttpStatus status = switch (ex.getErrorCode()) {
            case WORKFLOW_NOT_FOUND, NODE_NOT_FOUND -> HttpStatus.NOT_FOUND;
            case ALREADY_TERMINAL, ILLEGAL_TRANSITION, STALE_INTERVENTION -> HttpStatus.CONFLICT;
            default -> HttpStatus.BAD_REQUEST;
        };
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("errorCode", ex.getErrorCode().name());
        body.put("message", ex.getMessage());
        return ResponseEntity.status(status).body(body);
    }
}
