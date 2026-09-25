package com.github.xingjianluo.workfloworchestrationenginejava.workflow.web;

import com.github.xingjianluo.workfloworchestrationenginejava.workflow.engine.WorkflowEngine;
import com.github.xingjianluo.workfloworchestrationenginejava.workflow.model.WorkflowInstance;
import com.github.xingjianluo.workfloworchestrationenginejava.workflow.web.dto.InterventionRequest;
import com.github.xingjianluo.workfloworchestrationenginejava.workflow.web.dto.SubmitWorkflowRequest;
import org.springframework.web.bind.annotation.*;

/** 工作流 REST 接口。 */
@RestController
@RequestMapping("/api/workflows")
public class WorkflowController {

    private final WorkflowEngine engine;

    public WorkflowController(WorkflowEngine engine) {
        this.engine = engine;
    }

    @PostMapping
    public WorkflowInstance submit(@RequestBody SubmitWorkflowRequest request) {
        return engine.submit(request.getIdempotencyKey(), request.getDefinition());
    }

    @GetMapping("/{instanceId}")
    public WorkflowInstance get(@PathVariable String instanceId) {
        return engine.get(instanceId);
    }

    @PostMapping("/{instanceId}/pause")
    public WorkflowInstance pause(@PathVariable String instanceId,
                                  @RequestBody(required = false) InterventionRequest request) {
        return engine.pause(instanceId, expectedVersion(request));
    }

    @PostMapping("/{instanceId}/resume")
    public WorkflowInstance resume(@PathVariable String instanceId,
                                   @RequestBody(required = false) InterventionRequest request) {
        return engine.resume(instanceId, expectedVersion(request));
    }

    @PostMapping("/{instanceId}/cancel")
    public WorkflowInstance cancel(@PathVariable String instanceId,
                                   @RequestBody(required = false) InterventionRequest request) {
        return engine.cancel(instanceId, expectedVersion(request));
    }

    @PostMapping("/{instanceId}/nodes/{nodeId}/retry")
    public WorkflowInstance retryNode(@PathVariable String instanceId, @PathVariable String nodeId,
                                      @RequestBody(required = false) InterventionRequest request) {
        return engine.retryNode(instanceId, nodeId, expectedVersion(request));
    }

    private static Long expectedVersion(InterventionRequest request) {
        return request == null ? null : request.getExpectedVersion();
    }
}
