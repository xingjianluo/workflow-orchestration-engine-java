package com.github.xingjianluo.workfloworchestrationenginejava.tests;

import com.github.xingjianluo.workfloworchestrationenginejava.workflow.exec.TaskHandlerRegistry;
import com.github.xingjianluo.workfloworchestrationenginejava.workflow.model.NodeDefinition;
import com.github.xingjianluo.workfloworchestrationenginejava.workflow.model.WorkflowDefinition;
import com.github.xingjianluo.workfloworchestrationenginejava.workflow.model.WorkflowInstance;
import com.github.xingjianluo.workfloworchestrationenginejava.workflow.model.WorkflowStatus;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static com.github.xingjianluo.workfloworchestrationenginejava.tests.support.Await.until;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Spring Boot 全链路冒烟：真实 HTTP 接口 + 自动装配的调度器/执行器 + 默认配置绑定，
 * 自动把一个两节点流程跑完，并验证取消幂等与冲突状态码。
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "workflow.base-dir=target/smoke-workflow-data",
                "workflow.scheduler-tick-millis=50",
                "workflow.lease-ttl-millis=5000",
                "workflow.heartbeat-interval-millis=100",
                "workflow.worker-threads=2"
        })
class SpringBootSmokeTest {

    @LocalServerPort
    int port;

    @Autowired
    TestRestTemplate rest;

    @Autowired
    TaskHandlerRegistry registry;

    private String url(String path) {
        return "http://localhost:" + port + "/api/workflows" + path;
    }

    private Map<String, Object> body(Object... kv) {
        Map<String, Object> m = new LinkedHashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            m.put((String) kv[i], kv[i + 1]);
        }
        return m;
    }

    @Test
    void submitViaHttpAndAutoScheduleRunsToSuccess() {
        AtomicInteger executions = new AtomicInteger();
        registry.register("smoke", ctx -> executions.incrementAndGet());

        WorkflowDefinition def = new WorkflowDefinition();
        Map<String, NodeDefinition> nodes = new LinkedHashMap<>();
        NodeDefinition a = new NodeDefinition();
        a.setId("a");
        a.setType("smoke");
        a.setDependsOn(List.of());
        NodeDefinition b = new NodeDefinition();
        b.setId("b");
        b.setType("smoke");
        b.setDependsOn(List.of("a"));
        nodes.put("a", a);
        nodes.put("b", b);
        def.setNodes(nodes);

        Map<String, Object> req = body("idempotencyKey", "smoke-key-1", "definition", def);
        ResponseEntity<WorkflowInstance> resp = rest.postForEntity(url(""), req, WorkflowInstance.class);
        assertTrue(resp.getStatusCode().is2xxSuccessful());
        String instanceId = resp.getBody().getInstanceId();

        until("自动调度跑完整条流程",
                () -> {
                    WorkflowInstance wf = rest.getForObject(url("/" + instanceId), WorkflowInstance.class);
                    return wf.getStatus() == WorkflowStatus.SUCCEEDED;
                }, Duration.ofSeconds(10));
        assertEquals(2, executions.get());

        // 终态后取消 -> 409 + ALREADY_TERMINAL
        ResponseEntity<Map> cancelResp = rest.postForEntity(
                url("/" + instanceId + "/cancel"), new HttpEntity<>(body()), Map.class);
        assertEquals(409, cancelResp.getStatusCode().value());
        assertEquals("ALREADY_TERMINAL", cancelResp.getBody().get("errorCode"));

        // 重复提交同一业务请求 -> 返回同一实例
        ResponseEntity<WorkflowInstance> dup = rest.postForEntity(url(""), req, WorkflowInstance.class);
        assertEquals(instanceId, dup.getBody().getInstanceId());
    }
}
