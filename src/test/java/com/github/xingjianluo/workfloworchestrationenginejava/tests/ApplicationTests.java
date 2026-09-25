package com.github.xingjianluo.workfloworchestrationenginejava.tests;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * 全栈冒烟：引擎随容器启动、REST 提交/查询/非法定义返回可区分错误码。
 */
@SpringBootTest(properties = {
        // 每次运行使用独立目录，避免历史持久化文件造成重复提交
        "workflow.store-dir=target/workflow-store-test-smoke-" + "#{T(java.util.UUID).randomUUID()}",
        "workflow.scan-interval-millis=20",
        "workflow.recover-on-startup=false"
})
@AutoConfigureMockMvc
public class ApplicationTests {

    @Autowired
    private MockMvc mvc;

    @Test
    public void submitQueryAndDuplicateViaRest() throws Exception {
        String body = """
                {
                  "requestId": "rest-1",
                  "maxAttempts": 1,
                  "backoffMillis": 0,
                  "dependencies": {"a": [], "b": ["a"]}
                }
                """;
        mvc.perform(post("/api/workflows").contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.requestId").value("rest-1"));

        // 幂等重复提交
        mvc.perform(post("/api/workflows").contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.requestId").value("rest-1"));

        // 等待执行完成后查询
        org.awaitility.Awaitility.await().atMost(java.time.Duration.ofSeconds(10)).untilAsserted(() ->
                mvc.perform(get("/api/workflows/rest-1"))
                        .andExpect(status().isOk())
                        .andExpect(jsonPath("$.state").value("SUCCEEDED")));
    }

    @Test
    public void invalidDefinitionReturnsDistinguishableErrorCode() throws Exception {
        String body = """
                {
                  "requestId": "rest-bad",
                  "maxAttempts": 1,
                  "backoffMillis": 0,
                  "dependencies": {"a": ["ghost"]}
                }
                """;
        mvc.perform(post("/api/workflows").contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errorCode").value("MISSING_DEPENDENCY"));
    }

    @Test
    public void unknownWorkflowReturns404() throws Exception {
        mvc.perform(get("/api/workflows/nope"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.errorCode").value("WORKFLOW_NOT_FOUND"));
    }
}
