package com.github.xingjianluo.workfloworchestrationenginejava.workflow.config;

import com.github.xingjianluo.workfloworchestrationenginejava.workflow.engine.DefinitionValidator;
import com.github.xingjianluo.workfloworchestrationenginejava.workflow.engine.WorkflowEngine;
import com.github.xingjianluo.workfloworchestrationenginejava.workflow.exec.NodeExecutor;
import com.github.xingjianluo.workfloworchestrationenginejava.workflow.exec.TaskHandlerRegistry;
import com.github.xingjianluo.workfloworchestrationenginejava.workflow.scheduler.WorkflowScheduler;
import com.github.xingjianluo.workfloworchestrationenginejava.workflow.store.Clock;
import com.github.xingjianluo.workfloworchestrationenginejava.workflow.store.FileWorkflowStore;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/** Spring Boot 装配：本地文件存储 + 引擎 + 执行器 + 调度器。 */
@Configuration
@EnableConfigurationProperties(WorkflowProperties.class)
public class WorkflowAutoConfiguration {

    @Bean
    public Clock workflowClock() {
        return Clock.system();
    }

    @Bean
    public FileWorkflowStore workflowStore(WorkflowProperties properties, Clock clock) {
        return new FileWorkflowStore(properties.getBaseDir(), clock);
    }

    @Bean
    public DefinitionValidator definitionValidator() {
        return new DefinitionValidator();
    }

    @Bean
    public TaskHandlerRegistry taskHandlerRegistry() {
        return new TaskHandlerRegistry();
    }

    @Bean
    public WorkflowEngine workflowEngine(FileWorkflowStore store, DefinitionValidator validator,
                                         Clock clock, WorkflowProperties properties) {
        return new WorkflowEngine(store, validator, clock, properties);
    }

    @Bean
    public ExecutorService workflowWorkerPool(WorkflowProperties properties) {
        return Executors.newFixedThreadPool(Math.max(1, properties.getWorkerThreads()));
    }

    @Bean
    public NodeExecutor nodeExecutor(TaskHandlerRegistry registry, ExecutorService workflowWorkerPool,
                                     WorkflowEngine workflowEngine, WorkflowProperties properties) {
        return new NodeExecutor(registry, workflowWorkerPool, workflowEngine, properties);
    }

    @Bean(destroyMethod = "shutdown")
    public WorkflowScheduler workflowScheduler(WorkflowEngine engine, NodeExecutor executor,
                                               WorkflowProperties properties) {
        WorkflowScheduler scheduler = new WorkflowScheduler(engine, executor, properties);
        scheduler.start();
        return scheduler;
    }
}
