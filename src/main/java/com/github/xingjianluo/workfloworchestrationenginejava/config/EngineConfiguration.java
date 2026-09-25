package com.github.xingjianluo.workfloworchestrationenginejava.config;

import com.github.xingjianluo.workfloworchestrationenginejava.engine.Clock;
import com.github.xingjianluo.workfloworchestrationenginejava.engine.DefinitionValidator;
import com.github.xingjianluo.workfloworchestrationenginejava.engine.RetryPolicy;
import com.github.xingjianluo.workfloworchestrationenginejava.engine.WorkflowEngine;
import com.github.xingjianluo.workfloworchestrationenginejava.store.WorkflowStore;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 引擎 Spring 装配。
 */
@Configuration
@EnableConfigurationProperties(EngineProperties.class)
public class EngineConfiguration {

    @Bean
    public DefinitionValidator definitionValidator() {
        return new DefinitionValidator();
    }

    @Bean
    public RetryPolicy retryPolicy() {
        return new RetryPolicy();
    }

    @Bean(initMethod = "start", destroyMethod = "stop")
    public WorkflowEngine workflowEngine(WorkflowStore store,
                                         EngineProperties properties,
                                         RetryPolicy retryPolicy,
                                         DefinitionValidator validator) {
        return new WorkflowEngine(store, properties, Clock.SYSTEM, retryPolicy, validator);
    }
}
