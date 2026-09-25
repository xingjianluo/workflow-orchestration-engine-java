package com.github.xingjianluo.workfloworchestrationenginejava.workflow.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.nio.file.Path;

/** 工作流引擎配置（可直接 new，也可由 Spring 以前缀 workflow.* 绑定）。 */
@ConfigurationProperties(prefix = "workflow")
public class WorkflowProperties {
    private Path baseDir = Path.of("target/workflow-data");
    private long leaseTtlMillis = 10_000;
    private long heartbeatIntervalMillis = 2_000;
    private long baseBackoffMillis = 1_000;
    private long backoffMultiplier = 2;
    private int defaultMaxAttempts = 3;
    private long schedulerTickMillis = 200;
    private int workerThreads = 4;

    public Path getBaseDir() { return baseDir; }
    public void setBaseDir(Path baseDir) { this.baseDir = baseDir; }
    public long getLeaseTtlMillis() { return leaseTtlMillis; }
    public void setLeaseTtlMillis(long leaseTtlMillis) { this.leaseTtlMillis = leaseTtlMillis; }
    public long getHeartbeatIntervalMillis() { return heartbeatIntervalMillis; }
    public void setHeartbeatIntervalMillis(long heartbeatIntervalMillis) { this.heartbeatIntervalMillis = heartbeatIntervalMillis; }
    public long getBaseBackoffMillis() { return baseBackoffMillis; }
    public void setBaseBackoffMillis(long baseBackoffMillis) { this.baseBackoffMillis = baseBackoffMillis; }
    public long getBackoffMultiplier() { return backoffMultiplier; }
    public void setBackoffMultiplier(long backoffMultiplier) { this.backoffMultiplier = backoffMultiplier; }
    public int getDefaultMaxAttempts() { return defaultMaxAttempts; }
    public void setDefaultMaxAttempts(int defaultMaxAttempts) { this.defaultMaxAttempts = defaultMaxAttempts; }
    public long getSchedulerTickMillis() { return schedulerTickMillis; }
    public void setSchedulerTickMillis(long schedulerTickMillis) { this.schedulerTickMillis = schedulerTickMillis; }
    public int getWorkerThreads() { return workerThreads; }
    public void setWorkerThreads(int workerThreads) { this.workerThreads = workerThreads; }
}
