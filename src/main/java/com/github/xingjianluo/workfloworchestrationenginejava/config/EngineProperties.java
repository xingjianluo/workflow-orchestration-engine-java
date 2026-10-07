package com.github.xingjianluo.workfloworchestrationenginejava.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 引擎本地配置。
 */
@ConfigurationProperties(prefix = "workflow")
public class EngineProperties {

    /** 租约有效期（毫秒）：超过后其他执行器可安全接管 */
    private long leaseTtlMillis = 2000;
    /** 调度扫描间隔（毫秒） */
    private long scanIntervalMillis = 100;
    /** 执行线程数（核心线程数） */
    private int workerThreads = 4;
    /**
     * 执行线程池上限：长耗时节点占住核心线程时，池可弹性扩容到该值，
     * 避免个别长任务把可推进的节点顶住
     */
    private int maxWorkerThreads = 64;
    /**
     * 引擎级默认单次执行时长预算（毫秒）：0 表示不限制（默认，与引入预算前行为一致）。
     * 可被流程定义 executionTimeoutMillis、节点级 nodeTimeouts 逐层覆盖
     */
    private long defaultExecutionTimeoutMillis = 0L;
    /** 实例文件存储目录 */
    private String storeDir = "target/workflow-store";
    /** 启动时是否从磁盘恢复 */
    private boolean recoverOnStartup = true;

    public long getLeaseTtlMillis() { return leaseTtlMillis; }
    public void setLeaseTtlMillis(long leaseTtlMillis) { this.leaseTtlMillis = leaseTtlMillis; }
    public long getScanIntervalMillis() { return scanIntervalMillis; }
    public void setScanIntervalMillis(long scanIntervalMillis) { this.scanIntervalMillis = scanIntervalMillis; }
    public int getWorkerThreads() { return workerThreads; }
    public void setWorkerThreads(int workerThreads) { this.workerThreads = workerThreads; }
    public int getMaxWorkerThreads() { return maxWorkerThreads; }
    public void setMaxWorkerThreads(int maxWorkerThreads) { this.maxWorkerThreads = maxWorkerThreads; }
    public long getDefaultExecutionTimeoutMillis() { return defaultExecutionTimeoutMillis; }
    public void setDefaultExecutionTimeoutMillis(long defaultExecutionTimeoutMillis) { this.defaultExecutionTimeoutMillis = defaultExecutionTimeoutMillis; }
    public String getStoreDir() { return storeDir; }
    public void setStoreDir(String storeDir) { this.storeDir = storeDir; }
    public boolean isRecoverOnStartup() { return recoverOnStartup; }
    public void setRecoverOnStartup(boolean recoverOnStartup) { this.recoverOnStartup = recoverOnStartup; }
}
