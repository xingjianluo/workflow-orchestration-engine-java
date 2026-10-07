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
    /** 执行线程数 */
    private int workerThreads = 4;
    /** 实例文件存储目录 */
    private String storeDir = "target/workflow-store";
    /** 启动时是否从磁盘恢复 */
    private boolean recoverOnStartup = true;
    /**
     * 引擎级默认单次执行超时上限（毫秒）：当流程定义未指定任何上限时生效；
     * 0（默认）= 无上限，行为与引入超时能力前完全一致
     */
    private long defaultExecutionTimeoutMillis = 0L;

    public long getLeaseTtlMillis() { return leaseTtlMillis; }
    public void setLeaseTtlMillis(long leaseTtlMillis) { this.leaseTtlMillis = leaseTtlMillis; }
    public long getScanIntervalMillis() { return scanIntervalMillis; }
    public void setScanIntervalMillis(long scanIntervalMillis) { this.scanIntervalMillis = scanIntervalMillis; }
    public int getWorkerThreads() { return workerThreads; }
    public void setWorkerThreads(int workerThreads) { this.workerThreads = workerThreads; }
    public String getStoreDir() { return storeDir; }
    public void setStoreDir(String storeDir) { this.storeDir = storeDir; }
    public boolean isRecoverOnStartup() { return recoverOnStartup; }
    public void setRecoverOnStartup(boolean recoverOnStartup) { this.recoverOnStartup = recoverOnStartup; }
    public long getDefaultExecutionTimeoutMillis() { return defaultExecutionTimeoutMillis; }
    public void setDefaultExecutionTimeoutMillis(long defaultExecutionTimeoutMillis) { this.defaultExecutionTimeoutMillis = defaultExecutionTimeoutMillis; }
}
