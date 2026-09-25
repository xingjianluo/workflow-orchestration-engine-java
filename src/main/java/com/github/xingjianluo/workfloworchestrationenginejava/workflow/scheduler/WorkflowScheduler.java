package com.github.xingjianluo.workfloworchestrationenginejava.workflow.scheduler;

import com.github.xingjianluo.workfloworchestrationenginejava.workflow.config.WorkflowProperties;
import com.github.xingjianluo.workfloworchestrationenginejava.workflow.engine.WorkflowEngine;
import com.github.xingjianluo.workfloworchestrationenginejava.workflow.exec.NodeExecutor;
import com.github.xingjianluo.workfloworchestrationenginejava.workflow.model.NodeLease;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 单线程扫描器：
 * <ol>
 *   <li>启动（Bean 初始化）时先执行重启恢复：回收上次进程遗留的过期租约；</li>
 *   <li>之后每个 tick 调用引擎抢占一个可执行节点租约并交给 {@link NodeExecutor}；</li>
 *   <li>只有一个调度线程 + 引擎内原子 mutate，保证同一节点不会同时被两个线程抢占。</li>
 * </ol>
 */
public class WorkflowScheduler {

    private static final Logger log = LoggerFactory.getLogger(WorkflowScheduler.class);

    private final WorkflowEngine engine;
    private final NodeExecutor executor;
    private final WorkflowProperties properties;
    private ScheduledExecutorService ticker;
    private volatile boolean started;

    public WorkflowScheduler(WorkflowEngine engine, NodeExecutor executor, WorkflowProperties properties) {
        this.engine = engine;
        this.executor = executor;
        this.properties = properties;
    }

    /** 供测试手动使用：默认由 Spring 通过 @PostConstruct 触发。 */
    @PostConstruct
    public synchronized void start() {
        if (started) {
            return;
        }
        started = true;
        log.info("调度器启动 owner={}，先执行重启恢复扫描", engine.getOwnerId());
        engine.recoverExpiredLeases();
        ticker = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "workflow-scheduler");
            t.setDaemon(true);
            return t;
        });
        ticker.scheduleAtFixedRate(this::tickSafely, 0, properties.getSchedulerTickMillis(), TimeUnit.MILLISECONDS);
        log.info("调度器已运行 tick={}ms workerThreads={} leaseTtl={}ms",
                properties.getSchedulerTickMillis(), properties.getWorkerThreads(), properties.getLeaseTtlMillis());
    }

    private void tickSafely() {
        try {
            // 每个 tick 尽量派发：一次抢占成功后继续抢占（不同节点），直到没有可执行节点
            Optional<NodeLease> lease;
            while ((lease = engine.tryAcquireLease()).isPresent()) {
                NodeLease l = lease.get();
                log.info("调度派发 node={}/{} attempt={} token={}",
                        l.getInstanceId(), l.getNodeId(), l.getAttempt(), l.getToken());
                executor.submit(l);
            }
        } catch (Throwable t) {
            log.error("调度 tick 异常（下一 tick 重试）: {}", t.toString(), t);
        }
    }

    /** 供测试手动触发一次扫描。 */
    public void tickOnce() {
        tickSafely();
    }

    @PreDestroy
    public synchronized void shutdown() {
        if (!started) {
            return;
        }
        log.info("调度器关闭：停止派发新任务");
        started = false;
        if (ticker != null) {
            ticker.shutdownNow();
        }
        executor.shutdown();
    }
}
