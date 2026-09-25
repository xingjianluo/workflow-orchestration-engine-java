package com.github.xingjianluo.workfloworchestrationenginejava.workflow.exec;

import com.github.xingjianluo.workfloworchestrationenginejava.workflow.config.WorkflowProperties;
import com.github.xingjianluo.workfloworchestrationenginejava.workflow.engine.WorkflowEngine;
import com.github.xingjianluo.workfloworchestrationenginejava.workflow.model.CompletionOutcome;
import com.github.xingjianluo.workfloworchestrationenginejava.workflow.model.NodeLease;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * 把已获得租约的节点提交到工作线程池实际执行：
 * <ol>
 *   <li>执行期间以 heartbeatInterval 周期续租；续租失败说明租约被接管，任务可通过
 *       {@link NodeExecutionContext#heartbeat()} 感知并自行中止；</li>
 *   <li>业务处理器抛异常 -> completeFailure（退避重试 / 终态失败由引擎按租约与次数判定）；</li>
 *   <li>回传时若 token 已失效，结果以 LEASE_LOST 作废，绝不覆盖接管者的状态。</li>
 * </ol>
 */
public class NodeExecutor {

    private static final Logger log = LoggerFactory.getLogger(NodeExecutor.class);

    private final TaskHandlerRegistry registry;
    private final ExecutorService pool;
    private final WorkflowEngine engine;
    private final ScheduledExecutorService heartbeatScheduler;
    private final WorkflowProperties properties;

    public NodeExecutor(TaskHandlerRegistry registry, ExecutorService pool,
                        WorkflowEngine engine, WorkflowProperties properties) {
        this.registry = registry;
        this.pool = pool;
        this.engine = engine;
        this.properties = properties;
        this.heartbeatScheduler = java.util.concurrent.Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "workflow-heartbeat");
            t.setDaemon(true);
            return t;
        });
    }

    public void submit(NodeLease lease) {
        pool.submit(() -> runWithLease(lease));
    }

    private void runWithLease(NodeLease lease) {
        ScheduledFuture<?> heartbeatTask = heartbeatScheduler.scheduleAtFixedRate(
                () -> doHeartbeat(lease),
                properties.getHeartbeatIntervalMillis(),
                properties.getHeartbeatIntervalMillis(),
                TimeUnit.MILLISECONDS);
        NodeExecutionContext context = new NodeExecutionContext(lease, engine::heartbeat);
        try {
            TaskHandler handler = registry.resolve(typeOf(lease))
                    .orElse(null);
            if (handler == null) {
                // 未知类型按执行失败处理：同样受次数与退避约束，不会被无限空派发
                engine.completeFailure(lease, "未注册的任务类型: " + typeOf(lease));
                return;
            }
            try {
                handler.execute(context);
                CompletionOutcome outcome = engine.completeSuccess(lease);
                log.debug("执行回传 node={}/{} outcome={}", lease.getInstanceId(), lease.getNodeId(), outcome);
            } catch (Throwable t) {
                CompletionOutcome outcome = engine.completeFailure(lease, String.valueOf(t.getMessage()));
                log.debug("执行失败回传 node={}/{} outcome={}", lease.getInstanceId(), lease.getNodeId(), outcome);
            }
        } finally {
            heartbeatTask.cancel(false);
        }
    }

    private String typeOf(NodeLease lease) {
        return engine.get(lease.getInstanceId()).getDefinition().getNodes().get(lease.getNodeId()).getType();
    }

    private void doHeartbeat(NodeLease lease) {
        try {
            boolean alive = engine.heartbeat(lease);
            if (!alive) {
                log.warn("续租失败 node={}/{} token={}：租约已被接管，本线程结果后续将作废",
                        lease.getInstanceId(), lease.getNodeId(), lease.getToken());
            }
        } catch (Exception e) {
            log.warn("心跳异常 node={}/{}: {}", lease.getInstanceId(), lease.getNodeId(), e.getMessage());
        }
    }

    public void shutdown() {
        heartbeatScheduler.shutdownNow();
    }
}
