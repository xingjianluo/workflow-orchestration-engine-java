package com.github.xingjianluo.workfloworchestrationenginejava.engine;

import com.github.xingjianluo.workfloworchestrationenginejava.config.EngineProperties;
import com.github.xingjianluo.workfloworchestrationenginejava.error.ErrorCode;
import com.github.xingjianluo.workfloworchestrationenginejava.error.WorkflowException;
import com.github.xingjianluo.workfloworchestrationenginejava.handler.NodeTaskHandler;
import com.github.xingjianluo.workfloworchestrationenginejava.model.NodeExecution;
import com.github.xingjianluo.workfloworchestrationenginejava.model.NodeState;
import com.github.xingjianluo.workfloworchestrationenginejava.model.WorkflowDefinition;
import com.github.xingjianluo.workfloworchestrationenginejava.model.WorkflowInstance;
import com.github.xingjianluo.workfloworchestrationenginejava.model.WorkflowState;
import com.github.xingjianluo.workfloworchestrationenginejava.store.WorkflowStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 工作流编排核心。
 *
 * <p>一致性要点：
 * <ul>
 *     <li>每个实例一把锁，所有状态迁移、租约判定、提交结果（CAS）均在锁内完成，
 *     多线程下判定依据确定；状态变更后立即写盘。</li>
 *     <li>节点“实际执行”由租约授权：RUNNING 且租约未过期时其他执行器不得再拉起；
 *     租约过期后可被安全接管，旧属主回交结果时 CAS（owner + 状态）失败，结果被丢弃。</li>
 *     <li>单调度线程负责扫描/接管决策，避免多调度器重复入队。</li>
 *     <li>每次人工干预递增 instance.version；携带旧版本号的干预被拒绝（STALE_INTERVENTION）。</li>
 * </ul>
 */
public class WorkflowEngine {

    private static final Logger log = LoggerFactory.getLogger(WorkflowEngine.class);

    private final WorkflowStore store;
    private final EngineProperties properties;
    private final Clock clock;
    private final RetryPolicy retryPolicy;
    private final DefinitionValidator validator;

    /** requestId -> 运行时上下文（内存锁 + handler + 运行任务句柄） */
    private final Map<String, Context> contexts = new LinkedHashMap<>();
    private final Object contextsLock = new Object();

    private final String executorId = "exec-" + UUID.randomUUID().toString().substring(0, 8);
    private ScheduledExecutorService scheduler;
    private ExecutorService workers;
    private java.util.concurrent.ScheduledFuture<?> scanTask;
    private final AtomicBoolean running = new AtomicBoolean(false);

    public WorkflowEngine(WorkflowStore store,
                          EngineProperties properties,
                          Clock clock,
                          RetryPolicy retryPolicy,
                          DefinitionValidator validator) {
        this.store = store;
        this.properties = properties;
        this.clock = clock;
        this.retryPolicy = retryPolicy;
        this.validator = validator;
    }

    /** 上下文：每实例的串行锁与节点任务句柄 */
    private static final class Context {
        final ReentrantLock lock = new ReentrantLock();
        final Map<String, NodeTaskHandler> handlers = new LinkedHashMap<>();
        /** nodeId -> 当前运行任务（用于租约被接管后中断旧任务） */
        final Map<String, java.util.concurrent.Future<?>> runningTasks = new LinkedHashMap<>();

        NodeTaskHandler handler(String nodeId) {
            NodeTaskHandler h = handlers.get(nodeId);
            if (h == null) {
                throw new WorkflowException(ErrorCode.INVALID_DEFINITION,
                        "no handler registered for node: " + nodeId);
            }
            return h;
        }
    }

    // ------------------------------------------------------------------
    // 生命周期
    // ------------------------------------------------------------------

    public void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "workflow-scheduler");
            t.setDaemon(true);
            return t;
        });
        workers = Executors.newFixedThreadPool(Math.max(1, properties.getWorkerThreads()), r -> {
            Thread t = new Thread(r, "workflow-worker-" + executorId);
            t.setDaemon(true);
            return t;
        });
        if (properties.isRecoverOnStartup()) {
            recover();
        }
        scanTask = scheduler.scheduleWithFixedDelay(this::scanAllSafely,
                properties.getScanIntervalMillis(), properties.getScanIntervalMillis(),
                TimeUnit.MILLISECONDS);
        log.info("WorkflowEngine started: executorId={}, leaseTtl={}ms, workers={}",
                executorId, properties.getLeaseTtlMillis(), properties.getWorkerThreads());
    }

    public void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        if (scanTask != null) {
            scanTask.cancel(false);
        }
        scheduler.shutdownNow();
        workers.shutdown();
        try {
            if (!workers.awaitTermination(2, TimeUnit.SECONDS)) {
                workers.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            workers.shutdownNow();
        }
        log.info("WorkflowEngine stopped: {}", executorId);
    }

    private void recover() {
        List<WorkflowInstance> recovered = store.loadAll();
        long now = clock.nowMillis();
        for (WorkflowInstance inst : recovered) {
            Context ctx;
            synchronized (contextsLock) {
                ctx = contexts.computeIfAbsent(inst.getRequestId(), k -> new Context());
                for (String nodeId : inst.getNodes().keySet()) {
                    ctx.handlers.putIfAbsent(nodeId, node -> { });
                }
            }
            lockInstance(inst.getRequestId());
            try {
                for (NodeExecution node : inst.getNodes().values()) {
                    if (node.getState() == NodeState.RUNNING) {
                        // 上次进程退出时正在执行：租约直接判过期，等待扫描器重新拉起
                        node.setLeaseOwner(null);
                        node.setLeaseExpiresAt(0L);
                        node.setState(NodeState.PENDING);
                        node.setNextAttemptAt(now);
                        log.info("[{}] recovery: node {} was RUNNING, reset to PENDING for takeover",
                                inst.getRequestId(), node.getNodeId());
                    }
                }
                recomputeWorkflowState(inst, "recovery");
                store.save(inst);
            } finally {
                unlockInstance(inst.getRequestId());
            }
        }
    }

    // ------------------------------------------------------------------
    // 提交
    // ------------------------------------------------------------------

    public WorkflowInstance submit(WorkflowDefinition definition, Map<String, NodeTaskHandler> handlers) {
        validator.validate(definition);

        WorkflowInstance instance = new WorkflowInstance();
        long now = clock.nowMillis();
        instance.setRequestId(definition.requestId());
        instance.setMaxAttempts(definition.maxAttempts());
        instance.setBackoffMillis(definition.backoffMillis());
        instance.setState(WorkflowState.ACTIVE);
        instance.setCreatedAt(now);
        instance.setUpdatedAt(now);
        instance.setVersion(0L);

        Map<String, List<String>> deps = new LinkedHashMap<>(definition.dependencies());
        instance.setDependencies(deps);
        Map<String, NodeExecution> nodes = new LinkedHashMap<>();
        for (String nodeId : deps.keySet()) {
            NodeExecution n = new NodeExecution();
            n.setNodeId(nodeId);
            n.setState(NodeState.PENDING);
            nodes.put(nodeId, n);
        }
        instance.setNodes(nodes);

        // 先注册内存上下文，再做幂等落盘；落盘失败则回滚上下文
        Context ctx = new Context();
        if (handlers != null) {
            for (var e : handlers.entrySet()) {
                if (e.getValue() != null && nodes.containsKey(e.getKey())) {
                    ctx.handlers.put(e.getKey(), e.getValue());
                }
            }
        }
        synchronized (contextsLock) {
            Context existing = contexts.get(definition.requestId());
            if (existing != null) {
                mergeHandlers(existing, handlers, instance);
                log.info("[{}] duplicate submit ignored (in-memory), handlers merged, return existing",
                        definition.requestId());
                return store.find(definition.requestId())
                        .orElseThrow(() -> new WorkflowException(ErrorCode.WORKFLOW_NOT_FOUND,
                                "inconsistent state for: " + definition.requestId()));
            }
            boolean created = store.saveIfAbsent(instance);
            if (!created) {
                Context recoveredCtx = contexts.computeIfAbsent(definition.requestId(), k -> new Context());
                mergeHandlers(recoveredCtx, handlers, instance);
                log.info("[{}] duplicate submit ignored (on-disk), handlers merged, return existing",
                        definition.requestId());
                return store.find(definition.requestId())
                        .orElseThrow(() -> new WorkflowException(ErrorCode.WORKFLOW_NOT_FOUND,
                                "inconsistent state for: " + definition.requestId()));
            }
            contexts.put(definition.requestId(), ctx);
        }
        log.info("[{}] submitted: {} node(s), maxAttempts={}, backoff={}ms",
                definition.requestId(), nodes.size(), definition.maxAttempts(),
                definition.backoffMillis());
        return instance;
    }

    private void mergeHandlers(Context target, Map<String, NodeTaskHandler> handlers,
                               WorkflowInstance instance) {
        if (handlers == null) {
            return;
        }
        for (var e : handlers.entrySet()) {
            if (e.getValue() != null && instance.getNodes().containsKey(e.getKey())) {
                target.handlers.put(e.getKey(), e.getValue());
            }
        }
    }

    public Optional<WorkflowInstance> get(String requestId) {
        return store.find(requestId);
    }

    // ------------------------------------------------------------------
    // 人工干预
    // ------------------------------------------------------------------

    public void pause(String requestId, long expectedVersion) {
        WorkflowInstance inst = mustFind(requestId);
        lockInstance(requestId);
        try {
            checkVersion(inst, expectedVersion);
            if (isTerminal(inst.getState())) {
                throw new WorkflowException(ErrorCode.ALREADY_TERMINAL,
                        "cannot pause terminal workflow in state " + inst.getState());
            }
            if (inst.getState() == WorkflowState.PAUSED) {
                throw new WorkflowException(ErrorCode.ILLEGAL_TRANSITION,
                        "workflow already PAUSED: " + requestId);
            }
            // 等待中的节点下沉为 PAUSED；RUNNING 节点跑完当前尝试，其结果不影响 PAUSED 流程
            for (NodeExecution n : inst.getNodes().values()) {
                if (n.getState() == NodeState.PENDING || n.getState() == NodeState.RETRY_WAIT) {
                    n.setState(NodeState.PAUSED);
                }
            }
            inst.setState(WorkflowState.PAUSED);
            bumpAndSave(inst, "pause");
            log.info("[{}] paused by intervention (version -> {})", requestId, inst.getVersion());
        } finally {
            unlockInstance(requestId);
        }
    }

    public void resume(String requestId, long expectedVersion) {
        WorkflowInstance inst = mustFind(requestId);
        lockInstance(requestId);
        try {
            checkVersion(inst, expectedVersion);
            if (isTerminal(inst.getState())) {
                throw new WorkflowException(ErrorCode.ALREADY_TERMINAL,
                        "cannot resume terminal workflow in state " + inst.getState());
            }
            if (inst.getState() != WorkflowState.PAUSED) {
                throw new WorkflowException(ErrorCode.ILLEGAL_TRANSITION,
                        "only PAUSED workflow can be resumed, actual: " + inst.getState());
            }
            inst.setState(WorkflowState.ACTIVE);
            long now = clock.nowMillis();
            // PAUSED 的节点恢复后回到 PENDING 并立即可调度（退避等待已被人工恢复覆盖）
            for (NodeExecution n : inst.getNodes().values()) {
                if (n.getState() == NodeState.PAUSED) {
                    n.setState(NodeState.PENDING);
                    n.setNextAttemptAt(now);
                }
            }
            bumpAndSave(inst, "resume");
            log.info("[{}] resumed by intervention (version -> {})", requestId, inst.getVersion());
        } finally {
            unlockInstance(requestId);
        }
    }

    /**
     * 取消：幂等。已 CANCELLED 再次取消直接返回成功；其余终态拒绝。
     */
    public void cancel(String requestId, long expectedVersion) {
        WorkflowInstance inst = mustFind(requestId);
        lockInstance(requestId);
        try {
            if (inst.getState() == WorkflowState.CANCELLED) {
                log.info("[{}] cancel is idempotent, already CANCELLED", requestId);
                return;
            }
            checkVersion(inst, expectedVersion);
            if (isTerminal(inst.getState())) {
                throw new WorkflowException(ErrorCode.ALREADY_TERMINAL,
                        "cannot cancel terminal workflow in state " + inst.getState());
            }
            long now = clock.nowMillis();
            for (NodeExecution n : inst.getNodes().values()) {
                switch (n.getState()) {
                    case PENDING, RETRY_WAIT -> {
                        n.setState(NodeState.CANCELLED);
                        n.setCascadeCancelled(false);
                        n.setLeaseOwner(null);
                        n.setLeaseExpiresAt(0L);
                    }
                    case RUNNING -> {
                        // 先取消任务句柄，再清租约并置 CANCELLED；
                        // 被中断的旧任务迟到回交时会在 CAS 处被拒绝
                        interruptRunning(inst, n, "workflow cancelled");
                        n.setState(NodeState.CANCELLED);
                        n.setCascadeCancelled(false);
                        n.setLeaseOwner(null);
                        n.setLeaseExpiresAt(0L);
                    }
                    case PAUSED -> {
                        n.setState(NodeState.CANCELLED);
                        n.setCascadeCancelled(false);
                    }
                    default -> {
                        // SUCCEEDED / FAILED / CANCELLED 保持原样
                    }
                }
            }
            inst.setState(WorkflowState.CANCELLED);
            bumpAndSave(inst, "cancel");
            log.info("[{}] cancelled by intervention (version -> {})", requestId, inst.getVersion());
        } finally {
            unlockInstance(requestId);
        }
    }

    /**
     * 人工重试 FAILED 节点：重置 attempts，节点回到 PENDING；
     * 若流程已 FAILED，则整体回到 ACTIVE。
     */
    public void retryNode(String requestId, String nodeId, long expectedVersion) {
        WorkflowInstance inst = mustFind(requestId);
        lockInstance(requestId);
        try {
            checkVersion(inst, expectedVersion);
            NodeExecution node = inst.getNodes().get(nodeId);
            if (node == null) {
                throw new WorkflowException(ErrorCode.NODE_NOT_FOUND,
                        "node not found: " + nodeId + " in " + requestId);
            }
            if (node.getState() != NodeState.FAILED) {
                if (isTerminalNode(node.getState())) {
                    throw new WorkflowException(ErrorCode.ALREADY_TERMINAL,
                            "node " + nodeId + " is in terminal state " + node.getState());
                }
                throw new WorkflowException(ErrorCode.ILLEGAL_TRANSITION,
                        "manual retry allowed only for FAILED node, actual: " + node.getState());
            }
            node.setState(NodeState.PENDING);
            node.setAttempts(0);
            node.setNextAttemptAt(clock.nowMillis());
            node.setLastError(null);
            node.setLeaseOwner(null);
            node.setLeaseExpiresAt(0L);

            // 流程为终态 FAILED 时回到 ACTIVE；非终态（如仍有其他节点在跑）保持原状态。
            // CANCELLED/SUCCEEDED 流程不允许出现 FAILED 节点，无需处理。
            if (inst.getState() == WorkflowState.FAILED) {
                inst.setState(WorkflowState.ACTIVE);
            }
            reactivateDownstream(inst, nodeId);
            recomputeWorkflowState(inst, "manual-retry:" + nodeId);
            bumpAndSave(inst, "manual-retry:" + nodeId);
            log.info("[{}] node {} manually retried, attempts reset, workflow -> {}",
                    requestId, nodeId, inst.getState());
        } finally {
            unlockInstance(requestId);
        }
    }

    // ------------------------------------------------------------------
    // 调度
    // ------------------------------------------------------------------

    private void scanAllSafely() {
        try {
            scanAll();
        } catch (Throwable t) {
            log.error("scheduler scan error on {}", executorId, t);
        }
    }

    private void scanAll() {
        List<String> ids;
        synchronized (contextsLock) {
            ids = new ArrayList<>(contexts.keySet());
        }
        for (String id : ids) {
            try {
                scanOne(id);
            } catch (Throwable t) {
                log.error("[{}] scan error", id, t);
            }
        }
    }

    private void scanOne(String requestId) {
        WorkflowInstance inst = store.find(requestId).orElse(null);
        if (inst == null) {
            return;
        }
        Context ctx;
        synchronized (contextsLock) {
            ctx = contexts.get(requestId);
        }
        if (ctx == null) {
            return;
        }
        lockInstance(requestId);
        List<String> toLaunch;
        List<String> takeOver;
        try {
            long now = clock.nowMillis();
            toLaunch = new ArrayList<>();
            takeOver = new ArrayList<>();
            for (NodeExecution node : inst.getNodes().values()) {
                switch (node.getState()) {
                    case PENDING -> {
                        if (inst.getState() == WorkflowState.ACTIVE
                                && depsSatisfied(inst, node)
                                && node.getNextAttemptAt() <= now) {
                            toLaunch.add(node.getNodeId());
                        }
                    }
                    case RETRY_WAIT -> {
                        if (inst.getState() == WorkflowState.ACTIVE
                                && node.getNextAttemptAt() <= now) {
                            toLaunch.add(node.getNodeId());
                        }
                    }
                    case RUNNING -> {
                        if (node.getLeaseExpiresAt() <= now) {
                            takeOver.add(node.getNodeId());
                        }
                    }
                    default -> {
                    }
                }
            }
            // 先在锁内完成租约授权与落盘，保证“最多实际执行一次”
            for (String nodeId : toLaunch) {
                grantLease(inst, inst.getNodes().get(nodeId), ctx, now, "dependencies-ready");
            }
            for (String nodeId : takeOver) {
                grantLease(inst, inst.getNodes().get(nodeId), ctx, now, "lease-expired-takeover");
            }
            if (!toLaunch.isEmpty() || !takeOver.isEmpty()) {
                store.save(inst);
            }
        } finally {
            unlockInstance(requestId);
        }
        // 锁外提交任务到工作线程
        for (String nodeId : toLaunch) {
            submitExecution(requestId, nodeId);
        }
        for (String nodeId : takeOver) {
            submitExecution(requestId, nodeId);
        }
    }

    private boolean depsSatisfied(WorkflowInstance inst, NodeExecution node) {
        for (String dep : inst.getDependencies().getOrDefault(node.getNodeId(), List.of())) {
            if (inst.getNodes().get(dep).getState() != NodeState.SUCCEEDED) {
                return false;
            }
        }
        return true;
    }

    private void grantLease(WorkflowInstance inst, NodeExecution node, Context ctx,
                            long now, String reason) {
        String oldOwner = node.getLeaseOwner();
        String newOwner = executorId + "#" + UUID.randomUUID().toString().substring(0, 8);
        node.setState(NodeState.RUNNING);
        node.setLeaseOwner(newOwner);
        node.setLeaseExpiresAt(now + properties.getLeaseTtlMillis());
        node.setAttempts(node.getAttempts() + 1);
        log.info("[{}] lease granted: node={}, owner={}, attempts={}, reason={}, expiresAt={}",
                inst.getRequestId(), node.getNodeId(), newOwner, node.getAttempts(),
                reason, node.getLeaseExpiresAt());
        if (oldOwner != null && !oldOwner.equals(newOwner) && reason.contains("takeover")) {
            log.warn("[{}] lease takeover: node={}, staleOwner={}, newOwner={}, basis=expiresAt<={}",
                    inst.getRequestId(), node.getNodeId(), oldOwner, newOwner, now);
            interruptRunning(inst, node, "lease taken over by " + newOwner);
        }
    }

    private void interruptRunning(WorkflowInstance inst, NodeExecution node, String why) {
        Context ctx;
        synchronized (contextsLock) {
            ctx = contexts.get(inst.getRequestId());
        }
        if (ctx == null) {
            return;
        }
        java.util.concurrent.Future<?> task = ctx.runningTasks.remove(node.getNodeId());
        if (task != null) {
            task.cancel(true);
            log.info("[{}] interrupted running task node={}, basis={}",
                    inst.getRequestId(), node.getNodeId(), why);
        }
    }

    private void submitExecution(String requestId, String nodeId) {
        Context ctx;
        synchronized (contextsLock) {
            ctx = contexts.get(requestId);
        }
        if (ctx == null) {
            return;
        }
        final String owner;
        final NodeTaskHandler handler;
        lockInstance(requestId);
        try {
            WorkflowInstance inst = store.find(requestId).orElseThrow();
            NodeExecution node = inst.getNodes().get(nodeId);
            if (node.getState() != NodeState.RUNNING || node.getLeaseOwner() == null) {
                return;
            }
            owner = node.getLeaseOwner();
            handler = ctx.handler(nodeId);
        } finally {
            unlockInstance(requestId);
        }

        WorkerTask workerTask = new WorkerTask(requestId, nodeId, owner, handler);
        // 提交到多线程 worker 池执行；Future 仅用于租约接管时中断旧任务
        java.util.concurrent.Future<?> future = workers.submit(workerTask);
        java.util.concurrent.Future<?> previous = ctx.runningTasks.put(nodeId, future);
        if (previous != null && previous != future) {
            previous.cancel(true);
        }
    }

    /** 工作线程执行体：结束后用 CAS（owner+RUNNING）回交结果 */
    private final class WorkerTask implements Runnable {
        private final String requestId;
        private final String nodeId;
        private final String leaseOwner;
        private final NodeTaskHandler handler;

        WorkerTask(String requestId, String nodeId, String leaseOwner, NodeTaskHandler handler) {
            this.requestId = requestId;
            this.nodeId = nodeId;
            this.leaseOwner = leaseOwner;
            this.handler = handler;
        }

        @Override
        public void run() {
            Throwable error = null;
            long startedAt = clock.nowMillis();
            log.info("[{}] node={} execution START owner={}", requestId, nodeId, leaseOwner);
            try {
                handler.execute(nodeId);
            } catch (Throwable t) {
                error = t;
            }
            long finishedAt = clock.nowMillis();
            complete(requestId, nodeId, leaseOwner, error, startedAt, finishedAt);
        }
    }

    private void complete(String requestId, String nodeId, String owner,
                          Throwable error, long startedAt, long finishedAt) {
        lockInstance(requestId);
        try {
            WorkflowInstance inst = store.find(requestId).orElse(null);
            if (inst == null) {
                log.warn("[{}] node={} result dropped: instance gone", requestId, nodeId);
                return;
            }
            NodeExecution node = inst.getNodes().get(nodeId);
            Context ctx;
            synchronized (contextsLock) {
                ctx = contexts.get(requestId);
            }
            if (ctx != null) {
                java.util.concurrent.Future<?> current = ctx.runningTasks.get(nodeId);
                if (current != null && current.isDone()) {
                    ctx.runningTasks.remove(nodeId, current);
                }
            }

            // CAS：只有当前仍为 RUNNING 且租约属主是自己，结果才被接受
            if (node.getState() != NodeState.RUNNING || !owner.equals(node.getLeaseOwner())) {
                log.warn("[{}] node={} result REJECTED (stale owner): myOwner={}, currentState={}, currentOwner={}, basis=lease-cas",
                        requestId, nodeId, owner, node.getState(), node.getLeaseOwner());
                return;
            }

            if (error == null) {
                node.setState(NodeState.SUCCEEDED);
                node.setLeaseOwner(null);
                node.setLeaseExpiresAt(0L);
                log.info("[{}] node={} SUCCEEDED owner={} cost={}ms; downstream gate opened",
                        requestId, nodeId, owner, finishedAt - startedAt);
            } else {
                long nextAt = retryPolicy.nextAttemptAt(
                        node.getAttempts(), inst.getMaxAttempts(), finishedAt, inst.getBackoffMillis());
                node.setLeaseOwner(null);
                node.setLeaseExpiresAt(0L);
                node.setLastError(String.valueOf(error));
                if (nextAt < 0) {
                    node.setState(NodeState.FAILED);
                    node.setNextAttemptAt(0L);
                    log.warn("[{}] node={} FAILED permanently owner={} attempts={}/{} error={}",
                            requestId, nodeId, owner, node.getAttempts(), inst.getMaxAttempts(),
                            String.valueOf(error));
                    cascadeCancelDownstream(inst, nodeId);
                } else {
                    node.setState(NodeState.RETRY_WAIT);
                    node.setNextAttemptAt(nextAt);
                    log.info("[{}] node={} attempt #{} failed, RETRY_WAIT until {} (backoff={}ms) error={}",
                            requestId, nodeId, node.getAttempts(), nextAt,
                            inst.getBackoffMillis(), String.valueOf(error));
                }
            }
            recomputeWorkflowState(inst, "node-" + nodeId + "-" + (error == null ? "success" : "failure"));
            inst.setUpdatedAt(clock.nowMillis());
            store.save(inst);
            logState(inst);
        } finally {
            unlockInstance(requestId);
        }
    }

    /**
     * 依据节点状态集合重算流程状态（不在锁外调用）：
     * 任一 RUNNING / 可继续的 PENDING/RETRY_WAIT（未取消场景）→ ACTIVE；
     * 存在 FAILED 且无运行项 → FAILED；全 SUCCEEDED/CANCELLED → SUCCEEDED；
     * CANCELLED 仅由 cancel 显式设置，这里不覆盖。
     */
    private void recomputeWorkflowState(WorkflowInstance inst, String why) {
        if (inst.getState() == WorkflowState.CANCELLED) {
            return;
        }
        boolean anyRunning = false;
        boolean anyFailed = false;
        boolean anyRetryScheduled = false;
        boolean anyPaused = false;
        boolean anyPending = false;
        boolean allSucceeded = true;
        for (NodeExecution n : inst.getNodes().values()) {
            switch (n.getState()) {
                case RUNNING -> anyRunning = true;
                case FAILED -> anyFailed = true;
                case RETRY_WAIT -> anyRetryScheduled = true;
                case PAUSED -> anyPaused = true;
                case PENDING -> anyPending = true;
                case SUCCEEDED, CANCELLED -> {
                }
            }
            if (n.getState() != NodeState.SUCCEEDED && n.getState() != NodeState.CANCELLED) {
                allSucceeded = false;
            }
        }
        WorkflowState before = inst.getState();
        WorkflowState target;
        if (before == WorkflowState.PAUSED) {
            // 暂停期间只允许被 resume/cancel 改变；重算不覆盖
            target = WorkflowState.PAUSED;
        } else if (allSucceeded && !inst.getNodes().isEmpty()) {
            target = WorkflowState.SUCCEEDED;
        } else if (anyRunning || anyRetryScheduled) {
            target = WorkflowState.ACTIVE;
        } else if (anyPaused) {
            target = WorkflowState.PAUSED;
        } else if (anyFailed) {
            // 有永久失败节点，且没有运行中/待重试/暂停节点：
            // 其下游 PENDING 会被连锁阻塞，流程终态 FAILED，等待人工重试
            target = WorkflowState.FAILED;
        } else if (anyPending) {
            target = WorkflowState.ACTIVE;
        } else {
            target = before;
        }
        if (before != target) {
            inst.setState(target);
            log.info("[{}] workflow state {} -> {} (basis={})",
                    inst.getRequestId(), before, target, why);
        }
    }

    /**
     * 上游永久失败后沿依赖边级联：只有当一个待处理节点的全部上游都处于
     * FAILED 或“已级联 CANCELLED”时才取消它（多上游中仍有健康分支则保留）。
     * 已终态节点不覆盖。
     */
    private void cascadeCancelDownstream(WorkflowInstance inst, String failedNodeId) {
        boolean changed = true;
        while (changed) {
            changed = false;
            for (Map.Entry<String, List<String>> e : inst.getDependencies().entrySet()) {
                String child = e.getKey();
                NodeExecution childNode = inst.getNodes().get(child);
                if (childNode.getState() != NodeState.PENDING
                        && childNode.getState() != NodeState.RETRY_WAIT
                        && childNode.getState() != NodeState.PAUSED) {
                    continue;
                }
                List<String> ups = e.getValue();
                if (ups.isEmpty()) {
                    continue;
                }
                boolean allDead = true;
                for (String up : ups) {
                    NodeState s = inst.getNodes().get(up).getState();
                    boolean dead = s == NodeState.FAILED
                            || (s == NodeState.CANCELLED
                            && inst.getNodes().get(up).isCascadeCancelled());
                    if (!dead) {
                        allDead = false;
                        break;
                    }
                }
                if (allDead) {
                    childNode.setState(NodeState.CANCELLED);
                    childNode.setCascadeCancelled(true);
                    childNode.setLeaseOwner(null);
                    childNode.setLeaseExpiresAt(0L);
                    log.info("[{}] node={} CANCELLED by upstream failure propagation",
                            inst.getRequestId(), child);
                    changed = true;
                }
            }
        }
    }

    /**
     * 人工重试上游后，反复扫描被级联取消的下游：当其所有上游都不再处于 FAILED/CANCELLED
     * （即已经成功或被重新激活）时，重新置为 PENDING 参与调度。
     */
    private void reactivateDownstream(WorkflowInstance inst, String retriedNodeId) {
        boolean changed = true;
        while (changed) {
            changed = false;
            for (Map.Entry<String, List<String>> e : inst.getDependencies().entrySet()) {
                String child = e.getKey();
                NodeExecution childNode = inst.getNodes().get(child);
                if (childNode.getState() != NodeState.CANCELLED || !childNode.isCascadeCancelled()) {
                    continue;
                }
                boolean blocked = false;
                for (String up : e.getValue()) {
                    NodeState s = inst.getNodes().get(up).getState();
                    if (s == NodeState.FAILED || s == NodeState.CANCELLED) {
                        blocked = true;
                        break;
                    }
                }
                if (!blocked) {
                    childNode.setState(NodeState.PENDING);
                    childNode.setAttempts(0);
                    childNode.setNextAttemptAt(clock.nowMillis());
                    childNode.setLastError(null);
                    childNode.setCascadeCancelled(false);
                    childNode.setLeaseOwner(null);
                    childNode.setLeaseExpiresAt(0L);
                    log.info("[{}] node={} reactivated after manual retry of upstream {}",
                            inst.getRequestId(), child, retriedNodeId);
                    changed = true;
                }
            }
        }
    }

    private void logState(WorkflowInstance inst) {
        StringBuilder sb = new StringBuilder();
        for (NodeExecution n : inst.getNodes().values()) {
            sb.append(n.getNodeId()).append('=').append(n.getState())
                    .append("(attempts=").append(n.getAttempts())
                    .append(",owner=").append(n.getLeaseOwner() == null ? "-" : n.getLeaseOwner())
                    .append(") ");
        }
        log.info("[{}] state={} version={} nodes: {}",
                inst.getRequestId(), inst.getState(), inst.getVersion(), sb.toString().trim());
    }

    // ------------------------------------------------------------------
    // 辅助
    // ------------------------------------------------------------------

    private WorkflowInstance mustFind(String requestId) {
        return store.find(requestId).orElseThrow(() ->
                new WorkflowException(ErrorCode.WORKFLOW_NOT_FOUND,
                        "workflow not found: " + requestId));
    }

    private void checkVersion(WorkflowInstance inst, long expectedVersion) {
        if (expectedVersion != inst.getVersion()) {
            throw new WorkflowException(ErrorCode.STALE_INTERVENTION,
                    "stale intervention: expectedVersion=" + expectedVersion
                            + " but actualVersion=" + inst.getVersion());
        }
    }

    private void bumpAndSave(WorkflowInstance inst, String why) {
        inst.setVersion(inst.getVersion() + 1);
        inst.setUpdatedAt(clock.nowMillis());
        store.save(inst);
        logState(inst);
    }

    private boolean isTerminal(WorkflowState s) {
        return s == WorkflowState.SUCCEEDED
                || s == WorkflowState.FAILED
                || s == WorkflowState.CANCELLED;
    }

    private boolean isTerminalNode(NodeState s) {
        return s == NodeState.SUCCEEDED || s == NodeState.CANCELLED;
    }

    private void lockInstance(String requestId) {
        Context ctx;
        synchronized (contextsLock) {
            ctx = contexts.get(requestId);
        }
        if (ctx != null) {
            ctx.lock.lock();
        }
    }

    private void unlockInstance(String requestId) {
        Context ctx;
        synchronized (contextsLock) {
            ctx = contexts.get(requestId);
        }
        if (ctx != null) {
            ctx.lock.unlock();
        }
    }

    public String getExecutorId() {
        return executorId;
    }
}
