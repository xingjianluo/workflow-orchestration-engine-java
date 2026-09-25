package com.github.xingjianluo.workfloworchestrationenginejava.workflow.engine;

import com.github.xingjianluo.workfloworchestrationenginejava.workflow.config.WorkflowProperties;
import com.github.xingjianluo.workfloworchestrationenginejava.workflow.error.AlreadyTerminalException;
import com.github.xingjianluo.workfloworchestrationenginejava.workflow.error.ErrorCode;
import com.github.xingjianluo.workfloworchestrationenginejava.workflow.error.IllegalTransitionException;
import com.github.xingjianluo.workfloworchestrationenginejava.workflow.error.StaleInterventionException;
import com.github.xingjianluo.workfloworchestrationenginejava.workflow.error.WorkflowException;
import com.github.xingjianluo.workfloworchestrationenginejava.workflow.model.CompletionOutcome;
import com.github.xingjianluo.workfloworchestrationenginejava.workflow.model.NodeDefinition;
import com.github.xingjianluo.workfloworchestrationenginejava.workflow.model.NodeLease;
import com.github.xingjianluo.workfloworchestrationenginejava.workflow.model.NodeState;
import com.github.xingjianluo.workfloworchestrationenginejava.workflow.model.NodeStatus;
import com.github.xingjianluo.workfloworchestrationenginejava.workflow.model.WorkflowDefinition;
import com.github.xingjianluo.workfloworchestrationenginejava.workflow.model.WorkflowInstance;
import com.github.xingjianluo.workfloworchestrationenginejava.workflow.model.WorkflowStatus;
import com.github.xingjianluo.workfloworchestrationenginejava.workflow.store.Clock;
import com.github.xingjianluo.workfloworchestrationenginejava.workflow.store.WorkflowStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 工作流编排核心。
 *
 * <p>一致性要点：</p>
 * <ol>
 *   <li>所有状态演进都走 {@link WorkflowStore#mutate}（同一实例 JVM 内串行 + 原子落盘）；</li>
 *   <li>租约以 (token, expiresAt) 记录在节点状态中：抢占要求无有效租约，回传/心跳要求 token 匹配，
 *       因此多线程抢占只有一方成功；租约过期后他方可安全接管，旧持有者回传以 LEASE_LOST 作废；</li>
 *   <li>退避时间在失败回传时即固化到 nextRetryAt，重试次数与退避结果完全可预期；</li>
 *   <li>所有人工干预带 expectedVersion，版本不匹配以 STALE_INTERVENTION 拒绝。</li>
 * </ol>
 */
public class WorkflowEngine {

    private static final Logger log = LoggerFactory.getLogger(WorkflowEngine.class);

    private final WorkflowStore store;
    private final DefinitionValidator validator;
    private final Clock clock;
    private final WorkflowProperties properties;
    private final String ownerId;

    public WorkflowEngine(WorkflowStore store, DefinitionValidator validator,
                          Clock clock, WorkflowProperties properties) {
        this.store = store;
        this.validator = validator;
        this.clock = clock;
        this.properties = properties;
        this.ownerId = buildOwnerId();
    }

    private static String buildOwnerId() {
        String host;
        try {
            host = InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            host = "unknown-host";
        }
        return host + "#" + UUID.randomUUID().toString().substring(0, 8);
    }

    public String getOwnerId() {
        return ownerId;
    }

    // ==================================================================
    // 提交
    // ==================================================================

    public WorkflowInstance submit(String idempotencyKey, WorkflowDefinition definition) {
        if (idempotencyKey == null || idempotencyKey.isBlank()) {
            throw new WorkflowException(ErrorCode.DUPLICATE_SUBMISSION, "idempotencyKey 不能为空");
        }
        // 先完整校验：非法定义直接抛出，不创建任何实例（不会部分生效）
        validator.validate(definition);

        WorkflowInstance candidate = new WorkflowInstance();
        candidate.setInstanceId(UUID.randomUUID().toString());
        candidate.setDefinition(definition);
        candidate.setStatus(WorkflowStatus.RUNNING);
        candidate.setVersion(0);
        Map<String, NodeState> states = new LinkedHashMap<>();
        long now = clock.nowMillis();
        for (NodeDefinition node : definition.getNodes().values()) {
            NodeState state = new NodeState();
            state.setNodeId(node.getId());
            state.setStatus(NodeStatus.PENDING);
            state.setAttempts(0);
            state.setNextRetryAt(0);
            state.setUpdatedAt(now);
            states.put(node.getId(), state);
        }
        candidate.setStates(states);

        WorkflowInstance result = store.createIfAbsent(idempotencyKey, candidate);
        log.info("提交完成 key={} instanceId={} status={} nodes={}",
                idempotencyKey, result.getInstanceId(), result.getStatus(), result.getStates().size());
        return result;
    }

    // ==================================================================
    // 查询
    // ==================================================================

    public WorkflowInstance get(String instanceId) {
        return store.load(instanceId);
    }

    public Optional<WorkflowInstance> findByKey(String idempotencyKey) {
        return Optional.ofNullable(store.findByIdempotencyKey(idempotencyKey));
    }

    // ==================================================================
    // 人工干预
    // ==================================================================

    public WorkflowInstance pause(String instanceId, Long expectedVersion) {
        return intervene(instanceId, expectedVersion, "pause", wf -> {
            if (wf.getStatus() == WorkflowStatus.PAUSED) {
                // 暂停不幂等：重复暂停是可区分的非法转换（只有取消按需求要求幂等）
                throw new IllegalTransitionException("流程 " + instanceId + " 已处于 PAUSED，不可重复暂停");
            }
            if (StateTransitions.isWorkflowTerminal(wf.getStatus())) {
                throw new AlreadyTerminalException("流程 " + instanceId + " 已终态 "
                        + wf.getStatus() + "，不可暂停");
            }
            if (wf.getStatus() != WorkflowStatus.RUNNING) {
                throw new IllegalTransitionException("流程 " + instanceId + " 当前状态 "
                        + wf.getStatus() + "，不可暂停");
            }
            wf.setStatus(WorkflowStatus.PAUSED);
            // 冻结在执行节点：清除其租约并回到 PENDING，恢复后重新调度；
            // 其执行线程的迟到回传会因 token 已清空而 LEASE_LOST，暂停期间状态不演进
            for (NodeState node : wf.getStates().values()) {
                if (node.getStatus() == NodeStatus.RUNNING) {
                    log.info("暂停冻结在执行节点 {}/{}：旧租约 owner={} 作废，恢复后重新调度（不消耗重试额度）",
                            instanceId, node.getNodeId(), node.getLeaseOwner());
                    node.setStatus(NodeStatus.PENDING);
                    node.setAttempts(Math.max(0, node.getAttempts() - 1));
                    node.setLeaseToken(null);
                    node.setLeaseOwner(null);
                    node.setLeaseExpiresAt(0);
                    node.setUpdatedAt(clock.nowMillis());
                }
            }
            log.info("流程 {} 已暂停 version={}", instanceId, wf.getVersion() + 1);
            return wf;
        });
    }

    public WorkflowInstance resume(String instanceId, Long expectedVersion) {
        return intervene(instanceId, expectedVersion, "resume", wf -> {
            if (StateTransitions.isWorkflowTerminal(wf.getStatus())) {
                throw new AlreadyTerminalException("流程 " + instanceId + " 已终态 "
                        + wf.getStatus() + "，不可恢复");
            }
            if (wf.getStatus() != WorkflowStatus.PAUSED) {
                throw new IllegalTransitionException("流程 " + instanceId + " 当前状态 "
                        + wf.getStatus() + "，只有 PAUSED 可恢复");
            }
            wf.setStatus(WorkflowStatus.RUNNING);
            log.info("流程 {} 已恢复运行 version={}", instanceId, wf.getVersion() + 1);
            return wf;
        });
    }

    public WorkflowInstance cancel(String instanceId, Long expectedVersion) {
        return intervene(instanceId, expectedVersion, "cancel", wf -> {
            if (wf.getStatus() == WorkflowStatus.CANCELLED) {
                // 取消幂等：已取消直接返回，不报错
                log.info("取消幂等命中：流程 {} 已是 CANCELLED", instanceId);
                return wf;
            }
            if (StateTransitions.isWorkflowTerminal(wf.getStatus())) {
                throw new AlreadyTerminalException("流程 " + instanceId + " 已终态 "
                        + wf.getStatus() + "，不可取消（取消对 SUCCEEDED/FAILED 不幂等）");
            }
            wf.setStatus(WorkflowStatus.CANCELLED);
            // 所有非终态节点（含在执行中的 RUNNING）一并 CANCELLED 并作废租约；
            // 执行线程的迟到回传因 token 失效/流程已取消而 LEASE_LOST，保证取消后状态终态可解释
            for (NodeState node : wf.getStates().values()) {
                if (!StateTransitions.isNodeTerminal(node.getStatus())) {
                    if (node.getStatus() == NodeStatus.RUNNING) {
                        log.info("取消时回收在执行节点 {}/{}：旧租约 owner={} 作废",
                                instanceId, node.getNodeId(), node.getLeaseOwner());
                    }
                    node.setStatus(NodeStatus.CANCELLED);
                    node.setLeaseToken(null);
                    node.setLeaseOwner(null);
                    node.setLeaseExpiresAt(0);
                    node.setUpdatedAt(clock.nowMillis());
                }
            }
            log.info("流程 {} 已取消 version={}", instanceId, wf.getVersion() + 1);
            return wf;
        });
    }

    public WorkflowInstance retryNode(String instanceId, String nodeId, Long expectedVersion) {
        return intervene(instanceId, expectedVersion, "retryNode", wf -> {
            NodeState node = wf.getStates().get(nodeId);
            if (node == null) {
                throw new WorkflowException(ErrorCode.INSTANCE_NOT_FOUND,
                        "节点不存在: " + nodeId + "（实例 " + instanceId + "）");
            }
            if (StateTransitions.isWorkflowTerminal(wf.getStatus())
                    && wf.getStatus() != WorkflowStatus.FAILED) {
                throw new AlreadyTerminalException("流程 " + instanceId + " 已终态 "
                        + wf.getStatus() + "，不可人工重试节点");
            }
            StateTransitions.requireRetriable(nodeId, node.getStatus());
            // 人工重试：立即重新排队，退避清零，自动重试次数清零
            node.setStatus(NodeStatus.PENDING);
            node.setAttempts(0);
            node.setNextRetryAt(0);
            node.setLastError(null);
            node.setLeaseToken(null);
            node.setLeaseOwner(null);
            node.setLeaseExpiresAt(0);
            node.setUpdatedAt(clock.nowMillis());
            if (wf.getStatus() == WorkflowStatus.FAILED) {
                wf.setStatus(WorkflowStatus.RUNNING);
                log.info("人工重试节点 {} 使流程 {} 从 FAILED 回到 RUNNING", nodeId, instanceId);
            }
            log.info("人工重试节点 {}/{}：重置为 PENDING，attempts 清零", instanceId, nodeId);
            return wf;
        });
    }

    private WorkflowInstance intervene(String instanceId, Long expectedVersion, String action,
                                       java.util.function.UnaryOperator<WorkflowInstance> action_) {
        WorkflowInstance current = store.load(instanceId);
        if (expectedVersion != null && current.getVersion() != expectedVersion) {
            throw new StaleInterventionException("干预[" + action + "]基于过期版本: expected="
                    + expectedVersion + ", actual=" + current.getVersion()
                    + "（instanceId=" + instanceId + "）");
        }
        return store.mutate(instanceId, action_);
    }

    // ==================================================================
    // 租约协议
    // ==================================================================

    /**
     * 扫描全部实例，为第一个可执行节点抢占租约。
     * 抢占、过期接管都在单次原子 mutate 内判定并落盘。
     */
    public Optional<NodeLease> tryAcquireLease() {
        List<WorkflowInstance> all = store.loadAll();
        // 以实例 id 排序，保证调度顺序稳定、测试可预期
        all.sort((a, b) -> a.getInstanceId().compareTo(b.getInstanceId()));
        long now = clock.nowMillis();

        for (WorkflowInstance wf : all) {
            String instanceId = wf.getInstanceId();
            if (wf.getStatus() != WorkflowStatus.RUNNING) {
                continue;
            }
            List<String> candidateNodes = orderedNodeIds(wf.getDefinition());
            for (String nodeId : candidateNodes) {
                NodeState node = wf.getStates().get(nodeId);
                NodeStatus st = node.getStatus();
                boolean due;
                if (st == NodeStatus.PENDING) {
                    due = true;
                } else if (st == NodeStatus.WAITING) {
                    due = node.getNextRetryAt() <= now; // 退避未到则继续等待
                } else if (st == NodeStatus.RUNNING) {
                    // 租约过期的 RUNNING 节点可被安全接管
                    due = node.getLeaseToken() != null && node.getLeaseExpiresAt() <= now;
                } else {
                    due = false;
                }
                if (!due) {
                    continue;
                }
                if (st != NodeStatus.RUNNING && !dependenciesSatisfied(wf, nodeId)) {
                    continue; // 上游未全部成功（失败时该节点不会被选中，而由失败传播收敛）
                }
                Optional<NodeLease> acquired = tryAcquireOnNode(instanceId, nodeId, now);
                if (acquired.isPresent()) {
                    return acquired;
                }
            }
        }
        return Optional.empty();
    }

    private Optional<NodeLease> tryAcquireOnNode(String instanceId, String nodeId, long now) {
        AtomicReference<NodeLease> result = new AtomicReference<>();
        store.mutate(instanceId, wf -> {
            NodeState node = wf.getStates().get(nodeId);
            NodeStatus st = node.getStatus();
            boolean due = (st == NodeStatus.PENDING)
                    || (st == NodeStatus.WAITING && node.getNextRetryAt() <= now)
                    || (st == NodeStatus.RUNNING && node.getLeaseToken() != null
                        && node.getLeaseExpiresAt() <= now);
            // 双重判定（可能已被其他线程在本 mutate 排队前抢走）
            if (!due || wf.getStatus() != WorkflowStatus.RUNNING
                    || (st != NodeStatus.RUNNING && !dependenciesSatisfied(wf, nodeId))) {
                return wf;
            }
            if (node.getLeaseToken() != null && node.getLeaseExpiresAt() > now) {
                // 存在有效租约，绝不双跑
                log.debug("抢占未成功 node={}/{}：存在有效租约 owner={} expiresIn={}ms",
                        instanceId, nodeId, node.getLeaseOwner(), node.getLeaseExpiresAt() - now);
                return wf;
            }
            boolean takeover = node.getLeaseToken() != null;
            String token = UUID.randomUUID().toString();
            int attempt = node.getAttempts() + 1;
            node.setAttempts(attempt);
            node.setStatus(NodeStatus.RUNNING);
            node.setLeaseToken(token);
            node.setLeaseOwner(ownerId);
            node.setLeaseExpiresAt(now + properties.getLeaseTtlMillis());
            node.setUpdatedAt(now);
            if (takeover) {
                log.warn("租约接管 node={}/{}：旧租约 owner={} 已于 {} 过期，新 owner={} token={} attempt={}",
                        instanceId, nodeId, node.getLeaseOwner(), node.getLeaseExpiresAt(), ownerId, token, attempt);
            } else {
                log.info("租约授予 node={}/{}：owner={} token={} ttl={}ms attempt={}",
                        instanceId, nodeId, ownerId, token, properties.getLeaseTtlMillis(), attempt);
            }
            result.set(new NodeLease(instanceId, nodeId, token, attempt));
            return wf;
        });
        return Optional.ofNullable(result.get());
    }

    public boolean heartbeat(NodeLease lease) {
        AtomicReference<Boolean> ok = new AtomicReference<>(false);
        store.mutate(lease.getInstanceId(), wf -> {
            NodeState node = wf.getStates().get(lease.getNodeId());
            if (node != null && lease.getToken().equals(node.getLeaseToken())) {
                node.setLeaseExpiresAt(clock.nowMillis() + properties.getLeaseTtlMillis());
                node.setUpdatedAt(clock.nowMillis());
                ok.set(true);
            } else {
                log.warn("心跳作废 node={}/{}：token 不匹配（租约已被接管或释放）",
                        lease.getInstanceId(), lease.getNodeId());
            }
            return wf;
        });
        return ok.get();
    }

    public CompletionOutcome completeSuccess(NodeLease lease) {
        AtomicReference<CompletionOutcome> outcome = new AtomicReference<>();
        store.mutate(lease.getInstanceId(), wf -> {
            NodeState node = wf.getStates().get(lease.getNodeId());
            if (!lease.getToken().equals(node.getLeaseToken())) {
                log.warn("成功回传作废 node={}/{}：token 不匹配（判定依据：当前 owner={}），结果不生效以防双跑",
                        lease.getInstanceId(), lease.getNodeId(), node.getLeaseOwner());
                outcome.set(CompletionOutcome.LEASE_LOST);
                return wf;
            }
            if (wf.getStatus() == WorkflowStatus.PAUSED || wf.getStatus() == WorkflowStatus.CANCELLED) {
                log.warn("成功回传作废 node={}/{}：流程已 {}，暂停/取消期间结果不生效",
                        lease.getInstanceId(), lease.getNodeId(), wf.getStatus());
                outcome.set(CompletionOutcome.LEASE_LOST);
                return wf;
            }
            node.setStatus(NodeStatus.SUCCEEDED);
            node.setLeaseToken(null);
            node.setLeaseOwner(null);
            node.setLeaseExpiresAt(0);
            node.setLastError(null);
            node.setUpdatedAt(clock.nowMillis());
            log.info("节点成功 node={}/{} attempt={}", lease.getInstanceId(), lease.getNodeId(), lease.getAttempt());
            reconcileWorkflow(wf);
            outcome.set(CompletionOutcome.COMMITTED_SUCCESS);
            return wf;
        });
        return outcome.get();
    }

    public CompletionOutcome completeFailure(NodeLease lease, String error) {
        AtomicReference<CompletionOutcome> outcome = new AtomicReference<>();
        store.mutate(lease.getInstanceId(), wf -> {
            NodeState node = wf.getStates().get(lease.getNodeId());
            if (!lease.getToken().equals(node.getLeaseToken())) {
                log.warn("失败回传作废 node={}/{}：token 不匹配（判定依据：当前 owner={}），结果不生效",
                        lease.getInstanceId(), lease.getNodeId(), node.getLeaseOwner());
                outcome.set(CompletionOutcome.LEASE_LOST);
                return wf;
            }
            if (wf.getStatus() == WorkflowStatus.PAUSED || wf.getStatus() == WorkflowStatus.CANCELLED) {
                log.warn("失败回传作废 node={}/{}：流程已 {}，暂停/取消期间结果不生效",
                        lease.getInstanceId(), lease.getNodeId(), wf.getStatus());
                outcome.set(CompletionOutcome.LEASE_LOST);
                return wf;
            }
            int maxAttempts = maxAttemptsOf(wf.getDefinition(), lease.getNodeId());
            boolean canRetry = node.getAttempts() < maxAttempts;
            node.setLastError(truncate(error));
            node.setUpdatedAt(clock.nowMillis());
            if (canRetry) {
                long backoff = backoffMillis(node.getAttempts());
                node.setStatus(NodeStatus.WAITING);
                node.setNextRetryAt(clock.nowMillis() + backoff);
                node.setLeaseToken(null);
                node.setLeaseOwner(null);
                node.setLeaseExpiresAt(0);
                log.info("节点失败进入退避 node={}/{} attempt={}/{} backoff={}ms nextRetryAt={} error={}",
                        lease.getInstanceId(), lease.getNodeId(), node.getAttempts(), maxAttempts,
                        backoff, node.getNextRetryAt(), truncate(error));
                outcome.set(CompletionOutcome.COMMITTED_WAIT_RETRY);
            } else {
                node.setStatus(NodeStatus.FAILED);
                node.setLeaseToken(null);
                node.setLeaseOwner(null);
                node.setLeaseExpiresAt(0);
                log.warn("节点重试耗尽 node={}/{} attempts={} error={}",
                        lease.getInstanceId(), lease.getNodeId(), node.getAttempts(), truncate(error));
                reconcileWorkflow(wf);
                outcome.set(CompletionOutcome.COMMITTED_TERMINAL_FAILURE);
            }
            return wf;
        });
        return outcome.get();
    }

    /**
     * 重启恢复：对所有 RUNNING 但租约已过期的节点，回收为待重试/待执行状态。
     * 仍在租约期内的节点保持 RUNNING —— 若原执行者其实已死，等租约自然过期后由本方法或抢占接管，
     * 因此"短暂重启 + 快速恢复"时不会双跑。
     *
     * @return 被回收的 node 标识列表（instanceId/nodeId）
     */
    public List<String> recoverExpiredLeases() {
        List<String> recovered = new ArrayList<>();
        long now = clock.nowMillis();
        for (WorkflowInstance wf : store.loadAll()) {
            if (StateTransitions.isWorkflowTerminal(wf.getStatus())) {
                continue;
            }
            for (NodeState node : new ArrayList<>(wf.getStates().values())) {
                if (node.getStatus() != NodeStatus.RUNNING) {
                    continue;
                }
                if (node.getLeaseExpiresAt() > now) {
                    log.info("恢复扫描：node={}/{} 租约仍在有效期（剩余 {}ms），保持 RUNNING 不接管",
                            wf.getInstanceId(), node.getNodeId(), node.getLeaseExpiresAt() - now);
                    continue;
                }
                String staleOwner = node.getLeaseOwner();
                store.mutate(wf.getInstanceId(), w -> {
                    NodeState n = w.getStates().get(node.getNodeId());
                    if (n.getStatus() != NodeStatus.RUNNING || n.getLeaseExpiresAt() > now) {
                        return w;
                    }
                    reclaimAfterExpiredLease(w, n);
                    recovered.add(w.getInstanceId() + "/" + n.getNodeId());
                    log.warn("重启恢复：回收过期租约 node={}/{} 旧owner={} expiredAt={} -> {}",
                            w.getInstanceId(), n.getNodeId(), staleOwner, n.getLeaseExpiresAt(), n.getStatus());
                    reconcileWorkflow(w);
                    return w;
                });
            }
        }
        if (recovered.isEmpty()) {
            log.info("恢复扫描完成：无过期租约需要回收（owner={}）", ownerId);
        }
        return recovered;
    }

    /**
     * 把 RUNNING 但租约已失效的节点回退为 PENDING 立即重新参与调度。
     * 租约失效不产生失败回传，因此不增加 attempts、不受重试上限影响；
     * 新执行必须重新抢占租约，旧 token 的任何回传都会因 token 不匹配而作废。
     */
    private void reclaimAfterExpiredLease(WorkflowInstance wf, NodeState node) {
        node.setStatus(NodeStatus.PENDING);
        // 租约失效不产生失败回传，回退一次派发计数，不消耗自动重试失败额度
        node.setAttempts(Math.max(0, node.getAttempts() - 1));
        node.setNextRetryAt(0);
        node.setLeaseToken(null);
        node.setLeaseOwner(null);
        node.setLeaseExpiresAt(0);
        node.setUpdatedAt(clock.nowMillis());
    }

    // ==================================================================
    // 收敛与派生判定
    // ==================================================================

    private boolean dependenciesSatisfied(WorkflowInstance wf, String nodeId) {
        NodeDefinition def = wf.getDefinition().getNodes().get(nodeId);
        if (def.getDependsOn() == null) {
            return true;
        }
        for (String dep : def.getDependsOn()) {
            NodeState depState = wf.getStates().get(dep);
            if (depState.getStatus() != NodeStatus.SUCCEEDED) {
                return false;
            }
        }
        return true;
    }

    /**
     * 根据节点状态收敛流程状态并传播失败/取消：
     * 任一节点 FAILED -> 流程 FAILED，其未终态下游 SKIPPED（上游失败下游不得执行）；
     * 全部节点终态（含 SKIPPED/CANCELLED）-> 流程 SUCCEEDED（前提无 FAILED）；
     * 流程已 CANCELLED 时 RUNNING 节点完成后不改变流程终态。
     */
    private void reconcileWorkflow(WorkflowInstance wf) {
        if (wf.getStatus() == WorkflowStatus.CANCELLED) {
            return;
        }
        boolean anyFailed = wf.getStates().values().stream().anyMatch(n -> n.getStatus() == NodeStatus.FAILED);
        if (anyFailed) {
            for (NodeState node : wf.getStates().values()) {
                if (node.getStatus() == NodeStatus.FAILED) {
                    propagateSkip(wf, node.getNodeId());
                }
            }
            String failedNode = wf.getStates().values().stream()
                    .filter(n -> n.getStatus() == NodeStatus.FAILED)
                    .map(NodeState::getNodeId).findFirst().orElse("?");
            wf.setStatus(WorkflowStatus.FAILED);
            wf.setFailureReason("节点 " + failedNode + " 重试耗尽");
            log.warn("流程 {} 收敛为 FAILED（判定依据：节点 {} 终态失败，下游已 SKIP）",
                    wf.getInstanceId(), failedNode);
            return;
        }
        boolean allTerminal = wf.getStates().values().stream()
                .allMatch(n -> StateTransitions.isNodeTerminal(n.getStatus()));
        if (allTerminal) {
            boolean allSucceeded = wf.getStates().values().stream()
                    .allMatch(n -> n.getStatus() == NodeStatus.SUCCEEDED);
            if (allSucceeded) {
                wf.setStatus(WorkflowStatus.SUCCEEDED);
                wf.setFailureReason(null);
                log.info("流程 {} 收敛为 SUCCEEDED（判定依据：全部 {} 个节点 SUCCEEDED）",
                        wf.getInstanceId(), wf.getStates().size());
            } else {
                // 无 FAILED 但仍有 SKIPPED 残留：自动调度已无法推进，保持 FAILED 等待人工重试下游
                long skipped = wf.getStates().values().stream()
                        .filter(n -> n.getStatus() == NodeStatus.SKIPPED).count();
                wf.setStatus(WorkflowStatus.FAILED);
                wf.setFailureReason("存在 " + skipped + " 个因上游失败被 SKIPPED 的节点，需人工重试");
                log.warn("流程 {} 保持 FAILED（判定依据：无 FAILED 节点但有 {} 个 SKIPPED 残留）",
                        wf.getInstanceId(), skipped);
            }
        }
    }

    private void propagateSkip(WorkflowInstance wf, String failedNodeId) {
        for (NodeState node : wf.getStates().values()) {
            if (StateTransitions.isNodeTerminal(node.getStatus()) || node.getStatus() == NodeStatus.RUNNING) {
                continue;
            }
            NodeDefinition def = wf.getDefinition().getNodes().get(node.getNodeId());
            if (def.getDependsOn() != null && def.getDependsOn().contains(failedNodeId)) {
                node.setStatus(NodeStatus.SKIPPED);
                node.setUpdatedAt(clock.nowMillis());
                log.info("上游失败传播：节点 {}/{} -> SKIPPED（上游 {} FAILED）",
                        wf.getInstanceId(), node.getNodeId(), failedNodeId);
                propagateSkip(wf, node.getNodeId());
            }
        }
    }

    private int maxAttemptsOf(WorkflowDefinition definition, String nodeId) {
        NodeDefinition def = definition.getNodes().get(nodeId);
        if (def != null && def.getMaxAttempts() > 0) {
            return def.getMaxAttempts();
        }
        return properties.getDefaultMaxAttempts();
    }

    /** 确定性退避：baseBackoff * multiplier^(attempts-1)，attempts 为"已经失败的次数"。 */
    private long backoffMillis(int failedAttempts) {
        long backoff = properties.getBaseBackoffMillis();
        for (int i = 1; i < failedAttempts; i++) {
            backoff *= properties.getBackoffMultiplier();
        }
        return backoff;
    }

    private static List<String> orderedNodeIds(WorkflowDefinition definition) {
        return new ArrayList<>(definition.getNodes().keySet());
    }

    private static String truncate(String s) {
        if (s == null) {
            return null;
        }
        return s.length() <= 200 ? s : s.substring(0, 200) + "...";
    }
}
