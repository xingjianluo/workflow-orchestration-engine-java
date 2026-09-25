package com.github.xingjianluo.workfloworchestrationenginejava.workflow.engine;

import com.github.xingjianluo.workfloworchestrationenginejava.workflow.error.AlreadyTerminalException;
import com.github.xingjianluo.workfloworchestrationenginejava.workflow.error.IllegalTransitionException;
import com.github.xingjianluo.workfloworchestrationenginejava.workflow.model.NodeStatus;
import com.github.xingjianluo.workfloworchestrationenginejava.workflow.model.WorkflowStatus;

import java.util.EnumSet;
import java.util.Set;

/**
 * 状态迁移限制（单一事实来源）：
 * <pre>
 * 节点：PENDING/WAITING -> RUNNING -> SUCCEEDED
 *                        RUNNING -> WAITING（退避重试）-> RUNNING ...
 *                        RUNNING -> FAILED（重试耗尽，终态）
 *                        PENDING -> SKIPPED/CANCELLED（上游失败 / 流程取消）
 *                        WAITING -> CANCELLED
 * 流程：RUNNING <-> PAUSED；RUNNING/PAUSED -> SUCCEEDED/FAILED/CANCELLED
 * </pre>
 * 终态节点：SUCCEEDED / FAILED / SKIPPED / CANCELLED；终态流程：SUCCEEDED / FAILED / CANCELLED。
 */
public final class StateTransitions {

    private StateTransitions() {
    }

    public static final Set<NodeStatus> TERMINAL_NODE_STATUSES =
            EnumSet.of(NodeStatus.SUCCEEDED, NodeStatus.FAILED, NodeStatus.SKIPPED, NodeStatus.CANCELLED);

    public static final Set<WorkflowStatus> TERMINAL_WORKFLOW_STATUSES =
            EnumSet.of(WorkflowStatus.SUCCEEDED, WorkflowStatus.FAILED, WorkflowStatus.CANCELLED);

    /** 可被调度器抢占租约的节点状态（含退避到期判定，由调用方检查 nextRetryAt）。 */
    public static boolean isDispatchable(NodeStatus status) {
        return status == NodeStatus.PENDING || status == NodeStatus.WAITING;
    }

    public static boolean isNodeTerminal(NodeStatus status) {
        return TERMINAL_NODE_STATUSES.contains(status);
    }

    public static boolean isWorkflowTerminal(WorkflowStatus status) {
        return TERMINAL_WORKFLOW_STATUSES.contains(status);
    }

    /**
     * 节点级人工重试：FAILED（自身重试耗尽）与 SKIPPED（上游失败被连带跳过）可重试；
     * SUCCEEDED / CANCELLED 是不可推翻的终态，给出可区分的 ALREADY_TERMINAL 拒绝；
     * PENDING/WAITING/RUNNING 尚未失败，拒绝为 ILLEGAL_TRANSITION。
     */
    public static void requireRetriable(String nodeId, NodeStatus status) {
        if (status == NodeStatus.FAILED || status == NodeStatus.SKIPPED) {
            return;
        }
        if (status == NodeStatus.SUCCEEDED || status == NodeStatus.CANCELLED) {
            throw new AlreadyTerminalException("节点 " + nodeId + " 已处于终态 " + status + "，不可人工重试");
        }
        throw new IllegalTransitionException("节点 " + nodeId + " 处于 " + status
                + "，只有 FAILED/SKIPPED 节点允许人工重试");
    }
}
