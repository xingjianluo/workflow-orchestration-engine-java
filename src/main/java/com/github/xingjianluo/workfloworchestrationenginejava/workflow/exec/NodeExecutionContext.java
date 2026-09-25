package com.github.xingjianluo.workfloworchestrationenginejava.workflow.exec;

import com.github.xingjianluo.workfloworchestrationenginejava.workflow.model.NodeLease;

import java.util.function.Predicate;

/** 传给 {@link TaskHandler} 的执行上下文，可感知当前租约与心跳续租。 */
public class NodeExecutionContext {
    private final NodeLease lease;
    private final Predicate<NodeLease> heartbeatCheck;

    public NodeExecutionContext(NodeLease lease) {
        this(lease, l -> true);
    }

    public NodeExecutionContext(NodeLease lease, Predicate<NodeLease> heartbeatCheck) {
        this.lease = lease;
        this.heartbeatCheck = heartbeatCheck;
    }

    public NodeLease getLease() {
        return lease;
    }

    public String getInstanceId() {
        return lease.getInstanceId();
    }

    public String getNodeId() {
        return lease.getNodeId();
    }

    public int getAttempt() {
        return lease.getAttempt();
    }

    /** 周期性心跳：租约丢失（被接管）时返回 false，任务应主动中止以免双跑。 */
    public boolean heartbeat() {
        return heartbeatCheck.test(lease);
    }
}
