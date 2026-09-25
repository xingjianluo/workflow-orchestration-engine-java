package com.github.xingjianluo.workfloworchestrationenginejava.workflow.store;

import com.github.xingjianluo.workfloworchestrationenginejava.workflow.model.WorkflowInstance;

import java.util.List;
import java.util.function.UnaryOperator;

/**
 * 实例持久化存储。所有读-改-写都通过 {@link #mutate} 串行化完成，
 * 保证多线程下同一实例的原子演进；落盘保证重启可恢复。
 */
public interface WorkflowStore {

    /** 幂等创建：幂等键已存在时返回既有实例；否则原子写入新实例。 */
    WorkflowInstance createIfAbsent(String idempotencyKey, WorkflowInstance candidate);

    WorkflowInstance load(String instanceId);

    WorkflowInstance findByIdempotencyKey(String idempotencyKey);

    /** 读-改-写并原子持久化，version 自增。mutation 内抛异常则不写入。 */
    WorkflowInstance mutate(String instanceId, UnaryOperator<WorkflowInstance> mutation);

    List<WorkflowInstance> loadAll();
}
