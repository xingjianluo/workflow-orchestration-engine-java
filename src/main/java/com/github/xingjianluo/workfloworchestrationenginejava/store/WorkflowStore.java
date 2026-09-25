package com.github.xingjianluo.workfloworchestrationenginejava.store;

import com.github.xingjianluo.workfloworchestrationenginejava.model.WorkflowInstance;

import java.util.List;
import java.util.Optional;

/**
 * 实例持久化抽象：原子保存保证非法定义不会部分落盘。
 */
public interface WorkflowStore {

    /** 不存在则插入；返回 true 表示本次调用实际创建（用于幂等判定） */
    boolean saveIfAbsent(WorkflowInstance instance);

    /** 覆盖保存（状态流转后写盘） */
    void save(WorkflowInstance instance);

    Optional<WorkflowInstance> find(String requestId);

    List<WorkflowInstance> loadAll();
}
