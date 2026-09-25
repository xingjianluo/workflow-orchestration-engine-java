package com.github.xingjianluo.workfloworchestrationenginejava.workflow.exec;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/** node type -> TaskHandler 的注册表。 */
public class TaskHandlerRegistry {

    private final Map<String, TaskHandler> handlers = new ConcurrentHashMap<>();

    public void register(String type, TaskHandler handler) {
        if (type == null || type.isBlank()) {
            throw new IllegalArgumentException("任务类型不能为空");
        }
        if (handler == null) {
            throw new IllegalArgumentException("handler 不能为空");
        }
        handlers.put(type, handler);
    }

    public Optional<TaskHandler> resolve(String type) {
        return Optional.ofNullable(handlers.get(type));
    }

    public boolean containsType(String type) {
        return handlers.containsKey(type);
    }
}
