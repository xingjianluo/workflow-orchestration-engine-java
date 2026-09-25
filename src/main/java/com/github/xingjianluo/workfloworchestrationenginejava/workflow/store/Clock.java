package com.github.xingjianluo.workfloworchestrationenginejava.workflow.store;

/** 可替换的时钟，便于测试中精确控制退避与租约过期。 */
public interface Clock {
    long nowMillis();

    static Clock system() {
        return System::currentTimeMillis;
    }
}
