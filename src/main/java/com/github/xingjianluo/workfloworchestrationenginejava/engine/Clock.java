package com.github.xingjianluo.workfloworchestrationenginejava.engine;

/**
 * 可注入时钟，便于测试租约超时与退避。
 */
public interface Clock {
    long nowMillis();

    Clock SYSTEM = System::currentTimeMillis;
}
