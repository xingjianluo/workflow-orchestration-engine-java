package com.github.xingjianluo.workfloworchestrationenginejava.tests.support;

import java.time.Duration;
import java.util.function.Supplier;

/** 测试轮询等待工具。 */
public final class Await {
    private Await() {
    }

    public static void until(String description, Supplier<Boolean> condition, Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        AssertionError last = new AssertionError("未进入等待: " + description);
        while (System.nanoTime() < deadline) {
            try {
                if (Boolean.TRUE.equals(condition.get())) {
                    return;
                }
            } catch (AssertionError e) {
                last = e;
            }
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError(e);
            }
        }
        throw new AssertionError("超时（" + timeout + "）未满足: " + description, last);
    }
}
