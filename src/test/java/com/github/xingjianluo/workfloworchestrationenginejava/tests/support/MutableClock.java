package com.github.xingjianluo.workfloworchestrationenginejava.tests.support;

import com.github.xingjianluo.workfloworchestrationenginejava.workflow.store.Clock;

import java.util.concurrent.atomic.AtomicLong;

/** 测试可控时钟：手动推进时间，精确控制退避与租约过期。 */
public class MutableClock implements Clock {
    private final AtomicLong now;

    public MutableClock() {
        this(1_000_000L);
    }

    public MutableClock(long start) {
        this.now = new AtomicLong(start);
    }

    @Override
    public long nowMillis() {
        return now.get();
    }

    public void advance(long millis) {
        now.addAndGet(millis);
    }

    public void set(long millis) {
        now.set(millis);
    }
}
