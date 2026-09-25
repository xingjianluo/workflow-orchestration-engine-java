package com.github.xingjianluo.workfloworchestrationenginejava.tests;

import com.github.xingjianluo.workfloworchestrationenginejava.engine.RetryPolicy;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class RetryPolicyTest {

    private final RetryPolicy policy = new RetryPolicy();

    @Test
    void firstFailureOfThreeSchedulesExactlyBackoffLater() {
        // maxAttempts=3，已尝试 1 次 -> 下次时间 = now + backoff
        assertEquals(1_300L, policy.nextAttemptAt(1, 3, 1_000L, 300L));
    }

    @Test
    void secondFailureStillHasBudget() {
        assertEquals(2_500L, policy.nextAttemptAt(2, 3, 2_000L, 500L));
    }

    @Test
    void exhaustionReturnsMinusOne() {
        assertEquals(-1L, policy.nextAttemptAt(3, 3, 2_000L, 500L));
    }

    @Test
    void singleAttemptMeansNoRetryEver() {
        assertEquals(-1L, policy.nextAttemptAt(1, 1, 1_000L, 10_000L));
    }
}
