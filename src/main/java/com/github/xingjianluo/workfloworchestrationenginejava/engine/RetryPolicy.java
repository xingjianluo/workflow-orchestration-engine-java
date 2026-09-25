package com.github.xingjianluo.workfloworchestrationenginejava.engine;

/**
 * 固定退避重试策略：是否还有下一次尝试、下次可执行时间均可预期。
 *
 * <p>规则：attempts 表示已经发生（含刚刚失败）的尝试次数。
 * 当 {@code attempts >= maxAttempts} 时不再自动重试，返回 -1；
 * 否则下一次可执行时间为 {@code nowMillis + backoffMillis}。
 */
public class RetryPolicy {

    public RetryPolicy() {
    }

    /**
     * @param attempts      已发生的尝试次数（含刚失败的这次）
     * @param maxAttempts   最大自动尝试次数
     * @param nowMillis     当前时间
     * @param backoffMillis 固定退避
     * @return 下次尝试时间；返回 -1 表示已耗尽
     */
    public long nextAttemptAt(int attempts, int maxAttempts, long nowMillis, long backoffMillis) {
        if (attempts >= maxAttempts) {
            return -1L;
        }
        return nowMillis + backoffMillis;
    }
}
