package com.github.xingjianluo.workfloworchestrationenginejava.tests;

import com.github.xingjianluo.workfloworchestrationenginejava.tests.support.MutableClock;
import com.github.xingjianluo.workfloworchestrationenginejava.tests.support.WorkflowHarness;
import com.github.xingjianluo.workfloworchestrationenginejava.workflow.model.WorkflowDefinition;
import com.github.xingjianluo.workfloworchestrationenginejava.workflow.model.WorkflowInstance;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.github.xingjianluo.workfloworchestrationenginejava.tests.support.WorkflowHarness.definition;
import static com.github.xingjianluo.workfloworchestrationenginejava.tests.support.WorkflowHarness.node;
import static org.junit.jupiter.api.Assertions.*;

/** 同一业务请求（幂等键）重复提交 / 并发提交，只允许产生一个实例。 */
class SubmissionIdempotencyTest {

    @TempDir
    Path tempDir;
    private WorkflowHarness harness;

    @BeforeEach
    void setUp() {
        harness = new WorkflowHarness(tempDir, new MutableClock());
    }

    @Test
    void sequentialDuplicateSubmissionReturnsSameInstance() {
        WorkflowDefinition def = definition("wf", node("a", "noop"));
        WorkflowInstance first = harness.engine.submit("biz-001", def);
        WorkflowInstance second = harness.engine.submit("biz-001", def);

        assertEquals(first.getInstanceId(), second.getInstanceId(),
                "重复提交必须返回同一实例，不得产生重复实例");
        assertEquals(1, harness.store.loadAll().size());
        harness.dump("重复提交两次后", first.getInstanceId());
    }

    @Test
    void concurrentDuplicateSubmissionCreatesExactlyOneInstance() throws Exception {
        WorkflowDefinition def = definition("wf", node("a", "noop"), node("b", "noop", "a"));
        int threads = 16;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        Set<String> instanceIds = ConcurrentHashMap.newKeySet();
        CountDownLatch done = new CountDownLatch(threads);
        List<org.slf4j.Logger> ignored = List.of();

        for (int i = 0; i < threads; i++) {
            pool.submit(() -> {
                try {
                    start.await();
                    WorkflowInstance wf = harness.engine.submit("biz-concurrent", def);
                    instanceIds.add(wf.getInstanceId());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            });
        }
        start.countDown();
        assertTrue(done.await(10, TimeUnit.SECONDS));
        pool.shutdownNow();

        assertEquals(1, instanceIds.size(), "16 个线程并发提交同一业务请求，只能产生 1 个实例");
        assertEquals(1, harness.store.loadAll().size(), "存储中只能有 1 个实例文件");
        harness.dump("16 线程并发提交后", instanceIds.iterator().next());
    }

    @Test
    void differentBusinessKeysCreateDifferentInstances() {
        WorkflowDefinition def = definition("wf", node("a", "noop"));
        WorkflowInstance w1 = harness.engine.submit("biz-A", def);
        WorkflowInstance w2 = harness.engine.submit("biz-B", def);
        assertNotEquals(w1.getInstanceId(), w2.getInstanceId());
        assertEquals(2, harness.store.loadAll().size());
    }
}
