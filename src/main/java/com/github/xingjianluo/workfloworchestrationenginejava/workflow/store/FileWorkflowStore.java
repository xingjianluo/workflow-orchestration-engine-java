package com.github.xingjianluo.workfloworchestrationenginejava.workflow.store;

import com.github.xingjianluo.workfloworchestrationenginejava.workflow.error.ErrorCode;
import com.github.xingjianluo.workfloworchestrationenginejava.workflow.error.WorkflowException;
import com.github.xingjianluo.workfloworchestrationenginejava.workflow.model.WorkflowInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

/**
 * 基于本地文件的实例存储。
 *
 * <ul>
 *   <li>{@code instances/&lt;id&gt;.dat}：每个实例一个文件，Java 序列化，tmp + 原子 rename 落盘；</li>
 *   <li>{@code keys.idx}：幂等键 -> 实例 id 索引，同样原子替换；</li>
 *   <li>同一实例的读-改-写由 per-instance 锁串行化，跨实例可并行；</li>
 *   <li>{@link #mutate} 在 mutation 正常返回后才落盘并自增 version，mutation 抛异常则无任何写入。</li>
 * </ul>
 */
public class FileWorkflowStore implements WorkflowStore {

    private static final Logger log = LoggerFactory.getLogger(FileWorkflowStore.class);

    private final Path baseDir;
    private final Path instancesDir;
    private final Path keyIndexFile;
    private final Clock clock;

    private final ReentrantLock globalLock = new ReentrantLock();
    private final ConcurrentHashMap<String, ReentrantLock> instanceLocks = new ConcurrentHashMap<>();

    public FileWorkflowStore(Path baseDir, Clock clock) {
        this.baseDir = baseDir;
        this.instancesDir = baseDir.resolve("instances");
        this.keyIndexFile = baseDir.resolve("keys.idx");
        this.clock = clock;
        try {
            Files.createDirectories(instancesDir);
        } catch (IOException e) {
            throw new IllegalStateException("无法创建工作流数据目录: " + baseDir, e);
        }
    }

    private ReentrantLock lockFor(String instanceId) {
        return instanceLocks.computeIfAbsent(instanceId, k -> new ReentrantLock());
    }

    // ------------------------------------------------------------------
    // 幂等创建
    // ------------------------------------------------------------------

    @Override
    public WorkflowInstance createIfAbsent(String idempotencyKey, WorkflowInstance candidate) {
        globalLock.lock();
        try {
            Properties index = readKeyIndex();
            String existingId = index.getProperty(idempotencyKey);
            if (existingId != null) {
                Path existingFile = instanceFile(existingId);
                if (Files.exists(existingFile)) {
                    // 同一业务请求重复提交：返回既有实例，绝不产生第二个实例
                    WorkflowInstance existing = readFile(existingFile);
                    log.info("幂等命中 key={} -> 既有实例 {}（重复提交不再创建）", idempotencyKey, existingId);
                    return existing;
                }
                // 上次提交在“索引落盘后、实例落盘前”崩溃：沿用同一实例 id 补写，保证收敛唯一
                log.warn("发现未完成的提交残留 key={} -> instanceId={}，补写实例文件", idempotencyKey, existingId);
                candidate.setInstanceId(existingId);
                candidate.setCreatedAt(clock.nowMillis());
                candidate.setUpdatedAt(clock.nowMillis());
                writeInstanceFile(candidate);
                return copy(candidate);
            }

            // 先登记幂等索引，再写实例文件；中途崩溃后由上面的残留收敛逻辑修复
            index.setProperty(idempotencyKey, candidate.getInstanceId());
            writeKeyIndex(index);
            candidate.setCreatedAt(clock.nowMillis());
            candidate.setUpdatedAt(clock.nowMillis());
            writeInstanceFile(candidate);
            log.info("创建新实例 id={} key={}", candidate.getInstanceId(), idempotencyKey);
            return copy(candidate);
        } finally {
            globalLock.unlock();
        }
    }

    // ------------------------------------------------------------------
    // 读取
    // ------------------------------------------------------------------

    @Override
    public WorkflowInstance load(String instanceId) {
        Path file = instanceFile(instanceId);
        if (!Files.exists(file)) {
            throw new WorkflowException(ErrorCode.INSTANCE_NOT_FOUND, "流程实例不存在: " + instanceId);
        }
        return readFile(file);
    }

    @Override
    public WorkflowInstance findByIdempotencyKey(String idempotencyKey) {
        String id = readKeyIndex().getProperty(idempotencyKey);
        if (id == null || !Files.exists(instanceFile(id))) {
            return null;
        }
        return readFile(instanceFile(id));
    }

    @Override
    public List<WorkflowInstance> loadAll() {
        List<WorkflowInstance> result = new ArrayList<>();
        try (Stream<Path> files = Files.list(instancesDir)) {
            files.filter(f -> f.getFileName().toString().endsWith(".dat"))
                    .forEach(f -> {
                        try {
                            result.add(readFile(f));
                        } catch (RuntimeException e) {
                            log.error("跳过无法读取的实例文件 {}: {}", f, e.getMessage());
                        }
                    });
        } catch (IOException e) {
            throw new IllegalStateException("列举实例目录失败: " + instancesDir, e);
        }
        return result;
    }

    // ------------------------------------------------------------------
    // 原子读-改-写
    // ------------------------------------------------------------------

    @Override
    public WorkflowInstance mutate(String instanceId, UnaryOperator<WorkflowInstance> mutation) {
        ReentrantLock lock = lockFor(instanceId);
        lock.lock();
        try {
            WorkflowInstance before = load(instanceId);
            WorkflowInstance after = mutation.apply(copy(before));
            if (after == null) {
                throw new IllegalStateException("mutation 返回 null: " + instanceId);
            }
            after.setVersion(before.getVersion() + 1);
            after.setUpdatedAt(clock.nowMillis());
            writeInstanceFile(after);
            return copy(after);
        } finally {
            lock.unlock();
        }
    }

    // ------------------------------------------------------------------
    // 文件读写细节
    // ------------------------------------------------------------------

    private Path instanceFile(String instanceId) {
        return instancesDir.resolve(instanceId + ".dat");
    }

    private void writeInstanceFile(WorkflowInstance instance) {
        Path target = instanceFile(instance.getInstanceId());
        Path tmp = instancesDir.resolve(instance.getInstanceId() + ".dat." + Thread.currentThread().getId() + ".tmp");
        try (ObjectOutputStream oos = new ObjectOutputStream(
                new BufferedOutputStream(Files.newOutputStream(tmp)))) {
            oos.writeObject(instance);
        } catch (IOException e) {
            throw new IllegalStateException("序列化实例失败: " + instance.getInstanceId(), e);
        }
        atomicMove(tmp, target);
    }

    private WorkflowInstance readFile(Path file) {
        try (ObjectInputStream ois = new ObjectInputStream(
                new BufferedInputStream(Files.newInputStream(file)))) {
            return (WorkflowInstance) ois.readObject();
        } catch (IOException | ClassNotFoundException e) {
            throw new IllegalStateException("读取实例文件失败: " + file, e);
        }
    }

    private Properties readKeyIndex() {
        Properties props = new Properties();
        if (Files.exists(keyIndexFile)) {
            try (var in = Files.newBufferedReader(keyIndexFile)) {
                props.load(in);
            } catch (IOException e) {
                throw new IllegalStateException("读取幂等索引失败: " + keyIndexFile, e);
            }
        }
        return props;
    }

    private void writeKeyIndex(Properties index) {
        Path tmp = baseDir.resolve("keys.idx.tmp." + Thread.currentThread().getId());
        try (var out = Files.newBufferedWriter(tmp)) {
            index.store(out, "idempotency-key -> instance-id");
        } catch (IOException e) {
            throw new IllegalStateException("写幂等索引失败", e);
        }
        atomicMove(tmp, keyIndexFile);
    }

    private void atomicMove(Path tmp, Path target) {
        try {
            Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (AtomicMoveNotSupportedException e) {
            try {
                Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING);
            } catch (IOException ex) {
                throw new IllegalStateException("替换文件失败: " + target, ex);
            }
        } catch (IOException e) {
            throw new IllegalStateException("原子替换文件失败: " + target, e);
        }
    }

    @SuppressWarnings("unchecked")
    private static WorkflowInstance copy(WorkflowInstance instance) {
        // 每次读写都经过序列化，天然得到深拷贝，避免调用方持有可变对象绕过锁
        try {
            java.io.ByteArrayOutputStream bos = new java.io.ByteArrayOutputStream();
            try (ObjectOutputStream oos = new ObjectOutputStream(bos)) {
                oos.writeObject(instance);
            }
            try (ObjectInputStream ois = new ObjectInputStream(
                    new java.io.ByteArrayInputStream(bos.toByteArray()))) {
                return (WorkflowInstance) ois.readObject();
            }
        } catch (IOException | ClassNotFoundException e) {
            throw new IllegalStateException("深拷贝实例失败", e);
        }
    }
}
