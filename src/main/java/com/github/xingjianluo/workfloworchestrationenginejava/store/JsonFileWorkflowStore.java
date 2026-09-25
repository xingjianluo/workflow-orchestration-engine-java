package com.github.xingjianluo.workfloworchestrationenginejava.store;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.github.xingjianluo.workfloworchestrationenginejava.config.EngineProperties;
import com.github.xingjianluo.workfloworchestrationenginejava.error.ErrorCode;
import com.github.xingjianluo.workfloworchestrationenginejava.error.WorkflowException;
import com.github.xingjianluo.workfloworchestrationenginejava.model.WorkflowInstance;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;

/**
 * 基于本地 JSON 文件的存储。
 *
 * <ul>
 *     <li>每个流程实例一个文件 {@code <storeDir>/<requestId>.json}；</li>
 *     <li>写入采用“同目录临时文件 + 原子 ATOMIC_MOVE”，避免崩溃留下半文件；</li>
 *     <li>{@link #saveIfAbsent} 使用文件级锁 + {@code CREATE_NEW} 语义，
 *     多线程/多进程并发提交同一 requestId 只有一个实际创建；</li>
 *     <li>{@link #loadAll()} 扫描目录，供进程重启后恢复。</li>
 * </ul>
 */
@Component
public class JsonFileWorkflowStore implements WorkflowStore {

    private static final Logger log = LoggerFactory.getLogger(JsonFileWorkflowStore.class);
    private static final Pattern SAFE_ID = Pattern.compile("[A-Za-z0-9._-]+");

    private final Path storeDir;
    private final ObjectMapper mapper;

    public JsonFileWorkflowStore(EngineProperties properties) {
        this.storeDir = Path.of(properties.getStoreDir());
        this.mapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
    }

    @PostConstruct
    public void init() {
        try {
            Files.createDirectories(storeDir);
            log.info("Workflow store ready at: {}", storeDir.toAbsolutePath());
        } catch (IOException e) {
            throw new UncheckedIOException("cannot create store dir: " + storeDir, e);
        }
    }

    private Path targetFile(String requestId) {
        if (!SAFE_ID.matcher(requestId).matches()) {
            throw new WorkflowException(ErrorCode.INVALID_DEFINITION,
                    "requestId allows only [A-Za-z0-9._-] but was: " + requestId);
        }
        return storeDir.resolve(requestId + ".json");
    }

    @Override
    public boolean saveIfAbsent(WorkflowInstance instance) {
        Path target = targetFile(instance.getRequestId());
        Path lockFile = storeDir.resolve(instance.getRequestId() + ".lock");
        try (FileChannel channel = FileChannel.open(lockFile,
                StandardOpenOption.CREATE, StandardOpenOption.WRITE);
             FileLock ignored = channel.lock()) {
            if (Files.exists(target)) {
                log.debug("saveIfAbsent: {} already exists, skip", instance.getRequestId());
                return false;
            }
            atomicWrite(target, instance);
            log.info("Persisted new workflow instance: {}", instance.getRequestId());
            return true;
        } catch (IOException e) {
            throw new UncheckedIOException("saveIfAbsent failed: " + instance.getRequestId(), e);
        }
    }

    @Override
    public void save(WorkflowInstance instance) {
        Path target = targetFile(instance.getRequestId());
        Path lockFile = storeDir.resolve(instance.getRequestId() + ".lock");
        try (FileChannel channel = FileChannel.open(lockFile,
                StandardOpenOption.CREATE, StandardOpenOption.WRITE);
             FileLock ignored = channel.lock()) {
            atomicWrite(target, instance);
        } catch (IOException e) {
            throw new UncheckedIOException("save failed: " + instance.getRequestId(), e);
        }
    }

    private void atomicWrite(Path target, WorkflowInstance instance) throws IOException {
        Path tmp = storeDir.resolve(target.getFileName() + ".tmp." + Thread.currentThread().getId());
        byte[] payload = mapper.writeValueAsBytes(instance);
        Files.write(tmp, payload,
                StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING);
        try {
            Files.move(tmp, target, java.nio.file.StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException atomicNotSupported) {
            Files.move(tmp, target, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
        }
    }

    @Override
    public Optional<WorkflowInstance> find(String requestId) {
        Path target = targetFile(requestId);
        if (!Files.exists(target)) {
            return Optional.empty();
        }
        try {
            byte[] bytes = Files.readAllBytes(target);
            if (bytes.length == 0) {
                return Optional.empty();
            }
            return Optional.of(mapper.readValue(bytes, WorkflowInstance.class));
        } catch (IOException e) {
            throw new UncheckedIOException("read failed: " + requestId, e);
        }
    }

    @Override
    public List<WorkflowInstance> loadAll() {
        List<WorkflowInstance> result = new ArrayList<>();
        try (var stream = Files.list(storeDir)) {
            List<Path> files = stream
                    .filter(p -> p.getFileName().toString().endsWith(".json"))
                    .sorted(Comparator.comparing(p -> p.getFileName().toString()))
                    .toList();
            for (Path f : files) {
                byte[] bytes = Files.readAllBytes(f);
                if (bytes.length == 0) {
                    log.warn("Skip empty (possibly torn) file: {}", f);
                    continue;
                }
                try {
                    result.add(mapper.readValue(bytes, WorkflowInstance.class));
                } catch (IOException parseError) {
                    log.warn("Skip unreadable instance file {}: {}", f, parseError.toString());
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException("loadAll failed from " + storeDir, e);
        }
        log.info("Recovered {} workflow instance(s) from {}", result.size(), storeDir.toAbsolutePath());
        return result;
    }
}
