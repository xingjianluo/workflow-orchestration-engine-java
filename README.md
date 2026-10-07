# 本地工作流编排引擎（Spring Boot）

一个可在**单机多线程**运行、**重启后可恢复**的本地工作流编排服务。
目标：流程提交、执行、重试、人工干预后的最终状态**始终可解释且一致**——
同一节点在多线程抢占或进程短暂重启恢复期间，最多只被**实际提交执行结果一次**；
租约过期后可以被安全接管而不是双跑；上游失败时下游绝不执行。

针对**长耗时业务节点**（批量计算、等待外部批处理返回等，可能跑数分钟到十几分钟）：

* 业务侧通过**心跳续约**告知“我还在正常干活”，节点跑再久也不会被误判失效、
  不会被重新拉起重复执行；
* 可为整条流程配置**单次执行时间上限**，也可对个别节点单独覆盖；
  超时未结束的尝试按既有的失败/重试规则落状态，不会一直挂起；
  超时判负后旧执行迟到的结果依旧被 CAS 丢弃，不会改回已发生的状态。

不依赖任何外部服务（无数据库 / 无消息队列），状态以 JSON 文件落盘。

---

## 1. 快速开始

```bash
# 一条命令运行全部测试（编译 + 43 个测试用例，含并发/恢复/租约/长任务/超时场景）
mvn test

# 启动服务
mvn spring-boot:run
```

服务默认端口 `8080`，实例持久化目录 `target/workflow-store`。

### REST 示例

```bash
# 提交一个 DAG：a -> b，requestId 为业务幂等键
curl -X POST localhost:8080/api/workflows \
  -H 'Content-Type: application/json' \
  -d '{"requestId":"order-1","maxAttempts":3,"backoffMillis":500,
       "dependencies":{"a":[],"b":["a"]}}'

# 带执行时间预算的提交：流程级默认 10 分钟，节点 a 单独放宽到 30 分钟
curl -X POST localhost:8080/api/workflows \
  -H 'Content-Type: application/json' \
  -d '{"requestId":"order-2","maxAttempts":3,"backoffMillis":500,
       "dependencies":{"a":[],"b":["a"]},
       "executionTimeoutMillis":600000,
       "nodeExecutionTimeoutMillis":{"a":1800000}}'

# 查询实例状态（含每个节点状态、尝试次数、租约属主、本次尝试的时间预算）
curl localhost:8080/api/workflows/order-1

# 暂停 / 恢复 / 取消（version 为乐观干预版本，必须等于当前 version）
curl -X POST 'localhost:8080/api/workflows/order-1/pause?version=0'
curl -X POST 'localhost:8080/api/workflows/order-1/resume?version=1'
curl -X POST 'localhost:8080/api/workflows/order-1/cancel?version=2'   # 幂等

# 人工重试永久失败的节点（重置该节点的完整重试预算）
curl -X POST 'localhost:8080/api/workflows/order-1/nodes/a/retry?version=3'
```

> 通过 REST 提交的节点使用 no-op 处理器（执行即成功），用于演示依赖驱动的状态流转。
> 真实业务处理器通过代码注入：
> `workflowEngine.submit(definition, Map.of("a", nodeId -> doBusiness(), ...))`。

---

## 2. 核心模型与生命周期

### 2.1 节点状态 `NodeState`

| 状态 | 含义 | 是否终态 |
|---|---|---|
| `PENDING` | 等待上游就绪 / 等待被调度 | 否 |
| `RUNNING` | 已持有租约，任务正在工作线程执行 | 否 |
| `RETRY_WAIT` | 一次尝试失败，退避到期后自动重试 | 否 |
| `PAUSED` | 流程暂停期间，等待中的节点下沉到该状态 | 否 |
| `SUCCEEDED` | 执行成功 | 是 |
| `FAILED` | 自动重试次数耗尽，等待人工重试 | 半终态* |
| `CANCELLED` | 取消；分为“人工取消”和“上游失败级联取消”（`cascadeCancelled=true`） | 是 |

\* `FAILED` 是流程终态，但唯一允许的离开方式是**人工重试**（重置 attempts 后回到 `PENDING`）。

```
PENDING ──授予租约──> RUNNING ──成功──> SUCCEEDED
                         │
                         └──失败且仍有预算──> RETRY_WAIT ──退避到期──> RUNNING
                         │
                         └──失败且预算耗尽──> FAILED ──人工重试──> PENDING

PENDING / RETRY_WAIT ──暂停──> PAUSED ──恢复──> PENDING
PENDING / RETRY_WAIT / PAUSED ──取消──> CANCELLED
RUNNING ──取消（中断任务+清租约）──> CANCELLED
PENDING/RETRY_WAIT/PAUSED ──所有上游永久失败──> CANCELLED(cascade)
   └──人工重试使上游恢复后，级联取消的节点会被重新激活为 PENDING
```

### 2.2 流程状态 `WorkflowState`

| 状态 | 含义 |
|---|---|
| `ACTIVE` | 有节点在运行、待重试或仍可被调度 |
| `PAUSED` | 人工暂停；调度器不拉起新节点，运行中的节点跑完当前尝试 |
| `SUCCEEDED` | 所有节点处于 SUCCEEDED/CANCELLED（无 FAILED）——终态 |
| `FAILED` | 存在 FAILED 节点且无运行中/待重试节点（下游被连锁阻塞）——终态，可人工重试 |
| `CANCELLED` | 人工取消——终态 |

流程状态由节点状态集合**确定性地重算**，日志中会打印迁移依据（`basis=...`）。

---

## 3. 并发一致性规则（关键）

### 3.1 租约模型：最多“实际提交”一次

* 调度器是**单线程**的，周期扫描所有实例；只有它能做“是否授权租约 / 是否接管”的决定，
  因此不存在两个调度器同时把同一节点入队的竞态。
* 授权租约时，在**实例级锁内**完成状态翻转（`PENDING/RETRY_WAIT -> RUNNING`）、
  写入全局唯一 `leaseOwner=executorId#uuid` 与 `leaseExpiresAt=now+leaseTtl`、
  **先落盘**再把任务派发给多线程 worker 池。
* `RUNNING` 且租约未过期的节点不会被再次授权。
* worker 执行结束回交结果时执行 **CAS**：只有“节点仍为 `RUNNING` 且 `leaseOwner==自己`”
  结果才被接受；否则结果被拒绝并记录
  `result REJECTED (stale owner), basis=lease-cas`。
  因此即使旧执行者“复活”，它也无法翻转已被接管/取消的节点状态。

### 3.2 租约超时接管（不双跑）

* 扫描发现 `RUNNING` 且 `leaseExpiresAt <= now` → 判定旧租约过期，授予新 owner，
  attempts 加 1，日志输出
  `lease takeover ... staleOwner=... newOwner=... basis=expiresAt<=now`。
* 接管授权后会**中断旧任务句柄**（`Future.cancel(true)`），尽量阻止旧任务继续执行；
  即便旧任务无法被及时中断，其迟到结果也会在 CAS 处被丢弃。
* 判定依据完全是持久化的 `leaseOwner/leaseExpiresAt`，不依赖线程是否存活，
  因此对“进程暂停、GC 长时间停顿、执行器卡死”都成立。

### 3.3 进程重启恢复

* 每次状态变更都通过“同目录临时文件 + `ATOMIC_MOVE`”原子写盘，不会留下半文件。
* 启动时扫描存储目录：所有 `RUNNING` 节点一律视为上一个进程遗留、租约失效，
  重置为 `PENDING`、清空 owner、`nextAttemptAt=now`，随后由扫描器重新授权执行。
* 恢复的实例注册 no-op 默认处理器；可用相同 `requestId` 再次 `submit` 覆盖为业务处理器，
  **不会创建第二个实例**。

### 3.4 幂等提交

* `requestId` 是业务幂等键。提交时**先完整校验定义**，再做文件级 `saveIfAbsent`
  （锁文件 + `CREATE_NEW` 语义）。
* 多线程/多次重复提交：只有一个实际落盘，其余返回同一实例；节点只执行一次。
* 非法定义在落盘前被拒绝，**不会部分生效**，同一 requestId 修正后可正常提交。

### 3.5 上游失败 / 依赖门控

* 节点只有在“流程 ACTIVE 且所有直接上游均为 `SUCCEEDED` 且到达 `nextAttemptAt`”时才被授权。
* 上游重试预算耗尽进入 `FAILED` 后，其全部上游都已 FAILED/级联CANCELLED 的下游会被
  **级联取消**（`cascadeCancelled=true`），绝不执行；
  多上游节点只要还有健康分支就不会被取消。
* 人工重试让上游恢复后，级联取消的下游会按依赖关系自动重新激活。

---

## 3A. 执行时间预算与长耗时节点

### 3A.1 心跳续约：长任务跑得久 ≠ 失效

* 租约时长 `workflow.leaseTtlMillis`（默认 2000ms）只是“**没有心跳信号时**”的失效判定时长，
  不是节点允许运行的最长时间。
* 长耗时业务节点实现 `HeartbeatCapableNodeTaskHandler`，在循环/轮询等待中定期调用
  `ctx.heartbeat()`：

```java
HeartbeatCapableNodeTaskHandler batch = (nodeId, ctx) -> {
    while (hasMoreBatches()) {
        runOneBatch();                 // 业务推进
        if (!ctx.heartbeat()) {        // 续约失败 = 租约已丢失（被接管/取消/判超时）
            return;                    // 尽快退出，避免与接管者双跑
        }
    }
};
workflowEngine.submit(definition, Map.of("batch", batch));
```

* `heartbeat()` 与结果回交走同一条“实例锁 + owner CAS + 落盘”路径：
  只有“节点仍 RUNNING 且租约属主是自己”才续约成功；
  已被接管/取消/判超时的执行会拿到 `false`，业务应尽早退出。
* 只要业务持续心跳，租约就不会因“跑得久”过期，**同一节点只被真正执行一次**，
  `attempts` 与实际执行次数始终一致。
* 不实现心跳接口的节点行为与引入该能力前**完全一致**（租约到期可被接管）。

### 3A.2 单次执行时间上限（超时预算）

三级配置，优先级从高到低：

| 层级 | 配置位置 | 说明 |
|---|---|---|
| 节点级 | `WorkflowDefinition.nodeExecutionTimeoutMillis[nodeId]` | 单个节点自己的预算，覆盖流程级 |
| 流程级 | `WorkflowDefinition.executionTimeoutMillis` | 整条流程的默认上限 |
| 引擎级 | `workflow.defaultExecutionTimeoutMillis`（默认 `0`） | 流程未配置时的兜底 |

* 生效上限在每次租约授权时固化为 `executionDeadlineAt = 开始时间 + 上限`，
  持久化在节点状态中，重启恢复后仍然有效。
* `0` / 不配置 = **无上限**，行为与引入该能力前完全一致（老流程照常能跑）。
* 心跳续约**不能**把租约续过 `executionDeadlineAt`——上限对长任务同样是硬约束。

### 3A.3 超时判定与迟到结果

* 调度线程扫描发现 `RUNNING` 且 `executionDeadlineAt <= now` → 判定本次尝试**超时失败**：
  中断任务句柄（`Future.cancel(true)`）、清租约，然后**按与业务异常完全相同的规则**落状态：
  重试预算未尽 → `RETRY_WAIT`（固定退避后自动重试）；预算耗尽 → `FAILED`（可人工重试），
  下游按既有规则级联取消。`lastError` 记录 `execution timeout: exceeded budget ...`。
* 判定依据是持久化的 `executionDeadlineAt`，与执行方是否存活无关：
  “执行方拉不动”和“进程没了”同样适用（进程重启后由恢复逻辑 + 扫描接管）。
* 超时判负后，旧执行迟到的结果在回交 CAS 处被拒绝
  （`result REJECTED (stale owner), basis=lease-cas`），
  **不会**把后续尝试已经发生的状态改回去。

### 3A.4 运行中可观测

`GET /api/workflows/{requestId}` 返回的每个节点包含：

| 字段 | 含义 |
|---|---|
| `leaseOwner` | 当前租约属主（`executorId#uuid`），`-`/null 表示无属主 |
| `leaseExpiresAt` | 租约过期时间（心跳会不断顺延） |
| `executionStartedAt` | 本次尝试开始时间 |
| `executionTimeoutMillis` | 本次尝试生效的超时上限（0=无上限） |
| `executionDeadlineAt` | 超时截止时间；**剩余预算 = deadline − 当前时间** |

状态快照日志中运行中节点附带 `budgetLeft=...ms`，便于直接观察剩余预算。

### 3A.5 执行资源说明

长任务持有的是 worker 线程，调度/超时判定/心跳续约都在独立线程进行，
因此个别长任务**不会阻塞**调度循环与其它流程的判定；
但 worker 池容量（`workflow.workerThreads`，默认 4）决定可同时运行的节点数，
请按“预期并发长任务数 + 短任务余量”配置，避免池被长任务占满导致排队。

---

## 4. 重试规则（结果可预期）

* 每个节点的最大自动尝试次数 `maxAttempts`（含首次，≥1），失败后采用**固定退避**
  `nextAttemptAt = now + backoffMillis`；当 `attempts >= maxAttempts` 时返回“不再重试”，
  节点进入 `FAILED`。该计算纯函数化，见 `RetryPolicy`，单元测试逐档验证。
* **人工重试**会把目标节点 `attempts` 重置为 0，给予一套全新的完整预算，并重新激活级联取消的下游。
* 暂停期间到期的重试不会执行；恢复后相关节点重新变为可调度。

---

## 5. 人工干预与状态迁移限制

* 每次成功的 `pause/resume/cancel/retryNode` 都使实例 `version+1`。
  请求必须携带当前版本号；过期版本返回 `STALE_INTERVENTION`（HTTP 409），防止旧指令覆盖新状态。
* `pause`：仅 `ACTIVE` 可暂停；对终态操作返回 `ALREADY_TERMINAL`；重复暂停返回 `ILLEGAL_TRANSITION`。
* `resume`：仅 `PAUSED` 可恢复；终态返回 `ALREADY_TERMINAL`，其他状态返回 `ILLEGAL_TRANSITION`。
* `cancel`：**幂等**——已是 `CANCELLED` 时重复取消直接成功；其他终态返回 `ALREADY_TERMINAL`。
  取消会把等待中节点置 CANCELLED、中断运行中节点并清除租约。
* `retryNode`：只允许对 `FAILED` 节点；对 `SUCCEEDED/CANCELLED` 返回 `ALREADY_TERMINAL`，
  对其他状态返回 `ILLEGAL_TRANSITION`。

### 可区分的错误码 `ErrorCode`

| 错误码 | 场景 | HTTP |
|---|---|---|
| `INVALID_DEFINITION` | requestId 空、maxAttempts<1、backoff<0、空定义 | 400 |
| `DUPLICATE_NODE` | 节点重复声明 / 同一依赖重复列出 | 400 |
| `MISSING_DEPENDENCY` | 依赖指向不存在的节点 | 400 |
| `CYCLE_DETECTED` | 依赖图存在环（含自环） | 400 |
| `WORKFLOW_NOT_FOUND` / `NODE_NOT_FOUND` | 实例或节点不存在 | 404 |
| `ILLEGAL_TRANSITION` | 非法状态转换 | 409 |
| `ALREADY_TERMINAL` | 对已终态实例/节点做不允许的操作 | 409 |
| `STALE_INTERVENTION` | 干预版本号过期 | 409 |

---

## 6. 日志可观测性

引擎在关键判定点输出结构化日志，便于解释每一次状态与租约归属：

* `lease granted: node=..., owner=..., attempts=..., reason=dependencies-ready|lease-expired-takeover, expiresAt=..., timeout=...ms, deadline=...`
* `lease takeover: ... staleOwner=... newOwner=... basis=expiresAt<=...`
* `node=... heartbeat: owner=..., lease extended to ... (deadline=...)`
* `node=... execution TIMEOUT: owner=..., budget=...ms, startedAt=..., now=..., attempts=...`
* `node=... FAILED permanently ... basis=execution-timeout error=execution timeout: exceeded budget ...`
* `node=... heartbeat REJECTED (stale owner): ...`
* `node=... result REJECTED (stale owner): ... basis=lease-cas`
* `node=... attempt #k failed, RETRY_WAIT until ... (backoff=...ms)`
* `workflow state X -> Y (basis=...)`
* 每次状态变更后的快照：`state=... version=... nodes: a=RUNNING(attempts=1,owner=exec-x#y,budgetLeft=...ms) ...`

---

## 7. 本地验证方法

```bash
# 运行全部 43 个用例（并发抢占 / 租约接管 / 进程恢复 / 重复提交 /
# 非法依赖 / 重试上限 / 终态拒绝 / 过期干预 / 长任务心跳 / 超时预算 / REST 全栈）
mvn test

# 只跑某一组
mvn test -Dtest=ConcurrencyAndLeaseTest    # 并发抢占 + 租约超时接管 + 迟到结果 CAS 拒绝
mvn test -Dtest=RecoveryTest               # 模拟崩溃后第二进程从磁盘恢复
mvn test -Dtest=ExecutionAndRetryTest      # DAG 顺序、上游阻断、固定退避、重试上限
mvn test -Dtest=InterventionTest           # 暂停/恢复/取消幂等/人工重试/版本过期
mvn test -Dtest=DefinitionValidationTest   # 定义错误可区分（含非法超时配置）
mvn test -Dtest=LongRunningAndTimeoutTest  # 长耗时心跳续约 / 超时预算 / 迟到结果 / 资源隔离
```

测试特性：

* 全部使用 JUnit 5 + `@TempDir`，**零外部依赖**，可离线一条命令运行；
* 租约/扫描间隔被压到 20–100ms，测试秒级完成；
* 并发用例使用闩锁让多个节点真实同时在不同 worker 上运行，
  租约接管用例故意阻塞旧执行器直到接管发生，再放行旧任务验证其结果被拒绝；
* 恢复用例先构造“节点已 RUNNING 且落盘”的现场，再启动第二个引擎实例验证接管与最终一致；
* 长任务用例让节点真实运行约 8 倍租约时长并周期心跳，验证只执行一次、attempts 不虚高；
* 超时用例覆盖：节点级上限超时后按规则 RETRY_WAIT/FAILED、人工重试恢复、
  流程级默认与节点级覆盖的优先级、超时接手后迟到结果不改写状态、
  长任务不阻塞其它流程、运行中可观测属主与剩余预算；
  每个用例日志打印 `==================== 场景: ... ====================` 与校验结论。

## 8. 主要代码结构

```
model/        NodeState / WorkflowState / WorkflowDefinition（含超时预算）/ NodeExecution / WorkflowInstance
error/        ErrorCode（可区分拒绝原因）、WorkflowException
handler/      NodeTaskHandler（业务节点函数式接口）、
              HeartbeatCapableNodeTaskHandler + NodeExecutionContext（长任务心跳续约）
store/        WorkflowStore、JsonFileWorkflowStore（原子写 + 文件锁 + 启动扫描恢复）
engine/       DefinitionValidator、RetryPolicy、Clock、WorkflowEngine（调度/租约/心跳/超时/重试/干预核心）
config/       EngineProperties、EngineConfiguration（生命周期装配）
web/          WorkflowController（REST + 统一错误码响应）
```
