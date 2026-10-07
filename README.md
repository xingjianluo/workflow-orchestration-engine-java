# 本地工作流编排引擎（Spring Boot）

一个可在**单机多线程**运行、**重启后可恢复**的本地工作流编排服务。
目标：流程提交、执行、重试、人工干预后的最终状态**始终可解释且一致**——
同一节点在多线程抢占或进程短暂重启恢复期间，最多只被**实际提交执行结果一次**；
租约过期后可以被安全接管而不是双跑；上游失败时下游绝不执行。

不依赖任何外部服务（无数据库 / 无消息队列），状态以 JSON 文件落盘。

---

## 1. 快速开始

```bash
# 一条命令运行全部测试（编译 + 38 个测试用例，含并发/恢复/租约/执行预算场景）
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

# 可选：配置执行时长预算——流程级默认 10 分钟，节点 b 单独覆盖为 30 分钟
curl -X POST localhost:8080/api/workflows \
  -H 'Content-Type: application/json' \
  -d '{"requestId":"order-2","maxAttempts":3,"backoffMillis":500,
       "dependencies":{"a":[],"b":["a"]},
       "executionTimeoutMillis":600000,"nodeTimeouts":{"b":1800000}}'

# 查询实例状态（含每个节点状态、尝试次数、租约属主、执行预算窗口）
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

### 3.6 长耗时节点：租约续约（心跳）

* 节点执行可能远超租约 TTL（`workflow.lease-ttl-millis`，默认 2000ms）。
  调度器每轮扫描时，对**本执行器持有、任务确实还在 worker 上运行**（Future 未完成）
  且剩余租约不足半个 TTL 的节点**自动续约**（`lease renewed` 日志），
  因此“跑得久”本身永远不会被误判为卡死/失效，不会被重新拉起、重复执行。
* 续约只在调度线程存活时发生：进程卡死 / GC 长停顿 / 执行器崩溃时续约停止，
  租约自然过期，节点按 3.2 的接管语义被安全接管——活跃性与安全性兼得。
* 最终一致性不变：同一节点“实际提交结果”仍最多一次（CAS 保证），
  长任务场景的 `attempts` 不会因误判接管而虚高。

### 3.7 执行时长预算（超时上限）

* 每个节点有一次执行的**时长预算** `executionTimeoutMillis`，生效优先级：
  **节点级 `nodeTimeouts[nodeId]` > 流程级 `executionTimeoutMillis` > 引擎默认
  `workflow.default-execution-timeout-millis`**。`0` 或不配置 = 不限制，
  **与引入预算前的行为完全一致**（老流程照常能跑）。
* 授权租约时记录本次尝试窗口：`executionStartedAt=now`、
  `executionDeadlineAt=now+预算`（无预算则为 0）；续约不会越过 deadline。
* 扫描发现 `RUNNING` 且超过 `executionDeadlineAt` 仍未结束 → 判**执行超时失效**：
  中断任务句柄、清租约，然后**完全按既有失败/重试规则落态**——
  预算内还有次数则 `RETRY_WAIT`（固定退避后自动重试），
  次数耗尽则 `FAILED` 并级联取消下游，可人工重试（`retryNode` 重置整套预算）。
* 超时失效后被新尝试接手时，旧执行方（拉不动/进程没了又复活）**迟到回交的结果
  会在 complete 的 CAS 处被拒绝**，不会把后续已发生的状态改回去。
* 超时失败原因写入 `lastError`（`execution timeout: exceeded budget ...ms`），可解释。

### 3.8 执行资源隔离

* worker 池为核心 `workflow.worker-threads`（默认 4）+ 弹性上限
  `workflow.max-worker-threads`（默认 64）：个别长耗时节点占住核心线程时，
  其它流程/其它节点的可执行任务通过扩容线程照常推进，不会被顶住。

### 3.9 运行中节点的可查询字段

`GET /api/workflows/{requestId}` 返回的每个节点包含：

| 字段 | 含义 |
|---|---|
| `leaseOwner` | 当前租约属主（`executorId#uuid`），运行中可看出归谁执行 |
| `leaseExpiresAt` | 租约到期时刻（续约会推进该值） |
| `executionTimeoutMillis` | 该节点生效的执行预算（0=不限） |
| `executionStartedAt` | 本次尝试开始时刻 |
| `executionDeadlineAt` | 本次尝试截止时刻；`deadline - now` 即剩余时间预算 |

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

* `lease granted: node=..., owner=..., attempts=..., reason=dependencies-ready|lease-expired-takeover, expiresAt=..., budget=...ms, deadline=...`
* `lease renewed: node=..., owner=..., expiresAt=..., deadline=...`（长任务自动续约）
* `lease takeover: ... staleOwner=... newOwner=... basis=expiresAt<=...`
* `node=... execution TIMEOUT (attempt #k), RETRY_WAIT until ...; execution timeout: exceeded budget ...ms`
* `node=... execution TIMEOUT, attempts=k/n exhausted -> FAILED; ...`
* `node=... result REJECTED (stale owner): ... basis=lease-cas`
* `node=... attempt #k failed, RETRY_WAIT until ... (backoff=...ms)`
* `workflow state X -> Y (basis=...)`
* 每次状态变更后的快照：`state=... version=... nodes: a=RUNNING(attempts=1,owner=exec-x, budgetLeft=850ms) ...`

---

## 7. 本地验证方法

```bash
# 运行全部 38 个用例（并发抢占 / 租约接管 / 进程恢复 / 重复提交 /
# 非法依赖 / 重试上限 / 终态拒绝 / 过期干预 / 执行预算与长任务 / REST 全栈）
mvn test

# 只跑某一组
mvn test -Dtest=ExecutionBudgetTest       # 长任务只执行一次 / 超时按规则失败重试 / 迟到结果拒绝 / 预算层级 / 资源隔离
mvn test -Dtest=ConcurrencyAndLeaseTest   # 并发抢占 + 租约超时接管 + 迟到结果 CAS 拒绝
mvn test -Dtest=RecoveryTest              # 模拟崩溃后第二进程从磁盘恢复
mvn test -Dtest=ExecutionAndRetryTest     # DAG 顺序、上游阻断、固定退避、重试上限
mvn test -Dtest=InterventionTest          # 暂停/恢复/取消幂等/人工重试/版本过期
mvn test -Dtest=DefinitionValidationTest  # 四类定义错误可区分
```

测试特性：

* 全部使用 JUnit 5 + `@TempDir`，**零外部依赖**，可离线一条命令运行；
* 租约/扫描间隔被压到 20–100ms，执行预算压到 300ms 级，测试秒级完成；
* 并发用例使用闩锁让多个节点真实同时在不同 worker 上运行，
  租约接管用例故意阻塞旧执行器直到接管发生，再放行旧任务验证其结果被拒绝；
* 长任务用例让节点真实执行远超租约 TTL 的时长，验证续约期间不被接管、只执行一次；
* 超时用例用“不响应中断的执行方”模拟拉不动的任务，验证超时判失效后
  迟到结果被 CAS 拒绝、状态不回退；
* 每个预算用例在日志中打印 `=== 场景N：... ===` 说明覆盖的业务场景与结果；
* 恢复用例先构造“节点已 RUNNING 且落盘”的现场，再启动第二个引擎实例验证接管与最终一致。

## 8. 主要代码结构

```
model/        NodeState / WorkflowState / WorkflowDefinition / NodeExecution / WorkflowInstance
error/        ErrorCode（可区分拒绝原因）、WorkflowException
handler/      NodeTaskHandler（业务节点函数式接口）
store/        WorkflowStore、JsonFileWorkflowStore（原子写 + 文件锁 + 启动扫描恢复）
engine/       DefinitionValidator、RetryPolicy、Clock、WorkflowEngine（调度/租约/重试/干预核心）
config/       EngineProperties、EngineConfiguration（生命周期装配）
web/          WorkflowController（REST + 统一错误码响应）
```
