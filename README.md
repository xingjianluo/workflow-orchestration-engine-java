# 本地工作流编排引擎（Spring Boot）

在单机、多线程环境下提供可持久化、重启可恢复的本地工作流编排能力：流程提交后按 DAG
依赖自动调度执行，支持失败自动重试、暂停/恢复/取消与人工重试，并通过**租约协议**保证
同一节点在多线程抢占、进程短暂重启等情况下**最多被实际执行一次**、租约过期后**可安全
接管而非双跑**。不依赖任何外部服务（无数据库、无 MQ），状态保存在本地文件中。

## 1. 快速开始

```bash
mvn test          # 一条命令运行全部 28 个本地测试（无需任何外部服务）
mvn spring-boot:run
```

默认数据目录为 `target/workflow-data/`，可用 `workflow.base-dir` 修改。

## 2. 流程生命周期

### 2.1 定义与提交

- 流程定义为一张 DAG：每个节点有 `id`、`type`（对应注册的 `TaskHandler`）、`dependsOn` 与可选 `maxAttempts`。
- 提交必须携带 **idempotencyKey（业务幂等键）**：
  - 定义先经过完整校验，**任何一项不合法都不会创建实例（不会部分生效）**；
  - 同一幂等键重复提交（包括多线程并发提交）只会返回同一个实例，绝不产生重复实例。
- 定义校验返回三类可区分错误码：

| 错误码 | 含义 |
| --- | --- |
| `DUPLICATE_NODE` | 存在重复节点 id / 重复声明的依赖 |
| `MISSING_DEPENDENCY` | 依赖了不存在的节点 |
| `CYCLIC_DEPENDENCY` | 依赖中存在环（含自依赖），错误信息给出环路 |
| `DUPLICATE_SUBMISSION` | 同一业务请求重复提交（接口语义：返回既有实例，不抛错） |

### 2.2 节点状态机

```
PENDING ──抢占租约──▶ RUNNING ──成功回传──────────────▶ SUCCEEDED（终态）
   ▲                    │
   │                    ├──失败且仍有重试额度──▶ WAITING（退避到期后）──抢占租约──▶ RUNNING
   │                    │
   │                    └──失败且重试耗尽────▶ FAILED（终态）
   │
   ├─ 上游终态失败且本节点尚未执行/等待 ─▶ SKIPPED（终态，可人工重试）
   └─ 流程被取消 / 暂停时在执行中       ─▶ CANCELLED（取消，终态）/ 回 PENDING（暂停，恢复后重跑）

RUNNING + 租约过期 ──（恢复扫描或调度扫描）──▶ PENDING（重新参与调度，不消耗重试额度）
```

- **暂停**会冻结整个流程：暂停期间不派发新节点；已在执行中的节点租约作废、回到 `PENDING`
  （attempts 回退一次，不消耗自动重试额度），其执行线程的迟到回传以 `LEASE_LOST` 丢弃，恢复后重新执行。
- **取消**会把所有非终态节点（含 RUNNING）置为 `CANCELLED` 并作废租约，迟到回传同样丢弃。

- 上游失败时下游**绝不执行**：未开始/等待中的下游被标记 `SKIPPED`；正在运行的节点待其回传或租约过期后收敛。
- 重试次数达到节点 `maxAttempts`（默认 `workflow.default-max-attempts=3`）后节点进入 `FAILED`。

### 2.3 流程状态机

```
RUNNING ◀──resume── PAUSED
   │  └──pause──────▶ PAUSED
   ├──全部节点 SUCCEEDED────────────▶ SUCCEEDED（终态）
   ├──任一节点 FAILED / 有 SKIPPED 残留─▶ FAILED（终态）
   └──cancel──────────────────────▶ CANCELLED（终态）
```

- 存在 `SKIPPED` 残留（例如上游被修好并人工重试成功）时流程保持 `FAILED`，提示需对下游逐个
  人工重试；全部节点 `SUCCEEDED` 后才收敛为 `SUCCEEDED`。
- 流程 `FAILED` 不会阻塞人工重试：对 `FAILED`/`SKIPPED` 节点执行人工重试会让流程回到 `RUNNING`。

## 3. 重试与退避规则（结果可预期）

- 自动退避在失败回传的同一原子事务中固化为 `nextRetryAt`：
  `backoff(n) = workflow.base-backoff-millis × workflow.backoff-multiplier^(n-1)`
  （默认 1000ms、倍数 2 → 1000、2000、4000……）。
- 退避未到期的节点不会被派发，时间边界严格按 `nextRetryAt <= now` 判定。
- **人工重试**：仅 `FAILED`、`SKIPPED` 节点允许；立即重置为 `PENDING`、清空退避与自动重试
  计数（`attempts=0`），无需等待退避。`SUCCEEDED`/`CANCELLED` 节点以 `ALREADY_TERMINAL` 拒绝。

## 4. 并发一致性规则（租约协议）

节点租约是状态的一部分（随实例一起落盘），包含 `token`、`leaseOwner`、`leaseExpiresAt`：

1. **抢占**：只有 `PENDING`/退避到期的 `WAITING`/租约已过期的 `RUNNING` 节点可被抢占；
   抢占在实例级原子事务（per-instance 锁 + 原子文件落盘）中完成，因此多线程同时抢占同一节点，
   **只有一方成功**。成功抢占会生成新的随机 `token`。
2. **执行与心跳**：执行期间按 `workflow.heartbeat-interval-millis` 续租；任务也可通过
   `NodeExecutionContext.heartbeat()` 自行探测，租约被接管时应主动中止。
3. **结果回传**：成功/失败回传都必须携带 `token`，仅当存储中的 token 与之一致才生效；
   旧持有者的迟到回传得到 `LEASE_LOST` 并被丢弃，**绝不会覆盖接管者的状态（防双跑）**。
4. **过期接管**：租约超过 `workflow.lease-ttl-millis` 未续租即视为持有者死亡，其它线程/重启后的
   新进程可颁发新 token 安全接管。
5. **重启恢复**：启动时先扫描全部实例——已过期的 RUNNING 租约回收为 `PENDING`（attempts 回退一次，
   租约失效不产生失败回传，不消耗自动重试额度）；仍在有效期内的租约保持 `RUNNING`（短暂重启不接管、
   不双跑），等其自然到期后再接管。

存储层：每个实例一个文件（`instances/<id>.dat`，tmp + 原子 rename），幂等键索引
（`keys.idx`）先行落盘，提交过程崩溃后重启会自动收敛为唯一实例。

## 5. 人工干预与状态迁移限制

所有干预接口都可携带 `expectedVersion`（乐观锁，等于提交/每次变更后返回的 `version`）：

| 场景 | 结果 / 错误码 |
| --- | --- |
| 暂停一个 `RUNNING` 流程 | `PAUSED`，暂停期间不派发新节点 |
| 重复暂停 / 对 `RUNNING` 执行 resume | `ILLEGAL_TRANSITION` |
| 对终态流程暂停/恢复/取消/重试 | `ALREADY_TERMINAL`（取消除外，见下） |
| **取消** | `RUNNING/PAUSED → CANCELLED`；对已 `CANCELLED` 重复取消**幂等成功**；对 `SUCCEEDED/FAILED` 取消返回 `ALREADY_TERMINAL` |
| 人工重试非 FAILED/SKIPPED 节点 | 运行中 → `ILLEGAL_TRANSITION`；SUCCEEDED/CANCELLED → `ALREADY_TERMINAL` |
| 携带的 `expectedVersion` 与当前版本不符 | `STALE_INTERVENTION`（过期干预被拒绝，错误信息含 expected/actual） |
| 实例 / 节点不存在 | `INSTANCE_NOT_FOUND` |

REST 层错误码映射：定义类 → 400，冲突类（重复提交/非法转换/终态/过期干预）→ 409，不存在 → 404，
响应体形如 `{"errorCode":"STALE_INTERVENTION","message":"..."}`。

## 6. REST API

```
POST   /api/workflows                                提交（body: idempotencyKey + definition）
GET    /api/workflows/{instanceId}                   查询实例（含每个节点状态、租约、attempts、退避）
POST   /api/workflows/{instanceId}/pause             暂停     (body 可选: {"expectedVersion":1})
POST   /api/workflows/{instanceId}/resume            恢复
POST   /api/workflows/{instanceId}/cancel            取消（幂等）
POST   /api/workflows/{instanceId}/nodes/{nodeId}/retry   人工重试
```

业务任务通过 `TaskHandlerRegistry#register(type, handler)` 注册；`TaskHandler` 收到
`NodeExecutionContext`，可读取 instanceId/nodeId/attempt 并调用 `heartbeat()`。

## 7. 本地验证

```bash
mvn test
```

测试全部基于 JUnit 5、JDK 本地文件系统与内存线程池，`@TempDir` 隔离数据，无外部依赖。
覆盖场景：

- `DefinitionValidationTest`：重复节点、缺失依赖、环（含自依赖）、非法定义不落盘、合法 DAG；
- `SubmissionIdempotencyTest`：串行/16 线程并发重复提交只产生 1 个实例、不同业务键隔离；
- `LeaseConcurrencyTest`：24 线程抢占唯一租约、有效租约不可抢、过期接管与旧 token 回传作废、
  上游失败下游 SKIPPED；
- `RetryAndBackoffTest`：退避严格为 100/200ms、退避未到不派发、重试上限终态、人工重试清零立即执行；
- `InterventionTest`：暂停/恢复（含在执行节点冻结与迟到回传作废）、取消幂等/取消在执行节点/终态拒绝、
  非法转换、过期版本拒绝、节点重试边界；
- `RecoveryTest`：重启回收过期租约并续跑、ttl 内快速重启不双跑、退避/attempts/version 跨重启恢复；
- `SchedulerTakeoverTest`：真实调度循环 + 心跳，僵死 worker 租约过期后被自动接管完成；
- `EndToEndExecutionTest`：真实线程池端到端，菱形 DAG 每节点恰好执行一次、失败两次后第三次成功。

运行时日志（测试控制台与应用日志）会持续打印：流程整体状态与版本、每个节点的
`status/attempts/leaseOwner/租约剩余时间/下次重试时间`、以及每次决策的判定依据
（"存在有效租约，绝不双跑"、"旧租约已于 … 过期，新 owner=… 接管"、"上游失败传播 → SKIPPED" 等）。

## 8. 主要配置项

| 配置 | 默认值 | 说明 |
| --- | --- | --- |
| `workflow.base-dir` | `target/workflow-data` | 本地状态目录 |
| `workflow.lease-ttl-millis` | 10000 | 租约有效期；业务执行时长应小于该值（靠心跳续租） |
| `workflow.heartbeat-interval-millis` | 2000 | 执行期间续租间隔 |
| `workflow.base-backoff-millis` / `workflow.backoff-multiplier` | 1000 / 2 | 自动重试退避基数与倍数 |
| `workflow.default-max-attempts` | 3 | 节点默认最大尝试次数（节点定义可覆盖） |
| `workflow.scheduler-tick-millis` | 200 | 调度扫描周期 |
| `workflow.worker-threads` | 4 | 节点执行线程池大小 |
