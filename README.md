# TickSafe

这是一个面向实时系统与调度的实时调度可分析内核与运行时。长期目标是提供任务与优先级模型、抢占式调度、tickless 计时、互斥量与优先级继承、中断延迟测量、最坏执行时间估算和栈深度分析，把实时调度的可分析性沉淀为可复用运行时。

仓库采用 Go，当前冻结基线只提供进程健康检查。后续能力必须通过独立题目逐步实现；每个题目都应定义可观察的公共行为、兼容边界和失败语义，不得依赖未公开内部 API。

## 启动

```bash
go run ./cmd/ticksafe
```

服务默认监听 `127.0.0.1:8080`。可通过 `TICKSAFE_ADDR` 修改监听地址。`GET /healthz` 返回 JSON 健康状态。

## 调度分析

`POST /v1/schedules/analyze` 执行单核固定优先级抢占调度分析。请求需为 `application/json`：

```json
{
  "horizon": 100,
  "tasks": [
    {"id": "A", "priority": 2, "release": 0, "execution": 10, "deadline": 20},
    {"id": "B", "priority": 1, "release": 5, "execution": 10, "deadline": 20}
  ]
}
```

- `horizon`：正整数微秒区间 `[0,horizon)`，不超过 `1e12`。
- 每个任务为一次性任务：`id`（1–64 个非空白 Unicode 码点、唯一）、`priority`（0–255，越小越高）、`release`（区间内整数）、`execution`（正整数）、`deadline`（大于 `release` 的整数）。任务数 1–256。
- 处理器始终运行最高优先级的就绪任务；更高优先级任务到达立即抢占。同优先级按 `release` 先后运行，同时到达按输入顺序，运行中的任务不被同优先级任务抢占。

成功返回 200：`timeline` 按时间升序给出实际执行区间（`taskId`/`start`/`end`），省略空闲时间并合并同一任务的相邻区间；`results` 按输入顺序给出 `executed`、`remaining`、`completion`（未完成时为 `null`）和 `deadlineStatus`（`met`/`missed`/`pending`）。分析不保存任何状态。

错误响应沿用 `{"error":{"code":...}}` 结构：非 POST 返回 405 `method_not_allowed`（含 `Allow: POST`）；非 `application/json` 返回 415 `unsupported_media_type`；语法错误、尾随内容或未知字段返回 400 `invalid_json`；约束错误返回 422 `validation_failed`。错误响应不包含部分结果。

## 周期任务最坏响应时间分析

`POST /v1/schedules/periodic/analyze` 在不展开时间线的前提下，用固定点迭代计算各周期任务的最坏响应时间（WCRT）。模型限定为同步释放、单核、固定优先级、完全抢占、无阻塞、无释放抖动，且 `deadline <= period`。请求需为 `application/json`：

```json
{
  "tasks": [
    {"id": "A", "priority": 2, "execution": 2, "period": 6, "deadline": 6},
    {"id": "B", "priority": 1, "execution": 1, "period": 4, "deadline": 4}
  ]
}
```

- 每个任务包含 `id`（沿用一次性任务的规则且唯一）、`priority`（0–255 唯一整数，越小越高）、`execution`/`period`/`deadline`（不超过 `1e12` 的正整数，且 `deadline <= period`）。任务数 1–256。
- 对任务 $i$，响应时间初值 $w_0 = C_i$，迭代
  $w_{k+1} = C_i + \sum_{j:\,prio(j)<prio(i)}\lceil w_k/T_j\rceil C_j$，
  收敛且不超过 deadline 时，固定点作为 `responseTime`；初值或任一轮结果超过 deadline（含高优先级总利用率 $\sum C_j/T_j \ge 1$ 导致无固定点）时，`responseTime` 为 `null`。计算按 int64 边界饱和处理，溢出即视为超过 deadline，不回绕、不产生 5xx。
- 实现以线性下界 $C_i/(1-U)$ 起跳，规避远距离不动点的伪多项式爆炸；起跳点经高精度算术严格保证不越过最小固定点，结论与逐轮迭代逐字段一致。

成功返回 200：顶层 `schedulable` 仅当所有任务都为 `met` 时为 `true`；`results` 按输入顺序给出 `id`、`responseTime`（未满足时为 `null`）和 `deadlineStatus`（`met`/`missed`）。分析不保存任何状态，相同请求的结果逐字段一致。

该端点的错误语义与 `/v1/schedules/analyze` 完全一致：405 `method_not_allowed`（含 `Allow: POST`）、415 `unsupported_media_type`（允许媒体类型参数）、400 `invalid_json`（语法错误、尾随内容、重复键、未知字段或请求体超过 1 MiB）、422 `validation_failed`（类型不符、字段缺失或约束失败）；错误响应不含部分结果。

## 周期任务展开仿真

`POST /v1/schedules/periodic/simulate` 在区间 `[0,horizon)` 内把周期任务展开为作业，确定性仿真单核固定优先级完全抢占调度。请求需为 `application/json`：

```json
{
  "horizon": 12,
  "tasks": [
    {"id": "A", "priority": 1, "execution": 2, "period": 6, "deadline": 6, "offset": 0},
    {"id": "B", "priority": 0, "execution": 1, "period": 4, "deadline": 4, "offset": 1}
  ]
}
```

- `horizon`：正整数微秒区间 `[0,horizon)`，不超过 `1e12`。
- 任务字段与约束沿用周期 WCRT 入口（`id` 唯一、`priority` 0–255 唯一、`execution`/`period`/`deadline` 为不超过 `1e12` 的正整数且 `deadline <= period`，任务数 1–256），另加 `offset`：满足 `0 <= offset < period` 且 `offset < horizon` 的整数。
- 任务在 `offset + k*period < horizon` 时释放从 0 编号的作业；所有任务释放的作业总数超过 100000 时返回 422 `validation_failed`。
- 每个作业独立执行 `execution` 微秒，绝对截止时间为 `release + deadline`，逾期后继续执行。处理器运行优先级数值最小的就绪作业，更高优先级释放立即抢占；同一任务的作业按编号依次运行，同优先级新作业不抢占。忽略阻塞、抖动与切换开销；空闲时推进到下一次释放，恰在 `horizon` 完成视为完成。

成功返回 200：

- `status`：`completed`（全部已释放作业完成）、`horizon`（到达边界仍有作业未完成）。
- `timeline`：按时间升序给出 `taskId`、`job`、`start`、`end`，省略空闲时间，仅合并同一作业的相邻区间。
- `results`：按任务输入顺序给出 `id` 与 `jobs`（按作业编号排列），每项包含 `job`、`release`、`absoluteDeadline`、`executed`、`remaining`、`completion`（未完成为 `null`）与 `deadlineStatus`（按时完成为 `met`，迟到完成或未完成且截止时间不晚于 `horizon` 为 `missed`，其余未完成为 `pending`）。

仿真不保存任何状态，相同请求的响应逐字段一致。该端点复用现有 JSON 媒体类型、1 MiB 请求体上限、严格解码以及 405、415、400、422 错误语义；错误响应不含部分结果。

## 互斥量与优先级继承分析

`POST /v1/schedules/mutex/analyze` 在单核、区间 `[0,horizon)` 内模拟带优先级继承的互斥量调度。任务字段沿用一次性任务的 `id`、`priority`、`release`、`deadline` 与相同约束（不含 `execution`），并以 `actions` 描述程序：

```json
{
  "horizon": 100,
  "tasks": [
    {"id": "A", "priority": 2, "release": 0, "deadline": 20,
     "actions": [{"lock": "M"}, {"run": 10}, {"unlock": "M"}]},
    {"id": "B", "priority": 0, "release": 3, "deadline": 20,
     "actions": [{"lock": "M"}, {"run": 2}, {"unlock": "M"}]}
  ]
}
```

- `actions` 为 1–1024 项，每项恰有一个动词：`{"run":N}`（正整数微秒）、`{"lock":"M"}`、`{"unlock":"M"}`；互斥量名遵循 `id` 规则。程序必须平衡：不得重复持有同一锁、不得释放未持有的锁、结束时不得仍持锁；所有任务的 `run` 总量不得溢出 int64。以上任一约束失败均返回 422 `validation_failed`。
- `lock`、`unlock` 与任务完成均不耗时；锁被占用时任务阻塞在该锁上。持有者继承其直接与间接等待者的最高（数值最小）优先级，等待关系一变化即重算。
- 解锁时锁的所有权交给有效优先级最高的等待者，并列时按阻塞时刻、再按输入顺序；更高有效优先级的任务在同一时刻立即抢占，同优先级不抢占，首次选择按 `release`、再按输入顺序。
- 状态变化后先排空当前任务连续的零时长动作，再重新调度。等待环首次形成的时刻即判死锁并停止分析；否则运行至全部任务完成或到达 `horizon`。

成功返回 200：

- `status`：`completed`（全部完成）、`horizon`（到达边界仍有任务未完成）、`deadlocked`。
- `timeline`：仅含 `run` 区间，每项给出 `taskId`、`start`、`end`、`effectivePriority`；仅当相邻两区间这四项全部相同时才合并。
- `results`：按输入顺序给出 `executed`、`completion`（未完成为 `null`）、`state`（`completed`/`ready`/`blocked`/`unreleased`）、`blockedOn`（仅阻塞时为锁名，否则 `null`）与 `deadlineStatus`（`met`/`missed`/`pending`，规则同一一次性任务）。
- 死锁时额外给出 `deadlockAt`（成环时刻）与沿等待方向的 `cycle`（taskId 环，从输入顺序最早的环内任务开始）；非死锁时两者均为 `null`。

分析不保存任何状态，相同请求逐字段一致。该端点复用现有 JSON 媒体类型、1 MiB 请求体上限、严格解码以及 405、415、400、422 错误语义；错误响应不含部分结果。

## 有界消息邮箱调度分析

`POST /v1/schedules/mailbox/analyze` 在单核、区间 `[0,horizon)` 内模拟带先进先出消息邮箱的固定优先级抢占调度。任务字段沿用一次性任务的 `id`、`priority`、`release`、`deadline` 与相同约束（不含 `execution`），并额外声明 1–256 个邮箱；任务以 `actions` 描述程序：

```json
{
  "horizon": 100,
  "mailboxes": [{"name": "M", "capacity": 2}],
  "tasks": [
    {"id": "A", "priority": 1, "release": 0, "deadline": 50,
     "actions": [{"send": {"mailbox": "M", "message": "hi"}}, {"run": 3}]},
    {"id": "B", "priority": 0, "release": 1, "deadline": 50,
     "actions": [{"receive": "M"}, {"run": 2}]}
  ]
}
```

- 每个邮箱为 `{"name":..., "capacity":...}`：`name` 遵循 `id` 规则且唯一，`capacity` 为 1–65535 的整数。
- `actions` 为 1–1024 项，每项恰有一个动词：`{"run":N}`（正整数微秒）、`{"send":{"mailbox":"M","message":"..."}}`、`{"receive":"M"}`；`message` 为 1–256 个 Unicode 码点，`send`/`receive` 引用必须指向已声明邮箱。各任务及全局 `run` 总量不得溢出 int64。以上任一约束失败均返回 422 `validation_failed`。
- 邮箱按先进先出缓冲，只有 `run` 耗时。`send` 优先把消息直接交付给最早阻塞在该邮箱上的接收者（同刻按任务输入顺序）；否则有空位就入队，已满则发送者携消息阻塞。`receive` 从非空邮箱取队首；若取出前邮箱已满，最早阻塞发送者的消息立即补到队尾并解除其阻塞；空邮箱使接收者阻塞。通信不改变优先级。
- 状态变化后先排空当前任务连续的零时长动作，再重新调度；更高优先级立即抢占，同优先级按 `release`、再按输入顺序且不抢占。`run` 恰在 `horizon` 结束时，仅处理该任务紧随的零时长动作一次，其他任务不再执行。

成功返回 200：

- `status`：`completed`（全部完成）、`horizon`（到达边界仍有任务未完成）、`stalled`（无后续释放且所有未完成任务均阻塞）；`stoppedAt` 为实际停止时刻。
- `timeline`：仅含 `run` 区间，每项给出 `taskId`、`start`、`end`；省略空闲时间并合并同一任务的相邻区间。
- `results`：按输入顺序给出 `executed`、`completion`（未完成为 `null`）、`state`（`completed`/`ready`/`blocked`/`unreleased`）、`blockedAction` 与 `blockedOn`（阻塞时分别为 `send`/`receive` 与邮箱名，否则均为 `null`）、`received`（按交付顺序记录每次送达的 `mailbox`、`message`、`time`）与 `deadlineStatus`（`met`/`missed`/`pending`，规则同一次性任务）。

分析不保存任何状态，相同请求逐字段一致。该端点复用现有 JSON 媒体类型、1 MiB 请求体上限、严格解码以及 405、415、400、422 错误语义；错误响应不含部分结果。

## 计数信号量调度分析

`POST /v1/schedules/semaphore/analyze` 在单核、区间 `[0,horizon)` 内模拟带计数信号量的固定优先级抢占调度。任务字段沿用一次性任务的 `id`、`priority`、`release`、`deadline` 与相同约束（不含 `execution`），并额外声明 1–256 个信号量；任务以 `actions` 描述程序：

```json
{
  "horizon": 100,
  "semaphores": [{"name": "S", "initial": 1, "maximum": 2}],
  "tasks": [
    {"id": "A", "priority": 0, "release": 0, "deadline": 50,
     "actions": [{"wait": "S"}, {"run": 3}, {"post": "S"}]},
    {"id": "B", "priority": 1, "release": 1, "deadline": 50,
     "actions": [{"wait": "S"}, {"run": 2}, {"post": "S"}]}
  ]
}
```

- 每个信号量为 `{"name":..., "initial":..., "maximum":...}`：`name` 遵循 `id` 规则且唯一，`initial` 为 0–`maximum` 的整数，`maximum` 为 1–65535 的整数。
- `actions` 为 1–1024 项，每项恰有一个动词：`{"run":N}`（正整数微秒）、`{"wait":"S"}`、`{"post":"S"}`；`wait`/`post` 引用必须指向已声明信号量。各任务及全局 `run` 总量不得溢出 int64。以上任一约束失败均返回 422 `validation_failed`。
- 只有 `run` 耗时。`wait` 在计数非零时减一，否则阻塞在该信号量上。`post` 有等待者时把许可直接交付给最早阻塞者（同刻按任务输入顺序），其 `wait` 完成且计数不变；无等待者时计数加一。计数已为 `maximum` 且无等待者时，`post` 不改变计数并触发 overflow，分析当场停止。
- 状态变化后先排空当前任务连续的零时长动作，再重新调度；更高优先级立即抢占，同优先级按 `release`、再按输入顺序且不抢占。无任务可运行时跳到下一 `release`；没有未来 `release` 且仍有阻塞任务时为 stalled。`run` 恰在 `horizon` 结束时，仅处理该任务紧随的零时长动作一次，其他任务不再执行。

成功返回 200：

- `status`：`completed`（全部完成）、`horizon`（到达边界仍有任务未完成）、`stalled`（无后续释放且所有未完成任务均阻塞）、`overflow`；`stoppedAt` 为实际停止时刻。仅 `overflow` 时还返回 `faultAt`（溢出时刻）、`faultTask` 与 `faultSemaphore`。
- `timeline`：仅含 `run` 区间，每项给出 `taskId`、`start`、`end`；省略空闲时间并合并同一任务的相邻区间。
- `results`：按输入顺序给出 `executed`、`completion`（未完成为 `null`）、`state`（`completed`/`ready`/`blocked`/`unreleased`）、`blockedOn`（仅阻塞时为信号量名，否则 `null`）与 `deadlineStatus`（`met`/`missed`/`pending`，规则同一次性任务）。

分析不保存任何状态，相同请求逐字段一致。该端点复用现有 JSON 媒体类型、1 MiB 请求体上限、严格解码以及 405、415、400、422 错误语义；错误响应不含部分结果。

## 中断与不可抢占临界区分析

`POST /v1/schedules/interrupt/analyze` 在单核、区间 `[0,horizon)` 内模拟带中断与不可抢占临界区的固定优先级调度。任务字段沿用一次性任务的 `id`、`priority`、`release`、`deadline` 与相同约束（不含 `execution`），任务以 `actions` 描述程序，另声明 1–4096 个中断：

```json
{
  "horizon": 100,
  "tasks": [
    {"id": "A", "priority": 1, "release": 0, "deadline": 50,
     "actions": [{"run": 2}, {"critical": 5}, {"run": 1}]}
  ],
  "interrupts": [
    {"id": "I1", "priority": 0, "arrival": 3, "execution": 2, "deadline": 20}
  ]
}
```

- `actions` 为 1–1024 项，每项恰有一个动词：`{"run":N}` 或 `{"critical":N}`，`N` 为正整数微秒。每个中断含唯一 `id`（规则同任务 `id`，中断之间唯一）、`priority`（0–255，越小越高）、`arrival`（区间内整数）、`execution`（正整数）与 `deadline`（大于 `arrival`）。各任务动作总量以及全部动作与中断 `execution` 的全局总量不得溢出 int64，溢出返回 422 `validation_failed`。
- 中断一律优先于任务。`critical` 动作从开始到结束（或 `horizon`）不可抢占；`run` 动作可被更高优先级任务或任意中断抢占。中断只被优先级数值更小的中断抢占，同级不抢占，被抢占的中断挂起后恢复；任务间规则同一次性任务。同一时刻先纳入全部到达与释放事件，并列按 `arrival`/`release`、再按输入顺序。

成功返回 200：

- `status`：`completed`（全部任务与中断完成）或 `horizon`（到达边界仍有未完成者）；`stoppedAt` 为完成时刻或 `horizon`。
- `timeline`：每项给出 `actorType`（`task`/`interrupt`）、`actorId`、`mode`（`run`/`critical`）、`start`、`end`；省略空闲时间，仅合并同执行者同模式的相邻区间。
- `taskResults`/`interruptResults`：按各自输入顺序给出 `id`、`executed`、`completion`（未完成为 `null`）与 `deadlineStatus`（`met`/`missed`/`pending`，规则同一次性任务）；中断另含 `start` 与 `latency`（`start - arrival`），未开始时 `start`、`completion`、`latency` 均为 `null`。
- `criticalSections`：按任务输入顺序及零基 `actionIndex` 排列，每项给出 `taskId`、`actionIndex`、`start`、`end`、`observedDuration`、`completed`；未开始的临界区为 `null`、`null`、`0`、`false`，被 `horizon` 截断时 `end` 为 `null`。

分析不保存任何状态，相同请求逐字段一致。该端点复用现有 JSON 媒体类型、1 MiB 请求体上限、严格解码以及 405、415、400、422 错误语义；错误响应不含部分结果。

## 验证

```bash
go test ./...
```

当前基线刻意不包含调度器、同步原语与可分析性度量的实现，以便后续任务从已冻结事实出发独立设计并验证这些能力。
