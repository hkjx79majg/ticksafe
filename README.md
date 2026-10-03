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

## 验证

```bash
go test ./...
```

当前基线刻意不包含调度器、同步原语与可分析性度量的实现，以便后续任务从已冻结事实出发独立设计并验证这些能力。
