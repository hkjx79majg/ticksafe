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

### 周期任务最坏响应时间分析

`POST /v1/schedules/periodic/analyze` 在不展开时间线的前提下，用固定点迭代给出周期任务集的最坏响应时间（RTA）。分析模型限定为同步释放、单核、固定优先级、完全抢占、无阻塞且无释放抖动，服务不保存任何状态。请求需为 `application/json`：

```json
{
  "tasks": [
    {"id": "A", "priority": 1, "execution": 3, "period": 10, "deadline": 10},
    {"id": "B", "priority": 0, "execution": 1, "period": 4, "deadline": 4}
  ]
}
```

- `tasks` 数量为 1–256。每项包含 `id`（沿用一次性任务规则且唯一）、`priority`（0–255 的唯一整数，数值越小优先级越高）、`execution`、`period`、`deadline`（均为不超过 `1e12` 的正整数，且 `deadline ≤ period`）。
- 每个任务的响应时间从 `w0 = execution` 开始迭代：`w = execution + Σ ceil(w/period_j) · execution_j`（仅累加严格更高优先级任务的干扰），直到相邻两轮相等；收敛值不超过 `deadline` 时作为 `responseTime`。初值或任一轮结果超过 `deadline`，或中间结果超出 int64 时，`responseTime` 为 `null`。

成功返回 200：`results` 按输入顺序给出 `id`、`responseTime`（未满足时为 `null`）和 `deadlineStatus`（`met`/`missed`）；仅当所有任务均为 `met` 时顶层 `schedulable` 为 `true`。相同请求的结果逐字段一致。

错误语义与 `/v1/schedules/analyze` 一致：非 POST 返回 405 `method_not_allowed`（含 `Allow: POST`）；缺少媒体类型或非 `application/json`（允许媒体类型参数）返回 415 `unsupported_media_type`；JSON 语法错误、尾随内容、重复键、未知字段或请求体超过 1 MiB 返回 400 `invalid_json`；类型不符、字段缺失或约束失败返回 422 `validation_failed`。错误响应不包含部分结果。

## 验证

```bash
go test ./...
```

当前基线刻意不包含调度器、同步原语与可分析性度量的实现，以便后续任务从已冻结事实出发独立设计并验证这些能力。
