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

## 验证

```bash
go test ./...
```

当前基线刻意不包含调度器、同步原语与可分析性度量的实现，以便后续任务从已冻结事实出发独立设计并验证这些能力。
