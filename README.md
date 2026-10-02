# TickSafe

这是一个面向实时系统与调度的实时调度可分析内核与运行时。长期目标是提供任务与优先级模型、抢占式调度、tickless 计时、互斥量与优先级继承、中断延迟测量、最坏执行时间估算和栈深度分析，把实时调度的可分析性沉淀为可复用运行时。

仓库采用 Go，当前冻结基线只提供进程健康检查。后续能力必须通过独立题目逐步实现；每个题目都应定义可观察的公共行为、兼容边界和失败语义，不得依赖未公开内部 API。

## 启动

```bash
go run ./cmd/ticksafe
```

服务默认监听 `127.0.0.1:8080`。可通过 `TICKSAFE_ADDR` 修改监听地址。`GET /healthz` 返回 JSON 健康状态。

## 调度分析

`POST /v1/schedules/analyze` 接收 `application/json` 请求体，对单核固定优先级抢占调度在 `[0,horizon)` 微秒区间内做确定性仿真：

```json
{
  "horizon": 100,
  "tasks": [
    {"id": "a", "priority": 1, "release": 0, "execution": 10, "deadline": 20}
  ]
}
```

`priority` 数值越小优先级越高；同优先级按 release 先后、再按输入顺序运行，运行中的任务不被同优先级任务抢占。成功响应为 200，`timeline` 给出合并后的实际执行区间（省略空闲），`results` 按输入顺序给出 `executed`、`remaining`、`completion`（未完成为 `null`）与 `deadlineStatus`（`met` / `missed` / `pending`）。错误响应统一为 `{"error":{"code":...}}`：非 POST 为 405，非 JSON 为 415，JSON 语法/尾随内容/未知字段为 400，约束违例为 422。

## 验证

```bash
go test ./...
```

当前基线刻意不包含调度器、同步原语与可分析性度量的实现，以便后续任务从已冻结事实出发独立设计并验证这些能力。
