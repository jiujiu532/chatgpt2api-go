# Technical Design: Provider 权重分配与定时注册

## 1. 数据模型变更

### 1.1 Provider Entry 新增 `weight` 字段

```json
{
  "type": "gptmail",
  "enable": true,
  "weight": 8,
  "api_key": "..."
}
```

- 类型: `int`，范围 1-10，默认值 5
- 存储位置: `register.json` -> `mail.providers[*].weight`

### 1.2 Register Config 新增 `schedule` 对象

```json
{
  "schedule": {
    "enabled": false,
    "start_time": "02:00",
    "end_time": "06:00",
    "threads": 32
  }
}
```

- `enabled`: bool，定时注册总开关
- `start_time`: string，北京时间 HH:MM 格式
- `end_time`: string，北京时间 HH:MM 格式（支持跨日，如 start > end 表示跨午夜）
- `threads`: int，定时任务线程数，正整数

### 1.3 Stats 新增定时任务字段

```json
{
  "stats": {
    "scheduled_status": "idle",
    "scheduled_success": 0,
    "scheduled_fail": 0,
    "scheduled_next_start": "2025-01-15T18:00:00Z",
    "manual_paused": false,
    "manual_paused_reason": "scheduled_preemption"
  }
}
```

- `scheduled_status`: `"idle"` | `"waiting"` | `"running"` | `"ended"`
- `scheduled_next_start`: ISO 时间，下次定时任务启动时间
- `manual_paused`: bool，手动任务是否被抢占暂停
- `manual_paused_reason`: string，暂停原因

---

## 2. 后端变更 (Go)

### 2.1 加权随机选择 - `mail_provider.go`

替换 `selectRegisterMailEntry` 中的 round-robin 逻辑为加权随机选择：

```go
// selectRegisterMailEntry - 当无指定 provider 时使用加权随机
func selectRegisterMailEntry(mailConfig map[string]any, providerName, providerRef string) (map[string]any, error) {
    entries := registerMailEntries(mailConfig)
    enabled := filterEnabled(entries)
    if len(enabled) == 0 {
        return nil, fmt.Errorf("mail.providers has no enabled provider")
    }
    // 精确匹配 providerRef
    if providerRef != "" {
        for _, entry := range entries {
            if util.Clean(entry["provider_ref"]) == providerRef {
                return util.CopyMap(entry), nil
            }
        }
    }
    // 按 type 匹配
    if providerName != "" {
        for _, entry := range enabled {
            if util.Clean(entry["type"]) == providerName {
                return util.CopyMap(entry), nil
            }
        }
    }
    // 单个直接返回
    if len(enabled) == 1 {
        return util.CopyMap(enabled[0]), nil
    }
    // 加权随机选择
    return weightedRandomSelect(enabled), nil
}

func weightedRandomSelect(entries []map[string]any) map[string]any {
    totalWeight := 0
    for _, entry := range entries {
        totalWeight += clampWeight(util.ToInt(entry["weight"], 5))
    }
    r := rand.Intn(totalWeight)
    for _, entry := range entries {
        w := clampWeight(util.ToInt(entry["weight"], 5))
        r -= w
        if r < 0 {
            return util.CopyMap(entry)
        }
    }
    return util.CopyMap(entries[len(entries)-1])
}

func clampWeight(w int) int {
    if w < 1 { return 1 }
    if w > 10 { return 10 }
    return w
}
```

移除全局变量 `registerMailProviderMu` 和 `registerMailProviderSeq`（不再需要 round-robin 状态）。

### 2.2 定时注册配置结构

在 `registerDefaultConfig()` 中新增 schedule 默认值：

```go
func registerDefaultConfig() map[string]any {
    return map[string]any{
        // ... 现有字段 ...
        "schedule": map[string]any{
            "enabled":    false,
            "start_time": "02:00",
            "end_time":   "06:00",
            "threads":    32,
        },
    }
}
```

在 `normalizeRegisterConfig` 中新增 schedule 校验：

```go
schedule := util.StringMap(cfg["schedule"])
schedule["enabled"] = util.ToBool(schedule["enabled"])
schedule["threads"] = maxInt(1, util.ToInt(schedule["threads"], 32))
startTime := util.Clean(schedule["start_time"])
endTime := util.Clean(schedule["end_time"])
if !isValidHHMM(startTime) { startTime = "02:00" }
if !isValidHHMM(endTime) { endTime = "06:00" }
if startTime == endTime {
    // 拒绝相同时间，保持原值或返回错误
}
schedule["start_time"] = startTime
schedule["end_time"] = endTime
cfg["schedule"] = schedule
```

### 2.3 定时调度器 - `register_scheduler.go` (新文件)

```go
package service

// RegisterScheduler 管理定时注册任务的生命周期
type RegisterScheduler struct {
    mu              sync.Mutex
    service         *RegisterService
    stopCh          chan struct{}
    running         bool
    scheduledAlive  bool
    manualPaused    bool
    manualWasAlive  bool
}
```

核心逻辑：

1. **启动时机**: `RegisterService` 初始化时，如果 `schedule.enabled == true`，启动调度器 goroutine
2. **调度循环**: 每 30 秒检查一次当前北京时间是否在时间窗口内
3. **时间判断**: 支持跨日窗口（start > end 时，当前时间 >= start 或 < end 即为窗口内）

```go
func (sch *RegisterScheduler) loop() {
    ticker := time.NewTicker(30 * time.Second)
    defer ticker.Stop()
    for {
        select {
        case <-sch.stopCh:
            return
        case <-ticker.C:
            sch.tick()
        }
    }
}

func (sch *RegisterScheduler) tick() {
    now := nowBeijing()
    cfg := sch.service.getScheduleConfig()
    if !cfg.Enabled {
        return
    }
    inWindow := isInTimeWindow(now, cfg.StartTime, cfg.EndTime)
    preemptTime := subtractMinutes(cfg.StartTime, 3)
    inPreempt := isInTimeWindow(now, preemptTime, cfg.StartTime)

    switch {
    case inPreempt && !sch.manualPaused:
        sch.preemptManualTask()
    case inWindow && !sch.scheduledAlive:
        sch.startScheduledTask()
    case !inWindow && sch.scheduledAlive:
        sch.stopScheduledTask()
        sch.resumeManualTask()
    }
}
```

### 2.4 抢占逻辑

```go
// preemptManualTask 在定时任务开始前 3 分钟暂停手动任务
func (sch *RegisterScheduler) preemptManualTask() {
    sch.mu.Lock()
    defer sch.mu.Unlock()
    if sch.manualPaused {
        return
    }
    if sch.service.isManualRunning() {
        sch.manualWasAlive = true
        sch.service.pauseManualTask() // 设置 enabled=false，等待线程自然结束
        sch.manualPaused = true
        sch.service.updateScheduledStats("manual_paused", true)
        sch.service.notifySSE("manual_paused")
    }
}

// resumeManualTask 定时任务结束后恢复手动任务
func (sch *RegisterScheduler) resumeManualTask() {
    sch.mu.Lock()
    defer sch.mu.Unlock()
    if !sch.manualPaused || !sch.manualWasAlive {
        sch.manualPaused = false
        return
    }
    sch.manualPaused = false
    sch.manualWasAlive = false
    sch.service.resumeManualTask() // 重新 startLocked
    sch.service.updateScheduledStats("manual_paused", false)
    sch.service.notifySSE("manual_resumed")
}
```

暂停机制：设置 `enabled = false`，`run()` 循环检测到后停止提交新 worker，等待已运行 worker 自然完成。

### 2.5 定时任务执行

定时任务复用现有 `run()` 逻辑，但使用 schedule 中的 threads 配置：

```go
func (sch *RegisterScheduler) startScheduledTask() {
    sch.mu.Lock()
    defer sch.mu.Unlock()
    if sch.scheduledAlive {
        return
    }
    sch.scheduledAlive = true
    // 使用 schedule.threads 覆盖 config.threads
    sch.service.startScheduledRun(sch.getScheduleThreads())
    sch.service.updateScheduledStats("scheduled_status", "running")
    sch.service.notifySSE("scheduled_started")
}

func (sch *RegisterScheduler) stopScheduledTask() {
    sch.mu.Lock()
    defer sch.mu.Unlock()
    if !sch.scheduledAlive {
        return
    }
    sch.scheduledAlive = false
    sch.service.stopScheduledRun()
    sch.service.updateScheduledStats("scheduled_status", "ended")
    sch.service.notifySSE("scheduled_ended")
}
```

### 2.6 SSE 事件扩展

现有 SSE 推送通过 `notifyLocked()` 发送完整 snapshot。新增事件类型字段以区分状态变更：

```go
// SSE payload 新增 event_type 字段
func (s *RegisterService) notifyScheduledEvent(eventType string) {
    s.mu.Lock()
    defer s.mu.Unlock()
    snapshot := s.snapshotLocked()
    snapshot["event_type"] = eventType // "scheduled_started", "scheduled_ended", "manual_paused", "manual_resumed"
    data, _ := json.Marshal(snapshot)
    for ch := range s.subscribers {
        select {
        case ch <- string(data):
        default:
        }
    }
}
```

### 2.7 RegisterService 新增方法

```go
// 暴露给 scheduler 的接口
func (s *RegisterService) isManualRunning() bool
func (s *RegisterService) pauseManualTask()
func (s *RegisterService) resumeManualTask()
func (s *RegisterService) startScheduledRun(threads int)
func (s *RegisterService) stopScheduledRun()
func (s *RegisterService) getScheduleConfig() ScheduleConfig
func (s *RegisterService) updateScheduledStats(key string, value any)
```

### 2.8 时间工具函数

```go
func nowBeijing() time.Time {
    loc, _ := time.LoadLocation("Asia/Shanghai")
    return time.Now().In(loc)
}

func isInTimeWindow(now time.Time, start, end string) bool {
    startMin := parseHHMM(start)
    endMin := parseHHMM(end)
    nowMin := now.Hour()*60 + now.Minute()
    if startMin <= endMin {
        // 同日: 02:00 - 06:00
        return nowMin >= startMin && nowMin < endMin
    }
    // 跨日: 23:00 - 02:00
    return nowMin >= startMin || nowMin < endMin
}

func parseHHMM(s string) int {
    parts := strings.Split(s, ":")
    h, _ := strconv.Atoi(parts[0])
    m, _ := strconv.Atoi(parts[1])
    return h*60 + m
}
```

---

## 3. 前端变更 (React/TypeScript)

### 3.1 类型定义扩展 - `api.ts`

```typescript
export type RegisterConfig = {
  // ... 现有字段 ...
  schedule: {
    enabled: boolean;
    start_time: string;
    end_time: string;
    threads: number;
  };
  stats: {
    // ... 现有字段 ...
    scheduled_status?: "idle" | "waiting" | "running" | "ended";
    scheduled_success?: number;
    scheduled_fail?: number;
    scheduled_next_start?: string;
    manual_paused?: boolean;
    manual_paused_reason?: string;
  };
};
```

Provider 类型中新增 weight:

```typescript
// providers 数组元素
{
  type: string;
  enable: boolean;
  weight?: number; // 1-10, 默认 5
  // ... 其他字段
}
```

### 3.2 Provider 卡片新增权重输入 - `register-card.tsx`

在每个 provider 卡片的 grid 区域内新增权重输入控件：

```tsx
<div className="space-y-2">
  <label className="text-sm text-stone-700">权重 (1-10)</label>
  <Input
    type="number"
    min={1}
    max={10}
    value={String(provider.weight ?? 5)}
    onChange={(e) => updateProvider(index, {
      weight: Math.min(10, Math.max(1, Number(e.target.value) || 5))
    })}
    className="h-10 rounded-xl border-stone-200 bg-white"
    disabled={config.enabled}
  />
</div>
```

位置: 放在每个 provider 的类型选择器同行（`md:grid-cols-2` 改为 `md:grid-cols-3`，或在启用 checkbox 行旁边）。

### 3.3 定时注册配置行

在邮箱配置区域（`<h3>邮箱配置</h3>`）上方新增定时注册配置区块：

```tsx
<div className="space-y-3 border-t border-border pt-3">
  <div className="flex items-center justify-between gap-3">
    <h3 className="text-sm font-semibold text-stone-800">定时注册</h3>
    <Checkbox
      checked={config.schedule?.enabled ?? false}
      onCheckedChange={(checked) => setScheduleField("enabled", Boolean(checked))}
      disabled={config.enabled}
    />
  </div>
  <div className="grid gap-4 md:grid-cols-3">
    <div className="space-y-2">
      <label className="text-sm text-stone-700">开始时间 (北京时间)</label>
      <Input
        type="time"
        value={config.schedule?.start_time ?? "02:00"}
        onChange={(e) => setScheduleField("start_time", e.target.value)}
        className="h-10 rounded-xl border-stone-200 bg-white"
        disabled={config.enabled || !config.schedule?.enabled}
      />
    </div>
    <div className="space-y-2">
      <label className="text-sm text-stone-700">结束时间 (北京时间)</label>
      <Input
        type="time"
        value={config.schedule?.end_time ?? "06:00"}
        onChange={(e) => setScheduleField("end_time", e.target.value)}
        className="h-10 rounded-xl border-stone-200 bg-white"
        disabled={config.enabled || !config.schedule?.enabled}
      />
    </div>
    <div className="space-y-2">
      <label className="text-sm text-stone-700">线程数</label>
      <Input
        type="number"
        value={String(config.schedule?.threads ?? 32)}
        onChange={(e) => setScheduleField("threads", Number(e.target.value) || 32)}
        className="h-10 rounded-xl border-stone-200 bg-white"
        disabled={config.enabled || !config.schedule?.enabled}
      />
    </div>
  </div>
</div>
```

### 3.4 定时任务状态展示

在运行结果区域的 Badge 旁边或下方显示定时任务状态：

```tsx
{config.schedule?.enabled && (
  <div className="flex items-center gap-2">
    <Badge variant={
      stats.scheduled_status === "running" ? "success" :
      stats.scheduled_status === "waiting" ? "warning" : "secondary"
    } className="rounded-md">
      {stats.scheduled_status === "running" && "定时运行中"}
      {stats.scheduled_status === "waiting" && `等待启动 ${countdown}`}
      {stats.scheduled_status === "ended" && "今日已结束"}
      {(!stats.scheduled_status || stats.scheduled_status === "idle") && "定时已启用"}
    </Badge>
  </div>
)}
```

### 3.5 手动任务暂停指示器

当 `stats.manual_paused === true` 时，在运行状态 Badge 处显示暂停原因：

```tsx
{stats.manual_paused && (
  <div className="flex items-center gap-2 rounded-md border border-orange-200 bg-orange-50 px-3 py-2 text-xs text-orange-800">
    <AlertTriangle className="size-4 shrink-0" />
    手动任务已暂停（定时任务抢占中），定时任务结束后将自动恢复。
  </div>
)}
```

### 3.6 Store 新增方法

```typescript
// store.ts 新增
setScheduleField: (key: string, value: unknown) => void;
```

实现：

```typescript
setScheduleField: (key, value) => {
  set((state) => state.registerConfig ? {
    registerConfig: {
      ...state.registerConfig,
      schedule: { ...state.registerConfig.schedule, [key]: value },
    },
  } : {});
},
```

`saveRegister` 中新增 schedule 字段提交：

```typescript
schedule: registerConfig.schedule,
```

---

## 4. API 接口变更

### 4.1 GET/POST `/api/register`

响应和请求 body 中新增 `schedule` 字段，结构同 1.2。

### 4.2 SSE `/api/register/events`

推送 payload 新增字段：
- `event_type`: 可选，标识触发原因
- `stats.scheduled_status`
- `stats.manual_paused`

无需新增 endpoint，复用现有 SSE 通道。

---

## 5. 文件变更清单

| 文件 | 变更类型 | 说明 |
|------|----------|------|
| `internal/service/mail_provider.go` | 修改 | `selectRegisterMailEntry` 改为加权随机；新增 `weightedRandomSelect`、`clampWeight` |
| `internal/service/register.go` | 修改 | `registerDefaultConfig` 新增 schedule；`normalizeRegisterConfig` 新增 schedule 校验；新增 pause/resume 方法 |
| `internal/service/register_scheduler.go` | 新增 | 定时调度器，含 loop/tick/preempt/resume 逻辑 |
| `web/src/lib/api.ts` | 修改 | `RegisterConfig` 类型新增 schedule 和 stats 扩展字段 |
| `web/src/app/settings/store.ts` | 修改 | 新增 `setScheduleField`；`saveRegister` 提交 schedule |
| `web/src/app/register/components/register-card.tsx` | 修改 | 新增权重输入、定时配置行、状态展示、暂停指示器 |

---

## 6. 关键设计决策

1. **加权随机 vs 加权轮询**: 选择加权随机，实现简单且在大量请求下统计分布趋近权重比例。无需维护全局状态。

2. **调度器独立 goroutine**: 与现有 `run()` 解耦，通过 channel 和 mutex 协调。避免修改现有 runner 核心循环。

3. **抢占采用优雅停止**: 不强制 kill worker，而是设置 `enabled=false` 让 runner 自然停止提交新任务，等待已有 worker 完成。最多等待一个注册周期（约 30-60s）。

4. **时间基准为北京时间**: 用户输入和展示均为北京时间，后端使用 `Asia/Shanghai` 时区转换。

5. **复用 SSE 通道**: 不新增 endpoint，在现有 snapshot 推送中附加 event_type 和新 stats 字段，前端根据字段变化更新 UI。

6. **定时任务模式固定为 total=无限**: 定时任务在窗口内持续注册直到窗口结束，不设注册总数上限（由时间窗口控制）。
