# Requirements Document

## Introduction

本功能为现有注册机系统添加两项核心能力：邮箱 Provider 权重分配和定时注册服务。通过权重配置，用户可以控制不同邮箱 Provider 的使用比例（如优先消耗额度有限的 gptmail）；通过定时注册，系统可在指定时间窗口内自动执行注册任务，并在与手动注册冲突时通过抢占机制保证定时任务优先执行。

## Glossary

- **Register_Service**: 现有的注册机后端服务，负责管理注册任务的生命周期、线程调度和状态推送
- **Provider**: 邮箱服务提供商实例，如 gptmail、tempmail_lol、duckmail 等，用于创建临时邮箱和接收验证码
- **Weight**: 每个 Provider 的权重值（整数 1-10），决定该 Provider 被选中的概率
- **Weighted_Selector**: 加权随机选择器，根据各 Provider 的 Weight 按比例随机选择 Provider
- **Scheduled_Task**: 定时注册任务，在用户配置的时间窗口内自动启动并执行注册
- **Manual_Task**: 用户通过 UI 手动启动的注册任务
- **Time_Window**: 定时注册的有效执行区间，由开始时间和结束时间（北京时间）定义
- **Preemption**: 抢占机制，定时任务开始前暂停正在运行的手动任务，定时任务结束后恢复手动任务
- **Provider_Pool**: 所有已启用的 Provider 集合，手动注册和定时注册共享同一个 Pool

## Requirements

### Requirement 1: Provider 权重配置

**User Story:** As a 系统管理员, I want to 为每个邮箱 Provider 设置权重值, so that 注册时按权重比例分配 Provider 使用频率，优先消耗额度有限的 Provider。

#### Acceptance Criteria

1. THE Register_Service SHALL 为每个 Provider 配置项支持一个 weight 字段，取值范围为整数 1 到 10，默认值为 5
2. WHEN 用户在前端 Provider 配置卡片中修改 weight 值, THE Register_Service SHALL 持久化该 weight 值到配置文件
3. WHEN 用户未显式设置 weight 值, THE Register_Service SHALL 使用默认权重值 5
4. THE 前端 Provider 配置卡片 SHALL 显示一个数字输入控件，允许用户输入 1 到 10 的整数权重值

### Requirement 2: 加权随机 Provider 选择

**User Story:** As a 系统管理员, I want to 注册任务按权重比例随机选择 Provider, so that 高权重的 Provider 被更频繁地使用。

#### Acceptance Criteria

1. WHEN 注册任务需要选择 Provider, THE Weighted_Selector SHALL 根据所有已启用 Provider 的 weight 值进行加权随机选择
2. THE Weighted_Selector SHALL 保证选择概率与 weight 值成正比（例如 weight=8 的 Provider 被选中概率是 weight=2 的 Provider 的 4 倍）
3. WHEN Provider_Pool 中仅有一个已启用的 Provider, THE Weighted_Selector SHALL 直接返回该 Provider
4. THE Weighted_Selector SHALL 同时应用于 Manual_Task 和 Scheduled_Task 的 Provider 选择

### Requirement 3: 定时注册配置

**User Story:** As a 系统管理员, I want to 配置定时注册的时间窗口和线程数, so that 系统在指定时段自动执行注册任务。

#### Acceptance Criteria

1. THE Register_Service SHALL 支持定时注册配置，包含以下字段：启用开关（enabled）、开始时间（start_time，北京时间 HH:MM 格式）、结束时间（end_time，北京时间 HH:MM 格式）、线程数（threads，正整数）
2. WHEN 用户保存定时注册配置, THE Register_Service SHALL 持久化配置到存储
3. THE 前端 SHALL 在邮箱配置区域上方显示定时注册配置行，包含启用开关、开始时间输入、结束时间输入和线程数输入
4. IF 用户设置的 start_time 等于 end_time, THEN THE Register_Service SHALL 拒绝该配置并返回错误提示
5. THE Register_Service SHALL 将用户输入的北京时间（UTC+8）转换为服务器本地时间进行调度

### Requirement 4: 定时注册自动执行

**User Story:** As a 系统管理员, I want to 定时注册在配置的时间窗口内自动启动, so that 无需人工干预即可在最佳时段执行注册。

#### Acceptance Criteria

1. WHEN 当前北京时间到达 Scheduled_Task 的 start_time, THE Register_Service SHALL 自动启动定时注册任务
2. WHEN 当前北京时间到达 Scheduled_Task 的 end_time, THE Register_Service SHALL 自动停止定时注册任务
3. WHILE Scheduled_Task 正在运行, THE Register_Service SHALL 使用定时注册配置中指定的线程数
4. THE Scheduled_Task SHALL 复用与 Manual_Task 相同的 Provider_Pool 和权重配置
5. WHEN Register_Service 启动时定时注册已启用且当前时间在 Time_Window 内, THE Register_Service SHALL 立即启动定时注册任务
6. THE Scheduled_Task SHALL 每日重复执行（在每个 Time_Window 到达时自动启动）

### Requirement 5: 定时注册抢占机制

**User Story:** As a 系统管理员, I want to 定时注册开始前自动暂停手动注册任务, so that 定时任务获得全部线程资源以最大化注册效率。

#### Acceptance Criteria

1. WHEN Scheduled_Task 的 start_time 前 3 分钟到达且 Manual_Task 正在运行, THE Register_Service SHALL 自动暂停 Manual_Task
2. WHEN Manual_Task 被暂停时, THE Register_Service SHALL 等待当前正在执行的注册线程完成后再释放线程资源
3. WHEN Scheduled_Task 结束后, THE Register_Service SHALL 自动恢复之前被暂停的 Manual_Task
4. WHEN Manual_Task 被暂停或恢复时, THE Register_Service SHALL 通过 SSE 推送状态变更通知前端
5. WHILE Manual_Task 处于暂停状态, THE 前端 SHALL 显示暂停状态标识和暂停原因（定时任务抢占）

### Requirement 6: 定时注册状态展示

**User Story:** As a 系统管理员, I want to 在前端查看定时注册的运行状态, so that 了解定时任务的执行情况和下次执行时间。

#### Acceptance Criteria

1. THE 前端 SHALL 显示定时注册的当前状态：未启用、等待中（显示距下次启动的倒计时）、运行中、已结束
2. WHILE Scheduled_Task 正在运行, THE 前端 SHALL 显示定时任务的实时统计信息（成功数、失败数、运行线程数）
3. WHEN Scheduled_Task 的状态发生变化, THE Register_Service SHALL 通过 SSE 推送更新到前端

### Requirement 7: 跨日时间窗口支持

**User Story:** As a 系统管理员, I want to 支持跨越午夜的时间窗口配置, so that 可以设置如 23:00 到 02:00 的注册时段。

#### Acceptance Criteria

1. WHEN start_time 大于 end_time（如 start_time=23:00, end_time=02:00）, THE Register_Service SHALL 将其解释为跨日时间窗口
2. WHEN 当前时间处于跨日时间窗口内（即当前时间 >= start_time 或当前时间 < end_time）, THE Register_Service SHALL 正常触发定时注册任务
