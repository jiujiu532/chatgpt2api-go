package service

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"chatgpt2api/internal/util"
)

// RegisterScheduler 管理定时注册任务的生命周期
type RegisterScheduler struct {
	mu             sync.Mutex
	service        *RegisterService
	stopCh         chan struct{}
	running        bool
	scheduledAlive bool
	manualPaused   bool
	manualWasAlive bool
}

// newRegisterScheduler 创建定时调度器
func newRegisterScheduler(service *RegisterService) *RegisterScheduler {
	return &RegisterScheduler{
		service: service,
		stopCh:  make(chan struct{}),
	}
}

// Start 启动调度器后台循环
func (sch *RegisterScheduler) Start() {
	sch.mu.Lock()
	defer sch.mu.Unlock()
	if sch.running {
		return
	}
	sch.running = true
	sch.stopCh = make(chan struct{})
	go sch.loop()
}

// Stop 停止调度器
func (sch *RegisterScheduler) Stop() {
	sch.mu.Lock()
	defer sch.mu.Unlock()
	if !sch.running {
		return
	}
	sch.running = false
	close(sch.stopCh)
}

// Reload 配置变更时重新加载
func (sch *RegisterScheduler) Reload() {
	// 调度器循环会自动读取最新配置，无需特殊处理
}

func (sch *RegisterScheduler) loop() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	// 启动时立即检查一次
	sch.tick()
	for {
		select {
		case <-sch.stopCh:
			// 停止时清理定时任务
			if sch.scheduledAlive {
				sch.stopScheduledTask()
			}
			if sch.manualPaused {
				sch.resumeManualTask()
			}
			return
		case <-ticker.C:
			sch.tick()
		}
	}
}

func (sch *RegisterScheduler) tick() {
	cfg := sch.getScheduleConfig()
	if !cfg.enabled {
		// 定时注册未启用，如果之前有运行中的定时任务则停止
		if sch.scheduledAlive {
			sch.stopScheduledTask()
		}
		if sch.manualPaused {
			sch.resumeManualTask()
		}
		return
	}

	now := nowBeijing()
	inWindow := isInTimeWindow(now, cfg.startTime, cfg.endTime)
	preemptStart := subtractMinutesHHMM(cfg.startTime, 3)
	inPreempt := isInTimeWindow(now, preemptStart, cfg.startTime) && !inWindow

	switch {
	case inPreempt && !sch.manualPaused:
		// 定时任务开始前 3 分钟，抢占手动任务
		sch.preemptManualTask()
	case inWindow && !sch.scheduledAlive:
		// 进入时间窗口，启动定时任务
		if !sch.manualPaused {
			sch.preemptManualTask()
		}
		sch.startScheduledTask(cfg.threads)
	case !inWindow && !inPreempt && sch.scheduledAlive:
		// 离开时间窗口，停止定时任务
		sch.stopScheduledTask()
		sch.resumeManualTask()
	case !inWindow && !inPreempt && sch.manualPaused && !sch.scheduledAlive:
		// 不在窗口也不在抢占期，但手动任务仍被暂停（异常恢复）
		sch.resumeManualTask()
	}
}

func (sch *RegisterScheduler) preemptManualTask() {
	sch.mu.Lock()
	defer sch.mu.Unlock()
	if sch.manualPaused {
		return
	}
	sch.service.mu.Lock()
	wasRunning := sch.service.runnerAlive && util.ToBool(sch.service.config["enabled"])
	if wasRunning {
		sch.service.config["enabled"] = false
		sch.service.appendLogLocked("定时注册即将开始，暂停手动注册任务", "yellow")
		sch.service.saveLocked()
		sch.service.notifyLocked()
	}
	sch.service.mu.Unlock()

	sch.manualWasAlive = wasRunning
	sch.manualPaused = true

	// 更新 stats 中的暂停状态
	sch.service.bumpStats(map[string]any{
		"manual_paused":        true,
		"manual_paused_reason": "scheduled_preemption",
		"scheduled_status":     "waiting",
	})
}

func (sch *RegisterScheduler) startScheduledTask(threads int) {
	sch.mu.Lock()
	defer sch.mu.Unlock()
	if sch.scheduledAlive {
		return
	}
	sch.scheduledAlive = true

	// 等待手动任务的 runner 自然结束（最多等 60 秒）
	deadline := time.Now().Add(60 * time.Second)
	for {
		sch.service.mu.Lock()
		alive := sch.service.runnerAlive
		sch.service.mu.Unlock()
		if !alive || time.Now().After(deadline) {
			break
		}
		time.Sleep(1 * time.Second)
	}

	// 启动定时注册任务
	sch.service.mu.Lock()
	sch.service.config["enabled"] = true
	originalThreads := sch.service.config["threads"]
	sch.service.config["threads"] = threads
	sch.service.config["_scheduled_original_threads"] = originalThreads
	sch.service.config["_scheduled_running"] = true
	sch.service.appendLogLocked(fmt.Sprintf("定时注册任务启动，线程数=%d", threads), "green")
	sch.service.startLocked(false)
	sch.service.mu.Unlock()

	sch.service.bumpStats(map[string]any{
		"scheduled_status": "running",
	})
}

func (sch *RegisterScheduler) stopScheduledTask() {
	sch.mu.Lock()
	defer sch.mu.Unlock()
	if !sch.scheduledAlive {
		return
	}
	sch.scheduledAlive = false

	sch.service.mu.Lock()
	sch.service.config["enabled"] = false
	sch.service.config["_scheduled_running"] = false
	// 恢复原始线程数
	if original := sch.service.config["_scheduled_original_threads"]; original != nil {
		sch.service.config["threads"] = original
		delete(sch.service.config, "_scheduled_original_threads")
	}
	sch.service.appendLogLocked("定时注册任务结束", "yellow")
	sch.service.saveLocked()
	sch.service.notifyLocked()
	sch.service.mu.Unlock()

	sch.service.bumpStats(map[string]any{
		"scheduled_status": "ended",
	})
}

func (sch *RegisterScheduler) resumeManualTask() {
	sch.mu.Lock()
	defer sch.mu.Unlock()
	if !sch.manualPaused {
		return
	}
	wasAlive := sch.manualWasAlive
	sch.manualPaused = false
	sch.manualWasAlive = false

	// 等待定时任务 runner 结束
	deadline := time.Now().Add(60 * time.Second)
	for {
		sch.service.mu.Lock()
		alive := sch.service.runnerAlive
		sch.service.mu.Unlock()
		if !alive || time.Now().After(deadline) {
			break
		}
		time.Sleep(1 * time.Second)
	}

	if wasAlive {
		sch.service.mu.Lock()
		sch.service.appendLogLocked("定时注册结束，恢复手动注册任务", "green")
		sch.service.startLocked(false)
		sch.service.mu.Unlock()
	}

	sch.service.bumpStats(map[string]any{
		"manual_paused":        false,
		"manual_paused_reason": "",
		"scheduled_status":     "idle",
	})
}

func (sch *RegisterScheduler) getScheduleConfig() scheduleConfig {
	sch.service.mu.Lock()
	defer sch.service.mu.Unlock()
	raw := util.StringMap(sch.service.config["schedule"])
	return scheduleConfig{
		enabled:   util.ToBool(raw["enabled"]),
		startTime: util.Clean(raw["start_time"]),
		endTime:   util.Clean(raw["end_time"]),
		threads:   maxInt(1, util.ToInt(raw["threads"], 32)),
	}
}

type scheduleConfig struct {
	enabled   bool
	startTime string
	endTime   string
	threads   int
}

// normalizeRegisterScheduleConfig 校验定时注册配置
func normalizeRegisterScheduleConfig(raw map[string]any) map[string]any {
	cfg := map[string]any{
		"enabled":    util.ToBool(raw["enabled"]),
		"start_time": "08:00",
		"end_time":   "10:00",
		"threads":    32,
	}
	if startTime := util.Clean(raw["start_time"]); isValidHHMM(startTime) {
		cfg["start_time"] = startTime
	}
	if endTime := util.Clean(raw["end_time"]); isValidHHMM(endTime) {
		cfg["end_time"] = endTime
	}
	if cfg["start_time"] == cfg["end_time"] {
		cfg["enabled"] = false
	}
	cfg["threads"] = maxInt(1, util.ToInt(raw["threads"], 32))
	return cfg
}

// 时间工具函数

func nowBeijing() time.Time {
	loc := time.FixedZone("CST", 8*3600)
	return time.Now().In(loc)
}

func isInTimeWindow(now time.Time, start, end string) bool {
	startMin := parseHHMM(start)
	endMin := parseHHMM(end)
	if startMin < 0 || endMin < 0 {
		return false
	}
	nowMin := now.Hour()*60 + now.Minute()
	if startMin <= endMin {
		// 同日窗口: 08:00 - 10:00
		return nowMin >= startMin && nowMin < endMin
	}
	// 跨日窗口: 23:00 - 02:00
	return nowMin >= startMin || nowMin < endMin
}

func parseHHMM(s string) int {
	parts := strings.Split(strings.TrimSpace(s), ":")
	if len(parts) != 2 {
		return -1
	}
	h, err1 := strconv.Atoi(parts[0])
	m, err2 := strconv.Atoi(parts[1])
	if err1 != nil || err2 != nil || h < 0 || h > 23 || m < 0 || m > 59 {
		return -1
	}
	return h*60 + m
}

func subtractMinutesHHMM(hhmm string, minutes int) string {
	total := parseHHMM(hhmm)
	if total < 0 {
		return "00:00"
	}
	total -= minutes
	if total < 0 {
		total += 24 * 60
	}
	return fmt.Sprintf("%02d:%02d", total/60, total%60)
}

func isValidHHMM(s string) bool {
	return parseHHMM(s) >= 0
}
