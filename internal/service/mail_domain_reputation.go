package service

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// 硬失败标记：域名被 OpenAI 明确拒绝，永久拉黑
var domainHardFailureMarkers = []string{
	"unsupported_email",
	"The email you provided is not supported",
	"Failed to create account. Please try again.",
	"account_creation_failed",
}

// 软失败标记：网络/超时等临时问题，不拉黑
var domainSoftFailureMarkers = []string{
	"waiting for register verification code timed out",
	"independent login waiting for verification code timed out",
	"YYDSMail",
	"timeout",
	"connection reset",
	"EOF",
	"token exchange",
}

// domainRecord 单个域名的声誉记录
type domainRecord struct {
	Success          int    `json:"success"`
	HardFail         int    `json:"hard_fail"`
	SoftFail         int    `json:"soft_fail"`
	ConsecutiveFail  int    `json:"consecutive_fail"`
	Disabled         bool   `json:"disabled"`
	LastSuccessAt    string `json:"last_success_at,omitempty"`
	LastFailureAt    string `json:"last_failure_at,omitempty"`
	LastFailureReason string `json:"last_failure_reason,omitempty"`
}

type domainReputationData struct {
	Providers map[string]*providerReputationData `json:"providers"`
}

type providerReputationData struct {
	Domains map[string]*domainRecord `json:"domains"`
}

// DomainReputationStore 域名声誉存储，线程安全
type DomainReputationStore struct {
	mu       sync.Mutex
	filePath string
}

// GlobalDomainReputation 全局单例
var GlobalDomainReputation *DomainReputationStore

// InitDomainReputation 初始化全局域名声誉存储
func InitDomainReputation(dataDir string) {
	GlobalDomainReputation = &DomainReputationStore{
		filePath: filepath.Join(dataDir, "mail_domain_reputation.json"),
	}
}

func (s *DomainReputationStore) load() *domainReputationData {
	data := &domainReputationData{Providers: map[string]*providerReputationData{}}
	raw, err := os.ReadFile(s.filePath)
	if err != nil {
		return data
	}
	if json.Unmarshal(raw, data) != nil {
		return &domainReputationData{Providers: map[string]*providerReputationData{}}
	}
	if data.Providers == nil {
		data.Providers = map[string]*providerReputationData{}
	}
	return data
}

func (s *DomainReputationStore) save(data *domainReputationData) {
	raw, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return
	}
	_ = os.MkdirAll(filepath.Dir(s.filePath), 0o755)
	tmp := s.filePath + ".tmp"
	if os.WriteFile(tmp, append(raw, '\n'), 0o644) == nil {
		_ = os.Rename(tmp, s.filePath)
	}
}

func (s *DomainReputationStore) getOrCreate(data *domainReputationData, provider, domain string) *domainRecord {
	p, ok := data.Providers[provider]
	if !ok {
		p = &providerReputationData{Domains: map[string]*domainRecord{}}
		data.Providers[provider] = p
	}
	if p.Domains == nil {
		p.Domains = map[string]*domainRecord{}
	}
	r, ok := p.Domains[domain]
	if !ok {
		r = &domainRecord{}
		p.Domains[domain] = r
	}
	return r
}

// RecordSuccess 记录域名注册成功
func (s *DomainReputationStore) RecordSuccess(provider, domain string) {
	domain = normalizeDomain(domain)
	if domain == "" || provider == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	data := s.load()
	r := s.getOrCreate(data, provider, domain)
	r.Success++
	r.ConsecutiveFail = 0
	r.LastSuccessAt = time.Now().UTC().Format(time.RFC3339)
	s.save(data)
}

// RecordFailure 记录域名注册失败，返回是否触发了拉黑
func (s *DomainReputationStore) RecordFailure(provider, domain, reason string) (bucket string, disabledNow bool) {
	domain = normalizeDomain(domain)
	if domain == "" || provider == "" {
		return classifyFailure(reason), false
	}
	bucket = classifyFailure(reason)
	s.mu.Lock()
	defer s.mu.Unlock()
	data := s.load()
	r := s.getOrCreate(data, provider, domain)
	wasDisabled := r.Disabled
	if bucket == "hard" {
		r.HardFail++
		r.Disabled = true
	} else {
		r.SoftFail++
	}
	r.ConsecutiveFail++
	r.LastFailureAt = time.Now().UTC().Format(time.RFC3339)
	if len(reason) > 500 {
		reason = reason[:500]
	}
	r.LastFailureReason = reason
	s.save(data)
	return bucket, r.Disabled && !wasDisabled
}

// IsDisabled 检查域名是否被拉黑
func (s *DomainReputationStore) IsDisabled(provider, domain string) bool {
	domain = normalizeDomain(domain)
	if domain == "" {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	data := s.load()
	p := data.Providers[provider]
	if p == nil || p.Domains == nil {
		return false
	}
	r := p.Domains[domain]
	if r == nil {
		return false
	}
	return r.Disabled
}

// FilterDomains 从配置域名列表中过滤掉被拉黑的，全被拉黑则返回空
func (s *DomainReputationStore) FilterDomains(provider string, domains []string) []string {
	if len(domains) == 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	data := s.load()
	p := data.Providers[provider]
	var enabled []string
	for _, d := range domains {
		nd := normalizeDomain(d)
		if nd == "" {
			continue
		}
		if p != nil && p.Domains != nil {
			if r := p.Domains[nd]; r != nil && r.Disabled {
				continue
			}
		}
		enabled = append(enabled, nd)
	}
	return enabled
}

// GoodDomains 返回历史成功过且未被拉黑的域名，按成功次数降序
func (s *DomainReputationStore) GoodDomains(provider string) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	data := s.load()
	p := data.Providers[provider]
	if p == nil || p.Domains == nil {
		return nil
	}
	type item struct {
		domain  string
		success int
	}
	var items []item
	for domain, r := range p.Domains {
		if r.Disabled || r.Success <= 0 {
			continue
		}
		items = append(items, item{domain: domain, success: r.Success})
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].success != items[j].success {
			return items[i].success > items[j].success
		}
		return items[i].domain < items[j].domain
	})
	result := make([]string, len(items))
	for i, it := range items {
		result[i] = it.domain
	}
	return result
}

// classifyFailure 判断失败类型：hard（永久拉黑）或 soft（临时问题）
func classifyFailure(reason string) string {
	for _, marker := range domainHardFailureMarkers {
		if strings.Contains(reason, marker) {
			return "hard"
		}
	}
	return "soft"
}

// normalizeDomain 规范化域名（小写，去掉 @ 前缀）
func normalizeDomain(value string) string {
	text := strings.TrimSpace(strings.ToLower(value))
	if idx := strings.LastIndex(text, "@"); idx >= 0 {
		text = text[idx+1:]
	}
	return strings.Trim(text, ".")
}
