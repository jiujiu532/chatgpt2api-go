package service

import (
	"context"
	"crypto/sha1"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"mime"
	"mime/multipart"
	"mime/quotedprintable"
	"net/http"
	"net/mail"
	"net/url"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"chatgpt2api/internal/util"
)

var (
	registerMailDomainMu sync.Mutex
	registerMailDomainSeq int

	registerMailCodePatterns = []*regexp.Regexp{
		regexp.MustCompile(`(?is)background-color:\s*#F3F3F3[^>]*>[\s\S]*?(\d{6})[\s\S]*?</p>`),
		regexp.MustCompile(`(?i)(?:Verification code|code is|代码为|验证码)[:\s]*(\d{6})`),
		regexp.MustCompile(`(?is)>\s*(\d{6})\s*<`),
		regexp.MustCompile(`\b(\d{6})\b`),
	}

	// GPTMail 公共 API Key 缓存
	gptMailPublicKeyMu      sync.Mutex
	gptMailPublicKeyCached  string
	gptMailPublicKeyFetched time.Time
)

type registerMailboxProvider interface {
	CreateMailbox(username string) (map[string]any, error)
	FetchLatestMessage(mailbox map[string]any) (map[string]any, error)
	Close()
}

// registerDomainFetcher 可选接口，支持从远程 API 获取可用域名列表
type registerDomainFetcher interface {
	FetchAvailableDomains() ([]string, error)
}

// registerMailboxDeleter 可选接口，支持注册完成后删除临时邮箱
type registerMailboxDeleter interface {
	DeleteMailbox(mailbox map[string]any) error
}

// fetchOrFallbackDomain 当用户未配置 domain 时，尝试从 API 获取可用域名随机选一个；失败则返回空字符串（降级为服务默认行为）
func fetchOrFallbackDomain(provider registerMailboxProvider) string {
	fetcher, ok := provider.(registerDomainFetcher)
	if !ok {
		return ""
	}
	domains, err := fetcher.FetchAvailableDomains()
	if err != nil || len(domains) == 0 {
		return ""
	}
	return domains[rand.Intn(len(domains))]
}

// deleteRegisterMailboxIfSupported 如果 provider 支持删除邮箱，则删除（用于 Stalwart 等临时邮箱）
func deleteRegisterMailboxIfSupported(mailConfig map[string]any, mailbox map[string]any) error {
	if util.Clean(mailbox["provider"]) != "stalwart" {
		return nil
	}
	provider, err := createRegisterMailProvider(mailConfig, util.Clean(mailbox["provider"]), util.Clean(mailbox["provider_ref"]))
	if err != nil {
		return err
	}
	defer provider.Close()
	if deleter, ok := provider.(registerMailboxDeleter); ok {
		return deleter.DeleteMailbox(mailbox)
	}
	return nil
}

type registerMailSettings struct {
	RequestTimeout time.Duration
	WaitTimeout    time.Duration
	WaitInterval   time.Duration
	UserAgent      string
}

type registerHTTPMailProvider struct {
	client *http.Client
	conf   registerMailSettings
}

type registerCloudflareTempMailProvider struct {
	registerHTTPMailProvider
	entry map[string]any
}

type registerTempMailLOLProvider struct {
	registerHTTPMailProvider
	entry map[string]any
}

type registerDuckMailProvider struct {
	registerHTTPMailProvider
	entry map[string]any
}

type registerGPTMailProvider struct {
	registerHTTPMailProvider
	entry map[string]any
}

type registerMoEmailProvider struct {
	registerHTTPMailProvider
	entry map[string]any
}

type registerInbucketMailProvider struct {
	registerHTTPMailProvider
	entry map[string]any
}

type registerYYDSMailProvider struct {
	registerHTTPMailProvider
	entry map[string]any
}

type registerMailTMProvider struct {
	registerHTTPMailProvider
	entry map[string]any
}

type registerGuerrillaMailProvider struct {
	registerHTTPMailProvider
	entry map[string]any
}

type registerStalwartProvider struct {
	registerHTTPMailProvider
	entry map[string]any
}

type registerAhemProvider struct {
	registerHTTPMailProvider
	entry map[string]any
}

// stalwart 域名映射缓存（全局，避免高并发下重复查询）
var (
	stalwartDomainCacheMu      sync.Mutex
	stalwartDomainCacheData    map[string]map[string]string // apiBase -> domain->id
	stalwartDomainCacheFetched map[string]time.Time
)

func init() {
	stalwartDomainCacheData = map[string]map[string]string{}
	stalwartDomainCacheFetched = map[string]time.Time{}
}

func createRegisterMailbox(mailConfig map[string]any, username string) (map[string]any, error) {
	provider, err := createRegisterMailProvider(mailConfig, "", "")
	if err != nil {
		return nil, err
	}
	defer provider.Close()
	return provider.CreateMailbox(username)
}

func waitRegisterCode(ctx context.Context, mailConfig map[string]any, mailbox map[string]any) (string, error) {
	provider, err := createRegisterMailProvider(mailConfig, util.Clean(mailbox["provider"]), util.Clean(mailbox["provider_ref"]))
	if err != nil {
		return "", err
	}
	defer provider.Close()
	conf := registerMailSettingsFromConfig(mailConfig)
	deadline := time.NewTimer(conf.WaitTimeout)
	defer deadline.Stop()
	attempt := 0
	for {
		message, fetchErr := provider.FetchLatestMessage(mailbox)
		if fetchErr == nil && message != nil {
			if code := extractUnseenRegisterMailCode(mailbox, message); code != "" {
				return code, nil
			}
		}
		// 指数退避：前3次每1秒，之后逐渐增加到 wait_interval
		var waitDur time.Duration
		if attempt < 3 {
			waitDur = 1 * time.Second
		} else {
			waitDur = conf.WaitInterval
		}
		attempt++
		interval := time.NewTimer(waitDur)
		select {
		case <-ctx.Done():
			interval.Stop()
			return "", ctx.Err()
		case <-deadline.C:
			interval.Stop()
			return "", nil
		case <-interval.C:
		}
	}
}

func createRegisterMailProvider(mailConfig map[string]any, providerName, providerRef string) (registerMailboxProvider, error) {
	entry, err := selectRegisterMailEntry(mailConfig, providerName, providerRef)
	if err != nil {
		return nil, err
	}
	conf := registerMailSettingsFromConfig(mailConfig)
	client := registerMailHTTPClient(conf.RequestTimeout)
	base := registerHTTPMailProvider{client: client, conf: conf}
	switch util.Clean(entry["type"]) {
	case "cloudflare_temp_email":
		return &registerCloudflareTempMailProvider{registerHTTPMailProvider: base, entry: entry}, nil
	case "tempmail_lol":
		return &registerTempMailLOLProvider{registerHTTPMailProvider: base, entry: entry}, nil
	case "duckmail":
		return &registerDuckMailProvider{registerHTTPMailProvider: base, entry: entry}, nil
	case "gptmail":
		return &registerGPTMailProvider{registerHTTPMailProvider: base, entry: entry}, nil
	case "moemail":
		return &registerMoEmailProvider{registerHTTPMailProvider: base, entry: entry}, nil
	case "inbucket":
		return &registerInbucketMailProvider{registerHTTPMailProvider: base, entry: entry}, nil
	case "yyds_mail":
		return &registerYYDSMailProvider{registerHTTPMailProvider: base, entry: entry}, nil
	case "mail_tm", "mail_gw":
		return &registerMailTMProvider{registerHTTPMailProvider: base, entry: entry}, nil
	case "guerrilla_mail":
		return &registerGuerrillaMailProvider{registerHTTPMailProvider: base, entry: entry}, nil
	case "stalwart":
		return &registerStalwartProvider{registerHTTPMailProvider: base, entry: entry}, nil
	case "ahem":
		return &registerAhemProvider{registerHTTPMailProvider: base, entry: entry}, nil
	default:
		return nil, fmt.Errorf("unsupported mail.provider: %s", util.Clean(entry["type"]))
	}
}

func registerMailSettingsFromConfig(mailConfig map[string]any) registerMailSettings {
	return registerMailSettings{
		RequestTimeout: time.Duration(maxInt(1, util.ToInt(mailConfig["request_timeout"], 15))) * time.Second,
		WaitTimeout:    time.Duration(maxInt(1, util.ToInt(mailConfig["wait_timeout"], 30))) * time.Second,
		WaitInterval:   time.Duration(maxInt(1, util.ToInt(mailConfig["wait_interval"], 3))) * time.Second,
		UserAgent:      firstNonEmpty(util.Clean(mailConfig["user_agent"]), "Mozilla/5.0"),
	}
}

func registerMailHTTPClient(timeout time.Duration) *http.Client {
	if timeout <= 0 {
		timeout = 15 * time.Second
	}
	return &http.Client{
		Timeout: timeout,
		Transport: &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			TLSClientConfig: &tls.Config{
				MinVersion: tls.VersionTLS12,
			},
		},
	}
}

func registerMailEntries(mailConfig map[string]any) []map[string]any {
	providers := util.AsMapSlice(mailConfig["providers"])
	out := make([]map[string]any, 0, len(providers))
	for index, item := range providers {
		entry := util.CopyMap(item)
		entry["provider_ref"] = fmt.Sprintf("%s#%d", util.Clean(entry["type"]), index+1)
		out = append(out, entry)
	}
	return out
}

func selectRegisterMailEntry(mailConfig map[string]any, providerName, providerRef string) (map[string]any, error) {
	entries := registerMailEntries(mailConfig)
	enabled := make([]map[string]any, 0, len(entries))
	for _, entry := range entries {
		if util.ToBool(entry["enable"]) {
			enabled = append(enabled, entry)
		}
	}
	if len(enabled) == 0 {
		return nil, fmt.Errorf("mail.providers has no enabled provider")
	}
	if providerRef != "" {
		for _, entry := range entries {
			if util.Clean(entry["provider_ref"]) == providerRef {
				return util.CopyMap(entry), nil
			}
		}
	}
	if providerName != "" {
		for _, entry := range enabled {
			if util.Clean(entry["type"]) == providerName {
				return util.CopyMap(entry), nil
			}
		}
	}
	if len(enabled) == 1 {
		return util.CopyMap(enabled[0]), nil
	}
	// 加权随机选择
	return weightedRandomSelect(enabled), nil
}

// weightedRandomSelect 根据 weight 字段加权随机选择一个 provider
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

// clampWeight 将权重值限制在 1-10 范围内
func clampWeight(w int) int {
	if w < 1 {
		return 1
	}
	if w > 10 {
		return 10
	}
	return w
}

func extractRegisterMailCode(message map[string]any) string {
	textContent, htmlContent := extractRegisterMailContent(message)
	content := strings.TrimSpace(strings.Join([]string{
		util.Clean(message["subject"]),
		textContent,
		htmlContent,
	}, "\n"))
	if content == "" {
		return ""
	}
	for _, pattern := range registerMailCodePatterns {
		match := pattern.FindStringSubmatch(content)
		if len(match) > 1 {
			code := strings.TrimSpace(match[1])
			if code != "" && code != "177010" {
				return code
			}
		}
	}
	return ""
}

func extractUnseenRegisterMailCode(mailbox map[string]any, message map[string]any) string {
	ref := registerMailMessageRef(message)
	seen := registerSeenMailRefs(mailbox["_seen_code_message_refs"])
	if ref != "" {
		if _, ok := seen[ref]; ok {
			return ""
		}
	}
	code := extractRegisterMailCode(message)
	if code == "" || ref == "" {
		return code
	}
	existing := registerSeenMailRefList(mailbox["_seen_code_message_refs"])
	mailbox["_seen_code_message_refs"] = append(existing, ref)
	return code
}

func registerSeenMailRefs(value any) map[string]struct{} {
	out := map[string]struct{}{}
	for _, item := range registerSeenMailRefList(value) {
		out[item] = struct{}{}
	}
	return out
}

func registerSeenMailRefList(value any) []string {
	switch typed := value.(type) {
	case []string:
		return append([]string(nil), typed...)
	case []any:
		out := make([]string, 0, len(typed))
		for _, item := range typed {
			if ref := util.Clean(item); ref != "" {
				out = append(out, ref)
			}
		}
		return out
	default:
		return nil
	}
}

func registerMailMessageRef(message map[string]any) string {
	provider := util.Clean(message["provider"])
	mailbox := util.Clean(message["mailbox"])
	if id := registerMessageID(message); id != "" {
		return "id:" + provider + ":" + mailbox + ":" + id
	}
	textContent, htmlContent := extractRegisterMailContent(message)
	received := util.Clean(message["received_at"])
	content := strings.Join([]string{util.Clean(message["subject"]), textContent, htmlContent}, "\n")
	if strings.TrimSpace(content) == "" {
		return ""
	}
	sum := sha1.Sum([]byte(content))
	return fmt.Sprintf("content:%s:%s:%s:%x", provider, mailbox, received, sum[:8])
}

func extractRegisterMailContent(data map[string]any) (string, string) {
	textContent := firstNonEmpty(
		registerContentString(data["text_content"]),
		registerContentString(data["text"]),
		registerContentString(data["body"]),
		registerContentString(data["content"]),
	)
	htmlContent := firstNonEmpty(
		registerContentString(data["html_content"]),
		registerContentString(data["html"]),
		registerContentString(data["html_body"]),
		registerContentString(data["body_html"]),
	)
	if textContent != "" || htmlContent != "" {
		return textContent, htmlContent
	}
	raw, ok := data["raw"].(string)
	if !ok || strings.TrimSpace(raw) == "" {
		return "", ""
	}
	textContent, htmlContent = parseRegisterRawMail(raw)
	if textContent == "" && htmlContent == "" {
		return raw, ""
	}
	return textContent, htmlContent
}

func registerContentString(value any) string {
	switch typed := value.(type) {
	case nil:
		return ""
	case string:
		return strings.TrimSpace(typed)
	case []string:
		return strings.TrimSpace(strings.Join(typed, ""))
	case []any:
		parts := make([]string, 0, len(typed))
		for _, item := range typed {
			if text := registerContentString(item); text != "" {
				parts = append(parts, text)
			}
		}
		return strings.TrimSpace(strings.Join(parts, ""))
	default:
		return util.Clean(value)
	}
}

func parseRegisterRawMail(raw string) (string, string) {
	message, err := mail.ReadMessage(strings.NewReader(raw))
	if err != nil {
		return raw, ""
	}
	plain, html := parseRegisterMIMEBody(message.Header.Get("Content-Type"), message.Header.Get("Content-Transfer-Encoding"), message.Body)
	return strings.TrimSpace(strings.Join(plain, "\n")), strings.TrimSpace(strings.Join(html, "\n"))
}

func parseRegisterMIMEBody(contentType, transferEncoding string, body io.Reader) ([]string, []string) {
	mediaType, params, err := mime.ParseMediaType(contentType)
	if err != nil {
		mediaType = strings.TrimSpace(strings.ToLower(strings.Split(contentType, ";")[0]))
	}
	if strings.HasPrefix(mediaType, "multipart/") {
		boundary := params["boundary"]
		if boundary == "" {
			return nil, nil
		}
		reader := multipart.NewReader(body, boundary)
		var plain []string
		var html []string
		for {
			part, partErr := reader.NextPart()
			if partErr == io.EOF {
				break
			}
			if partErr != nil {
				break
			}
			partPlain, partHTML := parseRegisterMIMEBody(part.Header.Get("Content-Type"), part.Header.Get("Content-Transfer-Encoding"), part)
			plain = append(plain, partPlain...)
			html = append(html, partHTML...)
		}
		return plain, html
	}
	payload, err := readRegisterMIMEPayload(body, transferEncoding)
	if err != nil || strings.TrimSpace(payload) == "" {
		return nil, nil
	}
	if mediaType == "text/html" {
		return nil, []string{payload}
	}
	if mediaType == "" || strings.HasPrefix(mediaType, "text/") {
		return []string{payload}, nil
	}
	return nil, nil
}

func readRegisterMIMEPayload(body io.Reader, transferEncoding string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(transferEncoding)) {
	case "base64":
		data, err := io.ReadAll(body)
		if err != nil {
			return "", err
		}
		cleaned := strings.NewReplacer("\r", "", "\n", "", " ", "", "\t", "").Replace(string(data))
		decoded, err := base64.StdEncoding.DecodeString(cleaned)
		if err != nil {
			return "", err
		}
		return string(decoded), nil
	case "quoted-printable":
		data, err := io.ReadAll(quotedprintable.NewReader(body))
		if err != nil {
			return "", err
		}
		return string(data), nil
	default:
		data, err := io.ReadAll(body)
		if err != nil {
			return "", err
		}
		return string(data), nil
	}
}

func registerMessageMatchesEmail(data map[string]any, email string) bool {
	target := strings.ToLower(strings.TrimSpace(email))
	if target == "" {
		return true
	}
	var candidates []string
	for _, key := range []string{"to", "mailTo", "receiver", "receivers", "address", "email", "envelope_to"} {
		if value, ok := data[key]; ok {
			candidates = append(candidates, registerTextCandidates(value)...)
		}
	}
	if len(candidates) == 0 {
		return true
	}
	for _, candidate := range candidates {
		if strings.Contains(strings.ToLower(strings.TrimSpace(candidate)), target) {
			return true
		}
	}
	return false
}

func registerTextCandidates(value any) []string {
	switch typed := value.(type) {
	case string:
		return []string{typed}
	case map[string]any:
		var out []string
		for _, key := range []string{"address", "email", "name", "value"} {
			if item, ok := typed[key]; ok {
				out = append(out, registerTextCandidates(item)...)
			}
		}
		return out
	case []any:
		var out []string
		for _, item := range typed {
			out = append(out, registerTextCandidates(item)...)
		}
		return out
	case []map[string]any:
		var out []string
		for _, item := range typed {
			out = append(out, registerTextCandidates(item)...)
		}
		return out
	default:
		return nil
	}
}

func latestRegisterMailMessage(items []map[string]any) map[string]any {
	if len(items) == 0 {
		return nil
	}
	candidates := append([]map[string]any(nil), items...)
	sort.SliceStable(candidates, func(i, j int) bool {
		left := registerMessageReceivedAt(candidates[i])
		right := registerMessageReceivedAt(candidates[j])
		if !left.IsZero() || !right.IsZero() {
			if !left.Equal(right) {
				return left.After(right)
			}
			return registerMessageID(candidates[i]) > registerMessageID(candidates[j])
		}
		return false
	})
	return candidates[0]
}

func registerMessageReceivedAt(data map[string]any) time.Time {
	for _, key := range []string{"created_at", "createdAt", "received_at", "receivedAt", "date", "timestamp"} {
		if value, ok := data[key]; ok {
			if parsed := parseRegisterMailTime(value); !parsed.IsZero() {
				return parsed
			}
		}
	}
	return time.Time{}
}

func registerMessageID(data map[string]any) string {
	return util.Clean(firstNonNil(data["id"], data["message_id"], data["_id"], data["token"], data["@id"]))
}

func parseRegisterMailTime(value any) time.Time {
	switch typed := value.(type) {
	case int:
		return time.Unix(int64(typed), 0).UTC()
	case int64:
		return time.Unix(typed, 0).UTC()
	case float64:
		return time.Unix(int64(typed), 0).UTC()
	case json.Number:
		if integer, err := typed.Int64(); err == nil {
			return time.Unix(integer, 0).UTC()
		}
		if number, err := typed.Float64(); err == nil {
			return time.Unix(int64(number), 0).UTC()
		}
	}
	text := util.Clean(value)
	if text == "" {
		return time.Time{}
	}
	if parsed, err := time.Parse(time.RFC3339Nano, text); err == nil {
		return parsed
	}
	if parsed, err := time.Parse(time.RFC1123Z, text); err == nil {
		return parsed
	}
	if parsed, err := time.Parse(time.RFC1123, text); err == nil {
		return parsed
	}
	if parsed, err := mail.ParseDate(text); err == nil {
		return parsed
	}
	return time.Time{}
}

func registerRandomMailboxName() string {
	// 12 个大小写字母随机组合，增加唯一性
	const chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	var b strings.Builder
	for i := 0; i < 12; i++ {
		b.WriteByte(chars[rand.Intn(len(chars))])
	}
	return b.String()
}

func registerRandomSubdomainLabel() string {
	return randomAlphaNum(4 + rand.Intn(7))
}

func nextRegisterDomain(domains []string) (string, error) {
	filtered := make([]string, 0, len(domains))
	for _, domain := range domains {
		if item := strings.TrimSpace(domain); item != "" {
			filtered = append(filtered, item)
		}
	}
	if len(filtered) == 0 {
		return "", fmt.Errorf("mail domain is required")
	}
	if len(filtered) == 1 {
		return filtered[0], nil
	}
	registerMailDomainMu.Lock()
	value := filtered[registerMailDomainSeq%len(filtered)]
	registerMailDomainSeq = (registerMailDomainSeq + 1) % len(filtered)
	registerMailDomainMu.Unlock()
	return value, nil
}

func randomLower(n int) string {
	const letters = "abcdefghijklmnopqrstuvwxyz"
	var b strings.Builder
	for i := 0; i < n; i++ {
		b.WriteByte(letters[rand.Intn(len(letters))])
	}
	return b.String()
}

func randomAlphaNum(n int) string {
	const chars = "abcdefghijklmnopqrstuvwxyz0123456789"
	var b strings.Builder
	for i := 0; i < n; i++ {
		b.WriteByte(chars[rand.Intn(len(chars))])
	}
	return b.String()
}

func (p *registerHTTPMailProvider) Close() {
	p.client.CloseIdleConnections()
}

func (p *registerCloudflareTempMailProvider) CreateMailbox(username string) (map[string]any, error) {
	apiBase := strings.TrimRight(util.Clean(p.entry["api_base"]), "/")
	adminPassword := util.Clean(p.entry["admin_password"])
	domains := util.AsStringSlice(p.entry["domain"])
	var domain string
	var err error
	if len(domains) > 0 {
		domain, err = nextRegisterDomain(domains)
		if err != nil {
			return nil, err
		}
	} else {
		// 尝试从 API 获取可用域名
		domain = fetchOrFallbackDomain(p)
		if domain == "" {
			return nil, fmt.Errorf("cloudflare_temp_email domain is required (configure domain list or ensure API returns available domains)")
		}
	}
	payload := map[string]any{
		"enablePrefix": true,
		"name":         firstNonEmpty(strings.TrimSpace(username), registerRandomMailboxName()),
		"domain":       domain,
	}
	data, err := registerMailRequestJSON(p.client, http.MethodPost, apiBase+"/admin/new_address", map[string]string{
		"Content-Type": "application/json",
		"User-Agent":   p.conf.UserAgent,
		"x-admin-auth": adminPassword,
	}, nil, payload, http.StatusOK)
	if err != nil {
		return nil, err
	}
	address := util.Clean(data["address"])
	token := util.Clean(data["jwt"])
	if address == "" || token == "" {
		return nil, fmt.Errorf("cloudflare_temp_email response missing address or jwt")
	}
	return map[string]any{"provider": "cloudflare_temp_email", "provider_ref": p.entry["provider_ref"], "address": address, "token": token}, nil
}

func (p *registerCloudflareTempMailProvider) FetchLatestMessage(mailbox map[string]any) (map[string]any, error) {
	apiBase := strings.TrimRight(util.Clean(p.entry["api_base"]), "/")
	token := util.Clean(mailbox["token"])
	data, err := registerMailRequestJSON(p.client, http.MethodGet, apiBase+"/api/mails", map[string]string{
		"Authorization": "Bearer " + token,
		"User-Agent":    p.conf.UserAgent,
	}, map[string]string{"limit": "10", "offset": "0"}, nil, http.StatusOK)
	if err != nil {
		return nil, err
	}
	items := util.AsMapSlice(data["results"])
	messages := make([]map[string]any, 0, len(items))
	for _, item := range items {
		if registerMessageMatchesEmail(item, util.Clean(mailbox["address"])) {
			messages = append(messages, item)
		}
	}
	if len(messages) == 0 {
		return nil, nil
	}
	message := latestRegisterMailMessage(messages)
	textContent, htmlContent := extractRegisterMailContent(message)
	sender := firstNonNil(message["from"], message["sender"])
	if senderMap, ok := sender.(map[string]any); ok {
		sender = firstNonNil(senderMap["address"], senderMap["email"], senderMap["name"])
	}
	return map[string]any{
		"provider":     "cloudflare_temp_email",
		"mailbox":      util.Clean(mailbox["address"]),
		"message_id":   firstNonEmpty(util.Clean(message["id"]), util.Clean(message["_id"])),
		"subject":      util.Clean(message["subject"]),
		"sender":       util.Clean(sender),
		"text_content": textContent,
		"html_content": htmlContent,
		"received_at":  firstNonNil(message["createdAt"], message["created_at"], message["receivedAt"], message["date"], message["timestamp"]),
		"raw":          message,
	}, nil
}

func (p *registerCloudflareTempMailProvider) FetchAvailableDomains() ([]string, error) {
	apiBase := strings.TrimRight(util.Clean(p.entry["api_base"]), "/")
	adminPassword := util.Clean(p.entry["admin_password"])
	if apiBase == "" {
		return nil, fmt.Errorf("cloudflare_temp_email api_base is required")
	}
	data, err := registerMailRequestAny(p.client, http.MethodGet, apiBase+"/admin/address", map[string]string{
		"User-Agent":   p.conf.UserAgent,
		"Accept":       "application/json",
		"x-admin-auth": adminPassword,
	}, map[string]string{"limit": "1"}, nil, http.StatusOK)
	if err != nil {
		return nil, err
	}
	// 尝试从 settings 或 domains 端点获取
	body := util.StringMap(data)
	items := util.AsMapSlice(body["domains"])
	if len(items) == 0 {
		// 尝试 /admin/domains 端点
		data2, err2 := registerMailRequestAny(p.client, http.MethodGet, apiBase+"/admin/domains", map[string]string{
			"User-Agent":   p.conf.UserAgent,
			"Accept":       "application/json",
			"x-admin-auth": adminPassword,
		}, nil, nil, http.StatusOK)
		if err2 == nil {
			items = util.AsMapSlice(data2)
		}
	}
	var domains []string
	for _, item := range items {
		domain := util.Clean(item["domain"])
		if domain == "" {
			domain = util.Clean(item["name"])
		}
		if domain != "" {
			domains = append(domains, domain)
		}
	}
	if len(domains) == 0 {
		for _, s := range util.AsStringSlice(data) {
			if s = strings.TrimSpace(s); s != "" {
				domains = append(domains, s)
			}
		}
	}
	return domains, nil
}

func (p *registerTempMailLOLProvider) CreateMailbox(username string) (map[string]any, error) {
	payload := map[string]any{}
	domains := util.AsStringSlice(p.entry["domain"])
	if len(domains) > 0 {
		domain := domains[rand.Intn(len(domains))]
		if strings.HasPrefix(domain, "*.") && len(domain) > 2 {
			payload["domain"] = registerRandomSubdomainLabel() + "." + strings.TrimPrefix(domain, "*.")
			payload["prefix"] = registerRandomMailboxName()
		} else if strings.TrimSpace(domain) != "" {
			payload["domain"] = strings.TrimSpace(domain)
		}
	}
	if username = strings.TrimSpace(username); username != "" && payload["prefix"] == nil {
		payload["prefix"] = username
	}
	data, err := registerMailRequestJSON(p.client, http.MethodPost, "https://api.tempmail.lol/v2/inbox/create", map[string]string{
		"Content-Type": "application/json",
		"User-Agent":   p.conf.UserAgent,
		"Accept":       "application/json",
		"Authorization": func() string {
			if key := util.Clean(p.entry["api_key"]); key != "" {
				return "Bearer " + key
			}
			return ""
		}(),
	}, nil, payload, http.StatusOK, http.StatusCreated)
	if err != nil {
		return nil, err
	}
	address := util.Clean(data["address"])
	token := util.Clean(data["token"])
	if address == "" || token == "" {
		return nil, fmt.Errorf("tempmail_lol response missing address or token")
	}
	return map[string]any{"provider": "tempmail_lol", "provider_ref": p.entry["provider_ref"], "address": address, "token": token}, nil
}

func (p *registerTempMailLOLProvider) FetchLatestMessage(mailbox map[string]any) (map[string]any, error) {
	data, err := registerMailRequestJSON(p.client, http.MethodGet, "https://api.tempmail.lol/v2/inbox", map[string]string{
		"User-Agent": p.conf.UserAgent,
		"Accept":     "application/json",
	}, map[string]string{"token": util.Clean(mailbox["token"])}, nil, http.StatusOK)
	if err != nil {
		return nil, err
	}
	items := util.AsMapSlice(firstNonNil(data["emails"], data["messages"]))
	if len(items) == 0 {
		return nil, nil
	}
	latest := latestRegisterMailMessage(items)
	textContent, htmlContent := extractRegisterMailContent(latest)
	return map[string]any{
		"provider":     "tempmail_lol",
		"mailbox":      util.Clean(mailbox["address"]),
		"message_id":   firstNonEmpty(util.Clean(latest["id"]), util.Clean(latest["token"])),
		"subject":      util.Clean(latest["subject"]),
		"sender":       firstNonEmpty(util.Clean(latest["from"]), util.Clean(latest["from_address"])),
		"text_content": textContent,
		"html_content": htmlContent,
		"received_at":  firstNonNil(latest["created_at"], latest["createdAt"], latest["date"], latest["received_at"], latest["timestamp"]),
		"raw":          latest,
	}, nil
}

func (p *registerDuckMailProvider) CreateMailbox(username string) (map[string]any, error) {
	apiKey := util.Clean(p.entry["api_key"])
	domain := util.Clean(p.entry["default_domain"])
	if domain == "" {
		// 尝试从 API 获取可用域名随机选一个
		domain = fetchOrFallbackDomain(p)
	}
	if domain == "" {
		domain = "duckmail.sbs"
	}
	password := randomAlphaNum(12)
	address := firstNonEmpty(strings.TrimSpace(username), registerRandomMailboxName()) + "@" + domain
	payload := map[string]any{"address": address, "password": password}
	account, err := registerMailRequestJSON(p.client, http.MethodPost, "https://api.duckmail.sbs/accounts", map[string]string{
		"Authorization": "Bearer " + apiKey,
		"User-Agent":    p.conf.UserAgent,
		"Accept":        "application/json",
		"Content-Type":  "application/json",
	}, nil, payload, http.StatusOK, http.StatusCreated)
	if err != nil {
		return nil, err
	}
	tokenData, err := registerMailRequestJSON(p.client, http.MethodPost, "https://api.duckmail.sbs/token", map[string]string{
		"Authorization": "Bearer " + apiKey,
		"User-Agent":    p.conf.UserAgent,
		"Accept":        "application/json",
		"Content-Type":  "application/json",
	}, nil, payload, http.StatusOK, http.StatusCreated)
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"provider":     "duckmail",
		"provider_ref": p.entry["provider_ref"],
		"address":      address,
		"token":        util.Clean(tokenData["token"]),
		"password":     password,
		"account_id":   util.Clean(account["id"]),
	}, nil
}

func (p *registerDuckMailProvider) FetchLatestMessage(mailbox map[string]any) (map[string]any, error) {
	token := util.Clean(mailbox["token"])
	data, err := registerMailRequestAny(p.client, http.MethodGet, "https://api.duckmail.sbs/messages", map[string]string{
		"Authorization": "Bearer " + token,
		"User-Agent":    p.conf.UserAgent,
		"Accept":        "application/json",
	}, map[string]string{"page": "1"}, nil, http.StatusOK, http.StatusCreated)
	if err != nil {
		return nil, err
	}
	items := duckMailItems(data)
	if len(items) == 0 {
		return nil, nil
	}
	messageID := strings.TrimPrefix(util.Clean(firstNonNil(items[0]["id"], items[0]["@id"])), "/messages/")
	if messageID == "" {
		return nil, nil
	}
	message, err := registerMailRequestJSON(p.client, http.MethodGet, "https://api.duckmail.sbs/messages/"+messageID, map[string]string{
		"Authorization": "Bearer " + token,
		"User-Agent":    p.conf.UserAgent,
		"Accept":        "application/json",
	}, nil, nil, http.StatusOK)
	if err != nil {
		return nil, err
	}
	textContent, htmlContent := extractRegisterMailContent(message)
	sender := message["from"]
	if senderMap, ok := sender.(map[string]any); ok {
		sender = firstNonNil(senderMap["address"], senderMap["name"])
	}
	return map[string]any{
		"provider":     "duckmail",
		"mailbox":      util.Clean(mailbox["address"]),
		"message_id":   messageID,
		"subject":      util.Clean(message["subject"]),
		"sender":       util.Clean(sender),
		"text_content": textContent,
		"html_content": htmlContent,
		"received_at":  firstNonNil(message["createdAt"], message["created_at"], message["receivedAt"], message["date"]),
		"raw":          message,
	}, nil
}

func (p *registerDuckMailProvider) FetchAvailableDomains() ([]string, error) {
	apiKey := util.Clean(p.entry["api_key"])
	data, err := registerMailRequestJSON(p.client, http.MethodGet, "https://api.duckmail.sbs/domains", map[string]string{
		"Authorization": "Bearer " + apiKey,
		"User-Agent":    p.conf.UserAgent,
		"Accept":        "application/json",
	}, nil, nil, http.StatusOK)
	if err != nil {
		return nil, err
	}
	members := util.AsMapSlice(data["hydra:member"])
	var domains []string
	for _, item := range members {
		domain := util.Clean(item["domain"])
		if domain != "" {
			domains = append(domains, domain)
		}
	}
	return domains, nil
}

func (p *registerGPTMailProvider) resolveAPIKey() string {
	key := util.Clean(p.entry["api_key"])
	if key != "" && key != "PUBLIC_API_KEY" {
		return key
	}
	// 使用公共 key，带缓存（北京时间每天 08:00 刷新）
	return fetchGPTMailPublicKey(p.client, p.conf.UserAgent)
}

// fetchGPTMailPublicKey 获取 GPTMail 公共测试密钥，缓存 23 小时后自动刷新
func fetchGPTMailPublicKey(client *http.Client, userAgent string) string {
	gptMailPublicKeyMu.Lock()
	defer gptMailPublicKeyMu.Unlock()

	// 缓存有效期 23 小时
	if gptMailPublicKeyCached != "" && time.Since(gptMailPublicKeyFetched) < 23*time.Hour {
		return gptMailPublicKeyCached
	}

	// 从远程获取
	data, err := registerMailRequestJSON(client, http.MethodGet, "https://mail.chatgpt.org.uk/api/public-key-status", map[string]string{
		"User-Agent":          userAgent,
		"Accept":              "application/json",
		"X-Public-Key-Reveal": "click",
	}, map[string]string{"reveal": "1"}, nil, http.StatusOK)
	if err != nil {
		if gptMailPublicKeyCached != "" {
			return gptMailPublicKeyCached
		}
		return ""
	}
	inner := util.StringMap(data["data"])
	key := util.Clean(inner["key"])
	if key == "" {
		if gptMailPublicKeyCached != "" {
			return gptMailPublicKeyCached
		}
		return ""
	}
	gptMailPublicKeyCached = key
	gptMailPublicKeyFetched = time.Now()
	return key
}

func (p *registerGPTMailProvider) CreateMailbox(username string) (map[string]any, error) {
	apiKey := p.resolveAPIKey()
	payload := map[string]any{}
	if username = strings.TrimSpace(username); username != "" {
		payload["prefix"] = username
	}
	if domain := util.Clean(p.entry["default_domain"]); domain != "" {
		payload["domain"] = domain
	}
	method := http.MethodGet
	var requestBody any
	if len(payload) > 0 {
		method = http.MethodPost
		requestBody = payload
	}
	data, err := registerMailRequestAny(p.client, method, "https://mail.chatgpt.org.uk/api/generate-email", map[string]string{
		"X-API-Key":    apiKey,
		"User-Agent":   p.conf.UserAgent,
		"Accept":       "application/json",
		"Content-Type": "application/json",
	}, nil, requestBody, http.StatusOK)
	if err != nil {
		return nil, err
	}
	typed := util.StringMap(data)
	payloadMap := util.StringMap(firstNonNil(typed["data"], data))
	address := util.Clean(payloadMap["email"])
	if address == "" {
		return nil, fmt.Errorf("gptmail response missing email")
	}
	return map[string]any{"provider": "gptmail", "provider_ref": p.entry["provider_ref"], "address": address}, nil
}

func (p *registerGPTMailProvider) FetchLatestMessage(mailbox map[string]any) (map[string]any, error) {
	apiKey := p.resolveAPIKey()
	data, err := registerMailRequestAny(p.client, http.MethodGet, "https://mail.chatgpt.org.uk/api/emails", map[string]string{
		"X-API-Key":  apiKey,
		"User-Agent": p.conf.UserAgent,
		"Accept":     "application/json",
	}, map[string]string{"email": util.Clean(mailbox["address"])}, nil, http.StatusOK)
	if err != nil {
		return nil, err
	}
	body := util.StringMap(data)
	if nested := util.StringMap(body["data"]); len(nested) > 0 {
		body = nested
	}
	items := util.AsMapSlice(firstNonNil(body["emails"], body))
	if len(items) == 0 {
		return nil, nil
	}
	latest := latestRegisterMailMessage(items)
	if id := util.Clean(latest["id"]); id != "" {
		detail, detailErr := registerMailRequestAny(p.client, http.MethodGet, "https://mail.chatgpt.org.uk/api/email/"+id, map[string]string{
			"X-API-Key":  apiKey,
			"User-Agent": p.conf.UserAgent,
			"Accept":     "application/json",
		}, nil, nil, http.StatusOK)
		if detailErr == nil {
			if typed, ok := detail.(map[string]any); ok && typed["data"] != nil {
				latest = util.StringMap(typed["data"])
			} else if typed, ok := detail.(map[string]any); ok {
				latest = typed
			}
		}
	}
	textContent, htmlContent := extractRegisterMailContent(latest)
	return map[string]any{
		"provider":     "gptmail",
		"mailbox":      util.Clean(mailbox["address"]),
		"message_id":   util.Clean(latest["id"]),
		"subject":      util.Clean(latest["subject"]),
		"sender":       util.Clean(latest["from_address"]),
		"text_content": textContent,
		"html_content": htmlContent,
		"received_at":  firstNonNil(latest["timestamp"], latest["created_at"]),
		"raw":          latest,
	}, nil
}

func (p *registerMoEmailProvider) CreateMailbox(username string) (map[string]any, error) {
	apiBase := strings.TrimRight(util.Clean(p.entry["api_base"]), "/")
	if apiBase == "" {
		return nil, fmt.Errorf("moemail api_base is required")
	}
	domains := util.AsStringSlice(p.entry["domain"])
	var domain string
	var err error
	if len(domains) > 0 {
		domain, err = nextRegisterDomain(domains)
		if err != nil {
			return nil, err
		}
	} else {
		// 尝试从 API 获取可用域名
		domain = fetchOrFallbackDomain(p)
		if domain == "" {
			return nil, fmt.Errorf("moemail domain is required (configure domain list or ensure API returns available domains)")
		}
	}
	expiryTime := util.ToInt(p.entry["expiry_time"], 3600000)
	if expiryTime <= 0 {
		expiryTime = 3600000 // 默认 1 小时，0 在 moemail 表示永久，不应作为默认值
	}
	payload := map[string]any{
		"name":       firstNonEmpty(strings.TrimSpace(username), registerRandomMailboxName()),
		"expiryTime": expiryTime,
		"domain":     domain,
	}
	data, err := registerMailRequestJSON(p.client, http.MethodPost, apiBase+"/api/emails/generate", map[string]string{
		"X-API-Key":    util.Clean(p.entry["api_key"]),
		"Content-Type": "application/json",
		"User-Agent":   p.conf.UserAgent,
		"Accept":       "application/json",
	}, nil, payload, http.StatusOK, http.StatusCreated)
	if err != nil {
		return nil, err
	}
	address := util.Clean(data["email"])
	emailID := firstNonEmpty(util.Clean(data["id"]), util.Clean(data["email_id"]))
	if address == "" || emailID == "" {
		return nil, fmt.Errorf("MoEmail missing email or id")
	}
	return map[string]any{"provider": "moemail", "provider_ref": p.entry["provider_ref"], "address": address, "email_id": emailID}, nil
}

func (p *registerMoEmailProvider) FetchLatestMessage(mailbox map[string]any) (map[string]any, error) {
	apiBase := strings.TrimRight(util.Clean(p.entry["api_base"]), "/")
	emailID := util.Clean(mailbox["email_id"])
	if apiBase == "" {
		return nil, fmt.Errorf("moemail api_base is required")
	}
	if emailID == "" {
		return nil, fmt.Errorf("MoEmail missing email_id")
	}
	data, err := registerMailRequestJSON(p.client, http.MethodGet, apiBase+"/api/emails/"+emailID, map[string]string{
		"X-API-Key":    util.Clean(p.entry["api_key"]),
		"Content-Type": "application/json",
		"User-Agent":   p.conf.UserAgent,
		"Accept":       "application/json",
	}, nil, nil, http.StatusOK)
	if err != nil {
		return nil, err
	}
	items := util.AsMapSlice(data["messages"])
	if len(items) == 0 {
		return nil, nil
	}
	latest := latestRegisterMailMessage(items)
	messageID := firstNonEmpty(util.Clean(latest["id"]), util.Clean(latest["message_id"]), util.Clean(latest["_id"]))
	message := latest
	raw := any(data)
	if messageID != "" {
		detail, detailErr := registerMailRequestJSON(p.client, http.MethodGet, apiBase+"/api/emails/"+emailID+"/"+messageID, map[string]string{
			"X-API-Key":    util.Clean(p.entry["api_key"]),
			"Content-Type": "application/json",
			"User-Agent":   p.conf.UserAgent,
			"Accept":       "application/json",
		}, nil, nil, http.StatusOK)
		if detailErr != nil {
			return nil, detailErr
		}
		raw = detail
		if nested := util.StringMap(detail["message"]); len(nested) > 0 {
			message = nested
		} else {
			message = detail
		}
	}
	textContent, htmlContent := extractRegisterMailContent(message)
	sender := firstNonNil(message["from"], message["sender"])
	if senderMap, ok := sender.(map[string]any); ok {
		sender = firstNonNil(senderMap["address"], senderMap["email"], senderMap["name"])
	}
	return map[string]any{
		"provider":     "moemail",
		"mailbox":      util.Clean(mailbox["address"]),
		"message_id":   messageID,
		"subject":      firstNonEmpty(util.Clean(message["subject"]), util.Clean(latest["subject"])),
		"sender":       util.Clean(sender),
		"text_content": textContent,
		"html_content": htmlContent,
		"received_at":  firstNonNil(message["createdAt"], message["created_at"], message["receivedAt"], message["date"], message["timestamp"], latest["createdAt"], latest["created_at"], latest["receivedAt"], latest["date"], latest["timestamp"]),
		"raw":          raw,
	}, nil
}

func (p *registerMoEmailProvider) FetchAvailableDomains() ([]string, error) {
	apiBase := strings.TrimRight(util.Clean(p.entry["api_base"]), "/")
	if apiBase == "" {
		return nil, fmt.Errorf("moemail api_base is required")
	}
	// MoeMail 通过 /api/config 接口获取域名列表（emailDomains 字段，逗号分隔）
	data, err := registerMailRequestJSON(p.client, http.MethodGet, apiBase+"/api/config", map[string]string{
		"X-API-Key":  util.Clean(p.entry["api_key"]),
		"User-Agent": p.conf.UserAgent,
		"Accept":     "application/json",
	}, nil, nil, http.StatusOK)
	if err != nil {
		return nil, err
	}
	// emailDomains 是逗号分隔的字符串
	domainStr := util.Clean(data["emailDomains"])
	if domainStr == "" {
		return nil, nil
	}
	var domains []string
	for _, d := range strings.Split(domainStr, ",") {
		if d = strings.TrimSpace(d); d != "" {
			domains = append(domains, d)
		}
	}
	return domains, nil
}

func (p *registerInbucketMailProvider) CreateMailbox(username string) (map[string]any, error) {
	apiBase := strings.TrimRight(util.Clean(p.entry["api_base"]), "/")
	if apiBase == "" {
		return nil, fmt.Errorf("inbucket api_base is required")
	}
	baseDomain, err := nextRegisterDomain(util.AsStringSlice(p.entry["domain"]))
	if err != nil {
		return nil, err
	}
	localPart := firstNonEmpty(strings.TrimSpace(username), registerRandomMailboxName())
	domain := baseDomain
	randomSubdomain := true
	if _, ok := p.entry["random_subdomain"]; ok {
		randomSubdomain = util.ToBool(p.entry["random_subdomain"])
	}
	if randomSubdomain {
		domain = registerRandomSubdomainLabel() + "." + baseDomain
	}
	address := localPart + "@" + domain
	return map[string]any{
		"provider":     "inbucket",
		"provider_ref": p.entry["provider_ref"],
		"address":      address,
		"base_domain":  baseDomain,
		"mailbox_name": localPart,
	}, nil
}

func (p *registerInbucketMailProvider) FetchLatestMessage(mailbox map[string]any) (map[string]any, error) {
	apiBase := strings.TrimRight(util.Clean(p.entry["api_base"]), "/")
	if apiBase == "" {
		return nil, fmt.Errorf("inbucket api_base is required")
	}
	mailboxName := util.Clean(mailbox["mailbox_name"])
	if mailboxName == "" {
		mailboxName = registerInbucketMailboxName(util.Clean(mailbox["address"]))
	}
	if mailboxName == "" {
		return nil, fmt.Errorf("inbucket missing mailbox_name")
	}
	data, err := registerMailRequestAny(p.client, http.MethodGet, apiBase+"/api/v1/mailbox/"+url.PathEscape(mailboxName), map[string]string{
		"User-Agent": p.conf.UserAgent,
		"Accept":     "application/json",
	}, nil, nil, http.StatusOK)
	if err != nil {
		return nil, err
	}
	items := util.AsMapSlice(data)
	if len(items) == 0 {
		return nil, nil
	}
	sort.SliceStable(items, func(i, j int) bool {
		left := registerMessageReceivedAt(items[i])
		right := registerMessageReceivedAt(items[j])
		if !left.IsZero() || !right.IsZero() {
			if !left.Equal(right) {
				return left.After(right)
			}
			return registerMessageID(items[i]) > registerMessageID(items[j])
		}
		return false
	})
	address := util.Clean(mailbox["address"])
	for _, item := range items {
		messageID := util.Clean(item["id"])
		if messageID == "" {
			continue
		}
		detail, detailErr := registerMailRequestJSON(p.client, http.MethodGet, apiBase+"/api/v1/mailbox/"+url.PathEscape(mailboxName)+"/"+url.PathEscape(messageID), map[string]string{
			"User-Agent": p.conf.UserAgent,
			"Accept":     "application/json",
		}, nil, nil, http.StatusOK)
		if detailErr != nil {
			return nil, detailErr
		}
		header := util.StringMap(detail["header"])
		body := util.StringMap(detail["body"])
		normalized := map[string]any{
			"provider":     "inbucket",
			"mailbox":      mailboxName,
			"message_id":   messageID,
			"subject":      firstNonEmpty(util.Clean(detail["subject"]), util.Clean(item["subject"])),
			"sender":       firstNonEmpty(util.Clean(detail["from"]), util.Clean(item["from"])),
			"text_content": util.Clean(body["text"]),
			"html_content": util.Clean(body["html"]),
			"received_at":  firstNonNil(detail["date"], item["date"]),
			"to":           firstNonNil(header["To"], header["to"]),
			"raw":          detail,
		}
		if registerMessageMatchesEmail(normalized, address) {
			return normalized, nil
		}
	}
	return nil, nil
}

func registerInbucketMailboxName(address string) string {
	localPart, _, _ := strings.Cut(strings.TrimSpace(address), "@")
	return strings.TrimSpace(localPart)
}

func (p *registerYYDSMailProvider) CreateMailbox(username string) (map[string]any, error) {
	payload := map[string]any{"localPart": firstNonEmpty(strings.TrimSpace(username), registerRandomMailboxName())}

	// 域名选择：优先使用声誉系统过滤后的配置域名，其次用历史好域名，最后从 API 获取
	configDomains := util.AsStringSlice(p.entry["domain"])
	var selectedDomain string
	if len(configDomains) > 0 {
		// 用声誉系统过滤掉被拉黑的域名
		var filteredDomains []string
		if GlobalDomainReputation != nil {
			filteredDomains = GlobalDomainReputation.FilterDomains("yyds_mail", configDomains)
		} else {
			filteredDomains = configDomains
		}
		if len(filteredDomains) == 0 {
			filteredDomains = configDomains // 全被拉黑时降级使用全部
		}
		domain, err := nextRegisterDomain(filteredDomains)
		if err != nil {
			return nil, err
		}
		selectedDomain = domain
	} else {
		// 未配置域名：先用历史好域名，再从 API 获取
		if GlobalDomainReputation != nil {
			goodDomains := GlobalDomainReputation.GoodDomains("yyds_mail")
			if len(goodDomains) > 0 {
				selectedDomain = goodDomains[rand.Intn(minInt(len(goodDomains), 5))] // 从前5个好域名随机选
			}
		}
		if selectedDomain == "" {
			selectedDomain = fetchOrFallbackDomain(p)
		}
	}
	if selectedDomain != "" {
		payload["domain"] = selectedDomain
	}

	if subdomain := util.Clean(p.entry["subdomain"]); subdomain != "" {
		payload["subdomain"] = subdomain
	}
	path := "/accounts"
	if util.ToBool(p.entry["wildcard"]) {
		path = "/accounts/wildcard"
	}
	data, err := p.request(http.MethodPost, path, "", nil, payload, http.StatusOK, http.StatusCreated, http.StatusNoContent)
	if err != nil {
		return nil, err
	}
	body := util.StringMap(data)
	address := firstNonEmpty(util.Clean(body["address"]), util.Clean(body["email"]))
	token := firstNonEmpty(util.Clean(body["token"]), util.Clean(body["temp_token"]), util.Clean(body["tempToken"]), util.Clean(body["access_token"]))
	if address == "" || token == "" {
		return nil, fmt.Errorf("YYDSMail missing address or token")
	}
	// 提取实际使用的域名（用于声誉记录）
	domain := selectedDomain
	if domain == "" && strings.Contains(address, "@") {
		domain = address[strings.LastIndex(address, "@")+1:]
	}
	return map[string]any{
		"provider":     "yyds_mail",
		"provider_ref": p.entry["provider_ref"],
		"address":      address,
		"domain":       domain,
		"token":        token,
		"account_id":   util.Clean(body["id"]),
	}, nil
}

func (p *registerYYDSMailProvider) FetchLatestMessage(mailbox map[string]any) (map[string]any, error) {
	token := util.Clean(mailbox["token"])
	if token == "" {
		return nil, fmt.Errorf("YYDSMail missing token")
	}
	data, err := p.request(http.MethodGet, "/messages", token, map[string]string{"address": util.Clean(mailbox["address"])}, nil, http.StatusOK, http.StatusCreated, http.StatusNoContent)
	if err != nil {
		return nil, err
	}
	items := yydsMailItems(data)
	if len(items) == 0 {
		return nil, nil
	}
	latest := latestRegisterMailMessage(items)
	messageID := firstNonEmpty(util.Clean(latest["id"]), util.Clean(latest["message_id"]))
	message := latest
	raw := any(latest)
	if messageID != "" {
		detail, detailErr := p.request(http.MethodGet, "/messages/"+url.PathEscape(messageID), token, map[string]string{"address": util.Clean(mailbox["address"])}, nil, http.StatusOK, http.StatusCreated, http.StatusNoContent)
		if detailErr != nil {
			return nil, detailErr
		}
		raw = detail
		if detailMap := util.StringMap(detail); len(detailMap) > 0 {
			message = detailMap
		}
	}
	textContent, htmlContent := extractRegisterMailContent(message)
	sender := firstNonNil(message["from"], message["sender"])
	if senderMap, ok := sender.(map[string]any); ok {
		sender = firstNonNil(senderMap["address"], senderMap["email"], senderMap["name"])
	}
	return map[string]any{
		"provider":     "yyds_mail",
		"mailbox":      util.Clean(mailbox["address"]),
		"message_id":   messageID,
		"subject":      util.Clean(message["subject"]),
		"sender":       util.Clean(sender),
		"text_content": textContent,
		"html_content": htmlContent,
		"received_at":  firstNonNil(message["createdAt"], message["created_at"], message["receivedAt"], message["date"], message["timestamp"]),
		"raw":          raw,
	}, nil
}

func (p *registerYYDSMailProvider) FetchAvailableDomains() ([]string, error) {
	data, err := p.request(http.MethodGet, "/domains", "", nil, nil, http.StatusOK)
	if err != nil {
		return nil, err
	}
	items := util.AsMapSlice(data)
	var domains []string
	for _, item := range items {
		if !util.ToBool(item["isVerified"]) || !util.ToBool(item["isPublic"]) {
			continue
		}
		domain := util.Clean(item["domain"])
		if domain != "" {
			domains = append(domains, domain)
		}
	}
	return domains, nil
}

func (p *registerYYDSMailProvider) request(method, path, token string, query map[string]string, payload any, expected ...int) (any, error) {
	apiBase := strings.TrimRight(firstNonEmpty(util.Clean(p.entry["api_base"]), "https://maliapi.215.im/v1"), "/")
	headers := map[string]string{
		"User-Agent":   p.conf.UserAgent,
		"Accept":       "application/json",
		"Content-Type": "application/json",
	}
	if token != "" {
		headers["Authorization"] = "Bearer " + token
	} else {
		headers["X-API-Key"] = util.Clean(p.entry["api_key"])
	}
	data, err := registerMailRequestAny(p.client, method, apiBase+path, headers, query, payload, expected...)
	if err != nil {
		return nil, err
	}
	body, ok := data.(map[string]any)
	if !ok {
		return data, nil
	}
	if success, exists := body["success"]; exists && !util.ToBool(success) {
		return nil, fmt.Errorf("YYDSMail request failed: %s", firstNonEmpty(util.Clean(body["errorCode"]), util.Clean(body["error"]), util.Clean(body["message"]), "unknown error"))
	}
	if nested, exists := body["data"]; exists {
		switch nested.(type) {
		case map[string]any, []any:
			return nested, nil
		}
	}
	return data, nil
}

func yydsMailItems(data any) []map[string]any {
	switch typed := data.(type) {
	case []map[string]any:
		return typed
	case []any:
		return util.AsMapSlice(typed)
	case map[string]any:
		return util.AsMapSlice(firstNonNil(typed["items"], typed["messages"], typed["data"]))
	default:
		return nil
	}
}

func duckMailItems(data any) []map[string]any {
	switch typed := data.(type) {
	case []any:
		return util.AsMapSlice(typed)
	case map[string]any:
		return util.AsMapSlice(firstNonNil(typed["hydra:member"], typed["member"], typed["data"]))
	default:
		return nil
	}
}

func registerMailRequestJSON(client *http.Client, method, target string, headers map[string]string, query map[string]string, payload any, expected ...int) (map[string]any, error) {
	data, err := registerMailRequestAny(client, method, target, headers, query, payload, expected...)
	if err != nil {
		return nil, err
	}
	return util.StringMap(data), nil
}

func registerMailRequestAny(client *http.Client, method, target string, headers map[string]string, query map[string]string, payload any, expected ...int) (any, error) {
	var bodyReader *strings.Reader
	if payload == nil {
		bodyReader = strings.NewReader("")
	} else {
		data, err := json.Marshal(payload)
		if err != nil {
			return nil, err
		}
		bodyReader = strings.NewReader(string(data))
	}
	if len(query) > 0 {
		parsed, err := url.Parse(target)
		if err != nil {
			return nil, err
		}
		values := parsed.Query()
		for key, value := range query {
			if strings.TrimSpace(value) != "" {
				values.Set(key, value)
			}
		}
		parsed.RawQuery = values.Encode()
		target = parsed.String()
	}
	req, err := http.NewRequest(method, target, bodyReader)
	if err != nil {
		return nil, err
	}
	for key, value := range headers {
		if strings.TrimSpace(value) != "" {
			req.Header.Set(key, value)
		}
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if !registerExpectedStatus(resp.StatusCode, expected...) {
		return nil, fmt.Errorf("mail request failed: %s %s -> HTTP %d", method, target, resp.StatusCode)
	}
	if resp.StatusCode == http.StatusNoContent {
		return map[string]any{}, nil
	}
	var data any
	if err := util.DecodeJSON(resp.Body, &data); err != nil {
		return nil, err
	}
	return data, nil
}

func registerExpectedStatus(status int, expected ...int) bool {
	for _, item := range expected {
		if status == item {
			return true
		}
	}
	return false
}

func firstNonNil(values ...any) any {
	for _, value := range values {
		if value != nil {
			return value
		}
	}
	return nil
}

// ==================== Mail.tm / Mail.gw Provider ====================
// API 完全兼容，通过 api_base 区分：
// - mail_tm: https://api.mail.tm
// - mail_gw: https://api.mail.gw

func (p *registerMailTMProvider) apiBase() string {
	if base := util.Clean(p.entry["api_base"]); base != "" {
		return strings.TrimRight(base, "/")
	}
	if util.Clean(p.entry["type"]) == "mail_gw" {
		return "https://api.mail.gw"
	}
	return "https://api.mail.tm"
}

func (p *registerMailTMProvider) CreateMailbox(username string) (map[string]any, error) {
	base := p.apiBase()
	// 获取可用域名
	domains := util.AsStringSlice(p.entry["domain"])
	var domain string
	if len(domains) > 0 {
		domain = domains[rand.Intn(len(domains))]
	} else {
		domain = fetchOrFallbackDomain(p)
	}
	if domain == "" {
		return nil, fmt.Errorf("mail_tm/mail_gw: no available domain")
	}
	address := firstNonEmpty(strings.TrimSpace(username), registerRandomMailboxName()) + "@" + domain
	password := randomAlphaNum(12)
	// 创建账号
	_, err := registerMailRequestJSON(p.client, http.MethodPost, base+"/accounts", map[string]string{
		"Content-Type": "application/json",
		"User-Agent":   p.conf.UserAgent,
		"Accept":       "application/json",
	}, nil, map[string]any{"address": address, "password": password}, http.StatusOK, http.StatusCreated)
	if err != nil {
		return nil, fmt.Errorf("mail_tm create account: %w", err)
	}
	// 获取 token
	tokenData, err := registerMailRequestJSON(p.client, http.MethodPost, base+"/token", map[string]string{
		"Content-Type": "application/json",
		"User-Agent":   p.conf.UserAgent,
		"Accept":       "application/json",
	}, nil, map[string]any{"address": address, "password": password}, http.StatusOK)
	if err != nil {
		return nil, fmt.Errorf("mail_tm get token: %w", err)
	}
	token := util.Clean(tokenData["token"])
	if token == "" {
		return nil, fmt.Errorf("mail_tm/mail_gw: token response missing token")
	}
	return map[string]any{
		"provider":     util.Clean(p.entry["type"]),
		"provider_ref": p.entry["provider_ref"],
		"address":      address,
		"token":        token,
		"password":     password,
	}, nil
}

func (p *registerMailTMProvider) FetchLatestMessage(mailbox map[string]any) (map[string]any, error) {
	base := p.apiBase()
	token := util.Clean(mailbox["token"])
	// 获取消息列表
	data, err := registerMailRequestJSON(p.client, http.MethodGet, base+"/messages", map[string]string{
		"Authorization": "Bearer " + token,
		"User-Agent":    p.conf.UserAgent,
		"Accept":        "application/json",
	}, nil, nil, http.StatusOK)
	if err != nil {
		return nil, err
	}
	items := util.AsMapSlice(data["hydra:member"])
	if len(items) == 0 {
		return nil, nil
	}
	latest := latestRegisterMailMessage(items)
	// 获取消息详情
	messageID := util.Clean(latest["id"])
	if messageID != "" {
		detail, detailErr := registerMailRequestJSON(p.client, http.MethodGet, base+"/messages/"+messageID, map[string]string{
			"Authorization": "Bearer " + token,
			"User-Agent":    p.conf.UserAgent,
			"Accept":        "application/json",
		}, nil, nil, http.StatusOK)
		if detailErr == nil && len(detail) > 0 {
			latest = detail
		}
	}
	textContent := util.Clean(latest["text"])
	htmlContent := ""
	// html 字段可能是字符串数组
	switch h := latest["html"].(type) {
	case string:
		htmlContent = h
	case []any:
		var parts []string
		for _, part := range h {
			if s, ok := part.(string); ok {
				parts = append(parts, s)
			}
		}
		htmlContent = strings.Join(parts, "")
	}
	if textContent == "" && htmlContent == "" {
		textContent, htmlContent = extractRegisterMailContent(latest)
	}
	sender := ""
	if fromMap, ok := latest["from"].(map[string]any); ok {
		sender = firstNonEmpty(util.Clean(fromMap["address"]), util.Clean(fromMap["name"]))
	}
	return map[string]any{
		"provider":     util.Clean(p.entry["type"]),
		"mailbox":      util.Clean(mailbox["address"]),
		"message_id":   messageID,
		"subject":      util.Clean(latest["subject"]),
		"sender":       sender,
		"text_content": textContent,
		"html_content": htmlContent,
		"received_at":  firstNonNil(latest["createdAt"], latest["created_at"], latest["date"]),
		"raw":          latest,
	}, nil
}

func (p *registerMailTMProvider) FetchAvailableDomains() ([]string, error) {
	base := p.apiBase()
	data, err := registerMailRequestJSON(p.client, http.MethodGet, base+"/domains", map[string]string{
		"User-Agent": p.conf.UserAgent,
		"Accept":     "application/json",
	}, nil, nil, http.StatusOK)
	if err != nil {
		return nil, err
	}
	members := util.AsMapSlice(data["hydra:member"])
	var domains []string
	for _, item := range members {
		if !util.ToBool(item["isActive"]) {
			continue
		}
		domain := util.Clean(item["domain"])
		if domain != "" {
			domains = append(domains, domain)
		}
	}
	// 如果没有 isActive 字段，取所有 domain
	if len(domains) == 0 {
		for _, item := range members {
			domain := util.Clean(item["domain"])
			if domain != "" {
				domains = append(domains, domain)
			}
		}
	}
	return domains, nil
}

// ==================== GuerrillaMail Provider ====================
// 基于 session/cookie 的 API

func (p *registerGuerrillaMailProvider) apiBase() string {
	if base := util.Clean(p.entry["api_base"]); base != "" {
		return strings.TrimRight(base, "/")
	}
	return "https://api.guerrillamail.com"
}

func (p *registerGuerrillaMailProvider) CreateMailbox(username string) (map[string]any, error) {
	base := p.apiBase()
	// 获取邮箱地址
	params := map[string]string{
		"f":     "get_email_address",
		"ip":    "127.0.0.1",
		"agent": p.conf.UserAgent,
	}
	data, err := registerMailRequestJSON(p.client, http.MethodGet, base+"/ajax.php", map[string]string{
		"User-Agent": p.conf.UserAgent,
		"Accept":     "application/json",
	}, params, nil, http.StatusOK)
	if err != nil {
		return nil, fmt.Errorf("guerrilla_mail get_email_address: %w", err)
	}
	address := util.Clean(data["email_addr"])
	sidToken := util.Clean(data["sid_token"])
	if address == "" {
		return nil, fmt.Errorf("guerrilla_mail: response missing email_addr")
	}
	// 如果用户指定了 username，设置邮箱用户名
	if username = strings.TrimSpace(username); username != "" {
		setParams := map[string]string{
			"f":          "set_email_user",
			"ip":         "127.0.0.1",
			"agent":      p.conf.UserAgent,
			"email_user": username,
			"lang":       "en",
		}
		if sidToken != "" {
			setParams["sid_token"] = sidToken
		}
		setData, setErr := registerMailRequestJSON(p.client, http.MethodGet, base+"/ajax.php", map[string]string{
			"User-Agent": p.conf.UserAgent,
			"Accept":     "application/json",
		}, setParams, nil, http.StatusOK)
		if setErr == nil {
			if newAddr := util.Clean(setData["email_addr"]); newAddr != "" {
				address = newAddr
			}
			if newSid := util.Clean(setData["sid_token"]); newSid != "" {
				sidToken = newSid
			}
		}
	}
	return map[string]any{
		"provider":     "guerrilla_mail",
		"provider_ref": p.entry["provider_ref"],
		"address":      address,
		"sid_token":    sidToken,
	}, nil
}

func (p *registerGuerrillaMailProvider) FetchLatestMessage(mailbox map[string]any) (map[string]any, error) {
	base := p.apiBase()
	sidToken := util.Clean(mailbox["sid_token"])
	// 检查新邮件
	params := map[string]string{
		"f":     "check_email",
		"ip":    "127.0.0.1",
		"agent": p.conf.UserAgent,
		"seq":   "0",
	}
	if sidToken != "" {
		params["sid_token"] = sidToken
	}
	data, err := registerMailRequestJSON(p.client, http.MethodGet, base+"/ajax.php", map[string]string{
		"User-Agent": p.conf.UserAgent,
		"Accept":     "application/json",
	}, params, nil, http.StatusOK)
	if err != nil {
		return nil, err
	}
	items := util.AsMapSlice(data["list"])
	if len(items) == 0 {
		return nil, nil
	}
	// 取最新的一封
	latest := items[0]
	mailID := util.Clean(latest["mail_id"])
	if mailID == "" {
		return nil, nil
	}
	// 获取邮件详情
	fetchParams := map[string]string{
		"f":        "fetch_email",
		"ip":       "127.0.0.1",
		"agent":    p.conf.UserAgent,
		"email_id": mailID,
	}
	if sidToken != "" {
		fetchParams["sid_token"] = sidToken
	}
	message, fetchErr := registerMailRequestJSON(p.client, http.MethodGet, base+"/ajax.php", map[string]string{
		"User-Agent": p.conf.UserAgent,
		"Accept":     "application/json",
	}, fetchParams, nil, http.StatusOK)
	if fetchErr != nil {
		// 降级使用列表中的信息
		message = latest
	}
	textContent := util.Clean(message["mail_body"])
	htmlContent := ""
	if textContent == "" {
		textContent, htmlContent = extractRegisterMailContent(message)
	}
	// 如果 mail_body 包含 HTML 标签，当作 html 处理
	if strings.Contains(textContent, "<") && strings.Contains(textContent, ">") {
		htmlContent = textContent
		textContent = ""
	}
	return map[string]any{
		"provider":     "guerrilla_mail",
		"mailbox":      util.Clean(mailbox["address"]),
		"message_id":   mailID,
		"subject":      util.Clean(firstNonNil(message["mail_subject"], latest["mail_subject"])),
		"sender":       util.Clean(firstNonNil(message["mail_from"], latest["mail_from"])),
		"text_content": textContent,
		"html_content": htmlContent,
		"received_at":  firstNonNil(message["mail_timestamp"], latest["mail_timestamp"]),
		"raw":          message,
	}, nil
}

// ==================== Stalwart Provider ====================
// 自建 Stalwart 邮件服务器，通过 JMAP API 创建/删除账号，通过 IMAP 收件

func (p *registerStalwartProvider) apiBase() string {
	return strings.TrimRight(util.Clean(p.entry["api_base"]), "/")
}

func (p *registerStalwartProvider) apiKey() string {
	return util.Clean(p.entry["api_key"])
}

func (p *registerStalwartProvider) imapHost() string {
	base := p.apiBase()
	// 从 api_base 提取主机名，去掉协议前缀
	host := strings.TrimPrefix(base, "https://")
	host = strings.TrimPrefix(host, "http://")
	// 去掉路径部分
	if idx := strings.Index(host, "/"); idx >= 0 {
		host = host[:idx]
	}
	return host
}

func (p *registerStalwartProvider) CreateMailbox(username string) (map[string]any, error) {
	apiBase := p.apiBase()
	if apiBase == "" {
		return nil, fmt.Errorf("stalwart api_base is required")
	}
	apiKey := p.apiKey()
	if apiKey == "" {
		return nil, fmt.Errorf("stalwart api_key is required")
	}

	// 获取域名及其 ID
	domainMap, err := p.fetchDomainMap(apiBase, apiKey)
	if err != nil {
		return nil, fmt.Errorf("stalwart fetch domains: %w", err)
	}
	if len(domainMap) == 0 {
		return nil, fmt.Errorf("stalwart: no available domains")
	}

	// 从配置域名中选择，或随机选一个
	configDomains := util.AsStringSlice(p.entry["domain"])
	var selectedDomain, selectedDomainID string
	if len(configDomains) > 0 {
		// 收集所有在 Stalwart 中存在的配置域名
		var validDomains []string
		for _, d := range configDomains {
			d = strings.TrimSpace(strings.ToLower(d))
			if _, ok := domainMap[d]; ok {
				validDomains = append(validDomains, d)
			}
		}
		if len(validDomains) > 0 {
			// 随机选一个
			selectedDomain = validDomains[rand.Intn(len(validDomains))]
			selectedDomainID = domainMap[selectedDomain]
		}
	}
	if selectedDomain == "" {
		// 没有配置域名或全部无效，随机选一个 Stalwart 中的域名
		keys := make([]string, 0, len(domainMap))
		for k := range domainMap {
			keys = append(keys, k)
		}
		selectedDomain = keys[rand.Intn(len(keys))]
		selectedDomainID = domainMap[selectedDomain]
	}

	// 生成随机用户名和强密码
	name := strings.ToLower(firstNonEmpty(strings.TrimSpace(username), registerRandomMailboxName()))
	password := stalwartRandomPassword()
	address := name + "@" + selectedDomain

	// 通过 JMAP API 创建账号
	payload := map[string]any{
		"using": []string{"urn:ietf:params:jmap:core", "urn:stalwart:jmap"},
		"methodCalls": []any{
			[]any{
				"x:Account/set",
				map[string]any{
					"create": map[string]any{
						"new1": map[string]any{
							"@type":            "User",
							"name":             name,
							"domainId":         selectedDomainID,
							"credentials":      map[string]any{"0": map[string]any{"@type": "Password", "secret": password}},
							"memberGroupIds":   map[string]any{},
							"roles":            map[string]any{"@type": "User"},
							"permissions":      map[string]any{"@type": "Inherit"},
							"quotas":           map[string]any{},
							"aliases":          map[string]any{},
							"encryptionAtRest": map[string]any{"@type": "Disabled"},
						},
					},
				},
				"c1",
			},
		},
	}

	data, err := registerMailRequestJSON(p.client, http.MethodPost, apiBase+"/jmap/", map[string]string{
		"Authorization": "Bearer " + apiKey,
		"Content-Type":  "application/json",
		"User-Agent":    p.conf.UserAgent,
		"Accept":        "application/json",
	}, nil, payload, http.StatusOK)
	if err != nil {
		return nil, fmt.Errorf("stalwart create account: %w", err)
	}

	// 解析响应
	methodResponsesRaw, _ := data["methodResponses"].([]any)
	if len(methodResponsesRaw) == 0 {
		return nil, fmt.Errorf("stalwart: empty methodResponses")
	}
	respArr, ok := methodResponsesRaw[0].([]any)
	if !ok || len(respArr) < 2 {
		return nil, fmt.Errorf("stalwart: invalid methodResponse format")
	}
	respData := util.StringMap(respArr[1])
	created := util.StringMap(respData["created"])
	if len(created) == 0 {
		notCreated := util.StringMap(respData["notCreated"])
		if len(notCreated) > 0 {
			for _, v := range notCreated {
				if errMap, ok := v.(map[string]any); ok {
					return nil, fmt.Errorf("stalwart create failed: %s", util.Clean(errMap["description"]))
				}
			}
		}
		return nil, fmt.Errorf("stalwart: account not created")
	}

	accountID := ""
	for _, v := range created {
		if m, ok := v.(map[string]any); ok {
			accountID = util.Clean(m["id"])
			break
		}
	}

	return map[string]any{
		"provider":     "stalwart",
		"provider_ref": p.entry["provider_ref"],
		"address":      address,
		"domain":       selectedDomain,
		"password":     password,
		"account_id":   accountID,
		"api_base":     apiBase,
		"api_key":      apiKey,
	}, nil
}

// fetchDomainMap 获取 Stalwart 的域名列表，返回 domain->id 的映射（带 10 分钟缓存）
func (p *registerStalwartProvider) fetchDomainMap(apiBase, apiKey string) (map[string]string, error) {
	stalwartDomainCacheMu.Lock()
	// 检查缓存是否有效（10 分钟内）
	if cached, ok := stalwartDomainCacheData[apiBase]; ok {
		if fetched, ok2 := stalwartDomainCacheFetched[apiBase]; ok2 && time.Since(fetched) < 10*time.Minute {
			stalwartDomainCacheMu.Unlock()
			return cached, nil
		}
	}
	stalwartDomainCacheMu.Unlock()

	// 缓存过期或不存在，重新查询
	payload := map[string]any{
		"using": []string{"urn:ietf:params:jmap:core", "urn:stalwart:jmap"},
		"methodCalls": []any{
			[]any{"x:Domain/query", map[string]any{"filter": map[string]any{}}, "c1"},
			[]any{"x:Domain/get", map[string]any{
				"#ids": map[string]any{
					"resultOf": "c1",
					"name":     "x:Domain/query",
					"path":     "/ids",
				},
			}, "c2"},
		},
	}

	data, err := registerMailRequestJSON(p.client, http.MethodPost, apiBase+"/jmap/", map[string]string{
		"Authorization": "Bearer " + apiKey,
		"Content-Type":  "application/json",
		"User-Agent":    p.conf.UserAgent,
		"Accept":        "application/json",
	}, nil, payload, http.StatusOK)
	if err != nil {
		return nil, err
	}

	result := map[string]string{}
	methodResponsesRaw, _ := data["methodResponses"].([]any)
	for _, resp := range methodResponsesRaw {
		respArr, ok := resp.([]any)
		if !ok || len(respArr) < 2 {
			continue
		}
		if util.Clean(respArr[0]) != "x:Domain/get" {
			continue
		}
		respData := util.StringMap(respArr[1])
		items := util.AsMapSlice(respData["list"])
		for _, item := range items {
			id := util.Clean(item["id"])
			name := util.Clean(item["name"])
			if id != "" && name != "" {
				result[strings.ToLower(name)] = id
			}
		}
	}
	// 保存到缓存
	stalwartDomainCacheMu.Lock()
	stalwartDomainCacheData[apiBase] = result
	stalwartDomainCacheFetched[apiBase] = time.Now()
	stalwartDomainCacheMu.Unlock()
	return result, nil
}

func (p *registerStalwartProvider) FetchLatestMessage(mailbox map[string]any) (map[string]any, error) {
	address := util.Clean(mailbox["address"])
	password := util.Clean(mailbox["password"])
	apiBase := p.apiBase()
	if apiBase == "" || address == "" || password == "" {
		return nil, fmt.Errorf("stalwart: missing connection info")
	}

	// 用账号自己的凭据获取 JMAP session token
	// Stalwart 支持用邮箱账号密码通过 Basic Auth 访问 JMAP
	authHeader := "Basic " + base64.StdEncoding.EncodeToString([]byte(address+":"+password))

	// 获取 JMAP session，拿到 accountId
	sessionData, err := registerMailRequestJSON(p.client, http.MethodGet, apiBase+"/jmap/session", map[string]string{
		"Authorization": authHeader,
		"User-Agent":    p.conf.UserAgent,
		"Accept":        "application/json",
	}, nil, nil, http.StatusOK)
	if err != nil {
		// JMAP session 失败，降级到 IMAP
		return p.fetchLatestMessageIMAP(mailbox)
	}

	// 获取 accountId
	accounts := util.StringMap(sessionData["accounts"])
	accountID := ""
	for id := range accounts {
		accountID = id
		break
	}
	if accountID == "" {
		return p.fetchLatestMessageIMAP(mailbox)
	}

	// 查询收件箱邮件列表
	queryPayload := map[string]any{
		"using": []string{"urn:ietf:params:jmap:core", "urn:ietf:params:jmap:mail"},
		"methodCalls": []any{
			[]any{
				"Email/query",
				map[string]any{
					"accountId": accountID,
					"sort":      []map[string]any{{"property": "receivedAt", "isAscending": false}},
					"limit":     1,
				},
				"c1",
			},
			[]any{
				"Email/get",
				map[string]any{
					"accountId": accountID,
					"#ids": map[string]any{
						"resultOf": "c1",
						"name":     "Email/query",
						"path":     "/ids",
					},
					"properties": []string{"subject", "from", "textBody", "htmlBody", "bodyValues", "receivedAt"},
					"fetchTextBodyValues": true,
					"fetchHTMLBodyValues": true,
				},
				"c2",
			},
		},
	}

	data, err := registerMailRequestJSON(p.client, http.MethodPost, apiBase+"/jmap/", map[string]string{
		"Authorization": authHeader,
		"Content-Type":  "application/json",
		"User-Agent":    p.conf.UserAgent,
		"Accept":        "application/json",
	}, nil, queryPayload, http.StatusOK)
	if err != nil {
		return p.fetchLatestMessageIMAP(mailbox)
	}

	// 解析邮件
	methodResponsesRaw, _ := data["methodResponses"].([]any)
	for _, resp := range methodResponsesRaw {
		respArr, ok := resp.([]any)
		if !ok || len(respArr) < 2 {
			continue
		}
		if util.Clean(respArr[0]) != "Email/get" {
			continue
		}
		respData := util.StringMap(respArr[1])
		emails := util.AsMapSlice(respData["list"])
		if len(emails) == 0 {
			return nil, nil
		}
		email := emails[0]
		subject := util.Clean(email["subject"])
		bodyValues := util.StringMap(email["bodyValues"])

		// 提取正文
		textContent, htmlContent := "", ""
		textBodies := util.AsMapSlice(email["textBody"])
		htmlBodies := util.AsMapSlice(email["htmlBody"])
		for _, part := range textBodies {
			partID := util.Clean(part["partId"])
			if bv := util.StringMap(bodyValues[partID]); len(bv) > 0 {
				textContent = util.Clean(bv["value"])
				break
			}
		}
		for _, part := range htmlBodies {
			partID := util.Clean(part["partId"])
			if bv := util.StringMap(bodyValues[partID]); len(bv) > 0 {
				htmlContent = util.Clean(bv["value"])
				break
			}
		}

		// 提取发件人
		from := ""
		if fromList, ok := email["from"].([]any); ok && len(fromList) > 0 {
			if fromMap, ok := fromList[0].(map[string]any); ok {
				from = firstNonEmpty(util.Clean(fromMap["email"]), util.Clean(fromMap["name"]))
			}
		}

		if textContent == "" && htmlContent == "" {
			return nil, nil
		}

		return map[string]any{
			"provider":     "stalwart",
			"mailbox":      address,
			"subject":      subject,
			"sender":       from,
			"text_content": textContent,
			"html_content": htmlContent,
			"received_at":  util.Clean(email["receivedAt"]),
			"raw":          email,
		}, nil
	}
	return nil, nil
}

// fetchLatestMessageIMAP 降级到 IMAP 收件
func (p *registerStalwartProvider) fetchLatestMessageIMAP(mailbox map[string]any) (map[string]any, error) {
	address := util.Clean(mailbox["address"])
	password := util.Clean(mailbox["password"])
	imapHost := p.imapHost()
	if imapHost == "" {
		return nil, fmt.Errorf("stalwart: cannot determine imap host")
	}
	body, subject, from, err := stalwartIMAPFetchLatest(imapHost, 993, address, password)
	if err != nil {
		return nil, err
	}
	if body == "" && subject == "" {
		return nil, nil
	}
	textContent, htmlContent := "", ""
	if strings.Contains(body, "<") && strings.Contains(body, ">") {
		htmlContent = body
	} else {
		textContent = body
	}
	return map[string]any{
		"provider":     "stalwart",
		"mailbox":      address,
		"subject":      subject,
		"sender":       from,
		"text_content": textContent,
		"html_content": htmlContent,
		"received_at":  util.NowISO(),
		"raw":          map[string]any{"body": body},
	}, nil
}

func (p *registerStalwartProvider) Close() {
	p.client.CloseIdleConnections()
	// 注册完成后删除临时账号
	// 注意：Close 在 defer 中调用，此时 mailbox 信息已不可用
	// 删除逻辑在 CreateMailbox 返回的 mailbox 中携带必要信息，由调用方负责
}

func (p *registerStalwartProvider) DeleteMailbox(mailbox map[string]any) error {
	apiBase := util.Clean(mailbox["api_base"])
	apiKey := util.Clean(mailbox["api_key"])
	accountID := util.Clean(mailbox["account_id"])
	if apiBase == "" || apiKey == "" || accountID == "" {
		return nil
	}

	payload := map[string]any{
		"using": []string{"urn:ietf:params:jmap:core", "urn:stalwart:jmap"},
		"methodCalls": []any{
			[]any{
				"x:Account/set",
				map[string]any{
					"destroy": []string{accountID},
				},
				"c1",
			},
		},
	}

	_, err := registerMailRequestJSON(p.client, http.MethodPost, apiBase+"/jmap/", map[string]string{
		"Authorization": "Bearer " + apiKey,
		"Content-Type":  "application/json",
		"User-Agent":    p.conf.UserAgent,
		"Accept":        "application/json",
	}, nil, payload, http.StatusOK)
	return err
}

func (p *registerStalwartProvider) FetchAvailableDomains() ([]string, error) {
	apiBase := p.apiBase()
	apiKey := p.apiKey()
	if apiBase == "" || apiKey == "" {
		return nil, fmt.Errorf("stalwart: api_base and api_key required")
	}

	payload := map[string]any{
		"using": []string{"urn:ietf:params:jmap:core", "urn:stalwart:jmap"},
		"methodCalls": []any{
			[]any{"x:Domain/query", map[string]any{"filter": map[string]any{}}, "c1"},
			[]any{"x:Domain/get", map[string]any{"#ids": map[string]any{"resultOf": "c1", "name": "x:Domain/query", "path": "/ids"}}, "c2"},
		},
	}

	data, err := registerMailRequestJSON(p.client, http.MethodPost, apiBase+"/jmap/", map[string]string{
		"Authorization": "Bearer " + apiKey,
		"Content-Type":  "application/json",
		"User-Agent":    p.conf.UserAgent,
		"Accept":        "application/json",
	}, nil, payload, http.StatusOK)
	if err != nil {
		return nil, err
	}

	methodResponsesRaw2, _ := data["methodResponses"].([]any)
	var domains []string
	for _, resp := range methodResponsesRaw2 {
		respArr, ok := resp.([]any)
		if !ok || len(respArr) < 2 {
			continue
		}
		if util.Clean(respArr[0]) != "x:Domain/get" {
			continue
		}
		respData := util.StringMap(respArr[1])
		items := util.AsMapSlice(respData["list"])
		for _, item := range items {
			if name := util.Clean(item["name"]); name != "" {
				domains = append(domains, name)
			}
		}
	}
	return domains, nil
}

// stalwartRandomPassword 生成满足 Stalwart 强密码要求的随机密码
func stalwartRandomPassword() string {
	const upper = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
	const lower = "abcdefghijklmnopqrstuvwxyz"
	const digits = "0123456789"
	const special = "!@#$%^&*"
	const all = upper + lower + digits + special

	// 确保包含每种字符
	pwd := []byte{
		upper[rand.Intn(len(upper))],
		upper[rand.Intn(len(upper))],
		lower[rand.Intn(len(lower))],
		lower[rand.Intn(len(lower))],
		digits[rand.Intn(len(digits))],
		digits[rand.Intn(len(digits))],
		special[rand.Intn(len(special))],
	}
	// 补充到 16 位
	for len(pwd) < 16 {
		pwd = append(pwd, all[rand.Intn(len(all))])
	}
	// 打乱顺序
	rand.Shuffle(len(pwd), func(i, j int) { pwd[i], pwd[j] = pwd[j], pwd[i] })
	return string(pwd)
}

// stalwartIMAPFetchLatest 通过 IMAP SSL 获取最新一封邮件的正文
func stalwartIMAPFetchLatest(host string, port int, user, password string) (body, subject, from string, err error) {
	tlsConfig := &tls.Config{
		ServerName: host,
		MinVersion: tls.VersionTLS12,
	}

	addr := fmt.Sprintf("%s:%d", host, port)
	conn, dialErr := tls.Dial("tcp", addr, tlsConfig)
	if dialErr != nil {
		return "", "", "", fmt.Errorf("stalwart imap dial: %w", dialErr)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(20 * time.Second))

	readLine := func() (string, error) {
		var line []byte
		buf := make([]byte, 1)
		for {
			n, e := conn.Read(buf)
			if n > 0 {
				line = append(line, buf[0])
				if buf[0] == '\n' {
					return strings.TrimRight(string(line), "\r\n"), nil
				}
			}
			if e != nil {
				return string(line), e
			}
		}
	}

	sendCmd := func(tag, cmd string) error {
		_, e := fmt.Fprintf(conn, "%s %s\r\n", tag, cmd)
		return e
	}

	readUntilTagged := func(tag string) ([]string, error) {
		var lines []string
		for {
			line, e := readLine()
			if e != nil {
				return lines, e
			}
			lines = append(lines, line)
			if strings.HasPrefix(line, tag+" ") {
				return lines, nil
			}
		}
	}

	// 读取服务器问候
	if _, err = readLine(); err != nil {
		return "", "", "", fmt.Errorf("stalwart imap greeting: %w", err)
	}

	// LOGIN
	if err = sendCmd("A1", fmt.Sprintf("LOGIN %q %q", user, password)); err != nil {
		return "", "", "", err
	}
	lines, err := readUntilTagged("A1")
	if err != nil {
		return "", "", "", err
	}
	loginOK := false
	for _, l := range lines {
		if strings.HasPrefix(l, "A1 OK") {
			loginOK = true
			break
		}
	}
	if !loginOK {
		return "", "", "", fmt.Errorf("stalwart imap login failed")
	}

	// SELECT INBOX
	if err = sendCmd("A2", "SELECT INBOX"); err != nil {
		return "", "", "", err
	}
	if _, err = readUntilTagged("A2"); err != nil {
		return "", "", "", err
	}

	// SEARCH ALL（获取所有邮件序号）
	if err = sendCmd("A3", "SEARCH ALL"); err != nil {
		return "", "", "", err
	}
	searchLines, err := readUntilTagged("A3")
	if err != nil {
		return "", "", "", err
	}

	// 解析邮件序号，取最后一封（最新）
	var msgNums []string
	for _, l := range searchLines {
		if strings.HasPrefix(l, "* SEARCH") {
			parts := strings.Fields(l)
			if len(parts) > 2 {
				msgNums = parts[2:]
			}
		}
	}
	if len(msgNums) == 0 {
		return "", "", "", nil // 没有邮件
	}
	latestNum := msgNums[len(msgNums)-1]

	// FETCH 最新邮件的 BODY[]
	if err = sendCmd("A4", fmt.Sprintf("FETCH %s (BODY.PEEK[HEADER.FIELDS (SUBJECT FROM)] BODY.PEEK[TEXT])", latestNum)); err != nil {
		return "", "", "", err
	}
	fetchLines, err := readUntilTagged("A4")
	if err != nil && len(fetchLines) == 0 {
		return "", "", "", err
	}

	// 解析 SUBJECT、FROM 和正文
	inHeader := false
	inBody := false
	var bodyLines []string
	for _, l := range fetchLines {
		if strings.Contains(l, "HEADER.FIELDS") {
			inHeader = true
			inBody = false
			continue
		}
		if inHeader {
			if strings.HasPrefix(strings.ToUpper(l), "SUBJECT:") {
				subject = strings.TrimSpace(l[8:])
			} else if strings.HasPrefix(strings.ToUpper(l), "FROM:") {
				from = strings.TrimSpace(l[5:])
			} else if l == "" {
				inHeader = false
			}
			continue
		}
		if strings.Contains(l, "BODY[TEXT]") || strings.Contains(l, "BODY.PEEK[TEXT]") {
			inBody = true
			continue
		}
		if inBody {
			if strings.HasPrefix(l, "A4 ") || l == ")" {
				break
			}
			bodyLines = append(bodyLines, l)
		}
	}
	body = strings.Join(bodyLines, "\n")

	// LOGOUT
	_ = sendCmd("A5", "LOGOUT")

	return body, subject, from, nil
}

// ==================== AHEM Provider ====================
// Ad-Hoc Email Server，无需认证，无需创建账号，任意前缀直接收信
// 项目：https://github.com/o4oren/Ad-Hoc-Email-Server

func (p *registerAhemProvider) apiBase() string {
	return strings.TrimRight(util.Clean(p.entry["api_base"]), "/")
}

func (p *registerAhemProvider) CreateMailbox(username string) (map[string]any, error) {
	apiBase := p.apiBase()
	if apiBase == "" {
		return nil, fmt.Errorf("ahem api_base is required")
	}

	// 获取域名（配置的或从 API 获取）
	domains := util.AsStringSlice(p.entry["domain"])
	var selectedDomain string
	if len(domains) > 0 {
		selectedDomain = domains[rand.Intn(len(domains))]
	} else {
		// 从 /api/properties 获取支持的域名列表
		available, err := p.FetchAvailableDomains()
		if err != nil || len(available) == 0 {
			return nil, fmt.Errorf("ahem: no available domain")
		}
		selectedDomain = available[rand.Intn(len(available))]
	}

	// 生成随机前缀（AHEM 无需创建账号，直接用即可）
	prefix := strings.ToLower(firstNonEmpty(strings.TrimSpace(username), registerRandomMailboxName()))
	address := prefix + "@" + selectedDomain

	return map[string]any{
		"provider":     "ahem",
		"provider_ref": p.entry["provider_ref"],
		"address":      address,
		"domain":       selectedDomain,
		"prefix":       prefix,
		"api_base":     apiBase,
	}, nil
}

func (p *registerAhemProvider) FetchLatestMessage(mailbox map[string]any) (map[string]any, error) {
	apiBase := util.Clean(mailbox["api_base"])
	prefix := util.Clean(mailbox["prefix"])
	if apiBase == "" || prefix == "" {
		return nil, fmt.Errorf("ahem: missing api_base or prefix")
	}

	// 获取邮件列表
	listData, err := registerMailRequestAny(p.client, http.MethodGet, apiBase+"/api/mailbox/"+prefix+"/email", map[string]string{
		"User-Agent": p.conf.UserAgent,
		"Accept":     "application/json",
	}, nil, nil, http.StatusOK)
	if err != nil {
		return nil, err
	}

	items := util.AsMapSlice(listData)
	if len(items) == 0 {
		return nil, nil
	}

	// 取最新一封（按 timestamp 降序，取第一个）
	latest := items[0]
	for _, item := range items[1:] {
		if util.ToInt(item["timestamp"], 0) > util.ToInt(latest["timestamp"], 0) {
			latest = item
		}
	}

	emailID := util.Clean(latest["emailId"])
	if emailID == "" {
		return nil, nil
	}

	// 获取邮件详情
	detailData, err := registerMailRequestJSON(p.client, http.MethodGet, apiBase+"/api/mailbox/"+prefix+"/email/"+emailID, map[string]string{
		"User-Agent": p.conf.UserAgent,
		"Accept":     "application/json",
	}, nil, nil, http.StatusOK)
	if err != nil {
		return nil, err
	}

	// 提取正文：优先 text，其次 html（html 为 false 时表示无 HTML）
	textContent := util.Clean(detailData["text"])
	htmlContent := ""
	if h, ok := detailData["html"].(string); ok {
		htmlContent = h
	}

	// 提取发件人
	from := ""
	if fromMap, ok := detailData["from"].(map[string]any); ok {
		from = util.Clean(fromMap["text"])
	}

	subject := util.Clean(detailData["subject"])
	if textContent == "" && htmlContent == "" {
		return nil, nil
	}

	return map[string]any{
		"provider":     "ahem",
		"mailbox":      util.Clean(mailbox["address"]),
		"message_id":   emailID,
		"subject":      subject,
		"sender":       from,
		"text_content": textContent,
		"html_content": htmlContent,
		"received_at":  firstNonNil(detailData["timestamp"]),
		"raw":          detailData,
	}, nil
}

func (p *registerAhemProvider) FetchAvailableDomains() ([]string, error) {
	apiBase := p.apiBase()
	if apiBase == "" {
		return nil, fmt.Errorf("ahem api_base is required")
	}

	data, err := registerMailRequestJSON(p.client, http.MethodGet, apiBase+"/api/properties", map[string]string{
		"User-Agent": p.conf.UserAgent,
		"Accept":     "application/json",
	}, nil, nil, http.StatusOK)
	if err != nil {
		return nil, err
	}

	return util.AsStringSlice(data["allowedDomains"]), nil
}
