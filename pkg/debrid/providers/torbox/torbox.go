package torbox

import (
	"bytes"
	"context"
	"crypto/sha256"
	stdjson "encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"path/filepath"
	"regexp"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	json "github.com/bytedance/sonic"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/customerror"
	"github.com/sirrobot01/decypharr/internal/logger"
	"github.com/sirrobot01/decypharr/internal/request"
	"github.com/sirrobot01/decypharr/internal/utils"
	"github.com/sirrobot01/decypharr/pkg/debrid/account"
	"github.com/sirrobot01/decypharr/pkg/debrid/types"
	"github.com/sirrobot01/decypharr/pkg/version"
	"go.uber.org/ratelimit"
)

var planSlots = map[string]int{
	"essential": 3,
	"standard":  5,
	"pro":       10,
}

// A manager reload may construct a second TorBox client while old workers are
// still alive. Keep their gate (and requestdl budget) shared for the process
// lifetime so a new client cannot forget an active provider ban.
var sharedThrottle = struct {
	sync.Mutex
	byProvider map[throttleKey]*request.Throttle
}{byProvider: make(map[throttleKey]*request.Throttle)}

var sharedNegativeCache = struct {
	sync.Mutex
	byProvider map[throttleKey]*NegativeCache
}{byProvider: make(map[throttleKey]*NegativeCache)}

var sharedMediator = struct {
	sync.Mutex
	byProvider map[throttleKey]*SubmissionMediator
}{byProvider: make(map[throttleKey]*SubmissionMediator)}

type throttleKey struct {
	configPath string
	apiKeyHash [sha256.Size]byte
}

type Torbox struct {
	Host                  string `json:"host"`
	APIKey                string
	accountsManager       *account.Manager
	autoExpiresLinksAfter time.Duration
	client                *request.Client
	submitClient          *request.Client
	throttle              *request.Throttle
	negativeCache         *NegativeCache
	mediator              *SubmissionMediator
	logger                zerolog.Logger
	Profile               *types.Profile
	config                config.Debrid
	downloadPresentCache  sync.Map
	downloadPresentMu     sync.Mutex
	downloadPresentLoaded bool
}

// requestdlPath is the TorBox endpoint that resolves a file into a
// short-lived CDN URL. It has a much tighter practical ceiling than the rest
// of the API, so every call to it shares one provider-wide budget.
const requestdlPath = "/api/torrents/requestdl"

func New(dc config.Debrid, ratelimits map[string]ratelimit.Limiter) (*Torbox, error) {
	cfg := config.Get()
	backoffMax, cooldown, readWait, threshold, err := throttleConfig(dc)
	if err != nil {
		return nil, err
	}
	negTTL, negMax, err := negativeCacheConfig(dc)
	if err != nil {
		return nil, err
	}
	key := throttleKey{config.GetMainPath(), sha256.Sum256([]byte(dc.APIKey))}
	sharedThrottle.Lock()
	defer sharedThrottle.Unlock()
	sharedNegativeCache.Lock()
	defer sharedNegativeCache.Unlock()
	sharedMediator.Lock()
	defer sharedMediator.Unlock()

	headers := map[string]string{
		"Authorization": fmt.Sprintf("Bearer %s", dc.APIKey),
	}
	if dc.UserAgent != "" {
		headers["User-Agent"] = dc.UserAgent
	} else {
		headers["User-Agent"] = fmt.Sprintf("Decypharr/%s (%s; %s)", version.GetInfo(), runtime.GOOS, runtime.GOARCH)
	}
	_log := logger.New(dc.Name)
	throttle, existing := sharedThrottle.byProvider[key]
	if !existing {
		throttle = request.NewThrottle(threshold, cooldown, backoffMax, _log).WithReadWait(readWait)
		if key.configPath == "" {
			return nil, fmt.Errorf("TorBox gate requires a persistent config path")
		}
		statePath := filepath.Join(key.configPath, "torbox-gate", fmt.Sprintf("%x.json", key.apiKeyHash))
		if err := throttle.WithPersistentState(statePath); err != nil {
			return nil, fmt.Errorf("TorBox gate state unavailable: %w", err)
		}
	}

	negCache, negExisting := sharedNegativeCache.byProvider[key]
	if !negExisting {
		negCache = NewNegativeCache(negTTL, negMax)
		sharedNegativeCache.byProvider[key] = negCache
	}

	mediator, medExisting := sharedMediator.byProvider[key]
	if !medExisting {
		mediator, err = NewSubmissionMediator(nil, dc, negCache, _log)
		if err != nil {
			return nil, err
		}
		sharedMediator.byProvider[key] = mediator
	}

	// TorBox enforces a hard cap of 300 req/min per API key, applied
	// synchronously across all servers since v8.4 (Feb 2026, GAP-002).
	// Default to that limit if the user has not configured one explicitly.
	mainRL := ratelimits["main"]
	if mainRL == nil {
		mainRL = ratelimit.New(300, ratelimit.Per(time.Minute), ratelimit.WithoutSlack)
	}

	submitRL := mainRL
	if !onlyUsesKey(dc.DownloadAPIKeys, dc.APIKey) {
		submitRL = ratelimits["download"]
		if submitRL == nil {
			submitRL = ratelimit.New(300, ratelimit.Per(time.Minute), ratelimit.WithoutSlack)
		}
	}
	opts := []request.ClientOption{
		request.WithHeaders(headers),
		request.WithRateLimiter(mainRL),
		request.WithMaxRetries(cfg.Retries),
		// Use the default status set (429, 500, 502, 503, 504); the previous
		// 429/502 override was redundant with DefaultRetryPolicy for these codes.
		request.WithRetryWait(time.Second, backoffMax),
		request.WithThrottle(throttle),
		request.WithLogger(_log),
	}
	if dc.Proxy != "" {
		opts = append(opts, request.WithProxy(dc.Proxy))
	}

	autoExpiresLinksAfter, err := utils.ParseDuration(dc.AutoExpireLinksAfter)
	if autoExpiresLinksAfter == 0 || err != nil {
		autoExpiresLinksAfter = 48 * time.Hour
	}

	tb := &Torbox{
		Host:                  "https://api.torbox.app/v1",
		APIKey:                dc.APIKey,
		accountsManager:       account.NewManager(dc, submitRL, _log, request.WithThrottle(throttle), request.WithRetryWait(time.Second, backoffMax)),
		config:                dc,
		autoExpiresLinksAfter: autoExpiresLinksAfter,
		client:                request.New(opts...),
		submitClient:          request.New(append(append([]request.ClientOption{}, opts...), request.WithRateLimiter(submitRL))...),
		throttle:              throttle,
		negativeCache:         negCache,
		mediator:              mediator,
		logger:                _log,
	}
	mediator.tb = tb

	// One shared /requestdl budget per configured provider, used by every
	// client and worker (API client, download accounts, streaming reads). The
	// matcher keys on this provider's host and exact endpoint path so lookalike
	// paths, redirect hops to the CDN, and unrelated API calls never consume
	// the budget.
	if !existing {
		limiter := requestdlOptions(dc, backoffMax, _log)
		throttle.UseRequestdl(limiter, requestdlMatcher(func() string { return tb.Host }))
		// One ticker per provider gate, including across manager reloads.
		throttle.StartCounterLogging(context.Background(), request.CounterLogInterval)
		sharedThrottle.byProvider[key] = throttle
	}

	return tb, nil
}

// requestdlMatcher reports whether a request targets this provider's
// /requestdl endpoint. Scheme, host, and the full path must match exactly, so
// a lookalike path (for example "/api/torrents/requestdlX") is not gated and
// redirect hops to the CDN are never charged to the budget. The host is read
// through a getter because tests point Host at a local server.
func requestdlMatcher(host func() string) func(*http.Request) bool {
	var mu sync.Mutex
	var lastHost, wantScheme, wantHost, wantPath string
	resolve := func() (string, string, string) {
		mu.Lock()
		defer mu.Unlock()
		h := host()
		if h != lastHost || wantPath == "" {
			lastHost = h
			if u, err := url.Parse(h); err == nil {
				wantScheme, wantHost = u.Scheme, u.Host
				wantPath = strings.TrimSuffix(u.Path, "/") + requestdlPath
			} else {
				wantScheme, wantHost, wantPath = "", "", ""
			}
		}
		return wantScheme, wantHost, wantPath
	}
	return func(r *http.Request) bool {
		scheme, matchHost, path := resolve()
		if path == "" || r.URL == nil {
			return false
		}
		return r.URL.Scheme == scheme && r.URL.Host == matchHost && r.URL.Path == path
	}
}

// requestdlOptions parses the shared-budget settings, falling back to safe
// defaults (with a warning) when a value is missing or invalid, so a typo can
// never remove the limit.
func requestdlOptions(dc config.Debrid, maxBackoff time.Duration, log zerolog.Logger) *request.RequestdlLimiter {
	rate := request.DefaultRequestdlBudgetPerMinute
	if dc.RequestdlBudget != "" {
		if v, ok := utils.ParseRateValue(dc.RequestdlBudget); ok && v > 0 {
			rate = v
		} else {
			log.Warn().Str("requestdl_budget", dc.RequestdlBudget).Msg("invalid requestdl_budget; using the default")
		}
	}
	if rate > request.MaxRequestdlBudgetPerMinute {
		log.Warn().Float64("requestdl_budget_per_minute", rate).Float64("max", request.MaxRequestdlBudgetPerMinute).Msg("requestdl_budget capped")
		rate = request.MaxRequestdlBudgetPerMinute
	}

	rampSeconds := dc.RequestdlRampSeconds
	if rampSeconds <= 0 || rampSeconds > 86400 {
		if dc.RequestdlRampSeconds != 0 {
			log.Warn().Int("requestdl_ramp_seconds", dc.RequestdlRampSeconds).Msg("invalid requestdl_ramp_seconds; using the default")
		}
		rampSeconds = request.DefaultRequestdlRampSeconds
	}

	freezeMax := request.DefaultRequestdlFreezeMax
	if dc.RequestdlFreezeMax != "" {
		if d, err := utils.ParseDuration(dc.RequestdlFreezeMax); err == nil && d > 0 && d <= 48*time.Hour {
			freezeMax = d
		} else {
			log.Warn().Str("requestdl_freeze_max", dc.RequestdlFreezeMax).Msg("invalid requestdl_freeze_max; using the default")
		}
	}

	return request.NewRequestdlLimiter(rate, time.Duration(rampSeconds)*time.Second, maxBackoff, freezeMax, log)
}

// RequestdlStats exposes the shared /requestdl budget for the local API.
func (tb *Torbox) RequestdlStats() any {
	return tb.throttle.RequestdlStats()
}

// RequestThrottle exposes the same provider gate to manager GET/HEAD reads.
func (tb *Torbox) RequestThrottle() *request.Throttle { return tb.throttle }

// maxThrottleDuration bounds even malformed server advice while allowing a
// genuine multi-hour TorBox ban (observed up to >24h) to be honored.
const maxThrottleDuration = 48 * time.Hour

// Durations may be configured up to 48h so a long ban can be honored rather
// than truncated to the previous 15m ceiling. The read-wait threshold is
// bounded much lower: it is how long a read blocks, not how long a ban lasts.
func throttleConfig(dc config.Debrid) (time.Duration, time.Duration, time.Duration, int, error) {
	backoffMax, cooldown, threshold := 5*time.Minute, time.Minute, 3
	readWait := request.DefaultReadWait
	for _, item := range []struct {
		name, value string
		dest        *time.Duration
	}{
		{"torbox_backoff_max", dc.TorboxBackoffMax, &backoffMax},
		{"torbox_breaker_cooldown", dc.TorboxBreakerCooldown, &cooldown},
		{"torbox_read_wait_max", dc.TorboxReadWaitMax, &readWait},
	} {
		if item.value == "" {
			continue
		}
		d, err := time.ParseDuration(item.value)
		if err != nil || d <= 0 || d > maxThrottleDuration {
			return 0, 0, 0, 0, fmt.Errorf("%s must be a positive duration no greater than 48h", item.name)
		}
		*item.dest = d
	}
	if readWait < request.MinReadWait || readWait > request.MaxReadWait {
		return 0, 0, 0, 0, fmt.Errorf("torbox_read_wait_max must be between 1s and 5m")
	}
	if dc.TorboxBreakerThreshold != 0 {
		threshold = dc.TorboxBreakerThreshold
	}
	if threshold < 1 || threshold > 100 {
		return 0, 0, 0, 0, fmt.Errorf("torbox_breaker_threshold must be between 1 and 100")
	}
	return backoffMax, cooldown, readWait, threshold, nil
}

func negativeCacheConfig(dc config.Debrid) (time.Duration, int, error) {
	ttl := DefaultNegativeCacheTTL
	maxEntries := DefaultNegativeCacheMax
	if dc.TorboxNegativeCacheTTL != "" {
		d, err := time.ParseDuration(dc.TorboxNegativeCacheTTL)
		if err != nil || d <= 0 || d > maxNegativeCacheTTL {
			return 0, 0, fmt.Errorf("torbox_negative_cache_ttl must be a positive duration no greater than 48h")
		}
		ttl = d
	}
	if dc.TorboxNegativeCacheMax != 0 {
		if dc.TorboxNegativeCacheMax < 1 || dc.TorboxNegativeCacheMax > 1000000 {
			return 0, 0, fmt.Errorf("torbox_negative_cache_max must be between 1 and 1000000")
		}
		maxEntries = dc.TorboxNegativeCacheMax
	}
	return ttl, maxEntries, nil
}

func (tb *Torbox) getNegativeCache() *NegativeCache {
	if tb.negativeCache != nil {
		return tb.negativeCache
	}
	tb.negativeCache = NewNegativeCache(DefaultNegativeCacheTTL, DefaultNegativeCacheMax)
	return tb.negativeCache
}

func (tb *Torbox) Config() config.Debrid {
	return tb.config
}

func (tb *Torbox) Logger() zerolog.Logger {
	return tb.logger
}

// doGet performs a GET request and unmarshals the response
func (tb *Torbox) doGet(endpoint string, queryParams map[string]string, result any) (*http.Response, error) {
	return tb.doGetWithClient(context.Background(), tb.client, endpoint, queryParams, result)
}

func (tb *Torbox) doGetWithClient(ctx context.Context, client *request.Client, endpoint string, queryParams map[string]string, result any) (*http.Response, error) {
	u, err := url.Parse(tb.Host + endpoint)
	if err != nil {
		return nil, err
	}

	if queryParams != nil {
		q := u.Query()
		for k, v := range queryParams {
			q.Set(k, v)
		}
		u.RawQuery = q.Encode()
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}

	return client.DoJSON(req, result)
}

// doPostForm performs a POST request with form data
func (tb *Torbox) doPostForm(endpoint string, formData map[string]string, result any) (*http.Response, error) {
	return tb.doPostFormWithClient(tb.submissionClient(), endpoint, formData, result)
}
func (tb *Torbox) doPostFormWithClient(client *request.Client, endpoint string, formData map[string]string, result any) (*http.Response, error) {
	form := url.Values{}
	for k, v := range formData {
		form.Set(k, v)
	}

	req, err := http.NewRequest(http.MethodPost, tb.Host+endpoint, strings.NewReader(form.Encode()))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := client.Do(req)
	if err != nil && (resp == nil || resp.StatusCode < 400) {
		return resp, err
	}
	defer request.DrainAndClose(resp.Body)

	if result != nil {
		bodyBytes, readErr := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		if readErr == nil && len(bodyBytes) > 0 {
			if err := json.ConfigDefault.Unmarshal(bodyBytes, result); err != nil {
				if resp.StatusCode >= 200 && resp.StatusCode < 300 {
					return resp, err
				}
			}
		}
	}

	return resp, nil
}

// doPostJSON performs a POST request with a JSON payload
func (tb *Torbox) doPostJSON(endpoint string, payload any, result any) (*http.Response, error) {
	var body io.Reader
	if payload != nil {
		data, err := json.Marshal(payload)
		if err != nil {
			return nil, err
		}
		body = bytes.NewReader(data)
	}

	req, err := http.NewRequest(http.MethodPost, tb.Host+endpoint, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	return tb.client.DoJSON(req, result)
}

func (tb *Torbox) IsAvailable(hashes []string) (map[string]bool, error) {
	if tb.mediator != nil && tb.mediator.IsEnabled() {
		return tb.mediator.CheckCached(context.Background(), hashes)
	}
	return tb.executeCheckCached(hashes)
}
func (tb *Torbox) SubmitMagnet(torrent *types.Torrent) (*types.Torrent, error) {
	if tb.mediator != nil && tb.mediator.IsEnabled() {
		return tb.mediator.Submit(context.Background(), torrent)
	}

	hash := torrent.InfoHash
	if hash == "" && torrent.Magnet != nil {
		hash = torrent.Magnet.InfoHash
	}
	return tb.executeSubmission(torrent, hash)
}

func (tb *Torbox) executeCheckCached(hashes []string) (map[string]bool, error) {
	result := make(map[string]bool)
	negCache := tb.getNegativeCache()
	for i := 0; i < len(hashes); i += 100 {
		end := min(i+100, len(hashes))
		validHashes := make([]string, 0, end-i)
		for _, hash := range hashes[i:end] {
			if hash == "" {
				continue
			}
			if _, found := negCache.Get(hash); found {
				result[hash] = false
				continue
			}
			validHashes = append(validHashes, hash)
		}
		if len(validHashes) == 0 {
			continue
		}
		var res AvailableResponse
		resp, err := tb.doGet("/api/torrents/checkcached", map[string]string{"hash": strings.Join(validHashes, ",")}, &res)
		if err != nil {
			return result, fmt.Errorf("check availability: %w", err)
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return result, fmt.Errorf("check availability: HTTP %d", resp.StatusCode)
		}
		if !res.Success {
			return result, fmt.Errorf("check availability: %v", res.Error)
		}
		cached := make(map[string]bool)
		if res.Data != nil {
			for h, item := range *res.Data {
				cached[strings.ToLower(h)] = item.Size > 0
			}
		}
		for _, h := range validHashes {
			result[h] = cached[strings.ToLower(h)]
			if result[h] {
				negCache.Evict(h)
			} else {
				negCache.Put(h, "DOWNLOAD_NOT_CACHED", 0)
			}
		}
	}
	return result, nil
}
func (tb *Torbox) executeSubmission(torrent *types.Torrent, hash string) (*types.Torrent, error) {
	var data AddMagnetResponse

	formData := map[string]string{
		"magnet": torrent.Magnet.Link,
	}

	if !torrent.DownloadUncached {
		if hash == "" {
			return nil, fmt.Errorf("missing info hash for TorBox cache check")
		}
		// Negative cache check: local fast-reject if recently found uncached
		if verdict, found := tb.getNegativeCache().Get(hash); found {
			tb.logger.Debug().
				Str("hash", hash).
				Str("reason", verdict.Reason).
				Time("expires_at", verdict.ExpiresAt).
				Msg("TorBox negative cache hit; rejecting locally without wire call")
			return nil, fmt.Errorf("DOWNLOAD_NOT_CACHED")
		}

		var availability AvailableResponse
		check, err := tb.doGet("/api/torrents/checkcached", map[string]string{"hash": hash}, &availability)
		if err != nil {
			return nil, fmt.Errorf("TorBox cache check failed: %w", err)
		}
		if check.StatusCode < 200 || check.StatusCode >= 300 || !availability.Success {
			return nil, fmt.Errorf("TorBox cache check failed: Status: %d", check.StatusCode)
		}
		cached := false
		if availability.Data != nil {
			for candidate, item := range *availability.Data {
				if strings.EqualFold(candidate, hash) && item.Size > 0 {
					cached = true
					break
				}
			}
		}
		if !cached {
			tb.getNegativeCache().Put(hash, "DOWNLOAD_NOT_CACHED", 0)
			return nil, fmt.Errorf("DOWNLOAD_NOT_CACHED")
		}
		tb.getNegativeCache().Evict(hash)
		formData["add_only_if_cached"] = "true"
	}

	resp, err := tb.doPostForm("/api/torrents/createtorrent", formData, &data)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, torboxAPIError(resp, data.Error, data.Detail)
	}
	if data.Data == nil {
		return nil, fmt.Errorf("error adding torrent")
	}
	expectedHash := hash
	torrentIdValue, err := parseAddMagnetData(*data.Data, expectedHash)
	if err != nil {
		return nil, err
	}
	if expectedHash != "" {
		tb.getNegativeCache().Evict(expectedHash)
	}
	torrentId := strconv.Itoa(torrentIdValue)
	torrent.Id = torrentId
	torrent.Debrid = tb.config.Name
	torrent.Added = time.Now()

	return torrent, nil
}

func torboxAPIError(resp *http.Response, apiError any, detail string) error {
	status := 0
	var header http.Header
	if resp != nil {
		status = resp.StatusCode
		header = resp.Header
	}
	parts := make([]string, 0, 4)
	if value := sanitizeTorboxMessage(fmt.Sprint(apiError)); value != "" && value != "<nil>" {
		parts = append(parts, "error="+strconv.Quote(value))
	}
	if value := sanitizeTorboxMessage(detail); value != "" {
		parts = append(parts, "detail="+strconv.Quote(value))
	}
	if len(parts) == 0 {
		parts = append(parts, "no body returned")
	}
	if header != nil {
		if attempts := header.Get("X-Decypharr-Attempts"); attempts != "" {
			parts = append(parts, "attempts="+attempts)
		}
		if ra := header.Get("Retry-After"); ra != "" {
			parts = append(parts, "retry-after="+strconv.Quote(ra))
		}
	}
	return fmt.Errorf("torbox API error: Status: %d (%s)", status, strings.Join(parts, ", "))
}

func sanitizeTorboxMessage(value string) string {
	value = strings.TrimSpace(value)
	value = regexp.MustCompile(`(?i)magnet:\?[^\s\"']+`).ReplaceAllString(value, "[redacted magnet]")
	const maxMessageLength = 512
	if len(value) > maxMessageLength {
		value = value[:maxMessageLength] + "..."
	}
	return value
}

func parseAddMagnetData(raw stdjson.RawMessage, expectedHash string) (int, error) {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 {
		return 0, fmt.Errorf("TorBox create torrent response contained empty data")
	}

	if raw[0] == '{' {
		var data addMagnetData
		if err := stdjson.Unmarshal(raw, &data); err != nil {
			return 0, fmt.Errorf("decode TorBox create torrent data: %w", err)
		}
		if data.torrentId() == 0 {
			return 0, fmt.Errorf("TorBox create torrent response contained no torrent id")
		}
		return data.torrentId(), nil
	}

	if raw[0] != '[' {
		return 0, fmt.Errorf("TorBox create torrent response contained unsupported data")
	}
	if expectedHash == "" {
		return 0, fmt.Errorf("cannot select TorBox torrent from array response without submitted hash")
	}

	var torrents []addMagnetData
	if err := stdjson.Unmarshal(raw, &torrents); err != nil {
		return 0, fmt.Errorf("decode TorBox create torrent array: %w", err)
	}
	for _, candidate := range torrents {
		if strings.EqualFold(candidate.Hash, expectedHash) && candidate.torrentId() != 0 {
			return candidate.torrentId(), nil
		}
	}

	return 0, fmt.Errorf("TorBox create torrent array contained no torrent matching submitted hash")
}

func (tb *Torbox) getTorboxStatus(status string, available bool) types.TorrentStatus {
	if available {
		return types.TorrentStatusDownloaded
	}
	downloading := []string{"paused", "downloading", "stalled",
		"checkingresumedata", "metadl", "pausedup", "queuedup", "checkingup",
		"forcedup", "allocating", "pauseddl", "queueddl", "checkingdl",
		"forceddl", "moving", "incomplete",
	}

	downloaded := []string{
		"completed", "cached", "uploading", "downloaded",
	}

	status = strings.ToLower(regexp.MustCompile(`\s*\(.*?\)\s*`).ReplaceAllString(status, ""))

	switch {
	case slices.Contains(downloading, status):
		return types.TorrentStatusDownloading
	case slices.Contains(downloaded, status):
		return types.TorrentStatusDownloaded
	default:
		return types.TorrentStatusError
	}
}

func torboxDownloadAvailable(data *torboxInfo) bool {
	return data != nil && (data.DownloadFinished || (data.DownloadPresent && data.Progress >= 1))
}

func torboxTorrentError(data *torboxInfo) error {
	if data == nil {
		return fmt.Errorf("torbox torrent returned no status data")
	}

	state := strings.TrimSpace(data.DownloadState)
	if state == "" {
		state = "unknown"
	}
	reason := strings.TrimSpace(fmt.Sprint(data.TrackerMessage))
	if reason == "" || reason == "<nil>" {
		reason = "no provider reason supplied"
	}
	hash := strings.TrimSpace(data.Hash)
	if hash == "" {
		hash = "unknown"
	}

	return fmt.Errorf("torbox torrent id %d hash %s state %q: %s", data.Id, hash, state, reason)
}

func (tb *Torbox) GetTorrent(torrentId string) (*types.Torrent, error) {
	var res InfoResponse

	resp, err := tb.doGet("/api/torrents/mylist", map[string]string{"id": torrentId}, &res)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("torbox API error: Status: %d", resp.StatusCode)
	}
	data := res.Data
	if data == nil {
		return nil, fmt.Errorf("error getting torrent")
	}
	t := &types.Torrent{
		Id:               strconv.Itoa(data.Id),
		Name:             data.Name,
		Bytes:            data.Size,
		Progress:         data.Progress * 100,
		Status:           tb.getTorboxStatus(data.DownloadState, torboxDownloadAvailable(data)),
		Speed:            data.DownloadSpeed,
		Seeders:          data.Seeds,
		Filename:         data.Name,
		OriginalFilename: data.Name,
		Debrid:           tb.config.Name,
		Files:            make(map[string]types.File),
		Added:            data.CreatedAt,
		InfoHash:         data.Hash,
	}
	cfg := config.Get()

	for _, f := range data.Files {
		fileName := filepath.Base(f.Name)
		pathToCheck := f.AbsolutePath
		if pathToCheck == "" {
			pathToCheck = f.Name
		}
		if err := cfg.ValidateFileAllowed(pathToCheck, f.Size); err != nil {
			continue
		}

		file := types.File{
			TorrentId: t.Id,
			Id:        strconv.Itoa(f.Id),
			Name:      fileName,
			Size:      f.Size,
			Path:      f.Name,
		}

		if torboxDownloadAvailable(data) {
			file.Link = fmt.Sprintf("torbox://%s/%d", t.Id, f.Id)
		}

		t.Files[fileName] = file
	}
	var cleanPath string
	if len(t.Files) > 0 {
		cleanPath = path.Clean(data.Files[0].Name)
	} else {
		cleanPath = path.Clean(data.Name)
	}

	t.OriginalFilename = strings.Split(cleanPath, "/")[0]
	t.Debrid = tb.config.Name

	return t, nil
}

func (tb *Torbox) loadDownloadPresent(ctx context.Context) error {
	offset := 0
	total := 0
	for {
		var res TorrentsListResponse
		resp, err := tb.doGetWithClient(ctx, tb.client, "/api/torrents/mylist", map[string]string{"offset": fmt.Sprintf("%d", offset)}, &res)
		if err != nil {
			return err
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return fmt.Errorf("torbox API error: Status: %d", resp.StatusCode)
		}
		if res.Data == nil || len(*res.Data) == 0 {
			break
		}
		for _, t := range *res.Data {
			tb.downloadPresentCache.Store(strconv.Itoa(t.Id), t.DownloadPresent)
		}
		total += len(*res.Data)
		offset += len(*res.Data)
	}
	tb.logger.Info().Int("count", total).Msg("loaded download_present cache for repair")
	return nil
}

func (tb *Torbox) updateTorrentWithClient(client *request.Client, t *types.Torrent) (*torboxInfo, error) {
	var res InfoResponse

	resp, err := tb.doGetWithClient(context.Background(), client, "/api/torrents/mylist", map[string]string{"id": t.Id}, &res)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("torbox API error: Status: %d", resp.StatusCode)
	}
	data := res.Data
	if data == nil {
		return nil, fmt.Errorf("error getting torrent")
	}
	name := data.Name

	t.Name = name
	t.Bytes = data.Size
	t.Progress = data.Progress * 100
	t.Status = tb.getTorboxStatus(data.DownloadState, torboxDownloadAvailable(data))
	t.Speed = data.DownloadSpeed
	t.Seeders = data.Seeds
	t.Filename = name
	t.OriginalFilename = name
	if data.Hash != "" {
		t.InfoHash = data.Hash
	}
	t.Debrid = tb.config.Name

	t.Files = make(map[string]types.File)

	cfg := config.Get()

	for _, f := range data.Files {
		fileName := filepath.Base(f.Name)
		pathToCheck := f.AbsolutePath
		if pathToCheck == "" {
			pathToCheck = f.Name
		}
		if err := cfg.ValidateFileAllowed(pathToCheck, f.Size); err != nil {
			continue
		}

		file := types.File{
			TorrentId: t.Id,
			Id:        strconv.Itoa(f.Id),
			Name:      fileName,
			Size:      f.Size,
			Path:      fileName,
		}

		if torboxDownloadAvailable(data) {
			file.Link = fmt.Sprintf("torbox://%s/%s", t.Id, strconv.Itoa(f.Id))
		}

		t.Files[fileName] = file
	}

	var cleanPath string
	if len(t.Files) > 0 {
		cleanPath = path.Clean(data.Files[0].Name)
	} else {
		cleanPath = path.Clean(data.Name)
	}

	t.OriginalFilename = strings.Split(cleanPath, "/")[0]
	t.Debrid = tb.config.Name
	return data, nil
}

func (tb *Torbox) UpdateTorrent(t *types.Torrent) error {
	_, err := tb.updateTorrentWithClient(tb.client, t)
	return err
}

func (tb *Torbox) CheckStatus(torrent *types.Torrent) (*types.Torrent, error) {
	for {
		data, err := tb.updateTorrentWithClient(tb.submissionClient(), torrent)

		if err != nil || torrent == nil {
			return torrent, err
		}

		switch torrent.Status {
		case types.TorrentStatusDownloaded:
			tb.logger.Info().Msgf("Torrent: %s downloaded", torrent.Name)
			return torrent, nil
		case types.TorrentStatusDownloading:
			if !torrent.DownloadUncached {
				return torrent, fmt.Errorf("torrent: %s not cached", torrent.Name)
			}
			return torrent, nil
		default:
			return torrent, torboxTorrentError(data)
		}
	}
}

func (tb *Torbox) DeleteTorrent(torrentId string) error {
	id, err := strconv.Atoi(torrentId)
	if err != nil {
		return fmt.Errorf("invalid torrent id: %s", torrentId)
	}

	payload := map[string]any{
		"torrent_id": id,
		"operation":  "delete",
		"all":        false,
	}

	resp, err := tb.doPostJSON("/api/torrents/controltorrent", payload, nil)
	if err != nil {
		return err
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("torbox API error: Status: %d", resp.StatusCode)
	}

	tb.logger.Info().Msgf("Torrent %s deleted from Torbox", torrentId)
	return nil
}

func (tb *Torbox) GetDownloadLink(ctx context.Context, id string, file *types.File) (types.DownloadLink, error) {
	return tb.accountsManager.GetDownloadLink(ctx, id, file, tb.fetchDownloadLink)
}

// GetDownloadLinkForPlayback resolves the CDN URL only when a consumer will
// read the file. Repair probes continue to use the network-free GetDownloadLink.
func (tb *Torbox) GetDownloadLinkForPlayback(ctx context.Context, id string, file *types.File) (types.DownloadLink, error) {
	dl, err := tb.GetDownloadLink(ctx, id, file)
	if err != nil || dl.Empty() {
		return dl, err
	}
	if !strings.HasPrefix(dl.DownloadLink, tb.Host+requestdlPath+"?") && (dl.ExpiresAt.IsZero() || time.Now().Before(dl.ExpiresAt)) {
		return dl, nil
	}
	account, err := tb.accountsManager.GetAccount(dl.Token)
	if err != nil {
		return types.DownloadLink{}, err
	}
	resolved, err := tb.resolveDownloadLink(ctx, account, id, file)
	if err != nil {
		// Never leave a requestdl placeholder (or expired CDN URL) in the
		// account cache after resolution failed.
		_ = tb.DeleteLink(dl)
		return types.DownloadLink{}, err
	}
	// A repair probe may have cached the cheap placeholder. Replace it only
	// after a complete, successful response; failures leave no CDN URL stored.
	tb.accountsManager.StoreDownloadLink(resolved)
	return resolved, nil
}

func (tb *Torbox) resolveDownloadLink(ctx context.Context, account *account.Account, id string, file *types.File) (types.DownloadLink, error) {
	query := url.Values{}
	query.Set("token", account.Token)
	query.Set("torrent_id", id)
	query.Set("file_id", file.Id)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, tb.Host+requestdlPath+"?"+query.Encode(), nil)
	if err != nil {
		return types.DownloadLink{}, request.RedactURLError(err)
	}
	resp, err := tb.client.DoOnce(req)
	if err != nil {
		return types.DownloadLink{}, request.RedactURLError(err)
	}
	defer request.DrainAndClose(resp.Body)
	if resp.StatusCode == http.StatusTooManyRequests && tb.throttle != nil {
		return types.DownloadLink{}, &request.ThrottleError{RetryAfter: tb.throttle.Remaining()}
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return types.DownloadLink{}, tb.requestdlFailure(resp)
	}
	var result DownloadLinksResponse
	if err := json.ConfigDefault.NewDecoder(resp.Body).Decode(&result); err != nil {
		return types.DownloadLink{}, fmt.Errorf("torbox requestdl response: %w", err)
	}
	if !result.Success || result.Data == nil || *result.Data == "" {
		return types.DownloadLink{}, tb.requestdlFailureResponse(resp.StatusCode, result.Error)
	}
	expiry := 3 * time.Hour
	if tb.autoExpiresLinksAfter > 0 && tb.autoExpiresLinksAfter < expiry {
		expiry = tb.autoExpiresLinksAfter
	}
	now := time.Now()
	dl := types.DownloadLink{Filename: file.Name, Size: file.Size, Token: account.Token, Link: file.Link,
		DownloadLink: *result.Data, Debrid: tb.config.Name, Id: file.Id, Generated: now, ExpiresAt: now.Add(expiry)}
	if err := dl.Valid(); err != nil {
		return types.DownloadLink{}, fmt.Errorf("torbox requestdl returned invalid CDN URL: %w", err)
	}
	return dl, nil
}

// Only TorBox's machine-readable code is safe to put in a log or read error.
// The detail field and the request URL may contain account tokens or links.
var torboxErrorCode = regexp.MustCompile(`^[A-Z][A-Z0-9_]{0,127}$`)

func (tb *Torbox) requestdlFailure(resp *http.Response) error {
	var result struct {
		Error any `json:"error"`
	}
	_ = json.ConfigDefault.NewDecoder(io.LimitReader(resp.Body, 4096)).Decode(&result)
	return tb.requestdlFailureResponse(resp.StatusCode, result.Error)
}

func (tb *Torbox) requestdlFailureResponse(status int, providerError any) error {
	code, _ := providerError.(string)
	if !torboxErrorCode.MatchString(code) {
		code = "UNKNOWN"
	}
	tb.logger.Warn().Int("http_status", status).Str("provider_error_code", code).Msg("TorBox requestdl failed")
	return fmt.Errorf("torbox requestdl failed: HTTP %d provider_error_code=%s", status, code)
}

func (tb *Torbox) fetchDownloadLink(ctx context.Context, account *account.Account, id string, file *types.File) (types.DownloadLink, error) {
	query := url.Values{}
	query.Set("token", account.Token)
	query.Set("torrent_id", id)
	query.Set("file_id", file.Id)
	query.Set("redirect", "true")

	downloadURL := fmt.Sprintf("%s%s?%s", tb.Host, requestdlPath, query.Encode())

	now := time.Now()

	// Always expires
	dl := types.DownloadLink{
		Filename:     file.Name,
		Size:         file.Size,
		Token:        account.Token,
		Link:         file.Link,
		DownloadLink: downloadURL,
		Debrid:       tb.config.Name,
		Id:           file.Id,
		Generated:    now,
		ExpiresAt:    now.Add(tb.autoExpiresLinksAfter),
	}
	return dl, nil
}

func (tb *Torbox) GetTorrents() ([]*types.Torrent, error) {
	offset := 0
	allTorrents := make([]*types.Torrent, 0)

	for {
		torrents, err := tb.getTorrents(offset)
		if err != nil {
			return nil, fmt.Errorf("get TorBox torrents at offset %d: %w", offset, err)
		}
		if len(torrents) == 0 {
			break
		}
		allTorrents = append(allTorrents, torrents...)
		offset += len(torrents)
	}
	return allTorrents, nil
}

func (tb *Torbox) getTorrents(offset int) ([]*types.Torrent, error) {
	var res TorrentsListResponse

	resp, err := tb.doGet("/api/torrents/mylist", map[string]string{
		"bypass_cache": "true",
		"offset":       strconv.Itoa(offset),
	}, &res)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("torbox API error: Status: %d", resp.StatusCode)
	}

	if !res.Success || res.Data == nil {
		return nil, fmt.Errorf("torbox API error: %v", res.Error)
	}

	torrents := make([]*types.Torrent, 0, len(*res.Data))
	cfg := config.Get()

	for _, data := range *res.Data {
		t := &types.Torrent{
			Id:               strconv.Itoa(data.Id),
			Name:             data.Name,
			Bytes:            data.Size,
			Progress:         data.Progress * 100,
			Status:           tb.getTorboxStatus(data.DownloadState, torboxDownloadAvailable(&data)),
			Speed:            data.DownloadSpeed,
			Seeders:          data.Seeds,
			Filename:         data.Name,
			OriginalFilename: data.Name,
			Debrid:           tb.config.Name,
			Files:            make(map[string]types.File),
			Added:            data.CreatedAt,
			InfoHash:         data.Hash,
		}

		for _, f := range data.Files {
			fileName := filepath.Base(f.Name)
			pathToCheck := f.AbsolutePath
			if pathToCheck == "" {
				pathToCheck = f.Name
			}
			if err := cfg.ValidateFileAllowed(pathToCheck, f.Size); err != nil {
				continue
			}
			file := types.File{
				TorrentId: t.Id,
				Id:        strconv.Itoa(f.Id),
				Name:      fileName,
				Size:      f.Size,
				Path:      f.Name,
			}

			if torboxDownloadAvailable(&data) {
				file.Link = fmt.Sprintf("torbox://%s/%d", t.Id, f.Id)
			}

			t.Files[fileName] = file
		}

		var cleanPath string
		if len(t.Files) > 0 {
			cleanPath = path.Clean(data.Files[0].Name)
		} else {
			cleanPath = path.Clean(data.Name)
		}
		t.OriginalFilename = strings.Split(cleanPath, "/")[0]

		torrents = append(torrents, t)
	}

	return torrents, nil
}

func (tb *Torbox) fetchDownloadLinks(account *account.Account) ([]types.DownloadLink, error) {
	return []types.DownloadLink{}, nil
}

func (tb *Torbox) RefreshDownloadLinks() error {
	return tb.accountsManager.RefreshLinks(tb.fetchDownloadLinks)
}

func (tb *Torbox) CheckFile(ctx context.Context, infohash, link string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	tb.downloadPresentMu.Lock()
	if err := ctx.Err(); err != nil {
		tb.downloadPresentMu.Unlock()
		return err
	}
	if !tb.downloadPresentLoaded {
		if err := tb.loadDownloadPresent(ctx); err != nil {
			tb.downloadPresentMu.Unlock()
			return err
		}
		tb.downloadPresentLoaded = true
	}
	tb.downloadPresentMu.Unlock()

	torrentID := link
	if after, ok := strings.CutPrefix(link, "torbox://"); ok {
		parts := strings.SplitN(after, "/", 2)
		if len(parts) > 0 {
			torrentID = parts[0]
		}
	}

	if err := ctx.Err(); err != nil {
		return err
	}
	if present, ok := tb.downloadPresentCache.Load(torrentID); ok {
		if !present.(bool) {
			return customerror.HosterUnavailableError
		}
		return nil
	}
	return customerror.HosterUnavailableError
}

func (tb *Torbox) GetAvailableSlots() (int, error) {
	var accountSlots = 1
	profile, err := tb.GetProfile()
	if err != nil {
		return 0, err
	}

	if slots, ok := planSlots[profile.Type]; ok {
		accountSlots = slots
	}
	return accountSlots, nil
}

func (tb *Torbox) GetProfile() (*types.Profile, error) {
	if tb.Profile != nil {
		return tb.Profile, nil
	}
	var data ProfileResponse

	resp, err := tb.doGet("/api/user/me", map[string]string{"settings": "true"}, &data)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("torbox API error: Status: %d", resp.StatusCode)
	}

	userData := data.Data
	if userData == nil {
		return nil, fmt.Errorf("error getting user profile")
	}

	expiration, err := time.Parse(time.RFC3339, userData.PremiumExpiresAt)
	if err != nil {
		expiration = time.Time{}
	}

	profile := &types.Profile{
		Name:       tb.config.Name,
		Id:         userData.Id,
		Username:   userData.Email,
		Email:      userData.Email,
		Expiration: expiration,
	}

	switch userData.Plan {
	case 1:
		profile.Type = "essential"
	case 2:
		profile.Type = "pro"
	case 3:
		profile.Type = "standard"
	default:
		profile.Type = "free"
	}

	tb.Profile = profile

	return profile, nil
}

func (tb *Torbox) AccountManager() *account.Manager {
	return tb.accountsManager
}

func (tb *Torbox) syncAccount(account *account.Account) error {
	return nil
}

func (tb *Torbox) SyncAccounts() {
	tb.accountsManager.Sync(tb.syncAccount)
}

func (tb *Torbox) deleteDownloadLink(account *account.Account, downloadLink types.DownloadLink) error {
	return nil
}

func (tb *Torbox) DeleteLink(downloadLink types.DownloadLink) error {
	return tb.accountsManager.DeleteDownloadLink(downloadLink, tb.deleteDownloadLink)
}

// SpeedTest measures API latency and download speed using cached links
func (tb *Torbox) SpeedTest(ctx context.Context) types.SpeedTestResult {
	// Speed probes are the lowest priority lane: they must never consume
	// capacity a playback read needs, and they are suppressed entirely during
	// a penalty window by the shared budget.
	ctx = request.WithClass(ctx, request.ClassProbe)
	result := types.SpeedTestResult{
		Provider: tb.config.Name,
		TestedAt: time.Now(),
	}

	start := time.Now()
	resp, err := tb.doGet("/api/user/me", nil, nil)
	latency := time.Since(start)

	if err != nil {
		result.Error = fmt.Sprintf("latency test failed: %v", err)
		return result
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		result.Error = fmt.Sprintf("latency test unexpected status: %d", resp.StatusCode)
		return result
	}
	result.LatencyMs = latency.Milliseconds()

	// Try to measure download speed using a cached link
	current := tb.accountsManager.Current()
	if current == nil {
		return result
	}

	link, found := current.GetRandomLink()
	if !found || link.DownloadLink == "" {
		return result
	}

	// Download first 1MB to measure speed
	const downloadSize = 1 * 1024 * 1024 // 1MB
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, link.DownloadLink, nil)
	if err != nil {
		return result
	}
	req.Header.Set("Range", fmt.Sprintf("bytes=0-%d", downloadSize-1))

	downloadStart := time.Now()
	dlResp, err := current.Client().Do(req)
	if err != nil {
		return result
	}
	defer dlResp.Body.Close()

	data, err := io.ReadAll(dlResp.Body)
	downloadDuration := time.Since(downloadStart)

	if err != nil || len(data) == 0 {
		return result
	}

	result.BytesRead = int64(len(data))
	if downloadDuration.Seconds() > 0 {
		result.SpeedMBps = float64(result.BytesRead) / downloadDuration.Seconds() / (1024 * 1024)
	}

	return result
}

func (tb *Torbox) SupportsCheck() bool {
	return true
}

func onlyUsesKey(downloadKeys []string, apiKey string) bool {
	for _, key := range downloadKeys {
		if key != "" && key != apiKey {
			return false
		}
	}
	return true
}

func (tb *Torbox) submissionClient() *request.Client {
	if tb.submitClient != nil {
		return tb.submitClient
	}
	return tb.client
}
