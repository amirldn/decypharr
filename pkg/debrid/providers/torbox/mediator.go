package torbox

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/pkg/debrid/types"
)

// Standard defaults for the Submission Mediator
const (
	DefaultCachedCheckRate       = 240.0 / 60.0       // 240 req/min (4.0/s), TorBox cap: 300/min
	DefaultUncachedCreateRate    = 45.0 / 3600.0      // 45 req/hour (0.0125/s), TorBox cap: 60/hr
	DefaultMaxQueueDepth         = 500
	DefaultMediatorRampSeconds   = 300                // 5 minutes linear relaxation
	DefaultFloorMultiplier       = 0.20               // 20% floor after 429
	DefaultQueueJournalFilename  = "submission_queue.jsonl"
)

var (
	ErrSubmissionQueueFull = errors.New("submission queue full, back off")
	ErrSubmissionCancelled = errors.New("submission cancelled by context")
)

// OpClass classifies operations for rate-valve enforcement.
type OpClass int

const (
	OpCheckCached OpClass = iota
	OpCreateCached
	OpCreateUncached
	OpControl
)

func (op OpClass) String() string {
	switch op {
	case OpCheckCached:
		return "check_cached"
	case OpCreateCached:
		return "create_cached"
	case OpCreateUncached:
		return "create_uncached"
	case OpControl:
		return "control"
	default:
		return "unknown"
	}
}

// SubmissionValve enforces token-bucket rate limits and dynamic 429 feedback loop (Mechanisms 1 & 4).
type SubmissionValve struct {
	mu                 sync.Mutex
	cachedRatePerSec   float64
	uncachedRatePerSec float64
	cachedCapacity     float64
	uncachedCapacity   float64
	cachedTokens       float64
	uncachedTokens     float64
	lastRefill         time.Time
	frozenUntil        time.Time
	rampStart          time.Time
	rampDuration       time.Duration
	floorMultiplier    float64
	consecutive429s    int
	freezes            int64
	denials            int64
	acquisitions       int64
}

// NewSubmissionValve constructs a rate valve with the given parameters.
func NewSubmissionValve(cachedPerSec, uncachedPerSec float64, rampDuration time.Duration) *SubmissionValve {
	if cachedPerSec <= 0 {
		cachedPerSec = DefaultCachedCheckRate
	}
	if uncachedPerSec <= 0 {
		uncachedPerSec = DefaultUncachedCreateRate
	}
	if rampDuration <= 0 {
		rampDuration = DefaultMediatorRampSeconds * time.Second
	}
	return &SubmissionValve{
		cachedRatePerSec:   cachedPerSec,
		uncachedRatePerSec: uncachedPerSec,
		cachedCapacity:     10.0,
		uncachedCapacity:   1.0,
		cachedTokens:       10.0,
		uncachedTokens:     1.0,
		lastRefill:         time.Now(),
		rampDuration:       rampDuration,
		floorMultiplier:    DefaultFloorMultiplier,
	}
}

func (v *SubmissionValve) refillLocked(now time.Time) {
	if v.lastRefill.IsZero() {
		v.lastRefill = now
		return
	}
	dt := now.Sub(v.lastRefill).Seconds()
	if dt <= 0 {
		return
	}
	v.lastRefill = now

	cachedRate := v.effectiveRateLocked(OpCreateCached, now)
	uncachedRate := v.effectiveRateLocked(OpCreateUncached, now)

	v.cachedTokens = math.Min(v.cachedCapacity, v.cachedTokens+dt*cachedRate)
	v.uncachedTokens = math.Min(v.uncachedCapacity, v.uncachedTokens+dt*uncachedRate)
}

func (v *SubmissionValve) effectiveRateLocked(class OpClass, now time.Time) float64 {
	nominal := v.cachedRatePerSec
	if class == OpCreateUncached {
		nominal = v.uncachedRatePerSec
	}

	if now.Before(v.frozenUntil) {
		return 0.0
	}
	if v.rampStart.IsZero() || now.After(v.rampStart.Add(v.rampDuration)) {
		return nominal
	}

	elapsed := now.Sub(v.rampStart).Seconds()
	fraction := math.Max(0.0, math.Min(1.0, elapsed/v.rampDuration.Seconds()))
	return nominal * (v.floorMultiplier + (1.0-v.floorMultiplier)*fraction)
}

// Observe429 captures an upstream HTTP 429 response, freezes the valve, and contracts rate limits.
func (v *SubmissionValve) Observe429(now time.Time, retryAfter time.Duration) {
	v.mu.Lock()
	defer v.mu.Unlock()

	v.consecutive429s++
	atomic.AddInt64(&v.freezes, 1)

	freezeDuration := retryAfter
	if freezeDuration <= 0 {
		exp := math.Pow(2, float64(int(math.Min(float64(v.consecutive429s-1), 6))))
		freezeDuration = 5 * time.Second * time.Duration(exp)
	}

	targetFreeze := now.Add(freezeDuration)
	if targetFreeze.After(v.frozenUntil) {
		v.frozenUntil = targetFreeze
	}
	v.rampStart = v.frozenUntil
}

// TryAcquire attempts to claim 1 token without blocking.
func (v *SubmissionValve) TryAcquire(class OpClass, now time.Time) bool {
	v.mu.Lock()
	defer v.mu.Unlock()

	v.refillLocked(now)

	if now.Before(v.frozenUntil) {
		atomic.AddInt64(&v.denials, 1)
		return false
	}

	if class == OpCreateUncached {
		if v.uncachedTokens >= 1.0 {
			v.uncachedTokens -= 1.0
			atomic.AddInt64(&v.acquisitions, 1)
			return true
		}
	} else {
		if v.cachedTokens >= 1.0 {
			v.cachedTokens -= 1.0
			atomic.AddInt64(&v.acquisitions, 1)
			return true
		}
	}

	atomic.AddInt64(&v.denials, 1)
	return false
}

// Acquire blocks until a token is available or the context is cancelled.
func (v *SubmissionValve) Acquire(ctx context.Context, class OpClass) error {
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()

	for {
		now := time.Now()
		if v.TryAcquire(class, now) {
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// CurrentRates returns the instantaneous effective rates for cached and uncached operations.
func (v *SubmissionValve) CurrentRates(now time.Time) (cachedRatePerMin, uncachedRatePerHour float64, isFrozen bool) {
	v.mu.Lock()
	defer v.mu.Unlock()

	isFrozen = now.Before(v.frozenUntil)
	cachedRatePerMin = v.effectiveRateLocked(OpCreateCached, now) * 60.0
	uncachedRatePerHour = v.effectiveRateLocked(OpCreateUncached, now) * 3600.0
	return cachedRatePerMin, uncachedRatePerHour, isFrozen
}

// Stats returns counters for observability.
func (v *SubmissionValve) Stats() (acquisitions, denials, freezes int64) {
	return atomic.LoadInt64(&v.acquisitions), atomic.LoadInt64(&v.denials), atomic.LoadInt64(&v.freezes)
}

// SubmissionDedup coalesces in-flight submissions by infohash (Mechanism 2).
type dedupPromise struct {
	done chan struct{}
	res  *types.Torrent
	err  error
}

type SubmissionDedup struct {
	mu        sync.Mutex
	inFlight  map[string]*dedupPromise
	coalesced int64
}

func NewSubmissionDedup() *SubmissionDedup {
	return &SubmissionDedup{
		inFlight: make(map[string]*dedupPromise),
	}
}

func (d *SubmissionDedup) RegisterOrWait(infohash string) (*dedupPromise, bool) {
	key := strings.ToLower(strings.TrimSpace(infohash))
	d.mu.Lock()
	defer d.mu.Unlock()

	if promise, exists := d.inFlight[key]; exists {
		atomic.AddInt64(&d.coalesced, 1)
		return promise, false
	}

	promise := &dedupPromise{
		done: make(chan struct{}),
	}
	d.inFlight[key] = promise
	return promise, true
}

func (d *SubmissionDedup) Complete(infohash string, res *types.Torrent, err error) {
	key := strings.ToLower(strings.TrimSpace(infohash))
	d.mu.Lock()
	defer d.mu.Unlock()

	if promise, exists := d.inFlight[key]; exists {
		promise.res = res
		promise.err = err
		close(promise.done)
		delete(d.inFlight, key)
	}
}

func (d *SubmissionDedup) InFlightCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.inFlight)
}

func (d *SubmissionDedup) CoalescedCount() int64 {
	return atomic.LoadInt64(&d.coalesced)
}

// QueueItemStatus tracks the lifecycle state of a submission in the persistent queue.
type QueueItemStatus string

const (
	StatusPending   QueueItemStatus = "PENDING"
	StatusInFlight  QueueItemStatus = "IN_FLIGHT"
	StatusCompleted QueueItemStatus = "COMPLETED"
	StatusRejected  QueueItemStatus = "REJECTED"
)

// JournalRecord represents a persistent WAL entry for crash recovery.
type JournalRecord struct {
	ID               string          `json:"id"`
	InfoHash         string          `json:"info_hash"`
	Magnet           string          `json:"magnet,omitempty"`
	DownloadUncached bool            `json:"download_uncached"`
	Status           QueueItemStatus `json:"status"`
	Reason           string          `json:"reason,omitempty"`
	CreatedAt        time.Time       `json:"created_at"`
	UpdatedAt        time.Time       `json:"updated_at"`
	Attempts         int             `json:"attempts"`
}

// PersistentSubmissionQueue manages disk-backed submission WAL and in-memory queue (Mechanism 5).
type PersistentSubmissionQueue struct {
	mu          sync.Mutex
	journalPath string
	maxDepth    int
	queue       []*JournalRecord
	inFlight    map[string]*JournalRecord
	logger      zerolog.Logger
	overflows   int64
}

func NewPersistentSubmissionQueue(journalPath string, maxDepth int, logger zerolog.Logger) (*PersistentSubmissionQueue, error) {
	if maxDepth <= 0 {
		maxDepth = DefaultMaxQueueDepth
	}
	pq := &PersistentSubmissionQueue{
		journalPath: journalPath,
		maxDepth:    maxDepth,
		queue:       make([]*JournalRecord, 0),
		inFlight:    make(map[string]*JournalRecord),
		logger:      logger,
	}

	if journalPath != "" {
		if err := pq.ReplayJournal(); err != nil {
			logger.Warn().Err(err).Str("path", journalPath).Msg("TorBox mediator: failed to replay journal, starting fresh")
		}
	}
	return pq, nil
}

func (pq *PersistentSubmissionQueue) appendJournalEntryLocked(record *JournalRecord) {
	if pq.journalPath == "" {
		return
	}
	dir := filepath.Dir(pq.journalPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		pq.logger.Error().Err(err).Str("dir", dir).Msg("failed to create journal directory")
		return
	}

	f, err := os.OpenFile(pq.journalPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		pq.logger.Error().Err(err).Str("file", pq.journalPath).Msg("failed to open submission journal")
		return
	}
	defer f.Close()

	data, err := json.Marshal(record)
	if err == nil {
		_, _ = f.Write(append(data, '\n'))
	}
}

// ReplayJournal scans the journal file on startup to restore uncommitted submissions.
func (pq *PersistentSubmissionQueue) ReplayJournal() error {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	pq.queue = pq.queue[:0]
	pq.inFlight = make(map[string]*JournalRecord)

	if _, err := os.Stat(pq.journalPath); os.IsNotExist(err) {
		return nil
	}

	f, err := os.Open(pq.journalPath)
	if err != nil {
		return err
	}
	defer f.Close()

	activeRecords := make(map[string]*JournalRecord)
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var record JournalRecord
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			continue
		}
		key := strings.ToLower(record.InfoHash)
		if record.Status == StatusCompleted || record.Status == StatusRejected {
			delete(activeRecords, key)
		} else {
			activeRecords[key] = &record
		}
	}

	for _, rec := range activeRecords {
		rec.Status = StatusPending
		pq.queue = append(pq.queue, rec)
	}

	if len(pq.queue) > 0 {
		pq.logger.Info().Int("recovered", len(pq.queue)).Msg("TorBox mediator: recovered pending submissions from journal")
	}
	return scanner.Err()
}

// Enqueue adds a submission to the persistent queue.
func (pq *PersistentSubmissionQueue) Enqueue(item *JournalRecord) (bool, error) {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	key := strings.ToLower(item.InfoHash)

	// Check if already in active queue or in flight
	for _, qItem := range pq.queue {
		if strings.ToLower(qItem.InfoHash) == key {
			return true, nil
		}
	}
	if _, exists := pq.inFlight[key]; exists {
		return true, nil
	}

	if len(pq.queue) >= pq.maxDepth {
		atomic.AddInt64(&pq.overflows, 1)
		return false, ErrSubmissionQueueFull
	}

	item.Status = StatusPending
	item.CreatedAt = time.Now()
	item.UpdatedAt = item.CreatedAt
	pq.queue = append(pq.queue, item)
	pq.appendJournalEntryLocked(item)
	return true, nil
}

// Complete updates the persistent journal and removes the item from active queue.
func (pq *PersistentSubmissionQueue) Complete(infohash string, status QueueItemStatus, reason string) {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	key := strings.ToLower(infohash)
	delete(pq.inFlight, key)

	filtered := pq.queue[:0]
	for _, item := range pq.queue {
		if strings.ToLower(item.InfoHash) != key {
			filtered = append(filtered, item)
		}
	}
	pq.queue = filtered

	record := &JournalRecord{
		InfoHash:  key,
		Status:    status,
		Reason:    reason,
		UpdatedAt: time.Now(),
	}
	pq.appendJournalEntryLocked(record)
}

func (pq *PersistentSubmissionQueue) Len() int {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	return len(pq.queue)
}

func (pq *PersistentSubmissionQueue) Overflows() int64 {
	return atomic.LoadInt64(&pq.overflows)
}

// SubmissionMediator orchestrates all 4 TorBox submission mechanisms behind one gate.
type SubmissionMediator struct {
	tb            *Torbox
	config        config.Debrid
	valve         *SubmissionValve
	dedup         *SubmissionDedup
	negativeCache *NegativeCache
	queue         *PersistentSubmissionQueue
	logger        zerolog.Logger
	enabled       bool
}

// NewSubmissionMediator constructs the complete mediator gate for TorBox.
func NewSubmissionMediator(tb *Torbox, dc config.Debrid, negCache *NegativeCache, logger zerolog.Logger) (*SubmissionMediator, error) {
	cachedRate, uncachedRate, err := parseMediatorRates(dc)
	if err != nil {
		return nil, err
	}

	rampSeconds := dc.TorboxMediatorRampSeconds
	if rampSeconds <= 0 {
		rampSeconds = DefaultMediatorRampSeconds
	}

	maxDepth := dc.TorboxMaxQueueDepth
	if maxDepth <= 0 {
		maxDepth = DefaultMaxQueueDepth
	}

	journalPath := dc.TorboxQueueJournalPath
	if journalPath == "" {
		mainPath := config.GetMainPath()
		if mainPath != "" {
			journalPath = filepath.Join(mainPath, "torbox-gate", DefaultQueueJournalFilename)
		} else {
			journalPath = filepath.Join("/data", "torbox-gate", DefaultQueueJournalFilename)
		}
	}

	valve := NewSubmissionValve(cachedRate, uncachedRate, time.Duration(rampSeconds)*time.Second)
	dedup := NewSubmissionDedup()
	queue, err := NewPersistentSubmissionQueue(journalPath, maxDepth, logger)
	if err != nil {
		return nil, err
	}

	if negCache == nil {
		negTTL, negMax, _ := negativeCacheConfig(dc)
		negCache = NewNegativeCache(negTTL, negMax)
	}

	return &SubmissionMediator{
		tb:            tb,
		config:        dc,
		valve:         valve,
		dedup:         dedup,
		negativeCache: negCache,
		queue:         queue,
		logger:        logger,
		enabled:       dc.SubmissionEnabled(),
	}, nil
}

func parseMediatorRates(dc config.Debrid) (cachedRatePerSec, uncachedRatePerSec float64, err error) {
	cachedRatePerSec = DefaultCachedCheckRate
	uncachedRatePerSec = DefaultUncachedCreateRate

	if dc.TorboxCachedCheckRate != "" {
		rate, err := parseRateString(dc.TorboxCachedCheckRate)
		if err != nil {
			return 0, 0, fmt.Errorf("torbox_cached_check_rate invalid: %w", err)
		}
		cachedRatePerSec = rate
	}

	if dc.TorboxUncachedCreateRate != "" {
		rate, err := parseRateString(dc.TorboxUncachedCreateRate)
		if err != nil {
			return 0, 0, fmt.Errorf("torbox_uncached_create_rate invalid: %w", err)
		}
		uncachedRatePerSec = rate
	}

	return cachedRatePerSec, uncachedRatePerSec, nil
}

func parseRateString(s string) (float64, error) {
	parts := strings.Split(strings.TrimSpace(s), "/")
	if len(parts) != 2 {
		return 0, fmt.Errorf("rate must be in format <count>/<unit> (e.g. 240/minute)")
	}
	count, err := strconv.ParseFloat(parts[0], 64)
	if err != nil || count <= 0 {
		return 0, fmt.Errorf("invalid rate count %q", parts[0])
	}
	unit := strings.ToLower(strings.TrimSpace(parts[1]))
	switch unit {
	case "second", "s", "sec":
		return count, nil
	case "minute", "m", "min":
		return count / 60.0, nil
	case "hour", "h", "hr":
		return count / 3600.0, nil
	case "day", "d":
		return count / 86400.0, nil
	default:
		return 0, fmt.Errorf("unknown rate unit %q", unit)
	}
}

// IsEnabled returns true if the mediator is active.
func (m *SubmissionMediator) IsEnabled() bool {
	return m != nil && m.enabled
}

// Submit mediates an incoming magnet submission through all 4 mechanisms.
func (m *SubmissionMediator) Submit(ctx context.Context, torrent *types.Torrent) (*types.Torrent, error) {
	hash := torrent.InfoHash
	if hash == "" && torrent.Magnet != nil {
		hash = torrent.Magnet.InfoHash
	}
	if hash == "" {
		return nil, fmt.Errorf("missing info hash for TorBox submission")
	}
	normalizedHash := strings.ToLower(hash)

	// Mechanism 3: Negative Cache (Futility Filter)
	if !torrent.DownloadUncached {
		if verdict, found := m.negativeCache.Get(normalizedHash); found {
			m.logger.Debug().
				Str("hash", normalizedHash).
				Str("reason", verdict.Reason).
				Time("expires_at", verdict.ExpiresAt).
				Msg("TorBox mediator: negative cache hit; fast-rejecting locally")
			return nil, fmt.Errorf("DOWNLOAD_NOT_CACHED")
		}
	}

	// Mechanism 2: In-Flight De-duplication (Singleflight)
	promise, isPrimary := m.dedup.RegisterOrWait(normalizedHash)
	if !isPrimary {
		m.logger.Debug().Str("hash", normalizedHash).Msg("TorBox mediator: coalesced duplicate submission with active in-flight request")
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-promise.done:
			return promise.res, promise.err
		}
	}

	// Mechanism 5: Bounded Persistent Queue (WAL)
	record := &JournalRecord{
		ID:               fmt.Sprintf("sub-%d", time.Now().UnixNano()),
		InfoHash:         normalizedHash,
		Magnet:           torrent.Magnet.Link,
		DownloadUncached: torrent.DownloadUncached,
	}
	if _, err := m.queue.Enqueue(record); err != nil {
		m.dedup.Complete(normalizedHash, nil, err)
		return nil, err
	}

	// Mechanism 1: Token-Bucket Valve Rate Governance
	opClass := OpCreateCached
	if torrent.DownloadUncached {
		opClass = OpCreateUncached
	}
	if err := m.valve.Acquire(ctx, opClass); err != nil {
		m.queue.Complete(normalizedHash, StatusRejected, "acquire_cancelled")
		m.dedup.Complete(normalizedHash, nil, err)
		return nil, err
	}

	// Execute Governed Wire Submission
	res, err := m.tb.executeSubmission(torrent, normalizedHash)

	if err != nil {
		if strings.Contains(err.Error(), "DOWNLOAD_NOT_CACHED") {
			m.negativeCache.Put(normalizedHash, "DOWNLOAD_NOT_CACHED", 0)
			m.queue.Complete(normalizedHash, StatusRejected, "DOWNLOAD_NOT_CACHED")
			m.dedup.Complete(normalizedHash, nil, err)
			return nil, err
		}
		if strings.Contains(err.Error(), "Status: 429") || strings.Contains(err.Error(), "Too Many Requests") {
			m.valve.Observe429(time.Now(), 30*time.Second)
			m.queue.Complete(normalizedHash, StatusRejected, "HTTP_429")
			m.dedup.Complete(normalizedHash, nil, err)
			return nil, err
		}
		m.queue.Complete(normalizedHash, StatusRejected, err.Error())
		m.dedup.Complete(normalizedHash, nil, err)
		return nil, err
	}

	// Success: Evict from negative cache & complete promise
	m.negativeCache.Evict(normalizedHash)
	m.queue.Complete(normalizedHash, StatusCompleted, "SUCCESS")
	m.dedup.Complete(normalizedHash, res, nil)
	return res, nil
}

// CheckCached mediates batch availability checks through the rate valve and negative cache.
func (m *SubmissionMediator) CheckCached(ctx context.Context, hashes []string) (map[string]bool, error) {
	result := make(map[string]bool)

	validHashes := make([]string, 0, len(hashes))
	for _, h := range hashes {
		if h == "" {
			continue
		}
		// Skip known negative-cached hashes
		if _, found := m.negativeCache.Get(h); found {
			continue
		}
		validHashes = append(validHashes, h)
	}

	if len(validHashes) == 0 {
		return result, nil
	}

	// Acquire token from valve
	if err := m.valve.Acquire(ctx, OpCheckCached); err != nil {
		return nil, err
	}

	// Execute check
	return m.tb.executeCheckCached(validHashes)
}

// LogTelemetry emits the structured status line defined in spec Section 7.1.
func (m *SubmissionMediator) LogTelemetry(event *zerolog.Event) {
	if m == nil {
		return
	}
	cachedRate, uncachedRate, isFrozen := m.valve.CurrentRates(time.Now())
	_, denials, freezes := m.valve.Stats()
	negHits, _, _, negSize := m.negativeCache.Stats()

	state := "ACTIVE"
	if !m.enabled {
		state = "DISABLED"
	} else if isFrozen {
		state = "FROZEN"
	}

	event.
		Str("mediator_state", state).
		Int("queue_depth", m.queue.Len()).
		Int("in_flight", m.dedup.InFlightCount()).
		Int("neg_cached", negSize).
		Int64("neg_hits", negHits).
		Float64("cached_rate_per_min", cachedRate).
		Float64("uncached_rate_per_hr", uncachedRate).
		Int64("denials", denials).
		Int64("freezes", freezes)
}
