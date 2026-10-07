package request

import (
	"context"
	"math"
	"math/rand/v2"
	"net/http"
	"sync"
	"time"

	"github.com/rs/zerolog"
)

// Defaults for the shared TorBox /requestdl budget. They are deliberately
// conservative: TorBox penalizes this endpoint far below its documented
// general API limit, so the budget is the account's practical read ceiling.
const (
	DefaultRequestdlBudgetPerMinute = 12.0
	DefaultRequestdlRampSeconds     = 300
	// DefaultRequestdlFreezeMax bounds the raw server Retry-After the bucket
	// honors. It is deliberately independent of torbox_backoff_max so a long
	// ban freezes the bucket for the full server window even when the breaker
	// clamps its own cooldown.
	DefaultRequestdlFreezeMax = 48 * time.Hour

	// MaxRequestdlBudgetPerMinute bounds a misconfigured budget so a typo
	// cannot remove the limit entirely.
	MaxRequestdlBudgetPerMinute = 600.0
	// requestdlRampFloor is the fraction of the budget the limiter starts at
	// when it resumes after a penalty window.
	requestdlRampFloor = 0.2
	// requestdlBackoffBase is the first full-jitter backoff ceiling.
	requestdlBackoffBase = time.Second
	// requestdlPriorityPoll is how often a low-priority waiter re-checks while
	// playback is waiting, so background work cannot hold a token playback
	// needs.
	requestdlPriorityPoll = 25 * time.Millisecond
)

// Class is the priority lane a /requestdl request belongs to. Playback is the
// high lane and is never blocked by background work; background and probe
// share the leftover capacity.
type Class int

const (
	// ClassBackground is the default lane for bulk or speculative work
	// (downloads, prefetch, anything unlabelled).
	ClassBackground Class = iota
	// ClassProbe covers health, repair, canary, and speed probes.
	ClassProbe
	// ClassPlayback covers interactive reads (mount, WebDAV, share streaming).
	ClassPlayback
)

func (c Class) String() string {
	switch c {
	case ClassPlayback:
		return "playback"
	case ClassProbe:
		return "probe"
	default:
		return "background"
	}
}

func (c Class) normalized() Class {
	if c < ClassBackground || c > ClassPlayback {
		return ClassBackground
	}
	return c
}

type classContextKey struct{}

// WithClass tags ctx with the requestdl priority lane. Callers on the playback
// path must tag their context; the default is the low background lane so
// unlabelled traffic can never starve playback.
func WithClass(ctx context.Context, class Class) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, classContextKey{}, class.normalized())
}

// ClassOf returns the priority lane tagged on ctx, defaulting to background.
func ClassOf(ctx context.Context) Class {
	if ctx != nil {
		if class, ok := ctx.Value(classContextKey{}).(Class); ok {
			return class.normalized()
		}
	}
	return ClassBackground
}

// RequestdlStats is a point-in-time snapshot of the shared /requestdl budget.
type RequestdlStats struct {
	BudgetPerMinute      float64   `json:"budget_per_minute"`
	CurrentRatePerMinute float64   `json:"current_rate_per_minute"`
	RampActive           bool      `json:"ramp_active"`
	RampProgress         float64   `json:"ramp_progress"`
	QueuedPlayback       int       `json:"queued_playback"`
	QueuedBackground     int       `json:"queued_background"`
	QueuedProbe          int       `json:"queued_probe"`
	RequestsPlayback     uint64    `json:"requests_playback"`
	RequestsBackground   uint64    `json:"requests_background"`
	RequestsProbe        uint64    `json:"requests_probe"`
	Denials              uint64    `json:"denials"`
	PenaltyWindows       uint64    `json:"penalty_windows"`
	PenaltyUntil         time.Time `json:"penalty_until,omitempty"`
}

// RequestdlLimiter is a process-wide token bucket for one provider's
// /requestdl endpoint. Every worker, client, and feature shares it through the
// provider's Throttle; there is no per-worker limiter.
//
// It layers on top of the existing circuit breaker: Retry-After freezes the
// whole bucket (the breaker already refuses calls during the window), after a
// penalty the rate ramps back from requestdlRampFloor to 100%, playback wins
// every contention, and 5xx pauses are per class with full jitter.
type RequestdlLimiter struct {
	mu  sync.Mutex
	log zerolog.Logger

	ratePerSec float64
	burst      float64
	tokens     float64
	last       time.Time

	rampPeriod  time.Duration
	rampStart   time.Time
	ramping     bool
	rampPending bool

	penaltyUntil time.Time

	classPause [3]time.Time
	classFails [3]int
	maxBackoff time.Duration
	freezeMax  time.Duration

	waiting   [3]int
	counts    [3]uint64
	denials   uint64
	penalties uint64

	now   func() time.Time
	sleep func(context.Context, time.Duration) error
	randN func(int64) int64
}

// NewRequestdlLimiter builds the shared bucket. Zero or invalid values fall
// back to safe defaults rather than failing provider construction. maxBackoff
// bounds the per-class 5xx jitter; freezeMax bounds the raw server Retry-After
// the bucket honors for a 429.
func NewRequestdlLimiter(ratePerMinute float64, rampPeriod, maxBackoff, freezeMax time.Duration, log zerolog.Logger) *RequestdlLimiter {
	if !(ratePerMinute > 0) || math.IsInf(ratePerMinute, 0) || math.IsNaN(ratePerMinute) {
		ratePerMinute = DefaultRequestdlBudgetPerMinute
	}
	if ratePerMinute > MaxRequestdlBudgetPerMinute {
		ratePerMinute = MaxRequestdlBudgetPerMinute
	}
	if rampPeriod < 0 {
		rampPeriod = 0
	}
	if maxBackoff <= 0 {
		maxBackoff = 5 * time.Minute
	}
	if freezeMax <= 0 {
		freezeMax = DefaultRequestdlFreezeMax
	}
	l := &RequestdlLimiter{
		ratePerSec: ratePerMinute / 60,
		rampPeriod: rampPeriod,
		maxBackoff: maxBackoff,
		freezeMax:  freezeMax,
		log:        log,
		now:        time.Now,
		sleep:      sleepContext,
		randN:      rand.Int64N,
	}
	// Allow a single queued token so the budget is a hard ceiling: at the
	// default 12/minute the bucket admits one request every five seconds.
	l.burst = math.Max(1, l.ratePerSec)
	l.tokens = l.burst
	l.last = l.now()
	return l
}

// BudgetPerMinute is the configured rate.
func (l *RequestdlLimiter) BudgetPerMinute() float64 { return l.ratePerSec * 60 }

// RampPeriod is the configured post-penalty ramp duration (zero disables it).
func (l *RequestdlLimiter) RampPeriod() time.Duration {
	if l == nil {
		return 0
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.rampPeriod
}

// FreezeMax is the ceiling on the raw server Retry-After the bucket honors.
func (l *RequestdlLimiter) FreezeMax() time.Duration {
	if l == nil {
		return 0
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.freezeMax
}

// CurrentRatePerMinute is the ramped rate in effect right now.
func (l *RequestdlLimiter) CurrentRatePerMinute() float64 {
	if l == nil {
		return 0
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.rateNowLocked(l.now()) * 60
}

// Take admits one /requestdl call in class, blocking until a token is
// available. It fails fast with a ThrottleError while a penalty window is
// open, so no requestdl call (probe, canary, retry, or read) is issued during
// the window.
func (l *RequestdlLimiter) Take(ctx context.Context, class Class) error {
	if l == nil {
		return nil
	}
	class = class.normalized()
	l.mu.Lock()
	// The waiter stays registered until admitted or cancelled, so a playback
	// waiter cannot be mistaken for absent between a wake-up and re-check.
	l.waiting[class]++
	defer func() {
		l.waiting[class]--
		l.mu.Unlock()
	}()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		now := l.now()
		if now.Before(l.penaltyUntil) {
			l.denials++
			return &ThrottleError{RetryAfter: l.penaltyUntil.Sub(now)}
		}
		if pause := l.classPause[class]; now.Before(pause) {
			wait := pause.Sub(now)
			l.mu.Unlock()
			err := l.sleep(ctx, wait)
			l.mu.Lock()
			if err != nil {
				return err
			}
			continue
		}
		l.refillLocked(now)
		if l.admitLocked(class) {
			if l.rampPending {
				l.rampPending = false
				l.ramping = l.rampPeriod > 0
				l.rampStart = now
			}
			return nil
		}
		wait := l.waitLocked(now, class)
		l.mu.Unlock()
		err := l.sleep(ctx, wait)
		l.mu.Lock()
		if err != nil {
			return err
		}
	}
}

// admitLocked consumes a token for class when its lane is eligible. Playback
// always wins: the low lanes are only served when no playback request is
// waiting.
func (l *RequestdlLimiter) admitLocked(class Class) bool {
	if class != ClassPlayback && l.waiting[ClassPlayback] > 0 {
		return false
	}
	if l.tokens < 1 {
		return false
	}
	l.tokens--
	l.counts[class]++
	return true
}

// waitLocked returns how long to sleep before re-checking admission. A low
// lane polls while playback waits so it yields the next token. It returns
// zero only when a token is available and the caller is eligible, which cannot
// happen here because admitLocked already failed.
func (l *RequestdlLimiter) waitLocked(now time.Time, class Class) time.Duration {
	if class != ClassPlayback && l.waiting[ClassPlayback] > 0 {
		return requestdlPriorityPoll
	}
	rate := l.rateNowLocked(now)
	if rate <= 0 {
		return requestdlPriorityPoll
	}
	if missing := 1 - l.tokens; missing > 0 {
		if d := time.Duration(float64(time.Second) * missing / rate); d > time.Millisecond {
			return d
		}
	}
	return requestdlPriorityPoll
}

func (l *RequestdlLimiter) refillLocked(now time.Time) {
	if l.last.IsZero() {
		l.last = now
		return
	}
	elapsed := now.Sub(l.last)
	if elapsed <= 0 {
		return
	}
	l.tokens = math.Min(l.burst, l.tokens+elapsed.Seconds()*l.rateNowLocked(now))
	l.last = now
	if l.ramping && l.rampPeriod > 0 && now.Sub(l.rampStart) >= l.rampPeriod {
		l.ramping = false
	}
}

// rateNowLocked applies the post-penalty ramp: requestdlRampFloor of the
// configured rate at rampStart, reaching 100% after rampPeriod.
func (l *RequestdlLimiter) rateNowLocked(now time.Time) float64 {
	if !l.ramping || l.rampPeriod <= 0 {
		return l.ratePerSec
	}
	elapsed := now.Sub(l.rampStart)
	if elapsed <= 0 {
		return l.ratePerSec * requestdlRampFloor
	}
	progress := float64(elapsed) / float64(l.rampPeriod)
	if progress >= 1 {
		l.ramping = false
		return l.ratePerSec
	}
	return l.ratePerSec * (requestdlRampFloor + (1-requestdlRampFloor)*progress)
}

// Observe records a /requestdl response. A 429 freezes the whole bucket for
// the server's raw Retry-After (bounded only by requestdl_freeze_max), never
// for the breaker's clamped cooldown, and arms the post-window ramp; 5xx pauses
// only the request's class with a bounded full-jitter backoff. penalty is the
// breaker's remaining cooldown and is used as a floor, so an escalated breaker
// wait still holds the bucket even without server advice.
func (l *RequestdlLimiter) Observe(resp *http.Response, class Class, penalty time.Duration) {
	if l == nil || resp == nil {
		return
	}
	class = class.normalized()
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	switch {
	case resp.StatusCode == http.StatusTooManyRequests:
		l.penalties++
		l.classFails[class]++
		wait := penalty
		if serverWait, ok := retryAfterWait(resp); ok && serverWait > 0 {
			if serverWait > l.freezeMax {
				serverWait = l.freezeMax
			}
			wait = max(wait, serverWait)
		}
		if wait <= 0 {
			wait = l.fullJitterLocked(class)
		}
		if until := now.Add(wait); until.After(l.penaltyUntil) {
			l.penaltyUntil = until
		}
		l.rampPending = true
	case resp.StatusCode >= 500:
		l.classFails[class]++
		wait := l.fullJitterLocked(class)
		if until := now.Add(wait); until.After(l.classPause[class]) {
			l.classPause[class] = until
		}
	case resp.StatusCode >= 200 && resp.StatusCode < 400:
		l.classFails[class] = 0
	}
}

// fullJitterLocked returns a bounded full-jitter delay for class's consecutive
// failures: uniform in [0, ceiling] with an exponential ceiling.
func (l *RequestdlLimiter) fullJitterLocked(class Class) time.Duration {
	ceiling := requestdlBackoffBase
	for i := 1; i < l.classFails[class]; i++ {
		if ceiling >= l.maxBackoff {
			break
		}
		if ceiling > l.maxBackoff/2 {
			ceiling = l.maxBackoff
			break
		}
		ceiling *= 2
	}
	if ceiling > l.maxBackoff {
		ceiling = l.maxBackoff
	}
	if ceiling <= 0 {
		return 0
	}
	return time.Duration(l.randN(int64(ceiling) + 1))
}

// Snapshot returns the shared bucket state for logs and the local API.
func (l *RequestdlLimiter) Snapshot() RequestdlStats {
	if l == nil {
		return RequestdlStats{}
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	stats := RequestdlStats{
		BudgetPerMinute:      l.ratePerSec * 60,
		CurrentRatePerMinute: l.rateNowLocked(now) * 60,
		RampActive:           l.ramping,
		QueuedPlayback:       l.waiting[ClassPlayback],
		QueuedBackground:     l.waiting[ClassBackground],
		QueuedProbe:          l.waiting[ClassProbe],
		RequestsPlayback:     l.counts[ClassPlayback],
		RequestsBackground:   l.counts[ClassBackground],
		RequestsProbe:        l.counts[ClassProbe],
		Denials:              l.denials,
		PenaltyWindows:       l.penalties,
	}
	if l.ramping && l.rampPeriod > 0 {
		if p := float64(now.Sub(l.rampStart)) / float64(l.rampPeriod); p > 0 {
			stats.RampProgress = math.Min(1, p)
		}
	}
	if !l.rampStart.IsZero() && l.rampPeriod > 0 {
		if p := float64(now.Sub(l.rampStart)) / float64(l.rampPeriod); p > stats.RampProgress {
			stats.RampProgress = math.Min(1, p)
		}
	}
	if now.Before(l.penaltyUntil) {
		stats.PenaltyUntil = l.penaltyUntil
	}
	return stats
}

// FrozenFor reports the remaining penalty window, or zero when calm.
func (l *RequestdlLimiter) FrozenFor() time.Duration {
	if l == nil {
		return 0
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return max(0, l.penaltyUntil.Sub(l.now()))
}

// requestdlLogFields renders the bucket counters for the periodic log line.
func (l *RequestdlLimiter) requestdlLogFields(ev *zerolog.Event) {
	if l == nil {
		return
	}
	stats := l.Snapshot()
	ev.Float64("requestdl_budget_per_minute", stats.BudgetPerMinute).
		Float64("requestdl_current_rate_per_minute", stats.CurrentRatePerMinute).
		Bool("requestdl_ramp_active", stats.RampActive).
		Int("requestdl_queued", stats.QueuedPlayback+stats.QueuedBackground+stats.QueuedProbe).
		Uint64("requestdl_playback", stats.RequestsPlayback).
		Uint64("requestdl_background", stats.RequestsBackground).
		Uint64("requestdl_probe", stats.RequestsProbe).
		Uint64("requestdl_denials", stats.Denials).
		Uint64("requestdl_penalty_windows", stats.PenaltyWindows)
}
