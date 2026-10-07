package request

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
)

func TestClassContextTagging(t *testing.T) {
	if got := ClassOf(context.Background()); got != ClassBackground {
		t.Fatalf("default class = %v, want background", got)
	}
	if got := ClassOf(nil); got != ClassBackground {
		t.Fatalf("nil context class = %v, want background", got)
	}
	for _, class := range []Class{ClassPlayback, ClassProbe, ClassBackground} {
		if got := ClassOf(WithClass(context.Background(), class)); got != class {
			t.Fatalf("round-trip class = %v, want %v", got, class)
		}
	}
	// Out-of-range values must normalize rather than panic an index.
	if got := ClassOf(WithClass(context.Background(), Class(99))); got != ClassBackground {
		t.Fatalf("invalid class = %v, want background", got)
	}
	if WithClass(nil, ClassPlayback) == nil {
		t.Fatal("WithClass(nil) returned nil context")
	}
	if ClassPlayback.String() != "playback" || ClassProbe.String() != "probe" || ClassBackground.String() != "background" {
		t.Fatal("class labels changed")
	}
}

func newTestLimiter(t *testing.T, ratePerMinute float64, ramp, maxBackoff time.Duration, now *time.Time) *RequestdlLimiter {
	t.Helper()
	l := NewRequestdlLimiter(ratePerMinute, ramp, maxBackoff, DefaultRequestdlFreezeMax, zerolog.Nop())
	l.now = func() time.Time { return *now }
	l.last = *now
	l.tokens = l.burst
	return l
}

func TestRequestdlRampShape(t *testing.T) {
	now := time.Unix(0, 0)
	l := newTestLimiter(t, 600, 5*time.Minute, time.Minute, &now)

	// A 429 arms the ramp; the first admission after the window starts it.
	l.Observe(&http.Response{StatusCode: http.StatusTooManyRequests, Header: http.Header{"Retry-After": {"1"}}}, ClassPlayback, time.Second)
	if l.Snapshot().PenaltyWindows != 1 {
		t.Fatal("penalty window not counted")
	}
	now = now.Add(time.Second)
	if err := l.Take(context.Background(), ClassPlayback); err != nil {
		t.Fatalf("admission after penalty: %v", err)
	}
	start := l.CurrentRatePerMinute()
	if start < 110 || start > 130 { // ~20% of 600
		t.Fatalf("ramp start = %.1f/min, want ~120/min", start)
	}
	now = now.Add(150 * time.Second)
	half := l.CurrentRatePerMinute()
	if half < 350 || half > 370 { // 60% of 600
		t.Fatalf("ramp midpoint = %.1f/min, want ~360/min", half)
	}
	now = now.Add(150 * time.Second)
	if end := l.CurrentRatePerMinute(); end != 600 {
		t.Fatalf("ramp end = %.1f/min, want 600/min", end)
	}
	if stats := l.Snapshot(); stats.RampActive || stats.RampProgress != 1 {
		t.Fatalf("ramp not finished: %+v", stats)
	}
}

func TestRequestdlRampStartsAtFloorForASlowBudget(t *testing.T) {
	now := time.Unix(0, 0)
	l := newTestLimiter(t, 12, 5*time.Minute, time.Minute, &now)
	l.Observe(&http.Response{StatusCode: http.StatusTooManyRequests}, ClassPlayback, time.Second)
	now = now.Add(time.Second)
	if err := l.Take(context.Background(), ClassPlayback); err != nil {
		t.Fatal(err)
	}
	if got := l.CurrentRatePerMinute(); got < 2.3 || got > 2.5 {
		t.Fatalf("12/min budget floor = %.2f/min, want ~2.4/min", got)
	}
}

func TestRequestdlFullJitterBounds(t *testing.T) {
	now := time.Unix(0, 0)
	l := newTestLimiter(t, 60, 0, 8*time.Second, &now)

	// Deterministic extremes: full jitter spans [0, ceiling].
	l.randN = func(int64) int64 { return 0 }
	if got := l.fullJitterLocked(ClassBackground); got != 0 {
		t.Fatalf("full jitter minimum = %s, want 0", got)
	}
	l.randN = func(n int64) int64 { return n - 1 }
	last := time.Duration(0)
	for fails := 1; fails <= 8; fails++ {
		l.classFails[ClassBackground] = fails
		got := l.fullJitterLocked(ClassBackground)
		if got < last {
			t.Fatalf("ceiling decreased at %d failures: %s < %s", fails, got, last)
		}
		if got > l.maxBackoff {
			t.Fatalf("jitter %s exceeds configured max %s", got, l.maxBackoff)
		}
		last = got
	}
	if last != l.maxBackoff {
		t.Fatalf("jitter ceiling = %s, want the configured max %s", last, l.maxBackoff)
	}

	// Real jitter stays within bounds and actually varies.
	l.randN = func(n int64) int64 { return time.Now().UnixNano() % n }
	for fails := 1; fails <= 6; fails++ {
		l.classFails[ClassBackground] = fails
		seen := map[time.Duration]bool{}
		for i := 0; i < 64; i++ {
			got := l.fullJitterLocked(ClassBackground)
			if got < 0 || got > l.maxBackoff {
				t.Fatalf("jitter out of bounds: %s", got)
			}
			seen[got] = true
		}
		if len(seen) < 2 && l.maxBackoff > time.Second {
			t.Fatalf("jitter produced no spread at %d failures", fails)
		}
	}
}

func TestRequestdl5xxPauseIsPerClass(t *testing.T) {
	now := time.Unix(0, 0)
	l := newTestLimiter(t, 600, 0, time.Minute, &now)
	l.Observe(&http.Response{StatusCode: http.StatusBadGateway}, ClassBackground, 0)
	if !l.classPause[ClassBackground].After(now) {
		t.Fatal("background class not paused on 5xx")
	}
	if !l.classPause[ClassPlayback].IsZero() || !l.classPause[ClassProbe].IsZero() {
		t.Fatal("5xx pause leaked across classes")
	}
	l.tokens = 5
	if err := l.Take(context.Background(), ClassPlayback); err != nil {
		t.Fatalf("playback blocked by a background 5xx: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	if err := l.Take(ctx, ClassBackground); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("background not paused on its own 5xx: %v", err)
	}
}

// TestRequestdlHonorsRawRetryAfterBeyondBreakerClamp is the #203 regression:
// the bucket must freeze for the server's raw Retry-After even when the
// breaker's own cooldown is clamped by torbox_backoff_max.
func TestRequestdlHonorsRawRetryAfterBeyondBreakerClamp(t *testing.T) {
	now := time.Unix(0, 0)
	// Default breaker-style max (5m) but the server asks for 2302s.
	l := NewRequestdlLimiter(12, 0, 5*time.Minute, DefaultRequestdlFreezeMax, zerolog.Nop())
	l.now = func() time.Time { return now }
	l.last = now

	l.Observe(&http.Response{
		StatusCode: http.StatusTooManyRequests,
		Header:     http.Header{"Retry-After": {"2302"}},
	}, ClassPlayback, 5*time.Minute)

	if got := l.FrozenFor(); got < 2302*time.Second {
		t.Fatalf("bucket freeze = %s, want >= 2302s (not truncated to the 5m breaker clamp)", got)
	}
	if err := l.Take(context.Background(), ClassPlayback); BackpressureError(err) == nil {
		t.Fatalf("bucket admitted a call during the raw Retry-After window: %v", err)
	}
}

func TestRequestdlFreezeMaxBoundsServerAdvice(t *testing.T) {
	now := time.Unix(0, 0)
	l := NewRequestdlLimiter(12, 0, time.Minute, 6*time.Hour, zerolog.Nop())
	l.now = func() time.Time { return now }
	l.last = now

	// A 100h ban is bounded by requestdl_freeze_max, not honored literally.
	l.Observe(&http.Response{
		StatusCode: http.StatusTooManyRequests,
		Header:     http.Header{"Retry-After": {"360000"}},
	}, ClassPlayback, 0)
	if got := l.FrozenFor(); got != 6*time.Hour {
		t.Fatalf("freeze = %s, want the 6h freeze max", got)
	}

	// No usable advice and no breaker penalty still freezes with full jitter.
	now = now.Add(6 * time.Hour)
	other := NewRequestdlLimiter(12, 0, time.Minute, DefaultRequestdlFreezeMax, zerolog.Nop())
	other.now = func() time.Time { return now }
	other.last = now
	other.Observe(&http.Response{StatusCode: http.StatusTooManyRequests}, ClassPlayback, 0)
	if got := other.FrozenFor(); got <= 0 || got > other.maxBackoff {
		t.Fatalf("jittered freeze = %s, want (0, %s]", got, other.maxBackoff)
	}
}

func TestRequestdlFreezeSuppressesWireCalls(t *testing.T) {
	config.SetConfigPath(t.TempDir())
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Retry-After", "120")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer server.Close()

	b := NewThrottle(1, time.Minute, 5*time.Minute, zerolog.Nop())
	limiter := NewRequestdlLimiter(600, 0, time.Minute, DefaultRequestdlFreezeMax, zerolog.Nop())
	b.UseRequestdl(limiter, func(r *http.Request) bool {
		return strings.HasPrefix(r.URL.String(), server.URL+"/api/torrents/requestdl")
	})

	req, _ := http.NewRequest("GET", server.URL+"/api/torrents/requestdl?token=x", nil)
	if _, err := b.Do(server.Client(), req); BackpressureError(err) == nil {
		t.Fatalf("first call must surface backpressure: %v", err)
	}
	if limiter.FrozenFor() <= 0 {
		t.Fatal("bucket did not freeze for the Retry-After window")
	}
	if err := limiter.Take(context.Background(), ClassPlayback); BackpressureError(err) == nil {
		t.Fatalf("bucket admitted a call during the penalty window: %v", err)
	}
	for i := 0; i < 5; i++ {
		req, _ := http.NewRequest("GET", server.URL+"/api/torrents/requestdl?token=x", nil)
		if _, err := b.Do(server.Client(), req); BackpressureError(err) == nil {
			t.Fatalf("call %d issued during the penalty window", i)
		}
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("wire calls during freeze = %d, want 1", got)
	}
	if stats := limiter.Snapshot(); stats.Denials == 0 || stats.PenaltyWindows != 1 {
		t.Fatalf("freeze not observable: %+v", stats)
	}
}

func TestRequestdlPriorityPlaybackWins(t *testing.T) {
	// 10 tokens/second with a burst of one: background work queued first must
	// not delay a playback read by more than a single token interval.
	l := NewRequestdlLimiter(600, 0, time.Minute, DefaultRequestdlFreezeMax, zerolog.Nop())
	l.mu.Lock()
	l.burst = 1
	l.tokens = 0
	l.last = time.Now()
	l.mu.Unlock()

	const background = 20
	var completed atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < background; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := l.Take(context.Background(), ClassBackground); err == nil {
				completed.Add(1)
			}
		}()
	}
	time.Sleep(30 * time.Millisecond)

	playbackStart := time.Now()
	playbackErr := make(chan error, 1)
	go func() { playbackErr <- l.Take(context.Background(), ClassPlayback) }()
	select {
	case err := <-playbackErr:
		if err != nil {
			t.Fatalf("playback admission: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("playback starved by background work")
	}
	elapsed := time.Since(playbackStart)
	if elapsed > 400*time.Millisecond {
		t.Fatalf("playback waited %s behind background work", elapsed)
	}
	if atPlayback := completed.Load(); atPlayback >= background {
		t.Fatalf("background drained (%d) before playback was served", atPlayback)
	}

	wg.Wait()
	if stats := l.Snapshot(); stats.RequestsPlayback != 1 || stats.RequestsBackground != background {
		t.Fatalf("class counters wrong: %+v", stats)
	}
}

func TestRequestdlSharedBucketAcrossGoroutines(t *testing.T) {
	l := NewRequestdlLimiter(6000, 0, time.Minute, DefaultRequestdlFreezeMax, zerolog.Nop())
	const callers = 50
	var errs atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := l.Take(context.Background(), ClassBackground); err != nil {
				errs.Add(1)
			}
		}()
	}
	wg.Wait()
	if errs.Load() != 0 {
		t.Fatalf("%d admissions failed", errs.Load())
	}
	stats := l.Snapshot()
	total := stats.RequestsPlayback + stats.RequestsBackground + stats.RequestsProbe
	if total != callers {
		t.Fatalf("total admissions = %d, want %d (shared counters)", total, callers)
	}
	if stats.QueuedPlayback+stats.QueuedBackground+stats.QueuedProbe != 0 {
		t.Fatalf("waiters leaked: %+v", stats)
	}
}

func TestRequestdlEnforcesBudget(t *testing.T) {
	l := NewRequestdlLimiter(1200, 0, time.Minute, DefaultRequestdlFreezeMax, zerolog.Nop()) // 20/s
	l.mu.Lock()
	l.burst = 1
	l.tokens = 1
	l.last = time.Now()
	l.mu.Unlock()

	start := time.Now()
	for i := 0; i < 5; i++ {
		if err := l.Take(context.Background(), ClassBackground); err != nil {
			t.Fatal(err)
		}
	}
	// Five tokens at 20/s: the first is immediate, the remaining four cost
	// 200ms. A per-worker limiter would have admitted all five at once.
	if elapsed := time.Since(start); elapsed < 150*time.Millisecond {
		t.Fatalf("budget not enforced: admitted 5 in %s", elapsed)
	}
}

func TestRequestdlNonRequestdlTrafficUnaffected(t *testing.T) {
	config.SetConfigPath(t.TempDir())
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	b := NewThrottle(3, time.Minute, 5*time.Minute, zerolog.Nop())
	limiter := NewRequestdlLimiter(1, 0, time.Minute, DefaultRequestdlFreezeMax, zerolog.Nop()) // 1/minute
	b.UseRequestdl(limiter, func(r *http.Request) bool {
		return strings.HasPrefix(r.URL.Path, "/api/torrents/requestdl")
	})

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, "GET", server.URL+"/api/torrents/mylist", nil)
	resp, err := b.Do(server.Client(), req)
	if err != nil {
		t.Fatalf("non-requestdl traffic was gated: %v", err)
	}
	resp.Body.Close()
	stats := limiter.Snapshot()
	if stats.RequestsPlayback+stats.RequestsBackground+stats.RequestsProbe != 0 {
		t.Fatalf("non-requestdl traffic consumed the budget: %+v", stats)
	}
}

func TestRequestdlCountersAppearInPeriodicLog(t *testing.T) {
	var buf bytes.Buffer
	b := NewThrottle(1, time.Minute, time.Minute, zerolog.New(&buf))
	limiter := NewRequestdlLimiter(12, 0, time.Minute, DefaultRequestdlFreezeMax, zerolog.Nop())
	b.UseRequestdl(limiter, func(*http.Request) bool { return true })
	if err := limiter.Take(context.Background(), ClassPlayback); err != nil {
		t.Fatal(err)
	}
	b.LogCounters()
	out := buf.String()
	for _, want := range []string{
		"TorBox throttle counters",
		`"requests":0`,
		`"requestdl_playback":1`,
		`"requestdl_budget_per_minute":12`,
		`"requestdl_penalty_windows":0`,
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("periodic log missing %s: %s", want, out)
		}
	}
}
