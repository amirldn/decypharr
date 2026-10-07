package request

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
)

func TestRateLimitBackoff(t *testing.T) {
	resp := &http.Response{StatusCode: 429, Header: http.Header{"Retry-After": {"120"}}}
	if got := retryAfterBackoff(time.Second, 5*time.Minute, 0, resp); got != 120*time.Second {
		t.Fatalf("Retry-After 120: %s", got)
	}
	resp.Header.Set("Retry-After", time.Now().Add(120*time.Second).UTC().Format(http.TimeFormat))
	if got := retryAfterBackoff(time.Second, 5*time.Minute, 0, resp); got < 119*time.Second || got > 120*time.Second {
		t.Fatalf("HTTP-date: %s", got)
	}
	resp.Header.Set("Retry-After", "9223372036854775807")
	if got := retryAfterBackoff(time.Second, 5*time.Minute, 0, resp); got != 5*time.Minute {
		t.Fatalf("overflow-safe cap: %s", got)
	}
	for _, header := range []string{"", "invalid", "-1", "0", time.Now().Add(-time.Minute).UTC().Format(http.TimeFormat)} {
		resp.Header.Set("Retry-After", header)
		seen := map[time.Duration]bool{}
		for i := 0; i < 32; i++ {
			got := retryAfterBackoff(time.Second, 5*time.Minute, 3, resp)
			if got < 4*time.Second || got > 8*time.Second {
				t.Fatalf("exponential jitter: %s", got)
			}
			seen[got] = true
		}
		if len(seen) < 2 {
			t.Fatal("backoff has no jitter")
		}
	}
}

func TestThrottleOpensAndRecovers(t *testing.T) {
	now := time.Now()
	b := NewThrottle(3, time.Minute, 5*time.Minute, zerolog.Nop())
	b.now = func() time.Time { return now }
	resp := &http.Response{StatusCode: 429, Header: http.Header{"Retry-After": {"40"}}}
	for i := 0; i < 3; i++ {
		if i == 0 {
			if err := b.Before(); err != nil {
				t.Fatal(err)
			}
		}
		b.Observe(resp, false)
		if i < 2 {
			now = now.Add(10 * time.Second)
		}
	}
	if !b.open {
		t.Fatal("breaker did not open at threshold")
	}
	if b.Before() == nil {
		t.Fatal("open breaker allowed a request")
	}
	// A response already in flight must not close the open breaker.
	b.Observe(&http.Response{StatusCode: 200}, false)
	if b.Before() == nil {
		t.Fatal("in-flight success closed breaker")
	}
	now = now.Add(time.Minute)
	if err := b.Before(); err != nil {
		t.Fatal(err)
	}
	if b.open {
		t.Fatal("breaker did not close after cooldown")
	}
	// Consecutive 429s survive cooldown so a re-trip can tell there was no
	// success in between.
	if b.consecutive != 3 {
		t.Fatalf("cooldown reset consecutive: %d", b.consecutive)
	}
	b.Observe(&http.Response{StatusCode: 200}, false)
	if b.consecutive != 0 || b.escalation != 0 {
		t.Fatalf("success must reset consecutive and escalation: %d/%d", b.consecutive, b.escalation)
	}
}

func TestRetryAfterLongBanBoundaries(t *testing.T) {
	resp := &http.Response{StatusCode: 429, Header: http.Header{"Retry-After": {"86400"}}}
	// Exactly 24h is honored at a 24h ceiling, not truncated to the old 5m/15m.
	if got := retryAfterBackoff(time.Second, 24*time.Hour, 0, resp); got != 24*time.Hour {
		t.Fatalf("86400s truncated at 24h ceiling: %s", got)
	}
	if got := retryAfterBackoff(time.Second, 25*time.Hour, 0, resp); got != 24*time.Hour {
		t.Fatalf("86400s truncated below server advice: %s", got)
	}
	if got := retryAfterBackoff(time.Second, time.Hour, 0, resp); got != time.Hour {
		t.Fatalf("ceiling not applied: %s", got)
	}
}

func TestThrottleHonorsLongRetryAfter(t *testing.T) {
	now := time.Now()
	b := NewThrottle(1, time.Minute, 24*time.Hour, zerolog.Nop())
	b.now = func() time.Time { return now }
	b.Observe(&http.Response{StatusCode: 429, Header: http.Header{"Retry-After": {"86400"}}}, false)
	if got := b.until.Sub(now); got != 24*time.Hour {
		t.Fatalf("long ban truncated: %s", got)
	}
	if b.Before() == nil {
		t.Fatal("24h ban did not hold the breaker")
	}
}

func TestThrottleNeverTruncatesServerRetryAfterToBackoffMax(t *testing.T) {
	now := time.Now()
	b := NewThrottle(3, time.Minute, 5*time.Minute, zerolog.Nop())
	b.now = func() time.Time { return now }
	b.Observe(&http.Response{StatusCode: 429, Header: http.Header{"Retry-After": {"2634"}}}, false)
	if got := b.Remaining(); got != 2634*time.Second {
		t.Fatalf("server ban truncated to configured backoff: %s", got)
	}
	now = now.Add(5 * time.Minute)
	if err := b.Before(); BackpressureError(err) == nil {
		t.Fatalf("admitted provider call during server ban: %v", err)
	}
	now = now.Add(2634*time.Second - 5*time.Minute)
	if err := b.Before(); err != nil {
		t.Fatalf("did not resume at server deadline: %v", err)
	}
}

func TestThrottleNeverAutomaticallyRetries429(t *testing.T) {
	config.SetConfigPath(t.TempDir())
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Retry-After", "2634")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer server.Close()
	b := NewThrottle(3, time.Minute, 5*time.Minute, zerolog.Nop())
	c := New(WithThrottle(b), WithMaxRetries(5))
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, server.URL, nil)
	resp, err := c.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusTooManyRequests || calls.Load() != 1 {
		t.Fatalf("status=%d provider_calls=%d; 429 must end the operation", resp.StatusCode, calls.Load())
	}
	if got := b.Remaining(); got < 2633*time.Second {
		t.Fatalf("server ban not held: %s", got)
	}
	_, err = c.Get(server.URL)
	if BackpressureError(err) == nil || calls.Load() != 1 {
		t.Fatalf("new call bypassed server ban: err=%v provider_calls=%d", err, calls.Load())
	}
}

func TestThrottleConcurrentAdmissionStopsAtFirst429(t *testing.T) {
	config.SetConfigPath(t.TempDir())
	var calls atomic.Int64
	entered := make(chan struct{})
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			close(entered)
			<-release
		}
		w.Header().Set("Retry-After", "2634")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer server.Close()
	b := NewThrottle(3, time.Minute, 5*time.Minute, zerolog.Nop())
	c := New(WithThrottle(b), WithMaxRetries(5))
	first := make(chan error, 1)
	go func() {
		resp, err := c.Get(server.URL)
		if resp != nil {
			resp.Body.Close()
		}
		first <- err
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("first wire request did not start")
	}
	const n = 32
	var wg sync.WaitGroup
	results := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, err := c.Get(server.URL)
			if resp != nil {
				resp.Body.Close()
			}
			results <- err
		}()
	}
	close(release)
	if err := <-first; err != nil {
		t.Fatalf("first 429: %v", err)
	}
	wg.Wait()
	close(results)
	for err := range results {
		if BackpressureError(err) == nil {
			t.Fatalf("concurrent request was not gated: %v", err)
		}
	}
	if calls.Load() != 1 {
		t.Fatalf("provider received %d calls after first 429", calls.Load())
	}
}

func TestThrottleEscalatesOnRetrip(t *testing.T) {
	now := time.Now()
	b := NewThrottle(1, time.Minute, 24*time.Hour, zerolog.Nop())
	b.now = func() time.Time { return now }
	resp := &http.Response{StatusCode: 429, Header: http.Header{"Retry-After": {"600"}}}
	b.Observe(resp, false)
	if got := b.until.Sub(now); got != 10*time.Minute {
		t.Fatalf("first open: %s", got)
	}
	now = now.Add(10 * time.Minute)
	if err := b.Before(); err != nil {
		t.Fatal(err)
	}
	b.Observe(resp, false)
	if got := b.until.Sub(now); got != 20*time.Minute {
		t.Fatalf("re-trip did not double: %s", got)
	}
	now = now.Add(20 * time.Minute)
	if err := b.Before(); err != nil {
		t.Fatal(err)
	}
	b.Observe(resp, false)
	if got := b.until.Sub(now); got != 40*time.Minute {
		t.Fatalf("second re-trip did not double: %s", got)
	}
}

func TestThrottleEscalationResetsOnSuccess(t *testing.T) {
	now := time.Now()
	b := NewThrottle(1, time.Minute, 24*time.Hour, zerolog.Nop())
	b.now = func() time.Time { return now }
	resp := &http.Response{StatusCode: 429, Header: http.Header{"Retry-After": {"600"}}}
	b.Observe(resp, false)
	now = now.Add(10 * time.Minute)
	if err := b.Before(); err != nil {
		t.Fatal(err)
	}
	b.Observe(resp, false)
	now = now.Add(20 * time.Minute)
	if err := b.Before(); err != nil {
		t.Fatal(err)
	}
	b.Observe(&http.Response{StatusCode: 200}, false)
	if b.escalation != 0 || b.consecutive != 0 {
		t.Fatalf("success did not reset: escalation=%d consecutive=%d", b.escalation, b.consecutive)
	}
	b.Observe(resp, false)
	if got := b.until.Sub(now); got != 10*time.Minute {
		t.Fatalf("post-success wait not reset to base: %s", got)
	}
}

func TestThrottleLogsLongBanDistinctly(t *testing.T) {
	now := time.Now()
	var long bytes.Buffer
	b := NewThrottle(1, time.Minute, 24*time.Hour, zerolog.New(&long))
	b.now = func() time.Time { return now }
	b.Observe(&http.Response{StatusCode: 429, Header: http.Header{"Retry-After": {"86400"}}}, false)
	if !strings.Contains(long.String(), "TorBox long ban: retry-after=86400s") {
		t.Fatalf("long ban not logged distinctly: %s", long.String())
	}
	var short bytes.Buffer
	s := NewThrottle(1, time.Minute, 24*time.Hour, zerolog.New(&short))
	s.now = func() time.Time { return now }
	s.Observe(&http.Response{StatusCode: 429, Header: http.Header{"Retry-After": {"600"}}}, false)
	if strings.Contains(short.String(), "long ban") {
		t.Fatalf("short ban logged as long: %s", short.String())
	}
}

func TestThrottleSharesAPIAndReadBackpressure(t *testing.T) {
	config.SetConfigPath(t.TempDir())
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Retry-After", "120")
		w.WriteHeader(429)
	}))
	defer server.Close()
	b := NewThrottle(3, time.Minute, 5*time.Minute, zerolog.Nop())
	client := New(WithThrottle(b), WithRetryWait(time.Second, 5*time.Minute), WithMaxRetries(3))
	req, _ := http.NewRequest("GET", server.URL, nil)
	resp, err := client.Do(req)
	if err != nil || resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("429 must end API operation: status=%v err=%v", resp, err)
	}
	resp.Body.Close()
	if got := b.Remaining(); got < 115*time.Second || got > 120*time.Second {
		t.Fatalf("configured retry wait: %s", got)
	}
	req, _ = http.NewRequest("HEAD", server.URL, nil)
	_, err = b.Do(server.Client(), req)
	if BackpressureError(err) == nil {
		t.Fatalf("read not rejected: %v", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("provider hammered: %d calls", calls.Load())
	}
	other := NewThrottle(3, time.Minute, 5*time.Minute, zerolog.Nop())
	if other.Before() != nil {
		t.Fatal("provider isolation lost")
	}
}

func TestRetryAfter120WallClock(t *testing.T) {
	if os.Getenv("TORBOX_LONG_RETRY_TEST") != "1" {
		t.Skip("set TORBOX_LONG_RETRY_TEST=1 for the two-minute wire test")
	}
	config.SetConfigPath(t.TempDir())
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			w.Header().Set("Retry-After", "120")
			w.WriteHeader(429)
			return
		}
		w.WriteHeader(200)
	}))
	defer server.Close()
	b := NewThrottle(3, time.Minute, 5*time.Minute, zerolog.Nop())
	c := New(WithThrottle(b), WithRetryWait(time.Second, 5*time.Minute), WithMaxRetries(1))
	start := time.Now()
	resp, err := c.Get(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 429 || calls.Load() != 1 {
		t.Fatalf("429 retried automatically: status=%d calls=%d", resp.StatusCode, calls.Load())
	}
	if _, err := c.Get(server.URL); BackpressureError(err) == nil || calls.Load() != 1 {
		t.Fatalf("provider reached before deadline: %v calls=%d", err, calls.Load())
	}
	time.Sleep(120*time.Second - time.Since(start))
	resp, err = c.Get(server.URL)
	if err != nil || resp.StatusCode != 200 || calls.Load() != 2 {
		t.Fatalf("explicit post-deadline call: status=%v err=%v calls=%d", resp, err, calls.Load())
	}
	resp.Body.Close()
}

type countingLimiter struct{ calls atomic.Int64 }

func (l *countingLimiter) Take() time.Time { l.calls.Add(1); return time.Now() }

func TestThrottleStopsRetryAndLimiterAfter429(t *testing.T) {
	config.SetConfigPath(t.TempDir())
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1); w.WriteHeader(429) }))
	defer server.Close()
	b := NewThrottle(3, time.Minute, time.Millisecond, zerolog.Nop())
	limiter := &countingLimiter{}
	c := New(WithThrottle(b), WithRateLimiter(limiter), WithRetryWait(time.Millisecond, time.Millisecond), WithMaxRetries(5))
	resp, err := c.Get(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 429 || calls.Load() != 1 || limiter.calls.Load() != 1 {
		t.Fatalf("status=%d requests=%d limiter=%d", resp.StatusCode, calls.Load(), limiter.calls.Load())
	}
	_, err = c.Get(server.URL)
	if BackpressureError(err) == nil || calls.Load() != 1 || limiter.calls.Load() != 1 {
		t.Fatalf("open circuit did not reject before limiter: %v", err)
	}
}

func TestReadGateCoversRedirects(t *testing.T) {
	b := NewThrottle(1, time.Minute, 5*time.Minute, zerolog.Nop())
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.URL.Path == "/" {
			http.Redirect(w, r, "/cdn", 302)
			return
		}
		w.Header().Set("Retry-After", "120")
		w.WriteHeader(429)
	}))
	defer server.Close()
	req, _ := http.NewRequest("GET", server.URL, nil)
	_, err := b.Do(server.Client(), req)
	if BackpressureError(err) == nil || calls.Load() != 2 {
		t.Fatalf("redirect throttle not observed: %v, calls=%d", err, calls.Load())
	}
	if b.read429 != 1 || b.reads != 2 {
		t.Fatalf("read counters: %d/%d", b.read429, b.reads)
	}
}

func TestOpenCircuitPolicyPreservesNetworkError(t *testing.T) {
	config.SetConfigPath(t.TempDir())
	gate := NewThrottle(1, time.Minute, 5*time.Minute, zerolog.Nop())
	c := New(WithThrottle(gate))
	gate.Observe(&http.Response{StatusCode: 429, Header: make(http.Header)}, false)
	failure := errors.New("network failed while another request opened the circuit")
	retry, err := c.client.CheckRetry(context.Background(), nil, failure)
	if retry || !errors.Is(err, failure) {
		t.Fatalf("retry=%v err=%v; want original network error without retry", retry, err)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func okResponse(r *http.Request) *http.Response {
	return &http.Response{
		StatusCode: 200,
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader("ok")),
		Request:    r,
	}
}

// A read blocked by a short remaining cooldown waits it out and then succeeds,
// instead of failing the FUSE read. The wait uses the injectable sleep so the
// test never touches the wall clock.
func TestReadWaitsOutShortCooldownThenSucceeds(t *testing.T) {
	now := time.Now()
	b := NewThrottle(1, time.Minute, 5*time.Minute, zerolog.Nop()).WithReadWait(90 * time.Second)
	b.now = func() time.Time { return now }
	var slept []time.Duration
	b.sleep = func(ctx context.Context, d time.Duration) error {
		slept = append(slept, d)
		now = now.Add(d)
		return nil
	}
	b.Observe(&http.Response{StatusCode: 429, Header: http.Header{"Retry-After": {"60"}}}, false)
	if got := b.Remaining(); got != time.Minute {
		t.Fatalf("cooldown: %s", got)
	}
	var calls atomic.Int64
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		calls.Add(1)
		return okResponse(r), nil
	})}
	req, _ := http.NewRequest("GET", "http://torbox.invalid/file", nil)
	resp, err := b.Do(client, req)
	if err != nil {
		t.Fatalf("read did not survive the short cooldown: %v", err)
	}
	resp.Body.Close()
	if calls.Load() != 1 {
		t.Fatalf("upstream not called after wait: %d", calls.Load())
	}
	if len(slept) != 1 || slept[0] != time.Minute {
		t.Fatalf("did not wait the remainder: %v", slept)
	}
	if b.isOpen() {
		t.Fatal("breaker still open after the read waited")
	}
}

// A long remaining cooldown keeps fail-fast: the read must not block and the
// upstream must not be dialed.
func TestReadFailsFastOnLongCooldown(t *testing.T) {
	now := time.Now()
	b := NewThrottle(1, time.Minute, 24*time.Hour, zerolog.Nop()).WithReadWait(90 * time.Second)
	b.now = func() time.Time { return now }
	slept := false
	b.sleep = func(ctx context.Context, d time.Duration) error { slept = true; return nil }
	b.Observe(&http.Response{StatusCode: 429, Header: http.Header{"Retry-After": {"600"}}}, false)
	var calls atomic.Int64
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		calls.Add(1)
		return okResponse(r), nil
	})}
	req, _ := http.NewRequest("GET", "http://torbox.invalid/file", nil)
	_, err := b.Do(client, req)
	backpressure := BackpressureError(err)
	if backpressure == nil {
		t.Fatalf("long cooldown did not fail fast: %v", err)
	}
	if backpressure.RetryAfter != 10*time.Minute {
		t.Fatalf("retry-after: %s", backpressure.RetryAfter)
	}
	if slept || calls.Load() != 0 {
		t.Fatalf("long cooldown slept=%v upstream_calls=%d", slept, calls.Load())
	}
}

// The blocking wait must honor request cancellation.
func TestReadWaitInterruptedByContext(t *testing.T) {
	b := NewThrottle(1, time.Minute, 5*time.Minute, zerolog.Nop()).WithReadWait(90 * time.Second)
	b.Observe(&http.Response{StatusCode: 429, Header: http.Header{"Retry-After": {"60"}}}, false)
	ctx, cancel := context.WithCancel(context.Background())
	b.sleep = func(sleepCtx context.Context, d time.Duration) error {
		cancel()
		return sleepCtx.Err()
	}
	if err := b.WaitRead(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel did not interrupt the wait: %v", err)
	}
}

// API calls keep the pre-#166 fail-fast behavior even for a short cooldown;
// only the read path waits.
func TestAPITransportStillFailsFast(t *testing.T) {
	b := NewThrottle(1, time.Minute, 5*time.Minute, zerolog.Nop()).WithReadWait(90 * time.Second)
	b.Observe(&http.Response{StatusCode: 429, Header: make(http.Header)}, false)
	var calls atomic.Int64
	transport := &throttleTransport{
		next:     roundTripFunc(func(r *http.Request) (*http.Response, error) { calls.Add(1); return okResponse(r), nil }),
		throttle: b,
	}
	req, _ := http.NewRequest("GET", "http://torbox.invalid/api", nil)
	_, err := transport.RoundTrip(req)
	if BackpressureError(err) == nil {
		t.Fatalf("API call did not fail fast: %v", err)
	}
	if calls.Load() != 0 {
		t.Fatalf("upstream called while circuit open: %d", calls.Load())
	}
}

// Concurrent reads and breaker transitions must not deadlock or race.
func TestConcurrentReadersAndBreakerTransitions(t *testing.T) {
	config.SetConfigPath(t.TempDir())
	var upstream atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstream.Add(1)
		w.WriteHeader(200)
	}))
	defer server.Close()
	b := NewThrottle(2, time.Millisecond, time.Millisecond, zerolog.Nop()).WithReadWait(50 * time.Millisecond)
	client := server.Client()
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 40; j++ {
				req, _ := http.NewRequest("GET", server.URL, nil)
				resp, err := b.Do(client, req)
				if resp != nil {
					resp.Body.Close()
				}
				if err != nil && BackpressureError(err) == nil {
					t.Errorf("unexpected read error: %v", err)
					return
				}
			}
		}()
	}
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 40; j++ {
				b.Observe(&http.Response{StatusCode: 429, Header: make(http.Header)}, true)
				if j%2 == 0 {
					_ = b.Before()
				}
			}
		}()
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("concurrent readers and breaker transitions deadlocked")
	}
}

type captureWriter struct{ lines chan string }

func (w *captureWriter) Write(p []byte) (int, error) {
	select {
	case w.lines <- string(append([]byte(nil), p...)):
	default:
	}
	return len(p), nil
}

// The periodic counter line is emitted from an injectable tick source, so the
// test drives the interval directly and verifies the goroutine shuts down.
func TestPeriodicCounterLog(t *testing.T) {
	w := &captureWriter{lines: make(chan string, 8)}
	b := NewThrottle(3, time.Minute, 5*time.Minute, zerolog.New(w))
	b.mu.Lock()
	b.requests, b.throttled, b.reads, b.read429 = 41, 7, 12, 3
	b.mu.Unlock()
	ticks := make(chan time.Time)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		b.runCounterLog(ctx, ticks)
	}()
	ticks <- time.Now()
	select {
	case line := <-w.lines:
		for _, field := range []string{`"requests":41`, `"429s":7`, `"read_requests":12`, `"read_429s":3`, "TorBox throttle counters"} {
			if !strings.Contains(line, field) {
				t.Fatalf("counter log missing %s: %s", field, line)
			}
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no periodic counter log emitted")
	}
	// The interval repeats, not just once.
	ticks <- time.Now()
	select {
	case <-w.lines:
	case <-time.After(2 * time.Second):
		t.Fatal("periodic counter log did not repeat")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("counter logger goroutine leaked after cancel")
	}
}

func TestDefaultRetryStatusesOnWire(t *testing.T) {
	config.SetConfigPath(t.TempDir())
	for _, status := range []int{429, 500, 502, 503, 504} {
		t.Run(strconv.Itoa(status), func(t *testing.T) {
			var calls atomic.Int64
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if calls.Add(1) == 1 {
					w.WriteHeader(status)
					return
				}
				w.WriteHeader(200)
			}))
			defer server.Close()
			c := New(WithThrottle(NewThrottle(3, time.Minute, time.Millisecond, zerolog.Nop())), WithRetryWait(time.Millisecond, time.Millisecond), WithMaxRetries(1))
			resp, err := c.Get(server.URL)
			if err != nil {
				t.Fatal(err)
			}
			resp.Body.Close()
			wantStatus, wantCalls := 200, int64(2)
			if status == http.StatusTooManyRequests {
				wantStatus, wantCalls = http.StatusTooManyRequests, 1
			}
			if resp.StatusCode != wantStatus || calls.Load() != wantCalls {
				t.Fatalf("status=%d calls=%d", resp.StatusCode, calls.Load())
			}
		})
	}
}
