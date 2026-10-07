package request

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
)

func TestPersistentGateFailsClosedAfterRestartDuringBan(t *testing.T) {
	config.SetConfigPath(t.TempDir())
	path := filepath.Join(t.TempDir(), "gate", "state.json")
	first := NewThrottle(3, time.Minute, 5*time.Minute, zerolog.Nop())
	if err := first.WithPersistentState(path); err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Retry-After", "2634")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer server.Close()
	resp, err := New(WithThrottle(first), WithMaxRetries(5)).Get(server.URL)
	if err != nil || resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("initial 429: status=%v err=%v", resp, err)
	}
	resp.Body.Close()
	restarted := NewThrottle(3, time.Minute, 5*time.Minute, zerolog.Nop())
	if err := restarted.WithPersistentState(path); err != nil {
		t.Fatal(err)
	}
	_, err = New(WithThrottle(restarted)).Get(server.URL)
	if BackpressureError(err) == nil || BackpressureError(err).RetryAfter >= 0 || calls.Load() != 1 {
		t.Fatalf("restart sent provider request during ban: err=%v calls=%d", err, calls.Load())
	}
	restarted.now = func() time.Time { return time.Now().Add(2635 * time.Second) }
	if err := restarted.Before(); BackpressureError(err) == nil {
		t.Fatalf("restart inferred a safe deadline from the wall clock: %v", err)
	}
}

func TestPersistentGateFailsClosedOnStartupWhenPending(t *testing.T) {
	config.SetConfigPath(t.TempDir())
	dir := filepath.Join(t.TempDir(), "gate")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "state.json")
	if err := os.WriteFile(path, []byte(`{"status":"pending"}`), 0600); err != nil {
		t.Fatal(err)
	}
	restarted := NewThrottle(3, time.Minute, 5*time.Minute, zerolog.Nop())
	if err := restarted.WithPersistentState(path); err != nil {
		t.Fatal(err)
	}
	if err := restarted.Before(); BackpressureError(err) == nil || BackpressureError(err).RetryAfter >= 0 {
		t.Fatalf("startup with pending journal did not fail closed: %v", err)
	}
}

func TestPersistentGateRecoversFromTransientWireError(t *testing.T) {
	config.SetConfigPath(t.TempDir())
	path := filepath.Join(t.TempDir(), "gate", "state.json")
	var log bytes.Buffer
	first := NewThrottle(3, time.Minute, 5*time.Minute, zerolog.New(&log))
	if err := first.WithPersistentState(path); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, _, err := w.(http.Hijacker).Hijack()
		if err != nil {
			t.Error(err)
			return
		}
		conn.Close()
	}))
	defer server.Close()
	_, err := New(WithThrottle(first), WithMaxRetries(0)).Get(server.URL + "?token=private-value")
	if err == nil {
		t.Fatal("expected wire error from closed connection, got nil")
	}
	if err := first.Before(); err != nil {
		t.Fatalf("live process was marked uncertain after transient wire error: %v", err)
	}
	data, readErr := os.ReadFile(path)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if !strings.Contains(string(data), `"idle"`) {
		t.Fatalf("expected journal to be reset to idle, got: %s", string(data))
	}
}

func TestPersistentGateRecoversFromContextCanceled(t *testing.T) {
	config.SetConfigPath(t.TempDir())
	path := filepath.Join(t.TempDir(), "gate", "state.json")
	b := NewThrottle(3, time.Minute, 5*time.Minute, zerolog.Nop())
	if err := b.WithPersistentState(path); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(50 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()

	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, server.URL, nil)
	client := &http.Client{}
	_, err := b.Do(client, req)
	if err == nil {
		t.Fatal("expected context cancellation error, got nil")
	}

	if err := b.Before(); err != nil {
		t.Fatalf("live process was marked uncertain after context cancellation: %v", err)
	}

	data, readErr := os.ReadFile(path)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if !strings.Contains(string(data), `"idle"`) {
		t.Fatalf("expected journal to be reset to idle, got: %s", string(data))
	}
}

func TestWireFailureClass(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want string
	}{
		{"missing response", nil, "missing_response"},
		{"canceled", context.Canceled, "context_canceled"},
		{"deadline", context.DeadlineExceeded, "deadline_exceeded"},
		{"dns", &url.Error{URL: "https://example.test/?token=private-value", Err: &net.DNSError{Err: "no such host", Name: "example.test"}}, "dns"},
		{"dial", &net.OpError{Op: "dial", Err: errors.New("refused")}, "network_dial"},
		{"read", &net.OpError{Op: "read", Err: io.EOF}, "network_read"},
		{"eof", &url.Error{URL: "https://example.test/?token=private-value", Err: io.EOF}, "eof"},
		{"unexpected eof", io.ErrUnexpectedEOF, "unexpected_eof"},
		{"other", errors.New("transport failed"), "other"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := wireFailureClass(tc.err); got != tc.want {
				t.Fatalf("wireFailureClass(%v) = %q, want %q", tc.err, got, tc.want)
			}
		})
	}
}

func TestDeadlineless429OpensCircuitAndRecoversWithoutOperatorAction(t *testing.T) {
	config.SetConfigPath(t.TempDir())
	path := filepath.Join(t.TempDir(), "gate", "state.json")
	var nowMu sync.Mutex
	fakeNow := time.Now()
	nowFn := func() time.Time {
		nowMu.Lock()
		defer nowMu.Unlock()
		return fakeNow
	}
	advance := func(d time.Duration) {
		nowMu.Lock()
		fakeNow = fakeNow.Add(d)
		nowMu.Unlock()
	}

	gate := NewThrottle(1, time.Minute, 5*time.Minute, zerolog.Nop())
	gate.now = nowFn
	if err := gate.WithPersistentState(path); err != nil {
		t.Fatal(err)
	}

	var calls atomic.Int64
	var status atomic.Int32
	status.Store(http.StatusTooManyRequests)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(int(status.Load()))
	}))
	defer server.Close()

	client := New(WithThrottle(gate), WithMaxRetries(5))

	// 1. First request receives 429 with no Retry-After.
	resp, err := client.Get(server.URL)
	if err != nil || resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("expected 429 response without client error, got resp=%v err=%v", resp, err)
	}
	resp.Body.Close()
	if calls.Load() != 1 {
		t.Fatalf("expected exactly 1 call (no retry loops on 429), got %d", calls.Load())
	}

	// 2. Immediate subsequent calls fail fast without hitting wire.
	if err := gate.Before(); BackpressureError(err) == nil || BackpressureError(err).RetryAfter <= 0 {
		t.Fatalf("expected circuit to be open with positive RetryAfter during cooldown, got %v", err)
	}
	if _, err := client.Get(server.URL); BackpressureError(err) == nil || BackpressureError(err).RetryAfter <= 0 || calls.Load() != 1 {
		t.Fatalf("expected client to fail fast at gate during cooldown, err=%v calls=%d", err, calls.Load())
	}

	// 3. Advance clock past cooldown (1 minute).
	advance(61 * time.Second)

	// 4. Circuit should now be closed; submissions resume without restart or operator action.
	if err := gate.Before(); err != nil {
		t.Fatalf("expected circuit to close after cooldown, got err=%v", err)
	}

	// 5. Provider is now healthy (200 OK).
	status.Store(http.StatusOK)
	resp2, err2 := client.Get(server.URL)
	if err2 != nil || resp2.StatusCode != http.StatusOK {
		t.Fatalf("expected successful 200 OK request after cooldown, got resp=%v err=%v", resp2, err2)
	}
	resp2.Body.Close()
	if calls.Load() != 2 {
		t.Fatalf("expected call to reach provider after cooldown, got %d", calls.Load())
	}

	// 6. Verify journal state on disk is idle.
	data, readErr := os.ReadFile(path)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if !strings.Contains(string(data), `"idle"`) {
		t.Fatalf("expected journal to be idle after recovery, got: %s", string(data))
	}

	// 7. Verify escalation on repeated deadline-less 429s without tight retry loops.
	status.Store(http.StatusTooManyRequests)
	// First trip of new sequence: wait is cooldown (60s).
	resp3, err3 := client.Get(server.URL)
	if err3 != nil || resp3.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("expected 429 on trip, got resp=%v err=%v", resp3, err3)
	}
	resp3.Body.Close()
	if err := gate.Before(); BackpressureError(err) == nil || BackpressureError(err).RetryAfter < time.Minute {
		t.Fatalf("expected >= 60s cooldown, got %v", err)
	}

	// Advance past 60s cooldown and trip again: escalation level 1 doubles cooldown to 120s.
	advance(61 * time.Second)
	resp4, err4 := client.Get(server.URL)
	if err4 != nil || resp4.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("expected 429 on retrip, got resp=%v err=%v", resp4, err4)
	}
	resp4.Body.Close()
	if err := gate.Before(); BackpressureError(err) == nil || BackpressureError(err).RetryAfter < 2*time.Minute {
		t.Fatalf("expected escalated cooldown >= 120s on retrip, got %v", err)
	}
}

func TestPersistentGateRefusesWireWhenJournalUnavailable(t *testing.T) {
	config.SetConfigPath(t.TempDir())
	path := filepath.Join(t.TempDir(), "gate", "state.json")
	b := NewThrottle(3, time.Minute, 5*time.Minute, zerolog.Nop())
	if err := b.WithPersistentState(path); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(filepath.Dir(path)); err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	_, err := New(WithThrottle(b)).Get(server.URL)
	if BackpressureError(err) == nil || calls.Load() != 0 {
		t.Fatalf("wire request escaped failed journal: err=%v calls=%d", err, calls.Load())
	}
}

func TestConcurrentAttemptsStopAtFirst429(t *testing.T) {
	config.SetConfigPath(t.TempDir())
	b := NewThrottle(3, time.Minute, 5*time.Minute, zerolog.Nop())
	if err := b.WithPersistentState(filepath.Join(t.TempDir(), "gate", "state.json")); err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Retry-After", "2634")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer server.Close()
	client := New(WithThrottle(b), WithMaxRetries(5))
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			resp, err := client.Get(server.URL)
			if resp != nil {
				resp.Body.Close()
			}
			if err != nil && BackpressureError(err) == nil {
				t.Errorf("unexpected request error: %v", err)
			}
		}()
	}
	close(start)
	wg.Wait()
	if got := calls.Load(); got != 1 {
		t.Fatalf("provider calls after first 429: %d", got)
	}
}
