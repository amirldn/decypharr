package torbox

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/request"
)

func TestRequestdlOptionsDefaults(t *testing.T) {
	limiter := requestdlOptions(config.Debrid{}, 5*time.Minute, zerolog.Nop())
	if got := limiter.BudgetPerMinute(); got != request.DefaultRequestdlBudgetPerMinute {
		t.Fatalf("budget = %v, want %v", got, request.DefaultRequestdlBudgetPerMinute)
	}
	if got := limiter.RampPeriod(); got != time.Duration(request.DefaultRequestdlRampSeconds)*time.Second {
		t.Fatalf("ramp = %s, want %ds", got, request.DefaultRequestdlRampSeconds)
	}
	if got := limiter.FreezeMax(); got != request.DefaultRequestdlFreezeMax {
		t.Fatalf("freeze max = %s, want %s", got, request.DefaultRequestdlFreezeMax)
	}
}

func TestRequestdlOptionsOverrides(t *testing.T) {
	limiter := requestdlOptions(config.Debrid{
		RequestdlBudget:      "120/minute",
		RequestdlRampSeconds: 60,
		RequestdlFreezeMax:   "2h",
	}, 5*time.Minute, zerolog.Nop())
	if got := limiter.BudgetPerMinute(); got != 120 {
		t.Fatalf("budget = %v, want 120", got)
	}
	if got := limiter.RampPeriod(); got != time.Minute {
		t.Fatalf("ramp = %s, want 1m", got)
	}
	if got := limiter.FreezeMax(); got != 2*time.Hour {
		t.Fatalf("freeze max = %s, want 2h", got)
	}
}

func TestRequestdlOptionsInvalidFallBackToDefaults(t *testing.T) {
	limiter := requestdlOptions(config.Debrid{
		RequestdlBudget:      "bogus",
		RequestdlRampSeconds: -5,
		RequestdlFreezeMax:   "not-a-duration",
	}, 5*time.Minute, zerolog.Nop())
	if got := limiter.BudgetPerMinute(); got != request.DefaultRequestdlBudgetPerMinute {
		t.Fatalf("invalid budget = %v, want the default", got)
	}
	if got := limiter.RampPeriod(); got != time.Duration(request.DefaultRequestdlRampSeconds)*time.Second {
		t.Fatalf("invalid ramp = %s, want the default", got)
	}
	if got := limiter.FreezeMax(); got != request.DefaultRequestdlFreezeMax {
		t.Fatalf("invalid freeze max = %s, want the default", got)
	}

	// A freeze max above the 48h ceiling is rejected rather than applied.
	over := requestdlOptions(config.Debrid{RequestdlFreezeMax: "72h"}, time.Minute, zerolog.Nop())
	if got := over.FreezeMax(); got != request.DefaultRequestdlFreezeMax {
		t.Fatalf("72h freeze max = %s, want the default", got)
	}
}

func TestRequestdlOptionsCapsRunawayBudget(t *testing.T) {
	limiter := requestdlOptions(config.Debrid{RequestdlBudget: "100000/minute"}, time.Minute, zerolog.Nop())
	if got := limiter.BudgetPerMinute(); got != request.MaxRequestdlBudgetPerMinute {
		t.Fatalf("runaway budget = %v, want the cap %v", got, request.MaxRequestdlBudgetPerMinute)
	}
}

func TestRequestdlMatcherRequiresExactPath(t *testing.T) {
	matcher := requestdlMatcher(func() string { return "https://api.torbox.app/v1" })
	cases := []struct {
		url   string
		match bool
	}{
		{"https://api.torbox.app/v1/api/torrents/requestdl?token=x", true},
		{"https://api.torbox.app/v1/api/torrents/requestdl", true},
		// Lookalike path must not match.
		{"https://api.torbox.app/v1/api/torrents/requestdlX", false},
		{"https://api.torbox.app/v1/api/torrents/requestdl/extra", false},
		// Same path on a different host (a CDN redirect target) must not match.
		{"https://cdn.torbox.app/v1/api/torrents/requestdl?token=x", false},
		// Unrelated API traffic must not match.
		{"https://api.torbox.app/v1/api/torrents/mylist", false},
	}
	for _, tc := range cases {
		req, err := http.NewRequest(http.MethodGet, tc.url, nil)
		if err != nil {
			t.Fatalf("bad test URL %q: %v", tc.url, err)
		}
		if got := matcher(req); got != tc.match {
			t.Errorf("matcher(%q) = %v, want %v", tc.url, got, tc.match)
		}
	}
}

func TestRequestdlFreezeHonorsRawRetryAfterThroughTransport(t *testing.T) {
	config.SetConfigPath(t.TempDir())
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "2302")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer server.Close()

	// Both the breaker and requestdl bucket must honor the raw 2302s window,
	// regardless of the configured fallback backoff ceiling.
	tb, err := New(config.Debrid{
		Name:                   "torbox-test",
		Provider:               "torbox",
		APIKey:                 "test",
		DownloadAPIKeys:        []string{"test"},
		TorboxBackoffMax:       "1s",
		TorboxBreakerThreshold: 1,
		TorboxBreakerCooldown:  "1s",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	tb.Host = server.URL

	req, _ := http.NewRequest(http.MethodGet, server.URL+"/api/torrents/requestdl?token=test", nil)
	if _, err := tb.throttle.Do(server.Client(), req); request.BackpressureError(err) == nil {
		t.Fatalf("expected backpressure, got %v", err)
	}
	if breaker := tb.throttle.Remaining(); breaker < 2300*time.Second {
		t.Fatalf("breaker truncated server deadline: %s", breaker)
	}
	stats := tb.RequestdlStats().(request.RequestdlStats)
	if remaining := time.Until(stats.PenaltyUntil); remaining < 2300*time.Second {
		t.Fatalf("bucket freeze = %s, want >= 2300s (raw Retry-After, not the 1s breaker clamp)", remaining)
	}
}

func TestRequestdlBudgetGovernsOnlyRequestdl(t *testing.T) {
	config.SetConfigPath(t.TempDir())
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	tb, err := New(config.Debrid{
		Name:                   "torbox-test",
		Provider:               "torbox",
		APIKey:                 "test",
		DownloadAPIKeys:        []string{"test"},
		RequestdlBudget:        "120/minute",
		TorboxBackoffMax:       "20ms",
		TorboxBreakerThreshold: 1,
		TorboxBreakerCooldown:  "20ms",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	// The matcher reads Host dynamically, so the test server stands in for
	// api.torbox.app.
	tb.Host = server.URL

	stats := tb.RequestdlStats().(request.RequestdlStats)
	if stats.BudgetPerMinute != 120 {
		t.Fatalf("budget = %v, want 120", stats.BudgetPerMinute)
	}

	req, _ := http.NewRequest(http.MethodGet, server.URL+"/api/torrents/requestdl?token=test", nil)
	resp, err := tb.throttle.Do(server.Client(), req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	stats = tb.RequestdlStats().(request.RequestdlStats)
	if stats.RequestsBackground != 1 {
		t.Fatalf("requestdl call not counted: %+v", stats)
	}

	// A plain API call must not consume the /requestdl budget, nor must a
	// lookalike path.
	for _, path := range []string{"/api/torrents/mylist", "/api/torrents/requestdlX"} {
		req, _ = http.NewRequest(http.MethodGet, server.URL+path, nil)
		resp, err = tb.throttle.Do(server.Client(), req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
	}
	stats = tb.RequestdlStats().(request.RequestdlStats)
	if stats.RequestsBackground != 1 {
		t.Fatalf("non-requestdl traffic consumed the budget: %+v", stats)
	}
	if calls.Load() != 3 {
		t.Fatalf("wire calls = %d, want 3", calls.Load())
	}
}
