package torbox

import (
	"net/http"
	"testing"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/request"
)

func TestProviderReloadRetainsRateLimitGate(t *testing.T) {
	config.SetConfigPath(t.TempDir())
	dc := config.Debrid{Name: "torbox-reload", Provider: "torbox", APIKey: "test-reload"}
	first, err := New(dc, nil)
	if err != nil {
		t.Fatal(err)
	}
	first.throttle.Observe(&http.Response{StatusCode: http.StatusTooManyRequests, Header: http.Header{"Retry-After": {"2634"}}}, false)
	dc.Name = "torbox-reload-alias"
	second, err := New(dc, nil)
	if err != nil {
		t.Fatal(err)
	}
	if first.throttle != second.throttle {
		t.Fatal("provider reload created an independent gate")
	}
	if err := second.throttle.Before(); request.BackpressureError(err) == nil {
		t.Fatalf("provider reload bypassed active ban: %v", err)
	}
}

func TestThrottleConfiguration(t *testing.T) {
	backoff, cooldown, readWait, threshold, err := throttleConfig(config.Debrid{})
	if err != nil || backoff != 5*time.Minute || cooldown != time.Minute || threshold != 3 || readWait != request.DefaultReadWait {
		t.Fatal("unsafe defaults")
	}
	for _, dc := range []config.Debrid{
		{TorboxBackoffMax: "-1s"}, {TorboxBackoffMax: "invalid"},
		{TorboxBreakerCooldown: "0s"}, {TorboxBreakerThreshold: -1}, {TorboxBreakerThreshold: 101},
		{TorboxBackoffMax: "49h"}, {TorboxBreakerCooldown: "49h"},
		{TorboxReadWaitMax: "0s"}, {TorboxReadWaitMax: "invalid"},
		{TorboxReadWaitMax: "500ms"}, {TorboxReadWaitMax: "6m"},
	} {
		if _, _, _, _, err := throttleConfig(dc); err == nil {
			t.Fatalf("accepted invalid config: %+v", dc)
		}
	}
	// A full-day TorBox ban must be configurable; the previous 15m ceiling
	// rejected both of these.
	for _, dc := range []config.Debrid{
		{TorboxBackoffMax: "24h"}, {TorboxBackoffMax: "48h"}, {TorboxBreakerCooldown: "24h"}, {TorboxBreakerCooldown: "48h"},
	} {
		if _, _, _, _, err := throttleConfig(dc); err != nil {
			t.Fatalf("rejected long bound %+v: %v", dc, err)
		}
	}
	// The read wait is deliberately bounded much lower than a ban: at most how
	// long a read may block.
	for _, dc := range []config.Debrid{
		{TorboxReadWaitMax: "1s"}, {TorboxReadWaitMax: "90s"}, {TorboxReadWaitMax: "5m"},
	} {
		if _, _, _, _, err := throttleConfig(dc); err != nil {
			t.Fatalf("rejected read wait %+v: %v", dc, err)
		}
	}
	backoff, cooldown, _, _, err = throttleConfig(config.Debrid{TorboxBackoffMax: "24h", TorboxBreakerCooldown: "24h"})
	if err != nil || backoff != 24*time.Hour || cooldown != 24*time.Hour {
		t.Fatalf("long bounds round-trip: %v %s %s", err, backoff, cooldown)
	}
	_, _, readWait, _, err = throttleConfig(config.Debrid{TorboxReadWaitMax: "2m"})
	if err != nil || readWait != 2*time.Minute {
		t.Fatalf("read wait round-trip: %v %s", err, readWait)
	}
}
