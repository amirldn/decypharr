package qbit

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	json "github.com/bytedance/sonic"
)

// On 2026-10-25 a degraded TorBox cache check held each add open until the
// Arr's own 100s HTTP timeout, so a request burned 100s per release (The
// Godfather Part III took 23 minutes). Healthy adds take 1s (p99 7s).
func TestBoundedAddReturnsWhenTheProviderHangs(t *testing.T) {
	hung := func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	}

	start := time.Now()
	err := boundedAdd(t.Context(), 50*time.Millisecond, hung)

	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("boundedAdd took %s, want it to give up after the 50ms limit", elapsed)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want it to wrap context.DeadlineExceeded", err)
	}
}

func TestBoundedAddPassesThroughFastResults(t *testing.T) {
	wantErr := errors.New("not cached")
	if err := boundedAdd(t.Context(), time.Second, func(context.Context) error { return wantErr }); !errors.Is(err, wantErr) {
		t.Fatalf("err = %v, want %v", err, wantErr)
	}
	if err := boundedAdd(t.Context(), time.Second, func(context.Context) error { return nil }); err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
}

func TestTimedOutAddIsReportedAsRetryableGatewayTimeout(t *testing.T) {
	err := boundedAdd(t.Context(), 10*time.Millisecond, func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	})

	recorder := httptest.NewRecorder()
	writeTorrentAddError(recorder, err)

	if recorder.Code != http.StatusGatewayTimeout {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusGatewayTimeout)
	}
	var response torrentAddErrorResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if response.Code != "provider_timeout" || !response.Retryable || response.Permanent {
		t.Fatalf("response = %+v, want retryable provider_timeout", response)
	}
}
