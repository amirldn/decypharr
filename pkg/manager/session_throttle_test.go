package manager

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/customerror"
	"github.com/sirrobot01/decypharr/internal/request"
	"github.com/sirrobot01/decypharr/pkg/debrid/types"
)

func TestTorboxReadFailsFastAndRecovers(t *testing.T) {
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(429)
			return
		}
		w.WriteHeader(206)
		_, _ = w.Write([]byte("data"))
	}))
	defer server.Close()
	gate := request.NewThrottle(1, 20*time.Millisecond, 20*time.Millisecond, zerolog.Nop())
	tr := &httpTransport{client: server.Client(), throttle: gate,
		getLink: func(context.Context) (types.DownloadLink, error) {
			return types.DownloadLink{DownloadLink: server.URL}, nil
		},
		refresh: func(context.Context, types.DownloadLink) (types.DownloadLink, error) {
			t.Error("throttle must not refresh a link")
			return types.DownloadLink{}, nil
		},
	}
	s := newSession(context.Background(), tr, 4, 0)
	defer s.Close()
	start := time.Now()
	_, err := s.Read(make([]byte, 4))
	if request.BackpressureError(err) == nil || customerror.IsRetriableError(err) {
		t.Fatalf("expected terminal error for current chunk, got %v", err)
	}
	if time.Since(start) > time.Second || calls.Load() != 1 || s.resumes.Load() != 0 {
		t.Fatal("read entered retry ladder")
	}
	time.Sleep(1100 * time.Millisecond)
	data, err := io.ReadAll(s)
	if err != nil || string(data) != "data" {
		t.Fatalf("automatic recovery: %q, %v", data, err)
	}
}
