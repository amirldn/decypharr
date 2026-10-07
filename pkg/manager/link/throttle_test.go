package link

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/puzpuzpuz/xsync/v4"
	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/request"
	debrid "github.com/sirrobot01/decypharr/pkg/debrid/common"
	"github.com/sirrobot01/decypharr/pkg/debrid/providers/torbox"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

func TestTorboxHEADBackpressureIsNotCached(t *testing.T) {
	config.SetConfigPath(t.TempDir())
	var calls atomic.Int64
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/cdn" && r.Method == http.MethodHead {
			w.WriteHeader(200)
			return
		}
		if r.URL.Path != "/api/torrents/requestdl" || r.Method != http.MethodGet {
			t.Errorf("unexpected %s %s", r.Method, r.URL.Path)
			w.WriteHeader(404)
			return
		}
		if calls.Add(1) == 1 {
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(429)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"success":true,"data":"` + server.URL + `/cdn"}`))
	}))
	defer server.Close()
	// The bucket honors the raw Retry-After independently of
	// torbox_backoff_max; pin requestdl_freeze_max low here too so this test
	// keeps exercising recovery instead of a 120s freeze.
	tb, err := torbox.New(config.Debrid{Name: "torbox-test", Provider: "torbox", APIKey: "test", DownloadAPIKeys: []string{"test"}, TorboxBackoffMax: "20ms", TorboxBreakerThreshold: 1, TorboxBreakerCooldown: "20ms", RequestdlFreezeMax: "20ms"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	tb.Host = server.URL
	clients := xsync.NewMap[string, debrid.Client]()
	clients.Store("torbox-test", tb)
	service := New(clients, nil, nil, nil, server.Client(), 3, zerolog.Nop())
	entry := &storage.Entry{InfoHash: "hash", ActiveProvider: "torbox-test", Files: map[string]*storage.File{"file": {Name: "file", Size: 4}}, Providers: map[string]*storage.ProviderEntry{"torbox-test": {ID: "1", Files: map[string]*storage.ProviderFile{"file": {Id: "2", Link: "torbox://1/2"}}}}}
	_, err = service.GetLink(context.Background(), entry, "file")
	if request.BackpressureError(err) == nil || service.validated.Size() != 0 {
		t.Fatalf("throttle cached or lost: %v", err)
	}
	if tb.AccountManager().Current().Disabled.Load() {
		t.Fatal("throttle disabled account")
	}
	time.Sleep(1100 * time.Millisecond)
	dl, err := service.GetLink(context.Background(), entry, "file")
	if err != nil || dl.Empty() || calls.Load() != 2 {
		t.Fatalf("HEAD did not recover automatically: %v, calls=%d", err, calls.Load())
	}
	if stats := tb.RequestdlStats().(request.RequestdlStats); stats.RequestsPlayback != 2 {
		t.Fatalf("requestdl was not charged to playback: %+v", stats)
	}
}
