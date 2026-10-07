package link

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/puzpuzpuz/xsync/v4"
	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/request"
	debrid "github.com/sirrobot01/decypharr/pkg/debrid/common"
	"github.com/sirrobot01/decypharr/pkg/debrid/providers/torbox"
	"github.com/sirrobot01/decypharr/pkg/debrid/types"
)

// TestResolveFollowsRequestdlRedirectThroughBudget covers the /api/browse gate:
// the requestdl call is made server-side by Decypharr (charged to the shared
// budget) and the caller gets the final CDN URL instead of the requestdl URL.
func TestResolveFollowsRequestdlRedirectThroughBudget(t *testing.T) {
	config.SetConfigPath(t.TempDir())
	var requestdlCalls, cdnCalls atomic.Int64
	var server *httptest.Server
	mux := http.NewServeMux()
	mux.HandleFunc("/api/torrents/requestdl", func(w http.ResponseWriter, r *http.Request) {
		requestdlCalls.Add(1)
		http.Redirect(w, r, server.URL+"/cdn/file", http.StatusFound)
	})
	mux.HandleFunc("/cdn/file", func(w http.ResponseWriter, r *http.Request) {
		cdnCalls.Add(1)
		w.WriteHeader(http.StatusOK)
	})
	server = httptest.NewServer(mux)
	defer server.Close()

	tb, err := torbox.New(config.Debrid{
		Name:                   "torbox-test",
		Provider:               "torbox",
		APIKey:                 "test",
		DownloadAPIKeys:        []string{"test"},
		RequestdlBudget:        "12/minute",
		TorboxBackoffMax:       "20ms",
		TorboxBreakerThreshold: 1,
		TorboxBreakerCooldown:  "20ms",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	tb.Host = server.URL

	clients := xsync.NewMap[string, debrid.Client]()
	clients.Store("torbox-test", tb)
	service := New(clients, nil, nil, nil, server.Client(), 3, zerolog.Nop())

	dl := types.DownloadLink{
		Debrid:       "torbox-test",
		Filename:     "movie.mkv",
		Link:         "torbox://1/2",
		DownloadLink: server.URL + "/api/torrents/requestdl?token=test",
		Id:           "2",
	}
	ctx := request.WithClass(context.Background(), request.ClassPlayback)
	got, err := service.Resolve(ctx, dl)
	if err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}
	if want := server.URL + "/cdn/file"; got != want {
		t.Fatalf("Resolve() = %q, want the CDN URL %q", got, want)
	}
	if requestdlCalls.Load() != 1 || cdnCalls.Load() < 1 {
		t.Fatalf("wire calls requestdl=%d cdn=%d, want 1 requestdl", requestdlCalls.Load(), cdnCalls.Load())
	}
	stats := tb.RequestdlStats().(request.RequestdlStats)
	if stats.RequestsPlayback != 1 {
		t.Fatalf("resolve not charged to the playback lane: %+v", stats)
	}
}

func TestResolveRejectsEmptyLink(t *testing.T) {
	config.SetConfigPath(t.TempDir())
	service := New(xsync.NewMap[string, debrid.Client](), nil, nil, nil, &http.Client{Timeout: time.Second}, 1, zerolog.Nop())
	if _, err := service.Resolve(context.Background(), types.DownloadLink{}); GetLinkError(err) == nil {
		t.Fatalf("Resolve() error = %v, want a typed link error", err)
	}
}

// TestResolveRedactsTokenFromError is the AGENTS.md §5.5 regression: a
// connection failure on the requestdl URL must not put the download-account
// token into the wrapped error that /api/browse logs.
func TestResolveRedactsTokenFromError(t *testing.T) {
	config.SetConfigPath(t.TempDir())
	service := New(xsync.NewMap[string, debrid.Client](), nil, nil, nil, &http.Client{Timeout: 2 * time.Second}, 1, zerolog.Nop())
	dl := types.DownloadLink{
		Debrid:       "torbox",
		Filename:     "movie.mkv",
		Link:         "torbox://1/2",
		DownloadLink: "http://127.0.0.1:1/api/torrents/requestdl?token=SUPERSECRETTOKEN&torrent_id=1&file_id=2&redirect=true",
	}
	_, err := service.Resolve(context.Background(), dl)
	if err == nil {
		t.Fatal("Resolve() error = nil, want a transport error")
	}
	if strings.Contains(err.Error(), "SUPERSECRETTOKEN") || strings.Contains(err.Error(), "token=") {
		t.Fatalf("Resolve() error leaked the account token: %v", err)
	}
	if GetLinkError(err) == nil {
		t.Fatalf("Resolve() error = %v, want a typed link error", err)
	}
}

// TestValidateLinkRedactsTokenFromError covers the pre-existing validateLink
// path the new resolve reuses.
func TestValidateLinkRedactsTokenFromError(t *testing.T) {
	config.SetConfigPath(t.TempDir())
	service := New(xsync.NewMap[string, debrid.Client](), nil, nil, nil, &http.Client{Timeout: 2 * time.Second}, 1, zerolog.Nop())
	dl := types.DownloadLink{
		Debrid:       "torbox",
		Filename:     "movie.mkv",
		Link:         "torbox://1/2",
		DownloadLink: "http://127.0.0.1:1/api/torrents/requestdl?token=SUPERSECRETTOKEN&torrent_id=1&file_id=2&redirect=true",
	}
	err := service.validateLink(context.Background(), &dl)
	if err == nil {
		t.Fatal("validateLink() error = nil, want a transport error")
	}
	if strings.Contains(err.Error(), "SUPERSECRETTOKEN") || strings.Contains(err.Error(), "token=") {
		t.Fatalf("validateLink() error leaked the account token: %v", err)
	}
	if GetLinkError(err) == nil {
		t.Fatalf("validateLink() error = %v, want a typed link error", err)
	}
}
