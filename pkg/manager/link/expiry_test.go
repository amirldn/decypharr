package link

import (
	"context"
	"fmt"
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
	"github.com/sirrobot01/decypharr/pkg/debrid/types"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

type expiringClient struct {
	debrid.Client
	old, fresh     types.DownloadLink
	deleted, calls int
}

func (c *expiringClient) GetDownloadLink(_ string, _ *types.File) (types.DownloadLink, error) {
	c.calls++
	if c.deleted > 0 {
		return c.fresh, nil
	}
	return c.old, nil
}
func (c *expiringClient) DeleteLink(_ types.DownloadLink) error { c.deleted++; return nil }

func TestFetchAndValidateRefetchesExpiredValidatedURL(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) }))
	defer server.Close()
	old := types.DownloadLink{Debrid: "fake", Token: "token", Filename: "movie", Link: "fake://1/2", DownloadLink: server.URL + "/old", ExpiresAt: time.Now().Add(-time.Minute)}
	fresh := old
	fresh.DownloadLink = server.URL + "/fresh"
	fresh.ExpiresAt = time.Now().Add(time.Hour)
	client := &expiringClient{old: old, fresh: fresh}
	clients := xsync.NewMap[string, debrid.Client]()
	clients.Store("fake", client)
	service := New(clients, nil, nil, nil, server.Client(), 0, zerolog.Nop())
	service.validated.Store(old.DownloadLink, nil)
	entry := &storage.Entry{InfoHash: "hash", ActiveProvider: "fake", Files: map[string]*storage.File{"movie": {Name: "movie", Size: 4}}, Providers: map[string]*storage.ProviderEntry{"fake": {ID: "1", Files: map[string]*storage.ProviderFile{"movie": {Id: "2", Link: "fake://1/2"}}}}}
	got, err := service.GetLink(context.Background(), entry, "movie")
	if err != nil || got.DownloadLink != fresh.DownloadLink || client.calls != 2 || client.deleted != 1 {
		t.Fatalf("link=%q err=%v fetches=%d deletes=%d", got.DownloadLink, err, client.calls, client.deleted)
	}
}

func TestTorboxPlaybackRangesUseCDNAfterOneRequestdl(t *testing.T) {
	config.SetConfigPath(t.TempDir())
	var requestdlCalls, cdnCalls atomic.Int64
	cdn := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cdnCalls.Add(1)
		if r.Method == http.MethodGet && r.Header.Get("Range") == "" {
			t.Error("range GET missing Range header")
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer cdn.Close()
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/torrents/requestdl" {
			t.Errorf("unexpected API path %s", r.URL.Path)
		}
		requestdlCalls.Add(1)
		_, _ = fmt.Fprintf(w, `{"success":true,"data":%q}`, cdn.URL+"/movie")
	}))
	defer api.Close()
	tb, err := torbox.New(config.Debrid{Name: "torbox-test", Provider: "torbox", APIKey: "test", DownloadAPIKeys: []string{"test"}, TorboxBackoffMax: "20ms"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	tb.Host = api.URL
	clients := xsync.NewMap[string, debrid.Client]()
	clients.Store("torbox-test", tb)
	service := New(clients, nil, nil, nil, cdn.Client(), 0, zerolog.Nop())
	entry := &storage.Entry{InfoHash: "hash", ActiveProvider: "torbox-test", Files: map[string]*storage.File{"movie": {Name: "movie", Size: 4}}, Providers: map[string]*storage.ProviderEntry{"torbox-test": {ID: "1", Files: map[string]*storage.ProviderFile{"movie": {Id: "2", Link: "torbox://1/2"}}}}}
	dl, err := service.GetLink(context.Background(), entry, "movie")
	if err != nil || dl.DownloadLink != cdn.URL+"/movie" {
		t.Fatalf("link=%q err=%v", dl.DownloadLink, err)
	}
	for _, byteRange := range []string{"bytes=0-1", "bytes=2-3"} {
		req, _ := http.NewRequest(http.MethodGet, dl.DownloadLink, nil)
		req.Header.Set("Range", byteRange)
		resp, err := cdn.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
	}
	if requestdlCalls.Load() != 1 || cdnCalls.Load() != 3 {
		t.Fatalf("requestdl=%d cdn=%d", requestdlCalls.Load(), cdnCalls.Load())
	}
	if stats := tb.RequestdlStats().(request.RequestdlStats); stats.RequestsPlayback != 1 {
		t.Fatalf("playback budget=%+v", stats)
	}
}
