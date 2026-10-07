package manager

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
	debrid "github.com/sirrobot01/decypharr/pkg/debrid/common"
	"github.com/sirrobot01/decypharr/pkg/debrid/providers/torbox"
	"github.com/sirrobot01/decypharr/pkg/manager/link"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

// A batch download whose first validation hits one passing provider 5xx must
// retry the same cached link and succeed instead of dropping the file as a
// permanent failure (#315/#258). resolveLinkWithRetry already retries
// IsRetryable errors; this pins that 5xx is classified that way and that the
// retry spends a single requestdl.
func TestBatchLinkResolutionRetriesTransient5xx(t *testing.T) {
	config.SetConfigPath(t.TempDir())
	var requestdls, heads atomic.Int64
	var failHeads atomic.Bool
	failHeads.Store(true)
	cdn := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		heads.Add(1)
		if failHeads.Load() {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer cdn.Close()
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requestdls.Add(1)
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
	d := &Downloader{manager: &Manager{linkService: link.New(clients, nil, nil, nil, cdn.Client(), 3, zerolog.Nop())}, logger: zerolog.Nop()}
	entry := &storage.Entry{
		InfoHash:       "hash",
		ActiveProvider: "torbox-test",
		Files:          map[string]*storage.File{"movie": {Name: "movie", Size: 4}},
		Providers: map[string]*storage.ProviderEntry{"torbox-test": {ID: "1", Files: map[string]*storage.ProviderFile{
			"movie": {Id: "2", Link: "torbox://1/2"},
		}}},
	}

	// The outage clears while resolveLinkWithRetry is between attempts.
	time.AfterFunc(200*time.Millisecond, func() { failHeads.Store(false) })

	dl, err := d.resolveLinkWithRetry(context.Background(), entry, "movie")
	if err != nil {
		t.Fatalf("transient 5xx dropped the file: %v", err)
	}
	if dl.DownloadLink != cdn.URL+"/movie" {
		t.Fatalf("link=%q want=%q", dl.DownloadLink, cdn.URL+"/movie")
	}
	if requestdls.Load() != 1 {
		t.Fatalf("requestdl=%d; the retry must reuse the resolved link", requestdls.Load())
	}
	if heads.Load() < 2 {
		t.Fatalf("heads=%d; expected a revalidation after the 5xx", heads.Load())
	}
}
