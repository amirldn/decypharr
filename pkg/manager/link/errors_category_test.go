package link

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/puzpuzpuz/xsync/v4"
	"github.com/rs/zerolog"
	debrid "github.com/sirrobot01/decypharr/pkg/debrid/common"
	"github.com/sirrobot01/decypharr/pkg/debrid/types"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

// TestErrorCodeCategories pins the provider-error taxonomy. The permanent
// rows are deliberate — notably 404, whose permanence is the fork's immunity
// to upstream #381's requestdl flood. The retryable rows are the transient
// codes that must never be memoised as permanent (upstream #369), including
// unrecognised codes: an unknown code is not evidence of permanent failure.
func TestErrorCodeCategories(t *testing.T) {
	tests := []struct {
		name     string
		codes    []string
		category ErrorCategory
	}{
		{"permanent", []string{"link_not_found", "file_not_available", "401", "unauthorized", "404"}, CategoryPermanent},
		{"account", []string{"bandwidth_exceeded", "quota_exceeded", "daily_limit_exceeded", "bytes_limit_reached"}, CategoryAccountIssue},
		{"refetchable", []string{"link_expired", "invalid_download_code", "400"}, CategoryRefetchable},
		{"retryable", []string{"429", "503", "read_pxy_timeout", "500", "502", "504", "SOMETHING_NEW", "418", ""}, CategoryRetryable},
	}
	for _, tt := range tests {
		for _, code := range tt.codes {
			t.Run(tt.name+"/"+code, func(t *testing.T) {
				err := ErrorCodeToLinkError(code)
				if err.Category != tt.category {
					t.Fatalf("code %q: category=%s want=%s", code, err.Category, tt.category)
				}
				if err.Code != code {
					t.Fatalf("code %q: Code=%q", code, err.Code)
				}
			})
		}
	}
}

func Test5xxCarriesProviderServerSentinel(t *testing.T) {
	for _, code := range []string{"500", "502", "504"} {
		err := ErrorCodeToLinkError(code)
		if !err.IsRetryable() || err.IsPermanent() {
			t.Fatalf("code %s: category=%s, want retryable", code, err.Category)
		}
		if !errors.Is(err, Err5xx) {
			t.Fatalf("code %s: %v does not wrap Err5xx", code, err)
		}
	}
}

// transientValidationClient models a provider with an account-level resolved
// link cache: repeat fetches return the same CDN URL without another
// requestdl until DeleteLink invalidates it. DeleteLink is the operation that
// forces the next fetch onto the wire, so it is the flood signal to count.
type transientValidationClient struct {
	debrid.Client
	dl         types.DownloadLink
	fetches    int
	deletes    int
	requestdls int
	cached     bool
}

func (c *transientValidationClient) GetDownloadLinkForPlayback(_ context.Context, _ string, _ *types.File) (types.DownloadLink, error) {
	c.fetches++
	if !c.cached {
		c.requestdls++
		c.cached = true
	}
	return c.dl, nil
}

func (c *transientValidationClient) DeleteLink(types.DownloadLink) error {
	c.deletes++
	c.cached = false
	return nil
}

// newLinkTestHarness serves the link-validation HEAD from an httptest server
// whose status is decided per call, so a test can flip an outage on and off.
func newLinkTestHarness(t *testing.T, status func() int) (*Service, *storage.Entry, *transientValidationClient) {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(status())
	}))
	t.Cleanup(server.Close)

	client := &transientValidationClient{dl: types.DownloadLink{
		Debrid: "fake", Token: "token", Filename: "movie", Link: "fake://1/2",
		DownloadLink: server.URL + "/movie",
	}}
	clients := xsync.NewMap[string, debrid.Client]()
	clients.Store("fake", client)
	service := New(clients, nil, nil, nil, server.Client(), 0, zerolog.Nop())
	entry := &storage.Entry{
		InfoHash:       "hash",
		ActiveProvider: "fake",
		Files:          map[string]*storage.File{"movie": {Name: "movie", Size: 4}},
		Providers: map[string]*storage.ProviderEntry{"fake": {ID: "1", Files: map[string]*storage.ProviderFile{
			"movie": {Id: "2", Link: "fake://1/2"},
		}}},
	}
	return service, entry, client
}

// A transient 5xx during validation must not be memoised as a permanent
// failure: once the provider recovers, the next GetLink validates the same
// cached link and succeeds. Before the fix the 502 was stored in s.validated
// as permanent and every later GetLink returned it until restart.
func TestTransient5xxDoesNotPoisonValidatedLink(t *testing.T) {
	var failing atomic.Bool
	failing.Store(true)
	service, entry, client := newLinkTestHarness(t, func() int {
		if failing.Load() {
			return http.StatusBadGateway
		}
		return http.StatusOK
	})

	_, err := service.GetLink(context.Background(), entry, "movie")
	if err == nil {
		t.Fatal("expected the 502 to fail validation")
	}
	lerr := GetLinkError(err)
	if lerr == nil || !lerr.ShouldRetry() || lerr.IsPermanent() {
		t.Fatalf("502 classified as %v (err=%v)", lerr, err)
	}
	if client.deletes != 0 {
		t.Fatalf("transient 5xx invalidated the link (%d deletes)", client.deletes)
	}

	failing.Store(false)
	got, err := service.GetLink(context.Background(), entry, "movie")
	if err != nil {
		t.Fatalf("link did not recover after the 5xx cleared: %v", err)
	}
	if got.DownloadLink != client.dl.DownloadLink {
		t.Fatalf("link=%q want=%q", got.DownloadLink, client.dl.DownloadLink)
	}
	if client.deletes != 0 {
		t.Fatalf("recovery deleted the cached link (%d deletes)", client.deletes)
	}
}

// The #381 guard: a persistent transient failure must not make every GetLink
// delete the cached link and spend another requestdl. Without this bound,
// rclone/Plex polling turns a 5xx window into a provider API flood — the
// shape that trips TorBox cooldowns. The same cached link is revalidated.
func TestPersistentTransientFailureKeepsRequestdlBounded(t *testing.T) {
	service, entry, client := newLinkTestHarness(t, func() int { return http.StatusBadGateway })

	for i := 0; i < 25; i++ {
		if _, err := service.GetLink(context.Background(), entry, "movie"); err == nil {
			t.Fatalf("call %d unexpectedly succeeded", i)
		}
	}
	if client.requestdls != 1 || client.deletes != 0 {
		t.Fatalf("requestdls=%d deletes=%d; want one resolve and no invalidation", client.requestdls, client.deletes)
	}
	if client.fetches != 25 {
		t.Fatalf("fetches=%d; want one per GetLink (no refetch amplification)", client.fetches)
	}
}

// 404 stays permanent by design — the fork's immunity to upstream #381's
// requestdl flood — so a 404 survives provider recovery and is never
// refetched. This is the deliberate counter-case to the transient codes.
func TestProvider404RemainsPermanent(t *testing.T) {
	var failing atomic.Bool
	failing.Store(true)
	service, entry, client := newLinkTestHarness(t, func() int {
		if failing.Load() {
			return http.StatusNotFound
		}
		return http.StatusOK
	})

	_, err := service.GetLink(context.Background(), entry, "movie")
	lerr := GetLinkError(err)
	if lerr == nil || !lerr.IsPermanent() {
		t.Fatalf("404 classified as %v (err=%v)", lerr, err)
	}

	failing.Store(false)
	if _, err := service.GetLink(context.Background(), entry, "movie"); err == nil {
		t.Fatal("memoised permanent 404 should outlive provider recovery")
	}
	if client.deletes != 0 || client.requestdls != 1 {
		t.Fatalf("deletes=%d requestdls=%d; want no invalidation and one resolve", client.deletes, client.requestdls)
	}
}
