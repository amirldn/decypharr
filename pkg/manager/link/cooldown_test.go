package link

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/puzpuzpuz/xsync/v4"
	"github.com/rs/zerolog"
	debrid "github.com/sirrobot01/decypharr/pkg/debrid/common"
	"github.com/sirrobot01/decypharr/pkg/debrid/types"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

func newCooldownTestService() *Service {
	return New(
		xsync.NewMap[string, debrid.Client](),
		nil, nil, nil,
		&http.Client{Timeout: 2 * time.Second},
		1,
		zerolog.Nop(),
	)
}

func TestNoteSlowHostCoolsDown(t *testing.T) {
	s := newCooldownTestService()
	if s.hostCooling("cdn1.torbox.app") {
		t.Fatal("unknown host should not be cooling")
	}
	s.NoteSlowHost("cdn1.torbox.app", 22*1024)
	if !s.hostCooling("cdn1.torbox.app") {
		t.Fatal("host should be cooling after NoteSlowHost")
	}
	if s.hostCooling("cdn2.torbox.app") {
		t.Fatal("other hosts must not be affected")
	}
	s.NoteSlowHost("", 22*1024) // empty host is a no-op, must not panic
}

func TestHostCoolingExpires(t *testing.T) {
	s := newCooldownTestService()
	s.cooldowns.Store("stale.torbox.app", time.Now().Add(-time.Minute))
	if s.hostCooling("stale.torbox.app") {
		t.Fatal("expired cooldown should report not cooling")
	}
	if _, ok := s.cooldowns.Load("stale.torbox.app"); ok {
		t.Fatal("expired entry should be reaped on read")
	}
}

func TestCooldownMapBounded(t *testing.T) {
	s := newCooldownTestService()
	// Pre-fill with expired entries: sweep must reclaim space instead of
	// dropping the new note.
	for i := 0; i < maxCooldownHosts; i++ {
		s.cooldowns.Store("stale-host-"+strconv.Itoa(i), time.Now().Add(-time.Hour))
	}
	s.NoteSlowHost("fresh.torbox.app", 1024)
	if !s.hostCooling("fresh.torbox.app") {
		t.Fatal("sweep should have reclaimed space for the fresh entry")
	}
}

func TestCDNHost(t *testing.T) {
	cases := []struct {
		url  string
		want string
	}{
		{"https://cdn12.torbox.app/dl/abc123/video.mkv", "cdn12.torbox.app"},
		{"https://cdn12.torbox.app:8443/dl/x?token=secret", "cdn12.torbox.app:8443"},
		{"http://127.0.0.1:8080/file", "127.0.0.1:8080"},
		{"", ""},
		{":://not a url", ""},
	}
	for _, c := range cases {
		if got := CDNHost(types.DownloadLink{DownloadLink: c.url}); got != c.want {
			t.Errorf("CDNHost(%q) = %q, want %q", c.url, got, c.want)
		}
	}
}

func TestSlowStreamErrorTaxonomy(t *testing.T) {
	serr := NewSlowStreamError("cdn1.torbox.app", 1<<20, 22*1024)
	var as *SlowStreamError
	if !errors.As(serr, &as) {
		t.Fatal("should unwrap to *SlowStreamError")
	}
	if as.Host != "cdn1.torbox.app" || as.Bps != 22*1024 || as.Bytes != 1<<20 {
		t.Fatalf("measurements not carried: %+v", as)
	}
	lerr := GetLinkError(serr)
	if lerr == nil {
		t.Fatal("GetLinkError should find the embedded link error")
	}
	if !lerr.ShouldRefetch() {
		t.Error("slow stream must classify as refetchable")
	}
	if lerr.IsPermanent() {
		t.Error("slow stream must not be permanent")
	}
	if lerr.ShouldDisableAccount() {
		t.Error("slow stream must not disable the account")
	}
}

// stubDebridClient implements debrid.Client via the embedded (nil) interface
// for every method except the two the link service touches on the refresh
// path. It deals canned links in order so tests can script CDN-host
// sequences.
type stubDebridClient struct {
	debrid.Client
	links   []types.DownloadLink
	calls   int
	deleted []string
}

func (s *stubDebridClient) GetDownloadLinkForPlayback(_ context.Context, _ string, _ *types.File) (types.DownloadLink, error) {
	dl := s.links[min(s.calls, len(s.links)-1)]
	s.calls++
	return dl, nil
}

func (s *stubDebridClient) DeleteLink(dl types.DownloadLink) error {
	s.deleted = append(s.deleted, dl.DownloadLink)
	return nil
}

func cooldownTestEntry() *storage.Entry {
	return &storage.Entry{
		InfoHash:       "abc123",
		Name:           "movie",
		ActiveProvider: "stub",
		Files: map[string]*storage.File{
			"movie.mkv": {Name: "movie.mkv", Size: 100},
		},
		Providers: map[string]*storage.ProviderEntry{
			"stub": {
				Provider: "stub",
				ID:       "tid1",
				Files: map[string]*storage.ProviderFile{
					"movie.mkv": {Id: "1"},
				},
			},
		},
	}
}

func cooldownTestService(stub *stubDebridClient) *Service {
	clients := xsync.NewMap[string, debrid.Client]()
	clients.Store("stub", stub)
	return New(clients, nil, nil, nil, &http.Client{Timeout: 2 * time.Second}, 1, zerolog.Nop())
}

// TestInvalidateAndRefetchSkipsCoolingHosts: the refresh path discards freshly
// dealt links that land on a cooling CDN host and returns the first healthy
// one.
func TestInvalidateAndRefetchSkipsCoolingHosts(t *testing.T) {
	stub := &stubDebridClient{links: []types.DownloadLink{
		{Debrid: "stub", Filename: "movie.mkv", DownloadLink: "https://cdn-bad.example/a"},
		{Debrid: "stub", Filename: "movie.mkv", DownloadLink: "https://cdn-bad.example/b"},
		{Debrid: "stub", Filename: "movie.mkv", DownloadLink: "https://cdn-good.example/c"},
	}}
	s := cooldownTestService(stub)
	s.NoteSlowHost("cdn-bad.example", 22*1024)

	bad := types.DownloadLink{Debrid: "stub", Filename: "movie.mkv", DownloadLink: "https://cdn-bad.example/old"}
	got, err := s.Refresh(context.Background(), cooldownTestEntry(), bad)
	if err != nil {
		t.Fatalf("Refresh failed: %v", err)
	}
	if host := CDNHost(got); host != "cdn-good.example" {
		t.Errorf("Refresh returned host %q, want cdn-good.example", host)
	}
	if stub.calls != 3 {
		t.Errorf("provider calls = %d, want 3 (2 skips + 1 kept)", stub.calls)
	}
	if len(stub.deleted) != 3 { // the bad link + the two skipped fresh links
		t.Errorf("deleted links = %d, want 3", len(stub.deleted))
	}
}

// TestInvalidateAndRefetchFallsBackAfterMaxSkips: when every dealt link lands
// on cooling hosts, the bounded skip loop gives up and returns the last one
// instead of looping forever.
func TestInvalidateAndRefetchFallsBackAfterMaxSkips(t *testing.T) {
	stub := &stubDebridClient{links: []types.DownloadLink{
		{Debrid: "stub", Filename: "movie.mkv", DownloadLink: "https://cdn-bad.example/a"},
	}}
	s := cooldownTestService(stub)
	s.NoteSlowHost("cdn-bad.example", 22*1024)

	bad := types.DownloadLink{Debrid: "stub", Filename: "movie.mkv", DownloadLink: "https://cdn-bad.example/old"}
	got, err := s.Refresh(context.Background(), cooldownTestEntry(), bad)
	if err != nil {
		t.Fatalf("Refresh failed: %v", err)
	}
	if host := CDNHost(got); host != "cdn-bad.example" {
		t.Errorf("fallback returned host %q, want the (cooling) cdn-bad.example", host)
	}
	if want := 1 + maxCooldownSkips; stub.calls != want {
		t.Errorf("provider calls = %d, want %d (bounded)", stub.calls, want)
	}
}

// TestValidateLinkCooldownBranch: a cached link pointing at a cooling CDN
// host validates as refetchable so a fresh link is dealt instead.
func TestValidateLinkCooldownBranch(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	s := newCooldownTestService()
	s.httpClient = server.Client()
	u, _ := url.Parse(server.URL)
	dl := types.DownloadLink{Debrid: "stub", Filename: "movie.mkv", DownloadLink: server.URL + "/f"}

	if err := s.validateLink(context.Background(), &dl); err != nil {
		t.Fatalf("uncooled host should validate clean, got %v", err)
	}

	s.NoteSlowHost(u.Host, 22*1024)
	err := s.validateLink(context.Background(), &dl)
	lerr := GetLinkError(err)
	if lerr == nil {
		t.Fatalf("cooling host should fail validation, got nil")
	}
	if lerr.Code != "host_cooldown" {
		t.Errorf("code = %q, want host_cooldown", lerr.Code)
	}
	if !lerr.ShouldRefetch() {
		t.Error("cooling host must classify as refetchable")
	}
}
