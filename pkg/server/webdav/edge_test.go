package webdav

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"

	"github.com/go-chi/chi/v5"
	"testing"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/pkg/edgecache"
	"github.com/sirrobot01/decypharr/pkg/manager"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

func edgeBytes(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i*13 + 1)
	}
	return b
}

// newEdgeServer builds a stream server whose manager has the edge cache on
// (head 100, tail 40 bytes). The manager has no debrid provider, so any range
// that is not served locally fails: these tests prove stored ranges never
// touch the remote path.
func newEdgeServer(t *testing.T) (srvURL string, entry *storage.Entry, content []byte, store *edgecache.Store) {
	t.Helper()
	config.Reset()
	config.SetConfigPath(t.TempDir())
	t.Cleanup(config.Reset)
	cfg := config.Get()
	cfg.EdgeCache = config.EdgeCache{Enabled: true, Dir: t.TempDir(), HeadSize: "100B", TailSize: "40B"}

	m := manager.New()
	t.Cleanup(func() { _ = m.Storage().Close() })
	if m.EdgeCache() == nil {
		t.Fatal("edge cache not initialised")
	}

	const size = 1234
	content = edgeBytes(size)
	entry = &storage.Entry{
		InfoHash: testInfohash,
		Name:     "Movie.2023",
		Files: map[string]*storage.File{
			"Movie.2023.mkv": {Name: "Movie.2023.mkv", Size: size, InfoHash: testInfohash},
		},
	}
	if err := m.Storage().AddOrUpdate(entry); err != nil {
		t.Fatal(err)
	}
	store = m.EdgeCache().Store()
	key := edgecache.Key(testInfohash, "Movie.2023.mkv", size)
	if err := store.PutHead(key, size, content[:100]); err != nil {
		t.Fatal(err)
	}
	if err := store.PutTail(key, size, content[size-40:]); err != nil {
		t.Fatal(err)
	}

	srv, _ := newStreamServerWith(t, m, entry)
	return srv, entry, content, store
}

func getRange(t *testing.T, url, rng string) (*http.Response, []byte) {
	t.Helper()
	req, _ := http.NewRequest(http.MethodGet, url, nil)
	req.Header.Set("Range", rng)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return resp, body
}

func TestEdgeServesStoredRangesWithoutDebrid(t *testing.T) {
	srvURL, entry, content, _ := newEdgeServer(t)
	url := streamURLFor(srvURL, entry)
	size := len(content)

	for _, c := range []struct{ start, end int }{
		{0, 99},               // whole head
		{10, 50},              // inside head
		{size - 40, size - 1}, // whole tail
		{size - 10, size - 1}, // inside tail
	} {
		resp, body := getRange(t, url, fmt.Sprintf("bytes=%d-%d", c.start, c.end))
		if resp.StatusCode != http.StatusPartialContent {
			t.Fatalf("range %d-%d: status %d", c.start, c.end, resp.StatusCode)
		}
		if !bytes.Equal(body, content[c.start:c.end+1]) {
			t.Fatalf("range %d-%d: wrong bytes", c.start, c.end)
		}
	}
}

func TestEdgeUncoveredRangeStillUsesRemote(t *testing.T) {
	srvURL, entry, _, _ := newEdgeServer(t)
	// Middle of the file is not stored and there is no debrid: must fail
	// before headers (fast-fail preserved), not return local bytes.
	resp, _ := getRange(t, streamURLFor(srvURL, entry), "bytes=500-600")
	if resp.StatusCode < 400 {
		t.Fatalf("uncovered range returned %d without a debrid source", resp.StatusCode)
	}
}

func TestEdgeHeadThenRemoteSendsStoredBytesFirst(t *testing.T) {
	srvURL, entry, content, _ := newEdgeServer(t)
	// Head is local, the rest needs debrid (absent): the client must still
	// receive the 100 stored bytes before the stream fails.
	req, _ := http.NewRequest(http.MethodGet, streamURLFor(srvURL, entry), nil)
	req.Header.Set("Range", "bytes=0-499")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusPartialContent {
		t.Fatalf("status %d", resp.StatusCode)
	}
	got := make([]byte, 100)
	if _, err := io.ReadFull(resp.Body, got); err != nil {
		t.Fatalf("stored head not delivered: %v", err)
	}
	if !bytes.Equal(got, content[:100]) {
		t.Fatal("wrong head bytes")
	}
}

func newStreamServerWith(t *testing.T, m *manager.Manager, entry *storage.Entry) (string, *storage.Entry) {
	t.Helper()
	h := NewHandler(m)
	r := chi.NewRouter()
	r.Get("/stream/{infohash}/{fileID}/{name}", h.handleStream)
	srv := httptest.NewServer(r)
	t.Cleanup(srv.Close)
	return srv.URL, entry
}

func streamURLFor(base string, entry *storage.Entry) string {
	return base + "/stream/" + entry.InfoHash + "/" + entry.Files["Movie.2023.mkv"].ID + "/Movie.2023.mkv"
}
