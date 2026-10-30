package arr

import (
	stdjson "encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
)

// fakeQueueArr serves a mutable Sonarr queue and records bulk removals.
type fakeQueueArr struct {
	mu       sync.Mutex
	records  []QueueSchema
	removals [][]int
	// fromClient records the removeFromClient flag of each bulk removal.
	fromClient []string
}

func (f *fakeQueueArr) set(records ...QueueSchema) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.records = records
}

func (f *fakeQueueArr) removed() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.removals)
}

func (f *fakeQueueArr) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	switch {
	case r.Method == http.MethodGet && r.URL.Path == "/api/v3/queue":
		records := f.records
		if r.URL.Query().Get("page") != "1" {
			records = nil
		}
		_ = stdjson.NewEncoder(w).Encode(QueueResponseScheme{TotalRecords: len(f.records), Records: records})
	case r.Method == http.MethodDelete && r.URL.Path == "/api/v3/queue/bulk":
		var payload struct {
			Ids []int `json:"ids"`
		}
		_ = stdjson.NewDecoder(r.Body).Decode(&payload)
		f.removals = append(f.removals, payload.Ids)
		f.fromClient = append(f.fromClient, r.URL.Query().Get("removeFromClient"))
		w.WriteHeader(http.StatusOK)
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

func importProblem(id int, downloadID string) QueueSchema {
	return queueProblem(id, downloadID, "No files found are eligible for import in /mnt/symlinks/sonarr/pack")
}

func queueProblem(id int, downloadID, message string) QueueSchema {
	item := QueueSchema{
		Id:                    id,
		DownloadId:            downloadID,
		Status:                "completed",
		TrackedDownloadStatus: "warning",
		TrackedDownloadState:  "importPending",
	}
	item.StatusMessages = append(item.StatusMessages, struct {
		Title    string   `json:"title"`
		Messages []string `json:"messages"`
	}{Title: message})
	return item
}

func graceTestService(t *testing.T, arr *fakeQueueArr) (*Service, *time.Time) {
	t.Helper()
	if _, err := config.Update(func(c *config.Config) error {
		c.QueueCleanup.Rules = config.DefaultQueueCleanupRules()
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(arr)
	t.Cleanup(server.Close)
	s := testService(Arr{Host: server.URL, Token: "secret", Type: Sonarr})
	now := time.Date(2026, time.October, 29, 17, 5, 30, 0, time.UTC)
	s.now = func() time.Time { return now }
	return s, &now
}

// On 2026-10-29 decypharr briefly answered 404 for fresh pack files during a
// torrent refresh. Sonarr's import check failed for ~20s, and the 10s cleanup
// blocklisted three good packs before Sonarr's own retry (about once a minute)
// could import them. Import problems now have to persist before any action.
func TestCleanupQueueWaitsBeforeBlocklistingImportProblems(t *testing.T) {
	arr := &fakeQueueArr{}
	s, now := graceTestService(t, arr)
	arr.set(importProblem(1, "PACK"), importProblem(2, "PACK"))

	if err := s.CleanupQueue(t.Context(), "arr"); err != nil {
		t.Fatal(err)
	}
	if arr.removed() != 0 {
		t.Fatal("blocklisted an import problem on first sight")
	}

	*now = now.Add(queueCleanupGrace - time.Minute)
	if err := s.CleanupQueue(t.Context(), "arr"); err != nil {
		t.Fatal(err)
	}
	if arr.removed() != 0 {
		t.Fatal("blocklisted before the grace period ended")
	}

	*now = now.Add(time.Minute)
	if err := s.CleanupQueue(t.Context(), "arr"); err != nil {
		t.Fatal(err)
	}
	if arr.removed() != 1 {
		t.Fatalf("bulk removals = %d, want 1 once the problem persisted for the grace period", arr.removed())
	}
}

func TestCleanupQueueBlocklistsFailedDownloadsImmediately(t *testing.T) {
	arr := &fakeQueueArr{}
	s, _ := graceTestService(t, arr)
	arr.set(QueueSchema{Id: 7, DownloadId: "BAD", Status: "failed", TrackedDownloadStatus: "error"})

	if err := s.CleanupQueue(t.Context(), "arr"); err != nil {
		t.Fatal(err)
	}
	if arr.removed() != 1 {
		t.Fatalf("bulk removals = %d, want an immediate blocklist for a failed download", arr.removed())
	}
}

func TestCleanupQueueRestartsGraceWhenProblemClears(t *testing.T) {
	arr := &fakeQueueArr{}
	s, now := graceTestService(t, arr)

	arr.set(importProblem(1, "PACK"))
	if err := s.CleanupQueue(t.Context(), "arr"); err != nil {
		t.Fatal(err)
	}

	*now = now.Add(5 * time.Minute)
	arr.set() // Sonarr's retry imported it; the item left the queue
	if err := s.CleanupQueue(t.Context(), "arr"); err != nil {
		t.Fatal(err)
	}

	*now = now.Add(queueCleanupGrace - time.Minute)
	arr.set(importProblem(1, "PACK")) // a new, unrelated hiccup later on
	if err := s.CleanupQueue(t.Context(), "arr"); err != nil {
		t.Fatal(err)
	}
	if arr.removed() != 0 {
		t.Fatal("grace timer carried over from an earlier, cleared problem")
	}
}

// A pack covering only part of a season is reported as "episodes missing"
// even after its files imported. Removing the download from decypharr then
// deletes the files those imported episodes link to (2026-10-30, Naruto
// S2 Part 1). Such items may still be blocklisted, but must stay in the client.
func TestCleanupQueueKeepsPartlyImportedDownloadsInClient(t *testing.T) {
	arr := &fakeQueueArr{}
	s, now := graceTestService(t, arr)
	arr.set(queueProblem(1, "PART", "One or more episodes expected in this release were not imported or missing from the release"))

	if err := s.CleanupQueue(t.Context(), "arr"); err != nil {
		t.Fatal(err)
	}
	*now = now.Add(queueCleanupGrace)
	if err := s.CleanupQueue(t.Context(), "arr"); err != nil {
		t.Fatal(err)
	}

	if arr.removed() != 1 {
		t.Fatalf("bulk removals = %d, want 1", arr.removed())
	}
	if got := arr.fromClient[0]; got != "false" {
		t.Fatalf("removeFromClient = %q, want \"false\" for a partly imported download", got)
	}
}

func TestCleanupQueueStillRemovesUnimportedDownloadsFromClient(t *testing.T) {
	arr := &fakeQueueArr{}
	s, now := graceTestService(t, arr)
	arr.set(importProblem(1, "NONE"))

	if err := s.CleanupQueue(t.Context(), "arr"); err != nil {
		t.Fatal(err)
	}
	*now = now.Add(queueCleanupGrace)
	if err := s.CleanupQueue(t.Context(), "arr"); err != nil {
		t.Fatal(err)
	}

	if arr.removed() != 1 || arr.fromClient[0] != "true" {
		t.Fatalf("removals = %d, removeFromClient = %v; want one removal from the client", arr.removed(), arr.fromClient)
	}
}
