package torbox

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/utils"
	debridTypes "github.com/sirrobot01/decypharr/pkg/debrid/types"
)

func TestNegativeCacheBasicGetPut(t *testing.T) {
	nc := NewNegativeCache(10*time.Minute, 100)

	// Miss on empty cache
	if _, found := nc.Get("AABBCC112233"); found {
		t.Fatal("expected cache miss on empty cache")
	}

	// Put entry with uppercase hash
	verdict := nc.Put("AABBCC112233", "DOWNLOAD_NOT_CACHED", 0)
	if verdict == nil {
		t.Fatal("expected non-nil verdict from Put")
	}
	if verdict.InfoHash != "aabbcc112233" {
		t.Fatalf("expected normalized hash aabbcc112233, got %s", verdict.InfoHash)
	}
	if verdict.Attempts != 1 {
		t.Fatalf("expected attempts=1, got %d", verdict.Attempts)
	}

	// Hit with lowercase hash (case insensitivity)
	got, found := nc.Get("aabbcc112233")
	if !found || got == nil {
		t.Fatal("expected cache hit for lowercase hash")
	}
	if got.Reason != "DOWNLOAD_NOT_CACHED" {
		t.Fatalf("expected reason DOWNLOAD_NOT_CACHED, got %s", got.Reason)
	}

	// Second Put increments attempts
	verdict2 := nc.Put("aabbcc112233", "DOWNLOAD_NOT_CACHED", 0)
	if verdict2.Attempts != 2 {
		t.Fatalf("expected attempts=2, got %d", verdict2.Attempts)
	}

	// Eviction
	if !nc.Evict("AABBCC112233") {
		t.Fatal("expected successful eviction")
	}
	if _, found := nc.Get("aabbcc112233"); found {
		t.Fatal("expected cache miss after eviction")
	}
}

func TestNegativeCacheTTL(t *testing.T) {
	shortTTL := 20 * time.Millisecond
	nc := NewNegativeCache(shortTTL, 100)

	nc.Put("test_hash_ttl", "DOWNLOAD_NOT_CACHED", shortTTL)

	if _, found := nc.Get("test_hash_ttl"); !found {
		t.Fatal("expected cache hit before TTL expiry")
	}

	time.Sleep(30 * time.Millisecond)

	if _, found := nc.Get("test_hash_ttl"); found {
		t.Fatal("expected cache miss after TTL expiry")
	}
	if nc.Len() != 0 {
		t.Fatalf("expected lazy eviction to clean expired entry, len=%d", nc.Len())
	}
}

func TestNegativeCacheCapacityEviction(t *testing.T) {
	maxCap := 3
	nc := NewNegativeCache(10*time.Minute, maxCap)

	nc.Put("hash1", "reason1", 0)
	time.Sleep(1 * time.Millisecond)
	nc.Put("hash2", "reason2", 0)
	time.Sleep(1 * time.Millisecond)
	nc.Put("hash3", "reason3", 0)

	if nc.Len() != 3 {
		t.Fatalf("expected len=3, got %d", nc.Len())
	}

	// Adding 4th item should evict oldest (hash1)
	time.Sleep(1 * time.Millisecond)
	nc.Put("hash4", "reason4", 0)

	if nc.Len() != 3 {
		t.Fatalf("expected len=3 after eviction, got %d", nc.Len())
	}
	if _, found := nc.Get("hash1"); found {
		t.Fatal("expected oldest entry hash1 to be evicted")
	}
	if _, found := nc.Get("hash4"); !found {
		t.Fatal("expected newest entry hash4 to be present")
	}
}

func TestNegativeCacheConcurrentSafety(t *testing.T) {
	nc := NewNegativeCache(10*time.Minute, 1000)
	var wg sync.WaitGroup
	numWorkers := 20
	iterations := 100

	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				hash := fmt.Sprintf("hash_%d_%d", workerID, i%10)
				nc.Put(hash, "DOWNLOAD_NOT_CACHED", 0)
				_, _ = nc.Get(hash)
				if i%5 == 0 {
					nc.Evict(hash)
				}
			}
		}(w)
	}

	wg.Wait()
	hits, misses, evictions, size := nc.Stats()
	if hits == 0 && misses == 0 {
		t.Fatal("expected non-zero hit/miss stats")
	}
	t.Logf("Concurrent stats: hits=%d misses=%d evictions=%d size=%d", hits, misses, evictions, size)
}

func TestSubmitMagnetNegativeCacheFastReject(t *testing.T) {
	config.SetConfigPath(t.TempDir())

	var wireChecks int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/api/torrents/checkcached" {
			atomic.AddInt32(&wireChecks, 1)
			// Return uncached response
			_, _ = fmt.Fprint(w, `{"success":true,"data":{}}`)
		} else {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
	}))
	defer server.Close()

	tb := testTorbox(server.URL)
	tb.negativeCache = NewNegativeCache(10*time.Minute, 100)

	torrent := &debridTypes.Torrent{
		InfoHash:         "DEADBEEF11223344",
		DownloadUncached: false,
		Magnet:           &utils.Magnet{Link: "magnet:?xt=urn:btih:DEADBEEF11223344"},
	}

	// 1st submission: must hit wire checkcached and return DOWNLOAD_NOT_CACHED
	_, err := tb.SubmitMagnet(torrent)
	if err == nil || err.Error() != "DOWNLOAD_NOT_CACHED" {
		t.Fatalf("first SubmitMagnet() error = %v, want DOWNLOAD_NOT_CACHED", err)
	}
	if got := atomic.LoadInt32(&wireChecks); got != 1 {
		t.Fatalf("expected 1 wire check, got %d", got)
	}

	// Subsequent 50 submissions: must be rejected locally from NegativeCache with ZERO additional wire calls
	for i := 0; i < 50; i++ {
		_, err := tb.SubmitMagnet(torrent)
		if err == nil || err.Error() != "DOWNLOAD_NOT_CACHED" {
			t.Fatalf("retry %d SubmitMagnet() error = %v, want DOWNLOAD_NOT_CACHED", i, err)
		}
	}
	if got := atomic.LoadInt32(&wireChecks); got != 1 {
		t.Fatalf("expected still exactly 1 wire check after 50 retries, got %d", got)
	}

	hits, _, _, _ := tb.negativeCache.Stats()
	if hits < 50 {
		t.Fatalf("expected at least 50 negative cache hits, got %d", hits)
	}
}

func TestIsAvailableNegativeCacheFastSkip(t *testing.T) {
	config.SetConfigPath(t.TempDir())

	var wireChecks int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/api/torrents/checkcached" {
			atomic.AddInt32(&wireChecks, 1)
			hashQuery := r.URL.Query().Get("hash")
			if hashQuery == "AVAILABLEHASH" {
				_, _ = fmt.Fprint(w, `{"success":true,"data":{"AVAILABLEHASH":{"hash":"AVAILABLEHASH","size":500}}}`)
			} else {
				_, _ = fmt.Fprint(w, `{"success":true,"data":{}}`)
			}
		}
	}))
	defer server.Close()

	tb := testTorbox(server.URL)
	tb.negativeCache = NewNegativeCache(10*time.Minute, 100)

	// Pre-seed negative cache with "UNCACHEDHASH"
	tb.negativeCache.Put("UNCACHEDHASH", "DOWNLOAD_NOT_CACHED", 0)

	// Check IsAvailable for UNCACHEDHASH alone -> 0 wire calls made
	res1 := tb.IsAvailable([]string{"UNCACHEDHASH"})
	if res1["UNCACHEDHASH"] {
		t.Fatal("expected UNCACHEDHASH to be unavailable")
	}
	if got := atomic.LoadInt32(&wireChecks); got != 0 {
		t.Fatalf("expected 0 wire checks for pre-cached negative entry, got %d", got)
	}

	// Check IsAvailable for AVAILABLEHASH -> 1 wire call made, returns true
	res2 := tb.IsAvailable([]string{"AVAILABLEHASH"})
	if !res2["AVAILABLEHASH"] {
		t.Fatal("expected AVAILABLEHASH to be available")
	}
	if got := atomic.LoadInt32(&wireChecks); got != 1 {
		t.Fatalf("expected 1 wire check for AVAILABLEHASH, got %d", got)
	}
}

func TestNegativeCacheConfiguration(t *testing.T) {
	// Valid defaults
	ttl, maxCap, err := negativeCacheConfig(config.Debrid{})
	if err != nil {
		t.Fatalf("negativeCacheConfig default error: %v", err)
	}
	if ttl != DefaultNegativeCacheTTL || maxCap != DefaultNegativeCacheMax {
		t.Fatalf("unexpected defaults: ttl=%v, maxCap=%d", ttl, maxCap)
	}

	// Custom valid options
	ttl, maxCap, err = negativeCacheConfig(config.Debrid{
		TorboxNegativeCacheTTL: "45m",
		TorboxNegativeCacheMax: 5000,
	})
	if err != nil {
		t.Fatalf("negativeCacheConfig custom error: %v", err)
	}
	if ttl != 45*time.Minute || maxCap != 5000 {
		t.Fatalf("unexpected parsed values: ttl=%v, maxCap=%d", ttl, maxCap)
	}

	// Invalid options
	if _, _, err := negativeCacheConfig(config.Debrid{TorboxNegativeCacheTTL: "invalid"}); err == nil {
		t.Fatal("expected error on invalid duration")
	}
	if _, _, err := negativeCacheConfig(config.Debrid{TorboxNegativeCacheTTL: "100h"}); err == nil {
		t.Fatal("expected error on TTL exceeding 48h")
	}
	if _, _, err := negativeCacheConfig(config.Debrid{TorboxNegativeCacheMax: -5}); err == nil {
		t.Fatal("expected error on negative max entries")
	}
}
