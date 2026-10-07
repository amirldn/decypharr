package torbox

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/utils"
	debridTypes "github.com/sirrobot01/decypharr/pkg/debrid/types"
)

func TestMediatorDualTokenBucket(t *testing.T) {
	valve := NewSubmissionValve(4.0, 45.0/3600.0, 300*time.Second)

	now := time.Now()
	// Cached tokens start with capacity=10
	for i := 0; i < 10; i++ {
		if !valve.TryAcquire(OpCreateCached, now) {
			t.Fatalf("expected cached token %d to be granted", i)
		}
	}
	// 11th should be denied
	if valve.TryAcquire(OpCreateCached, now) {
		t.Fatal("expected 11th cached token to be denied due to capacity")
	}

	// Uncached tokens start with capacity=1
	if !valve.TryAcquire(OpCreateUncached, now) {
		t.Fatal("expected first uncached token to be granted")
	}
	// 2nd uncached should be denied immediately
	if valve.TryAcquire(OpCreateUncached, now) {
		t.Fatal("expected second uncached token to be denied")
	}
}

func TestMediatorSingleflightDedup(t *testing.T) {
	config.SetConfigPath(t.TempDir())

	var wireCreates int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/api/torrents/createtorrent" {
			atomic.AddInt32(&wireCreates, 1)
			time.Sleep(50 * time.Millisecond) // Simulate wire latency
			_, _ = fmt.Fprint(w, `{"success":true,"data":{"torrent_id":999,"hash":"COALESCE_HASH"}}`)
		} else {
			_, _ = fmt.Fprint(w, `{"success":true,"data":{"coalesce_hash":{"hash":"COALESCE_HASH","size":1000}}}`)
		}
	}))
	defer server.Close()

	tb := testTorbox(server.URL)
	mediator, err := NewSubmissionMediator(tb, config.Debrid{
		Name: "torbox",
	}, nil, zerolog.Nop())
	if err != nil {
		t.Fatalf("failed to create mediator: %v", err)
	}
	tb.mediator = mediator

	numCallers := 10
	var wg sync.WaitGroup
	results := make([]*debridTypes.Torrent, numCallers)
	errorsList := make([]error, numCallers)

	for i := 0; i < numCallers; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			torrent := &debridTypes.Torrent{
				InfoHash:         "COALESCE_HASH",
				DownloadUncached: true,
				Magnet:           &utils.Magnet{Link: "magnet:?xt=urn:btih:COALESCE_HASH"},
			}
			res, err := tb.SubmitMagnet(torrent)
			results[idx] = res
			errorsList[idx] = err
		}(i)
	}

	wg.Wait()

	if got := atomic.LoadInt32(&wireCreates); got != 1 {
		t.Fatalf("expected exactly 1 wire create call for 10 concurrent submissions, got %d", got)
	}

	for i := 0; i < numCallers; i++ {
		if errorsList[i] != nil {
			t.Fatalf("caller %d got error: %v", i, errorsList[i])
		}
		if results[i] == nil || results[i].Id != "999" {
			t.Fatalf("caller %d got invalid result: %v", i, results[i])
		}
	}
}

func TestMediator429FeedbackLoop(t *testing.T) {
	valve := NewSubmissionValve(4.0, 45.0/3600.0, 100*time.Millisecond)
	now := time.Now()

	// Initially not frozen
	rate, _, isFrozen := valve.CurrentRates(now)
	if isFrozen || rate < 200 {
		t.Fatalf("unexpected initial state: rate=%f, isFrozen=%v", rate, isFrozen)
	}

	// Trigger 429 with 50ms Retry-After
	valve.Observe429(now, 50*time.Millisecond)

	// During freeze -> rate is 0, TryAcquire fails
	if valve.TryAcquire(OpCreateCached, now.Add(20*time.Millisecond)) {
		t.Fatal("expected acquire to fail during 429 freeze")
	}
	rateFrozen, _, isFrozen := valve.CurrentRates(now.Add(20 * time.Millisecond))
	if !isFrozen || rateFrozen != 0.0 {
		t.Fatalf("expected frozen state at t+20ms: rate=%f, isFrozen=%v", rateFrozen, isFrozen)
	}

	// After freeze (t+50ms) -> unfreezes at floor (20% of 240 = 48/min)
	rateFloor, _, isFrozen := valve.CurrentRates(now.Add(50 * time.Millisecond))
	if isFrozen || rateFloor < 47 || rateFloor > 49 {
		t.Fatalf("expected unfreezing at exact floor rate: rate=%f, isFrozen=%v", rateFloor, isFrozen)
	}

	// Mid-ramp (t+60ms) -> rate increases linearly (20% + 10% of 80% = 28% of 240 = 67.2/min)
	rateRamp, _, isFrozen := valve.CurrentRates(now.Add(60 * time.Millisecond))
	if isFrozen || rateRamp < 65 || rateRamp > 70 {
		t.Fatalf("expected mid-ramp rate: rate=%f, isFrozen=%v", rateRamp, isFrozen)
	}

	// After full ramp duration -> rate restored to 240/min
	rateRestored, _, isFrozen := valve.CurrentRates(now.Add(200 * time.Millisecond))
	if isFrozen || rateRestored < 235 {
		t.Fatalf("expected restored rate after ramp: rate=%f, isFrozen=%v", rateRestored, isFrozen)
	}
}

func TestMediatorPersistentQueueWALAndCrashRecovery(t *testing.T) {
	tempDir := t.TempDir()
	journalPath := filepath.Join(tempDir, "submission_queue.jsonl")

	// 1. Initialize queue and enqueue submissions
	queue, err := NewPersistentSubmissionQueue(journalPath, 500, zerolog.Nop())
	if err != nil {
		t.Fatalf("failed to create queue: %v", err)
	}

	item1 := &JournalRecord{InfoHash: "hash1", Status: StatusPending}
	item2 := &JournalRecord{InfoHash: "hash2", Status: StatusPending}
	item3 := &JournalRecord{InfoHash: "hash3", Status: StatusPending}

	_, _ = queue.Enqueue(item1)
	_, _ = queue.Enqueue(item2)
	_, _ = queue.Enqueue(item3)

	if queue.Len() != 3 {
		t.Fatalf("expected queue len=3, got %d", queue.Len())
	}

	// Mark item1 completed
	queue.Complete("hash1", StatusCompleted, "SUCCESS")
	if queue.Len() != 2 {
		t.Fatalf("expected queue len=2 after completion, got %d", queue.Len())
	}

	// 2. Simulate process crash: instantiate new queue from same journal file
	recoveredQueue, err := NewPersistentSubmissionQueue(journalPath, 500, zerolog.Nop())
	if err != nil {
		t.Fatalf("failed to recover queue: %v", err)
	}

	// Expected: item1 was completed, item2 and item3 were pending -> recovered len = 2
	if recoveredQueue.Len() != 2 {
		t.Fatalf("expected recovered queue len=2, got %d", recoveredQueue.Len())
	}

	// Verify journal file exists and contains entries
	if _, err := os.Stat(journalPath); os.IsNotExist(err) {
		t.Fatal("expected journal file to exist on disk")
	}
}

func TestMediatorQueueOverflowBackpressure(t *testing.T) {
	tempDir := t.TempDir()
	journalPath := filepath.Join(tempDir, "small_queue.jsonl")

	queue, err := NewPersistentSubmissionQueue(journalPath, 2, zerolog.Nop())
	if err != nil {
		t.Fatalf("failed to create queue: %v", err)
	}

	// Enqueue 2 items (reaches capacity)
	_, err1 := queue.Enqueue(&JournalRecord{InfoHash: "hashA"})
	_, err2 := queue.Enqueue(&JournalRecord{InfoHash: "hashB"})
	if err1 != nil || err2 != nil {
		t.Fatalf("unexpected error on valid enqueues: %v, %v", err1, err2)
	}

	// 3rd item should trigger queue full backpressure error
	_, err3 := queue.Enqueue(&JournalRecord{InfoHash: "hashC"})
	if err3 != ErrSubmissionQueueFull {
		t.Fatalf("expected ErrSubmissionQueueFull, got %v", err3)
	}
	if queue.Overflows() != 1 {
		t.Fatalf("expected 1 overflow recorded, got %d", queue.Overflows())
	}
}

func TestMediatorRateStringParsing(t *testing.T) {
	for _, tc := range []struct {
		input    string
		wantRate float64
		wantErr  bool
	}{
		{input: "240/minute", wantRate: 4.0},
		{input: "45/hour", wantRate: 45.0 / 3600.0},
		{input: "10/second", wantRate: 10.0},
		{input: "100/day", wantRate: 100.0 / 86400.0},
		{input: "invalid", wantErr: true},
		{input: "-5/minute", wantErr: true},
		{input: "10/unknown_unit", wantErr: true},
	} {
		rate, err := parseRateString(tc.input)
		if tc.wantErr {
			if err == nil {
				t.Errorf("parseRateString(%q) expected error, got nil", tc.input)
			}
		} else {
			if err != nil {
				t.Errorf("parseRateString(%q) unexpected error: %v", tc.input, err)
			}
			if rate != tc.wantRate {
				t.Errorf("parseRateString(%q) = %f, want %f", tc.input, rate, tc.wantRate)
			}
		}
	}
}

func TestMediatorTelemetryLogging(t *testing.T) {
	valve := NewSubmissionValve(4.0, 45.0/3600.0, 300*time.Second)
	dedup := NewSubmissionDedup()
	queue, _ := NewPersistentSubmissionQueue("", 500, zerolog.Nop())
	negCache := NewNegativeCache(30*time.Minute, 1000)

	mediator := &SubmissionMediator{
		valve:         valve,
		dedup:         dedup,
		queue:         queue,
		negativeCache: negCache,
		enabled:       true,
		logger:        zerolog.Nop(),
	}

	l := zerolog.Nop()
	event := l.Info()
	mediator.LogTelemetry(event)
	// Passes without panicking
}
