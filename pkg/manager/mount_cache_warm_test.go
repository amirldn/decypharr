package manager

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
)

func TestCacheWarmConcurrency(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		nMediaFiles int
		configured  int
		want        int
	}{
		{name: "default short pack", nMediaFiles: 2, configured: 0, want: 2},
		{name: "default single file", nMediaFiles: 1, configured: 0, want: 1},
		{name: "default season pack is serial", nMediaFiles: 8, configured: 0, want: 1},
		{name: "default 22-episode pack is serial", nMediaFiles: 22, configured: 0, want: 1},
		{name: "configured 1 is always serial", nMediaFiles: 3, configured: 1, want: 1},
		{name: "short pack honors configured 2", nMediaFiles: 3, configured: 2, want: 2},
		{name: "four files serial even if configured 10", nMediaFiles: 4, configured: 10, want: 1},
		{name: "empty list", nMediaFiles: 0, configured: 2, want: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := cacheWarmConcurrency(tt.nMediaFiles, tt.configured)
			if got != tt.want {
				t.Fatalf("cacheWarmConcurrency(%d, %d) = %d, want %d", tt.nMediaFiles, tt.configured, got, tt.want)
			}
		})
	}
}

func writeMediaFiles(t *testing.T, n int) []string {
	t.Helper()
	dir := t.TempDir()
	paths := make([]string, n)
	for i := 0; i < n; i++ {
		p := filepath.Join(dir, fmt.Sprintf("ep%02d.mkv", i+1))
		if err := os.WriteFile(p, []byte("media"), 0o644); err != nil {
			t.Fatal(err)
		}
		paths[i] = p
	}
	return paths
}

type warmTracker struct {
	current atomic.Int32
	max     atomic.Int32
	started chan struct{}
	block   chan struct{}
}

func newWarmTracker(buf int) *warmTracker {
	return &warmTracker{
		started: make(chan struct{}, buf),
		block:   make(chan struct{}),
	}
}

func (w *warmTracker) fn(ctx context.Context, path string) error {
	n := w.current.Add(1)
	for {
		old := w.max.Load()
		if n <= old || w.max.CompareAndSwap(old, n) {
			break
		}
	}
	select {
	case w.started <- struct{}{}:
	default:
	}
	select {
	case <-w.block:
	case <-ctx.Done():
		w.current.Add(-1)
		return ctx.Err()
	}
	w.current.Add(-1)
	return nil
}

func waitStarted(t *testing.T, started <-chan struct{}, n int) {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for i := 0; i < n; i++ {
		select {
		case <-started:
		case <-deadline:
			t.Fatalf("timed out waiting for warm %d/%d to start", i+1, n)
		}
	}
}

func closeWarmBlock(t *testing.T, tr *warmTracker) {
	t.Helper()
	t.Cleanup(func() {
		select {
		case <-tr.block:
		default:
			close(tr.block)
		}
	})
}

func TestWarmFileCacheShortPackUsesConfiguredWorkers(t *testing.T) {
	paths := writeMediaFiles(t, 3)
	tr := newWarmTracker(3)
	closeWarmBlock(t, tr)
	m := &Manager{
		logger:        zerolog.Nop(),
		config:        &config.Config{MaxCacheWarmWorkers: 2},
		warmOneFileFn: tr.fn,
	}

	done := make(chan error, 1)
	go func() { done <- m.WarmFileCache(paths) }()

	waitStarted(t, tr.started, 2)
	if tr.current.Load() != 2 {
		t.Fatalf("short pack concurrent warms = %d, want 2", tr.current.Load())
	}
	select {
	case <-tr.started:
		t.Fatal("third file started before a slot was free")
	case <-time.After(100 * time.Millisecond):
	}

	close(tr.block)
	if err := <-done; err != nil {
		t.Fatalf("WarmFileCache: %v", err)
	}
	if got := tr.max.Load(); got != 2 {
		t.Fatalf("max concurrent = %d, want 2", got)
	}
}

func TestWarmFileCacheSeasonPackIsSerial(t *testing.T) {
	paths := writeMediaFiles(t, 8)
	tr := newWarmTracker(8)
	closeWarmBlock(t, tr)
	m := &Manager{
		logger:        zerolog.Nop(),
		config:        &config.Config{MaxCacheWarmWorkers: 10},
		warmOneFileFn: tr.fn,
	}

	done := make(chan error, 1)
	go func() { done <- m.WarmFileCache(paths) }()

	waitStarted(t, tr.started, 1)
	if tr.current.Load() != 1 {
		t.Fatalf("season pack concurrent warms = %d, want 1", tr.current.Load())
	}
	select {
	case <-tr.started:
		t.Fatal("season pack started a second warm concurrently")
	case <-time.After(100 * time.Millisecond):
	}

	close(tr.block)
	if err := <-done; err != nil {
		t.Fatalf("WarmFileCache: %v", err)
	}
	if got := tr.max.Load(); got != 1 {
		t.Fatalf("max concurrent = %d, want 1", got)
	}
}

func TestWarmFileCacheSharesBudgetAcrossOverlappingPacks(t *testing.T) {
	left := writeMediaFiles(t, 3)
	right := writeMediaFiles(t, 3)
	tr := newWarmTracker(6)
	closeWarmBlock(t, tr)
	m := &Manager{
		logger:        zerolog.Nop(),
		config:        &config.Config{MaxCacheWarmWorkers: 2},
		warmOneFileFn: tr.fn,
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		if err := m.WarmFileCache(left); err != nil {
			t.Errorf("left pack: %v", err)
		}
	}()
	go func() {
		defer wg.Done()
		if err := m.WarmFileCache(right); err != nil {
			t.Errorf("right pack: %v", err)
		}
	}()

	waitStarted(t, tr.started, 2)
	if tr.current.Load() != 2 {
		t.Fatalf("overlapping packs concurrent warms = %d, want 2", tr.current.Load())
	}
	select {
	case <-tr.started:
		t.Fatal("overlapping packs exceeded the shared budget of 2")
	case <-time.After(100 * time.Millisecond):
	}

	close(tr.block)
	wg.Wait()
	if got := tr.max.Load(); got != 2 {
		t.Fatalf("max concurrent = %d, want 2 (shared across both packs)", got)
	}
}

func TestWarmFileCacheSkipsNonMedia(t *testing.T) {
	dir := t.TempDir()
	mkv := filepath.Join(dir, "ep.mkv")
	nfo := filepath.Join(dir, "ep.nfo")
	if err := os.WriteFile(mkv, []byte("media"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(nfo, []byte("info"), 0o644); err != nil {
		t.Fatal(err)
	}

	var warmed atomic.Int32
	m := &Manager{
		logger: zerolog.Nop(),
		config: &config.Config{MaxCacheWarmWorkers: 2},
		warmOneFileFn: func(ctx context.Context, path string) error {
			warmed.Add(1)
			if path != mkv {
				t.Errorf("warmed non-media %q", path)
			}
			return nil
		},
	}
	if err := m.WarmFileCache([]string{nfo, mkv}); err != nil {
		t.Fatalf("WarmFileCache: %v", err)
	}
	if warmed.Load() != 1 {
		t.Fatalf("warmed %d files, want 1", warmed.Load())
	}
}

func TestWarmFileCacheDefaultWorkersWhenConfigUnset(t *testing.T) {
	if got := (&Manager{}).cacheWarmMaxWorkers(); got != config.DefaultCacheWarmWorkers {
		t.Fatalf("nil config workers = %d, want %d", got, config.DefaultCacheWarmWorkers)
	}
	if got := (&Manager{config: &config.Config{}}).cacheWarmMaxWorkers(); got != config.DefaultCacheWarmWorkers {
		t.Fatalf("zero config workers = %d, want %d", got, config.DefaultCacheWarmWorkers)
	}
}
