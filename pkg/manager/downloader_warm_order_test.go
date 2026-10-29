package manager

import (
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

// The Arr only starts importing once decypharr reports the entry complete, so
// the pre-cache warm must not sit in front of that report: it measured 5–11s
// per film and 37–47s per pack on 2026-10-28.
func TestCompleteAndWarmReportsCompletionBeforeWarmFinishes(t *testing.T) {
	paths := writeMediaFiles(t, 1)
	tr := newWarmTracker(1)
	closeWarmBlock(t, tr)
	m := &Manager{
		logger:        zerolog.Nop(),
		config:        cacheWarmTestConfig(t, 2),
		warmOneFileFn: tr.fn,
	}
	completed := make(chan struct{})
	d := &Downloader{
		manager:    m,
		logger:     zerolog.Nop(),
		completeFn: func(*storage.Entry) { close(completed) },
	}

	go d.completeAndWarm(&storage.Entry{Name: "film"}, paths)

	waitStarted(t, tr.started, 1) // the warm is running and held open
	select {
	case <-completed:
	case <-time.After(2 * time.Second):
		t.Fatal("entry was not reported complete while its cache warm was still running")
	}
}

func TestCompleteAndWarmStillWarmsAfterCompleting(t *testing.T) {
	paths := writeMediaFiles(t, 2)
	tr := newWarmTracker(2)
	close(tr.block)
	m := &Manager{
		logger:        zerolog.Nop(),
		config:        cacheWarmTestConfig(t, 2),
		warmOneFileFn: tr.fn,
	}
	d := &Downloader{manager: m, logger: zerolog.Nop(), completeFn: func(*storage.Entry) {}}

	d.completeAndWarm(&storage.Entry{Name: "pack"}, paths)

	waitStarted(t, tr.started, 2)
}

func TestCompleteAndWarmSkipsWarmWhenPreCacheDisabled(t *testing.T) {
	paths := writeMediaFiles(t, 1)
	tr := newWarmTracker(1)
	close(tr.block)
	cfg := cacheWarmTestConfig(t, 2)
	cfg.SkipPreCache = true
	m := &Manager{logger: zerolog.Nop(), config: cfg, warmOneFileFn: tr.fn}
	completed := false
	d := &Downloader{manager: m, logger: zerolog.Nop(), completeFn: func(*storage.Entry) { completed = true }}

	d.completeAndWarm(&storage.Entry{Name: "film"}, paths)

	if !completed {
		t.Fatal("entry not reported complete")
	}
	select {
	case <-tr.started:
		t.Fatal("warmed although skip_pre_cache is set")
	case <-time.After(200 * time.Millisecond):
	}
}
