package manager

import (
	"context"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/utils"
	"github.com/sourcegraph/conc/pool"
)

const (
	MaxNZBPreCacheFiles = 5
	CacheWarmTimeout    = 60 * time.Second

	// cacheWarmSerialFileCount is the media-file count at which a finished pack
	// warms one file at a time. A 4+ episode pack used to fill the old 10-wide
	// pool on its own; serializing it keeps the burst under the TorBox 429s
	// from KGBos/liteflix#249 while a single episode or 2–3 file pack can still
	// use DefaultCacheWarmWorkers.
	cacheWarmSerialFileCount = 4

	// Container metadata lives at the head (streamable MP4 moov, EBML header)
	// or the tail (non-streamable MP4 moov, MKV cues/seek index), so warming
	// head+tail covers what a downstream ffprobe/import scan will seek to.
	cacheWarmHeadSize = 2 * 1024 * 1024 // 2MB
	cacheWarmTailSize = 2 * 1024 * 1024 // 2MB
)

type MountManager interface {
	Start(ctx context.Context) error
	Stop() error
	Stats() map[string]any
	IsReady() bool
	Type() string
	Refresh(dirs []string) error
}

// InvalidateEntryCache clears cached entries. Reads rebuild them on demand.
func (m *Manager) InvalidateEntryCache() {
	m.entry.InvalidateAll()
}

func (m *Manager) RefreshMount() error {
	dirs := strings.FieldsFunc(config.Get().RefreshDirs, func(r rune) bool {
		return r == ',' || r == '&'
	})
	if len(dirs) == 0 {
		dirs = []string{"__all__"}
	}

	// Call event handler if set
	if m.mountManager != nil {
		return m.mountManager.Refresh(dirs)
	}
	return nil
}

// cacheWarmMaxWorkers returns the process-wide cache-warm slot cap from
// config, falling back to DefaultCacheWarmWorkers when unset.
func (m *Manager) cacheWarmMaxWorkers() int {
	if m != nil && m.config != nil && config.Get().MaxCacheWarmWorkers > 0 {
		return config.Get().MaxCacheWarmWorkers
	}
	return config.DefaultCacheWarmWorkers
}

// cacheWarmConcurrency is the per-call worker-pool size. Short packs (under
// cacheWarmSerialFileCount media files) may run up to `configured` workers so
// a single episode stays fast; larger packs serialize.
func cacheWarmConcurrency(nMediaFiles, configured int) int {
	max := configured
	if max <= 0 {
		max = config.DefaultCacheWarmWorkers
	}
	if nMediaFiles <= 0 {
		return 1
	}
	if nMediaFiles >= cacheWarmSerialFileCount {
		return 1
	}
	return min(nMediaFiles, max)
}

// cacheWarmGate is a counting semaphore whose max is supplied at acquire
// time, so a live config change takes effect on the next slot.
type cacheWarmGate struct {
	mu    sync.Mutex
	cond  *sync.Cond
	inUse int
}

func newCacheWarmGate() *cacheWarmGate {
	g := &cacheWarmGate{}
	g.cond = sync.NewCond(&g.mu)
	return g
}

func (g *cacheWarmGate) acquire(max int) {
	if max < 1 {
		max = 1
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	for g.inUse >= max {
		g.cond.Wait()
	}
	g.inUse++
}

func (g *cacheWarmGate) release() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.inUse > 0 {
		g.inUse--
	}
	g.cond.Signal()
}

func (m *Manager) getCacheWarmGate() *cacheWarmGate {
	m.cacheWarmOnce.Do(func() {
		m.cacheWarmGate = newCacheWarmGate()
	})
	return m.cacheWarmGate
}

// WarmFileCache reads the head and tail of each media file through the mount
// to warm the VFS disk cache, so a subsequent media probe or import scan over
// the mount is fast. This replaces spawning ffprobe: the read pattern is
// deterministic, needs no external binary, and warms the exact bytes a
// downstream probe seeks to (see cacheWarmHeadSize/cacheWarmTailSize).
//
// Concurrency is capped two ways so a finished season pack cannot open 10
// TorBox reads at once (liteflix#249): a per-call pool (serial for 4+ media
// files) and a Manager-wide gate so overlapping packs share the same budget.
func (m *Manager) WarmFileCache(filePaths []string) error {
	if len(filePaths) == 0 {
		return nil
	}

	media := make([]string, 0, len(filePaths))
	for _, fp := range filePaths {
		if utils.IsMediaFile(fp) {
			media = append(media, fp)
		}
	}
	if len(media) == 0 {
		return nil
	}

	max := m.cacheWarmMaxWorkers()
	workers := cacheWarmConcurrency(len(media), max)
	p := pool.New().WithMaxGoroutines(workers)
	gate := m.getCacheWarmGate()

	for _, fp := range media {
		p.Go(func() {
			gate.acquire(max)
			defer gate.release()
			ctx, cancel := context.WithTimeout(context.Background(), CacheWarmTimeout)
			defer cancel()
			warm := m.warmOneFile
			if m.warmOneFileFn != nil {
				warm = m.warmOneFileFn
			}
			if err := warm(ctx, fp); err != nil {
				// Log error but continue
				m.logger.Warn().
					Err(err).
					Str("file", fp).
					Msg("cache warm failed")
			}
		})
	}

	p.Wait()
	return nil
}

// warmOneFile reads the head and (for large enough files) the tail of path,
// going through the mount so the FUSE/VFS cache is populated.
func (m *Manager) warmOneFile(ctx context.Context, path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()

	fi, err := f.Stat()
	if err != nil {
		return err
	}
	size := fi.Size()
	if size == 0 {
		return nil
	}

	head := min(int64(cacheWarmHeadSize), size)
	if err := drainRange(ctx, f, 0, head); err != nil {
		return err
	}

	// Only warm the tail when it doesn't overlap the head we just read.
	if size > int64(cacheWarmHeadSize)+int64(cacheWarmTailSize) {
		if err := drainRange(ctx, f, size-int64(cacheWarmTailSize), int64(cacheWarmTailSize)); err != nil {
			return err
		}
	}
	return nil
}

// drainRange reads length bytes starting at off, in chunks, discarding the
// data and checking ctx between chunks so a stalled mount can't pin a worker
// past CacheWarmTimeout.
func drainRange(ctx context.Context, r io.ReaderAt, off, length int64) error {
	const chunk = 1 << 20 // 1MB
	buf := make([]byte, chunk)
	for read := int64(0); read < length; {
		if err := ctx.Err(); err != nil {
			return err
		}
		n := min(length-read, chunk)
		got, err := r.ReadAt(buf[:n], off+read)
		read += int64(got)
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
	}
	return nil
}

type stubMountManager struct{}

func (s *stubMountManager) Refresh(dirs []string) error {
	return nil
}

func NewStubMountManager() MountManager {
	return &stubMountManager{}
}

func (s *stubMountManager) Start(ctx context.Context) error {
	return nil
}
func (s *stubMountManager) Stop() error {
	return nil
}
func (s *stubMountManager) Stats() map[string]any {
	return map[string]any{
		"message": "no mount configured",
	}
}
func (s *stubMountManager) IsReady() bool {
	return false
}
func (s *stubMountManager) Type() string {
	return "none"
}
