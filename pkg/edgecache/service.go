package edgecache

import (
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/request"
	"github.com/sirrobot01/decypharr/internal/utils"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

// Source is what the service needs from the manager.
type Source interface {
	// OpenForCache opens an untracked read of filename at offset.
	OpenForCache(ctx context.Context, entry *storage.Entry, filename string, offset int64) (io.ReadCloser, error)
	// ActiveStreams is the number of user streams currently open.
	ActiveStreams() int
	// ForEachEntry visits every stored entry.
	ForEachEntry(fn func(*storage.Entry) error) error
}

const (
	pruneInterval      = 24 * time.Hour
	backfillStartDelay = 2 * time.Minute
	busyPoll           = 15 * time.Second
	fileFetchTimeout   = 2 * time.Minute
	importQueueSize    = 256
)

// Service owns the store and fills it: new downloads on completion, existing
// files by a paced background backfill, and passive capture of head/tail
// bytes that playback reads anyway. Fills use the background requestdl lane
// and pause while anyone is streaming.
type Service struct {
	store    *Store
	source   Source
	logger   zerolog.Logger
	backfill bool
	delay    time.Duration

	imports  chan *storage.Entry
	inflight sync.Map // key -> struct{}

	filled  atomic.Int64
	failed  atomic.Int64
	skipped atomic.Int64
}

// New builds the service from config. It returns nil, nil when disabled.
func New(cfg config.EdgeCache, source Source, logger zerolog.Logger) (*Service, error) {
	if !cfg.Enabled {
		return nil, nil
	}
	head, tail, err := cfg.EdgeCacheSizes()
	if err != nil {
		return nil, err
	}
	store, err := NewStore(cfg.Dir, head, tail)
	if err != nil {
		return nil, err
	}
	delayStr := cfg.BackfillDelay
	if delayStr == "" {
		delayStr = config.DefaultEdgeCacheBackfillDelay
	}
	delay, err := time.ParseDuration(delayStr)
	if err != nil {
		return nil, fmt.Errorf("edge_cache.backfill_delay: %w", err)
	}
	return &Service{
		store:    store,
		source:   source,
		logger:   logger,
		backfill: cfg.Backfill,
		delay:    delay,
		imports:  make(chan *storage.Entry, importQueueSize),
	}, nil
}

// Store exposes the underlying store for the read path.
func (s *Service) Store() *Store {
	if s == nil {
		return nil
	}
	return s.store
}

// Start launches the filler and pruner. It returns immediately.
func (s *Service) Start(ctx context.Context) {
	if s == nil {
		return
	}
	files, bytes := s.store.Usage()
	s.logger.Info().
		Str("dir", s.store.dir).
		Int64("head_bytes", s.store.headSize).
		Int64("tail_bytes", s.store.tailSize).
		Bool("backfill", s.backfill).
		Int("stored_parts", files).
		Int64("stored_bytes", bytes).
		Msg("Edge cache enabled")
	go s.run(ctx)
	go s.pruneLoop(ctx)
}

// EnqueueImport schedules a completed download for filling ahead of backfill.
// It never blocks the caller.
func (s *Service) EnqueueImport(entry *storage.Entry) {
	if s == nil || entry == nil {
		return
	}
	select {
	case s.imports <- entry:
	default:
		s.logger.Debug().Str("entry", entry.Name).Msg("Edge cache import queue full; backfill will cover it")
	}
}

func (s *Service) run(ctx context.Context) {
	backfillPending := s.backfill
	backfillAt := time.Now().Add(backfillStartDelay)
	var queue []*storage.Entry
	var listed bool
	for {
		// Imports always go ahead of backfill.
		select {
		case <-ctx.Done():
			return
		case e := <-s.imports:
			s.fillEntry(ctx, e)
			continue
		default:
		}

		if backfillPending && !time.Now().Before(backfillAt) {
			if !listed {
				queue = s.collectBackfill()
				listed = true
				s.logger.Info().Int("entries", len(queue)).Msg("Edge cache backfill starting")
			}
			if len(queue) > 0 {
				s.fillEntry(ctx, queue[0])
				queue = queue[1:]
				continue
			}
			backfillPending = false
			s.logger.Info().
				Int64("filled", s.filled.Load()).
				Int64("failed", s.failed.Load()).
				Int64("already_present", s.skipped.Load()).
				Msg("Edge cache backfill finished")
		}

		var wake <-chan time.Time
		if backfillPending {
			wake = time.After(time.Until(backfillAt))
		}
		select {
		case <-ctx.Done():
			return
		case e := <-s.imports:
			s.fillEntry(ctx, e)
		case <-wake:
		}
	}
}

func (s *Service) collectBackfill() []*storage.Entry {
	var out []*storage.Entry
	err := s.source.ForEachEntry(func(e *storage.Entry) error {
		if e != nil && e.Protocol != config.ProtocolNZB && len(e.Files) > 0 {
			out = append(out, e)
		}
		return nil
	})
	if err != nil {
		s.logger.Warn().Err(err).Msg("Edge cache backfill: listing entries failed")
	}
	return out
}

func fillable(f *storage.File) bool {
	return f != nil && !f.Deleted && f.Size > 0 && utils.IsMediaFile(f.Name)
}

func (s *Service) fillEntry(ctx context.Context, entry *storage.Entry) {
	for name, f := range entry.Files {
		if !fillable(f) {
			continue
		}
		key := Key(entry.InfoHash, name, f.Size)
		if s.store.Complete(key, f.Size) {
			s.skipped.Add(1)
			continue
		}
		if !s.waitIdle(ctx) {
			return
		}
		if err := s.fillFile(ctx, entry, name, f.Size, key); err != nil {
			s.failed.Add(1)
			s.logger.Debug().Err(err).Str("entry", entry.Name).Str("file", name).Msg("Edge cache fill failed")
		} else {
			s.filled.Add(1)
			if n := s.filled.Load(); n%100 == 0 {
				files, bytes := s.store.Usage()
				s.logger.Info().Int64("filled", n).Int64("failed", s.failed.Load()).
					Int("stored_parts", files).Int64("stored_bytes", bytes).Msg("Edge cache progress")
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(s.delay):
		}
	}
}

// waitIdle blocks while user streams are open. It returns false when ctx ends.
func (s *Service) waitIdle(ctx context.Context) bool {
	for s.source.ActiveStreams() > 0 {
		select {
		case <-ctx.Done():
			return false
		case <-time.After(busyPoll):
		}
	}
	return ctx.Err() == nil
}

func (s *Service) fillFile(ctx context.Context, entry *storage.Entry, name string, size int64, key string) error {
	if _, loaded := s.inflight.LoadOrStore(key, struct{}{}); loaded {
		return nil
	}
	defer s.inflight.Delete(key)

	ctx, cancel := context.WithTimeout(request.WithClass(ctx, request.ClassBackground), fileFetchTimeout)
	defer cancel()

	head, tail := s.store.Coverage(key, size)
	if want := s.store.HeadLen(size); head != want {
		data, err := s.fetch(ctx, entry, name, 0, want)
		if err != nil {
			return fmt.Errorf("head: %w", err)
		}
		if err := s.store.PutHead(key, size, data); err != nil {
			return err
		}
	}
	if want := s.store.TailLen(size); tail != want {
		data, err := s.fetch(ctx, entry, name, size-want, want)
		if err != nil {
			return fmt.Errorf("tail: %w", err)
		}
		if err := s.store.PutTail(key, size, data); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) fetch(ctx context.Context, entry *storage.Entry, name string, offset, length int64) ([]byte, error) {
	r, err := s.source.OpenForCache(ctx, entry, name, offset)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	buf := make([]byte, length)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}
	return buf, nil
}

// Capture stores bytes that a playback read already fetched, when they cover a
// whole missing head or tail. It is called from the read path and never blocks.
func (s *Service) Capture(infohash, name string, size int64, offset int64, data []byte, tail bool) {
	if s == nil {
		return
	}
	key := Key(infohash, name, size)
	if _, loaded := s.inflight.LoadOrStore(key, struct{}{}); loaded {
		return
	}
	go func() {
		defer s.inflight.Delete(key)
		var err error
		if tail {
			err = s.store.PutTail(key, size, data)
		} else {
			err = s.store.PutHead(key, size, data)
		}
		if err != nil {
			s.logger.Debug().Err(err).Str("file", name).Msg("Edge cache capture failed")
		}
	}()
}

func (s *Service) pruneLoop(ctx context.Context) {
	timer := time.NewTimer(backfillStartDelay + time.Hour)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
		s.prune()
		timer.Reset(pruneInterval)
	}
}

func (s *Service) prune() {
	keep := make(map[string]struct{})
	err := s.source.ForEachEntry(func(e *storage.Entry) error {
		for name, f := range e.Files {
			if fillable(f) {
				keep[Key(e.InfoHash, name, f.Size)] = struct{}{}
			}
		}
		return nil
	})
	if err != nil || len(keep) == 0 {
		// Never prune against an empty or failed listing: it would wipe the store.
		s.logger.Warn().Err(err).Int("keys", len(keep)).Msg("Edge cache prune skipped")
		return
	}
	removed, freed, err := s.store.Prune(keep)
	if err != nil {
		s.logger.Warn().Err(err).Msg("Edge cache prune walk failed")
	}
	if removed > 0 {
		s.logger.Info().Int("removed", removed).Int64("freed_bytes", freed).Msg("Edge cache pruned files no longer in the library")
	}
}
