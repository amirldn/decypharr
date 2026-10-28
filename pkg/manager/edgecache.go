package manager

import (
	"context"
	"io"

	"github.com/sirrobot01/decypharr/pkg/edgecache"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

// edgeSource adapts the manager to edgecache.Source.
type edgeSource struct{ m *Manager }

func (s edgeSource) OpenForCache(ctx context.Context, entry *storage.Entry, filename string, offset int64) (io.ReadCloser, error) {
	return s.m.OpenStreamUntrackedForCache(ctx, entry, filename, offset)
}

func (s edgeSource) ActiveStreams() int { return s.m.GetActiveStreamsCount() }

func (s edgeSource) ForEachEntry(fn func(*storage.Entry) error) error {
	return s.m.storage.ForEach(fn)
}

func (m *Manager) initEdgeCache() {
	svc, err := edgecache.New(m.config.EdgeCache, edgeSource{m: m}, m.logger.With().Str("component", "edgecache").Logger())
	if err != nil {
		m.logger.Error().Err(err).Msg("Edge cache disabled: invalid configuration")
		return
	}
	m.edgeCache = svc
}

// EdgeCache returns the head/tail store service, or nil when disabled.
func (m *Manager) EdgeCache() *edgecache.Service { return m.edgeCache }
