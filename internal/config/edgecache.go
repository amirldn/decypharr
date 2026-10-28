package config

import "fmt"

// EdgeCache configures the persistent head/tail store. It keeps the first
// HeadSize and last TailSize bytes of every media file on local disk so player
// and transcoder start-up probes (container header, end-of-file index) never
// wait on a cold debrid fetch, however long ago the title was last played.
// A size-capped LRU cache in front of the mount (rclone VFS, DFS) cannot do
// this: it evicts whole files by last access, so unplayed titles go cold.
type EdgeCache struct {
	Enabled  bool   `json:"enabled,omitzero"`
	Dir      string `json:"dir,omitempty"`
	HeadSize string `json:"head_size,omitempty"`
	TailSize string `json:"tail_size,omitempty"`
	// Backfill fills existing library files in the background. New downloads
	// are filled when they complete regardless of this setting.
	Backfill bool `json:"backfill,omitzero"`
	// BackfillDelay is the pause between files during backfill and import
	// fills, so filling never competes with playback for bandwidth.
	BackfillDelay string `json:"backfill_delay,omitempty"`
}

const (
	DefaultEdgeCacheHeadSize      = "16MB"
	DefaultEdgeCacheTailSize      = "4MB"
	DefaultEdgeCacheBackfillDelay = "5s"
)

// EdgeCacheSizes returns the parsed head and tail sizes, applying defaults.
func (e EdgeCache) EdgeCacheSizes() (head, tail int64, err error) {
	headStr, tailStr := e.HeadSize, e.TailSize
	if headStr == "" {
		headStr = DefaultEdgeCacheHeadSize
	}
	if tailStr == "" {
		tailStr = DefaultEdgeCacheTailSize
	}
	if head, err = ParseSize(headStr); err != nil {
		return 0, 0, fmt.Errorf("edge_cache.head_size: %w", err)
	}
	if tail, err = ParseSize(tailStr); err != nil {
		return 0, 0, fmt.Errorf("edge_cache.tail_size: %w", err)
	}
	if head < 0 || tail < 0 {
		return 0, 0, fmt.Errorf("edge_cache sizes must not be negative")
	}
	return head, tail, nil
}
