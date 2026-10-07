package torbox

import (
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	DefaultNegativeCacheTTL = 30 * time.Minute
	DefaultNegativeCacheMax = 10000
	maxNegativeCacheTTL     = 48 * time.Hour
)

// NegativeVerdict represents a cached deterministic rejection for an infohash.
type NegativeVerdict struct {
	InfoHash  string    `json:"info_hash"`
	Reason    string    `json:"reason"`
	CachedAt  time.Time `json:"cached_at"`
	ExpiresAt time.Time `json:"expires_at"`
	Attempts  int       `json:"attempts"`
}

// NegativeCache manages deterministic rejections in memory with bounded capacity
// and expiration tracking, eliminating unthrottled retry loops (Issue #320 / #329).
type NegativeCache struct {
	mu         sync.RWMutex
	entries    map[string]*NegativeVerdict
	defaultTTL time.Duration
	maxEntries int
	hits       int64
	misses     int64
	evictions  int64
}

// NewNegativeCache constructs a NegativeCache with the specified default TTL and capacity.
func NewNegativeCache(defaultTTL time.Duration, maxEntries int) *NegativeCache {
	if defaultTTL <= 0 {
		defaultTTL = DefaultNegativeCacheTTL
	}
	if maxEntries <= 0 {
		maxEntries = DefaultNegativeCacheMax
	}
	return &NegativeCache{
		entries:    make(map[string]*NegativeVerdict),
		defaultTTL: defaultTTL,
		maxEntries: maxEntries,
	}
}

func (nc *NegativeCache) normalizeHash(hash string) string {
	return strings.ToLower(strings.TrimSpace(hash))
}

// Get checks if an active negative verdict exists for the given infohash.
func (nc *NegativeCache) Get(infohash string) (*NegativeVerdict, bool) {
	key := nc.normalizeHash(infohash)
	if key == "" {
		atomic.AddInt64(&nc.misses, 1)
		return nil, false
	}

	now := time.Now()

	nc.mu.RLock()
	verdict, exists := nc.entries[key]
	if !exists {
		nc.mu.RUnlock()
		atomic.AddInt64(&nc.misses, 1)
		return nil, false
	}
	if now.After(verdict.ExpiresAt) {
		nc.mu.RUnlock()
		// Lazy expiry cleanup
		nc.mu.Lock()
		if v, ok := nc.entries[key]; ok && now.After(v.ExpiresAt) {
			delete(nc.entries, key)
			atomic.AddInt64(&nc.evictions, 1)
		}
		nc.mu.Unlock()
		atomic.AddInt64(&nc.misses, 1)
		return nil, false
	}
	nc.mu.RUnlock()

	atomic.AddInt64(&nc.hits, 1)
	return verdict, true
}

// Put records a deterministic rejection verdict for the infohash.
func (nc *NegativeCache) Put(infohash string, reason string, ttl time.Duration) *NegativeVerdict {
	key := nc.normalizeHash(infohash)
	if key == "" {
		return nil
	}

	if ttl <= 0 {
		ttl = nc.defaultTTL
	}
	if ttl > maxNegativeCacheTTL {
		ttl = maxNegativeCacheTTL
	}

	now := time.Now()
	expiresAt := now.Add(ttl)

	nc.mu.Lock()
	defer nc.mu.Unlock()

	existing, exists := nc.entries[key]
	attempts := 1
	if exists {
		attempts = existing.Attempts + 1
	} else if len(nc.entries) >= nc.maxEntries {
		// Evict expired entry or oldest entry if capacity reached
		var oldestKey string
		var oldestTime time.Time
		for k, v := range nc.entries {
			if now.After(v.ExpiresAt) {
				oldestKey = k
				break
			}
			if oldestKey == "" || v.CachedAt.Before(oldestTime) {
				oldestKey = k
				oldestTime = v.CachedAt
			}
		}
		if oldestKey != "" {
			delete(nc.entries, oldestKey)
			atomic.AddInt64(&nc.evictions, 1)
		}
	}

	verdict := &NegativeVerdict{
		InfoHash:  key,
		Reason:    reason,
		CachedAt:  now,
		ExpiresAt: expiresAt,
		Attempts:  attempts,
	}
	nc.entries[key] = verdict
	return verdict
}

// Evict removes an infohash from the negative cache (e.g. when successfully cached/created).
func (nc *NegativeCache) Evict(infohash string) bool {
	key := nc.normalizeHash(infohash)
	if key == "" {
		return false
	}

	nc.mu.Lock()
	defer nc.mu.Unlock()

	_, exists := nc.entries[key]
	if exists {
		delete(nc.entries, key)
		atomic.AddInt64(&nc.evictions, 1)
		return true
	}
	return false
}

// Len returns the current number of cached entries.
func (nc *NegativeCache) Len() int {
	nc.mu.RLock()
	defer nc.mu.RUnlock()
	return len(nc.entries)
}

// Stats returns hit, miss, eviction counters and current size.
func (nc *NegativeCache) Stats() (hits, misses, evictions int64, size int) {
	nc.mu.RLock()
	size = len(nc.entries)
	nc.mu.RUnlock()
	return atomic.LoadInt64(&nc.hits), atomic.LoadInt64(&nc.misses), atomic.LoadInt64(&nc.evictions), size
}

// Clear removes all entries from the negative cache.
func (nc *NegativeCache) Clear() {
	nc.mu.Lock()
	defer nc.mu.Unlock()
	nc.entries = make(map[string]*NegativeVerdict)
}
