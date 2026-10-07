package config

import "time"

const (
	DefaultNFSPort uint16 = 20490
	DefaultSMBPort uint16 = 1445
)

var (
	DefaultPort     = "8282"
	DefaultLogLevel = "info"

	DefaultRateLimit                = "250/minute"
	DefaultTorrentsRefreshInterval  = "10m"
	DefaultDownloadsRefreshInterval = "5m"
	DefaultAutoExpireLinksAfter     = "3d"

	DefaultRclonePort = "5572"

	DefaultDFSChunkSize     = "8MB"
	DefaultDFSReadAheadSize = "128MB"
	DefaultDFSCacheExpiry   = "24h"
	DefaultDFSDiskCacheSize = "500MB"

	DefaultAccountSyncInterval = "10m"
	DefaultAvailableSlots      = 100 // This is for providers that does not provide available slots info

	// DefaultCacheWarmWorkers caps concurrent head+tail mount reads after a
	// download finishes. 10 (the previous hardcoded pool size) opened a
	// TorBox 429 circuit when two season packs finished together.
	DefaultCacheWarmWorkers = 2

	DefaultRetryDelay    = 500 * time.Millisecond
	DefaultRetryDelayMax = 30 * time.Second
)
