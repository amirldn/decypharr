package config

import "testing"

func TestMaxCacheWarmWorkersDefault(t *testing.T) {
	SetConfigPath(t.TempDir())
	c := Config{}
	c.setDefaults()
	if c.MaxCacheWarmWorkers != DefaultCacheWarmWorkers {
		t.Fatalf("MaxCacheWarmWorkers = %d, want default %d", c.MaxCacheWarmWorkers, DefaultCacheWarmWorkers)
	}
}

func TestMaxCacheWarmWorkersIsRuntimeApplicable(t *testing.T) {
	t.Parallel()
	current := Config{MaxCacheWarmWorkers: 2}
	next := Config{MaxCacheWarmWorkers: 1}
	if current.RequiresRestart(&next) {
		t.Fatal("changing only max_cache_warm_workers should not require a service restart")
	}
}
