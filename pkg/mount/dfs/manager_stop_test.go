package dfs

import (
	"context"
	"errors"
	"testing"

	"github.com/sirrobot01/decypharr/pkg/mount/dfs/backend"
)

type failingUnmountBackend struct {
	backend.Backend
	err error
}

func (b *failingUnmountBackend) Type() backend.Type            { return backend.Hanwen }
func (b *failingUnmountBackend) Unmount(context.Context) error { return b.err }

func TestStopReturnsBackendErrorAndClearsReady(t *testing.T) {
	failure := errors.New("cleanup pending")
	m := &Manager{backend: &failingUnmountBackend{err: failure}}
	m.ready.Store(true)
	if err := m.Stop(); !errors.Is(err, failure) {
		t.Fatalf("Stop error=%v", err)
	}
	if m.IsReady() {
		t.Fatal("manager still reports ready")
	}
}
