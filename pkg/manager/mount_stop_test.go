package manager

import (
	"errors"
	"testing"
)

type failingStopMount struct {
	stubMountManager
	err error
}

func (m *failingStopMount) Stop() error { return m.err }

func TestResetAbortsOnMountStopFailure(t *testing.T) {
	failure := errors.New("mount cleanup pending")
	m := &Manager{mountManager: &failingStopMount{err: failure}}
	if err := m.Stop(); !errors.Is(err, failure) {
		t.Fatalf("Stop error=%v", err)
	}
	// Reset must return before storage is reopened or any manager is recreated.
	if err := m.Reset(); !errors.Is(err, failure) {
		t.Fatalf("Reset error=%v", err)
	}
}
