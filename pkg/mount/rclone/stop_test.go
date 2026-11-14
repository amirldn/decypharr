//go:build !windows

package rclone

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/rclone"
)

// fakeRclone puts an "rclone" on PATH that idles until SIGINT, then records
// the signal in the returned marker file and exits.
func fakeRclone(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	marker := filepath.Join(dir, "interrupted")
	script := "#!/bin/sh\ntrap 'touch \"" + marker + "\"; exit 0' INT\nwhile :; do sleep 0.1; done\n"
	if err := os.WriteFile(filepath.Join(dir, "rclone"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	return marker
}

func newStopTestManager() *Manager {
	ctx, cancel := context.WithCancel(context.Background())
	return &Manager{
		logger:      zerolog.Nop(),
		ctx:         ctx,
		cancel:      cancel,
		client:      rclone.NewClient("http://127.0.0.1:1", "", "", zerolog.Nop()),
		serverReady: make(chan struct{}),
	}
}

// The caller's context is the application's signal context. Binding rclone
// to it killed rclone on SIGTERM before Stop could unmount, leaving the FUSE
// mount behind on every shutdown.
func TestRcloneOutlivesCallerContextUntilStop(t *testing.T) {
	config.SetConfigPath(t.TempDir())
	t.Cleanup(config.Reset)
	marker := fakeRclone(t)

	m := newStopTestManager()
	ctx, cancel := context.WithCancel(context.Background())
	if err := m.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() { _ = m.Stop() })

	cancel() // SIGTERM reached the application
	select {
	case <-m.exited:
		t.Fatal("rclone exited when the caller's context was cancelled, before Stop")
	case <-time.After(500 * time.Millisecond):
	}

	if err := m.Stop(); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
	select {
	case <-m.exited:
	default:
		t.Fatal("Stop() returned before rclone was reaped")
	}
	if _, err := os.Stat(marker); err != nil {
		t.Fatal("rclone was not asked to exit with SIGINT")
	}
}
