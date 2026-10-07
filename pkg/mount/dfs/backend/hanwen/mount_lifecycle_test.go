package hanwen

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

// The old backend let its caller return on timeout while pathname-based
// unmounting continued in another goroutine. A new backend could then mount at
// that path and have its filesystem removed by the old cleanup.
func TestDelayedCleanupCannotUnmountReplacement(t *testing.T) {
	path := t.TempDir()
	old, err := acquireMount(path)
	if err != nil {
		t.Fatal(err)
	}
	var mounted atomic.Int32
	mounted.Store(1)
	entered := make(chan struct{})
	release := make(chan struct{})
	old.startCleanup(func() error {
		close(entered)
		<-release        // model VFS.Close or an unmount syscall stuck past caller timeout
		mounted.Store(0) // pathname unmount removes whichever generation owns path
		return nil
	})
	<-entered
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := old.wait(ctx); !errors.Is(err, context.Canceled) {
		t.Errorf("cleanup wait = %v; want context cancellation", err)
	}
	replacement, acquireErr := acquireMount(path)
	if acquireErr == nil {
		mounted.Store(2)
	}
	close(release)
	doneCtx, doneCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer doneCancel()
	if err := old.wait(doneCtx); err != nil {
		t.Fatal(err)
	}
	if acquireErr == nil {
		replacement.startCleanup(func() error { return nil })
		_ = replacement.wait(doneCtx)
		t.Fatalf("replacement allowed before old cleanup completed; surviving generation = %d", mounted.Load())
	}
	replacement, err = acquireMount(path)
	if err != nil {
		t.Fatalf("path not reusable after completed cleanup: %v", err)
	}
	mounted.Store(2)
	// Waiting on old cleanup again must not repeat its pathname unmount.
	if err := old.wait(doneCtx); err != nil {
		t.Fatal(err)
	}
	if mounted.Load() != 2 {
		t.Fatal("old cleanup removed replacement mount")
	}
	replacement.startCleanup(func() error { return nil })
	if err := replacement.wait(doneCtx); err != nil {
		t.Fatal(err)
	}
}

func TestCleanupFailureRetainsPathOwnership(t *testing.T) {
	path := t.TempDir()
	lease, err := acquireMount(path)
	if err != nil {
		t.Fatal(err)
	}
	failure := errors.New("unmount failed")
	lease.startCleanup(func() error { return failure })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := lease.wait(ctx); !errors.Is(err, failure) {
		t.Fatalf("wait = %v; want cleanup error", err)
	}
	if _, err := acquireMount(path); err == nil {
		t.Fatal("failed teardown released path for unsafe replacement")
	}
}

func TestCleanupStartsOnlyOnce(t *testing.T) {
	lease, err := acquireMount(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	release := make(chan struct{})
	cleanup := func() error { calls.Add(1); <-release; return nil }
	lease.startCleanup(cleanup)
	lease.startCleanup(cleanup)
	close(release)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := lease.wait(ctx); err != nil {
		t.Fatal(err)
	}
	lease.startCleanup(cleanup)
	if err := lease.wait(ctx); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 {
		t.Fatalf("cleanup ran %d times", calls.Load())
	}
}

func TestMountOwnershipNormalizesExistingSymlink(t *testing.T) {
	root := t.TempDir()
	realPath := filepath.Join(root, "real")
	if err := os.Mkdir(realPath, 0700); err != nil {
		t.Fatal(err)
	}
	alias := filepath.Join(root, "alias")
	if err := os.Symlink(realPath, alias); err != nil {
		t.Fatal(err)
	}
	lease, err := acquireMount(realPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := acquireMount(alias); err == nil {
		t.Error("symlink alias bypassed active mount ownership")
	}
	lease.startCleanup(func() error { return nil })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := lease.wait(ctx); err != nil {
		t.Fatal(err)
	}
}
