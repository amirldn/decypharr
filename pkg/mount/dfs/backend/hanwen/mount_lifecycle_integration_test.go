//go:build linux

package hanwen

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

// Run only in an isolated mount namespace with /dev/fuse and mount capability:
// DECYPHARR_FUSE_TEST=1 go test ./pkg/mount/dfs/backend/hanwen -run TestFUSE -v
func TestFUSEDelayedCleanupPreservesReplacement(t *testing.T) {
	if os.Getenv("DECYPHARR_FUSE_TEST") != "1" {
		t.Skip("requires explicitly enabled isolated Linux FUSE staging")
	}
	root := t.TempDir()
	mountPath := filepath.Join(root, "mount")
	for _, path := range []string{mountPath} {
		if err := os.Mkdir(path, 0700); err != nil {
			t.Fatal(err)
		}
	}
	sentinel := []byte("isolated decypharr lifecycle canary\n")
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	mount := func() *fuse.Server {
		t.Helper()
		node := &fs.Inode{}
		opts := &fs.Options{MountOptions: fuse.MountOptions{DirectMountStrict: true}, OnAdd: func(ctx context.Context) {
			child := node.NewPersistentInode(ctx, &fs.MemRegularFile{Data: sentinel, Attr: fuse.Attr{Mode: 0444}}, fs.StableAttr{Mode: syscall.S_IFREG})
			node.AddChild("canary.txt", child, false)
		}}
		server, err := fs.Mount(mountPath, node, opts)
		if err != nil {
			t.Fatal(err)
		}
		// Per-server cleanup runs LIFO, so a replacement unmounts before an old
		// server whose already successful Unmount is a no-op.
		t.Cleanup(func() { _ = server.Unmount() })
		return server
	}
	readCanary := func() {
		t.Helper()
		got, err := os.ReadFile(filepath.Join(mountPath, "canary.txt"))
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != string(sentinel) {
			t.Fatalf("unexpected canary: %q", got)
		}
	}
	lease, err := acquireMount(mountPath)
	if err != nil {
		t.Fatal(err)
	}
	server := mount()
	readCanary()
	entered, release := make(chan struct{}), make(chan struct{})
	lease.startCleanup(func() error {
		close(entered)
		<-release
		return server.Unmount()
	})
	<-entered
	timedOut, timedOutCancel := context.WithCancel(ctx)
	timedOutCancel()
	if err := lease.wait(timedOut); !errors.Is(err, context.Canceled) {
		t.Errorf("wait = %v", err)
	}
	_, blockedErr := acquireMount(mountPath)
	close(release)
	if err := lease.wait(ctx); err != nil {
		t.Fatal(err)
	}
	if blockedErr == nil {
		t.Fatal("replacement accepted while delayed real unmount was pending")
	}
	// Exercise several new server generations on the same pathname. The stale
	// lease must remain inert after each replacement becomes readable.
	for i := 0; i < 5; i++ {
		replacement, err := acquireMount(mountPath)
		if err != nil {
			t.Fatal(err)
		}
		current := mount()
		if err := lease.wait(ctx); err != nil {
			t.Fatal(err)
		}
		readCanary()
		replacement.startCleanup(current.Unmount)
		if err := replacement.wait(ctx); err != nil {
			t.Fatal(err)
		}
	}
}
