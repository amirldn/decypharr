//go:build linux || (darwin && amd64)

package hanwen

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/sirrobot01/decypharr/pkg/mount/dfs/config"
	"github.com/sirrobot01/decypharr/pkg/mount/dfs/vfs"
)

type delayedServer struct {
	unmount func() error
	wait    func() error
}

func (s *delayedServer) Unmount() error { return s.unmount() }
func (s *delayedServer) WaitMount() error {
	if s.wait != nil {
		return s.wait()
	}
	return nil
}

func lifecycleBackend(path string, closeVFS func() error) *Backend {
	return &Backend{config: &config.FuseConfig{MountPath: path, DaemonTimeout: time.Second}, root: &Dir{}, vfs: &vfs.Manager{}, closeVFSFn: closeVFS}
}

func TestBackendLateMountRetainsPathUntilUnmountCompletes(t *testing.T) {
	path := t.TempDir()
	entered, releaseMount, cleanupEntered, releaseCleanup := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
	var closed atomic.Int32
	b := lifecycleBackend(path, func() error { closed.Add(1); return nil })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() {
		result <- b.mount(ctx, func(string, fs.InodeEmbedder, *fs.Options) (mountedServer, error) {
			close(entered)
			<-releaseMount
			return &delayedServer{unmount: func() error { close(cleanupEntered); <-releaseCleanup; return nil }}, nil
		})
	}()
	<-entered
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("mount error = %v", err)
	}
	if b.IsReady() {
		t.Error("timed-out backend is ready")
	}
	newBackend := lifecycleBackend(path, func() error { return nil })
	if err := newBackend.mount(context.Background(), func(string, fs.InodeEmbedder, *fs.Options) (mountedServer, error) {
		t.Error("replacement reached FUSE")
		return nil, errors.New("unexpected mount")
	}); err == nil {
		t.Error("replacement accepted before late mount returned")
	}
	close(releaseMount)
	<-cleanupEntered
	if _, err := acquireMount(path); err == nil {
		t.Error("path released before real unmount completed")
	}
	close(releaseCleanup)
	waitCtx, waitCancel := context.WithTimeout(context.Background(), time.Second)
	defer waitCancel()
	if err := b.Unmount(waitCtx); err != nil {
		t.Fatal(err)
	}
	if closed.Load() != 1 {
		t.Fatalf("VFS closed %d times", closed.Load())
	}
	replacement, err := acquireMount(path)
	if err != nil {
		t.Fatal(err)
	}
	replacement.startCleanup(func() error { return nil })
	if err := replacement.wait(waitCtx); err != nil {
		t.Fatal(err)
	}
}

func TestBackendUnmountTimeoutDoesNotReleasePath(t *testing.T) {
	path := t.TempDir()
	entered, release := make(chan struct{}), make(chan struct{})
	b := lifecycleBackend(path, func() error { close(entered); <-release; return nil })
	if err := b.mount(context.Background(), func(string, fs.InodeEmbedder, *fs.Options) (mountedServer, error) {
		return &delayedServer{unmount: func() error { return nil }}, nil
	}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := b.Unmount(ctx); !errors.Is(err, context.Canceled) {
		t.Errorf("unmount=%v", err)
	}
	<-entered
	if b.IsReady() {
		t.Error("unmounting backend is ready")
	}
	if _, err := acquireMount(path); err == nil {
		t.Error("path released while VFS close blocked")
	}
	close(release)
	if err := b.Unmount(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestBackendMountRefusalClosesVFS(t *testing.T) {
	path := t.TempDir()
	owner, err := acquireMount(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { owner.startCleanup(func() error { return nil }); _ = owner.wait(context.Background()) }()
	var closed atomic.Int32
	b := lifecycleBackend(path, func() error { closed.Add(1); return nil })
	if err := b.mount(context.Background(), func(string, fs.InodeEmbedder, *fs.Options) (mountedServer, error) {
		t.Error("mount called on owned path")
		return nil, nil
	}); err == nil {
		t.Error("owned path accepted")
	}
	if closed.Load() != 1 {
		t.Fatalf("VFS close count=%d", closed.Load())
	}
	_ = b.Unmount(context.Background())
	if closed.Load() != 1 {
		t.Errorf("VFS double close: %d", closed.Load())
	}
}
