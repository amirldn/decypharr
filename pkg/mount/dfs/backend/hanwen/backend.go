//go:build linux || (darwin && amd64)

package hanwen

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/logger"
	"github.com/sirrobot01/decypharr/pkg/mount/dfs/backend"
	"github.com/sirrobot01/decypharr/pkg/mount/dfs/config"
	"github.com/sirrobot01/decypharr/pkg/mount/dfs/vfs"
)

const (
	// ReadTimeout is the maximum time a single read operation can take
	// Increased to 120s to handle slow debrid/CDN connections
	ReadTimeout  = 120 * time.Second
	AttrTimeout  = 30 * time.Second
	EntryTimeout = 1 * time.Second
)

func init() {
	backend.Register(backend.Hanwen, NewBackend)
}

type mountedServer interface {
	Unmount() error
	WaitMount() error
}

type mountFilesystem func(string, fs.InodeEmbedder, *fs.Options) (mountedServer, error)

// Backend implements the hanwen/go-fuse backend.
type Backend struct {
	config      *config.FuseConfig
	logger      zerolog.Logger
	server      mountedServer
	ready       atomic.Bool
	unmountFunc func(ctx context.Context) error
	lifecycleMu sync.Mutex
	closeOnce   sync.Once
	closeErr    error
	closeVFSFn  func() error
	lease       *mountLease
	root        *Dir
	vfs         *vfs.Manager
}

// NewBackend creates a new hanwen backend
func NewBackend(vfs *vfs.Manager, config *config.FuseConfig) (backend.Backend, error) {
	now := time.Now()
	log := logger.New("hanwen-backend")
	// One shared rate-limited logger for the whole mount. Files/Dirs reference
	// it instead of allocating their own xsync map per inode — dedup keys are
	// already unique per inode so a shared map gives identical behaviour.
	rl := logger.NewRateLimitedLogger(logger.WithLogger(log))
	root := NewDir(vfs, "", LevelRoot, uint64(now.Unix()), config, log, rl)
	return &Backend{
		config:     config,
		logger:     log,
		root:       root,
		vfs:        vfs,
		closeVFSFn: vfs.Close,
	}, nil
}

// Mount mounts the filesystem using hanwen/go-fuse
func (b *Backend) Mount(ctx context.Context) error {
	return b.mount(ctx, mountFuse)
}

func mountFuse(path string, root fs.InodeEmbedder, opts *fs.Options) (mountedServer, error) {
	server, err := fs.Mount(path, root, opts)
	if err != nil {
		return nil, err
	}
	return server, nil
}

func (b *Backend) closeVFS() error {
	b.closeOnce.Do(func() {
		if b.closeVFSFn != nil {
			b.closeErr = b.closeVFSFn()
		} else if b.vfs != nil {
			b.closeErr = b.vfs.Close()
		}
	})
	return b.closeErr
}

func (b *Backend) mount(ctx context.Context, mountFS mountFilesystem) (retErr error) {
	b.lifecycleMu.Lock()
	defer b.lifecycleMu.Unlock()
	defer func() {
		if retErr != nil && b.unmountFunc == nil {
			retErr = errors.Join(retErr, b.closeVFS())
		}
	}()
	if b.lease != nil {
		return fmt.Errorf("backend already used; create a new backend for a new mount")
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	if b.root == nil {
		return fmt.Errorf("root node is not initialized")
	}
	if b.vfs == nil {
		return fmt.Errorf("VFS manager is not initialized")
	}

	// Inspect the mount table before stat/mkdir/alias resolution can touch
	// an existing, possibly stalled FUSE mount.
	absolutePath, err := filepath.Abs(b.config.MountPath)
	if err != nil {
		return err
	}
	if err := checkMountpoint(absolutePath); err != nil {
		return err
	}
	if err := os.MkdirAll(b.config.MountPath, 0755); err != nil {
		return err
	}
	lease, err := acquireMount(b.config.MountPath)
	if err != nil {
		return err
	}
	b.lease = lease
	// Use the canonical name for both the ownership key and all FUSE calls.
	b.config.MountPath = lease.path
	if err := checkMountpoint(lease.path); err != nil {
		lease.startCleanup(func() error { return nil })
		_ = lease.wait(context.Background())
		return err
	}

	mountOpt := fuse.MountOptions{
		FsName:               "decypharr",
		Debug:                false,
		Name:                 "decypharr",
		DisableXAttrs:        true,
		IgnoreSecurityLabels: true,
		MaxWrite:             1024 * 1024,
		// The kernel defaults MaxBackground to 12, which caps in-flight
		// readahead far below the VFS readahead window.
		MaxBackground: b.config.FuseMaxBackground,
		MaxReadAhead:  b.config.FuseMaxReadAhead,
		AllowOther:    true,
		// Route handler panics through our logger; go-fuse fails the single
		// request with EIO instead of the panic unwinding into its serve loop.
		PanicHandler: func(p any) fuse.Status {
			b.logger.Error().Any("panic", p).Bytes("stack", debug.Stack()).Msg("FUSE handler panic")
			return fuse.EIO
		},
	}

	var opt []string

	opt = append(opt, "default_permissions")

	if runtime.GOOS == "darwin" {
		opt = append(opt, "volname=decypharr")
		opt = append(opt, "noapplexattr")
		opt = append(opt, "noappledouble")
	}

	mountOpt.Options = opt

	// Configure FUSE options
	// Use short entry timeout (1s) to ensure new files appear quickly
	entryTimeout := EntryTimeout
	attrTimeout := AttrTimeout
	opts := &fs.Options{
		AttrTimeout:  &attrTimeout,
		EntryTimeout: &entryTimeout,
		MountOptions: mountOpt,
		UID:          b.config.UID,
		GID:          b.config.GID,
	}

	// Start timer before creating NodeFS - adjust timeout duration as needed
	mountCtx, cancel := context.WithTimeout(ctx, b.config.DaemonTimeout)
	defer cancel()

	// Channel to receive the result of fs.Mount
	type fsResult struct {
		server mountedServer
		err    error
	}
	fsResultChan := make(chan fsResult, 1)

	// The mount operation owns the path even if its caller times out. A
	// late result is cleaned up before any replacement can acquire the path.
	go func() {
		server, err := mountFS(lease.path, b.root, opts)
		fsResultChan <- fsResult{server: server, err: err}
	}()
	cleanup := func(server mountedServer) error {
		closeErr := b.closeVFS()
		if server != nil {
			return errors.Join(closeErr, server.Unmount())
		}
		return closeErr
	}
	b.unmountFunc = func(ctx context.Context) error {
		lease.startCleanup(func() error { return cleanup(b.server) })
		return lease.wait(ctx)
	}
	var server mountedServer
	select {
	case result := <-fsResultChan:
		server = result.server
		b.server = server
		if result.err != nil {
			lease.startCleanup(func() error { return cleanup(server) })
			return errors.Join(fmt.Errorf("failed to create mount: %w", result.err), lease.wait(mountCtx))
		}
	case <-mountCtx.Done():
		lease.startCleanup(func() error {
			result := <-fsResultChan
			return cleanup(result.server)
		})
		return fmt.Errorf("timeout creating mount: %w", mountCtx.Err())
	}
	b.logger.Info().Str("mount_path", lease.path).Msg("Waiting for mount to be ready")
	waitChan := make(chan error, 1)
	go func() { waitChan <- server.WaitMount() }()
	select {
	case err := <-waitChan:
		if err != nil {
			lease.startCleanup(func() error { return cleanup(server) })
			return errors.Join(fmt.Errorf("failed to wait for mount: %w", err), lease.wait(mountCtx))
		}
	case <-mountCtx.Done():
		lease.startCleanup(func() error { return cleanup(server) })
		return fmt.Errorf("timeout waiting for mount to be ready: %w", mountCtx.Err())
	}
	b.ready.Store(true)
	return nil
}

// Unmount unmounts the filesystem
func (b *Backend) Unmount(ctx context.Context) error {
	b.lifecycleMu.Lock()
	defer b.lifecycleMu.Unlock()
	b.ready.Store(false)
	b.logger.Info().Msg("Unmounting hanwen backend")
	if b.unmountFunc != nil {
		return b.unmountFunc(ctx)
	}
	// No server was started (for example the path was already mounted).
	return b.closeVFS()
}

// WaitReady waits for the mount to be ready
func (b *Backend) WaitReady(ctx context.Context) error {
	if b.server == nil {
		return fmt.Errorf("server not initialized")
	}
	return b.server.WaitMount()
}

// IsReady returns true if the mount is ready
func (b *Backend) IsReady() bool {
	return b.ready.Load()
}

// Type returns the backend type
func (b *Backend) Type() backend.Type {
	return backend.Hanwen
}

func (b *Backend) Refresh(dir string) {
	// Refresh the root dir first
	if b.root != nil {
		b.root.Refresh()
		if dir != "" {
			b.root.RefreshChild(dir)
		}
	}
}
