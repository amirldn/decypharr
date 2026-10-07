package hanwen

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
)

// A path stays owned until every operation capable of unmounting it has
// finished. In particular, a caller timing out does not cancel that ownership.
var mountPaths = struct {
	sync.Mutex
	owners map[string]*mountLease
}{owners: make(map[string]*mountLease)}

type mountLease struct {
	path string
	once sync.Once
	done chan struct{}
	err  error
}

func acquireMount(path string) (*mountLease, error) {
	path, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	// Mount creates the directory before acquiring it. Resolve aliases so two
	// Backend instances cannot acquire the same directory via different names.
	path, err = filepath.EvalSymlinks(path)
	if err != nil {
		return nil, err
	}
	mountPaths.Lock()
	defer mountPaths.Unlock()
	if _, ok := mountPaths.owners[path]; ok {
		return nil, fmt.Errorf("mount path %q is still owned by an active mount or unfinished cleanup", path)
	}
	lease := &mountLease{path: path, done: make(chan struct{})}
	mountPaths.owners[path] = lease
	return lease, nil
}

func (l *mountLease) startCleanup(cleanup func() error) {
	l.once.Do(func() {
		go func() {
			l.err = cleanup()
			if l.err == nil {
				mountPaths.Lock()
				delete(mountPaths.owners, l.path)
				mountPaths.Unlock()
			}
			// An error keeps the path reserved: allowing another mount would hide
			// an incomplete teardown. A process restart can recover this state.
			close(l.done)
		}()
	})
}

func (l *mountLease) wait(ctx context.Context) error {
	select {
	case <-l.done:
		return l.err
	default:
	}
	select {
	case <-l.done:
		return l.err
	case <-ctx.Done():
		return fmt.Errorf("mount cleanup for %q is still pending: %w", l.path, ctx.Err())
	}
}
