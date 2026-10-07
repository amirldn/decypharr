package hanwen

import (
	"fmt"
	"golang.org/x/sys/unix"
)

func checkMountpoint(path string) error {
	n, err := unix.Getfsstat(nil, unix.MNT_NOWAIT)
	if err != nil {
		return err
	}
	mounts := make([]unix.Statfs_t, n+16)
	n, err = unix.Getfsstat(mounts, unix.MNT_NOWAIT)
	if err != nil {
		return err
	}
	if n >= len(mounts) {
		return fmt.Errorf("mount table changed while checking %q", path)
	}
	for _, m := range mounts[:n] {
		if unix.ByteSliceToString(m.Mntonname[:]) == path {
			return fmt.Errorf("refusing to replace existing mount at %q; unmount it before starting DFS", path)
		}
	}
	return nil
}
