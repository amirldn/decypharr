package hanwen

import (
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"strings"
	"testing"
)

func TestMountinfoRejectsExistingMountOnly(t *testing.T) {
	table := "23 1 0:2 / / rw - rootfs rootfs rw\n25 23 0:50 / /tmp/test\\040mount rw - fuse.decypharr decypharr rw\n"
	if err := checkMountinfo(strings.NewReader(table), "/tmp/test mount"); err == nil {
		t.Fatal("accepted existing FUSE mount")
	}
	if err := checkMountinfo(strings.NewReader(table), "/tmp/ordinary-directory"); err != nil {
		t.Fatal(err)
	}
	if err := checkMountinfo(strings.NewReader("invalid\n"), "/tmp/x"); err == nil {
		t.Fatal("accepted malformed mount table")
	}
}

func TestMountFuseErrorHasNoServer(t *testing.T) {
	server, err := mountFuse(t.TempDir()+"/missing", &fs.Inode{}, &fs.Options{MountOptions: fuse.MountOptions{DirectMountStrict: true}})
	if err == nil {
		t.Fatal("mount at missing path unexpectedly succeeded")
	}
	if server != nil {
		t.Fatal("failed mount returned non-nil server interface")
	}
}
