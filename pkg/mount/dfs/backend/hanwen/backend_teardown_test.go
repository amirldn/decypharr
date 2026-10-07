//go:build linux || (darwin && amd64)

package hanwen

import (
	"context"
	"testing"
)

func TestUnmountClearsReady(t *testing.T) {
	b := &Backend{unmountFunc: func(context.Context) error { return nil }}
	b.ready.Store(true)
	if err := b.Unmount(context.Background()); err != nil {
		t.Fatal(err)
	}
	if b.IsReady() {
		t.Fatal("backend still reports ready after unmount")
	}
}
