package webdav

import (
	"bytes"
	"io"
	"testing"

	"github.com/sirrobot01/decypharr/pkg/edgecache"
)

func TestEdgeCaptureCollectsHeadAndTailAcrossChunks(t *testing.T) {
	store, err := edgecache.NewStore(t.TempDir(), 10, 4)
	if err != nil {
		t.Fatal(err)
	}
	const size = 100
	content := edgeBytes(size)
	var out bytes.Buffer
	c := newEdgeCapture(&out, store, edgecache.Segment{Start: 0, End: size - 1}, size, 0, 0)
	if c == nil || len(c.windows) != 2 {
		t.Fatal("expected head and tail windows")
	}
	// Odd chunk sizes so windows straddle chunk boundaries.
	if _, err := io.CopyBuffer(c, bytes.NewReader(content), make([]byte, 7)); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(out.Bytes(), content) {
		t.Fatal("capture altered the pass-through bytes")
	}
	head, tail := c.windows[0], c.windows[1]
	if !bytes.Equal(head.buf, content[:10]) || head.got != 10 {
		t.Fatal("head window wrong")
	}
	if !bytes.Equal(tail.buf, content[96:]) || tail.got != 4 {
		t.Fatal("tail window wrong")
	}
}

func TestEdgeCaptureSkipsPartialOrPresent(t *testing.T) {
	store, _ := edgecache.NewStore(t.TempDir(), 10, 4)
	// Starts after byte 0 and stops before the end: cannot complete either part.
	if newEdgeCapture(io.Discard, store, edgecache.Segment{Start: 5, End: 50}, 100, 0, 0) != nil {
		t.Fatal("capture created for a segment that covers no complete part")
	}
	// Both parts already present.
	if newEdgeCapture(io.Discard, store, edgecache.Segment{Start: 0, End: 99}, 100, 10, 4) != nil {
		t.Fatal("capture created when nothing is missing")
	}
}
