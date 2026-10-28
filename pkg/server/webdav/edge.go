package webdav

import (
	"io"

	"github.com/sirrobot01/decypharr/pkg/edgecache"
)

// edgeCapture copies the bytes of a debrid read that fall inside a missing
// stored head or tail, so playback fills the edge cache as a side effect.
type edgeCapture struct {
	w       io.Writer
	pos     int64 // absolute file offset of the next byte written
	windows []*captureWindow
}

type captureWindow struct {
	start, end int64 // inclusive absolute range
	buf        []byte
	got        int64
	tail       bool
}

// newEdgeCapture returns nil when the remote segment cannot complete a
// missing part: it must cover the whole head (from byte 0) or the whole tail
// (through the last byte).
func newEdgeCapture(w io.Writer, store *edgecache.Store, seg edgecache.Segment, size, headHave, tailHave int64) *edgeCapture {
	if store == nil {
		return nil
	}
	c := &edgeCapture{w: w, pos: seg.Start}
	if want := store.HeadLen(size); want > 0 && headHave == 0 && seg.Start == 0 && seg.End >= want-1 {
		c.windows = append(c.windows, &captureWindow{start: 0, end: want - 1, buf: make([]byte, want)})
	}
	if want := store.TailLen(size); want > 0 && tailHave == 0 && seg.Start <= size-want && seg.End == size-1 {
		c.windows = append(c.windows, &captureWindow{start: size - want, end: size - 1, buf: make([]byte, want), tail: true})
	}
	if len(c.windows) == 0 {
		return nil
	}
	return c
}

func (c *edgeCapture) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	chunkStart, chunkEnd := c.pos, c.pos+int64(n)-1
	for _, win := range c.windows {
		lo, hi := max(chunkStart, win.start), min(chunkEnd, win.end)
		if lo > hi {
			continue
		}
		copy(win.buf[lo-win.start:hi-win.start+1], p[lo-chunkStart:hi-chunkStart+1])
		win.got += hi - lo + 1
	}
	c.pos += int64(n)
	return n, err
}

// finish hands every fully captured window to the edge cache.
func (c *edgeCapture) finish(svc *edgecache.Service, infohash, name string, size int64) {
	if c == nil || svc == nil {
		return
	}
	for _, win := range c.windows {
		if win.got == win.end-win.start+1 {
			svc.Capture(infohash, name, size, win.start, win.buf, win.tail)
		}
	}
}
