package edgecache

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/request"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

type fakeSource struct {
	mu      sync.Mutex
	data    map[string][]byte // filename -> content
	entries []*storage.Entry
	active  atomic.Int32
	opens   atomic.Int32
	classes []request.Class
}

func (f *fakeSource) OpenForCache(ctx context.Context, e *storage.Entry, name string, off int64) (io.ReadCloser, error) {
	f.opens.Add(1)
	f.mu.Lock()
	f.classes = append(f.classes, request.ClassOf(ctx))
	b, ok := f.data[name]
	f.mu.Unlock()
	if !ok {
		return nil, errors.New("no such file")
	}
	return io.NopCloser(bytes.NewReader(b[off:])), nil
}
func (f *fakeSource) ActiveStreams() int { return int(f.active.Load()) }
func (f *fakeSource) ForEachEntry(fn func(*storage.Entry) error) error {
	for _, e := range f.entries {
		if err := fn(e); err != nil {
			return err
		}
	}
	return nil
}

func testService(t *testing.T, src *fakeSource) *Service {
	t.Helper()
	store := newTestStore(t, 10, 4)
	return &Service{store: store, source: src, logger: zerolog.Nop(), delay: time.Millisecond, imports: make(chan *storage.Entry, 4)}
}

func entryWith(files map[string]int64) *storage.Entry {
	e := &storage.Entry{InfoHash: "hash", Name: "Show", Files: map[string]*storage.File{}}
	for n, sz := range files {
		e.Files[n] = &storage.File{Name: n, Size: sz}
	}
	return e
}

func TestFillEntryStoresHeadAndTailOfMediaOnly(t *testing.T) {
	content := fileBytes(100)
	src := &fakeSource{data: map[string][]byte{"ep1.mkv": content, "info.nfo": fileBytes(50)}}
	s := testService(t, src)
	e := entryWith(map[string]int64{"ep1.mkv": 100, "info.nfo": 50})

	s.fillEntry(context.Background(), e)

	key := Key("hash", "ep1.mkv", 100)
	if !s.store.Complete(key, 100) {
		t.Fatal("media file not filled")
	}
	var buf bytes.Buffer
	_ = s.store.CopySegment(&buf, key, 100, Segment{Local: true, Tail: true, Start: 96, End: 99})
	if !bytes.Equal(buf.Bytes(), content[96:]) {
		t.Fatal("tail bytes wrong")
	}
	if s.store.Complete(Key("hash", "info.nfo", 50), 50) {
		t.Fatal("non-media file was filled")
	}
	for _, c := range src.classes {
		if c != request.ClassBackground {
			t.Fatalf("fill used %s lane, want background", c)
		}
	}

	// A second pass is a no-op.
	before := src.opens.Load()
	s.fillEntry(context.Background(), e)
	if src.opens.Load() != before {
		t.Fatal("complete file was fetched again")
	}
}

func TestFillWaitsForActiveStreams(t *testing.T) {
	src := &fakeSource{data: map[string][]byte{"ep1.mkv": fileBytes(100)}}
	src.active.Store(1)
	s := testService(t, src)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	s.fillEntry(ctx, entryWith(map[string]int64{"ep1.mkv": 100}))
	if src.opens.Load() != 0 {
		t.Fatal("filled while a user stream was active")
	}
}

func TestCaptureStoresPart(t *testing.T) {
	s := testService(t, &fakeSource{})
	content := fileBytes(100)
	s.Capture("hash", "ep1.mkv", 100, 0, content[:10], false)
	key := Key("hash", "ep1.mkv", 100)
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if h, _ := s.store.Coverage(key, 100); h == 10 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("captured head not stored")
}

func TestPruneSkipsEmptyListing(t *testing.T) {
	src := &fakeSource{data: map[string][]byte{"ep1.mkv": fileBytes(100)}}
	s := testService(t, src)
	e := entryWith(map[string]int64{"ep1.mkv": 100})
	s.fillEntry(context.Background(), e)

	s.prune() // no entries listed: must not wipe the store
	if !s.store.Complete(Key("hash", "ep1.mkv", 100), 100) {
		t.Fatal("prune against an empty listing removed data")
	}

	other := entryWith(map[string]int64{"ep2.mkv": 100})
	src.entries = []*storage.Entry{other}
	s.prune()
	if s.store.Complete(Key("hash", "ep1.mkv", 100), 100) {
		t.Fatal("prune kept a file no longer in the library")
	}
}
