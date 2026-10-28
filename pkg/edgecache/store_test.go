package edgecache

import (
	"bytes"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func newTestStore(t *testing.T, head, tail int64) *Store {
	t.Helper()
	s, err := NewStore(t.TempDir(), head, tail)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func fileBytes(size int) []byte {
	b := make([]byte, size)
	for i := range b {
		b[i] = byte(i * 7)
	}
	return b
}

func TestHeadTailLenNeverOverlap(t *testing.T) {
	s := newTestStore(t, 10, 4)
	cases := []struct{ size, head, tail int64 }{
		{0, 0, 0},
		{5, 5, 0},
		{10, 10, 0},
		{12, 10, 2},
		{14, 10, 4},
		{100, 10, 4},
	}
	for _, c := range cases {
		if h, tl := s.HeadLen(c.size), s.TailLen(c.size); h != c.head || tl != c.tail {
			t.Errorf("size %d: head/tail = %d/%d, want %d/%d", c.size, h, tl, c.head, c.tail)
		}
	}
}

func TestKeyDependsOnIdentity(t *testing.T) {
	base := Key("hash", "a.mkv", 100)
	for _, other := range []string{Key("hash2", "a.mkv", 100), Key("hash", "b.mkv", 100), Key("hash", "a.mkv", 101)} {
		if other == base {
			t.Fatal("key collision across different identities")
		}
	}
	if Key("hash", "a.mkv", 100) != base {
		t.Fatal("key not deterministic")
	}
}

func TestPutCoverageAndCopy(t *testing.T) {
	s := newTestStore(t, 10, 4)
	data := fileBytes(100)
	key := Key("h", "f.mkv", 100)

	if h, tl := s.Coverage(key, 100); h != 0 || tl != 0 {
		t.Fatalf("empty store coverage %d/%d", h, tl)
	}
	if err := s.PutHead(key, 100, data[:9]); err == nil {
		t.Fatal("short head accepted")
	}
	if err := s.PutHead(key, 100, data[:10]); err != nil {
		t.Fatal(err)
	}
	if err := s.PutTail(key, 100, data[96:]); err != nil {
		t.Fatal(err)
	}
	if !s.Complete(key, 100) {
		t.Fatal("store not complete after head+tail")
	}

	for _, seg := range []Segment{
		{Local: true, Start: 0, End: 9},
		{Local: true, Start: 3, End: 6},
		{Local: true, Tail: true, Start: 96, End: 99},
		{Local: true, Tail: true, Start: 98, End: 99},
	} {
		var buf bytes.Buffer
		if err := s.CopySegment(&buf, key, 100, seg); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(buf.Bytes(), data[seg.Start:seg.End+1]) {
			t.Fatalf("segment %+v returned wrong bytes", seg)
		}
	}
}

func TestTruncatedPartCountsAsMissing(t *testing.T) {
	s := newTestStore(t, 10, 4)
	key := Key("h", "f.mkv", 100)
	if err := s.PutHead(key, 100, fileBytes(10)); err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(s.path(key, headSuffix), 5); err != nil {
		t.Fatal(err)
	}
	if h, _ := s.Coverage(key, 100); h != 0 {
		t.Fatalf("truncated head reported as %d bytes", h)
	}
}

func TestPrune(t *testing.T) {
	s := newTestStore(t, 10, 4)
	keep, drop := Key("a", "x", 100), Key("b", "y", 100)
	for _, k := range []string{keep, drop} {
		if err := s.PutHead(k, 100, fileBytes(10)); err != nil {
			t.Fatal(err)
		}
		if err := s.PutTail(k, 100, fileBytes(4)); err != nil {
			t.Fatal(err)
		}
	}
	stale := filepath.Join(s.dir, keep[:2], keep+".head.123.tmp")
	if err := os.WriteFile(stale, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	removed, freed, err := s.Prune(map[string]struct{}{keep: {}})
	if err != nil {
		t.Fatal(err)
	}
	if removed != 3 || freed != 15 {
		t.Fatalf("removed %d files / %d bytes, want 3 / 15", removed, freed)
	}
	if !s.Complete(keep, 100) || s.Complete(drop, 100) {
		t.Fatal("prune kept/removed the wrong keys")
	}
}

func TestPlan(t *testing.T) {
	const size = 100
	cases := []struct {
		name             string
		start, end, h, t int64
		want             []Segment
	}{
		{"all in head", 0, 9, 10, 4, []Segment{{Local: true, Start: 0, End: 9}}},
		{"all in tail", 96, 99, 10, 4, []Segment{{Local: true, Tail: true, Start: 96, End: 99}}},
		{"head then remote", 0, 50, 10, 4, []Segment{{Local: true, Start: 0, End: 9}, {Start: 10, End: 50}}},
		{"whole file", 0, 99, 10, 4, []Segment{{Local: true, Start: 0, End: 9}, {Start: 10, End: 95}, {Local: true, Tail: true, Start: 96, End: 99}}},
		{"middle only", 20, 30, 10, 4, []Segment{{Start: 20, End: 30}}},
		{"middle into tail", 50, 99, 10, 4, []Segment{{Start: 50, End: 95}, {Local: true, Tail: true, Start: 96, End: 99}}},
		{"nothing stored", 0, 99, 0, 0, []Segment{{Start: 0, End: 99}}},
		{"head missing tail present", 0, 99, 0, 4, []Segment{{Start: 0, End: 95}, {Local: true, Tail: true, Start: 96, End: 99}}},
		{"invalid", 50, 10, 10, 4, nil},
	}
	for _, c := range cases {
		if got := Plan(c.start, c.end, size, c.h, c.t); !reflect.DeepEqual(got, c.want) {
			t.Errorf("%s: got %+v, want %+v", c.name, got, c.want)
		}
	}
}
