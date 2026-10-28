// Package edgecache keeps the first and last bytes of every media file on
// local disk, so start-up reads (container header, end-of-file index) are
// served without a debrid round trip however long ago a title was played.
package edgecache

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

const (
	headSuffix = ".head"
	tailSuffix = ".tail"
	tmpSuffix  = ".tmp"
)

// Store is the on-disk head/tail store. Each file is identified by its
// entry infohash, filename and size, so a replaced or resized file never
// matches a stale entry. Writes are atomic (temp file + rename).
type Store struct {
	dir      string
	headSize int64
	tailSize int64
}

// NewStore creates the store directory if needed.
func NewStore(dir string, headSize, tailSize int64) (*Store, error) {
	if dir == "" {
		return nil, errors.New("edge cache dir is required")
	}
	if headSize <= 0 && tailSize <= 0 {
		return nil, errors.New("edge cache head and tail sizes are both zero")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create edge cache dir: %w", err)
	}
	return &Store{dir: dir, headSize: headSize, tailSize: tailSize}, nil
}

// Key derives the stable key for a file.
func Key(infohash, filename string, size int64) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s\x00%s\x00%d", infohash, filename, size)))
	return hex.EncodeToString(sum[:16])
}

func (s *Store) path(key, suffix string) string {
	return filepath.Join(s.dir, key[:2], key+suffix)
}

// HeadLen is the number of leading bytes the store keeps for a file of size.
func (s *Store) HeadLen(size int64) int64 { return min(s.headSize, max(size, 0)) }

// TailLen is the number of trailing bytes the store keeps for a file of size.
// The tail never overlaps the head; a file small enough to fit in the head has
// no tail.
func (s *Store) TailLen(size int64) int64 {
	return max(min(s.tailSize, size-s.HeadLen(size)), 0)
}

// Coverage reports how many head and tail bytes are present for key. A part
// only counts when its stored length matches exactly, so a truncated write is
// treated as missing.
func (s *Store) Coverage(key string, size int64) (head, tail int64) {
	if want := s.HeadLen(size); want > 0 && fileLen(s.path(key, headSuffix)) == want {
		head = want
	}
	if want := s.TailLen(size); want > 0 && fileLen(s.path(key, tailSuffix)) == want {
		tail = want
	}
	return head, tail
}

// Complete reports whether both parts are stored.
func (s *Store) Complete(key string, size int64) bool {
	head, tail := s.Coverage(key, size)
	return head == s.HeadLen(size) && tail == s.TailLen(size)
}

func fileLen(path string) int64 {
	fi, err := os.Stat(path)
	if err != nil {
		return -1
	}
	return fi.Size()
}

// PutHead stores the head bytes for key. data must be exactly HeadLen(size).
func (s *Store) PutHead(key string, size int64, data []byte) error {
	if int64(len(data)) != s.HeadLen(size) {
		return fmt.Errorf("head length %d, want %d", len(data), s.HeadLen(size))
	}
	return s.writeAtomic(s.path(key, headSuffix), data)
}

// PutTail stores the tail bytes for key. data must be exactly TailLen(size).
func (s *Store) PutTail(key string, size int64, data []byte) error {
	if int64(len(data)) != s.TailLen(size) {
		return fmt.Errorf("tail length %d, want %d", len(data), s.TailLen(size))
	}
	if len(data) == 0 {
		return nil
	}
	return s.writeAtomic(s.path(key, tailSuffix), data)
}

func (s *Store) writeAtomic(path string, data []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(path), filepath.Base(path)+".*"+tmpSuffix)
	if err != nil {
		return err
	}
	tmp := f.Name()
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return nil
}

// OpenPart opens a stored part. tail selects the tail file.
func (s *Store) OpenPart(key string, tail bool) (*os.File, error) {
	if tail {
		return os.Open(s.path(key, tailSuffix))
	}
	return os.Open(s.path(key, headSuffix))
}

// Remove deletes both parts for key.
func (s *Store) Remove(key string) {
	_ = os.Remove(s.path(key, headSuffix))
	_ = os.Remove(s.path(key, tailSuffix))
}

// Prune deletes every stored part whose key is not in keep, plus stale temp
// files. It returns the number of files removed and bytes freed.
func (s *Store) Prune(keep map[string]struct{}) (removed int, freed int64, err error) {
	err = filepath.WalkDir(s.dir, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil || d.IsDir() {
			return nil
		}
		name := d.Name()
		var key string
		switch {
		case strings.HasSuffix(name, tmpSuffix):
			key = "" // always remove leftovers from interrupted writes
		case strings.HasSuffix(name, headSuffix):
			key = strings.TrimSuffix(name, headSuffix)
		case strings.HasSuffix(name, tailSuffix):
			key = strings.TrimSuffix(name, tailSuffix)
		default:
			return nil
		}
		if key != "" {
			if _, ok := keep[key]; ok {
				return nil
			}
		}
		if info, ierr := d.Info(); ierr == nil {
			freed += info.Size()
		}
		if os.Remove(path) == nil {
			removed++
		}
		return nil
	})
	return removed, freed, err
}

// Usage returns the number of stored part files and their total bytes.
func (s *Store) Usage() (files int, bytes int64) {
	_ = filepath.WalkDir(s.dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || strings.HasSuffix(d.Name(), tmpSuffix) {
			return nil
		}
		if info, ierr := d.Info(); ierr == nil {
			files++
			bytes += info.Size()
		}
		return nil
	})
	return files, bytes
}

// Segment is one contiguous piece of a response: either served from a stored
// part (Local) or fetched from the remote source.
type Segment struct {
	Local bool
	Tail  bool  // which stored part, when Local
	Start int64 // absolute file offset
	End   int64 // inclusive absolute file offset
}

// Plan splits the inclusive range [start, end] of a file into local and remote
// segments given the stored head and tail lengths. Remote segments are always
// a single contiguous run, so at most one remote stream is needed.
func Plan(start, end, size, head, tail int64) []Segment {
	if start < 0 || end < start || end >= size {
		return nil
	}
	tailStart := size - tail
	var segs []Segment
	pos := start
	if head > 0 && pos < head {
		segEnd := min(end, head-1)
		segs = append(segs, Segment{Local: true, Start: pos, End: segEnd})
		pos = segEnd + 1
	}
	if pos > end {
		return segs
	}
	if tail > 0 && end >= tailStart {
		if pos < tailStart {
			segs = append(segs, Segment{Start: pos, End: tailStart - 1})
			pos = tailStart
		}
		segs = append(segs, Segment{Local: true, Tail: true, Start: pos, End: end})
		return segs
	}
	return append(segs, Segment{Start: pos, End: end})
}

// CopySegment writes a local segment to w.
func (s *Store) CopySegment(w io.Writer, key string, size int64, seg Segment) error {
	f, err := s.OpenPart(key, seg.Tail)
	if err != nil {
		return err
	}
	defer f.Close()
	base := int64(0)
	if seg.Tail {
		base = size - s.TailLen(size)
	}
	_, err = io.Copy(w, io.NewSectionReader(f, seg.Start-base, seg.End-seg.Start+1))
	return err
}
