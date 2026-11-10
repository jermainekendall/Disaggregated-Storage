// Package shardstore keeps the shards this peer holds for others.
//
// Layout under the storage directory (all 0700 dirs, 0600 files):
//
//	shards/<aa>/<shard_id>        shard bytes
//	shards/<aa>/<shard_id>.json   metadata (file, index, size, SHA-256)
//	tmp/                          partial writes, emptied at startup
//
// A shard is written to tmp/, fsynced, checked against the expected SHA-256
// and size, then renamed into place, so a crash never leaves a half-written
// shard that looks complete. The index is rebuilt from the metadata files at
// startup. Space is reserved before a write starts, under the same lock as
// the capacity check, so concurrent stores cannot overcommit the disk.
package shardstore

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"sync"
	"syscall"
	"time"
)

// Meta describes one stored shard.
type Meta struct {
	ShardID  string    `json:"shard_id"`
	FileID   string    `json:"file_id"`
	Index    int       `json:"shard_index"`
	Size     int64     `json:"size"`
	SHA256   string    `json:"sha256"`
	StoredAt time.Time `json:"stored_at"`
}

// Store is safe for concurrent use.
type Store struct {
	dir      string
	capacity int64 // bytes the user dedicated to the network

	mu       sync.Mutex
	shards   map[string]Meta
	used     int64
	reserved int64
}

var (
	ErrNoSpace  = errors.New("not enough free space")
	ErrNotFound = errors.New("shard not found")
	ErrExists   = errors.New("shard already stored")
	idRe        = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)
)

// CheckPermissions refuses a storage directory others can read or write.
func CheckPermissions(dir string) error {
	st, err := os.Stat(dir)
	if err != nil {
		return err
	}
	if !st.IsDir() {
		return fmt.Errorf("%s is not a directory", dir)
	}
	if st.Mode().Perm()&0o077 != 0 {
		return fmt.Errorf("storage directory %s has mode %o; it must be 0700 (run: chmod 700 %s)",
			dir, st.Mode().Perm(), dir)
	}
	return nil
}

// Open loads the index from disk. dir must exist with mode 0700.
func Open(dir string, capacityBytes int64) (*Store, error) {
	if err := CheckPermissions(dir); err != nil {
		return nil, err
	}
	s := &Store{dir: dir, capacity: capacityBytes, shards: map[string]Meta{}}
	for _, d := range []string{"shards", "tmp"} {
		if err := os.MkdirAll(filepath.Join(dir, d), 0o700); err != nil {
			return nil, err
		}
	}
	// Anything in tmp/ is an interrupted write.
	entries, _ := os.ReadDir(filepath.Join(dir, "tmp"))
	for _, e := range entries {
		os.Remove(filepath.Join(dir, "tmp", e.Name()))
	}
	err := filepath.WalkDir(filepath.Join(dir, "shards"), func(p string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() || filepath.Ext(p) != ".json" {
			return err
		}
		raw, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		var m Meta
		if json.Unmarshal(raw, &m) != nil || !idRe.MatchString(m.ShardID) {
			return nil // ignore junk
		}
		st, err := os.Stat(s.dataPath(m.ShardID))
		if err != nil || st.Size() != m.Size {
			// Metadata without matching data: drop both.
			os.Remove(p)
			os.Remove(s.dataPath(m.ShardID))
			return nil
		}
		s.shards[m.ShardID] = m
		s.used += m.Size
		return nil
	})
	return s, err
}

func (s *Store) dataPath(id string) string {
	return filepath.Join(s.dir, "shards", id[:2], id)
}

// Path returns the data file of a stored shard.
func (s *Store) Path(id string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.shards[id]; !ok {
		return "", ErrNotFound
	}
	return s.dataPath(id), nil
}

// Get returns a shard's metadata.
func (s *Store) Get(id string) (Meta, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	m, ok := s.shards[id]
	return m, ok
}

// Stats returns the shard count, bytes used and capacity.
func (s *Store) Stats() (count int, used, capacity int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.shards), s.used, s.capacity
}

// Available is the space still offered to the network: the configured
// capacity minus what is used or reserved, never more than the disk has free.
func (s *Store) Available() int64 {
	s.mu.Lock()
	avail := s.capacity - s.used - s.reserved
	s.mu.Unlock()
	if free := diskFree(s.dir); free >= 0 && free < avail {
		avail = free
	}
	if avail < 0 {
		avail = 0
	}
	return avail
}

func diskFree(dir string) int64 {
	var st syscall.Statfs_t
	if syscall.Statfs(dir, &st) != nil {
		return -1
	}
	return int64(st.Bavail) * int64(st.Bsize)
}

// Put stores size bytes from r as shard m.ShardID. If m.SHA256 is set the
// bytes must match it. The returned Meta has the computed hash.
func (s *Store) Put(m Meta, r io.Reader) (Meta, error) {
	if !idRe.MatchString(m.ShardID) {
		return Meta{}, fmt.Errorf("invalid shard id %q", m.ShardID)
	}
	if m.Size <= 0 {
		return Meta{}, errors.New("shard size must be positive")
	}

	// Reserve space and claim the ID under one lock.
	s.mu.Lock()
	if _, ok := s.shards[m.ShardID]; ok {
		s.mu.Unlock()
		return Meta{}, ErrExists
	}
	free := diskFree(s.dir)
	if s.used+s.reserved+m.Size > s.capacity || (free >= 0 && m.Size > free-s.reserved) {
		s.mu.Unlock()
		return Meta{}, ErrNoSpace
	}
	s.reserved += m.Size
	s.mu.Unlock()
	release := func() {
		s.mu.Lock()
		s.reserved -= m.Size
		s.mu.Unlock()
	}

	tmp, err := os.CreateTemp(filepath.Join(s.dir, "tmp"), m.ShardID+"-*")
	if err != nil {
		release()
		return Meta{}, err
	}
	fail := func(err error) (Meta, error) {
		tmp.Close()
		os.Remove(tmp.Name())
		release()
		return Meta{}, err
	}
	h := sha256.New()
	n, err := io.Copy(io.MultiWriter(tmp, h), io.LimitReader(r, m.Size+1))
	if err != nil {
		return fail(err)
	}
	if n != m.Size {
		return fail(fmt.Errorf("received %d bytes, expected %d", n, m.Size))
	}
	sum := hex.EncodeToString(h.Sum(nil))
	if m.SHA256 != "" && sum != m.SHA256 {
		return fail(fmt.Errorf("hash mismatch: got %s, expected %s", sum, m.SHA256))
	}
	if err := tmp.Sync(); err != nil {
		return fail(err)
	}
	if err := tmp.Close(); err != nil {
		return fail(err)
	}

	m.SHA256 = sum
	m.StoredAt = time.Now().UTC()
	final := s.dataPath(m.ShardID)
	if err := os.MkdirAll(filepath.Dir(final), 0o700); err != nil {
		return fail(err)
	}
	if err := os.Rename(tmp.Name(), final); err != nil {
		return fail(err)
	}
	raw, _ := json.Marshal(m)
	if err := writeFileSync(final+".json", raw); err != nil {
		os.Remove(final)
		release()
		return Meta{}, err
	}
	syncDir(filepath.Dir(final))

	s.mu.Lock()
	s.reserved -= m.Size
	s.used += m.Size
	s.shards[m.ShardID] = m
	s.mu.Unlock()
	return m, nil
}

// Delete removes a shard's files. Deleting an unknown shard is not an error.
func (s *Store) Delete(id string) error {
	if !idRe.MatchString(id) {
		return fmt.Errorf("invalid shard id %q", id)
	}
	s.mu.Lock()
	m, ok := s.shards[id]
	delete(s.shards, id)
	if ok {
		s.used -= m.Size
	}
	s.mu.Unlock()
	p := s.dataPath(id)
	if err := os.Remove(p + ".json"); err != nil && !os.IsNotExist(err) {
		return err
	}
	if err := os.Remove(p); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// Verify re-reads a shard from disk and compares it with its stored hash.
func (s *Store) Verify(id string) error {
	m, ok := s.Get(id)
	if !ok {
		return ErrNotFound
	}
	f, err := os.Open(s.dataPath(id))
	if err != nil {
		return err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return err
	}
	if got := hex.EncodeToString(h.Sum(nil)); got != m.SHA256 {
		return fmt.Errorf("shard %s is corrupt: hash %s, expected %s", id, got, m.SHA256)
	}
	return nil
}

// List returns all stored shards sorted by ID.
func (s *Store) List() []Meta {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Meta, 0, len(s.shards))
	for _, m := range s.shards {
		out = append(out, m)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ShardID < out[j].ShardID })
	return out
}

func writeFileSync(path string, data []byte) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

func syncDir(dir string) {
	if d, err := os.Open(dir); err == nil {
		d.Sync()
		d.Close()
	}
}
