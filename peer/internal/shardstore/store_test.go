package shardstore

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

const (
	id1 = "11111111-1111-4111-8111-111111111111"
	id2 = "22222222-2222-4222-8222-222222222222"
)

func newStore(t *testing.T, capacity int64) (*Store, string) {
	dir := filepath.Join(t.TempDir(), "storage")
	os.Mkdir(dir, 0o700)
	s, err := Open(dir, capacity)
	if err != nil {
		t.Fatal(err)
	}
	return s, dir
}

func data(n int) ([]byte, string) {
	b := make([]byte, n)
	rand.Read(b)
	h := sha256.Sum256(b)
	return b, hex.EncodeToString(h[:])
}

func TestPutGetSurvivesRestart(t *testing.T) {
	s, dir := newStore(t, 1<<20)
	b, sum := data(1000)
	m, err := s.Put(Meta{ShardID: id1, FileID: "f", Index: 2, Size: 1000, SHA256: sum}, bytes.NewReader(b))
	if err != nil || m.SHA256 != sum {
		t.Fatalf("put: %v", err)
	}
	st, _ := os.Stat(filepath.Join(dir, "shards", id1[:2], id1))
	if st.Mode().Perm() != 0o600 {
		t.Fatalf("shard file mode %o", st.Mode().Perm())
	}

	// A fresh Store over the same directory finds it again (F022).
	s2, err := Open(dir, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if got, ok := s2.Get(id1); !ok || got.Index != 2 || got.SHA256 != sum {
		t.Fatalf("after restart: %+v %v", got, ok)
	}
	if err := s2.Verify(id1); err != nil {
		t.Fatal(err)
	}
	if n, used, _ := s2.Stats(); n != 1 || used != 1000 {
		t.Fatalf("stats %d %d", n, used)
	}
}

func TestPutRejectsBadHashAndShortData(t *testing.T) {
	s, dir := newStore(t, 1<<20)
	b, _ := data(100)
	if _, err := s.Put(Meta{ShardID: id1, Size: 100, SHA256: "00"}, bytes.NewReader(b)); err == nil {
		t.Fatal("hash mismatch accepted")
	}
	if _, err := s.Put(Meta{ShardID: id1, Size: 200}, bytes.NewReader(b)); err == nil {
		t.Fatal("short data accepted")
	}
	if _, err := s.Put(Meta{ShardID: "../../etc/passwd", Size: 1}, bytes.NewReader(b)); err == nil {
		t.Fatal("path traversal id accepted")
	}
	if _, ok := s.Get(id1); ok {
		t.Fatal("failed write left a shard")
	}
	left, _ := os.ReadDir(filepath.Join(dir, "tmp"))
	if len(left) != 0 {
		t.Fatalf("temp files left behind: %d", len(left))
	}
	if _, used, _ := s.Stats(); used != 0 || s.reserved != 0 {
		t.Fatal("space not released after failed write")
	}
}

func TestCapacityUnderConcurrency(t *testing.T) {
	// Capacity for 5 shards; 20 concurrent writers must not overcommit (F023).
	s, _ := newStore(t, 5*1000)
	var wg sync.WaitGroup
	var mu sync.Mutex
	ok := 0
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			b, sum := data(1000)
			id := id1[:24] + "0000000000" + string(rune('a'+i%6)) + string(rune('a'+i/6))
			if _, err := s.Put(Meta{ShardID: id, Size: 1000, SHA256: sum}, bytes.NewReader(b)); err == nil {
				mu.Lock()
				ok++
				mu.Unlock()
			}
		}(i)
	}
	wg.Wait()
	if ok != 5 {
		t.Fatalf("%d shards stored, capacity allows 5", ok)
	}
}

func TestDeleteRemovesFiles(t *testing.T) {
	s, dir := newStore(t, 1<<20)
	b, sum := data(10)
	s.Put(Meta{ShardID: id2, Size: 10, SHA256: sum}, bytes.NewReader(b))
	if err := s.Delete(id2); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "shards", id2[:2], id2)); !os.IsNotExist(err) {
		t.Fatal("data file still on disk (F024)")
	}
	if _, used, _ := s.Stats(); used != 0 {
		t.Fatal("used bytes not released")
	}
	if err := s.Delete(id2); err != nil {
		t.Fatal("second delete should be a no-op")
	}
}

func TestVerifyDetectsCorruption(t *testing.T) {
	s, dir := newStore(t, 1<<20)
	b, sum := data(100)
	s.Put(Meta{ShardID: id1, Size: 100, SHA256: sum}, bytes.NewReader(b))
	p := filepath.Join(dir, "shards", id1[:2], id1)
	b[0] ^= 1
	os.WriteFile(p, b, 0o600)
	if err := s.Verify(id1); err == nil {
		t.Fatal("corruption not detected")
	}
}

func TestOpenRefusesLoosePermissions(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "storage")
	os.Mkdir(dir, 0o755)
	if _, err := Open(dir, 1); err == nil {
		t.Fatal("0755 storage directory accepted (F019)")
	}
}
