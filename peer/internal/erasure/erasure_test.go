package erasure

import (
	"bytes"
	"crypto/rand"
	"os"
	"path/filepath"
	"testing"
)

func TestEncodeReconstructJoin(t *testing.T) {
	for _, size := range []int{1, 1000, 4 << 20, 4<<20 + 3} {
		dir := t.TempDir()
		data := make([]byte, size)
		rand.Read(data)
		in := filepath.Join(dir, "input")
		os.WriteFile(in, data, 0600)

		const k, m = 4, 2
		shards, err := Encode(in, k, m, dir)
		if err != nil {
			t.Fatal(err)
		}
		want := ShardSize(int64(size), k)
		for _, p := range shards {
			st, _ := os.Stat(p)
			if st.Size() != want {
				t.Fatalf("shard %s is %d bytes, want %d", p, st.Size(), want)
			}
		}

		// Lose two data shards (the worst case for k=4, m=2).
		have := map[int]string{2: shards[2], 3: shards[3], 4: shards[4], 5: shards[5]}
		outDir := t.TempDir()
		rebuilt, err := Reconstruct(have, []int{0, 1}, k, m, outDir)
		if err != nil {
			t.Fatal(err)
		}
		for _, i := range []int{0, 1} {
			a, _ := os.ReadFile(shards[i])
			b, _ := os.ReadFile(rebuilt[i])
			if !bytes.Equal(a, b) {
				t.Fatalf("size %d: rebuilt shard %d differs", size, i)
			}
		}

		var out bytes.Buffer
		if err := Join([]string{rebuilt[0], rebuilt[1], shards[2], shards[3]}, int64(size), &out); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(out.Bytes(), data) {
			t.Fatalf("size %d: joined file differs", size)
		}
	}
}

func TestReconstructParity(t *testing.T) {
	dir := t.TempDir()
	data := make([]byte, 100000)
	rand.Read(data)
	in := filepath.Join(dir, "input")
	os.WriteFile(in, data, 0600)
	shards, _ := Encode(in, 3, 2, dir)
	rebuilt, err := Reconstruct(map[int]string{0: shards[0], 1: shards[1], 3: shards[3]}, []int{4}, 3, 2, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	a, _ := os.ReadFile(shards[4])
	b, _ := os.ReadFile(rebuilt[4])
	if !bytes.Equal(a, b) {
		t.Fatal("rebuilt parity shard differs")
	}
}

func TestReconstructNeedsEnough(t *testing.T) {
	if _, err := Reconstruct(map[int]string{0: "a"}, []int{1}, 2, 1, t.TempDir()); err == nil {
		t.Fatal("expected error with too few shards")
	}
}
