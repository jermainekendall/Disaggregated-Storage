// Package erasure splits a file into k data shards and m parity shards with
// Reed-Solomon coding, and rebuilds shards or the file from any k of them.
// Everything streams through files on disk, so shards of several gigabytes
// do not need to fit in memory.
//
// Data shard i holds bytes [i*S, (i+1)*S) of the input, where
// S = ceil(size / k) (the same rule the placement service uses); the last data
// shard is zero-padded to S. Joining the data shards and truncating to the
// original size gives the input back.
package erasure

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/klauspost/reedsolomon"
)

// ShardSize returns ceil(size / data).
func ShardSize(size int64, data int) int64 {
	return (size + int64(data) - 1) / int64(data)
}

// Encode writes data+parity shard files named shard-<i> into dir and returns
// their paths in index order.
func Encode(input string, data, parity int, dir string) ([]string, error) {
	st, err := os.Stat(input)
	if err != nil {
		return nil, err
	}
	shardSize := ShardSize(st.Size(), data)
	enc, err := reedsolomon.NewStreamC(data, parity, true, true)
	if err != nil {
		return nil, err
	}

	in, err := os.Open(input)
	if err != nil {
		return nil, err
	}
	defer in.Close()

	paths := make([]string, data+parity)
	for i := range paths {
		paths[i] = filepath.Join(dir, fmt.Sprintf("shard-%d", i))
	}
	// Data shards: consecutive slices of the input, zero-padded.
	for i := 0; i < data; i++ {
		f, err := os.Create(paths[i])
		if err != nil {
			return nil, err
		}
		n, err := io.Copy(f, io.LimitReader(in, shardSize))
		if err == nil && n < shardSize {
			_, err = io.Copy(f, io.LimitReader(zeros{}, shardSize-n))
		}
		if cerr := f.Close(); err == nil {
			err = cerr
		}
		if err != nil {
			return nil, err
		}
	}

	readers, closeR, err := openAll(paths[:data])
	if err != nil {
		return nil, err
	}
	defer closeR()
	writers, closeW, err := createAll(paths[data:])
	if err != nil {
		return nil, err
	}
	if err := enc.Encode(readers, writers); err != nil {
		closeW()
		return nil, err
	}
	if err := closeW(); err != nil {
		return nil, err
	}
	return paths, nil
}

// Reconstruct rebuilds the shards listed in want. have maps shard index to
// the path of a verified copy and must contain at least `data` entries.
// Rebuilt shards are written to dir as shard-<i>; the returned map gives
// their paths.
func Reconstruct(have map[int]string, want []int, data, parity int, dir string) (map[int]string, error) {
	if len(have) < data {
		return nil, fmt.Errorf("need %d shards to rebuild, have %d", data, len(have))
	}
	enc, err := reedsolomon.NewStreamC(data, parity, true, true)
	if err != nil {
		return nil, err
	}
	total := data + parity
	valid := make([]io.Reader, total)
	fill := make([]io.Writer, total)
	var closers []io.Closer
	defer func() {
		for _, c := range closers {
			c.Close()
		}
	}()

	// Use exactly `data` inputs; more is allowed but only slows things down.
	used := 0
	for i := 0; i < total && used < data; i++ {
		p, ok := have[i]
		if !ok {
			continue
		}
		f, err := os.Open(p)
		if err != nil {
			return nil, err
		}
		closers = append(closers, f)
		valid[i] = bufio.NewReaderSize(f, 1<<20)
		used++
	}

	out := map[int]string{}
	var files []*os.File
	var bufs []*bufio.Writer
	for _, i := range want {
		if i < 0 || i >= total {
			return nil, fmt.Errorf("shard index %d out of range", i)
		}
		if valid[i] != nil {
			return nil, fmt.Errorf("shard %d is already present", i)
		}
		p := filepath.Join(dir, fmt.Sprintf("shard-%d", i))
		f, err := os.Create(p)
		if err != nil {
			return nil, err
		}
		files = append(files, f)
		bw := bufio.NewWriterSize(f, 1<<20)
		bufs = append(bufs, bw)
		fill[i] = bw
		out[i] = p
	}
	if err := enc.Reconstruct(valid, fill); err != nil {
		for _, f := range files {
			f.Close()
		}
		return nil, err
	}
	for i, f := range files {
		if err := bufs[i].Flush(); err != nil {
			return nil, err
		}
		if err := f.Close(); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// Join writes the original file (size bytes) from the data shards at
// dataPaths (index order) to w.
func Join(dataPaths []string, size int64, w io.Writer) error {
	remaining := size
	for _, p := range dataPaths {
		if remaining <= 0 {
			break
		}
		f, err := os.Open(p)
		if err != nil {
			return err
		}
		n, err := io.Copy(w, io.LimitReader(f, remaining))
		f.Close()
		if err != nil {
			return err
		}
		remaining -= n
	}
	if remaining != 0 {
		return errors.New("data shards are shorter than the file size")
	}
	return nil
}

type zeros struct{}

func (zeros) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = 0
	}
	return len(p), nil
}

func openAll(paths []string) ([]io.Reader, func(), error) {
	var files []*os.File
	closeAll := func() {
		for _, f := range files {
			f.Close()
		}
	}
	rs := make([]io.Reader, len(paths))
	for i, p := range paths {
		f, err := os.Open(p)
		if err != nil {
			closeAll()
			return nil, nil, err
		}
		files = append(files, f)
		rs[i] = bufio.NewReaderSize(f, 1<<20)
	}
	return rs, closeAll, nil
}

func createAll(paths []string) ([]io.Writer, func() error, error) {
	var files []*os.File
	var bufs []*bufio.Writer
	closeAll := func() error {
		var first error
		for i, f := range files {
			if err := bufs[i].Flush(); err != nil && first == nil {
				first = err
			}
			if err := f.Close(); err != nil && first == nil {
				first = err
			}
		}
		return first
	}
	ws := make([]io.Writer, len(paths))
	for i, p := range paths {
		f, err := os.Create(p)
		if err != nil {
			closeAll()
			return nil, nil, err
		}
		files = append(files, f)
		bw := bufio.NewWriterSize(f, 1<<20)
		bufs = append(bufs, bw)
		ws[i] = bw
	}
	return ws, closeAll, nil
}
