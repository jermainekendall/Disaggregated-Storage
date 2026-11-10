package p2p

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"

	"github.com/jermainekendall/disaggregated-storage/peer/internal/shardstore"
)

// Protocols. Every message is a 4-byte big-endian length followed by a JSON
// header; shard bytes follow the header where noted.
const (
	ProtoStore    protocol.ID = "/orchestrator/store/1.0.0"    // header + bytes -> result
	ProtoRetrieve protocol.ID = "/orchestrator/retrieve/1.0.0" // header -> result + bytes
	ProtoStatus   protocol.ID = "/orchestrator/status/1.0.0"   // -> status

	maxHeader = 64 << 10
)

type StoreRequest struct {
	ShardID string `json:"shard_id"`
	Size    int64  `json:"size"`
	SHA256  string `json:"sha256"`
}

type RetrieveRequest struct {
	ShardID string `json:"shard_id"`
}

// Result is the reply to store and retrieve.
type Result struct {
	OK     bool   `json:"ok"`
	Error  string `json:"error,omitempty"`
	Size   int64  `json:"size,omitempty"`
	SHA256 string `json:"sha256,omitempty"`
}

// Status is the reply to the status protocol. It deliberately reveals no
// local paths.
type Status struct {
	PeerID        string `json:"peer_id"`
	Shards        int    `json:"shards"`
	UsedBytes     int64  `json:"used_bytes"`
	CapacityBytes int64  `json:"capacity_bytes"`
	Version       string `json:"version"`
}

func writeMsg(w io.Writer, v interface{}) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	var n [4]byte
	binary.BigEndian.PutUint32(n[:], uint32(len(b)))
	if _, err := w.Write(n[:]); err != nil {
		return err
	}
	_, err = w.Write(b)
	return err
}

func readMsg(r io.Reader, v interface{}) error {
	var n [4]byte
	if _, err := io.ReadFull(r, n[:]); err != nil {
		return err
	}
	l := binary.BigEndian.Uint32(n[:])
	if l > maxHeader {
		return fmt.Errorf("header too large (%d bytes)", l)
	}
	b := make([]byte, l)
	if _, err := io.ReadFull(r, b); err != nil {
		return err
	}
	return json.Unmarshal(b, v)
}

// ---------------------------------------------------------------- server side

// Authorizer decides whether a peer may push a shard here. It returns the
// shard as the server assigned it (size, file, index and the expected hash,
// "" if unknown), or an error if this peer was not asked to store it.
type Authorizer func(ctx context.Context, shardID string) (shardstore.Meta, error)

// Handlers serves the shard protocols from a store.
type Handlers struct {
	Store     *shardstore.Store
	Authorize Authorizer
	// Stored is called after a pushed shard is safely on disk (the agent
	// confirms it to the server).
	Stored  func(ctx context.Context, m shardstore.Meta)
	PeerID  string
	Version string
}

// Register installs the protocol handlers on h.
func (hs *Handlers) Register(h *Host) {
	h.SetStreamHandler(ProtoStore, hs.handleStore)
	h.SetStreamHandler(ProtoRetrieve, hs.handleRetrieve)
	h.SetStreamHandler(ProtoStatus, hs.handleStatus)
}

func (hs *Handlers) handleStore(s network.Stream) {
	defer s.Close()
	remote := s.Conn().RemotePeer()
	s.SetReadDeadline(time.Now().Add(30 * time.Second))
	var req StoreRequest
	if err := readMsg(s, &req); err != nil {
		s.Reset()
		return
	}
	reply := func(err error, m shardstore.Meta) {
		res := Result{OK: err == nil, Size: m.Size, SHA256: m.SHA256}
		if err != nil {
			res.Error = err.Error()
			log.Printf("store %s from %s refused: %v", req.ShardID, remote, err)
		}
		s.SetWriteDeadline(time.Now().Add(30 * time.Second))
		writeMsg(s, res)
		if err != nil {
			// The sender may still be writing the body. Let it read the
			// refusal (it then resets the stream) instead of closing at once,
			// which could discard the reply; give up after a few seconds.
			s.CloseWrite()
			s.SetReadDeadline(time.Now().Add(5 * time.Second))
			io.Copy(io.Discard, s)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	want, err := hs.Authorize(ctx, req.ShardID)
	cancel()
	if err != nil {
		reply(err, shardstore.Meta{})
		return
	}
	size, expected := want.Size, want.SHA256
	if req.Size != size {
		reply(fmt.Errorf("size %d does not match the assigned shard size %d", req.Size, size), shardstore.Meta{})
		return
	}
	if expected == "" {
		expected = req.SHA256
	} else if req.SHA256 != "" && req.SHA256 != expected {
		reply(errors.New("hash does not match the shard's recorded hash"), shardstore.Meta{})
		return
	}
	// Allow a generous time for the body: at least 1 MB/s.
	s.SetReadDeadline(time.Now().Add(time.Minute + time.Duration(size/(1<<20))*time.Second))
	m, err := hs.Store.Put(shardstore.Meta{ShardID: req.ShardID, FileID: want.FileID, Index: want.Index,
		Size: size, SHA256: expected},
		bufio.NewReaderSize(s, 1<<20))
	if errors.Is(err, shardstore.ErrExists) {
		// A retry of a store that already succeeded.
		if prev, ok := hs.Store.Get(req.ShardID); ok && (expected == "" || prev.SHA256 == expected) {
			m, err = prev, nil
		}
	}
	if err == nil && hs.Stored != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		hs.Stored(ctx, m)
		cancel()
	}
	reply(err, m)
}

func (hs *Handlers) handleRetrieve(s network.Stream) {
	defer s.Close()
	s.SetReadDeadline(time.Now().Add(30 * time.Second))
	var req RetrieveRequest
	if err := readMsg(s, &req); err != nil {
		s.Reset()
		return
	}
	// Shards are encrypted by their owner, so serving them reveals only
	// ciphertext. Anyone who knows a shard ID may read it.
	m, ok := hs.Store.Get(req.ShardID)
	path, err := hs.Store.Path(req.ShardID)
	if !ok || err != nil {
		writeMsg(s, Result{Error: "shard not found"})
		return
	}
	f, err := openFile(path)
	if err != nil {
		writeMsg(s, Result{Error: "shard unreadable"})
		return
	}
	defer f.Close()
	s.SetWriteDeadline(time.Now().Add(time.Minute + time.Duration(m.Size/(1<<20))*time.Second))
	if err := writeMsg(s, Result{OK: true, Size: m.Size, SHA256: m.SHA256}); err != nil {
		return
	}
	io.Copy(s, f)
}

func (hs *Handlers) handleStatus(s network.Stream) {
	defer s.Close()
	n, used, capacity := hs.Store.Stats()
	s.SetWriteDeadline(time.Now().Add(10 * time.Second))
	writeMsg(s, Status{PeerID: hs.PeerID, Shards: n, UsedBytes: used, CapacityBytes: capacity, Version: hs.Version})
}

// ---------------------------------------------------------------- client side

func (h *Host) open(ctx context.Context, id peer.ID, p protocol.ID) (network.Stream, error) {
	return h.NewStream(network.WithUseTransient(ctx, "shard transfer"), id, p)
}

// PushShard sends size bytes from r to a peer as shardID. It returns once the
// peer has stored the shard (and confirmed it to the server).
func (h *Host) PushShard(ctx context.Context, id peer.ID, shardID string, size int64, sha256hex string, r io.Reader) error {
	s, err := h.open(ctx, id, ProtoStore)
	if err != nil {
		return err
	}
	defer s.Close()
	// An overall deadline, generous for slow links (at least ~1 MB/s).
	s.SetDeadline(time.Now().Add(2*time.Minute + time.Duration(size/(1<<20))*time.Second))
	if err := writeMsg(s, StoreRequest{ShardID: shardID, Size: size, SHA256: sha256hex}); err != nil {
		s.Reset()
		return err
	}
	// Read the reply while sending: a peer that refuses the shard replies
	// at once and stops reading, and blocking on the write would hang.
	type reply struct {
		res Result
		err error
	}
	replies := make(chan reply, 1)
	go func() {
		var r reply
		r.err = readMsg(s, &r.res)
		replies <- r
	}()
	sent := make(chan error, 1)
	go func() {
		_, err := io.CopyN(s, r, size)
		if err == nil {
			err = s.CloseWrite()
		}
		sent <- err
	}()

	var res Result
	select {
	case rp := <-replies:
		if rp.err == nil && !rp.res.OK {
			s.Reset() // unblocks the sender
			<-sent
			return fmt.Errorf("peer refused shard: %s", rp.res.Error)
		}
		if err := <-sent; err != nil && rp.err != nil {
			return err
		}
		if rp.err != nil {
			return fmt.Errorf("no reply from peer: %w", rp.err)
		}
		res = rp.res
	case err := <-sent:
		if err != nil {
			s.Reset()
			// Prefer the peer's stated reason if its reply got through.
			if rp := <-replies; rp.err == nil && !rp.res.OK {
				return fmt.Errorf("peer refused shard: %s", rp.res.Error)
			}
			return err
		}
		rp := <-replies
		if rp.err != nil {
			return fmt.Errorf("no reply from peer: %w", rp.err)
		}
		res = rp.res
	case <-ctx.Done():
		s.Reset()
		return ctx.Err()
	}
	if !res.OK {
		return fmt.Errorf("peer refused shard: %s", res.Error)
	}
	if sha256hex != "" && res.SHA256 != sha256hex {
		return errors.New("peer stored different bytes than sent")
	}
	return nil
}

// FetchShard downloads a shard into w and returns its size and the hash the
// peer reports (the caller verifies the bytes).
func (h *Host) FetchShard(ctx context.Context, id peer.ID, shardID string, w io.Writer) (int64, string, error) {
	s, err := h.open(ctx, id, ProtoRetrieve)
	if err != nil {
		return 0, "", err
	}
	defer s.Close()
	if err := writeMsg(s, RetrieveRequest{ShardID: shardID}); err != nil {
		s.Reset()
		return 0, "", err
	}
	s.CloseWrite()
	var res Result
	if err := readMsg(s, &res); err != nil {
		return 0, "", err
	}
	if !res.OK {
		return 0, "", fmt.Errorf("peer: %s", res.Error)
	}
	n, err := io.CopyN(w, s, res.Size)
	if err != nil {
		return n, "", fmt.Errorf("transfer interrupted after %d of %d bytes: %w", n, res.Size, err)
	}
	return n, res.SHA256, nil
}

// QueryStatus asks a peer for its status.
func (h *Host) QueryStatus(ctx context.Context, id peer.ID) (Status, error) {
	var st Status
	s, err := h.open(ctx, id, ProtoStatus)
	if err != nil {
		return st, err
	}
	defer s.Close()
	s.CloseWrite()
	err = readMsg(s, &st)
	return st, err
}
