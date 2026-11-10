package p2p

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/crypto"

	"github.com/jermainekendall/disaggregated-storage/peer/internal/shardstore"
)

const shardID = "33333333-3333-4333-8333-333333333333"

func newHost(t *testing.T, listen int) *Host {
	k, _, _ := crypto.GenerateEd25519Key(nil)
	h, err := NewHost(HostConfig{Key: k, ListenPort: listen, AllowLoopback: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { h.Close() })
	return h
}

func setupPeers(t *testing.T, authorize Authorizer) (*Host, *Host, *shardstore.Store) {
	dir := filepath.Join(t.TempDir(), "s")
	os.Mkdir(dir, 0o700)
	store, err := shardstore.Open(dir, 1<<30)
	if err != nil {
		t.Fatal(err)
	}
	server := newHost(t, 0)
	(&Handlers{Store: store, Authorize: authorize}).Register(server)
	client := newHost(t, -1)
	if _, err := client.Dial(context.Background(), server.ID().String(), server.P2PAddrs()); err != nil {
		t.Fatal(err)
	}
	return server, client, store
}

func payload(n int) ([]byte, string) {
	b := make([]byte, n)
	rand.Read(b)
	s := sha256.Sum256(b)
	return b, hex.EncodeToString(s[:])
}

func TestPushAndFetch(t *testing.T) {
	data, sum := payload(3<<20 + 7)
	server, client, store := setupPeers(t, func(context.Context, string) (shardstore.Meta, error) {
		return shardstore.Meta{ShardID: shardID, Size: int64(len(data)), FileID: "f", Index: 3}, nil
	})
	ctx := context.Background()
	if err := client.PushShard(ctx, server.ID(), shardID, int64(len(data)), sum, bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
	if m, ok := store.Get(shardID); !ok || m.SHA256 != sum || m.Index != 3 {
		t.Fatalf("stored: %+v %v", m, ok)
	}
	var out bytes.Buffer
	n, reported, err := client.FetchShard(ctx, server.ID(), shardID, &out)
	if err != nil || n != int64(len(data)) || reported != sum || !bytes.Equal(out.Bytes(), data) {
		t.Fatalf("fetch: %d %s %v", n, reported, err)
	}
	st, err := client.QueryStatus(ctx, server.ID())
	if err != nil || st.Shards != 1 {
		t.Fatalf("status: %+v %v", st, err)
	}
}

// A refused push of a large shard must fail fast with the peer's reason,
// not block writing a body the peer will never read.
func TestRefusedPushDoesNotHang(t *testing.T) {
	data, sum := payload(8 << 20)
	server, client, store := setupPeers(t, func(context.Context, string) (shardstore.Meta, error) {
		return shardstore.Meta{}, errors.New("this peer was not assigned that shard")
	})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	start := time.Now()
	err := client.PushShard(ctx, server.ID(), shardID, int64(len(data)), sum, bytes.NewReader(data))
	if err == nil || !strings.Contains(err.Error(), "not assigned") {
		t.Fatalf("want refusal, got %v", err)
	}
	if time.Since(start) > 10*time.Second {
		t.Fatalf("refusal took %s", time.Since(start))
	}
	if _, ok := store.Get(shardID); ok {
		t.Fatal("refused shard was stored")
	}
}

func TestPushRejectsWrongSize(t *testing.T) {
	data, sum := payload(1000)
	server, client, _ := setupPeers(t, func(context.Context, string) (shardstore.Meta, error) {
		return shardstore.Meta{ShardID: shardID, Size: 999}, nil
	})
	err := client.PushShard(context.Background(), server.ID(), shardID, 1000, sum, bytes.NewReader(data))
	if err == nil || !strings.Contains(err.Error(), "size") {
		t.Fatalf("want size refusal, got %v", err)
	}
}
