package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"io"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/protocol/circuitv2/client"
	"github.com/libp2p/go-libp2p/p2p/protocol/circuitv2/relay"
	ma "github.com/multiformats/go-multiaddr"
)

// TestRelayCarriesLargeTransfer reserves a slot on the relay, dials the
// reserving peer through the circuit and sends 8 MiB, which the old
// hard-coded 1 MiB circuit limit would have cut off.
func TestRelayCarriesLargeTransfer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	priv, _, _ := crypto.GenerateEd25519Key(nil)
	r, err := newRelay(priv, "/ip4/127.0.0.1/tcp/0", nil, relay.Resources{
		Limit: buildLimit(0, 0), ReservationTTL: time.Hour, MaxReservations: 8, MaxCircuits: 8,
		BufferSize: 64 << 10, MaxReservationsPerPeer: 4, MaxReservationsPerIP: 8, MaxReservationsPerASN: 8,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	relayInfo := peer.AddrInfo{ID: r.ID(), Addrs: r.Addrs()}

	receiver, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"), libp2p.EnableRelay())
	if err != nil {
		t.Fatal(err)
	}
	defer receiver.Close()
	sender, err := libp2p.New(libp2p.NoListenAddrs, libp2p.EnableRelay())
	if err != nil {
		t.Fatal(err)
	}
	defer sender.Close()

	if err := receiver.Connect(ctx, relayInfo); err != nil {
		t.Fatal(err)
	}
	rsvp, err := client.Reserve(ctx, receiver, relayInfo)
	if err != nil {
		t.Fatalf("reserve: %v", err)
	}
	if rsvp.LimitDuration != 0 || rsvp.LimitData != 0 {
		t.Fatalf("relay advertised a limit: %s / %d bytes", rsvp.LimitDuration, rsvp.LimitData)
	}

	const proto = "/orchestrator/test-shard/1.0.0"
	want := make([]byte, 8<<20)
	rand.Read(want)
	got := make(chan []byte, 1)
	receiver.SetStreamHandler(proto, func(s network.Stream) {
		defer s.Close()
		b, _ := io.ReadAll(s)
		got <- b
	})

	circuit, _ := ma.NewMultiaddr("/p2p/" + r.ID().String() + "/p2p-circuit")
	addrs := []ma.Multiaddr{}
	for _, a := range r.Addrs() {
		addrs = append(addrs, a.Encapsulate(circuit))
	}
	if err := sender.Connect(ctx, peer.AddrInfo{ID: receiver.ID(), Addrs: addrs}); err != nil {
		t.Fatalf("dial through relay: %v", err)
	}
	s, err := sender.NewStream(network.WithUseTransient(ctx, "test"), receiver.ID(), proto)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	if _, err := s.Write(want); err != nil {
		t.Fatalf("write: %v", err)
	}
	s.CloseWrite()

	select {
	case b := <-got:
		if !bytes.Equal(b, want) {
			t.Fatalf("received %d bytes, want %d intact", len(b), len(want))
		}
	case <-ctx.Done():
		t.Fatal("transfer timed out")
	}
}

func TestBuildLimit(t *testing.T) {
	if buildLimit(0, 0) != nil {
		t.Fatal("no flags should mean no limit")
	}
	if l := buildLimit(time.Minute, 0); l.Data <= 0 || l.Duration != time.Minute {
		t.Fatalf("duration only: %+v", l)
	}
	if l := buildLimit(0, 1<<30); l.Duration <= 0 || l.Data != 1<<30 {
		t.Fatalf("data only: %+v", l)
	}
}
