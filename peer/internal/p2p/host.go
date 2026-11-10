// Package p2p is the peer-to-peer layer: the libp2p host and the shard
// protocols spoken between agents and clients.
package p2p

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/net/connmgr"
	"github.com/libp2p/go-libp2p/p2p/protocol/circuitv2/client"
	ma "github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr/net"
)

// HostConfig configures a libp2p host.
type HostConfig struct {
	Key           crypto.PrivKey
	ListenPort    int // 0 picks a free port; -1 does not listen (client-only)
	RelayAddr     string
	Announce      []string // replace the advertised addresses
	AllowLoopback bool     // advertise 127.0.0.1 (single-machine tests)
}

// Host wraps a libp2p host and keeps a relay reservation alive.
type Host struct {
	host.Host
	cfg   HostConfig
	relay *peer.AddrInfo

	mu      sync.Mutex
	circuit ma.Multiaddr // our address through the relay, once reserved
}

// NewHost starts a libp2p host on TCP and QUIC (IPv4 and IPv6) with a
// connection manager, NAT port mapping, hole punching and relay support.
func NewHost(cfg HostConfig) (*Host, error) {
	cm, err := connmgr.NewConnManager(100, 400, connmgr.WithGracePeriod(time.Minute))
	if err != nil {
		return nil, err
	}
	opts := []libp2p.Option{
		libp2p.Identity(cfg.Key),
		libp2p.ConnectionManager(cm),
		libp2p.NATPortMap(),
		libp2p.EnableHolePunching(),
		libp2p.EnableRelay(),
	}
	if cfg.ListenPort >= 0 {
		p := cfg.ListenPort
		opts = append(opts, libp2p.ListenAddrStrings(
			fmt.Sprintf("/ip4/0.0.0.0/tcp/%d", p),
			fmt.Sprintf("/ip6/::/tcp/%d", p),
			fmt.Sprintf("/ip4/0.0.0.0/udp/%d/quic-v1", p),
			fmt.Sprintf("/ip6/::/udp/%d/quic-v1", p),
		))
	} else {
		opts = append(opts, libp2p.NoListenAddrs)
	}

	h := &Host{cfg: cfg}
	if cfg.RelayAddr != "" {
		ai, err := peer.AddrInfoFromString(cfg.RelayAddr)
		if err != nil {
			return nil, fmt.Errorf("relay_addr: %w", err)
		}
		h.relay = ai
	}
	opts = append(opts, libp2p.AddrsFactory(h.advertised))

	lh, err := libp2p.New(opts...)
	if err != nil {
		return nil, err
	}
	h.Host = lh
	return h, nil
}

// advertised filters the addresses we tell others about and adds the relay
// circuit address.
func (h *Host) advertised(addrs []ma.Multiaddr) []ma.Multiaddr {
	var out []ma.Multiaddr
	if len(h.cfg.Announce) > 0 {
		for _, s := range h.cfg.Announce {
			if a, err := ma.NewMultiaddr(s); err == nil {
				out = append(out, a)
			}
		}
	} else {
		for _, a := range addrs {
			if manet.IsIPLoopback(a) && !h.cfg.AllowLoopback {
				continue
			}
			if manet.IsIPUnspecified(a) {
				continue
			}
			out = append(out, a)
		}
	}
	h.mu.Lock()
	if h.circuit != nil {
		out = append(out, h.circuit)
	}
	h.mu.Unlock()
	return out
}

// P2PAddrs returns our advertised addresses with /p2p/<id> appended, as they
// are registered with the server.
func (h *Host) P2PAddrs() []string {
	var out []string
	for _, a := range h.Addrs() {
		s := a.String()
		if !strings.Contains(s, "/p2p/"+h.ID().String()) {
			s += "/p2p/" + h.ID().String()
		}
		out = append(out, s)
	}
	return out
}

// KeepRelayReservation reserves a slot on the relay and renews it before it
// expires, so peers behind NAT can be reached through the relay. It returns
// when ctx ends.
func (h *Host) KeepRelayReservation(ctx context.Context) {
	if h.relay == nil {
		return
	}
	for {
		next := time.Minute
		rctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		err := h.Connect(rctx, *h.relay)
		var rsvp *client.Reservation
		if err == nil {
			rsvp, err = client.Reserve(rctx, h.Host, *h.relay)
		}
		cancel()
		if err != nil {
			log.Printf("relay: reservation failed: %v (retrying in %s)", err, next)
		} else {
			circuit, _ := ma.NewMultiaddr(h.relay.Addrs[0].String() + "/p2p/" + h.relay.ID.String() + "/p2p-circuit")
			h.mu.Lock()
			first := h.circuit == nil
			h.circuit = circuit
			h.mu.Unlock()
			if first {
				log.Printf("relay: reachable through %s", circuit)
			}
			// Renew halfway through the reservation.
			next = time.Until(rsvp.Expiration) / 2
			if next < time.Minute {
				next = time.Minute
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(next):
		}
	}
}

// Dial connects to a peer using every address we know for it. Addresses may
// be plain or carry /p2p/<id>; relay circuit addresses work too.
func (h *Host) Dial(ctx context.Context, libp2pID string, addrs []string) (peer.ID, error) {
	id, err := peer.Decode(libp2pID)
	if err != nil {
		return "", fmt.Errorf("bad peer id %q: %w", libp2pID, err)
	}
	ai := peer.AddrInfo{ID: id}
	for _, s := range addrs {
		a, err := ma.NewMultiaddr(s)
		if err != nil {
			continue
		}
		// Strip a trailing /p2p/<id> for this peer; keep relay /p2p parts.
		if tr, last := ma.SplitLast(a); last != nil && last.Protocol().Code == ma.P_P2P && last.Value() == id.String() {
			a = tr
		}
		ai.Addrs = append(ai.Addrs, a)
	}
	if len(ai.Addrs) == 0 {
		return "", fmt.Errorf("no usable address for peer %s", id)
	}
	cctx, cancel := context.WithTimeout(network.WithUseTransient(ctx, "shard transfer"), 30*time.Second)
	defer cancel()
	return id, h.Connect(cctx, ai)
}
