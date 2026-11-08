// libp2p circuit relay (v2) server for peers that cannot accept inbound
// connections. It is a relay *server*: it does not use AutoRelay.
//
// Identity: the relay's peer ID is part of every relay address peers store
// (/ip4/.../tcp/30001/p2p/<relay-id>/p2p-circuit), so it must survive
// restarts. The private key is read from KEY_FILE; generate one with
//
//	relay-service -genkey > relay.key
//
// and mount it from a Kubernetes Secret.
package main

import (
	"encoding/base64"
	"errors"
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/p2p/protocol/circuitv2/relay"
	"github.com/multiformats/go-multiaddr"

	"github.com/jermainekendall/disaggregated-storage/server/internal/common"
)

func main() {
	genKey := flag.Bool("genkey", false, "print a new base64 private key and exit")
	port := flag.Int("port", common.EnvInt("PORT", 4001), "listen port")
	keyFile := flag.String("key-file", common.Env("KEY_FILE", ""), "file holding the base64 private key")
	announce := flag.String("announce", common.Env("ANNOUNCE_ADDRS", ""),
		"comma-separated public multiaddrs to advertise, e.g. /ip4/203.0.113.7/tcp/30001")
	maxRes := flag.Int("max-reservations", common.EnvInt("MAX_RESERVATIONS", 512), "max reservations")
	maxCirc := flag.Int("max-circuits", common.EnvInt("MAX_CIRCUITS", 256), "max concurrent circuits")
	ttl := flag.Duration("reservation-ttl", common.EnvDuration("RESERVATION_TTL", time.Hour), "reservation TTL")
	limitDur := flag.Duration("limit-duration", common.EnvDuration("CIRCUIT_LIMIT_DURATION", 0),
		"max lifetime of one relayed connection, 0 for no limit")
	limitData := flag.Int64("limit-data", int64(common.EnvInt("CIRCUIT_LIMIT_BYTES", 0)),
		"max bytes per relayed connection in each direction, 0 for no limit")
	flag.Parse()

	if *genKey {
		priv, _, err := crypto.GenerateEd25519Key(nil)
		if err != nil {
			log.Fatal(err)
		}
		raw, err := crypto.MarshalPrivateKey(priv)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(base64.StdEncoding.EncodeToString(raw))
		return
	}

	priv, err := loadKey(*keyFile)
	if err != nil {
		log.Fatalf("identity: %v", err)
	}

	announceAddrs, err := parseAddrs(*announce)
	if err != nil {
		log.Fatalf("announce addresses: %v", err)
	}
	limit := buildLimit(*limitDur, *limitData)
	h, err := newRelay(priv, fmt.Sprintf("/ip4/0.0.0.0/tcp/%d", *port), announceAddrs, relay.Resources{
		Limit:                  limit,
		ReservationTTL:         *ttl,
		MaxReservations:        *maxRes,
		MaxCircuits:            *maxCirc,
		BufferSize:             64 << 10,
		MaxReservationsPerPeer: 4,
		MaxReservationsPerIP:   8,
		MaxReservationsPerASN:  64,
	})
	if err != nil {
		log.Fatalf("start relay: %v", err)
	}
	defer h.Close()

	log.Printf("relay %s up: max_reservations=%d max_circuits=%d ttl=%s limit=%s",
		h.ID(), *maxRes, *maxCirc, *ttl, describeLimit(limit))
	for _, a := range h.Addrs() {
		log.Printf("  peers reserve at %s/p2p/%s", a, h.ID())
	}

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	tick := time.NewTicker(time.Minute)
	defer tick.Stop()
	for {
		select {
		case <-tick.C:
			log.Printf("%d peers, %d connections", len(h.Network().Peers()), len(h.Network().Conns()))
		case <-stop:
			log.Println("shutting down")
			return
		}
	}
}

// buildLimit turns the flags into a relay limit. The libp2p defaults
// (2 minutes, 128 KiB) are meant for hole-punch coordination; shards are
// large, so there is no limit unless one is configured. A zero in RelayLimit
// means "nothing allowed", so an unset half of the pair is made unlimited.
func buildLimit(d time.Duration, data int64) *relay.RelayLimit {
	if d <= 0 && data <= 0 {
		return nil
	}
	l := &relay.RelayLimit{Duration: d, Data: data}
	if l.Duration <= 0 {
		l.Duration = 365 * 24 * time.Hour
	}
	if l.Data <= 0 {
		l.Data = math.MaxInt64
	}
	return l
}

// newRelay starts a libp2p host running the circuit v2 relay service.
func newRelay(priv crypto.PrivKey, listen string, announce []multiaddr.Multiaddr, res relay.Resources) (host.Host, error) {
	opts := []libp2p.Option{
		libp2p.Identity(priv),
		libp2p.ListenAddrStrings(listen),
		// Lets peers ask whether they are publicly reachable (AutoNAT).
		libp2p.EnableNATService(),
		// The relay runs behind a NodePort, so it is public by configuration.
		libp2p.ForceReachabilityPublic(),
	}
	if len(announce) > 0 {
		opts = append(opts, libp2p.AddrsFactory(func([]multiaddr.Multiaddr) []multiaddr.Multiaddr { return announce }))
	}
	h, err := libp2p.New(opts...)
	if err != nil {
		return nil, err
	}
	if _, err := relay.New(h, relay.WithResources(res)); err != nil {
		h.Close()
		return nil, err
	}
	return h, nil
}

// loadKey reads the relay key. With no key file it generates a throwaway key
// and warns, which is fine for a quick local run but changes the peer ID on
// every restart.
func loadKey(path string) (crypto.PrivKey, error) {
	if path == "" {
		log.Println("WARNING: KEY_FILE not set, using a random identity; relay addresses will change on restart")
		priv, _, err := crypto.GenerateEd25519Key(nil)
		return priv, err
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	raw, err := base64.StdEncoding.DecodeString(strings.TrimSpace(string(data)))
	if err != nil {
		return nil, fmt.Errorf("%s is not base64 (generate with -genkey): %w", path, err)
	}
	return crypto.UnmarshalPrivateKey(raw)
}

func parseAddrs(list string) ([]multiaddr.Multiaddr, error) {
	var out []multiaddr.Multiaddr
	for _, s := range strings.Split(list, ",") {
		if s = strings.TrimSpace(s); s == "" {
			continue
		}
		a, err := multiaddr.NewMultiaddr(s)
		if err != nil {
			return nil, fmt.Errorf("%q: %w", s, err)
		}
		out = append(out, a)
	}
	if len(out) == 0 && list != "" {
		return nil, errors.New("no valid addresses")
	}
	return out, nil
}

func describeLimit(l *relay.RelayLimit) string {
	if l == nil {
		return "none"
	}
	return fmt.Sprintf("%s/%d bytes", l.Duration, l.Data)
}
