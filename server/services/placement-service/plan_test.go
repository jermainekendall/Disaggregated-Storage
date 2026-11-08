package main

import (
	"fmt"
	"math/rand/v2"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var prodCfg = Config{
	MinShardCount: 10, MaxShardCount: 100, MaxShardSizeGB: 5, ParityShards: 4,
	MinHealthScore: 70, MaxPeersPerASN: 3, MinASNsPerFile: 5, CapacityHeadroom: 1.2,
}

func peers(n, asns int, freeGB float64) []Candidate {
	out := make([]Candidate, n)
	for i := range out {
		out[i] = Candidate{
			PeerID: fmt.Sprintf("p%02d", i), ASN: fmt.Sprintf("AS%d", i%asns),
			Multiaddr: "/ip4/10.0.0.1/tcp/4001", FreeGB: freeGB, HealthScore: 100,
		}
	}
	return out
}

func seeded() *rand.Rand { return rand.New(rand.NewPCG(1, 2)) }

func code(err error) codes.Code { return status.Code(err) }

func TestPlanDocExample(t *testing.T) {
	// 20 GB file -> 10 data shards of 2 GB + 4 parity = 14 peers.
	p, err := Plan(prodCfg, 20*bytesPerGB, peers(30, 10, 100), seeded())
	if err != nil {
		t.Fatal(err)
	}
	if p.DataShards != 10 || p.ParityShards != 4 || len(p.Peers) != 14 {
		t.Fatalf("got %d+%d on %d peers", p.DataShards, p.ParityShards, len(p.Peers))
	}
	if p.ShardSizeBytes != 2*bytesPerGB {
		t.Fatalf("shard size %d", p.ShardSizeBytes)
	}
}

func TestPlanDoublesForLargeFiles(t *testing.T) {
	// 120 GB: 10 -> 20 -> 40 shards of 3 GB.
	p, err := Plan(prodCfg, 120*bytesPerGB, peers(60, 20, 100), seeded())
	if err != nil {
		t.Fatal(err)
	}
	if p.DataShards != 40 {
		t.Fatalf("data shards = %d, want 40", p.DataShards)
	}
}

func TestPlanRejectsOversizedFile(t *testing.T) {
	_, err := Plan(prodCfg, 600*bytesPerGB, peers(200, 50, 100), seeded())
	if code(err) != codes.InvalidArgument {
		t.Fatalf("want InvalidArgument, got %v", err)
	}
}

func TestPlanReducesShardsWhenFewPeers(t *testing.T) {
	// 120 GB wants 40 data shards (44 peers); with 30 peers it drops to
	// 26 data + 4 parity, and shards grow to about 4.6 GB.
	p, err := Plan(prodCfg, 120*bytesPerGB, peers(30, 15, 100), seeded())
	if err != nil {
		t.Fatal(err)
	}
	if p.DataShards != 26 || len(p.Peers) != 30 {
		t.Fatalf("got %d data on %d peers", p.DataShards, len(p.Peers))
	}

	// With only 20 peers, 16 data shards would be 7.5 GB each: refuse.
	if _, err := Plan(prodCfg, 120*bytesPerGB, peers(20, 10, 100), seeded()); code(err) != codes.FailedPrecondition {
		t.Fatalf("want FailedPrecondition, got %v", err)
	}
}

func TestPlanFailsBelowMinimum(t *testing.T) {
	_, err := Plan(prodCfg, bytesPerGB, peers(13, 13, 100), seeded())
	if code(err) != codes.FailedPrecondition {
		t.Fatalf("want FailedPrecondition, got %v", err)
	}
}

func TestPlanChecksCapacityWithHeadroom(t *testing.T) {
	// 10 GB / 10 shards = 1 GB each, needing 1.2 GB free. Peers have 1.1 GB.
	_, err := Plan(prodCfg, 10*bytesPerGB, peers(20, 10, 1.1), seeded())
	if code(err) != codes.FailedPrecondition {
		t.Fatalf("want FailedPrecondition, got %v", err)
	}
}

func TestPlanEnforcesASNRules(t *testing.T) {
	p, err := Plan(prodCfg, bytesPerGB, peers(40, 8, 100), seeded())
	if err != nil {
		t.Fatal(err)
	}
	per := map[string]int{}
	for _, c := range p.Peers {
		per[c.ASN]++
	}
	for asn, n := range per {
		if n > 3 {
			t.Errorf("%s holds %d shards, cap is 3", asn, n)
		}
	}
	if len(per) < 5 {
		t.Errorf("shards span %d ASNs, need 5", len(per))
	}

	// 14 shards cannot fit in 4 ASNs at 3 per ASN.
	if _, err := Plan(prodCfg, bytesPerGB, peers(40, 4, 100), seeded()); code(err) != codes.FailedPrecondition {
		t.Fatalf("want FailedPrecondition for too few ASNs, got %v", err)
	}
}

func TestPlanNoDuplicatePeers(t *testing.T) {
	p, err := Plan(prodCfg, bytesPerGB, peers(14, 14, 100), seeded())
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, c := range p.Peers {
		if seen[c.PeerID] {
			t.Fatalf("peer %s chosen twice", c.PeerID)
		}
		seen[c.PeerID] = true
	}
}

func TestPlanSpreadsLoad(t *testing.T) {
	// With equal health, 100 placements of 14 shards over 50 peers should touch
	// nearly every peer (the old ORDER BY health_score picked the same 14).
	cs := peers(50, 25, 100)
	hits := map[string]int{}
	rng := seeded()
	for i := 0; i < 100; i++ {
		p, err := Plan(prodCfg, bytesPerGB, cs, rng)
		if err != nil {
			t.Fatal(err)
		}
		for _, c := range p.Peers {
			hits[c.PeerID]++
		}
	}
	if len(hits) < 45 {
		t.Fatalf("only %d of 50 peers used", len(hits))
	}
}
