package main

import (
	"fmt"
	"math"
	"math/rand/v2"
	"sort"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const bytesPerGB = 1 << 30

// Config controls the erasure code and the diversity rules.
type Config struct {
	MinShardCount    int     // fewest data shards a file may use
	MaxShardCount    int     // most data shards a file may use
	MaxShardSizeGB   float64 // data shards are made at most this large
	ParityShards     int
	MinHealthScore   float64
	MaxPeersPerASN   int     // 0 disables the cap
	MinASNsPerFile   int     // 0 or 1 disables the requirement
	CapacityHeadroom float64 // a peer needs shard size x this much free space
}

// Validate rejects settings that can never produce a placement.
func (c Config) Validate() error {
	switch {
	case c.MinShardCount < 1:
		return fmt.Errorf("MIN_SHARD_COUNT must be at least 1")
	case c.MaxShardCount < c.MinShardCount:
		return fmt.Errorf("MAX_SHARD_COUNT (%d) is below MIN_SHARD_COUNT (%d)", c.MaxShardCount, c.MinShardCount)
	case c.ParityShards < 1:
		return fmt.Errorf("PARITY_SHARDS must be at least 1")
	case c.MaxShardSizeGB <= 0:
		return fmt.Errorf("MAX_SHARD_SIZE_GB must be positive")
	case c.CapacityHeadroom < 1:
		return fmt.Errorf("CAPACITY_HEADROOM must be at least 1")
	}
	return nil
}

// Candidate is a peer that could hold a shard.
type Candidate struct {
	PeerID      string
	Libp2pID    string
	Multiaddr   string
	Multiaddrs  []string
	ASN         string
	Region      string
	Timezone    string
	FreeGB      float64
	HealthScore float64
}

// Placement is the chosen code and one peer per shard (data shards first).
type Placement struct {
	DataShards     int
	ParityShards   int
	ShardSizeBytes int64
	Peers          []Candidate
	DistinctASNs   int
}

// Plan chooses the shard count and the peers for a file of size bytes.
//
// The data shard count starts at MinShardCount and doubles (up to
// MaxShardCount) until each shard fits in MaxShardSizeGB. If there are too few
// peers with room for a shard, the count drops to (peers - parity), which makes
// shards bigger, so capacity is checked again until the numbers agree. It
// fails rather than go below MinShardCount.
//
// rng may be nil; tests pass a seeded one.
func Plan(cfg Config, size int64, candidates []Candidate, rng *rand.Rand) (*Placement, error) {
	if rng == nil {
		rng = rand.New(rand.NewPCG(rand.Uint64(), rand.Uint64()))
	}
	sizeGB := float64(size) / bytesPerGB

	data := cfg.MinShardCount
	for sizeGB/float64(data) > cfg.MaxShardSizeGB && data < cfg.MaxShardCount {
		data *= 2
	}
	if data > cfg.MaxShardCount {
		data = cfg.MaxShardCount
	}
	if sizeGB/float64(data) > cfg.MaxShardSizeGB {
		return nil, status.Errorf(codes.InvalidArgument,
			"file of %.1f GB needs shards larger than %.1f GB even with %d data shards",
			sizeGB, cfg.MaxShardSizeGB, cfg.MaxShardCount)
	}

	var eligible []Candidate
	for {
		shardBytes := ceilDiv(size, int64(data))
		need := float64(shardBytes) / bytesPerGB * cfg.CapacityHeadroom
		eligible = eligible[:0]
		for _, c := range candidates {
			if c.FreeGB >= need {
				eligible = append(eligible, c)
			}
		}
		if len(eligible) >= data+cfg.ParityShards {
			break
		}
		reduced := len(eligible) - cfg.ParityShards
		if reduced < cfg.MinShardCount {
			return nil, status.Errorf(codes.FailedPrecondition,
				"insufficient peers: need at least %d (%d data + %d parity) with %.2f GB free, have %d",
				cfg.MinShardCount+cfg.ParityShards, cfg.MinShardCount, cfg.ParityShards, need, len(eligible))
		}
		if reduced >= data { // cannot happen, but never loop forever
			return nil, status.Error(codes.Internal, "placement did not converge")
		}
		if sizeGB/float64(reduced) > cfg.MaxShardSizeGB {
			return nil, status.Errorf(codes.FailedPrecondition,
				"insufficient peers: %d eligible, but %.1f GB in %d data shards exceeds %.1f GB per shard",
				len(eligible), sizeGB, reduced, cfg.MaxShardSizeGB)
		}
		data = reduced
	}

	total := data + cfg.ParityShards
	chosen, asns, err := pickDiverse(cfg, eligible, total, rng)
	if err != nil {
		return nil, err
	}
	return &Placement{
		DataShards:     data,
		ParityShards:   cfg.ParityShards,
		ShardSizeBytes: ceilDiv(size, int64(data)),
		Peers:          chosen,
		DistinctASNs:   asns,
	}, nil
}

// pickDiverse picks n peers in a random order weighted by health score, so
// healthier peers are preferred without every file landing on the same few
// peers. It first takes one peer from each new ASN until MinASNsPerFile is met,
// then fills the rest, never exceeding MaxPeersPerASN in one ASN.
func pickDiverse(cfg Config, eligible []Candidate, n int, rng *rand.Rand) ([]Candidate, int, error) {
	// Efraimidis-Spirakis weighted shuffle: sort by u^(1/w) descending.
	type keyed struct {
		c   Candidate
		key float64
	}
	order := make([]keyed, len(eligible))
	for i, c := range eligible {
		w := math.Max(c.HealthScore, 1)
		order[i] = keyed{c, math.Pow(rng.Float64(), 1/w)}
	}
	sort.Slice(order, func(i, j int) bool { return order[i].key > order[j].key })

	perASN := map[string]int{}
	used := make([]bool, len(order))
	var chosen []Candidate
	take := func(i int) {
		used[i] = true
		perASN[order[i].c.ASN]++
		chosen = append(chosen, order[i].c)
	}

	if cfg.MinASNsPerFile > 1 {
		for i := range order {
			if len(perASN) >= cfg.MinASNsPerFile || len(chosen) == n {
				break
			}
			if perASN[order[i].c.ASN] == 0 {
				take(i)
			}
		}
	}
	for i := range order {
		if len(chosen) == n {
			break
		}
		if used[i] || (cfg.MaxPeersPerASN > 0 && perASN[order[i].c.ASN] >= cfg.MaxPeersPerASN) {
			continue
		}
		take(i)
	}

	if len(chosen) < n {
		return nil, 0, status.Errorf(codes.FailedPrecondition,
			"cannot place %d shards with at most %d per ASN: only %d eligible peers across %d ASNs",
			n, cfg.MaxPeersPerASN, len(eligible), countASNs(eligible))
	}
	if cfg.MinASNsPerFile > 1 && len(perASN) < cfg.MinASNsPerFile {
		return nil, 0, status.Errorf(codes.FailedPrecondition,
			"need shards in at least %d ASNs, eligible peers span only %d", cfg.MinASNsPerFile, len(perASN))
	}
	return chosen, len(perASN), nil
}

func countASNs(cs []Candidate) int {
	seen := map[string]bool{}
	for _, c := range cs {
		seen[c.ASN] = true
	}
	return len(seen)
}

func ceilDiv(a, b int64) int64 { return (a + b - 1) / b }
