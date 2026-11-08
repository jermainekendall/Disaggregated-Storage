// Placement Service: decides the erasure code for a new file and which peers
// hold its shards.
package main

import (
	"context"
	"database/sql"
	"log"

	"github.com/lib/pq"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/jermainekendall/disaggregated-storage/server/internal/common"
	pb "github.com/jermainekendall/disaggregated-storage/server/proto/placement"
)

var requiredSchema = map[string][]string{
	"peers": {"peer_id", "libp2p_peer_id", "multiaddr", "multiaddrs", "asn", "region", "timezone",
		"storage_free_gb", "health_score", "is_online"},
}

type server struct {
	pb.UnimplementedPlacementServiceServer
	db  *sql.DB
	cfg Config
}

func loadConfig() Config {
	return Config{
		MinShardCount:    common.EnvInt("MIN_SHARD_COUNT", 10),
		MaxShardCount:    common.EnvInt("MAX_SHARD_COUNT", 100),
		MaxShardSizeGB:   common.EnvFloat("MAX_SHARD_SIZE_GB", 5),
		ParityShards:     common.EnvInt("PARITY_SHARDS", 4),
		MinHealthScore:   common.EnvFloat("MIN_HEALTH_SCORE", 70),
		MaxPeersPerASN:   common.EnvInt("MAX_PEERS_PER_ASN", 3),
		MinASNsPerFile:   common.EnvInt("MIN_ASNS_PER_FILE", 5),
		CapacityHeadroom: common.EnvFloat("CAPACITY_HEADROOM", 1.2),
	}
}

// SelectPeersForUpload picks one peer per shard for a new file.
func (s *server) SelectPeersForUpload(ctx context.Context, req *pb.UploadRequest) (*pb.PeerSelection, error) {
	if req.FileSizeBytes <= 0 {
		return nil, status.Error(codes.InvalidArgument, "file_size_bytes must be positive")
	}
	candidates, err := s.loadCandidates(ctx, req.OwnerPeerId)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "load peers: %v", err)
	}

	plan, err := Plan(s.cfg, req.FileSizeBytes, candidates, nil)
	if err != nil {
		log.Printf("placement for file %s (%d bytes) failed: %v", req.FileId, req.FileSizeBytes, err)
		return nil, err
	}

	resp := &pb.PeerSelection{
		FileId:         req.FileId,
		ShardCount:     int32(plan.DataShards),
		ParityShards:   int32(plan.ParityShards),
		ShardSizeBytes: plan.ShardSizeBytes,
	}
	for i, p := range plan.Peers {
		shardType := "data"
		if i >= plan.DataShards {
			shardType = "parity"
		}
		resp.Peers = append(resp.Peers, &pb.SelectedPeer{
			PeerId:       p.PeerID,
			ShardIndex:   int32(i),
			ShardType:    shardType,
			PeerAddress:  p.Multiaddr,
			HealthScore:  p.HealthScore,
			Asn:          p.ASN,
			Region:       p.Region,
			Libp2PPeerId: p.Libp2pID,
			Multiaddrs:   p.Multiaddrs,
			Timezone:     p.Timezone,
		})
	}
	log.Printf("file %s: %d data + %d parity shards of %d bytes on %d peers across %d ASNs",
		req.FileId, plan.DataShards, plan.ParityShards, plan.ShardSizeBytes, len(plan.Peers), plan.DistinctASNs)
	return resp, nil
}

// loadCandidates returns online, healthy, dialable peers other than the owner.
// Capacity is filtered in Plan, because the shard size depends on how many
// peers are available.
func (s *server) loadCandidates(ctx context.Context, owner string) ([]Candidate, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT peer_id, COALESCE(libp2p_peer_id, ''), multiaddr, multiaddrs, asn, region, timezone,
		       storage_free_gb, health_score
		FROM peers
		WHERE is_online = true
		  AND health_score >= $1
		  AND multiaddr IS NOT NULL
		  AND peer_id::STRING != $2
		  AND COALESCE(libp2p_peer_id, '') != $2
		LIMIT 5000`, s.cfg.MinHealthScore, owner)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []Candidate
	for rows.Next() {
		var c Candidate
		if err := rows.Scan(&c.PeerID, &c.Libp2pID, &c.Multiaddr, pq.Array(&c.Multiaddrs), &c.ASN,
			&c.Region, &c.Timezone, &c.FreeGB, &c.HealthScore); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

func main() {
	ctx, stop := common.SignalContext()
	defer stop()

	cfg := loadConfig()
	if err := cfg.Validate(); err != nil {
		log.Fatalf("placement config: %v", err)
	}
	db := common.MustOpenDB(ctx, requiredSchema)
	defer db.Close()

	s := &server{db: db, cfg: cfg}
	common.ServeGRPC(ctx, "placement-service", common.Env("GRPC_PORT", "50052"), func(g *grpc.Server) {
		pb.RegisterPlacementServiceServer(g, s)
	})
}
