// Placement Service - Complete Fresh Implementation
// File: main.go

package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"time"

	_ "github.com/lib/pq"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	
	pb "github.com/orchestrator/placement-service/proto/placement"
)

// Configuration
const (
	MinShardCount       = 10
	MaxShardCount       = 100
	MaxShardSizeGB      = 5.0
	DefaultParityShards = 4
)

type Config struct {
	DBHost     string
	DBPort     string
	DBName     string
	DBUser     string
	DBPassword string
	GRPCPort   string
}

func loadConfig() *Config {
	return &Config{
		DBHost:     getEnv("DB_HOST", "cockroachdb-public"),
		DBPort:     getEnv("DB_PORT", "26257"),
		DBName:     getEnv("DB_NAME", "orchestrator"),
		DBUser:     getEnv("DB_USER", "root"),
		DBPassword: getEnv("DB_PASSWORD", ""),
		GRPCPort:   getEnv("GRPC_PORT", "50052"),
	}
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

type Peer struct {
	PeerID        string
	ASN           string
	Region        string
	StorageFreeGB float64
	HealthScore   float64
	Timezone      int
}

type PlacementServer struct {
	pb.UnimplementedPlacementServiceServer
	db     *sql.DB
	config *Config
}

// SelectPeersForUpload selects optimal peers for file storage
func (s *PlacementServer) SelectPeersForUpload(ctx context.Context, req *pb.UploadRequest) (*pb.PeerSelection, error) {
	log.Printf("SelectPeersForUpload called for file: %s, size: %d bytes", req.FileId, req.FileSizeBytes)

	// Calculate shard strategy
	fileSizeGB := float64(req.FileSizeBytes) / 1073741824.0
	shardCount := MinShardCount
	
	// Increase shard count for large files
	for fileSizeGB/float64(shardCount) > MaxShardSizeGB && shardCount < MaxShardCount {
		shardCount *= 2
	}
	if shardCount > MaxShardCount {
		shardCount = MaxShardCount
	}

	totalShards := shardCount + DefaultParityShards

	log.Printf("Adaptive sharding: %d data + %d parity = %d total shards", 
		shardCount, DefaultParityShards, totalShards)

	// Get candidate peers
	peers, err := s.getCandidatePeers(ctx, totalShards*2)
	if err != nil {
		return nil, fmt.Errorf("failed to get peers: %v", err)
	}

	if len(peers) < totalShards {
		return nil, fmt.Errorf("insufficient peers: need %d, have %d", totalShards, len(peers))
	}

	// Select best peers (top N by health score)
	selectedPeers := peers[:totalShards]

	// Build response
	response := &pb.PeerSelection{
		FileId:       req.FileId,
		ShardCount:   int32(shardCount),
		ParityShards: int32(DefaultParityShards),
		Peers:        make([]*pb.SelectedPeer, totalShards),
	}

	for i, peer := range selectedPeers {
		shardType := "data"
		if i >= shardCount {
			shardType = "parity"
		}

		response.Peers[i] = &pb.SelectedPeer{
			PeerId:      peer.PeerID,
			ShardIndex:  int32(i),
			ShardType:   shardType,
			PeerAddress: fmt.Sprintf("peer-%s:9000", peer.PeerID),
			HealthScore: peer.HealthScore,
			Asn:         peer.ASN,
			Region:      peer.Region,
		}
	}

	log.Printf("Selected %d peers for file %s", len(selectedPeers), req.FileId)
	return response, nil
}

// getCandidatePeers retrieves peers from database
func (s *PlacementServer) getCandidatePeers(ctx context.Context, count int) ([]*Peer, error) {
	query := `
		SELECT peer_id, asn, region, storage_free_gb, health_score, timezone
		FROM peers
		WHERE is_online = true AND health_score > 70
		ORDER BY health_score DESC, RANDOM()
		LIMIT $1
	`

	rows, err := s.db.QueryContext(ctx, query, count)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var peers []*Peer
	for rows.Next() {
		peer := &Peer{}
		err := rows.Scan(&peer.PeerID, &peer.ASN, &peer.Region,
			&peer.StorageFreeGB, &peer.HealthScore, &peer.Timezone)
		if err != nil {
			continue
		}
		peers = append(peers, peer)
	}

	return peers, nil
}

func main() {
	log.Println("Starting Placement Service...")

	config := loadConfig()

	// Connect to database
	connStr := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		config.DBHost, config.DBPort, config.DBUser, config.DBPassword, config.DBName)

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		log.Fatalf("Failed to ping database: %v", err)
	}

	log.Println("Connected to database successfully")

	// Create server
	server := &PlacementServer{
		db:     db,
		config: config,
	}

	// Seed random
	rand.Seed(time.Now().UnixNano())

	// Create gRPC server
	lis, err := net.Listen("tcp", ":"+config.GRPCPort)
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterPlacementServiceServer(grpcServer, server)
	reflection.Register(grpcServer)

	log.Printf("Placement Service listening on port %s", config.GRPCPort)

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
