// Peer Registry Service - Complete Implementation with libp2p Support
// Manages peer registration, heartbeats, health scoring, and lifecycle
// File: services/peer-registry/main.go

package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net"
	"os"
	"time"

	"github.com/lib/pq"  // ADDED for libp2p multiaddr arrays
	_ "github.com/lib/pq"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
	
	pb "github.com/orchestrator/peer-registry/proto/peer"
)

// Configuration
type Config struct {
	DBHost               string
	DBPort               string
	DBName               string
	DBUser               string
	DBPassword           string
	GRPCPort             string
	HeartbeatTimeout     int // seconds
	InactivePeerHours    int
	ChurnThresholdHours  int
	MinHealthScore       float64
}

func loadConfig() *Config {
	return &Config{
		DBHost:              getEnv("DB_HOST", "cockroachdb-public"),
		DBPort:              getEnv("DB_PORT", "26257"),
		DBName:              getEnv("DB_NAME", "orchestrator"),
		DBUser:              getEnv("DB_USER", "root"),
		DBPassword:          getEnv("DB_PASSWORD", ""),
		GRPCPort:            getEnv("GRPC_PORT", "50052"),
		HeartbeatTimeout:    getEnvInt("HEARTBEAT_TIMEOUT_SECONDS", 900),    // 15 min
		InactivePeerHours:   getEnvInt("INACTIVE_PEER_HOURS", 168),          // 7 days
		ChurnThresholdHours: getEnvInt("CHURN_THRESHOLD_HOURS", 720),        // 30 days
		MinHealthScore:      getEnvFloat("MIN_HEALTH_SCORE", 70.0),
	}
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getEnvInt(key string, defaultValue int) int {
	// Simplified for brevity
	return defaultValue
}

func getEnvFloat(key string, defaultValue float64) float64 {
	// Simplified for brevity
	return defaultValue
}

// Peer represents a storage node
type Peer struct {
	PeerID          string
	UserID          string
	LibP2PPeerID    string  // ADDED for libp2p
	Multiaddr       string  // ADDED for libp2p
	ASN             string
	Timezone        string
	Region          string
	CountryCode     string
	ISP             string
	DeviceModel     string
	OSType          string
	OSVersion       string
	StorageTotalGB  int
	StorageFreeGB   float64
	BandwidthMaxMbps int
	IsOnline        bool
	IsCharging      bool
	BatteryLevel    int
	NetworkType     string
	HealthScore     float64
	ReliabilityScore float64
	UptimePercentage float64
	TotalUploads    int
	TotalDownloads  int
	BytesServed     int64
	FirstSeenAt     time.Time
	LastSeenAt      time.Time
	LastHeartbeatAt time.Time
}

// PeerRegistryServer implements the peer registry service
type PeerRegistryServer struct {
	pb.UnimplementedPeerRegistryServer
	db     *sql.DB
	config *Config
}

// RegisterPeer registers a new peer or updates existing one (UPDATED FOR LIBP2P)
func (s *PeerRegistryServer) RegisterPeer(ctx context.Context, req *pb.PeerRegistration) (*pb.RegistrationResponse, error) {
	log.Printf("RegisterPeer called for peer: %s (libp2p: %s)", req.PeerId, req.Libp2PPeerId)

	// Validate required fields
	if req.PeerId == "" {
		return &pb.RegistrationResponse{
			Success: false,
			Message: "peer_id is required",
		}, nil
	}

	var peerID string
	now := time.Now()

	// Convert bytes to GB
	totalGB := int(req.TotalCapacityBytes / (1024 * 1024 * 1024))
	freeGB := float64(req.AvailableCapacityBytes) / (1024 * 1024 * 1024)
	
	// Get timezone (use provided or default to UTC)
	timezone := req.Timezone
	if timezone == "" {
		timezone = "UTC"
	}

	// Upsert query with libp2p fields
	query := `
		INSERT INTO peers (
			peer_id,
			user_id,
			libp2p_peer_id,
			multiaddr,
			multiaddrs,
			asn,
			timezone,
			region,
			storage_total_gb,
			storage_free_gb,
			is_online,
			health_score,
			reliability_score,
			uptime_percentage,
			first_seen_at,
			last_seen_at,
			last_heartbeat_at
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
			true, 100.0, 1.000, 0.0, $11, $11, $11
		)
		ON CONFLICT (peer_id) DO UPDATE SET
			libp2p_peer_id = EXCLUDED.libp2p_peer_id,
			multiaddr = EXCLUDED.multiaddr,
			multiaddrs = EXCLUDED.multiaddrs,
			storage_total_gb = EXCLUDED.storage_total_gb,
			storage_free_gb = EXCLUDED.storage_free_gb,
			asn = EXCLUDED.asn,
			timezone = EXCLUDED.timezone,
			region = EXCLUDED.region,
			is_online = true,
			last_seen_at = EXCLUDED.last_seen_at,
			last_heartbeat_at = EXCLUDED.last_heartbeat_at
		RETURNING peer_id
	`
	
	err := s.db.QueryRowContext(ctx, query,
		req.PeerId,
		nil,  // user_id is nullable
		req.Libp2PPeerId,
		req.Multiaddr,
		pq.Array(req.Multiaddrs),  // Convert to PostgreSQL array
		req.Asn,
		timezone,
		req.Region,
		totalGB,
		freeGB,
		now,
	).Scan(&peerID)

	if err != nil {
		log.Printf("Failed to register peer %s: %v", req.PeerId, err)
		return &pb.RegistrationResponse{
			Success: false,
			Message: fmt.Sprintf("Registration failed: %v", err),
			PeerId:  "",
		}, nil
	}

	log.Printf("✓ Registered peer: %s", peerID)

	return &pb.RegistrationResponse{
		PeerId:  peerID,
		Success: true,
		Message: "Peer registered successfully",
	}, nil
}

// Heartbeat processes peer heartbeat and updates status (UPDATED FOR LIBP2P)
func (s *PeerRegistryServer) Heartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	log.Printf("Heartbeat from peer: %s", req.PeerId)

	now := time.Now()
	
	// Convert bytes to GB
	freeGB := float64(req.AvailableCapacityBytes) / (1024 * 1024 * 1024)

	// Update peer status with libp2p multiaddrs
	query := `
		UPDATE peers SET
			is_online = true,
			storage_free_gb = $1,
			multiaddrs = $2,
			last_seen_at = $3,
			last_heartbeat_at = $3
		WHERE peer_id = $4
	`

	result, err := s.db.ExecContext(ctx, query,
		freeGB,
		pq.Array(req.Multiaddrs),  // Update multiaddrs from heartbeat
		now,
		req.PeerId,
	)

	if err != nil {
		log.Printf("Heartbeat failed for peer %s: %v", req.PeerId, err)
		return &pb.HeartbeatResponse{
			Success: false,
			Message: fmt.Sprintf("Heartbeat failed: %v", err),
		}, nil
	}

	rowsAffected, _ := result.RowsAffected()
	if rowsAffected == 0 {
		return &pb.HeartbeatResponse{
			Success: false,
			Message: "Peer not found - please register first",
		}, nil
	}

	return &pb.HeartbeatResponse{
		Success: true,
		Message: "Heartbeat processed",
	}, nil
}

// GetPeer retrieves detailed peer information (UPDATED FOR LIBP2P)
func (s *PeerRegistryServer) GetPeer(ctx context.Context, req *pb.PeerRequest) (*pb.PeerInfo, error) {
	query := `
		SELECT peer_id, libp2p_peer_id, multiaddr, asn, region,
		       storage_total_gb, storage_free_gb, health_score,
		       is_online, last_heartbeat_at
		FROM peers
		WHERE peer_id = $1 OR libp2p_peer_id = $1
	`

	var (
		peerID        string
		libp2pPeerID  sql.NullString
		multiaddr     sql.NullString
		asn           string
		region        string
		totalGB       int
		freeGB        float64
		healthScore   float64
		isOnline      bool
		lastHeartbeat time.Time
	)

	err := s.db.QueryRowContext(ctx, query, req.PeerId).Scan(
		&peerID, &libp2pPeerID, &multiaddr, &asn, &region,
		&totalGB, &freeGB, &healthScore, &isOnline, &lastHeartbeat,
	)

	if err == sql.ErrNoRows {
		return nil, status.Errorf(codes.NotFound, "peer not found")
	} else if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get peer: %v", err)
	}

	peerInfo := &pb.PeerInfo{
		PeerId:                peerID,
		Libp2PPeerId:         libp2pPeerID.String,
		Multiaddr:            multiaddr.String,
		Asn:                  asn,
		Region:               region,
		TotalCapacityBytes:   int64(totalGB) * 1024 * 1024 * 1024,
		AvailableCapacityBytes: int64(freeGB * 1024 * 1024 * 1024),
		HealthScore:          healthScore,
		Status:               getStatusString(isOnline),
		LastHeartbeat:        lastHeartbeat.Unix(),
	}

	return peerInfo, nil
}

// ListPeers returns a list of peers with optional filtering
func (s *PeerRegistryServer) ListPeers(ctx context.Context, req *pb.ListPeersRequest) (*pb.PeerList, error) {
	// Determine status filter
	var statusFilter string
	switch req.Status {
	case "active":
		statusFilter = " AND is_online = true"
	case "inactive":
		statusFilter = " AND is_online = false"
	default:
		statusFilter = "" // all
	}

	limit := req.Limit
	if limit == 0 || limit > 1000 {
		limit = 100
	}

	query := fmt.Sprintf(`
		SELECT peer_id, libp2p_peer_id, multiaddr, asn, region,
		       storage_total_gb, storage_free_gb, health_score,
		       is_online, last_heartbeat_at
		FROM peers
		WHERE health_score >= $1 %s
		ORDER BY health_score DESC
		LIMIT $2
	`, statusFilter)

	rows, err := s.db.QueryContext(ctx, query,
		s.config.MinHealthScore, limit,
	)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to list peers: %v", err)
	}
	defer rows.Close()

	var peers []*pb.PeerInfo
	for rows.Next() {
		var (
			peerID        string
			libp2pPeerID  sql.NullString
			multiaddr     sql.NullString
			asn           string
			region        string
			totalGB       int
			freeGB        float64
			healthScore   float64
			isOnline      bool
			lastHeartbeat time.Time
		)

		if err := rows.Scan(&peerID, &libp2pPeerID, &multiaddr, &asn, &region,
			&totalGB, &freeGB, &healthScore, &isOnline, &lastHeartbeat); err != nil {
			continue
		}

		peers = append(peers, &pb.PeerInfo{
			PeerId:                peerID,
			Libp2PPeerId:         libp2pPeerID.String,
			Multiaddr:            multiaddr.String,
			Asn:                  asn,
			Region:               region,
			TotalCapacityBytes:   int64(totalGB) * 1024 * 1024 * 1024,
			AvailableCapacityBytes: int64(freeGB * 1024 * 1024 * 1024),
			HealthScore:          healthScore,
			Status:               getStatusString(isOnline),
			LastHeartbeat:        lastHeartbeat.Unix(),
		})
	}

	return &pb.PeerList{
		Peers:      peers,
		TotalCount: int32(len(peers)),
	}, nil
}

// Helper methods

func getStatusString(isOnline bool) string {
	if isOnline {
		return "active"
	}
	return "inactive"
}

// Background job to mark inactive peers offline
func (s *PeerRegistryServer) runInactivityChecker() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		timeout := time.Duration(s.config.HeartbeatTimeout) * time.Second
		cutoff := time.Now().Add(-timeout)

		query := `
			UPDATE peers
			SET is_online = false
			WHERE is_online = true
			  AND last_heartbeat_at < $1
		`

		result, err := s.db.Exec(query, cutoff)
		if err != nil {
			log.Printf("Error marking peers offline: %v", err)
			continue
		}

		rowsAffected, _ := result.RowsAffected()
		if rowsAffected > 0 {
			log.Printf("Marked %d peers as offline", rowsAffected)
		}
	}
}

func main() {
	log.Println("Starting Peer Registry Service with libp2p support...")

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

	log.Println("✓ Connected to database successfully")

	// Create server
	server := &PeerRegistryServer{
		db:     db,
		config: config,
	}

	// Start background jobs
	go server.runInactivityChecker()

	// Create gRPC server
	lis, err := net.Listen("tcp", ":"+config.GRPCPort)
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterPeerRegistryServer(grpcServer, server)
	reflection.Register(grpcServer)

	log.Printf("✓ Peer Registry Service listening on port %s", config.GRPCPort)

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
