// Peer Registry: peer registration, heartbeats, lookup and liveness.
package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/lib/pq"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/jermainekendall/disaggregated-storage/server/internal/common"
	pb "github.com/jermainekendall/disaggregated-storage/server/proto/peer"
)

const gb = 1024 * 1024 * 1024

type config struct {
	GRPCPort         string
	HeartbeatTimeout time.Duration
	CheckInterval    time.Duration
}

func loadConfig() config {
	return config{
		GRPCPort:         common.Env("GRPC_PORT", "50051"),
		HeartbeatTimeout: common.EnvDuration("HEARTBEAT_TIMEOUT_SECONDS", 15*time.Minute),
		CheckInterval:    common.EnvDuration("INACTIVITY_CHECK_INTERVAL", time.Minute),
	}
}

var requiredSchema = map[string][]string{
	"peers": {"peer_id", "user_id", "libp2p_peer_id", "multiaddr", "multiaddrs", "asn", "timezone",
		"region", "device_model", "os_type", "app_version", "storage_total_gb", "storage_free_gb",
		"is_online", "is_charging", "battery_level", "network_type", "health_score",
		"last_seen_at", "last_heartbeat_at", "updated_at"},
}

type server struct {
	pb.UnimplementedPeerRegistryServer
	db  *sql.DB
	cfg config
}

// idColumn picks the column to match a caller-supplied peer identifier
// against: server UUIDs match peer_id, anything else is a libp2p peer ID.
// Comparing a non-UUID string with the UUID column would be a type error.
func idColumn(id string) string {
	if common.IsUUID(id) {
		return "peer_id"
	}
	return "libp2p_peer_id"
}

// RegisterPeer creates a peer or updates an existing one. New peers usually
// send only libp2p_peer_id; the server assigns peer_id and returns it.
func (s *server) RegisterPeer(ctx context.Context, req *pb.PeerRegistration) (*pb.RegistrationResponse, error) {
	fail := func(msg string) (*pb.RegistrationResponse, error) {
		log.Printf("RegisterPeer rejected (peer_id=%q libp2p=%q): %s", req.PeerId, req.Libp2PPeerId, msg)
		return &pb.RegistrationResponse{Success: false, Message: msg}, nil
	}

	req.PeerId = strings.TrimSpace(req.PeerId)
	req.Libp2PPeerId = strings.TrimSpace(req.Libp2PPeerId)
	switch {
	case req.PeerId == "" && req.Libp2PPeerId == "":
		return fail("peer_id or libp2p_peer_id is required")
	case req.PeerId != "" && !common.IsUUID(req.PeerId):
		return fail("peer_id must be the UUID returned by a previous registration; send a libp2p ID in libp2p_peer_id")
	case req.UserId != "" && !common.IsUUID(req.UserId):
		return fail("user_id must be a UUID")
	case req.TotalCapacityBytes < 0 || req.AvailableCapacityBytes < 0:
		return fail("capacity cannot be negative")
	case req.AvailableCapacityBytes > req.TotalCapacityBytes && req.TotalCapacityBytes > 0:
		return fail("available capacity exceeds total capacity")
	}

	// Empty location fields keep the stored values on re-registration (new
	// peers get '' / UTC defaults in the INSERT).
	tz := req.Timezone
	if tz != "" {
		if _, err := time.LoadLocation(tz); err != nil {
			return fail(fmt.Sprintf("timezone %q is not an IANA name (e.g. America/Guyana)", tz))
		}
	}
	primary := req.Multiaddr
	if primary == "" && len(req.Multiaddrs) > 0 {
		primary = req.Multiaddrs[0]
	}

	// Conflict on peer_id when the client already has one, otherwise on the
	// libp2p identity, so re-registering never creates a duplicate row.
	conflict := "peer_id"
	peerID := sql.NullString{String: req.PeerId, Valid: req.PeerId != ""}
	if !peerID.Valid {
		conflict = "libp2p_peer_id"
	}
	query := fmt.Sprintf(`
		INSERT INTO peers (
			peer_id, user_id, libp2p_peer_id, multiaddr, multiaddrs, asn, timezone, region,
			device_model, os_type, app_version, storage_total_gb, storage_free_gb,
			is_online, first_seen_at, last_seen_at, last_heartbeat_at, updated_at
		) VALUES (
			COALESCE($1::UUID, gen_random_uuid()), $2, $3, $4, $5,
			COALESCE($6, ''), COALESCE($7, 'UTC'), COALESCE($8, ''),
			$9, $10, $11, $12, $13, true, now(), now(), now(), now()
		)
		ON CONFLICT (%s) DO UPDATE SET
			user_id           = COALESCE(excluded.user_id, peers.user_id),
			libp2p_peer_id    = COALESCE(excluded.libp2p_peer_id, peers.libp2p_peer_id),
			multiaddr         = COALESCE(excluded.multiaddr, peers.multiaddr),
			multiaddrs        = COALESCE(excluded.multiaddrs, peers.multiaddrs),
			asn               = COALESCE($6, peers.asn),
			timezone          = COALESCE($7, peers.timezone),
			region            = COALESCE($8, peers.region),
			device_model      = COALESCE(excluded.device_model, peers.device_model),
			os_type           = COALESCE(excluded.os_type, peers.os_type),
			app_version       = COALESCE(excluded.app_version, peers.app_version),
			storage_total_gb  = excluded.storage_total_gb,
			storage_free_gb   = excluded.storage_free_gb,
			is_online         = true,
			last_seen_at      = now(),
			last_heartbeat_at = now(),
			updated_at        = now()
		RETURNING peer_id`, conflict)

	var multiaddrs interface{}
	if len(req.Multiaddrs) > 0 {
		multiaddrs = pq.Array(req.Multiaddrs)
	}
	var id string
	err := s.db.QueryRowContext(ctx, query,
		peerID,
		common.NullIfEmpty(req.UserId),
		common.NullIfEmpty(req.Libp2PPeerId),
		common.NullIfEmpty(primary),
		multiaddrs,
		common.NullIfEmpty(req.Asn), common.NullIfEmpty(tz), common.NullIfEmpty(req.Region),
		common.NullIfEmpty(req.DeviceModel), common.NullIfEmpty(req.OsType), common.NullIfEmpty(req.Version),
		req.TotalCapacityBytes/gb,
		float64(req.AvailableCapacityBytes)/gb,
	).Scan(&id)
	if common.IsUniqueViolation(err) {
		return fail("libp2p_peer_id is already registered to a different peer_id")
	}
	if err != nil {
		log.Printf("RegisterPeer DB error: %v", err)
		return fail("registration failed: " + err.Error())
	}

	log.Printf("registered peer %s (libp2p %s)", id, req.Libp2PPeerId)
	return &pb.RegistrationResponse{Success: true, Message: "peer registered", PeerId: id}, nil
}

// Heartbeat marks a peer online and refreshes capacity, device state and
// addresses. Fields left empty keep their stored values.
func (s *server) Heartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	if req.PeerId == "" {
		return &pb.HeartbeatResponse{Success: false, Message: "peer_id is required"}, nil
	}
	var multiaddrs interface{}
	if len(req.Multiaddrs) > 0 {
		multiaddrs = pq.Array(req.Multiaddrs)
	}
	// Proto3 cannot tell 0 from "not sent", and a device reporting 0% battery
	// is not running, so only 1-100 updates the stored level.
	battery := sql.NullInt32{Int32: req.BatteryLevel, Valid: req.BatteryLevel >= 1 && req.BatteryLevel <= 100}

	res, err := s.db.ExecContext(ctx, fmt.Sprintf(`
		UPDATE peers SET
			is_online         = true,
			storage_free_gb   = $1,
			multiaddrs        = COALESCE($2, multiaddrs),
			multiaddr         = COALESCE(($2::TEXT[])[1], multiaddr),
			is_charging       = $3,
			battery_level     = COALESCE($4, battery_level),
			network_type      = COALESCE($5, network_type),
			last_seen_at      = now(),
			last_heartbeat_at = now(),
			updated_at        = now()
		WHERE %s = $6`, idColumn(req.PeerId)),
		float64(req.AvailableCapacityBytes)/gb, multiaddrs, req.IsCharging, battery,
		common.NullIfEmpty(req.NetworkType), req.PeerId)
	if err != nil {
		log.Printf("Heartbeat %s DB error: %v", req.PeerId, err)
		return &pb.HeartbeatResponse{Success: false, Message: "heartbeat failed: " + err.Error()}, nil
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return &pb.HeartbeatResponse{Success: false, Message: "peer not found - please register first"}, nil
	}
	return &pb.HeartbeatResponse{Success: true, Message: "heartbeat processed"}, nil
}

const peerColumns = `peer_id, user_id, libp2p_peer_id, multiaddr, multiaddrs, asn, region, timezone,
	storage_total_gb, storage_free_gb, health_score, is_online, last_heartbeat_at`

type rowScanner interface{ Scan(...interface{}) error }

func scanPeer(r rowScanner) (*pb.PeerInfo, error) {
	var (
		p              pb.PeerInfo
		userID, libp2p sql.NullString
		multiaddr      sql.NullString
		multiaddrs     []string
		totalGB        int64
		freeGB, health float64
		online         bool
		lastHeartbeat  time.Time
	)
	if err := r.Scan(&p.PeerId, &userID, &libp2p, &multiaddr, pq.Array(&multiaddrs), &p.Asn, &p.Region,
		&p.Timezone, &totalGB, &freeGB, &health, &online, &lastHeartbeat); err != nil {
		return nil, err
	}
	p.UserId = userID.String
	p.Libp2PPeerId = libp2p.String
	p.Multiaddr = multiaddr.String
	p.Multiaddrs = multiaddrs
	p.TotalCapacityBytes = totalGB * gb
	p.AvailableCapacityBytes = int64(freeGB * gb)
	p.HealthScore = health
	p.Status = "inactive"
	if online {
		p.Status = "active"
	}
	p.LastHeartbeat = lastHeartbeat.Unix()
	return &p, nil
}

// GetPeer looks a peer up by server UUID or libp2p peer ID.
func (s *server) GetPeer(ctx context.Context, req *pb.PeerRequest) (*pb.PeerInfo, error) {
	if req.PeerId == "" {
		return nil, status.Error(codes.InvalidArgument, "peer_id is required")
	}
	row := s.db.QueryRowContext(ctx,
		fmt.Sprintf(`SELECT %s FROM peers WHERE %s = $1`, peerColumns, idColumn(req.PeerId)), req.PeerId)
	p, err := scanPeer(row)
	if err == sql.ErrNoRows {
		return nil, status.Error(codes.NotFound, "peer not found")
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get peer: %v", err)
	}
	return p, nil
}

// ListPeers returns one page of peers plus the total matching the filter.
func (s *server) ListPeers(ctx context.Context, req *pb.ListPeersRequest) (*pb.PeerList, error) {
	where := []string{"health_score >= $1"}
	switch req.Status {
	case "active":
		where = append(where, "is_online = true")
	case "inactive":
		where = append(where, "is_online = false")
	case "", "all":
	default:
		return nil, status.Error(codes.InvalidArgument, `status must be "active", "inactive" or "all"`)
	}
	limit := req.Limit
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	offset := req.Offset
	if offset < 0 {
		offset = 0
	}
	cond := strings.Join(where, " AND ")

	var total int32
	if err := s.db.QueryRowContext(ctx, `SELECT count(*) FROM peers WHERE `+cond, req.MinHealthScore).Scan(&total); err != nil {
		return nil, status.Errorf(codes.Internal, "count peers: %v", err)
	}
	rows, err := s.db.QueryContext(ctx,
		fmt.Sprintf(`SELECT %s FROM peers WHERE %s ORDER BY health_score DESC, peer_id LIMIT $2 OFFSET $3`, peerColumns, cond),
		req.MinHealthScore, limit, offset)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list peers: %v", err)
	}
	defer rows.Close()

	out := &pb.PeerList{TotalCount: total}
	for rows.Next() {
		p, err := scanPeer(rows)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "scan peer: %v", err)
		}
		out.Peers = append(out.Peers, p)
	}
	if err := rows.Err(); err != nil {
		return nil, status.Errorf(codes.Internal, "list peers: %v", err)
	}
	return out, nil
}

// markOffline flags peers whose last heartbeat is older than the timeout.
// It is a single idempotent UPDATE, so running it in several replicas is safe.
func (s *server) markOffline(ctx context.Context) {
	res, err := s.db.ExecContext(ctx, `
		UPDATE peers SET is_online = false, updated_at = now()
		WHERE is_online = true AND last_heartbeat_at < now() - $1::INTERVAL`,
		fmt.Sprintf("%d seconds", int(s.cfg.HeartbeatTimeout.Seconds())))
	if err != nil {
		log.Printf("inactivity check: %v", err)
		return
	}
	if n, _ := res.RowsAffected(); n > 0 {
		log.Printf("marked %d peers offline", n)
	}
}

func main() {
	ctx, stop := common.SignalContext()
	defer stop()

	cfg := loadConfig()
	db := common.MustOpenDB(ctx, requiredSchema)
	defer db.Close()

	s := &server{db: db, cfg: cfg}
	go common.RunEvery(ctx, "inactivity-check", cfg.CheckInterval, s.markOffline)

	common.ServeGRPC(ctx, "peer-registry", cfg.GRPCPort, func(g *grpc.Server) {
		pb.RegisterPeerRegistryServer(g, s)
	})
}
