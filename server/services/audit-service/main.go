// Audit Service: proof-of-storage challenges against the peers holding shards.
//
// See proto/audit/audit.proto for the challenge scheme. In short: uploaders
// register (challenge, SHA-256(challenge || shard)) pairs; each audit spends
// one; the holding peer collects it with GetPendingAudits and answers with
// VerifyResponse. Repeated failures inside a window mark the shard missing
// and queue the file for repair.
package main

import (
	"context"
	"crypto/subtle"
	"database/sql"
	"fmt"
	"log"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/jermainekendall/disaggregated-storage/server/internal/common"
	"github.com/jermainekendall/disaggregated-storage/server/internal/storage"
	pb "github.com/jermainekendall/disaggregated-storage/server/proto/audit"
)

type config struct {
	GRPCPort          string
	AuditFrequency    time.Duration
	ChallengeTimeout  time.Duration
	MaxAuditFailures  int
	FailureWindow     time.Duration
	SchedulerInterval time.Duration
	BatchSize         int
	RunScheduler      bool
}

func loadConfig() config {
	return config{
		GRPCPort:          common.Env("GRPC_PORT", "50054"),
		AuditFrequency:    time.Duration(common.EnvInt("AUDIT_FREQUENCY_HOURS", 168)) * time.Hour,
		ChallengeTimeout:  common.EnvDuration("CHALLENGE_TIMEOUT_SECONDS", 15*time.Minute),
		MaxAuditFailures:  common.EnvInt("MAX_AUDIT_FAILURES", 3),
		FailureWindow:     time.Duration(common.EnvInt("AUDIT_FAILURE_WINDOW_DAYS", 30)) * 24 * time.Hour,
		SchedulerInterval: common.EnvDuration("AUDIT_SCHEDULER_INTERVAL", time.Hour),
		BatchSize:         common.EnvInt("AUDIT_BATCH_SIZE", 100),
		RunScheduler:      common.EnvBool("RUN_BACKGROUND_JOBS", true),
	}
}

var requiredSchema = map[string][]string{
	"audit_challenges": {"challenge_id", "shard_id", "challenge", "expected_proof", "used_at"},
	"audit_logs": {"audit_id", "shard_id", "peer_id", "challenge_id", "challenge", "response", "status",
		"scheduled_at", "challenged_at", "responded_at", "verified_at", "expires_at", "failure_reason"},
	"shards": {"shard_id", "file_id", "peer_id", "health_status", "last_verified_at", "verification_failures", "removed_at"},
	"peers":  {"peer_id", "successful_audits", "failed_audits"},
}

type server struct {
	pb.UnimplementedAuditServiceServer
	db  *sql.DB
	cfg config
}

func (s *server) RegisterChallenges(ctx context.Context, req *pb.RegisterChallengesRequest) (*pb.RegisterChallengesResponse, error) {
	if !common.IsUUID(req.ShardId) {
		return nil, status.Error(codes.InvalidArgument, "shard_id must be a UUID")
	}
	if len(req.Challenges) == 0 || len(req.Challenges) > 1000 {
		return nil, status.Error(codes.InvalidArgument, "send between 1 and 1000 challenges")
	}
	for i, c := range req.Challenges {
		if len(c.Challenge) < 16 || len(c.ExpectedProof) != 32 {
			return nil, status.Errorf(codes.InvalidArgument,
				"challenge %d: need a nonce of at least 16 bytes and a 32-byte SHA-256 proof", i)
		}
	}

	var unused int32
	err := common.RunTx(ctx, s.db, func(tx *sql.Tx) error {
		var exists bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM shards WHERE shard_id = $1)`, req.ShardId).Scan(&exists); err != nil {
			return err
		}
		if !exists {
			return status.Error(codes.NotFound, "shard not found")
		}
		for _, c := range req.Challenges {
			if _, err := tx.ExecContext(ctx,
				`INSERT INTO audit_challenges (shard_id, challenge, expected_proof) VALUES ($1, $2, $3)`,
				req.ShardId, c.Challenge, c.ExpectedProof); err != nil {
				return err
			}
		}
		return tx.QueryRowContext(ctx,
			`SELECT count(*) FROM audit_challenges WHERE shard_id = $1 AND used_at IS NULL`, req.ShardId).Scan(&unused)
	})
	if err != nil {
		return nil, grpcErr(err, "register challenges")
	}
	return &pb.RegisterChallengesResponse{Stored: int32(len(req.Challenges)), UnusedTotal: unused}, nil
}

// ChallengeAudit opens an audit for a shard, spending one unused challenge. If
// an unexpired audit is already open for the shard it is returned instead.
func (s *server) ChallengeAudit(ctx context.Context, req *pb.ChallengeRequest) (*pb.ChallengeResponse, error) {
	if !common.IsUUID(req.ShardId) {
		return nil, status.Error(codes.InvalidArgument, "shard_id must be a UUID")
	}
	var resp pb.ChallengeResponse
	err := common.RunTx(ctx, s.db, func(tx *sql.Tx) error {
		var expires time.Time
		err := tx.QueryRowContext(ctx, `
			SELECT audit_id, peer_id, challenge, expires_at FROM audit_logs
			WHERE shard_id = $1 AND status = 'pending' AND expires_at > now()
			LIMIT 1`, req.ShardId).Scan(&resp.AuditId, &resp.PeerId, &resp.Challenge, &expires)
		if err == nil {
			resp.ExpiresAt = expires.Unix()
			return nil
		}
		if err != sql.ErrNoRows {
			return err
		}

		var peerID string
		err = tx.QueryRowContext(ctx, `
			SELECT peer_id FROM shards
			WHERE shard_id = $1 AND health_status IN ('good', 'degraded')`, req.ShardId).Scan(&peerID)
		if err == sql.ErrNoRows {
			return status.Error(codes.NotFound, "shard not found or not auditable (pending, missing or retiring)")
		}
		if err != nil {
			return err
		}

		var challengeID string
		var challenge []byte
		err = tx.QueryRowContext(ctx, `
			UPDATE audit_challenges SET used_at = now()
			WHERE challenge_id = (
				SELECT challenge_id FROM audit_challenges
				WHERE shard_id = $1 AND used_at IS NULL
				ORDER BY created_at LIMIT 1)
			RETURNING challenge_id, challenge`, req.ShardId).Scan(&challengeID, &challenge)
		if err == sql.ErrNoRows {
			return status.Error(codes.FailedPrecondition, "no unused challenges for this shard; the uploader must register more")
		}
		if err != nil {
			return err
		}

		expires = time.Now().Add(s.cfg.ChallengeTimeout)
		if err := tx.QueryRowContext(ctx, `
			INSERT INTO audit_logs (shard_id, peer_id, challenge_id, challenge, status,
			                        scheduled_at, challenged_at, expires_at)
			VALUES ($1, $2, $3, $4, 'pending', now(), now(), $5)
			RETURNING audit_id`, req.ShardId, peerID, challengeID, challenge, expires).Scan(&resp.AuditId); err != nil {
			return err
		}
		resp.PeerId, resp.Challenge, resp.ExpiresAt = peerID, challenge, expires.Unix()
		return nil
	})
	if err != nil {
		return nil, grpcErr(err, "challenge")
	}
	return &resp, nil
}

func (s *server) GetPendingAudits(ctx context.Context, req *pb.PendingAuditsRequest) (*pb.PendingAuditsResponse, error) {
	if !common.IsUUID(req.PeerId) {
		return nil, status.Error(codes.InvalidArgument, "peer_id must be the server-assigned UUID")
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT audit_id, shard_id, challenge, expires_at FROM audit_logs
		WHERE peer_id = $1 AND status = 'pending' AND expires_at > now()
		ORDER BY expires_at LIMIT 100`, req.PeerId)
	if err != nil {
		return nil, grpcErr(err, "pending audits")
	}
	defer rows.Close()
	out := &pb.PendingAuditsResponse{}
	for rows.Next() {
		var a pb.PendingAudit
		var exp time.Time
		if err := rows.Scan(&a.AuditId, &a.ShardId, &a.Challenge, &exp); err != nil {
			return nil, grpcErr(err, "pending audits")
		}
		a.ExpiresAt = exp.Unix()
		out.Audits = append(out.Audits, &a)
	}
	return out, grpcErr(rows.Err(), "pending audits")
}

// VerifyResponse checks a peer's proof. The audit row is locked and must still
// be pending, so an audit can only be answered once.
func (s *server) VerifyResponse(ctx context.Context, req *pb.VerifyRequest) (*pb.VerifyResult, error) {
	if !common.IsUUID(req.AuditId) {
		return nil, status.Error(codes.InvalidArgument, "audit_id must be a UUID")
	}
	var result pb.VerifyResult
	var shardID, fileID string
	err := common.RunTx(ctx, s.db, func(tx *sql.Tx) error {
		var st string
		var expires time.Time
		var expected []byte
		var peerID string
		err := tx.QueryRowContext(ctx, `
			SELECT a.status, a.expires_at, a.shard_id, a.peer_id, c.expected_proof, s.file_id
			FROM audit_logs a
			JOIN audit_challenges c ON c.challenge_id = a.challenge_id
			JOIN shards s ON s.shard_id = a.shard_id
			WHERE a.audit_id = $1
			FOR UPDATE OF a`, req.AuditId).Scan(&st, &expires, &shardID, &peerID, &expected, &fileID)
		if err == sql.ErrNoRows {
			return status.Error(codes.NotFound, "audit not found")
		}
		if err != nil {
			return err
		}
		if st != "pending" {
			return status.Errorf(codes.FailedPrecondition, "audit already %s", st)
		}

		passed := subtle.ConstantTimeCompare(req.Proof, expected) == 1
		newStatus, reason := "passed", ""
		switch {
		case time.Now().After(expires):
			newStatus, reason, passed = "timeout", "response after deadline", false
		case !passed:
			newStatus, reason = "failed", "proof mismatch"
		}
		if _, err := tx.ExecContext(ctx, `
			UPDATE audit_logs SET response = $1, status = $2, responded_at = now(), verified_at = now(),
			                      failure_reason = $3
			WHERE audit_id = $4`, req.Proof, newStatus, common.NullIfEmpty(reason), req.AuditId); err != nil {
			return err
		}
		if err := recordOutcome(ctx, tx, shardID, peerID, passed); err != nil {
			return err
		}
		result = pb.VerifyResult{AuditId: req.AuditId, Passed: passed, Status: newStatus, Message: "audit " + newStatus}
		return nil
	})
	if err != nil {
		return nil, grpcErr(err, "verify")
	}
	if !result.Passed {
		s.evaluateShard(ctx, shardID, fileID)
	}
	return &result, nil
}

// recordOutcome updates the peer counters and the shard's health.
func recordOutcome(ctx context.Context, q storage.Querier, shardID, peerID string, passed bool) error {
	if passed {
		if _, err := q.ExecContext(ctx,
			`UPDATE peers SET successful_audits = successful_audits + 1 WHERE peer_id = $1`, peerID); err != nil {
			return err
		}
		_, err := q.ExecContext(ctx, `
			UPDATE shards SET last_verified_at = now(), verification_failures = 0,
			       health_status = CASE WHEN health_status = 'degraded' THEN 'good' ELSE health_status END,
			       updated_at = now()
			WHERE shard_id = $1`, shardID)
		return err
	}
	if _, err := q.ExecContext(ctx,
		`UPDATE peers SET failed_audits = failed_audits + 1 WHERE peer_id = $1`, peerID); err != nil {
		return err
	}
	_, err := q.ExecContext(ctx, `
		UPDATE shards SET verification_failures = verification_failures + 1,
		       health_status = CASE WHEN health_status = 'good' THEN 'degraded' ELSE health_status END,
		       updated_at = now()
		WHERE shard_id = $1`, shardID)
	return err
}

// evaluateShard marks a shard missing once it has failed MaxAuditFailures
// audits inside FailureWindow (failures older than the window do not count),
// and queues the file for repair.
func (s *server) evaluateShard(ctx context.Context, shardID, fileID string) {
	var recent int
	if err := s.db.QueryRowContext(ctx, `
		SELECT count(*) FROM audit_logs
		WHERE shard_id = $1 AND status IN ('failed', 'timeout') AND verified_at > now() - $2::INTERVAL`,
		shardID, interval(s.cfg.FailureWindow)).Scan(&recent); err != nil {
		log.Printf("evaluate shard %s: %v", shardID, err)
		return
	}
	if recent < s.cfg.MaxAuditFailures {
		return
	}
	if _, err := s.db.ExecContext(ctx, `
		UPDATE shards SET health_status = 'missing', removed_at = now(), updated_at = now()
		WHERE shard_id = $1 AND health_status != 'missing'`, shardID); err != nil {
		log.Printf("mark shard %s missing: %v", shardID, err)
		return
	}
	h, err := storage.RefreshFile(ctx, s.db, fileID)
	if err != nil {
		log.Printf("refresh file %s: %v", fileID, err)
		return
	}
	if _, err := storage.EnqueueRepair(ctx, s.db, fileID, "failed_audit", storage.RepairPriority(h)); err != nil {
		log.Printf("enqueue repair for %s: %v", fileID, err)
		return
	}
	log.Printf("shard %s failed %d audits in %s: marked missing, file %s queued for repair (%d/%d shards left)",
		shardID, recent, s.cfg.FailureWindow, fileID, h.Available, h.Total())
}

// expireAudits turns unanswered audits into timeouts, which count as failures.
func (s *server) expireAudits(ctx context.Context) {
	rows, err := s.db.QueryContext(ctx, `
		UPDATE audit_logs a SET status = 'timeout', verified_at = now(), failure_reason = 'no response'
		WHERE status = 'pending' AND expires_at < now()
		RETURNING a.shard_id, a.peer_id`)
	if err != nil {
		log.Printf("expire audits: %v", err)
		return
	}
	type pair struct{ shard, peer string }
	var expired []pair
	for rows.Next() {
		var p pair
		if err := rows.Scan(&p.shard, &p.peer); err == nil {
			expired = append(expired, p)
		}
	}
	rows.Close()
	for _, p := range expired {
		if err := recordOutcome(ctx, s.db, p.shard, p.peer, false); err != nil {
			log.Printf("record timeout for %s: %v", p.shard, err)
			continue
		}
		var fileID string
		if err := s.db.QueryRowContext(ctx, `SELECT file_id FROM shards WHERE shard_id = $1`, p.shard).Scan(&fileID); err == nil {
			s.evaluateShard(ctx, p.shard, fileID)
		}
	}
	if len(expired) > 0 {
		log.Printf("%d audits timed out", len(expired))
	}
}

// scheduleAudits opens audits for shards not audited within AuditFrequency.
func (s *server) scheduleAudits(ctx context.Context) {
	s.expireAudits(ctx)

	rows, err := s.db.QueryContext(ctx, `
		SELECT s.shard_id FROM shards s
		WHERE s.health_status IN ('good', 'degraded')
		  AND EXISTS (SELECT 1 FROM audit_challenges c WHERE c.shard_id = s.shard_id AND c.used_at IS NULL)
		  AND NOT EXISTS (
		      SELECT 1 FROM audit_logs a
		      WHERE a.shard_id = s.shard_id
		        AND (a.status = 'pending' OR a.challenged_at > now() - $1::INTERVAL))
		ORDER BY s.last_verified_at NULLS FIRST
		LIMIT $2`, interval(s.cfg.AuditFrequency), s.cfg.BatchSize)
	if err != nil {
		log.Printf("find shards to audit: %v", err)
		return
	}
	var due []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err == nil {
			due = append(due, id)
		}
	}
	rows.Close()

	opened := 0
	for _, id := range due {
		if _, err := s.ChallengeAudit(ctx, &pb.ChallengeRequest{ShardId: id}); err != nil {
			log.Printf("audit shard %s: %v", id, err)
			continue
		}
		opened++
	}
	if opened > 0 {
		log.Printf("opened %d audits", opened)
	}
}

func (s *server) GetAuditHistory(ctx context.Context, req *pb.AuditHistoryRequest) (*pb.AuditHistoryResponse, error) {
	limit := req.Limit
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	var col, id string
	switch {
	case req.PeerId != "":
		col, id = "peer_id", req.PeerId
	case req.ShardId != "":
		col, id = "shard_id", req.ShardId
	default:
		return nil, status.Error(codes.InvalidArgument, "set peer_id or shard_id")
	}
	if !common.IsUUID(id) {
		return nil, status.Errorf(codes.InvalidArgument, "%s must be a UUID", col)
	}
	rows, err := s.db.QueryContext(ctx, fmt.Sprintf(`
		SELECT audit_id, shard_id, peer_id, status, challenged_at, responded_at
		FROM audit_logs WHERE %s = $1
		ORDER BY challenged_at DESC LIMIT $2`, col), id, limit)
	if err != nil {
		return nil, grpcErr(err, "history")
	}
	defer rows.Close()
	out := &pb.AuditHistoryResponse{}
	for rows.Next() {
		var a pb.AuditRecord
		var challenged, responded sql.NullTime
		if err := rows.Scan(&a.AuditId, &a.ShardId, &a.PeerId, &a.Status, &challenged, &responded); err != nil {
			return nil, grpcErr(err, "history")
		}
		a.ChallengedAt, a.RespondedAt = common.Unix(challenged), common.Unix(responded)
		out.Audits = append(out.Audits, &a)
	}
	out.Total = int32(len(out.Audits))
	return out, grpcErr(rows.Err(), "history")
}

func interval(d time.Duration) string { return fmt.Sprintf("%d seconds", int64(d.Seconds())) }

// grpcErr passes gRPC status errors through and wraps anything else as Internal.
func grpcErr(err error, what string) error {
	if err == nil {
		return nil
	}
	if _, ok := status.FromError(err); ok {
		return err
	}
	return status.Errorf(codes.Internal, "%s: %v", what, err)
}

func main() {
	ctx, stop := common.SignalContext()
	defer stop()

	cfg := loadConfig()
	db := common.MustOpenDB(ctx, requiredSchema)
	defer db.Close()

	s := &server{db: db, cfg: cfg}
	if cfg.RunScheduler {
		go common.RunEvery(ctx, "audit-scheduler", cfg.SchedulerInterval, s.scheduleAudits)
	}
	common.ServeGRPC(ctx, "audit-service", cfg.GRPCPort, func(g *grpc.Server) {
		pb.RegisterAuditServiceServer(g, s)
	})
}
