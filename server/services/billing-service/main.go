// Billing Service: monthly peer earnings and user payouts.
//
// Earnings are computed per peer per calendar month from the shards it held
// during that month (GB-days by storage tier), the bytes it served during the
// month, a reliability bonus and a penalty for failed audits. Payouts are per
// user, because one user can own several peers.
//
// Test deployment: payments are simulated. A payout is marked completed after
// PAYMENT_SIMULATION_DELAY with external_transaction_id "simulated-<id>".
package main

import (
	"context"
	"database/sql"
	"log"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/jermainekendall/disaggregated-storage/server/internal/common"
	pb "github.com/jermainekendall/disaggregated-storage/server/proto/billing"
)

type config struct {
	GRPCPort         string
	CheckInterval    time.Duration
	RunBackground    bool
	SimulatePayments bool
	SimulationDelay  time.Duration
}

func loadConfig() config {
	return config{
		GRPCPort:         common.Env("GRPC_PORT", "50053"),
		CheckInterval:    common.EnvDuration("BILLING_CHECK_INTERVAL", time.Hour),
		RunBackground:    common.EnvBool("RUN_BACKGROUND_JOBS", true),
		SimulatePayments: common.EnvBool("SIMULATE_PAYMENTS", true),
		SimulationDelay:  common.EnvDuration("PAYMENT_SIMULATION_DELAY", 5*time.Second),
	}
}

var requiredSchema = map[string][]string{
	"earnings": {"earning_id", "peer_id", "user_id", "period_start", "period_end", "storage_gb_days",
		"base_rate_usd", "bandwidth_gb", "bandwidth_earnings_usd", "bytes_served_snapshot",
		"gross_earnings_usd", "reliability_bonus_usd", "audit_penalty_usd", "net_earnings_usd", "status"},
	"payouts": {"payout_id", "user_id", "amount_usd", "period_start", "period_end", "payment_method",
		"payment_email", "payment_address", "status", "external_transaction_id", "created_at",
		"processed_at", "completed_at", "failure_reason"},
	"users":         {"user_id", "payment_method", "payment_email", "payment_address", "updated_at"},
	"peers":         {"peer_id", "user_id", "bytes_served", "uptime_percentage", "last_seen_at"},
	"shards":        {"peer_id", "file_id", "shard_size_bytes", "health_status", "stored_at", "removed_at"},
	"files":         {"file_id", "storage_tier"},
	"audit_logs":    {"peer_id", "status", "verified_at"},
	"system_config": {"config_key", "config_value"},
}

type server struct {
	pb.UnimplementedBillingServiceServer
	db  *sql.DB
	cfg config
}

// configFloat reads a numeric system_config value, falling back to def.
func configFloat(ctx context.Context, q interface {
	QueryRowContext(context.Context, string, ...interface{}) *sql.Row
}, key string, def float64) float64 {
	var v float64
	if err := q.QueryRowContext(ctx,
		`SELECT config_value::FLOAT8 FROM system_config WHERE config_key = $1`, key).Scan(&v); err != nil {
		return def
	}
	return v
}

func monthBounds(year, month int32) (time.Time, time.Time) {
	start := time.Date(int(year), time.Month(month), 1, 0, 0, 0, 0, time.UTC)
	return start, start.AddDate(0, 1, 0)
}

// CalculateEarnings computes (or recomputes, while still pending) one peer's
// earnings for a calendar month. A month in progress is billed up to now.
func (s *server) CalculateEarnings(ctx context.Context, req *pb.CalculateEarningsRequest) (*pb.EarningsRecord, error) {
	if !common.IsUUID(req.PeerId) {
		return nil, status.Error(codes.InvalidArgument, "peer_id must be a UUID")
	}
	if req.Month < 1 || req.Month > 12 || req.Year < 2020 {
		return nil, status.Error(codes.InvalidArgument, "year/month out of range")
	}
	start, end := monthBounds(req.Year, req.Month)
	if start.After(time.Now()) {
		return nil, status.Error(codes.InvalidArgument, "cannot bill a future month")
	}
	upTo := end
	if now := time.Now().UTC(); now.Before(end) {
		upTo = now
	}

	var rec *pb.EarningsRecord
	err := common.RunTx(ctx, s.db, func(tx *sql.Tx) error {
		var userID sql.NullString
		var bytesServed int64
		var uptime float64
		err := tx.QueryRowContext(ctx,
			`SELECT user_id, bytes_served, uptime_percentage FROM peers WHERE peer_id = $1`, req.PeerId).
			Scan(&userID, &bytesServed, &uptime)
		if err == sql.ErrNoRows {
			return status.Error(codes.NotFound, "peer not found")
		}
		if err != nil {
			return err
		}

		// GB-days per tier: each shard copy counts for the part of the
		// period between when it was stored and when it stopped counting.
		rows, err := tx.QueryContext(ctx, `
			SELECT f.storage_tier,
			       COALESCE(sum(s.shard_size_bytes::FLOAT8 / 1073741824.0 *
			           greatest(0, extract(epoch FROM
			               least(COALESCE(s.removed_at, $3::TIMESTAMP), $3::TIMESTAMP)
			               - greatest(s.stored_at, $2::TIMESTAMP)) / 86400.0)), 0)
			FROM shards s JOIN files f ON f.file_id = s.file_id
			WHERE s.peer_id = $1
			  AND s.health_status != 'pending'
			  AND s.stored_at < $3::TIMESTAMP
			  AND COALESCE(s.removed_at, $3::TIMESTAMP) > $2::TIMESTAMP
			GROUP BY f.storage_tier`, req.PeerId, start, upTo)
		if err != nil {
			return err
		}
		var gbDays, storageUSD float64
		for rows.Next() {
			var tier string
			var d float64
			if err := rows.Scan(&tier, &d); err != nil {
				rows.Close()
				return err
			}
			rate := configFloat(ctx, s.db, "consumer_earnings_rate_"+tier, 0.0063) // USD per GB-month
			gbDays += d
			storageUSD += d * rate / 30.0
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}

		// Bandwidth: bytes served since the previous period's snapshot. A
		// recalculation keeps this period's original snapshot, so bytes are
		// never paid twice.
		var prevSnap int64
		if err := tx.QueryRowContext(ctx, `
			SELECT COALESCE(max(bytes_served_snapshot), 0) FROM earnings
			WHERE peer_id = $1 AND period_start < $2`, req.PeerId, start).Scan(&prevSnap); err != nil {
			return err
		}
		snap := bytesServed
		var existingSnap sql.NullInt64
		var existingStatus string
		err = tx.QueryRowContext(ctx, `SELECT bytes_served_snapshot, status FROM earnings WHERE peer_id = $1 AND period_start = $2`,
			req.PeerId, start).Scan(&existingSnap, &existingStatus)
		if err == nil {
			if existingStatus != "pending" {
				rec, err = loadEarning(ctx, tx, req.PeerId, start)
				return err // approved or paid: never rewrite
			}
			snap = existingSnap.Int64
		} else if err != sql.ErrNoRows {
			return err
		}
		bandwidthGB := float64(max64(snap-prevSnap, 0)) / (1 << 30)
		bandwidthUSD := bandwidthGB * configFloat(ctx, s.db, "consumer_bandwidth_rate_gb", 0.01)

		gross := storageUSD + bandwidthUSD
		bonus := 0.0
		switch {
		case uptime > 99:
			bonus = gross * 0.10
		case uptime > 95:
			bonus = gross * 0.05
		}
		var failures int
		if err := tx.QueryRowContext(ctx, `
			SELECT count(*) FROM audit_logs
			WHERE peer_id = $1 AND status IN ('failed', 'timeout')
			  AND verified_at >= $2::TIMESTAMP AND verified_at < $3::TIMESTAMP`, req.PeerId, start, end).Scan(&failures); err != nil {
			return err
		}
		penalty := minf(float64(failures)*configFloat(ctx, s.db, "audit_failure_penalty_usd", 0.01), gross+bonus)
		net := gross + bonus - penalty
		baseRate := 0.0
		if gbDays > 0 {
			baseRate = storageUSD / gbDays * 30
		}

		if _, err := tx.ExecContext(ctx, `
			INSERT INTO earnings (peer_id, user_id, period_start, period_end, storage_gb_days, base_rate_usd,
			                      bandwidth_gb, bandwidth_earnings_usd, bytes_served_snapshot, gross_earnings_usd,
			                      reliability_bonus_usd, audit_penalty_usd, net_earnings_usd, status, calculated_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, 'pending', now())
			ON CONFLICT (peer_id, period_start) DO UPDATE SET
				user_id = excluded.user_id, storage_gb_days = excluded.storage_gb_days,
				base_rate_usd = excluded.base_rate_usd, bandwidth_gb = excluded.bandwidth_gb,
				bandwidth_earnings_usd = excluded.bandwidth_earnings_usd,
				gross_earnings_usd = excluded.gross_earnings_usd,
				reliability_bonus_usd = excluded.reliability_bonus_usd,
				audit_penalty_usd = excluded.audit_penalty_usd,
				net_earnings_usd = excluded.net_earnings_usd, calculated_at = now()`,
			req.PeerId, userID, start, end, gbDays, baseRate, bandwidthGB, bandwidthUSD, snap,
			gross, bonus, penalty, net); err != nil {
			return err
		}
		rec, err = loadEarning(ctx, tx, req.PeerId, start)
		return err
	})
	if err != nil {
		return nil, grpcErr(err, "calculate earnings")
	}
	return rec, nil
}

const earningCols = `earning_id, peer_id, period_start, period_end, storage_gb_days, bandwidth_gb,
	gross_earnings_usd, reliability_bonus_usd, audit_penalty_usd, net_earnings_usd, status`

func scanEarning(r interface{ Scan(...interface{}) error }) (*pb.EarningsRecord, error) {
	var e pb.EarningsRecord
	var ps, pe time.Time
	if err := r.Scan(&e.EarningId, &e.PeerId, &ps, &pe, &e.StorageGbDays, &e.BandwidthGb, &e.GrossEarningsUsd,
		&e.ReliabilityBonusUsd, &e.AuditPenaltyUsd, &e.NetEarningsUsd, &e.Status); err != nil {
		return nil, err
	}
	e.PeriodStart, e.PeriodEnd = ps.Format("2006-01-02"), pe.Format("2006-01-02")
	return &e, nil
}

func loadEarning(ctx context.Context, tx *sql.Tx, peerID string, start time.Time) (*pb.EarningsRecord, error) {
	return scanEarning(tx.QueryRowContext(ctx,
		`SELECT `+earningCols+` FROM earnings WHERE peer_id = $1 AND period_start = $2`, peerID, start))
}

func (s *server) ListEarnings(ctx context.Context, req *pb.ListEarningsRequest) (*pb.EarningsList, error) {
	limit := req.Limit
	if limit <= 0 || limit > 120 {
		limit = 24
	}
	col, id := "user_id", req.UserId
	if req.PeerId != "" {
		col, id = "peer_id", req.PeerId
	}
	if !common.IsUUID(id) {
		return nil, status.Error(codes.InvalidArgument, "set user_id or peer_id (UUID)")
	}
	rows, err := s.db.QueryContext(ctx,
		`SELECT `+earningCols+` FROM earnings WHERE `+col+` = $1 ORDER BY period_start DESC LIMIT $2`, id, limit)
	if err != nil {
		return nil, grpcErr(err, "list earnings")
	}
	defer rows.Close()
	out := &pb.EarningsList{}
	for rows.Next() {
		e, err := scanEarning(rows)
		if err != nil {
			return nil, grpcErr(err, "list earnings")
		}
		out.Earnings = append(out.Earnings, e)
	}
	return out, grpcErr(rows.Err(), "list earnings")
}

// balance computes earned, paid out and available for a user. The two sums
// are separate subqueries; joining earnings to payouts first would multiply
// each side by the other's row count.
func balance(ctx context.Context, q interface {
	QueryRowContext(context.Context, string, ...interface{}) *sql.Row
}, userID string) (earned, paid float64, err error) {
	err = q.QueryRowContext(ctx, `
		SELECT
		  (SELECT COALESCE(sum(net_earnings_usd), 0)::FLOAT8 FROM earnings WHERE user_id = $1),
		  (SELECT COALESCE(sum(amount_usd), 0)::FLOAT8 FROM payouts WHERE user_id = $1 AND status != 'failed')`,
		userID).Scan(&earned, &paid)
	return
}

func (s *server) GetBalance(ctx context.Context, req *pb.BalanceRequest) (*pb.BalanceResponse, error) {
	if !common.IsUUID(req.UserId) {
		return nil, status.Error(codes.InvalidArgument, "user_id must be a UUID")
	}
	earned, paid, err := balance(ctx, s.db, req.UserId)
	if err != nil {
		return nil, grpcErr(err, "balance")
	}
	return &pb.BalanceResponse{EarnedUsd: earned, PaidOutUsd: paid, AvailableUsd: earned - paid}, nil
}

// ProcessPayout creates a payout if the user's available balance covers it.
// Pending and processing payouts already count against the balance, and the
// user row is locked, so concurrent requests cannot spend the same money.
func (s *server) ProcessPayout(ctx context.Context, req *pb.PayoutRequest) (*pb.PayoutResponse, error) {
	if !common.IsUUID(req.UserId) {
		return nil, status.Error(codes.InvalidArgument, "user_id must be a UUID")
	}
	minPayout := configFloat(ctx, s.db, "payout_minimum_usd", 5)
	if req.AmountUsd < minPayout {
		return nil, status.Errorf(codes.FailedPrecondition, "minimum payout is $%.2f", minPayout)
	}

	var payoutID string
	err := common.RunTx(ctx, s.db, func(tx *sql.Tx) error {
		var method, email, address sql.NullString
		err := tx.QueryRowContext(ctx, `
			SELECT payment_method, payment_email, payment_address FROM users
			WHERE user_id = $1 FOR UPDATE`, req.UserId).Scan(&method, &email, &address)
		if err == sql.ErrNoRows {
			return status.Error(codes.NotFound, "user not found")
		}
		if err != nil {
			return err
		}
		if method.String == "" {
			return status.Error(codes.FailedPrecondition, "no payment method configured")
		}
		earned, paid, err := balance(ctx, tx, req.UserId)
		if err != nil {
			return err
		}
		if avail := earned - paid; avail+1e-9 < req.AmountUsd {
			return status.Errorf(codes.FailedPrecondition,
				"insufficient balance: have $%.2f, requested $%.2f", avail, req.AmountUsd)
		}
		return tx.QueryRowContext(ctx, `
			INSERT INTO payouts (user_id, amount_usd, period_start, period_end, payment_method,
			                     payment_email, payment_address, status)
			SELECT $1, $2,
			       COALESCE((SELECT min(period_start) FROM earnings WHERE user_id = $1), current_date),
			       COALESCE((SELECT max(period_end) FROM earnings WHERE user_id = $1), current_date),
			       $3, $4, $5, 'pending'
			RETURNING payout_id`, req.UserId, req.AmountUsd, method, email, address).Scan(&payoutID)
	})
	if err != nil {
		return nil, grpcErr(err, "payout")
	}

	msg := "payout created; payment processing is not connected"
	if s.cfg.SimulatePayments {
		go s.simulatePayment(payoutID)
		msg = "payout created; payment will be simulated (test deployment)"
	}
	log.Printf("payout %s for user %s: $%.2f", payoutID, req.UserId, req.AmountUsd)
	return &pb.PayoutResponse{PayoutId: payoutID, Status: "pending", Message: msg, EstimatedDays: 3}, nil
}

func (s *server) simulatePayment(payoutID string) {
	ctx := context.Background()
	if _, err := s.db.ExecContext(ctx,
		`UPDATE payouts SET status = 'processing', processed_at = now() WHERE payout_id = $1`, payoutID); err != nil {
		log.Printf("payout %s: %v", payoutID, err)
		return
	}
	time.Sleep(s.cfg.SimulationDelay)
	if _, err := s.db.ExecContext(ctx, `
		UPDATE payouts SET status = 'completed', completed_at = now(), external_transaction_id = $2
		WHERE payout_id = $1`, payoutID, "simulated-"+payoutID); err != nil {
		log.Printf("payout %s: %v", payoutID, err)
		s.db.ExecContext(ctx, `UPDATE payouts SET status = 'failed', failure_reason = $2 WHERE payout_id = $1`,
			payoutID, err.Error())
	}
}

func (s *server) GetPayoutHistory(ctx context.Context, req *pb.PayoutHistoryRequest) (*pb.PayoutHistoryResponse, error) {
	if !common.IsUUID(req.UserId) {
		return nil, status.Error(codes.InvalidArgument, "user_id must be a UUID")
	}
	limit := req.Limit
	if limit <= 0 || limit > 100 {
		limit = 50
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT payout_id, amount_usd, payment_method, status, created_at, completed_at
		FROM payouts WHERE user_id = $1 ORDER BY created_at DESC LIMIT $2`, req.UserId, limit)
	if err != nil {
		return nil, grpcErr(err, "payout history")
	}
	defer rows.Close()
	out := &pb.PayoutHistoryResponse{}
	for rows.Next() {
		var p pb.PayoutRecord
		var created time.Time
		var completed sql.NullTime
		if err := rows.Scan(&p.PayoutId, &p.AmountUsd, &p.PaymentMethod, &p.Status, &created, &completed); err != nil {
			return nil, grpcErr(err, "payout history")
		}
		p.CreatedAt, p.CompletedAt = created.Unix(), common.Unix(completed)
		out.Payouts = append(out.Payouts, &p)
	}
	out.Total = int32(len(out.Payouts))
	return out, grpcErr(rows.Err(), "payout history")
}

var paymentMethods = map[string]bool{"paypal": true, "bank": true, "crypto": true, "gift_card": true, "platform_credit": true}

func (s *server) UpdatePaymentMethod(ctx context.Context, req *pb.UpdatePaymentRequest) (*pb.UpdatePaymentResponse, error) {
	if !common.IsUUID(req.UserId) {
		return nil, status.Error(codes.InvalidArgument, "user_id must be a UUID")
	}
	if !paymentMethods[req.PaymentMethod] {
		return nil, status.Error(codes.InvalidArgument, "payment_method must be paypal, bank, crypto, gift_card or platform_credit")
	}
	res, err := s.db.ExecContext(ctx, `
		UPDATE users SET payment_method = $1, payment_email = $2, payment_address = $3, updated_at = now()
		WHERE user_id = $4`, req.PaymentMethod, common.NullIfEmpty(req.PaymentEmail),
		common.NullIfEmpty(req.PaymentAddress), req.UserId)
	if err != nil {
		return nil, grpcErr(err, "update payment method")
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return nil, status.Error(codes.NotFound, "user not found")
	}
	return &pb.UpdatePaymentResponse{Success: true, Message: "payment method updated"}, nil
}

// billPreviousMonth bills every peer that was active last month and has no
// earnings row for it yet. It runs hourly, so a missed day or a restart does
// not skip a month, and the unique (peer_id, period_start) key makes reruns
// harmless.
func (s *server) billPreviousMonth(ctx context.Context) {
	now := time.Now().UTC()
	start := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC).AddDate(0, -1, 0)
	end := start.AddDate(0, 1, 0)

	rows, err := s.db.QueryContext(ctx, `
		SELECT p.peer_id FROM peers p
		WHERE NOT EXISTS (SELECT 1 FROM earnings e WHERE e.peer_id = p.peer_id AND e.period_start = $1)
		  AND (p.last_seen_at >= $1::TIMESTAMP OR EXISTS (
		       SELECT 1 FROM shards s WHERE s.peer_id = p.peer_id
		         AND s.stored_at < $2::TIMESTAMP AND COALESCE(s.removed_at, $2::TIMESTAMP) > $1::TIMESTAMP))`,
		start, end)
	if err != nil {
		log.Printf("billing: find peers: %v", err)
		return
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err == nil {
			ids = append(ids, id)
		}
	}
	rows.Close()

	for _, id := range ids {
		if _, err := s.CalculateEarnings(ctx, &pb.CalculateEarningsRequest{
			PeerId: id, Year: int32(start.Year()), Month: int32(start.Month())}); err != nil {
			log.Printf("billing %s for %s: %v", start.Format("2006-01"), id, err)
		}
	}
	if len(ids) > 0 {
		log.Printf("billed %d peers for %s", len(ids), start.Format("2006-01"))
	}
}

func grpcErr(err error, what string) error {
	if err == nil {
		return nil
	}
	if _, ok := status.FromError(err); ok {
		return err
	}
	return status.Errorf(codes.Internal, "%s: %v", what, err)
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func minf(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}

func main() {
	ctx, stop := common.SignalContext()
	defer stop()

	cfg := loadConfig()
	db := common.MustOpenDB(ctx, requiredSchema)
	defer db.Close()

	s := &server{db: db, cfg: cfg}
	if cfg.RunBackground {
		go common.RunEvery(ctx, "monthly-billing", cfg.CheckInterval, s.billPreviousMonth)
	}
	common.ServeGRPC(ctx, "billing-service", cfg.GRPCPort, func(g *grpc.Server) {
		pb.RegisterBillingServiceServer(g, s)
	})
}
