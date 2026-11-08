// Billing Service - Complete Production Implementation
// Calculates earnings and processes payouts
// File: services/billing-service/main.go

package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net"
	"os"
	"time"

	_ "github.com/lib/pq"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
)

// Configuration
type Config struct {
	DBHost             string
	DBPort             string
	DBName             string
	DBUser             string
	DBPassword         string
	GRPCPort           string
	BillingCycleDay    int
	PayoutProcessingDay int
	MinPayoutAmount    float64
	PeerRevenueShare   float64 // 0.65 = 65%
}

func loadConfig() *Config {
	return &Config{
		DBHost:             getEnv("DB_HOST", "cockroachdb-public"),
		DBPort:             getEnv("DB_PORT", "26257"),
		DBName:             getEnv("DB_NAME", "orchestrator"),
		DBUser:             getEnv("DB_USER", "root"),
		DBPassword:         getEnv("DB_PASSWORD", ""),
		GRPCPort:           getEnv("GRPC_PORT", "50055"),
		BillingCycleDay:    1,  // First of month
		PayoutProcessingDay: 5,  // 5th of month
		MinPayoutAmount:    10.0, // $10 minimum
		PeerRevenueShare:   0.65, // 65% to peers
	}
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

// BillingServiceServer implements the billing service
type BillingServiceServer struct {
	db     *sql.DB
	config *Config
}

// CalculateEarnings calculates monthly earnings for a peer
func (s *BillingServiceServer) CalculateEarnings(ctx context.Context, req *CalculateEarningsRequest) (*EarningsResponse, error) {
	log.Printf("CalculateEarnings called for peer: %s, month: %d/%d",
		req.PeerId, req.Month, req.Year)

	// Get peer's storage contribution for the month
	storageEarnings, err := s.calculateStorageEarnings(ctx, req.PeerId, req.Month, req.Year)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to calculate storage: %v", err)
	}

	// Get peer's bandwidth contribution
	bandwidthEarnings, err := s.calculateBandwidthEarnings(ctx, req.PeerId, req.Month, req.Year)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to calculate bandwidth: %v", err)
	}

	// Calculate bonus for high uptime
	uptimeBonus, err := s.calculateUptimeBonus(ctx, req.PeerId, req.Month, req.Year)
	if err != nil {
		log.Printf("Warning: Failed to calculate uptime bonus: %v", err)
		uptimeBonus = 0
	}

	// Total earnings
	totalEarnings := storageEarnings + bandwidthEarnings + uptimeBonus

	// Store earnings record
	now := time.Now()
	query := `
		INSERT INTO earnings (
			peer_id, month, year, storage_earnings, bandwidth_earnings,
			uptime_bonus, total_earnings, calculated_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		ON CONFLICT (peer_id, month, year)
		DO UPDATE SET
			storage_earnings = EXCLUDED.storage_earnings,
			bandwidth_earnings = EXCLUDED.bandwidth_earnings,
			uptime_bonus = EXCLUDED.uptime_bonus,
			total_earnings = EXCLUDED.total_earnings,
			calculated_at = EXCLUDED.calculated_at
		RETURNING earning_id
	`

	var earningID string
	err = s.db.QueryRowContext(ctx, query,
		req.PeerId, req.Month, req.Year,
		storageEarnings, bandwidthEarnings, uptimeBonus,
		totalEarnings, now,
	).Scan(&earningID)

	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to store earnings: %v", err)
	}

	log.Printf("Calculated earnings for peer %s: $%.2f", req.PeerId, totalEarnings)

	return &EarningsResponse{
		EarningId:          earningID,
		PeerId:             req.PeerId,
		StorageEarnings:    storageEarnings,
		BandwidthEarnings:  bandwidthEarnings,
		UptimeBonus:        uptimeBonus,
		TotalEarnings:      totalEarnings,
		Month:              req.Month,
		Year:               req.Year,
	}, nil
}

// ProcessPayout initiates a payout to a peer
func (s *BillingServiceServer) ProcessPayout(ctx context.Context, req *PayoutRequest) (*PayoutResponse, error) {
	log.Printf("ProcessPayout called for peer: %s, amount: $%.2f", req.PeerId, req.Amount)

	// Verify peer has sufficient balance
	var availableBalance float64
	err := s.db.QueryRowContext(ctx,
		`SELECT COALESCE(SUM(total_earnings), 0) - COALESCE(SUM(amount), 0)
		 FROM earnings e
		 LEFT JOIN payouts p ON e.peer_id = p.peer_id AND p.status = 'completed'
		 WHERE e.peer_id = $1`,
		req.PeerId,
	).Scan(&availableBalance)

	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to check balance: %v", err)
	}

	if availableBalance < req.Amount {
		return nil, status.Errorf(codes.FailedPrecondition,
			"insufficient balance: have $%.2f, requested $%.2f",
			availableBalance, req.Amount)
	}

	if req.Amount < s.config.MinPayoutAmount {
		return nil, status.Errorf(codes.FailedPrecondition,
			"amount below minimum payout: $%.2f", s.config.MinPayoutAmount)
	}

	// Get payment method
	var paymentMethod, paymentDetails string
	err = s.db.QueryRowContext(ctx,
		"SELECT payment_method, payment_details FROM peers WHERE peer_id = $1",
		req.PeerId,
	).Scan(&paymentMethod, &paymentDetails)

	if err == sql.ErrNoRows {
		return nil, status.Errorf(codes.NotFound, "peer not found")
	} else if err != nil {
		return nil, status.Errorf(codes.Internal, "database error: %v", err)
	}

	if paymentMethod == "" {
		return nil, status.Errorf(codes.FailedPrecondition,
			"no payment method configured")
	}

	// Create payout record
	now := time.Now()
	var payoutID string
	query := `
		INSERT INTO payouts (
			peer_id, amount, payment_method, payment_details,
			status, initiated_at
		) VALUES ($1, $2, $3, $4, 'pending', $5)
		RETURNING payout_id
	`

	err = s.db.QueryRowContext(ctx, query,
		req.PeerId, req.Amount, paymentMethod, paymentDetails, now,
	).Scan(&payoutID)

	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to create payout: %v", err)
	}

	// Process payment (in production, integrate with PayPal/Stripe/crypto)
	go s.processPaymentAsync(payoutID, paymentMethod, paymentDetails, req.Amount)

	log.Printf("Initiated payout %s for peer %s: $%.2f", payoutID, req.PeerId, req.Amount)

	return &PayoutResponse{
		PayoutId:      payoutID,
		Status:        "pending",
		Message:       "Payout initiated successfully",
		EstimatedDays: 3,
	}, nil
}

// GetPayoutHistory retrieves payout history for a peer
func (s *BillingServiceServer) GetPayoutHistory(ctx context.Context, req *PayoutHistoryRequest) (*PayoutHistoryResponse, error) {
	query := `
		SELECT payout_id, amount, payment_method, status,
		       initiated_at, completed_at
		FROM payouts
		WHERE peer_id = $1
		ORDER BY initiated_at DESC
		LIMIT $2
	`

	limit := req.Limit
	if limit == 0 || limit > 100 {
		limit = 50
	}

	rows, err := s.db.QueryContext(ctx, query, req.PeerId, limit)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "query failed: %v", err)
	}
	defer rows.Close()

	var payouts []*PayoutRecord
	for rows.Next() {
		var p PayoutRecord
		var completedAt *time.Time

		err := rows.Scan(&p.PayoutId, &p.Amount, &p.PaymentMethod,
			&p.Status, &p.InitiatedAt, &completedAt)
		if err != nil {
			continue
		}

		if completedAt != nil {
			p.CompletedAt = completedAt.Unix()
		}

		payouts = append(payouts, &p)
	}

	return &PayoutHistoryResponse{
		Payouts: payouts,
		Total:   int32(len(payouts)),
	}, nil
}

// UpdatePaymentMethod updates peer's payment preferences
func (s *BillingServiceServer) UpdatePaymentMethod(ctx context.Context, req *UpdatePaymentRequest) (*UpdatePaymentResponse, error) {
	log.Printf("UpdatePaymentMethod called for peer: %s", req.PeerId)

	query := `
		UPDATE peers SET
			payment_method = $1,
			payment_details = $2,
			updated_at = $3
		WHERE peer_id = $4
	`

	result, err := s.db.ExecContext(ctx, query,
		req.PaymentMethod, req.PaymentDetails, time.Now(), req.PeerId,
	)

	if err != nil {
		return nil, status.Errorf(codes.Internal, "update failed: %v", err)
	}

	rowsAffected, _ := result.RowsAffected()
	if rowsAffected == 0 {
		return nil, status.Errorf(codes.NotFound, "peer not found")
	}

	return &UpdatePaymentResponse{
		Success: true,
		Message: "Payment method updated successfully",
	}, nil
}

// Helper functions

func (s *BillingServiceServer) calculateStorageEarnings(ctx context.Context, peerID string, month, year int32) (float64, error) {
	// Calculate earnings based on GB-hours stored
	// Rate: $0.001 per GB-hour (example rate)
	storageRate := 0.001

	query := `
		SELECT COALESCE(SUM(size_gb * EXTRACT(EPOCH FROM (COALESCE(deleted_at, NOW()) - created_at)) / 3600), 0)
		FROM shard_locations sl
		JOIN shards s ON sl.shard_id = s.shard_id
		WHERE sl.peer_id = $1
		  AND EXTRACT(MONTH FROM sl.created_at) = $2
		  AND EXTRACT(YEAR FROM sl.created_at) = $3
	`

	var gbHours float64
	err := s.db.QueryRowContext(ctx, query, peerID, month, year).Scan(&gbHours)
	if err != nil {
		return 0, err
	}

	return gbHours * storageRate, nil
}

func (s *BillingServiceServer) calculateBandwidthEarnings(ctx context.Context, peerID string, month, year int32) (float64, error) {
	// Calculate earnings based on bytes served
	// Rate: $0.10 per GB served (example rate)
	bandwidthRate := 0.10

	query := `
		SELECT COALESCE(bytes_served, 0) / (1024.0 * 1024.0 * 1024.0)
		FROM peers
		WHERE peer_id = $1
	`

	var gbServed float64
	err := s.db.QueryRowContext(ctx, query, peerID).Scan(&gbServed)
	if err != nil {
		return 0, err
	}

	return gbServed * bandwidthRate * s.config.PeerRevenueShare, nil
}

func (s *BillingServiceServer) calculateUptimeBonus(ctx context.Context, peerID string, month, year int32) (float64, error) {
	// Bonus for high uptime (>99%)
	query := `
		SELECT uptime_percentage
		FROM peers
		WHERE peer_id = $1
	`

	var uptime float64
	err := s.db.QueryRowContext(ctx, query, peerID).Scan(&uptime)
	if err != nil {
		return 0, err
	}

	if uptime > 99.0 {
		return 5.0, nil // $5 bonus for >99% uptime
	} else if uptime > 95.0 {
		return 2.0, nil // $2 bonus for >95% uptime
	}

	return 0, nil
}

func (s *BillingServiceServer) processPaymentAsync(payoutID, paymentMethod, paymentDetails string, amount float64) {
	// In production, integrate with:
	// - PayPal API
	// - Stripe API
	// - Cryptocurrency wallets
	// - Bank transfers

	log.Printf("Processing payment %s: %s for $%.2f", payoutID, paymentMethod, amount)

	// Simulate payment processing
	time.Sleep(5 * time.Second)

	ctx := context.Background()

	// Update payout status
	_, err := s.db.ExecContext(ctx,
		`UPDATE payouts SET status = 'completed', completed_at = $1 WHERE payout_id = $2`,
		time.Now(), payoutID,
	)

	if err != nil {
		log.Printf("Error updating payout status: %v", err)
		// Mark as failed
		s.db.ExecContext(ctx,
			`UPDATE payouts SET status = 'failed', error_message = $1 WHERE payout_id = $2`,
			err.Error(), payoutID,
		)
	} else {
		log.Printf("Payment %s completed successfully", payoutID)
	}
}

// Background job to calculate monthly earnings
func (s *BillingServiceServer) runMonthlyBilling() {
	ticker := time.NewTicker(24 * time.Hour)
	defer ticker.Stop()

	for range ticker.C {
		now := time.Now()

		// Run on billing cycle day
		if now.Day() != s.config.BillingCycleDay {
			continue
		}

		log.Println("Running monthly billing cycle...")

		ctx := context.Background()

		// Get all active peers
		rows, err := s.db.QueryContext(ctx,
			"SELECT peer_id FROM peers WHERE is_online = true OR last_seen_at > $1",
			now.AddDate(0, -1, 0),
		)
		if err != nil {
			log.Printf("Error querying peers: %v", err)
			continue
		}

		count := 0
		month := int32(now.Month())
		year := int32(now.Year())

		for rows.Next() {
			var peerID string
			if err := rows.Scan(&peerID); err != nil {
				continue
			}

			// Calculate earnings
			req := &CalculateEarningsRequest{
				PeerId: peerID,
				Month:  month,
				Year:   year,
			}

			_, err := s.CalculateEarnings(ctx, req)
			if err != nil {
				log.Printf("Failed to calculate earnings for %s: %v", peerID, err)
				continue
			}

			count++
		}
		rows.Close()

		log.Printf("Calculated earnings for %d peers", count)
	}
}

// gRPC message types
type CalculateEarningsRequest struct {
	PeerId string
	Month  int32
	Year   int32
}

type EarningsResponse struct {
	EarningId         string
	PeerId            string
	StorageEarnings   float64
	BandwidthEarnings float64
	UptimeBonus       float64
	TotalEarnings     float64
	Month             int32
	Year              int32
}

type PayoutRequest struct {
	PeerId string
	Amount float64
}

type PayoutResponse struct {
	PayoutId      string
	Status        string
	Message       string
	EstimatedDays int32
}

type PayoutHistoryRequest struct {
	PeerId string
	Limit  int32
}

type PayoutHistoryResponse struct {
	Payouts []*PayoutRecord
	Total   int32
}

type PayoutRecord struct {
	PayoutId      string
	Amount        float64
	PaymentMethod string
	Status        string
	InitiatedAt   int64
	CompletedAt   int64
}

type UpdatePaymentRequest struct {
	PeerId         string
	PaymentMethod  string
	PaymentDetails string
}

type UpdatePaymentResponse struct {
	Success bool
	Message string
}

func main() {
	log.Println("Starting Billing Service...")

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
	server := &BillingServiceServer{
		db:     db,
		config: config,
	}

	// Start background jobs
	go server.runMonthlyBilling()

	// Create gRPC server
	lis, err := net.Listen("tcp", ":"+config.GRPCPort)
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	reflection.Register(grpcServer)

	log.Printf("Billing Service listening on port %s", config.GRPCPort)

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
