// Admin console: a web interface for operators of the Orchestrator network.
//
// Server-rendered HTML (html/template, embedded templates and CSS, no
// JavaScript build). It reads the database directly and calls the billing
// service for billing runs. Every change is written to admin_audit_log.
//
// Access: users with role 'admin' sign in with their account email and
// password. The first admin is created at startup from
// ADMIN_BOOTSTRAP_EMAIL / ADMIN_BOOTSTRAP_PASSWORD (install.sh generates
// them). Sessions are HMAC-signed cookies; every request re-checks that the
// user is still an active admin, so revoking access takes effect at once.
// All forms carry a CSRF token.
//
// TEST DEPLOYMENT: served over plain HTTP. Put it behind TLS before using
// it outside a trusted network.
package main

import (
	"context"
	"crypto/rand"
	"database/sql"
	"embed"
	"errors"
	"log"
	"net/http"
	"strings"
	"time"

	"golang.org/x/crypto/bcrypt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/jermainekendall/disaggregated-storage/server/internal/common"
	billingpb "github.com/jermainekendall/disaggregated-storage/server/proto/billing"
)

//go:embed templates/*.html static/*
var assets embed.FS

type config struct {
	Port           string
	SessionSecret  []byte
	SessionTTL     time.Duration
	CookieSecure   bool
	BillingAddr    string
	BootstrapEmail string
	BootstrapPass  string
}

func loadConfig() config {
	secret := []byte(common.Env("SESSION_SECRET", ""))
	if len(secret) < 16 {
		secret = make([]byte, 32)
		rand.Read(secret)
		log.Println("WARNING: SESSION_SECRET not set (or shorter than 16 bytes); using a random one, so sessions end on restart")
	}
	return config{
		Port:           common.Env("PORT", "8080"),
		SessionSecret:  secret,
		SessionTTL:     time.Duration(common.EnvInt("SESSION_HOURS", 8)) * time.Hour,
		CookieSecure:   common.EnvBool("COOKIE_SECURE", false),
		BillingAddr:    common.Env("BILLING_ADDR", "billing-service:50053"),
		BootstrapEmail: strings.ToLower(strings.TrimSpace(common.Env("ADMIN_BOOTSTRAP_EMAIL", ""))),
		BootstrapPass:  common.Env("ADMIN_BOOTSTRAP_PASSWORD", ""),
	}
}

var requiredSchema = map[string][]string{
	"users": {"user_id", "email", "password_hash", "tier", "role", "status", "storage_quota_gb",
		"current_balance_usd", "payment_method", "created_at", "last_login_at", "updated_at"},
	"peers": {"peer_id", "user_id", "libp2p_peer_id", "multiaddr", "asn", "region", "timezone", "device_model",
		"os_type", "app_version", "storage_total_gb", "storage_free_gb", "is_online", "is_charging",
		"battery_level", "network_type", "health_score", "reliability_score", "uptime_percentage",
		"churn_prediction", "failed_audits", "successful_audits", "bytes_served", "first_seen_at",
		"last_heartbeat_at", "updated_at"},
	"files": {"file_id", "owner_id", "file_size_bytes", "storage_tier", "shard_count", "parity_shards",
		"status", "available_shards", "access_count", "last_accessed_at", "created_at", "original_size_bytes"},
	"shards": {"shard_id", "file_id", "peer_id", "shard_index", "shard_type", "shard_size_bytes",
		"health_status", "is_replica", "stored_at", "removed_at", "updated_at", "verification_failures"},
	"repair_queue":     {"job_id", "file_id", "priority", "missing_shards", "status", "attempts", "reason", "queued_at", "updated_at", "last_error"},
	"replication_jobs": {"job_id", "file_id", "shard_index", "status", "queued_at", "last_error"},
	"audit_logs":       {"audit_id", "shard_id", "peer_id", "status", "challenged_at", "verified_at", "failure_reason"},
	"earnings": {"earning_id", "peer_id", "user_id", "period_start", "storage_gb_days", "bandwidth_gb",
		"gross_earnings_usd", "net_earnings_usd", "status", "approved_at"},
	"payouts":          {"payout_id", "user_id", "amount_usd", "payment_method", "status", "created_at", "completed_at", "failure_reason"},
	"system_config":    {"config_key", "config_value", "config_type", "description", "version", "updated_at", "updated_by"},
	"admin_audit_log":  {"entry_id", "admin_id", "admin_email", "action", "target_type", "target_id", "details", "client_ip", "created_at"},
	"analytics_events": {"event_type", "event_data", "created_at"},
}

type server struct {
	db      *sql.DB
	cfg     config
	billing billingpb.BillingServiceClient
	pages   map[string]*pageTemplate
	login   *loginLimiter
}

// bootstrapAdmin makes sure the configured account exists and is an active
// admin. An existing account keeps its password.
func (s *server) bootstrapAdmin(ctx context.Context) error {
	if s.cfg.BootstrapEmail == "" {
		var n int
		s.db.QueryRowContext(ctx, `SELECT count(*) FROM users WHERE role = 'admin'`).Scan(&n)
		if n == 0 {
			log.Println("WARNING: no admin account exists; set ADMIN_BOOTSTRAP_EMAIL and ADMIN_BOOTSTRAP_PASSWORD")
		}
		return nil
	}
	res, err := s.db.ExecContext(ctx, `
		UPDATE users SET role = 'admin', status = 'active', updated_at = now() WHERE email = $1`, s.cfg.BootstrapEmail)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n > 0 {
		log.Printf("bootstrap admin %s is ready", s.cfg.BootstrapEmail)
		return nil
	}
	if len(s.cfg.BootstrapPass) < 12 {
		return errors.New("ADMIN_BOOTSTRAP_PASSWORD must be at least 12 characters to create the first admin")
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(s.cfg.BootstrapPass), bcrypt.DefaultCost)
	if err != nil {
		return err
	}
	if _, err := s.db.ExecContext(ctx, `
		INSERT INTO users (email, password_hash, role, tier) VALUES ($1, $2, 'admin', 'paid')`,
		s.cfg.BootstrapEmail, string(hash)); err != nil {
		return err
	}
	log.Printf("created bootstrap admin %s", s.cfg.BootstrapEmail)
	return nil
}

func main() {
	ctx, stop := common.SignalContext()
	defer stop()

	cfg := loadConfig()
	db := common.MustOpenDB(ctx, requiredSchema)
	defer db.Close()

	conn, err := grpc.NewClient(cfg.BillingAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("billing client: %v", err)
	}
	defer conn.Close()

	pages, err := loadTemplates()
	if err != nil {
		log.Fatalf("templates: %v", err)
	}
	s := &server{db: db, cfg: cfg, billing: billingpb.NewBillingServiceClient(conn), pages: pages,
		login: newLoginLimiter()}
	if err := s.bootstrapAdmin(ctx); err != nil {
		log.Fatalf("bootstrap admin: %v", err)
	}

	srv := &http.Server{
		Addr:              ":" + cfg.Port,
		Handler:           s.routes(),
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      5 * time.Minute, // billing runs can take a while
		IdleTimeout:       2 * time.Minute,
	}
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		srv.Shutdown(shutdown)
	}()
	log.Printf("admin-console listening on :%s", cfg.Port)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("serve: %v", err)
	}
}
