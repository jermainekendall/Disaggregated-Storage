// API Gateway: HTTP/JSON entry point for clients and peer agents.
//
// Clients (authenticated with a bearer token from /auth/login):
//
//	POST   /api/v1/auth/register        {email, password}
//	POST   /api/v1/auth/login           {email, password} -> {token}
//	GET    /api/v1/users/me
//	GET    /api/v1/users/me/earnings
//	GET    /api/v1/files
//	POST   /api/v1/files/upload         file metadata -> placement (one peer per shard)
//	GET    /api/v1/files/{id}           file + shard locations for download
//	DELETE /api/v1/files/{id}
//	GET    /api/v1/peers
//
// Peer agents (TEST DEPLOYMENT: not authenticated, scoped by peer UUID):
//
//	POST   /api/v1/peers/register                 same contract as PeerRegistry.RegisterPeer
//	POST   /api/v1/shards/{id}/confirm            {peer_id, shard_hash} after storing a shard
//	GET    /api/v1/peers/{peer_id}/pending-shards shards to store/rebuild/copy and to delete
//	POST   /api/v1/shards/{id}/deleted            {peer_id} after deleting a retiring shard
//
// Peer registration and placement go through the peer-registry and
// placement gRPC services, so validation lives in one place.
package main

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"log"
	"net/http"
	"time"

	"github.com/gorilla/mux"
	"github.com/rs/cors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/jermainekendall/disaggregated-storage/server/internal/common"
	billingpb "github.com/jermainekendall/disaggregated-storage/server/proto/billing"
	peerpb "github.com/jermainekendall/disaggregated-storage/server/proto/peer"
	placementpb "github.com/jermainekendall/disaggregated-storage/server/proto/placement"
)

type config struct {
	Port           string
	JWTSecret      []byte
	TokenTTL       time.Duration
	RatePerMinute  int
	PeerRegistry   string
	Placement      string
	Billing        string
	AllowedOrigins string
}

func loadConfig() config {
	secret := []byte(common.Env("JWT_SECRET", ""))
	if len(secret) == 0 {
		secret = make([]byte, 32)
		rand.Read(secret)
		log.Println("WARNING: JWT_SECRET not set; using a random secret, so tokens stop working on restart")
	}
	return config{
		Port:           common.Env("PORT", "8080"),
		JWTSecret:      secret,
		TokenTTL:       time.Duration(common.EnvInt("JWT_EXPIRY_MINUTES", 60)) * time.Minute,
		RatePerMinute:  common.EnvInt("RATE_LIMIT_REQUESTS_PER_MINUTE", 600),
		PeerRegistry:   common.Env("PEER_REGISTRY_ADDR", "peer-registry:50051"),
		Placement:      common.Env("PLACEMENT_ADDR", "placement-service:50052"),
		Billing:        common.Env("BILLING_ADDR", "billing-service:50053"),
		AllowedOrigins: common.Env("CORS_ALLOWED_ORIGINS", "*"),
	}
}

var requiredSchema = map[string][]string{
	"users": {"user_id", "email", "password_hash", "tier", "status", "storage_quota_gb", "storage_used_gb",
		"current_balance_usd", "created_at", "last_login_at"},
	"files": {"file_id", "owner_id", "file_name_encrypted", "file_size_bytes", "mime_type", "storage_tier",
		"shard_count", "parity_shards", "is_compressed", "compression_algorithm", "original_size_bytes",
		"status", "available_shards", "access_count", "last_accessed_at", "created_at", "deleted_at"},
	"shards": {"shard_id", "file_id", "peer_id", "shard_index", "shard_type", "shard_size_bytes",
		"shard_hash", "health_status", "is_replica", "stored_at", "removed_at"},
	"file_access_logs": {"file_id", "user_id", "access_type", "client_ip"},
	"peers":            {"peer_id", "multiaddr", "multiaddrs", "is_online", "total_uploads"},
}

type server struct {
	db        *sql.DB
	cfg       config
	peers     peerpb.PeerRegistryClient
	placement placementpb.PlacementServiceClient
	billing   billingpb.BillingServiceClient
	limiter   *ipLimiter
}

func (s *server) routes() http.Handler {
	r := mux.NewRouter()
	r.HandleFunc("/health", s.handleHealth).Methods("GET")
	r.HandleFunc("/ready", s.handleReady).Methods("GET")

	api := r.PathPrefix("/api/v1").Subrouter()
	api.Use(s.limiter.middleware)

	api.HandleFunc("/auth/register", s.handleRegister).Methods("POST")
	api.HandleFunc("/auth/login", s.handleLogin).Methods("POST")

	// Peer agent endpoints (see package comment).
	api.HandleFunc("/peers/register", s.handleRegisterPeer).Methods("POST")
	api.HandleFunc("/peers/{peer_id}/pending-shards", s.handlePendingShards).Methods("GET")
	api.HandleFunc("/shards/{id}/confirm", s.handleConfirmShard).Methods("POST")
	api.HandleFunc("/shards/{id}/deleted", s.handleShardDeleted).Methods("POST")

	authed := api.NewRoute().Subrouter()
	authed.Use(s.requireAuth)
	authed.HandleFunc("/users/me", s.handleMe).Methods("GET")
	authed.HandleFunc("/users/me/earnings", s.handleEarnings).Methods("GET")
	authed.HandleFunc("/files", s.handleListFiles).Methods("GET")
	authed.HandleFunc("/files/upload", s.handleUpload).Methods("POST")
	authed.HandleFunc("/files/{id}", s.handleGetFile).Methods("GET")
	authed.HandleFunc("/files/{id}", s.handleDeleteFile).Methods("DELETE")
	authed.HandleFunc("/peers", s.handleListPeers).Methods("GET")

	r.NotFoundHandler = http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeError(w, http.StatusNotFound, "not_found", "no such endpoint")
	})

	c := cors.New(cors.Options{
		AllowedOrigins: []string{s.cfg.AllowedOrigins},
		AllowedMethods: []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
		AllowedHeaders: []string{"Authorization", "Content-Type"},
	})
	return logRequests(c.Handler(r))
}

func (s *server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "healthy", "time": time.Now().UTC().Format(time.RFC3339)})
}

func (s *server) handleReady(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if err := s.db.PingContext(ctx); err != nil {
		writeError(w, http.StatusServiceUnavailable, "not_ready", "database: "+err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

func newUUID() string {
	b := make([]byte, 16)
	rand.Read(b)
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	h := hex.EncodeToString(b)
	return h[0:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:]
}

func dial(addr string) *grpc.ClientConn {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("grpc client for %s: %v", addr, err)
	}
	return conn
}

func main() {
	ctx, stop := common.SignalContext()
	defer stop()

	cfg := loadConfig()
	db := common.MustOpenDB(ctx, requiredSchema)
	defer db.Close()

	s := &server{
		db:        db,
		cfg:       cfg,
		peers:     peerpb.NewPeerRegistryClient(dial(cfg.PeerRegistry)),
		placement: placementpb.NewPlacementServiceClient(dial(cfg.Placement)),
		billing:   billingpb.NewBillingServiceClient(dial(cfg.Billing)),
		limiter:   newIPLimiter(cfg.RatePerMinute),
	}
	go s.limiter.cleanup(ctx)

	srv := &http.Server{
		Addr:              ":" + cfg.Port,
		Handler:           s.routes(),
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      60 * time.Second,
		IdleTimeout:       120 * time.Second,
	}
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		srv.Shutdown(shutdown)
	}()
	log.Printf("api-gateway listening on :%s", cfg.Port)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("serve: %v", err)
	}
}
