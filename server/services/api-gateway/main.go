// API Gateway - HTTP/REST entry point for all client requests
// File: services/api-gateway/main.go

package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	_ "github.com/lib/pq"
	"github.com/gorilla/mux"
	"github.com/rs/cors"
	"golang.org/x/time/rate"
)

// Config holds application configuration
type Config struct {
	Port       string
	DBHost     string
	DBPort     string
	DBName     string
	DBUser     string
	DBPassword string
}

// Server represents the API Gateway server
type Server struct {
	db      *sql.DB
	router  *mux.Router
	limiter *rate.Limiter
	config  *Config
}

// Response structures
type HealthResponse struct {
	Status  string `json:"status"`
	Version string `json:"version"`
	Time    string `json:"time"`
}

type ErrorResponse struct {
	Error   string `json:"error"`
	Message string `json:"message"`
}

type SuccessResponse struct {
	Success bool        `json:"success"`
	Data    interface{} `json:"data,omitempty"`
	Message string      `json:"message,omitempty"`
}

// Load configuration from environment
func loadConfig() *Config {
	return &Config{
		Port:       getEnv("PORT", "8080"),
		DBHost:     getEnv("DB_HOST", "cockroachdb-public"),
		DBPort:     getEnv("DB_PORT", "26257"),
		DBName:     getEnv("DB_NAME", "orchestrator"),
		DBUser:     getEnv("DB_USER", "root"),
		DBPassword: getEnv("DB_PASSWORD", ""),
	}
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

// Initialize server
func NewServer(config *Config) (*Server, error) {
	// Connect to database
	connStr := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		config.DBHost, config.DBPort, config.DBUser, config.DBPassword, config.DBName)

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %v", err)
	}

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping database: %v", err)
	}

	log.Println("Connected to database successfully")

	// Create router
	router := mux.NewRouter()

	// Create rate limiter (1000 requests per minute)
	limiter := rate.NewLimiter(rate.Every(time.Minute/1000), 1000)

	server := &Server{
		db:      db,
		router:  router,
		limiter: limiter,
		config:  config,
	}

	// Setup routes
	server.setupRoutes()

	return server, nil
}

// Setup HTTP routes
func (s *Server) setupRoutes() {
	// Health check
	s.router.HandleFunc("/health", s.handleHealth).Methods("GET")
	s.router.HandleFunc("/ready", s.handleReady).Methods("GET")

	// API routes
	api := s.router.PathPrefix("/api/v1").Subrouter()

	// Auth routes
	api.HandleFunc("/auth/login", s.handleLogin).Methods("POST")
	api.HandleFunc("/auth/register", s.handleRegister).Methods("POST")

	// File routes
	api.HandleFunc("/files", s.handleListFiles).Methods("GET")
	api.HandleFunc("/files/{id}", s.handleGetFile).Methods("GET")
	api.HandleFunc("/files/upload", s.handleUploadFile).Methods("POST")
	api.HandleFunc("/files/{id}", s.handleDeleteFile).Methods("DELETE")

	// Peer routes
	api.HandleFunc("/peers", s.handleListPeers).Methods("GET")
	api.HandleFunc("/peers/register", s.handleRegisterPeer).Methods("POST")

	// User routes
	api.HandleFunc("/users/me", s.handleGetCurrentUser).Methods("GET")
	api.HandleFunc("/users/me/earnings", s.handleGetEarnings).Methods("GET")

	log.Println("Routes configured")
}

// Health check handler
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	response := HealthResponse{
		Status:  "healthy",
		Version: "1.0.0",
		Time:    time.Now().Format(time.RFC3339),
	}
	s.respondJSON(w, http.StatusOK, response)
}

// Readiness check handler
func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	// Check database connection
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := s.db.PingContext(ctx); err != nil {
		s.respondError(w, http.StatusServiceUnavailable, "database not ready", err.Error())
		return
	}

	response := HealthResponse{
		Status:  "ready",
		Version: "1.0.0",
		Time:    time.Now().Format(time.RFC3339),
	}
	s.respondJSON(w, http.StatusOK, response)
}

// Auth handlers (simplified - implement JWT in production)
func (s *Server) handleLogin(w http.ResponseWriter, r *http.Request) {
	// TODO: Implement JWT authentication
	response := SuccessResponse{
		Success: true,
		Message: "Login endpoint - implement JWT auth",
	}
	s.respondJSON(w, http.StatusOK, response)
}

func (s *Server) handleRegister(w http.ResponseWriter, r *http.Request) {
	// TODO: Implement user registration
	response := SuccessResponse{
		Success: true,
		Message: "Register endpoint - implement user creation",
	}
	s.respondJSON(w, http.StatusOK, response)
}

// File handlers
func (s *Server) handleListFiles(w http.ResponseWriter, r *http.Request) {
	// Query files from database
	query := `SELECT file_id, file_name, file_size, tier, created_at FROM files LIMIT 100`

	rows, err := s.db.Query(query)
	if err != nil {
		s.respondError(w, http.StatusInternalServerError, "failed to query files", err.Error())
		return
	}
	defer rows.Close()

	var files []map[string]interface{}
	for rows.Next() {
		var fileID, fileName, tier string
		var fileSize int64
		var createdAt time.Time

		if err := rows.Scan(&fileID, &fileName, &fileSize, &tier, &createdAt); err != nil {
			continue
		}

		files = append(files, map[string]interface{}{
			"file_id":    fileID,
			"file_name":  fileName,
			"file_size":  fileSize,
			"tier":       tier,
			"created_at": createdAt,
		})
	}

	response := SuccessResponse{
		Success: true,
		Data:    files,
	}
	s.respondJSON(w, http.StatusOK, response)
}

func (s *Server) handleGetFile(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	fileID := vars["id"]

	// Query file details
	query := `SELECT file_id, file_name, file_size, tier FROM files WHERE file_id = $1`

	var fileName, tier string
	var fileSize int64
	err := s.db.QueryRow(query, fileID).Scan(&fileID, &fileName, &fileSize, &tier)

	if err == sql.ErrNoRows {
		s.respondError(w, http.StatusNotFound, "file not found", fileID)
		return
	} else if err != nil {
		s.respondError(w, http.StatusInternalServerError, "failed to query file", err.Error())
		return
	}

	response := SuccessResponse{
		Success: true,
		Data: map[string]interface{}{
			"file_id":   fileID,
			"file_name": fileName,
			"file_size": fileSize,
			"tier":      tier,
		},
	}
	s.respondJSON(w, http.StatusOK, response)
}

func (s *Server) handleUploadFile(w http.ResponseWriter, r *http.Request) {
	// TODO: Implement file upload logic
	response := SuccessResponse{
		Success: true,
		Message: "Upload endpoint - implement file upload flow",
	}
	s.respondJSON(w, http.StatusOK, response)
}

func (s *Server) handleDeleteFile(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	fileID := vars["id"]

	// TODO: Implement file deletion
	response := SuccessResponse{
		Success: true,
		Message: fmt.Sprintf("Delete file %s - implement deletion flow", fileID),
	}
	s.respondJSON(w, http.StatusOK, response)
}

// Peer handlers
func (s *Server) handleListPeers(w http.ResponseWriter, r *http.Request) {
	query := `SELECT peer_id, asn, region, health_score, is_online FROM peers ORDER BY health_score DESC LIMIT 100`

	rows, err := s.db.Query(query)
	if err != nil {
		s.respondError(w, http.StatusInternalServerError, "failed to query peers", err.Error())
		return
	}
	defer rows.Close()

	var peers []map[string]interface{}
	for rows.Next() {
		var peerID, asn, region string
		var healthScore float64
		var isOnline bool

		if err := rows.Scan(&peerID, &asn, &region, &healthScore, &isOnline); err != nil {
			continue
		}

		peers = append(peers, map[string]interface{}{
			"peer_id":      peerID,
			"asn":          asn,
			"region":       region,
			"health_score": healthScore,
			"is_online":    isOnline,
		})
	}

	response := SuccessResponse{
		Success: true,
		Data:    peers,
	}
	s.respondJSON(w, http.StatusOK, response)
}

func (s *Server) handleRegisterPeer(w http.ResponseWriter, r *http.Request) {
	// TODO: Implement peer registration
	response := SuccessResponse{
		Success: true,
		Message: "Peer registration - implement registration flow",
	}
	s.respondJSON(w, http.StatusOK, response)
}

// User handlers
func (s *Server) handleGetCurrentUser(w http.ResponseWriter, r *http.Request) {
	// TODO: Get user from JWT token
	response := SuccessResponse{
		Success: true,
		Message: "Current user endpoint - implement JWT validation",
	}
	s.respondJSON(w, http.StatusOK, response)
}

func (s *Server) handleGetEarnings(w http.ResponseWriter, r *http.Request) {
	// TODO: Get earnings for current user
	response := SuccessResponse{
		Success: true,
		Message: "Earnings endpoint - implement earnings calculation",
	}
	s.respondJSON(w, http.StatusOK, response)
}

// Helper methods
func (s *Server) respondJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}

func (s *Server) respondError(w http.ResponseWriter, status int, error, message string) {
	response := ErrorResponse{
		Error:   error,
		Message: message,
	}
	s.respondJSON(w, status, response)
}

// Start server
func (s *Server) Start() error {
	// Setup CORS
	corsHandler := cors.New(cors.Options{
		AllowedOrigins:   []string{"*"},
		AllowedMethods:   []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
		AllowedHeaders:   []string{"*"},
		AllowCredentials: true,
	})

	handler := corsHandler.Handler(s.router)

	addr := ":" + s.config.Port
	log.Printf("API Gateway listening on %s", addr)

	return http.ListenAndServe(addr, handler)
}

func main() {
	log.Println("Starting API Gateway...")

	config := loadConfig()

	server, err := NewServer(config)
	if err != nil {
		log.Fatalf("Failed to create server: %v", err)
	}

	if err := server.Start(); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}
