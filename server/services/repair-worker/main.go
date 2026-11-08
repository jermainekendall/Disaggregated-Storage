 
// Repair Worker - Complete Production Implementation
// Background job to repair degraded files
// File: services/repair-worker/main.go

package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"time"

	_ "github.com/lib/pq"
)

// Configuration
type Config struct {
	DBHost               string
	DBPort               string
	DBName               string
	DBUser               string
	DBPassword           string
	PollInterval         time.Duration
	BatchSize            int
	MaxConcurrentRepairs int
	RepairPriority       int
}

func loadConfig() *Config {
	return &Config{
		DBHost:               getEnv("DB_HOST", "cockroachdb-public"),
		DBPort:               getEnv("DB_PORT", "26257"),
		DBName:               getEnv("DB_NAME", "orchestrator"),
		DBUser:               getEnv("DB_USER", "root"),
		DBPassword:           getEnv("DB_PASSWORD", ""),
		PollInterval:         60 * time.Second,
		BatchSize:            10,
		MaxConcurrentRepairs: 5,
		RepairPriority:       10, // <10 healthy shards = critical
	}
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

// RepairWorker handles file repair operations
type RepairWorker struct {
	db     *sql.DB
	config *Config
	semaphore chan struct{}
}

func NewRepairWorker(db *sql.DB, config *Config) *RepairWorker {
	return &RepairWorker{
		db:        db,
		config:    config,
		semaphore: make(chan struct{}, config.MaxConcurrentRepairs),
	}
}

// Run starts the repair worker loop
func (w *RepairWorker) Run() {
	log.Println("Repair Worker started")

	ticker := time.NewTicker(w.config.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			w.processRepairQueue()
		}
	}
}

func (w *RepairWorker) processRepairQueue() {
	ctx := context.Background()

	// Get degraded files from repair queue
	query := `
		SELECT rq.repair_id, rq.shard_id, rq.reason, rq.priority
		FROM repair_queue rq
		WHERE rq.status = 'pending'
		ORDER BY rq.priority DESC, rq.created_at ASC
		LIMIT $1
	`

	rows, err := w.db.QueryContext(ctx, query, w.config.BatchSize)
	if err != nil {
		log.Printf("Error querying repair queue: %v", err)
		return
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var repairID, shardID, reason string
		var priority int

		if err := rows.Scan(&repairID, &shardID, &reason, &priority); err != nil {
			log.Printf("Error scanning row: %v", err)
			continue
		}

		// Limit concurrent repairs
		w.semaphore <- struct{}{}
		go func(rid, sid, rsn string, pri int) {
			defer func() { <-w.semaphore }()
			w.repairShard(rid, sid, rsn, pri)
		}(repairID, shardID, reason, priority)

		count++
	}

	if count > 0 {
		log.Printf("Processing %d shard repairs", count)
	}
}

func (w *RepairWorker) repairShard(repairID, shardID, reason string, priority int) {
	ctx := context.Background()

	log.Printf("Repairing shard %s (reason: %s, priority: %d)", shardID, reason, priority)

	// Mark repair as in-progress
	_, err := w.db.ExecContext(ctx,
		"UPDATE repair_queue SET status = 'in_progress', started_at = $1 WHERE repair_id = $2",
		time.Now(), repairID,
	)
	if err != nil {
		log.Printf("Error updating repair status: %v", err)
		return
	}

	// Get current shard locations
	healthyLocations, err := w.getHealthyLocations(ctx, shardID)
	if err != nil {
		w.markRepairFailed(ctx, repairID, err.Error())
		return
	}

	// Check if we have enough healthy locations
	targetReplicas := 14 // Minimum erasure code requirement (10+4)
	if len(healthyLocations) >= targetReplicas {
		log.Printf("Shard %s has %d healthy locations, no repair needed", shardID, len(healthyLocations))
		w.markRepairCompleted(ctx, repairID)
		return
	}

	neededReplicas := targetReplicas - len(healthyLocations)
	log.Printf("Shard %s needs %d additional replicas", shardID, neededReplicas)

	// Select new peers for replication
	newPeers, err := w.selectPeersForReplication(ctx, shardID, neededReplicas)
	if err != nil {
		w.markRepairFailed(ctx, repairID, err.Error())
		return
	}

	if len(newPeers) == 0 {
		w.markRepairFailed(ctx, repairID, "no available peers")
		return
	}

	// Create replication jobs
	for _, peerID := range newPeers {
		_, err := w.db.ExecContext(ctx,
			`INSERT INTO shard_locations (shard_id, peer_id, status, created_at)
			 VALUES ($1, $2, 'replicating', $3)`,
			shardID, peerID, time.Now(),
		)
		if err != nil {
			log.Printf("Error creating shard location: %v", err)
			continue
		}

		log.Printf("Created replication job: shard %s -> peer %s", shardID, peerID)
	}

	// Mark repair as completed
	w.markRepairCompleted(ctx, repairID)
	log.Printf("Repair completed for shard %s", shardID)
}

func (w *RepairWorker) getHealthyLocations(ctx context.Context, shardID string) ([]string, error) {
	query := `
		SELECT sl.peer_id
		FROM shard_locations sl
		JOIN peers p ON sl.peer_id = p.peer_id
		WHERE sl.shard_id = $1
		  AND sl.status = 'active'
		  AND p.is_online = true
		  AND p.health_score >= 70
	`

	rows, err := w.db.QueryContext(ctx, query, shardID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var locations []string
	for rows.Next() {
		var peerID string
		if err := rows.Scan(&peerID); err != nil {
			continue
		}
		locations = append(locations, peerID)
	}

	return locations, nil
}

func (w *RepairWorker) selectPeersForReplication(ctx context.Context, shardID string, count int) ([]string, error) {
	// Get peers that don't already have this shard
	query := `
		SELECT p.peer_id
		FROM peers p
		WHERE p.is_online = true
		  AND p.health_score >= 70
		  AND p.storage_free_gb > 1
		  AND p.peer_id NOT IN (
		    SELECT peer_id FROM shard_locations WHERE shard_id = $1
		  )
		ORDER BY p.health_score DESC, RANDOM()
		LIMIT $2
	`

	rows, err := w.db.QueryContext(ctx, query, shardID, count)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var peers []string
	for rows.Next() {
		var peerID string
		if err := rows.Scan(&peerID); err != nil {
			continue
		}
		peers = append(peers, peerID)
	}

	return peers, nil
}

func (w *RepairWorker) markRepairCompleted(ctx context.Context, repairID string) {
	_, err := w.db.ExecContext(ctx,
		`UPDATE repair_queue SET status = 'completed', completed_at = $1 WHERE repair_id = $2`,
		time.Now(), repairID,
	)
	if err != nil {
		log.Printf("Error marking repair as completed: %v", err)
	}
}

func (w *RepairWorker) markRepairFailed(ctx context.Context, repairID, errorMsg string) {
	_, err := w.db.ExecContext(ctx,
		`UPDATE repair_queue SET status = 'failed', error_message = $1, completed_at = $2 WHERE repair_id = $3`,
		errorMsg, time.Now(), repairID,
	)
	if err != nil {
		log.Printf("Error marking repair as failed: %v", err)
	}
}

func main() {
	log.Println("Starting Repair Worker...")

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

	// Create and run worker
	worker := NewRepairWorker(db, config)
	worker.Run()
}
