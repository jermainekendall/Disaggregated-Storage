// Analytics Worker - Complete Production Implementation
// ML predictions, usage analytics, and reporting
// File: services/analytics-worker/main.go

package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"math"
	"os"
	"time"

	_ "github.com/lib/pq"
)

// Configuration
type Config struct {
	DBHost            string
	DBPort            string
	DBName            string
	DBUser            string
	DBPassword        string
	PollInterval      time.Duration
	BatchSize         int
	ChurnThreshold    float64
}

func loadConfig() *Config {
	return &Config{
		DBHost:         getEnv("DB_HOST", "cockroachdb-public"),
		DBPort:         getEnv("DB_PORT", "26257"),
		DBName:         getEnv("DB_NAME", "orchestrator"),
		DBUser:         getEnv("DB_USER", "root"),
		DBPassword:     getEnv("DB_PASSWORD", ""),
		PollInterval:   10 * time.Minute,
		BatchSize:      1000,
		ChurnThreshold: 0.7, // 70% probability
	}
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

// AnalyticsWorker handles ML predictions and analytics
type AnalyticsWorker struct {
	db     *sql.DB
	config *Config
}

func NewAnalyticsWorker(db *sql.DB, config *Config) *AnalyticsWorker {
	return &AnalyticsWorker{
		db:     db,
		config: config,
	}
}

// Run starts the analytics worker loop
func (w *AnalyticsWorker) Run() {
	log.Println("Analytics Worker started")

	ticker := time.NewTicker(w.config.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			w.processAnalytics()
		}
	}
}

func (w *AnalyticsWorker) processAnalytics() {
	ctx := context.Background()

	// Run various analytics tasks
	w.predictChurn(ctx)
	w.calculatePeerReliability(ctx)
	w.generateUsageReports(ctx)
	w.trackAnalyticsEvents(ctx)
}

// Predict which peers are likely to churn (stop providing storage)
func (w *AnalyticsWorker) predictChurn(ctx context.Context) {
	log.Println("Running churn prediction...")

	// Get active peers
	query := `
		SELECT peer_id, uptime_percentage, failed_audits, successful_audits,
		       health_score, EXTRACT(EPOCH FROM (NOW() - first_seen_at)) / 86400 AS age_days,
		       EXTRACT(EPOCH FROM (NOW() - last_heartbeat_at)) / 3600 AS hours_since_heartbeat
		FROM peers
		WHERE is_online = true OR last_seen_at > NOW() - INTERVAL '7 days'
	`

	rows, err := w.db.QueryContext(ctx, query)
	if err != nil {
		log.Printf("Error querying peers for churn prediction: %v", err)
		return
	}
	defer rows.Close()

	count := 0
	highRiskCount := 0

	for rows.Next() {
		var peerID string
		var uptime, healthScore float64
		var failedAudits, successfulAudits int
		var ageDays, hoursSinceHeartbeat float64

		if err := rows.Scan(&peerID, &uptime, &failedAudits, &successfulAudits,
			&healthScore, &ageDays, &hoursSinceHeartbeat); err != nil {
			continue
		}

		// Simple churn prediction model
		churnProbability := w.calculateChurnProbability(
			uptime, healthScore, failedAudits, successfulAudits,
			ageDays, hoursSinceHeartbeat,
		)

		// Update prediction
		_, err = w.db.ExecContext(ctx,
			`UPDATE peers SET churn_prediction = $1, churn_prediction_updated_at = $2
			 WHERE peer_id = $3`,
			churnProbability, time.Now(), peerID,
		)
		if err != nil {
			log.Printf("Error updating churn prediction: %v", err)
			continue
		}

		if churnProbability > w.config.ChurnThreshold {
			highRiskCount++
			log.Printf("High churn risk for peer %s: %.2f%%", peerID, churnProbability*100)
		}

		count++
	}

	log.Printf("Churn prediction complete: %d peers analyzed, %d high-risk", count, highRiskCount)
}

func (w *AnalyticsWorker) calculateChurnProbability(uptime, healthScore float64,
	failedAudits, successfulAudits int, ageDays, hoursSinceHeartbeat float64) float64 {

	// Simplified ML model (in production, use proper ML library)
	score := 0.0

	// Low uptime increases churn risk
	if uptime < 90 {
		score += (90 - uptime) / 100.0 // 0-0.9
	}

	// Low health score increases risk
	if healthScore < 80 {
		score += (80 - healthScore) / 200.0 // 0-0.4
	}

	// Failed audits increase risk
	if successfulAudits > 0 {
		failureRate := float64(failedAudits) / float64(failedAudits+successfulAudits)
		score += failureRate * 0.5 // 0-0.5
	}

	// New peers are more likely to churn
	if ageDays < 30 {
		score += (30 - ageDays) / 60.0 // 0-0.5
	}

	// Recent inactivity increases risk
	if hoursSinceHeartbeat > 24 {
		score += math.Min(hoursSinceHeartbeat/168.0, 0.5) // 0-0.5
	}

	// Cap at 1.0
	if score > 1.0 {
		score = 1.0
	}

	return score
}

// Calculate peer reliability scores
func (w *AnalyticsWorker) calculatePeerReliability(ctx context.Context) {
	log.Println("Calculating peer reliability scores...")

	query := `
		UPDATE peers
		SET reliability_score = CASE
			WHEN (successful_audits + failed_audits) = 0 THEN 1.0
			ELSE CAST(successful_audits AS FLOAT) / (successful_audits + failed_audits)
		END
		WHERE is_online = true OR last_seen_at > NOW() - INTERVAL '30 days'
	`

	result, err := w.db.ExecContext(ctx, query)
	if err != nil {
		log.Printf("Error updating reliability scores: %v", err)
		return
	}

	rowsAffected, _ := result.RowsAffected()
	log.Printf("Updated reliability scores for %d peers", rowsAffected)
}

// Generate usage reports
func (w *AnalyticsWorker) generateUsageReports(ctx context.Context) {
	log.Println("Generating usage reports...")

	// Calculate system-wide statistics
	var stats struct {
		TotalPeers        int
		OnlinePeers       int
		TotalFiles        int
		TotalStorageGB    float64
		TotalBandwidthTB  float64
		ActiveUsers       int
	}

	// Total peers
	w.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM peers").Scan(&stats.TotalPeers)

	// Online peers
	w.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM peers WHERE is_online = true").Scan(&stats.OnlinePeers)

	// Total files
	w.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM files").Scan(&stats.TotalFiles)

	// Total storage
	w.db.QueryRowContext(ctx,
		"SELECT COALESCE(SUM(size_gb), 0) FROM files",
	).Scan(&stats.TotalStorageGB)

	// Active users (last 30 days)
	w.db.QueryRowContext(ctx,
		"SELECT COUNT(DISTINCT user_id) FROM files WHERE created_at > NOW() - INTERVAL '30 days'",
	).Scan(&stats.ActiveUsers)

	// Store analytics event
	_, err := w.db.ExecContext(ctx,
		`INSERT INTO analytics_events (event_type, event_data, created_at)
		 VALUES ('system_stats', $1, $2)`,
		fmt.Sprintf(`{"total_peers":%d,"online_peers":%d,"total_files":%d,"total_storage_gb":%.2f,"active_users":%d}`,
			stats.TotalPeers, stats.OnlinePeers, stats.TotalFiles, stats.TotalStorageGB, stats.ActiveUsers),
		time.Now(),
	)

	if err != nil {
		log.Printf("Error storing analytics event: %v", err)
		return
	}

	log.Printf("System stats: %d peers (%d online), %d files, %.2f GB storage, %d active users",
		stats.TotalPeers, stats.OnlinePeers, stats.TotalFiles, stats.TotalStorageGB, stats.ActiveUsers)
}

// Track and process analytics events
func (w *AnalyticsWorker) trackAnalyticsEvents(ctx context.Context) {
	// Clean up old analytics events (keep last 90 days)
	cutoff := time.Now().AddDate(0, 0, -90)

	result, err := w.db.ExecContext(ctx,
		"DELETE FROM analytics_events WHERE created_at < $1",
		cutoff,
	)

	if err != nil {
		log.Printf("Error cleaning old analytics events: %v", err)
		return
	}

	rowsDeleted, _ := result.RowsAffected()
	if rowsDeleted > 0 {
		log.Printf("Cleaned up %d old analytics events", rowsDeleted)
	}
}

// Generate revenue forecasts (simplified)
func (w *AnalyticsWorker) forecastRevenue(ctx context.Context) {
	log.Println("Forecasting revenue...")

	// Get last 3 months revenue
	query := `
		SELECT COALESCE(SUM(amount), 0)
		FROM enterprise_usage
		WHERE created_at > NOW() - INTERVAL '3 months'
	`

	var last3MonthsRevenue float64
	err := w.db.QueryRowContext(ctx, query).Scan(&last3MonthsRevenue)
	if err != nil {
		log.Printf("Error querying revenue: %v", err)
		return
	}

	// Simple forecast: average of last 3 months
	monthlyAverage := last3MonthsRevenue / 3.0
	forecast3Months := monthlyAverage * 3.0
	forecast12Months := monthlyAverage * 12.0

	log.Printf("Revenue forecast: 3mo=$%.2f, 12mo=$%.2f (based on $%.2f monthly avg)",
		forecast3Months, forecast12Months, monthlyAverage)

	// Store forecast
	_, err = w.db.ExecContext(ctx,
		`INSERT INTO analytics_events (event_type, event_data, created_at)
		 VALUES ('revenue_forecast', $1, $2)`,
		fmt.Sprintf(`{"3_month":%.2f,"12_month":%.2f,"monthly_avg":%.2f}`,
			forecast3Months, forecast12Months, monthlyAverage),
		time.Now(),
	)

	if err != nil {
		log.Printf("Error storing revenue forecast: %v", err)
	}
}

func main() {
	log.Println("Starting Analytics Worker...")

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
	worker := NewAnalyticsWorker(db, config)
	worker.Run()
}
