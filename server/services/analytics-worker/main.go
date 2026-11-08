// Analytics Worker: churn risk, reliability scores and system statistics.
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/jermainekendall/disaggregated-storage/server/internal/common"
)

type config struct {
	PollInterval     time.Duration
	ForecastInterval time.Duration
	ChurnThreshold   float64
	RetentionDays    int
}

func loadConfig() config {
	return config{
		PollInterval:     common.EnvDuration("POLL_INTERVAL_SECONDS", 10*time.Minute),
		ForecastInterval: common.EnvDuration("FORECAST_INTERVAL", 24*time.Hour),
		ChurnThreshold:   common.EnvFloat("CHURN_THRESHOLD", 0.7),
		RetentionDays:    common.EnvInt("EVENT_RETENTION_DAYS", 90),
	}
}

var requiredSchema = map[string][]string{
	"peers": {"peer_id", "uptime_percentage", "failed_audits", "successful_audits", "health_score",
		"first_seen_at", "last_heartbeat_at", "last_seen_at", "is_online", "churn_prediction",
		"churn_prediction_updated_at", "reliability_score", "storage_total_gb", "storage_free_gb"},
	"files":            {"file_id", "owner_id", "file_size_bytes", "status", "created_at"},
	"analytics_events": {"event_type", "event_data", "created_at"},
	"enterprise_usage": {"total_charge_usd", "calculated_at"},
}

type worker struct {
	db  *sql.DB
	cfg config
}

func (w *worker) cycle(ctx context.Context) {
	w.predictChurn(ctx)
	w.updateReliability(ctx)
	w.recordSystemStats(ctx)
	w.pruneEvents(ctx)
}

// predictChurn scores every recently active peer in one statement. The score
// adds up, capped at 1:
//
//	low uptime          (90 - uptime) / 100        when uptime < 90
//	low health          (80 - health) / 200        when health < 80
//	audit failure rate  failed / (failed + passed) x 0.5, whenever audits exist
//	new peer            (30 - age_days) / 60       when younger than 30 days
//	silence             min(hours_quiet / 168, 0.5) when quiet over 24 h
func (w *worker) predictChurn(ctx context.Context) {
	res, err := w.db.ExecContext(ctx, `
		UPDATE peers SET churn_prediction = least(1.0::FLOAT8,
			  CASE WHEN uptime_percentage < 90 THEN (90 - uptime_percentage::FLOAT8) / 100 ELSE 0 END
			+ CASE WHEN health_score < 80 THEN (80 - health_score::FLOAT8) / 200 ELSE 0 END
			+ CASE WHEN failed_audits + successful_audits > 0
			       THEN failed_audits::FLOAT8 / (failed_audits + successful_audits)::FLOAT8 * 0.5 ELSE 0 END
			+ CASE WHEN now() - first_seen_at < INTERVAL '30 days'
			       THEN (30 - extract(epoch FROM now() - first_seen_at)::FLOAT8 / 86400) / 60 ELSE 0 END
			+ CASE WHEN now() - last_heartbeat_at > INTERVAL '24 hours'
			       THEN least(extract(epoch FROM now() - last_heartbeat_at)::FLOAT8 / 3600 / 168, 0.5) ELSE 0 END
			), churn_prediction_updated_at = now()
		WHERE is_online = true OR last_seen_at > now() - INTERVAL '7 days'`)
	if err != nil {
		log.Printf("churn prediction: %v", err)
		return
	}
	n, _ := res.RowsAffected()
	var high int
	w.db.QueryRowContext(ctx, `SELECT count(*) FROM peers WHERE churn_prediction > $1
		AND churn_prediction_updated_at > now() - INTERVAL '1 hour'`, w.cfg.ChurnThreshold).Scan(&high)
	log.Printf("churn prediction: %d peers scored, %d above %.0f%%", n, high, w.cfg.ChurnThreshold*100)
}

func (w *worker) updateReliability(ctx context.Context) {
	res, err := w.db.ExecContext(ctx, `
		UPDATE peers SET reliability_score = CASE
			WHEN successful_audits + failed_audits = 0 THEN 1.0
			ELSE successful_audits::DECIMAL / (successful_audits + failed_audits) END
		WHERE is_online = true OR last_seen_at > now() - INTERVAL '30 days'`)
	if err != nil {
		log.Printf("reliability: %v", err)
		return
	}
	n, _ := res.RowsAffected()
	log.Printf("reliability scores updated for %d peers", n)
}

type systemStats struct {
	TotalPeers     int     `json:"total_peers"`
	OnlinePeers    int     `json:"online_peers"`
	CapacityGB     float64 `json:"capacity_gb"`
	FreeGB         float64 `json:"free_gb"`
	TotalFiles     int     `json:"total_files"`
	StoredGB       float64 `json:"stored_gb"`
	ActiveUploader int     `json:"active_uploaders_30d"`
}

// recordSystemStats gathers the totals in one query and stores them as an event.
func (w *worker) recordSystemStats(ctx context.Context) {
	var s systemStats
	err := w.db.QueryRowContext(ctx, `
		SELECT
		  (SELECT count(*) FROM peers),
		  (SELECT count(*) FROM peers WHERE is_online),
		  (SELECT COALESCE(sum(storage_total_gb), 0)::FLOAT8 FROM peers),
		  (SELECT COALESCE(sum(storage_free_gb), 0)::FLOAT8 FROM peers WHERE is_online),
		  (SELECT count(*) FROM files WHERE status NOT IN ('deleting', 'deleted')),
		  (SELECT COALESCE(sum(file_size_bytes), 0)::FLOAT8 / 1073741824 FROM files WHERE status NOT IN ('deleting', 'deleted')),
		  (SELECT count(DISTINCT owner_id) FROM files WHERE created_at > now() - INTERVAL '30 days')`).
		Scan(&s.TotalPeers, &s.OnlinePeers, &s.CapacityGB, &s.FreeGB, &s.TotalFiles, &s.StoredGB, &s.ActiveUploader)
	if err != nil {
		log.Printf("system stats: %v", err)
		return
	}
	w.storeEvent(ctx, "system_stats", s)
	log.Printf("stats: %d peers (%d online), %d files, %.2f GB stored, %d active uploaders",
		s.TotalPeers, s.OnlinePeers, s.TotalFiles, s.StoredGB, s.ActiveUploader)
}

// forecastRevenue projects enterprise revenue from the last three months.
func (w *worker) forecastRevenue(ctx context.Context) {
	var last3 float64
	if err := w.db.QueryRowContext(ctx, `
		SELECT COALESCE(sum(total_charge_usd), 0)::FLOAT8 FROM enterprise_usage
		WHERE calculated_at > now() - INTERVAL '3 months'`).Scan(&last3); err != nil {
		log.Printf("revenue forecast: %v", err)
		return
	}
	monthly := last3 / 3
	w.storeEvent(ctx, "revenue_forecast", map[string]float64{
		"monthly_avg": monthly, "next_3_months": monthly * 3, "next_12_months": monthly * 12,
	})
}

func (w *worker) storeEvent(ctx context.Context, kind string, data interface{}) {
	raw, err := json.Marshal(data)
	if err != nil {
		log.Printf("encode %s: %v", kind, err)
		return
	}
	if _, err := w.db.ExecContext(ctx,
		`INSERT INTO analytics_events (event_type, event_data) VALUES ($1, $2)`, kind, string(raw)); err != nil {
		log.Printf("store %s: %v", kind, err)
	}
}

func (w *worker) pruneEvents(ctx context.Context) {
	res, err := w.db.ExecContext(ctx,
		`DELETE FROM analytics_events WHERE created_at < now() - $1::INTERVAL`,
		fmt.Sprintf("%d days", w.cfg.RetentionDays))
	if err != nil {
		log.Printf("prune events: %v", err)
		return
	}
	if n, _ := res.RowsAffected(); n > 0 {
		log.Printf("pruned %d old analytics events", n)
	}
}

func main() {
	ctx, stop := common.SignalContext()
	defer stop()

	cfg := loadConfig()
	db := common.MustOpenDB(ctx, requiredSchema)
	defer db.Close()

	w := &worker{db: db, cfg: cfg}
	go common.RunEvery(ctx, "revenue-forecast", cfg.ForecastInterval, w.forecastRevenue)
	common.RunEvery(ctx, "analytics", cfg.PollInterval, w.cycle)
}
