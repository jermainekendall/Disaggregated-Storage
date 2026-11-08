// Replication Worker: access-based tiering and extra copies for popular files.
//
// Every file keeps its data + parity distinct shards (the repair worker's
// job). On top of that, warm and hot files get extra copies of their data
// shards (shards.is_replica = true) so reads can be spread over more peers.
// Total placements stay within MIN/MAX_REPLICATION_COUNT (14/28 by default).
//
// Tier comes from accesses in the last ACCESS_WINDOW_DAYS (file_access_logs),
// not the lifetime access_count, so files move down again when they cool off:
//
//	>= HOT_TIER_THRESHOLD accesses  -> hot:  copies up to MAX_REPLICATION_COUNT
//	>= WARM_TIER_THRESHOLD accesses -> warm: about half the data shards copied once
//	otherwise                       -> cold: no extra copies
//
// A copy is a replication_jobs row plus a pending shard on the target peer.
// The target peer pulls the shard from the source peer and confirms it via
// the gateway; the job completes when the shard turns good. Surplus copies
// are marked 'retiring' for their peer to delete.
package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"

	"github.com/jermainekendall/disaggregated-storage/server/internal/common"
)

type config struct {
	PollInterval      time.Duration
	BatchSize         int
	HotThreshold      int
	WarmThreshold     int
	AccessWindow      time.Duration
	MaxPlacements     int
	MinHealthScore    float64
	CapacityHeadroom  float64
	CopyTimeout       time.Duration
	RequireChargingWF bool
}

func loadConfig() config {
	return config{
		PollInterval:      common.EnvDuration("POLL_INTERVAL_SECONDS", 5*time.Minute),
		BatchSize:         common.EnvInt("BATCH_SIZE", 10),
		HotThreshold:      common.EnvInt("HOT_TIER_THRESHOLD", 100),
		WarmThreshold:     common.EnvInt("WARM_TIER_THRESHOLD", 10),
		AccessWindow:      time.Duration(common.EnvInt("ACCESS_WINDOW_DAYS", 30)) * 24 * time.Hour,
		MaxPlacements:     common.EnvInt("MAX_REPLICATION_COUNT", 28),
		MinHealthScore:    common.EnvFloat("MIN_HEALTH_SCORE", 70),
		CapacityHeadroom:  common.EnvFloat("CAPACITY_HEADROOM", 1.2),
		CopyTimeout:       time.Duration(common.EnvInt("COPY_TIMEOUT_HOURS", 48)) * time.Hour,
		RequireChargingWF: common.EnvBool("COPY_REQUIRES_CHARGING_WIFI", true),
	}
}

var requiredSchema = map[string][]string{
	"files":            {"file_id", "storage_tier", "shard_count", "parity_shards", "status", "updated_at"},
	"file_access_logs": {"file_id", "access_id", "accessed_at"},
	"shards": {"shard_id", "file_id", "peer_id", "shard_index", "shard_type", "shard_size_bytes",
		"health_status", "is_replica", "stored_at", "removed_at"},
	"replication_jobs": {"job_id", "file_id", "source_peer_id", "target_peer_id", "target_shard_id",
		"shard_index", "priority", "status", "requires_charging", "requires_wifi", "queued_at", "completed_at", "last_error"},
	"peers": {"peer_id", "is_online", "health_score", "storage_free_gb", "multiaddr"},
}

type worker struct {
	db  *sql.DB
	cfg config
}

func interval(d time.Duration) string { return fmt.Sprintf("%d seconds", int64(d.Seconds())) }

func (w *worker) cycle(ctx context.Context) {
	w.updateTiers(ctx)
	w.completeCopies(ctx)
	w.reconcile(ctx)
}

// updateTiers sets storage_tier from recent accesses, in both directions.
func (w *worker) updateTiers(ctx context.Context) {
	rows, err := w.db.QueryContext(ctx, `
		UPDATE files f SET storage_tier = t.tier, updated_at = now()
		FROM (
			SELECT f2.file_id,
			       CASE WHEN count(l.access_id) >= $1 THEN 'hot'
			            WHEN count(l.access_id) >= $2 THEN 'warm'
			            ELSE 'cold' END AS tier
			FROM files f2
			LEFT JOIN file_access_logs l ON l.file_id = f2.file_id AND l.accessed_at > now() - $3::INTERVAL
			WHERE f2.status IN ('active', 'degraded')
			GROUP BY f2.file_id
		) t
		WHERE f.file_id = t.file_id AND f.storage_tier != t.tier
		RETURNING f.file_id, t.tier`,
		w.cfg.HotThreshold, w.cfg.WarmThreshold, interval(w.cfg.AccessWindow))
	if err != nil {
		log.Printf("update tiers: %v", err)
		return
	}
	defer rows.Close()
	for rows.Next() {
		var id, tier string
		if rows.Scan(&id, &tier) == nil {
			log.Printf("file %s moved to %s tier", id, tier)
		}
	}
}

// completeCopies closes jobs whose target shard was confirmed, and gives up
// on copies that were never confirmed.
func (w *worker) completeCopies(ctx context.Context) {
	if _, err := w.db.ExecContext(ctx, `
		UPDATE replication_jobs SET status = 'completed', completed_at = now()
		WHERE status = 'queued'
		  AND target_shard_id IN (SELECT shard_id FROM shards WHERE health_status = 'good')`); err != nil {
		log.Printf("complete copies: %v", err)
	}
	rows, err := w.db.QueryContext(ctx, `
		UPDATE replication_jobs SET status = 'failed', completed_at = now(), last_error = 'copy not confirmed in time'
		WHERE status = 'queued' AND queued_at < now() - $1::INTERVAL
		RETURNING target_shard_id`, interval(w.cfg.CopyTimeout))
	if err != nil {
		log.Printf("expire copies: %v", err)
		return
	}
	var stale []string
	for rows.Next() {
		var id sql.NullString
		if rows.Scan(&id) == nil && id.Valid {
			stale = append(stale, id.String)
		}
	}
	rows.Close()
	for _, id := range stale {
		w.db.ExecContext(ctx, `
			UPDATE shards SET health_status = 'missing', removed_at = now(), updated_at = now()
			WHERE shard_id = $1 AND health_status = 'pending'`, id)
	}
}

// reconcile adds or retires extra copies until each file matches its tier.
func (w *worker) reconcile(ctx context.Context) {
	rows, err := w.db.QueryContext(ctx, `
		WITH counts AS (
			SELECT f.file_id, f.storage_tier, f.shard_count, f.parity_shards,
			       (SELECT count(*) FROM shards s WHERE s.file_id = f.file_id AND s.is_replica
			          AND s.health_status IN ('good', 'pending', 'degraded')) AS copies
			FROM files f WHERE f.status = 'active'
		)
		SELECT file_id, storage_tier, shard_count, parity_shards, copies FROM counts
		WHERE copies != CASE storage_tier
			WHEN 'hot'  THEN greatest($1 - shard_count - parity_shards, 0)
			WHEN 'warm' THEN least(ceil(shard_count / 2.0)::INT, greatest($1 - shard_count - parity_shards, 0))
			ELSE 0 END
		LIMIT $2`, w.cfg.MaxPlacements, w.cfg.BatchSize)
	if err != nil {
		log.Printf("find files to reconcile: %v", err)
		return
	}
	type fileRow struct {
		id, tier             string
		data, parity, copies int
	}
	var files []fileRow
	for rows.Next() {
		var f fileRow
		if rows.Scan(&f.id, &f.tier, &f.data, &f.parity, &f.copies) == nil {
			files = append(files, f)
		}
	}
	rows.Close()

	for _, f := range files {
		target := targetCopies(f.tier, f.data, f.parity, w.cfg.MaxPlacements)
		switch {
		case f.copies < target:
			added := w.addCopies(ctx, f.id, f.data, f.copies, target-f.copies)
			log.Printf("file %s (%s): queued %d of %d extra copies", f.id, f.tier, added, target-f.copies)
		case f.copies > target:
			removed := w.retireCopies(ctx, f.id, f.copies-target)
			log.Printf("file %s (%s): retired %d extra copies", f.id, f.tier, removed)
		}
	}
}

// targetCopies is the number of extra data-shard copies a file should have.
func targetCopies(tier string, data, parity, maxPlacements int) int {
	room := maxPlacements - data - parity
	if room < 0 {
		room = 0
	}
	switch tier {
	case "hot":
		return room
	case "warm":
		half := (data + 1) / 2
		if half < room {
			return half
		}
		return room
	}
	return 0
}

// addCopies queues n new copies, cycling through data shard indexes so the
// copies spread evenly. Each copy goes to a peer holding nothing of the file.
func (w *worker) addCopies(ctx context.Context, fileID string, data, existing, n int) int {
	added := 0
	for i := 0; i < n; i++ {
		idx := (existing + i) % data
		var srcPeer string
		var size int64
		err := w.db.QueryRowContext(ctx, `
			SELECT s.peer_id, s.shard_size_bytes FROM shards s JOIN peers p ON p.peer_id = s.peer_id
			WHERE s.file_id = $1 AND s.shard_index = $2 AND s.health_status = 'good' AND p.is_online
			ORDER BY random() LIMIT 1`, fileID, idx).Scan(&srcPeer, &size)
		if err != nil {
			continue // no online source for this index right now
		}
		var target string
		err = w.db.QueryRowContext(ctx, `
			SELECT peer_id FROM peers p
			WHERE is_online AND health_score >= $2 AND storage_free_gb >= $3 AND multiaddr IS NOT NULL
			  AND NOT EXISTS (SELECT 1 FROM shards s WHERE s.file_id = $1 AND s.peer_id = p.peer_id)
			ORDER BY random() LIMIT 1`,
			fileID, w.cfg.MinHealthScore, float64(size)/(1<<30)*w.cfg.CapacityHeadroom).Scan(&target)
		if err != nil {
			break // no eligible peers left
		}
		err = common.RunTx(ctx, w.db, func(tx *sql.Tx) error {
			var shardID string
			if err := tx.QueryRowContext(ctx, `
				INSERT INTO shards (file_id, peer_id, shard_index, shard_type, shard_size_bytes,
				                    is_replica, health_status, stored_at)
				VALUES ($1, $2, $3, 'data', $4, true, 'pending', now())
				RETURNING shard_id`, fileID, target, idx, size).Scan(&shardID); err != nil {
				return err
			}
			_, err := tx.ExecContext(ctx, `
				INSERT INTO replication_jobs (file_id, source_peer_id, target_peer_id, target_shard_id,
				                              shard_index, priority, status, requires_charging, requires_wifi)
				VALUES ($1, $2, $3, $4, $5, 'normal', 'queued', $6, $6)`,
				fileID, srcPeer, target, shardID, idx, w.cfg.RequireChargingWF)
			return err
		})
		if err != nil {
			log.Printf("queue copy of %s/%d: %v", fileID, idx, err)
			continue
		}
		added++
	}
	return added
}

// retireCopies marks n extra copies retiring, unconfirmed ones first, and
// cancels their copy jobs.
func (w *worker) retireCopies(ctx context.Context, fileID string, n int) int {
	rows, err := w.db.QueryContext(ctx, `
		UPDATE shards SET health_status = 'retiring', removed_at = now(), updated_at = now()
		WHERE shard_id IN (
			SELECT shard_id FROM shards
			WHERE file_id = $1 AND is_replica AND health_status IN ('pending', 'degraded', 'good')
			ORDER BY CASE health_status WHEN 'pending' THEN 0 WHEN 'degraded' THEN 1 ELSE 2 END, stored_at DESC
			LIMIT $2)
		RETURNING shard_id`, fileID, n)
	if err != nil {
		log.Printf("retire copies of %s: %v", fileID, err)
		return 0
	}
	var ids []string
	for rows.Next() {
		var id string
		if rows.Scan(&id) == nil {
			ids = append(ids, id)
		}
	}
	rows.Close()
	for _, id := range ids {
		w.db.ExecContext(ctx, `
			UPDATE replication_jobs SET status = 'cancelled', completed_at = now()
			WHERE target_shard_id = $1 AND status = 'queued'`, id)
	}
	return len(ids)
}

func main() {
	ctx, stop := common.SignalContext()
	defer stop()

	cfg := loadConfig()
	db := common.MustOpenDB(ctx, requiredSchema)
	defer db.Close()

	w := &worker{db: db, cfg: cfg}
	log.Printf("replication worker started (poll %s)", cfg.PollInterval)
	common.RunEvery(ctx, "replication", cfg.PollInterval, w.cycle)
}
