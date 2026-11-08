// Repair Worker: restores lost shards of erasure-coded files.
//
// A file has data + parity distinct shards; any `data` of them rebuild the
// file. Repair therefore never copies a shard many times. For each missing
// shard index it picks one new peer and records a pending shard there. That
// peer rebuilds the shard from `data` surviving shards (listed by the
// gateway at GET /api/v1/peers/{id}/pending-shards) and confirms it with
// POST /api/v1/shards/{id}/confirm. The job stays in 'awaiting_data' until
// every index has a good copy again.
//
// Losses are detected here (shards on peers offline longer than
// INACTIVE_PEER_HOURS) and by the audit service (repeated failed audits).
package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/jermainekendall/disaggregated-storage/server/internal/common"
	"github.com/jermainekendall/disaggregated-storage/server/internal/storage"
)

type config struct {
	PollInterval     time.Duration
	BatchSize        int
	MaxConcurrent    int
	InactivePeer     time.Duration
	MinHealthScore   float64
	CapacityHeadroom float64
	StaleJobAfter    time.Duration // a 'processing' job untouched this long is reclaimed
	RebuildTimeout   time.Duration // a pending rebuilt shard not confirmed by then is re-placed
	WorkerID         string
}

func loadConfig() config {
	host, _ := os.Hostname()
	return config{
		PollInterval:     common.EnvDuration("POLL_INTERVAL_SECONDS", time.Minute),
		BatchSize:        common.EnvInt("BATCH_SIZE", 10),
		MaxConcurrent:    common.EnvInt("MAX_CONCURRENT_REPAIRS", 5),
		InactivePeer:     time.Duration(common.EnvInt("INACTIVE_PEER_HOURS", 168)) * time.Hour,
		MinHealthScore:   common.EnvFloat("MIN_HEALTH_SCORE", 70),
		CapacityHeadroom: common.EnvFloat("CAPACITY_HEADROOM", 1.2),
		StaleJobAfter:    common.EnvDuration("STALE_JOB_SECONDS", 30*time.Minute),
		RebuildTimeout:   time.Duration(common.EnvInt("REBUILD_TIMEOUT_HOURS", 24)) * time.Hour,
		WorkerID:         common.Env("WORKER_ID", host),
	}
}

var requiredSchema = map[string][]string{
	"repair_queue": {"job_id", "file_id", "priority", "missing_shards", "status", "attempts", "max_attempts",
		"assigned_worker", "reason", "queued_at", "started_at", "completed_at", "updated_at", "last_error"},
	"shards": {"shard_id", "file_id", "peer_id", "shard_index", "shard_type", "shard_size_bytes",
		"health_status", "is_replica", "stored_at", "updated_at", "removed_at"},
	"files": {"file_id", "shard_count", "parity_shards", "available_shards", "status"},
	"peers": {"peer_id", "asn", "is_online", "health_score", "storage_free_gb", "last_seen_at"},
}

type worker struct {
	db  *sql.DB
	cfg config
}

type job struct {
	id, fileID       string
	attempts, maxTry int
}

func (w *worker) cycle(ctx context.Context) {
	w.detectLostShards(ctx)
	w.requeueStalledRebuilds(ctx)
	w.checkAwaitingJobs(ctx)

	jobs, err := w.claimJobs(ctx)
	if err != nil {
		log.Printf("claim jobs: %v", err)
		return
	}
	sem := make(chan struct{}, w.cfg.MaxConcurrent)
	var wg sync.WaitGroup
	for _, j := range jobs {
		sem <- struct{}{}
		wg.Add(1)
		go func(j job) {
			defer func() { <-sem; wg.Done() }()
			w.repair(ctx, j)
		}(j)
	}
	wg.Wait()
	if len(jobs) > 0 {
		log.Printf("processed %d repair jobs", len(jobs))
	}
}

// detectLostShards marks copies on long-offline peers as missing and queues
// every affected file. It also queues degraded files that have no open job.
func (w *worker) detectLostShards(ctx context.Context) {
	rows, err := w.db.QueryContext(ctx, `
		UPDATE shards SET health_status = 'missing', removed_at = now(), updated_at = now()
		WHERE health_status IN ('good', 'degraded', 'pending')
		  AND peer_id IN (SELECT peer_id FROM peers WHERE is_online = false AND last_seen_at < now() - $1::INTERVAL)
		RETURNING file_id`, interval(w.cfg.InactivePeer))
	if err != nil {
		log.Printf("detect lost shards: %v", err)
		return
	}
	files := map[string]bool{}
	for rows.Next() {
		var id string
		if rows.Scan(&id) == nil {
			files[id] = true
		}
	}
	rows.Close()

	rows, err = w.db.QueryContext(ctx, `
		SELECT f.file_id FROM files f
		WHERE f.status = 'degraded'
		  AND NOT EXISTS (SELECT 1 FROM repair_queue q WHERE q.file_id = f.file_id
		                  AND q.status IN ('queued', 'processing', 'awaiting_data'))
		LIMIT 1000`)
	if err == nil {
		for rows.Next() {
			var id string
			if rows.Scan(&id) == nil {
				files[id] = true
			}
		}
		rows.Close()
	}

	for id := range files {
		h, err := storage.RefreshFile(ctx, w.db, id)
		if err != nil {
			log.Printf("refresh %s: %v", id, err)
			continue
		}
		if h.Available >= h.Total() {
			continue
		}
		if ok, err := storage.EnqueueRepair(ctx, w.db, id, "peer_offline", storage.RepairPriority(h)); err != nil {
			log.Printf("enqueue %s: %v", id, err)
		} else if ok {
			log.Printf("file %s queued for repair: %d/%d shards available", id, h.Available, h.Total())
		}
	}
}

// requeueStalledRebuilds gives up on rebuilt shards a peer never confirmed,
// so the next pass places them on someone else.
func (w *worker) requeueStalledRebuilds(ctx context.Context) {
	_, err := w.db.ExecContext(ctx, `
		UPDATE shards SET health_status = 'missing', removed_at = now(), updated_at = now()
		WHERE health_status = 'pending' AND is_replica = false AND stored_at < now() - $1::INTERVAL
		  AND file_id IN (SELECT file_id FROM repair_queue WHERE status = 'awaiting_data')`,
		interval(w.cfg.RebuildTimeout))
	if err != nil {
		log.Printf("requeue stalled rebuilds: %v", err)
		return
	}
	// Jobs whose pending shards were just dropped go back to the queue.
	if _, err := w.db.ExecContext(ctx, `
		UPDATE repair_queue q SET status = 'queued', updated_at = now(), last_error = 'rebuild timed out'
		WHERE status = 'awaiting_data'
		  AND NOT EXISTS (SELECT 1 FROM shards s WHERE s.file_id = q.file_id AND s.health_status = 'pending')
		  AND EXISTS (SELECT 1 FROM files f WHERE f.file_id = q.file_id
		              AND f.available_shards < f.shard_count + f.parity_shards)`); err != nil {
		log.Printf("requeue jobs: %v", err)
	}
}

// checkAwaitingJobs completes jobs whose rebuilt shards have all been confirmed.
func (w *worker) checkAwaitingJobs(ctx context.Context) {
	rows, err := w.db.QueryContext(ctx, `SELECT job_id, file_id FROM repair_queue WHERE status = 'awaiting_data' LIMIT 500`)
	if err != nil {
		log.Printf("awaiting jobs: %v", err)
		return
	}
	type pair struct{ job, file string }
	var list []pair
	for rows.Next() {
		var p pair
		if rows.Scan(&p.job, &p.file) == nil {
			list = append(list, p)
		}
	}
	rows.Close()
	for _, p := range list {
		h, err := storage.RefreshFile(ctx, w.db, p.file)
		if err != nil {
			continue
		}
		if h.Available >= h.Total() {
			w.finish(ctx, p.job, "completed", "")
			log.Printf("file %s fully repaired", p.file)
		}
	}
}

// claimJobs atomically takes up to BatchSize queued jobs (most urgent first;
// priority 0 is critical), plus 'processing' jobs abandoned by a crashed
// worker. Several workers can run without picking the same job.
func (w *worker) claimJobs(ctx context.Context) ([]job, error) {
	rows, err := w.db.QueryContext(ctx, `
		UPDATE repair_queue SET status = 'processing', assigned_worker = $1, started_at = now(),
		       attempts = attempts + 1, updated_at = now()
		WHERE job_id IN (
			SELECT job_id FROM repair_queue
			WHERE status = 'queued' OR (status = 'processing' AND updated_at < now() - $2::INTERVAL)
			ORDER BY priority ASC, queued_at ASC
			LIMIT $3)
		RETURNING job_id, file_id, attempts, max_attempts`,
		w.cfg.WorkerID, interval(w.cfg.StaleJobAfter), w.cfg.BatchSize)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []job
	for rows.Next() {
		var j job
		if err := rows.Scan(&j.id, &j.fileID, &j.attempts, &j.maxTry); err != nil {
			return nil, err
		}
		out = append(out, j)
	}
	return out, rows.Err()
}

func (w *worker) repair(ctx context.Context, j job) {
	h, err := storage.RefreshFile(ctx, w.db, j.fileID)
	if err != nil {
		w.retryOrFail(ctx, j, fmt.Sprintf("refresh file: %v", err))
		return
	}
	if h.Status == "deleting" || h.Status == "deleted" {
		w.finish(ctx, j.id, "completed", "file deleted")
		return
	}
	if h.Available >= h.Total() {
		w.finish(ctx, j.id, "completed", "")
		return
	}
	if !h.Recoverable() {
		// Fewer than `data` shards left: no peer can rebuild anything.
		w.finish(ctx, j.id, "failed", fmt.Sprintf("unrecoverable: %d of %d shards left, %d needed",
			h.Available, h.Total(), h.Data))
		log.Printf("file %s is unrecoverable (%d/%d shards, need %d)", j.fileID, h.Available, h.Total(), h.Data)
		return
	}

	// Indexes with neither a good copy nor a rebuild already under way.
	rows, err := w.db.QueryContext(ctx, `
		SELECT i FROM generate_series(0, $2 - 1) AS g(i)
		WHERE NOT EXISTS (SELECT 1 FROM shards s WHERE s.file_id = $1 AND s.shard_index = g.i
		                  AND s.health_status IN ('good', 'pending'))
		ORDER BY i`, j.fileID, h.Total())
	if err != nil {
		w.retryOrFail(ctx, j, err.Error())
		return
	}
	var missing []int
	for rows.Next() {
		var i int
		if rows.Scan(&i) == nil {
			missing = append(missing, i)
		}
	}
	rows.Close()
	if len(missing) == 0 {
		w.finish(ctx, j.id, "awaiting_data", "")
		return
	}

	var shardSize int64
	if err := w.db.QueryRowContext(ctx,
		`SELECT max(shard_size_bytes) FROM shards WHERE file_id = $1`, j.fileID).Scan(&shardSize); err != nil {
		w.retryOrFail(ctx, j, "no shard size: "+err.Error())
		return
	}

	targets, err := w.pickPeers(ctx, j.fileID, shardSize, len(missing))
	if err != nil {
		w.retryOrFail(ctx, j, err.Error())
		return
	}

	placed := 0
	for i, idx := range missing {
		if i >= len(targets) {
			break
		}
		shardType := "data"
		if idx >= h.Data {
			shardType = "parity"
		}
		if _, err := w.db.ExecContext(ctx, `
			INSERT INTO shards (file_id, peer_id, shard_index, shard_type, shard_size_bytes, health_status, stored_at)
			VALUES ($1, $2, $3, $4, $5, 'pending', now())`,
			j.fileID, targets[i], idx, shardType, shardSize); err != nil {
			log.Printf("place shard %d of %s: %v", idx, j.fileID, err)
			continue
		}
		placed++
	}
	log.Printf("file %s: %d of %d missing shards assigned for rebuild", j.fileID, placed, len(missing))

	switch {
	case placed == len(missing):
		w.finish(ctx, j.id, "awaiting_data", "")
	case placed > 0:
		// Some placed; the rest go back to the queue for another pass.
		w.retryOrFail(ctx, j, fmt.Sprintf("only %d of %d shards could be placed", placed, len(missing)))
	default:
		w.retryOrFail(ctx, j, "no eligible peers for rebuilt shards")
	}
}

// pickPeers returns up to n online, healthy peers with room for a shard that
// have never held a copy of this file, preferring ASNs the file does not use yet.
func (w *worker) pickPeers(ctx context.Context, fileID string, shardSize int64, n int) ([]string, error) {
	needGB := float64(shardSize) / (1 << 30) * w.cfg.CapacityHeadroom
	rows, err := w.db.QueryContext(ctx, `
		SELECT p.peer_id,
		       p.asn IN (SELECT p2.asn FROM shards s JOIN peers p2 ON p2.peer_id = s.peer_id
		                 WHERE s.file_id = $1 AND s.health_status IN ('good', 'pending', 'degraded')) AS asn_used
		FROM peers p
		WHERE p.is_online = true AND p.health_score >= $2 AND p.storage_free_gb >= $3
		  AND p.multiaddr IS NOT NULL
		  -- any earlier row, even a missing one, would collide with UNIQUE (file_id, peer_id, shard_index)
		  AND NOT EXISTS (SELECT 1 FROM shards s WHERE s.file_id = $1 AND s.peer_id = p.peer_id)
		ORDER BY asn_used ASC, random()
		LIMIT $4`, fileID, w.cfg.MinHealthScore, needGB, n)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id string
		var used bool
		if err := rows.Scan(&id, &used); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

func (w *worker) retryOrFail(ctx context.Context, j job, msg string) {
	if j.attempts >= j.maxTry {
		w.finish(ctx, j.id, "failed", msg)
		log.Printf("repair job %s failed after %d attempts: %s", j.id, j.attempts, msg)
		return
	}
	if _, err := w.db.ExecContext(ctx, `
		UPDATE repair_queue SET status = 'queued', last_error = $2, updated_at = now() WHERE job_id = $1`,
		j.id, msg); err != nil {
		log.Printf("requeue job %s: %v", j.id, err)
	}
}

func (w *worker) finish(ctx context.Context, jobID, st, msg string) {
	q := `UPDATE repair_queue SET status = $2, last_error = COALESCE($3, last_error), updated_at = now()`
	if st == "completed" || st == "failed" {
		q += `, completed_at = now()`
	}
	if _, err := w.db.ExecContext(ctx, q+` WHERE job_id = $1`, jobID, st, common.NullIfEmpty(msg)); err != nil {
		log.Printf("update job %s: %v", jobID, err)
	}
}

func interval(d time.Duration) string { return fmt.Sprintf("%d seconds", int64(d.Seconds())) }

func main() {
	ctx, stop := common.SignalContext()
	defer stop()

	cfg := loadConfig()
	db := common.MustOpenDB(ctx, requiredSchema)
	defer db.Close()

	w := &worker{db: db, cfg: cfg}
	log.Printf("repair worker %s started (poll %s)", cfg.WorkerID, cfg.PollInterval)
	common.RunEvery(ctx, "repair", cfg.PollInterval, w.cycle)
}
