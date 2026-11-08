// Package storage holds the file and shard bookkeeping shared by the audit
// service, the gateway and the repair and replication workers.
//
// Model: a file is erasure coded into shard_count data shards plus
// parity_shards parity shards, with distinct shard_index values 0..n-1. Any
// shard_count of them rebuild the file. Each row in the shards table is one
// copy of one shard on one peer; normally there is exactly one copy per index.
package storage

import (
	"context"
	"database/sql"
)

// Querier is satisfied by both *sql.DB and *sql.Tx.
type Querier interface {
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row
}

// FileHealth is the redundancy state of one file.
type FileHealth struct {
	Available int // distinct shard indexes with a good copy
	Data      int
	Parity    int
	Status    string
}

// Total is the number of distinct shards the file should have.
func (h FileHealth) Total() int { return h.Data + h.Parity }

// Recoverable reports whether enough shards remain to rebuild the file.
func (h FileHealth) Recoverable() bool { return h.Available >= h.Data }

// RefreshFile recomputes files.available_shards and the file status from the
// shards table. An uploading file becomes active once every shard is
// confirmed; an active file whose copies are lost becomes degraded, and back
// to active once repaired. Deleting and deleted files are left alone.
func RefreshFile(ctx context.Context, q Querier, fileID string) (FileHealth, error) {
	var h FileHealth
	err := q.QueryRowContext(ctx, `
		WITH avail AS (
			SELECT count(DISTINCT shard_index) AS n
			FROM shards
			WHERE file_id = $1 AND health_status = 'good'
		)
		UPDATE files SET
			available_shards = (SELECT n FROM avail),
			status = CASE
				WHEN status IN ('deleting', 'deleted') THEN status
				WHEN (SELECT n FROM avail) >= shard_count + parity_shards THEN 'active'
				WHEN status = 'uploading' THEN 'uploading'
				ELSE 'degraded'
			END,
			updated_at = now()
		WHERE file_id = $1
		RETURNING available_shards, shard_count, parity_shards, status`, fileID).
		Scan(&h.Available, &h.Data, &h.Parity, &h.Status)
	return h, err
}

// EnqueueRepair opens a repair job for the file unless one is already open
// (the repair_queue_one_open_per_file index makes this race-free). Lower
// priority numbers are more urgent.
func EnqueueRepair(ctx context.Context, q Querier, fileID, reason string, priority int) (bool, error) {
	res, err := q.ExecContext(ctx, `
		INSERT INTO repair_queue (file_id, priority, missing_shards, target_shard_count, reason)
		SELECT f.file_id, $2,
		       greatest(f.shard_count + f.parity_shards - f.available_shards, 0),
		       f.shard_count + f.parity_shards, $3
		FROM files f
		WHERE f.file_id = $1 AND f.status NOT IN ('deleting', 'deleted', 'uploading')
		ON CONFLICT DO NOTHING`, fileID, priority, reason)
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}

// RepairPriority maps remaining redundancy to a queue priority: 0 when the
// file is one loss from unrecoverable (or already is), rising to 10.
func RepairPriority(h FileHealth) int {
	spare := h.Available - h.Data // copies that can still be lost
	switch {
	case spare <= 1:
		return 0
	case spare >= 10:
		return 10
	default:
		return spare
	}
}
