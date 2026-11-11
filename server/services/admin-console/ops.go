package main

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/mux"

	"github.com/jermainekendall/disaggregated-storage/server/internal/storage"
)

func mux_id(r *http.Request) string { return mux.Vars(r)["id"] }

// ---------------------------------------------------------------- peers

type peerRow struct {
	ID, Libp2p, UserID, Email, ASN, Region, Timezone, Device, Network, Version string
	Online, Charging                                                           bool
	Health, Reliability, Uptime                                                float64
	Churn                                                                      sql.NullFloat64
	Battery                                                                    sql.NullInt64
	TotalGB                                                                    int
	FreeGB                                                                     float64
	Shards                                                                     int
	ShardBytes                                                                 int64
	AuditsOK, AuditsFailed                                                     int
	BytesServed                                                                int64
	FirstSeen, LastHeartbeat                                                   time.Time
}

const peerCols = `p.peer_id, COALESCE(p.libp2p_peer_id, ''), COALESCE(p.user_id::STRING, ''), COALESCE(u.email, ''),
	p.asn, p.region, p.timezone, COALESCE(p.device_model, ''), COALESCE(p.network_type, ''), COALESCE(p.app_version, ''),
	p.is_online, p.is_charging, p.health_score::FLOAT8, p.reliability_score::FLOAT8, p.uptime_percentage::FLOAT8,
	p.churn_prediction::FLOAT8, p.battery_level, p.storage_total_gb, p.storage_free_gb::FLOAT8,
	(SELECT count(*) FROM shards s WHERE s.peer_id = p.peer_id AND s.health_status IN ('good', 'degraded')),
	(SELECT COALESCE(sum(shard_size_bytes), 0) FROM shards s WHERE s.peer_id = p.peer_id AND s.health_status IN ('good', 'degraded')),
	p.successful_audits, p.failed_audits, p.bytes_served, p.first_seen_at, p.last_heartbeat_at`

func (s *server) queryPeers(ctx context.Context, where string, arg interface{}, limit, offset int) ([]peerRow, error) {
	rows, err := s.db.QueryContext(ctx, fmt.Sprintf(`
		SELECT %s FROM peers p LEFT JOIN users u ON u.user_id = p.user_id
		WHERE %s ORDER BY p.is_online DESC, p.last_heartbeat_at DESC LIMIT %d OFFSET %d`,
		peerCols, where, limit, offset), arg)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []peerRow
	for rows.Next() {
		var p peerRow
		if err := rows.Scan(&p.ID, &p.Libp2p, &p.UserID, &p.Email, &p.ASN, &p.Region, &p.Timezone, &p.Device,
			&p.Network, &p.Version, &p.Online, &p.Charging, &p.Health, &p.Reliability, &p.Uptime, &p.Churn,
			&p.Battery, &p.TotalGB, &p.FreeGB, &p.Shards, &p.ShardBytes, &p.AuditsOK, &p.AuditsFailed,
			&p.BytesServed, &p.FirstSeen, &p.LastHeartbeat); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

func (s *server) handlePeers(w http.ResponseWriter, r *http.Request) {
	q := strings.TrimSpace(r.URL.Query().Get("q"))
	st := r.URL.Query().Get("status")
	page := pageNum(r)
	where := `($1 = '' OR p.peer_id::STRING LIKE $1 || '%' OR p.libp2p_peer_id LIKE '%' || $1 || '%'
		OR p.region ILIKE '%' || $1 || '%' OR p.asn ILIKE $1 OR p.device_model ILIKE '%' || $1 || '%'
		OR u.email ILIKE '%' || $1 || '%')`
	switch st {
	case "online":
		where += " AND p.is_online"
	case "offline":
		where += " AND NOT p.is_online"
	case "drained":
		where += " AND p.health_score < 70"
	}
	peers, err := s.queryPeers(r.Context(), where, q, pageSize+1, (page-1)*pageSize)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	more := len(peers) > pageSize
	if more {
		peers = peers[:pageSize]
	}
	s.render(w, r, "peers.html", map[string]interface{}{"Peers": peers, "Q": q, "Status": st, "Page": page, "More": more})
}

type shardRow struct {
	ID, FileID, PeerID, Type, Health string
	Index                            int
	Size                             int64
	Replica, PeerOnline              bool
	Stored                           time.Time
	Failures                         int
}

type auditRow struct {
	ID, ShardID, PeerID, Status, Reason string
	Challenged, Verified                sql.NullTime
}

func (s *server) handlePeer(w http.ResponseWriter, r *http.Request) {
	id, ok := idParam(w, r)
	if !ok {
		return
	}
	peers, err := s.queryPeers(r.Context(), "p.peer_id = $1", id, 1, 0)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	if len(peers) == 0 {
		http.Error(w, "peer not found", http.StatusNotFound)
		return
	}
	shards, err := s.queryShards(r.Context(), "s.peer_id = $1 AND s.health_status != 'deleted'", id)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	audits, err := s.queryAudits(r.Context(), "a.peer_id = $1", id, 30)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	history, _ := s.recentLog(r.Context(), 20, "peer", id)
	s.render(w, r, "peer.html", map[string]interface{}{"P": peers[0], "Shards": shards, "Audits": audits, "History": history})
}

func (s *server) queryShards(ctx context.Context, where string, arg interface{}) ([]shardRow, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT s.shard_id, s.file_id, s.peer_id, s.shard_type, s.health_status, s.shard_index, s.shard_size_bytes,
		       s.is_replica, p.is_online, s.stored_at, s.verification_failures
		FROM shards s JOIN peers p ON p.peer_id = s.peer_id
		WHERE `+where+` ORDER BY s.file_id, s.shard_index, s.is_replica LIMIT 500`, arg)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []shardRow
	for rows.Next() {
		var x shardRow
		if err := rows.Scan(&x.ID, &x.FileID, &x.PeerID, &x.Type, &x.Health, &x.Index, &x.Size, &x.Replica,
			&x.PeerOnline, &x.Stored, &x.Failures); err != nil {
			return nil, err
		}
		out = append(out, x)
	}
	return out, rows.Err()
}

func (s *server) queryAudits(ctx context.Context, where string, arg interface{}, limit int) ([]auditRow, error) {
	q := `SELECT a.audit_id, a.shard_id, a.peer_id, a.status, COALESCE(a.failure_reason, ''), a.challenged_at, a.verified_at
		FROM audit_logs a`
	args := []interface{}{}
	if where != "" {
		q += " WHERE " + where
		if arg != nil {
			args = append(args, arg)
		}
	}
	q += fmt.Sprintf(" ORDER BY a.challenged_at DESC NULLS LAST LIMIT %d", limit)
	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []auditRow
	for rows.Next() {
		var a auditRow
		if err := rows.Scan(&a.ID, &a.ShardID, &a.PeerID, &a.Status, &a.Reason, &a.Challenged, &a.Verified); err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

// handlePeerDrain takes a peer out of service: it gets no new shards
// (health 0 is below the placement threshold), and every shard it holds is
// marked missing and rebuilt elsewhere by the repair worker. The peer keeps
// its copies on disk until it is wiped or decommissioned.
func (s *server) handlePeerDrain(w http.ResponseWriter, r *http.Request) {
	id, ok := idParam(w, r)
	if !ok {
		return
	}
	ctx := r.Context()
	if _, err := s.db.ExecContext(ctx, `UPDATE peers SET health_score = 0, updated_at = now() WHERE peer_id = $1`, id); err != nil {
		s.fail(w, r, err)
		return
	}
	rows, err := s.db.QueryContext(ctx, `
		UPDATE shards SET health_status = 'missing', removed_at = now(), updated_at = now()
		WHERE peer_id = $1 AND health_status IN ('good', 'degraded', 'pending')
		RETURNING file_id`, id)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	files := map[string]bool{}
	moved := 0
	for rows.Next() {
		var f string
		if rows.Scan(&f) == nil {
			files[f] = true
			moved++
		}
	}
	rows.Close()
	queued := 0
	for f := range files {
		h, err := storage.RefreshFile(ctx, s.db, f)
		if err != nil {
			continue
		}
		if ok, _ := storage.EnqueueRepair(ctx, s.db, f, "admin_drain", storage.RepairPriority(h)); ok {
			queued++
		}
	}
	s.audit(r, currentAdmin(r), "peer.drain", "peer", id, map[string]interface{}{"shards": moved, "files": len(files)})
	back(w, r, "/peers/"+id, fmt.Sprintf("Peer drained: %d shards from %d files will be rebuilt elsewhere (%d repair jobs queued). Set health back to 100 to return it to service.", moved, len(files), queued), false)
}

func (s *server) handlePeerHealth(w http.ResponseWriter, r *http.Request) {
	id, ok := idParam(w, r)
	if !ok {
		return
	}
	h, err := strconv.ParseFloat(strings.TrimSpace(r.PostFormValue("health")), 64)
	if err != nil || h < 0 || h > 200 {
		back(w, r, "/peers/"+id, "health score must be between 0 and 200", true)
		return
	}
	res, err := s.db.ExecContext(r.Context(), `UPDATE peers SET health_score = $2, updated_at = now() WHERE peer_id = $1`, id, h)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	if n, _ := res.RowsAffected(); n == 0 {
		back(w, r, "/peers", "peer not found", true)
		return
	}
	s.audit(r, currentAdmin(r), "peer.health", "peer", id, map[string]interface{}{"health_score": h})
	back(w, r, "/peers/"+id, fmt.Sprintf("Health score set to %.0f (peers below 70 get no new shards)", h), false)
}

// ---------------------------------------------------------------- files

type fileRow struct {
	ID, OwnerID, Owner, Tier, Status string
	Size                             int64
	Original                         sql.NullInt64
	Data, Parity, Available, Access  int
	Created                          time.Time
	LastAccess                       sql.NullTime
}

func (s *server) queryFiles(ctx context.Context, where string, arg interface{}, limit, offset int) ([]fileRow, error) {
	rows, err := s.db.QueryContext(ctx, fmt.Sprintf(`
		SELECT f.file_id, f.owner_id, COALESCE(u.email, ''), f.storage_tier, f.status, f.file_size_bytes,
		       f.original_size_bytes, f.shard_count, f.parity_shards, f.available_shards, f.access_count,
		       f.created_at, f.last_accessed_at
		FROM files f LEFT JOIN users u ON u.user_id = f.owner_id
		WHERE %s ORDER BY f.created_at DESC LIMIT %d OFFSET %d`, where, limit, offset), arg)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []fileRow
	for rows.Next() {
		var f fileRow
		if err := rows.Scan(&f.ID, &f.OwnerID, &f.Owner, &f.Tier, &f.Status, &f.Size, &f.Original, &f.Data,
			&f.Parity, &f.Available, &f.Access, &f.Created, &f.LastAccess); err != nil {
			return nil, err
		}
		out = append(out, f)
	}
	return out, rows.Err()
}

func (s *server) handleFiles(w http.ResponseWriter, r *http.Request) {
	q := strings.TrimSpace(r.URL.Query().Get("q"))
	st := r.URL.Query().Get("status")
	page := pageNum(r)
	where := `($1 = '' OR f.file_id::STRING LIKE $1 || '%' OR u.email ILIKE '%' || $1 || '%')`
	switch st {
	case "active", "degraded", "uploading", "deleting", "deleted":
		where += " AND f.status = '" + st + "'" // value checked against a fixed list
	case "":
		where += " AND f.status != 'deleted'"
	}
	files, err := s.queryFiles(r.Context(), where, q, pageSize+1, (page-1)*pageSize)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	more := len(files) > pageSize
	if more {
		files = files[:pageSize]
	}
	s.render(w, r, "files.html", map[string]interface{}{"Files": files, "Q": q, "Status": st, "Page": page, "More": more})
}

type repairRow struct {
	ID, FileID, Status, Reason, LastError string
	Priority, Missing, Attempts           int
	Queued, Updated                       time.Time
}

func (s *server) queryRepairs(ctx context.Context, where string, arg interface{}, limit int) ([]repairRow, error) {
	q := `SELECT job_id, file_id, status, COALESCE(reason, ''), COALESCE(last_error, ''), priority, missing_shards,
	             attempts, queued_at, updated_at FROM repair_queue`
	args := []interface{}{}
	if where != "" {
		q += " WHERE " + where
		if arg != nil {
			args = append(args, arg)
		}
	}
	q += fmt.Sprintf(" ORDER BY queued_at DESC LIMIT %d", limit)
	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []repairRow
	for rows.Next() {
		var x repairRow
		if err := rows.Scan(&x.ID, &x.FileID, &x.Status, &x.Reason, &x.LastError, &x.Priority, &x.Missing,
			&x.Attempts, &x.Queued, &x.Updated); err != nil {
			return nil, err
		}
		out = append(out, x)
	}
	return out, rows.Err()
}

func (s *server) handleFile(w http.ResponseWriter, r *http.Request) {
	id, ok := idParam(w, r)
	if !ok {
		return
	}
	files, err := s.queryFiles(r.Context(), "f.file_id = $1", id, 1, 0)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	if len(files) == 0 {
		http.Error(w, "file not found", http.StatusNotFound)
		return
	}
	shards, err := s.queryShards(r.Context(), "s.file_id = $1 AND s.health_status != 'deleted'", id)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	repairs, err := s.queryRepairs(r.Context(), "file_id = $1", id, 20)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	history, _ := s.recentLog(r.Context(), 20, "file", id)
	s.render(w, r, "file.html", map[string]interface{}{"F": files[0], "Shards": shards, "Repairs": repairs, "History": history})
}

func (s *server) handleFileRepair(w http.ResponseWriter, r *http.Request) {
	id, ok := idParam(w, r)
	if !ok {
		return
	}
	h, err := storage.RefreshFile(r.Context(), s.db, id)
	if err == sql.ErrNoRows {
		back(w, r, "/files", "file not found", true)
		return
	}
	if err != nil {
		s.fail(w, r, err)
		return
	}
	if h.Available >= h.Total() {
		back(w, r, "/files/"+id, "All shards are healthy; nothing to repair.", false)
		return
	}
	queued, err := storage.EnqueueRepair(r.Context(), s.db, id, "admin", 0)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	s.audit(r, currentAdmin(r), "file.repair", "file", id, map[string]interface{}{"available": h.Available, "total": h.Total()})
	if !queued {
		back(w, r, "/files/"+id, "A repair job is already open for this file (or the file is not active).", true)
		return
	}
	back(w, r, "/files/"+id, fmt.Sprintf("Repair queued at top priority (%d of %d shards available).", h.Available, h.Total()), false)
}

// ---------------------------------------------------------------- repairs and audits

func (s *server) handleRepairs(w http.ResponseWriter, r *http.Request) {
	st := r.URL.Query().Get("status")
	where := "status IN ('queued', 'processing', 'awaiting_data')"
	switch st {
	case "failed", "completed":
		where = "status = '" + st + "'"
	case "all":
		where = ""
	default:
		st = "open"
	}
	jobs, err := s.queryRepairs(r.Context(), where, nil, 200)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	type count struct {
		Status string
		N      int
	}
	var repl []count
	rows, err := s.db.QueryContext(r.Context(), `SELECT status, count(*) FROM replication_jobs GROUP BY status ORDER BY status`)
	if err == nil {
		for rows.Next() {
			var c count
			if rows.Scan(&c.Status, &c.N) == nil {
				repl = append(repl, c)
			}
		}
		rows.Close()
	}
	s.render(w, r, "repairs.html", map[string]interface{}{"Jobs": jobs, "Status": st, "Replication": repl})
}

func (s *server) handleRepairRetry(w http.ResponseWriter, r *http.Request) {
	id, ok := idParam(w, r)
	if !ok {
		return
	}
	res, err := s.db.ExecContext(r.Context(), `
		UPDATE repair_queue SET status = 'queued', attempts = 0, last_error = NULL, updated_at = now(),
		       completed_at = NULL
		WHERE job_id = $1 AND status = 'failed'
		  AND NOT EXISTS (SELECT 1 FROM repair_queue o WHERE o.file_id = repair_queue.file_id
		                  AND o.status IN ('queued', 'processing', 'awaiting_data'))`, id)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	if n, _ := res.RowsAffected(); n == 0 {
		back(w, r, "/repairs?status=failed", "Job not retried: it is not failed, or the file already has an open repair.", true)
		return
	}
	s.audit(r, currentAdmin(r), "repair.retry", "repair", id, nil)
	back(w, r, "/repairs?status=failed", "Repair job queued again.", false)
}

type peerAuditStat struct {
	PeerID, Libp2p string
	Passed, Failed int
}

func (s *server) handleAudits(w http.ResponseWriter, r *http.Request) {
	var passed, failed, timeout, pending int
	s.db.QueryRowContext(r.Context(), `
		SELECT count(*) FILTER (WHERE status = 'passed'), count(*) FILTER (WHERE status = 'failed'),
		       count(*) FILTER (WHERE status = 'timeout'), count(*) FILTER (WHERE status = 'pending')
		FROM audit_logs WHERE challenged_at > now() - INTERVAL '7 days'`).Scan(&passed, &failed, &timeout, &pending)
	rows, err := s.db.QueryContext(r.Context(), `
		SELECT a.peer_id, COALESCE(p.libp2p_peer_id, ''),
		       count(*) FILTER (WHERE a.status = 'passed'), count(*) FILTER (WHERE a.status IN ('failed', 'timeout'))
		FROM audit_logs a JOIN peers p ON p.peer_id = a.peer_id
		WHERE a.challenged_at > now() - INTERVAL '7 days'
		GROUP BY a.peer_id, p.libp2p_peer_id
		HAVING count(*) FILTER (WHERE a.status IN ('failed', 'timeout')) > 0
		ORDER BY 4 DESC LIMIT 50`)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	var worst []peerAuditStat
	for rows.Next() {
		var p peerAuditStat
		if rows.Scan(&p.PeerID, &p.Libp2p, &p.Passed, &p.Failed) == nil {
			worst = append(worst, p)
		}
	}
	rows.Close()
	recent, err := s.queryAudits(r.Context(), "a.status IN ('failed', 'timeout')", nil, 50)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	s.render(w, r, "audits.html", map[string]interface{}{"Passed": passed, "Failed": failed, "Timeout": timeout,
		"Pending": pending, "Worst": worst, "Recent": recent})
}
