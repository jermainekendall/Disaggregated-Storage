package main

import (
	"context"
	"database/sql"
	"net/http"
	"strconv"
	"strings"
	"time"

	"golang.org/x/crypto/bcrypt"

	billingpb "github.com/jermainekendall/disaggregated-storage/server/proto/billing"
)

type dashStats struct {
	Users, ActiveUsers, SuspendedUsers, Admins             int
	Peers, OnlinePeers                                     int
	CapacityGB, FreeGB                                     float64
	FilesActive, FilesDegraded, FilesUploading, FilesOther int
	StoredBytes                                            int64
	ShardsGood, ShardsMissing, ShardsPending               int
	RepairsOpen, RepairsFailed                             int
	AuditsPassed, AuditsFailed                             int
	PendingEarnings, PendingPayouts                        float64
}

type logEntry struct {
	When                        time.Time
	Email, Action, Type, Target string
	Details, IP                 string
}

func (s *server) handleDashboard(w http.ResponseWriter, r *http.Request) {
	var d dashStats
	err := s.db.QueryRowContext(r.Context(), `
		SELECT
		  (SELECT count(*) FROM users),
		  (SELECT count(*) FROM users WHERE status = 'active'),
		  (SELECT count(*) FROM users WHERE status = 'suspended'),
		  (SELECT count(*) FROM users WHERE role = 'admin'),
		  (SELECT count(*) FROM peers),
		  (SELECT count(*) FROM peers WHERE is_online),
		  (SELECT COALESCE(sum(storage_total_gb), 0)::FLOAT8 FROM peers WHERE is_online),
		  (SELECT COALESCE(sum(storage_free_gb), 0)::FLOAT8 FROM peers WHERE is_online),
		  (SELECT count(*) FROM files WHERE status = 'active'),
		  (SELECT count(*) FROM files WHERE status = 'degraded'),
		  (SELECT count(*) FROM files WHERE status = 'uploading'),
		  (SELECT count(*) FROM files WHERE status NOT IN ('active', 'degraded', 'uploading', 'deleted')),
		  (SELECT COALESCE(sum(file_size_bytes), 0) FROM files WHERE status NOT IN ('deleting', 'deleted')),
		  (SELECT count(*) FROM shards WHERE health_status = 'good'),
		  (SELECT count(*) FROM shards s JOIN files f ON f.file_id = s.file_id
		     WHERE s.health_status = 'missing' AND f.status NOT IN ('deleting', 'deleted')),
		  (SELECT count(*) FROM shards WHERE health_status = 'pending'),
		  (SELECT count(*) FROM repair_queue WHERE status IN ('queued', 'processing', 'awaiting_data')),
		  (SELECT count(*) FROM repair_queue WHERE status = 'failed'),
		  (SELECT count(*) FROM audit_logs WHERE status = 'passed' AND verified_at > now() - INTERVAL '24 hours'),
		  (SELECT count(*) FROM audit_logs WHERE status IN ('failed', 'timeout') AND verified_at > now() - INTERVAL '24 hours'),
		  (SELECT COALESCE(sum(net_earnings_usd), 0)::FLOAT8 FROM earnings WHERE status = 'pending'),
		  (SELECT COALESCE(sum(amount_usd), 0)::FLOAT8 FROM payouts WHERE status IN ('pending', 'processing'))`).
		Scan(&d.Users, &d.ActiveUsers, &d.SuspendedUsers, &d.Admins, &d.Peers, &d.OnlinePeers,
			&d.CapacityGB, &d.FreeGB, &d.FilesActive, &d.FilesDegraded, &d.FilesUploading, &d.FilesOther,
			&d.StoredBytes, &d.ShardsGood, &d.ShardsMissing, &d.ShardsPending, &d.RepairsOpen, &d.RepairsFailed,
			&d.AuditsPassed, &d.AuditsFailed, &d.PendingEarnings, &d.PendingPayouts)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	recent, err := s.recentLog(r.Context(), 8, "", "")
	if err != nil {
		s.fail(w, r, err)
		return
	}
	s.render(w, r, "dashboard.html", map[string]interface{}{"S": d, "Log": recent})
}

func (s *server) recentLog(ctx context.Context, limit int, targetType, targetID string) ([]logEntry, error) {
	q := `SELECT created_at, admin_email, action, COALESCE(target_type, ''), COALESCE(target_id, ''),
	             COALESCE(details::STRING, ''), COALESCE(client_ip, '') FROM admin_audit_log`
	args := []interface{}{}
	if targetType != "" {
		q += ` WHERE target_type = $1 AND target_id = $2`
		args = append(args, targetType, targetID)
	}
	q += ` ORDER BY created_at DESC LIMIT ` + strconv.Itoa(limit)
	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []logEntry
	for rows.Next() {
		var e logEntry
		if err := rows.Scan(&e.When, &e.Email, &e.Action, &e.Type, &e.Target, &e.Details, &e.IP); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// ---------------------------------------------------------------- users

type userRow struct {
	ID, Email, Role, Tier, Status, PaymentMethod string
	QuotaGB                                      int
	UsedBytes                                    int64
	Files, Peers                                 int
	Created                                      time.Time
	LastLogin                                    sql.NullTime
}

const userCols = `u.user_id, u.email, u.role, u.tier, u.status, COALESCE(u.payment_method, ''), u.storage_quota_gb,
	(SELECT COALESCE(sum(file_size_bytes), 0) FROM files f WHERE f.owner_id = u.user_id AND f.status NOT IN ('deleting', 'deleted')),
	(SELECT count(*) FROM files f WHERE f.owner_id = u.user_id AND f.status NOT IN ('deleting', 'deleted')),
	(SELECT count(*) FROM peers p WHERE p.user_id = u.user_id),
	u.created_at, u.last_login_at`

func scanUser(sc interface{ Scan(...interface{}) error }) (userRow, error) {
	var u userRow
	err := sc.Scan(&u.ID, &u.Email, &u.Role, &u.Tier, &u.Status, &u.PaymentMethod, &u.QuotaGB, &u.UsedBytes,
		&u.Files, &u.Peers, &u.Created, &u.LastLogin)
	return u, err
}

func (s *server) handleUsers(w http.ResponseWriter, r *http.Request) {
	q := strings.TrimSpace(r.URL.Query().Get("q"))
	status := r.URL.Query().Get("status")
	page := pageNum(r)
	rows, err := s.db.QueryContext(r.Context(), `
		SELECT `+userCols+` FROM users u
		WHERE ($1 = '' OR u.email ILIKE '%' || $1 || '%')
		  AND ($2 = '' OR u.status = $2 OR ($2 = 'admin' AND u.role = 'admin'))
		ORDER BY u.created_at DESC LIMIT $3 OFFSET $4`, q, status, pageSize+1, (page-1)*pageSize)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	defer rows.Close()
	var users []userRow
	for rows.Next() {
		u, err := scanUser(rows)
		if err != nil {
			s.fail(w, r, err)
			return
		}
		users = append(users, u)
	}
	more := len(users) > pageSize
	if more {
		users = users[:pageSize]
	}
	s.render(w, r, "users.html", map[string]interface{}{"Users": users, "Q": q, "Status": status,
		"Page": page, "More": more})
}

type payoutRow struct {
	ID, UserID, Email, Method, Status, Reason string
	Amount                                    float64
	Created                                   time.Time
	Completed                                 sql.NullTime
}

func (s *server) handleUser(w http.ResponseWriter, r *http.Request) {
	id, ok := idParam(w, r)
	if !ok {
		return
	}
	u, err := scanUser(s.db.QueryRowContext(r.Context(), `SELECT `+userCols+` FROM users u WHERE u.user_id = $1`, id))
	if err == sql.ErrNoRows {
		http.Error(w, "user not found", http.StatusNotFound)
		return
	}
	if err != nil {
		s.fail(w, r, err)
		return
	}
	files, err := s.queryFiles(r.Context(), "f.owner_id = $1", id, 50, 0)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	peers, err := s.queryPeers(r.Context(), "p.user_id = $1", id, 50, 0)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	earnings, err := s.queryEarnings(r.Context(), "e.user_id = $1", id)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	payouts, err := s.queryPayouts(r.Context(), "p.user_id = $1", id)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	var balance *billingpb.BalanceResponse
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()
	if b, err := s.billing.GetBalance(ctx, &billingpb.BalanceRequest{UserId: id}); err == nil {
		balance = b
	}
	history, _ := s.recentLog(r.Context(), 20, "user", id)
	s.render(w, r, "user.html", map[string]interface{}{"U": u, "Files": files, "Peers": peers,
		"Earnings": earnings, "Payouts": payouts, "Balance": balance, "History": history,
		"Self": currentAdmin(r).UserID == id})
}

// userUpdate runs one guarded UPDATE on a user and records it.
func (s *server) userUpdate(w http.ResponseWriter, r *http.Request, action, query string, details map[string]interface{}, args ...interface{}) {
	id, ok := idParam(w, r)
	if !ok {
		return
	}
	res, err := s.db.ExecContext(r.Context(), query, append([]interface{}{id}, args...)...)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	if n, _ := res.RowsAffected(); n == 0 {
		back(w, r, "/users/"+id, "user not found", true)
		return
	}
	s.audit(r, currentAdmin(r), action, "user", id, details)
	back(w, r, "/users/"+id, "Saved: "+action, false)
}

func (s *server) handleUserStatus(w http.ResponseWriter, r *http.Request) {
	id := mux_id(r)
	st := r.PostFormValue("status")
	if st != "active" && st != "suspended" {
		back(w, r, "/users/"+id, "status must be active or suspended", true)
		return
	}
	if st == "suspended" && id == currentAdmin(r).UserID {
		back(w, r, "/users/"+id, "you cannot suspend your own account", true)
		return
	}
	s.userUpdate(w, r, "user."+map[string]string{"active": "activate", "suspended": "suspend"}[st],
		`UPDATE users SET status = $2, updated_at = now() WHERE user_id = $1`, map[string]interface{}{"status": st}, st)
}

func (s *server) handleUserTier(w http.ResponseWriter, r *http.Request) {
	tier := r.PostFormValue("tier")
	if tier != "free" && tier != "paid" {
		back(w, r, "/users/"+mux_id(r), "tier must be free or paid", true)
		return
	}
	s.userUpdate(w, r, "user.tier", `UPDATE users SET tier = $2, updated_at = now() WHERE user_id = $1`,
		map[string]interface{}{"tier": tier}, tier)
}

func (s *server) handleUserQuota(w http.ResponseWriter, r *http.Request) {
	q, err := strconv.Atoi(strings.TrimSpace(r.PostFormValue("quota_gb")))
	if err != nil || q < 0 || q > 1000000 {
		back(w, r, "/users/"+mux_id(r), "quota must be a whole number of GB between 0 and 1000000", true)
		return
	}
	s.userUpdate(w, r, "user.quota", `UPDATE users SET storage_quota_gb = $2, updated_at = now() WHERE user_id = $1`,
		map[string]interface{}{"quota_gb": q}, q)
}

func (s *server) handleUserRole(w http.ResponseWriter, r *http.Request) {
	id := mux_id(r)
	role := r.PostFormValue("role")
	if role != "user" && role != "admin" {
		back(w, r, "/users/"+id, "role must be user or admin", true)
		return
	}
	if role == "user" && id == currentAdmin(r).UserID {
		back(w, r, "/users/"+id, "you cannot remove your own admin access", true)
		return
	}
	s.userUpdate(w, r, "user.role", `UPDATE users SET role = $2, updated_at = now() WHERE user_id = $1`,
		map[string]interface{}{"role": role}, role)
}

func (s *server) handleUserPassword(w http.ResponseWriter, r *http.Request) {
	pw := r.PostFormValue("password")
	if len(pw) < 8 || len(pw) > 72 {
		back(w, r, "/users/"+mux_id(r), "password must be 8-72 characters", true)
		return
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(pw), bcrypt.DefaultCost)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	// The new password itself is never logged.
	s.userUpdate(w, r, "user.password_reset", `UPDATE users SET password_hash = $2, updated_at = now() WHERE user_id = $1`,
		nil, string(hash))
}
