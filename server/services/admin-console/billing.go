package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/mux"

	billingpb "github.com/jermainekendall/disaggregated-storage/server/proto/billing"
)

type earningsLine struct {
	PeerID, Email, Status string
	Period                time.Time
	GBDays, BandwidthGB   float64
	Gross, Net            float64
}

func (s *server) queryEarnings(ctx context.Context, where string, arg interface{}) ([]earningsLine, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT e.peer_id, COALESCE(u.email, ''), e.status, e.period_start, e.storage_gb_days::FLOAT8,
		       e.bandwidth_gb::FLOAT8, e.gross_earnings_usd::FLOAT8, e.net_earnings_usd::FLOAT8
		FROM earnings e LEFT JOIN users u ON u.user_id = e.user_id
		WHERE `+where+` ORDER BY e.period_start DESC, e.net_earnings_usd DESC LIMIT 500`, arg)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []earningsLine
	for rows.Next() {
		var e earningsLine
		if err := rows.Scan(&e.PeerID, &e.Email, &e.Status, &e.Period, &e.GBDays, &e.BandwidthGB, &e.Gross, &e.Net); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

func (s *server) queryPayouts(ctx context.Context, where string, arg interface{}) ([]payoutRow, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT p.payout_id, p.user_id, COALESCE(u.email, ''), p.payment_method, p.status, COALESCE(p.failure_reason, ''),
		       p.amount_usd::FLOAT8, p.created_at, p.completed_at
		FROM payouts p LEFT JOIN users u ON u.user_id = p.user_id
		WHERE `+where+` ORDER BY p.created_at DESC LIMIT 200`, arg)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []payoutRow
	for rows.Next() {
		var p payoutRow
		if err := rows.Scan(&p.ID, &p.UserID, &p.Email, &p.Method, &p.Status, &p.Reason, &p.Amount, &p.Created, &p.Completed); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// period parses YYYY-MM, defaulting to the previous calendar month.
func period(v string) time.Time {
	if t, err := time.Parse("2006-01", v); err == nil {
		return t
	}
	now := time.Now().UTC()
	return time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC).AddDate(0, -1, 0)
}

func (s *server) handleBilling(w http.ResponseWriter, r *http.Request) {
	p := period(r.URL.Query().Get("period"))
	earnings, err := s.queryEarnings(r.Context(), "e.period_start = $1", p)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	var gbDays, gross, net float64
	pending := 0
	for _, e := range earnings {
		gbDays, gross, net = gbDays+e.GBDays, gross+e.Gross, net+e.Net
		if e.Status == "pending" {
			pending++
		}
	}
	pst := r.URL.Query().Get("payouts")
	where := "p.status IN ('pending', 'processing') OR $1 = 'x'"
	if pst == "all" {
		where = "$1 = 'all'"
	} else {
		pst = "open"
	}
	payouts, err := s.queryPayouts(r.Context(), where, pst)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	s.render(w, r, "billing.html", map[string]interface{}{
		"Period": p.Format("2006-01"), "PeriodName": p.Format("January 2006"),
		"Prev": p.AddDate(0, -1, 0).Format("2006-01"), "Next": p.AddDate(0, 1, 0).Format("2006-01"),
		"Earnings": earnings, "GBDays": gbDays, "Gross": gross, "Net": net, "PendingCount": pending,
		"Payouts": payouts, "PayoutFilter": pst,
	})
}

// handleBillingRun computes (or recomputes, while pending) earnings for every
// peer for one month through the billing service.
func (s *server) handleBillingRun(w http.ResponseWriter, r *http.Request) {
	p := period(r.PostFormValue("period"))
	if p.After(time.Now()) {
		back(w, r, "/billing?period="+p.Format("2006-01"), "cannot bill a future month", true)
		return
	}
	rows, err := s.db.QueryContext(r.Context(), `SELECT peer_id FROM peers`)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	var ids []string
	for rows.Next() {
		var id string
		if rows.Scan(&id) == nil {
			ids = append(ids, id)
		}
	}
	rows.Close()

	var mu sync.Mutex
	okCount, failed := 0, []string{}
	sem := make(chan struct{}, 8)
	var wg sync.WaitGroup
	for _, id := range ids {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
			defer cancel()
			_, err := s.billing.CalculateEarnings(ctx, &billingpb.CalculateEarningsRequest{
				PeerId: id, Year: int32(p.Year()), Month: int32(p.Month())})
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				failed = append(failed, id[:8]+": "+err.Error())
			} else {
				okCount++
			}
		}(id)
	}
	wg.Wait()
	s.audit(r, currentAdmin(r), "billing.run", "period", p.Format("2006-01"),
		map[string]interface{}{"peers": len(ids), "ok": okCount, "failed": len(failed)})
	msg := fmt.Sprintf("Billing for %s computed for %d of %d peers.", p.Format("January 2006"), okCount, len(ids))
	if len(failed) > 0 {
		if len(failed) > 3 {
			failed = append(failed[:3], "...")
		}
		back(w, r, "/billing?period="+p.Format("2006-01"), msg+" Failures: "+strings.Join(failed, "; "), true)
		return
	}
	back(w, r, "/billing?period="+p.Format("2006-01"), msg, false)
}

func (s *server) handleBillingApprove(w http.ResponseWriter, r *http.Request) {
	p := period(r.PostFormValue("period"))
	res, err := s.db.ExecContext(r.Context(), `
		UPDATE earnings SET status = 'approved', approved_at = now()
		WHERE period_start = $1 AND status = 'pending'`, p)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	n, _ := res.RowsAffected()
	s.audit(r, currentAdmin(r), "billing.approve", "period", p.Format("2006-01"), map[string]interface{}{"rows": n})
	back(w, r, "/billing?period="+p.Format("2006-01"), fmt.Sprintf("Approved %d earnings records for %s.", n, p.Format("January 2006")), false)
}

// handlePayoutStatus settles a payout by hand (payments are simulated in
// the test deployment, so this records what happened outside the system).
func (s *server) handlePayoutStatus(w http.ResponseWriter, r *http.Request) {
	id, ok := idParam(w, r)
	if !ok {
		return
	}
	st := r.PostFormValue("status")
	reason := strings.TrimSpace(r.PostFormValue("reason"))
	if st != "completed" && st != "failed" {
		back(w, r, "/billing", "status must be completed or failed", true)
		return
	}
	if st == "failed" && reason == "" {
		back(w, r, "/billing", "give a reason when marking a payout failed", true)
		return
	}
	res, err := s.db.ExecContext(r.Context(), `
		UPDATE payouts SET status = $2, failure_reason = $3,
		       completed_at = CASE WHEN $2 = 'completed' THEN now() ELSE completed_at END
		WHERE payout_id = $1 AND status IN ('pending', 'processing')`, id, st, sql.NullString{String: reason, Valid: reason != ""})
	if err != nil {
		s.fail(w, r, err)
		return
	}
	if n, _ := res.RowsAffected(); n == 0 {
		back(w, r, "/billing", "payout not found or already settled", true)
		return
	}
	s.audit(r, currentAdmin(r), "payout."+st, "payout", id, map[string]interface{}{"reason": reason})
	back(w, r, "/billing", "Payout marked "+st+".", false)
}

// ---------------------------------------------------------------- settings

type setting struct {
	Key, Value, Type, Description, UpdatedBy string
	Version                                  int
	Updated                                  time.Time
	Live                                     bool
}

// liveSettings are read by running services on every use; the rest are
// reference values (services take those from their deployment settings).
var liveSettings = map[string]bool{
	"consumer_earnings_rate_cold": true, "consumer_earnings_rate_warm": true, "consumer_earnings_rate_hot": true,
	"consumer_bandwidth_rate_gb": true, "payout_minimum_usd": true, "audit_failure_penalty_usd": true,
}

func (s *server) handleSettings(w http.ResponseWriter, r *http.Request) {
	rows, err := s.db.QueryContext(r.Context(), `
		SELECT config_key, config_value, config_type, COALESCE(description, ''), COALESCE(updated_by, ''),
		       version, updated_at FROM system_config ORDER BY config_key`)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	defer rows.Close()
	var live, ref []setting
	for rows.Next() {
		var x setting
		if err := rows.Scan(&x.Key, &x.Value, &x.Type, &x.Description, &x.UpdatedBy, &x.Version, &x.Updated); err != nil {
			s.fail(w, r, err)
			return
		}
		x.Live = liveSettings[x.Key]
		if x.Live {
			live = append(live, x)
		} else {
			ref = append(ref, x)
		}
	}
	s.render(w, r, "settings.html", map[string]interface{}{"Live": live, "Reference": ref})
}

func validSetting(typ, v string) error {
	switch typ {
	case "int":
		if _, err := strconv.ParseInt(v, 10, 64); err != nil {
			return fmt.Errorf("%q is not a whole number", v)
		}
	case "float":
		f, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return fmt.Errorf("%q is not a number", v)
		}
		if f < 0 {
			return fmt.Errorf("value cannot be negative")
		}
	case "bool":
		if _, err := strconv.ParseBool(v); err != nil {
			return fmt.Errorf("%q is not true or false", v)
		}
	case "json":
		if !json.Valid([]byte(v)) {
			return fmt.Errorf("value is not valid JSON")
		}
	}
	return nil
}

func (s *server) handleSettingUpdate(w http.ResponseWriter, r *http.Request) {
	key := mux.Vars(r)["key"]
	val := strings.TrimSpace(r.PostFormValue("value"))
	var typ, old string
	err := s.db.QueryRowContext(r.Context(), `SELECT config_type, config_value FROM system_config WHERE config_key = $1`, key).Scan(&typ, &old)
	if err == sql.ErrNoRows {
		back(w, r, "/settings", "unknown setting", true)
		return
	}
	if err != nil {
		s.fail(w, r, err)
		return
	}
	if err := validSetting(typ, val); err != nil {
		back(w, r, "/settings", key+": "+err.Error(), true)
		return
	}
	if val == old {
		back(w, r, "/settings", key+" unchanged", false)
		return
	}
	if _, err := s.db.ExecContext(r.Context(), `
		UPDATE system_config SET config_value = $2, version = version + 1, updated_at = now(), updated_by = $3
		WHERE config_key = $1`, key, val, currentAdmin(r).Email); err != nil {
		s.fail(w, r, err)
		return
	}
	s.audit(r, currentAdmin(r), "config.update", "config", key, map[string]interface{}{"from": old, "to": val})
	back(w, r, "/settings", fmt.Sprintf("%s changed from %s to %s", key, old, val), false)
}

func (s *server) handleLog(w http.ResponseWriter, r *http.Request) {
	q := strings.TrimSpace(r.URL.Query().Get("q"))
	page := pageNum(r)
	rows, err := s.db.QueryContext(r.Context(), `
		SELECT created_at, admin_email, action, COALESCE(target_type, ''), COALESCE(target_id, ''),
		       COALESCE(details::STRING, ''), COALESCE(client_ip, '')
		FROM admin_audit_log
		WHERE $1 = '' OR action LIKE $1 || '%' OR admin_email ILIKE '%' || $1 || '%' OR target_id LIKE $1 || '%'
		ORDER BY created_at DESC LIMIT $2 OFFSET $3`, q, pageSize+1, (page-1)*pageSize)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	defer rows.Close()
	var entries []logEntry
	for rows.Next() {
		var e logEntry
		if err := rows.Scan(&e.When, &e.Email, &e.Action, &e.Type, &e.Target, &e.Details, &e.IP); err != nil {
			s.fail(w, r, err)
			return
		}
		entries = append(entries, e)
	}
	more := len(entries) > pageSize
	if more {
		entries = entries[:pageSize]
	}
	s.render(w, r, "log.html", map[string]interface{}{"Entries": entries, "Q": q, "Page": page, "More": more})
}
