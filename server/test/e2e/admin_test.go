package e2e

import (
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"regexp"
	"strings"
	"testing"
	"time"
)

type browser struct {
	t    *testing.T
	base string
	c    *http.Client
}

func newBrowser(t *testing.T, base string) *browser {
	jar, _ := cookiejar.New(nil)
	return &browser{t: t, base: base, c: &http.Client{Jar: jar, Timeout: 60 * time.Second}}
}

func (b *browser) get(path string) (int, string) {
	resp, err := b.c.Get(b.base + path)
	if err != nil {
		b.t.Fatalf("GET %s: %v", path, err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(body)
}

func (b *browser) post(path string, form url.Values) (int, string, string) {
	resp, err := b.c.PostForm(b.base+path, form)
	if err != nil {
		b.t.Fatalf("POST %s: %v", path, err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, resp.Request.URL.String(), string(body)
}

var csrfRe = regexp.MustCompile(`name="csrf" value="([^"]+)"`)

// act loads page, takes its CSRF token and posts form to action.
func (b *browser) act(page, action string, form url.Values) (int, string, string) {
	b.t.Helper()
	_, html := b.get(page)
	m := csrfRe.FindStringSubmatch(html)
	if m == nil {
		b.t.Fatalf("no CSRF token on %s", page)
	}
	form.Set("csrf", m[1])
	return b.post(action, form)
}

func TestAdminConsole(t *testing.T) {
	e := setup(t)

	// Billing for the "run billing" action; the console renders without it too.
	billing := e.startGRPC("billing-service", "RUN_BACKGROUND_JOBS=false")
	port := freePort(t)
	e.start("admin-console", "PORT="+port, "SESSION_SECRET=test-session-secret-123456",
		"ADMIN_BOOTSTRAP_EMAIL=root@example.com", "ADMIN_BOOTSTRAP_PASSWORD=bootstrap-password-1",
		"BILLING_ADDR="+billing)
	base := "http://127.0.0.1:" + port
	e.waitFor("admin console", 30*time.Second, func() bool {
		r, err := http.Get(base + "/healthz")
		if err != nil {
			return false
		}
		r.Body.Close()
		return r.StatusCode == 200
	})

	// ---- seed data
	var userID, peerA, peerB, fileID string
	e.db.QueryRow(`INSERT INTO users (email, password_hash) VALUES ('alice@example.com', 'x') RETURNING user_id`).Scan(&userID)
	e.db.QueryRow(`INSERT INTO peers (user_id, libp2p_peer_id, multiaddr, asn, region, storage_total_gb, storage_free_gb, is_online, device_model)
		VALUES ($1, '12D3KooWAdminA', '/ip4/10.0.0.1/tcp/4001', 'AS1', 'gy', 100, 80, true, 'desk-a') RETURNING peer_id`, userID).Scan(&peerA)
	e.db.QueryRow(`INSERT INTO peers (libp2p_peer_id, multiaddr, asn, storage_total_gb, storage_free_gb, is_online)
		VALUES ('12D3KooWAdminB', '/ip4/10.0.0.2/tcp/4001', 'AS2', 50, 40, true) RETURNING peer_id`).Scan(&peerB)
	e.db.QueryRow(`INSERT INTO files (owner_id, file_name_encrypted, file_size_bytes, shard_count, parity_shards, status, available_shards)
		VALUES ($1, 'enc', 4096, 2, 1, 'active', 3) RETURNING file_id`, userID).Scan(&fileID)
	for i, p := range []string{peerA, peerB, peerB} {
		e.db.Exec(`INSERT INTO shards (file_id, peer_id, shard_index, shard_type, shard_size_bytes, health_status, shard_hash, stored_at)
			VALUES ($1, $2, $3, 'data', 2048, 'good', 'h', now() - INTERVAL '3 days')`, fileID, p, i)
	}
	e.db.Exec(`INSERT INTO payouts (user_id, amount_usd, period_start, period_end, payment_method, status)
		VALUES ($1, 12.5, '2026-01-01', '2026-02-01', 'paypal', 'pending')`, userID)

	b := newBrowser(t, base)

	t.Run("pages require sign-in", func(t *testing.T) {
		code, body := b.get("/users")
		if code != 200 || !strings.Contains(body, "Sign in with an administrator account") {
			t.Fatalf("expected login page, got %d", code)
		}
		if code, _, _ := b.post("/users/"+userID+"/status", url.Values{"status": {"suspended"}}); code != 401 {
			t.Fatalf("unauthenticated POST got %d", code)
		}
	})

	t.Run("non-admin and wrong password are refused", func(t *testing.T) {
		_, _, body := b.post("/login", url.Values{"email": {"root@example.com"}, "password": {"nope"}})
		if !strings.Contains(body, "Wrong email or password") {
			t.Fatal("wrong password accepted")
		}
	})

	t.Run("sign in", func(t *testing.T) {
		_, final, body := b.post("/login", url.Values{"email": {"root@example.com"},
			"password": {"bootstrap-password-1"}, "next": {"/peers"}})
		if !strings.HasSuffix(final, "/peers") || !strings.Contains(body, "desk-a") {
			t.Fatalf("sign-in did not land on /peers: %s", final)
		}
	})

	t.Run("every page renders", func(t *testing.T) {
		pages := map[string]string{
			"/":                   "Dashboard",
			"/users":              "alice@example.com",
			"/users/" + userID:    "alice@example.com",
			"/peers":              "desk-a",
			"/peers/" + peerA:     "12D3KooWAdminA",
			"/files":              fileID[:8],
			"/files/" + fileID:    "Shard map",
			"/repairs":            "Repairs",
			"/repairs?status=all": "Repairs",
			"/audits":             "Audits",
			"/billing":            "Billing",
			"/settings":           "consumer_earnings_rate_cold",
			"/log":                "admin.login",
			"/static/style.css":   "--accent",
		}
		for p, want := range pages {
			code, body := b.get(p)
			if code != 200 || !strings.Contains(body, want) {
				t.Errorf("%s: %d, missing %q", p, code, want)
			}
		}
	})

	t.Run("forms without a valid token are rejected", func(t *testing.T) {
		code, _, _ := b.post("/users/"+userID+"/status", url.Values{"status": {"suspended"}, "csrf": {"forged"}})
		if code != http.StatusForbidden {
			t.Fatalf("forged CSRF token got %d", code)
		}
	})

	t.Run("user management", func(t *testing.T) {
		page := "/users/" + userID
		if _, final, _ := b.act(page, page+"/quota", url.Values{"quota_gb": {"100"}}); !strings.Contains(final, "msg=") {
			t.Fatalf("quota: %s", final)
		}
		b.act(page, page+"/tier", url.Values{"tier": {"paid"}})
		b.act(page, page+"/status", url.Values{"status": {"suspended"}})
		var quota int
		var tier, st string
		e.db.QueryRow(`SELECT storage_quota_gb, tier, status FROM users WHERE user_id = $1`, userID).Scan(&quota, &tier, &st)
		if quota != 100 || tier != "paid" || st != "suspended" {
			t.Fatalf("user now %d %s %s", quota, tier, st)
		}
		if _, final, _ := b.act(page, page+"/quota", url.Values{"quota_gb": {"-5"}}); !strings.Contains(final, "err=") {
			t.Fatal("negative quota accepted")
		}
		b.act(page, page+"/password", url.Values{"password": {"new-password-123"}})
		var hash string
		e.db.QueryRow(`SELECT password_hash FROM users WHERE user_id = $1`, userID).Scan(&hash)
		if !strings.HasPrefix(hash, "$2") {
			t.Fatal("password not reset to a bcrypt hash")
		}
		var details string
		e.db.QueryRow(`SELECT COALESCE(details::STRING, '') FROM admin_audit_log WHERE action = 'user.password_reset'`).Scan(&details)
		if strings.Contains(details, "new-password-123") {
			t.Fatal("password written to the admin log")
		}
	})

	t.Run("admins cannot lock themselves out", func(t *testing.T) {
		var me string
		e.db.QueryRow(`SELECT user_id FROM users WHERE email = 'root@example.com'`).Scan(&me)
		page := "/users/" + me
		if _, final, _ := b.act(page, page+"/role", url.Values{"role": {"user"}}); !strings.Contains(final, "err=") {
			t.Fatal("admin removed own role")
		}
		if _, final, _ := b.act(page, page+"/status", url.Values{"status": {"suspended"}}); !strings.Contains(final, "err=") {
			t.Fatal("admin suspended self")
		}
	})

	t.Run("revoking admin takes effect immediately", func(t *testing.T) {
		// Make alice an admin, sign in as her, then revoke from the root session.
		page := "/users/" + userID
		b.act(page, page+"/status", url.Values{"status": {"active"}})
		b.act(page, page+"/role", url.Values{"role": {"admin"}})
		alice := newBrowser(t, base)
		_, final, _ := alice.post("/login", url.Values{"email": {"alice@example.com"}, "password": {"new-password-123"}})
		if strings.Contains(final, "/login") {
			t.Fatal("new admin could not sign in")
		}
		b.act(page, page+"/role", url.Values{"role": {"user"}})
		if _, body := alice.get("/users"); !strings.Contains(body, "Sign in with an administrator account") {
			t.Fatal("revoked admin still has access")
		}
	})

	t.Run("drain a peer queues repairs", func(t *testing.T) {
		page := "/peers/" + peerB
		_, final, _ := b.act(page, page+"/drain", url.Values{})
		if !strings.Contains(final, "msg=") {
			t.Fatalf("drain: %s", final)
		}
		var health float64
		e.db.QueryRow(`SELECT health_score::FLOAT8 FROM peers WHERE peer_id = $1`, peerB).Scan(&health)
		if health != 0 {
			t.Fatalf("health %v after drain", health)
		}
		if n := e.count(`SELECT count(*) FROM shards WHERE peer_id = $1 AND health_status = 'missing'`, peerB); n != 2 {
			t.Fatalf("%d shards marked missing, want 2", n)
		}
		if n := e.count(`SELECT count(*) FROM repair_queue WHERE file_id = $1 AND reason = 'admin_drain'`, fileID); n != 1 {
			t.Fatalf("%d repair jobs, want 1", n)
		}
		b.act(page, page+"/health", url.Values{"health": {"100"}})
		e.db.QueryRow(`SELECT health_score::FLOAT8 FROM peers WHERE peer_id = $1`, peerB).Scan(&health)
		if health != 100 {
			t.Fatalf("health %v after restore", health)
		}
	})

	t.Run("failed repair can be retried", func(t *testing.T) {
		e.db.Exec(`UPDATE repair_queue SET status = 'failed', last_error = 'test' WHERE file_id = $1`, fileID)
		var job string
		e.db.QueryRow(`SELECT job_id FROM repair_queue WHERE file_id = $1`, fileID).Scan(&job)
		b.act("/repairs?status=failed", "/repairs/"+job+"/retry", url.Values{})
		var st string
		e.db.QueryRow(`SELECT status FROM repair_queue WHERE job_id = $1`, job).Scan(&st)
		if st != "queued" {
			t.Fatalf("job status %s after retry", st)
		}
	})

	t.Run("billing run, approve and payout", func(t *testing.T) {
		period := time.Now().UTC().Format("2006-01")
		_, final, _ := b.act("/billing?period="+period, "/billing/run", url.Values{"period": {period}})
		if !strings.Contains(final, "computed+for+2+of+2") {
			t.Fatalf("billing run: %s", final)
		}
		if n := e.count(`SELECT count(*) FROM earnings WHERE status = 'pending'`); n != 2 {
			t.Fatalf("%d pending earnings", n)
		}
		b.act("/billing?period="+period, "/billing/approve", url.Values{"period": {period}})
		if n := e.count(`SELECT count(*) FROM earnings WHERE status = 'approved'`); n != 2 {
			t.Fatalf("%d approved earnings", n)
		}
		var payout string
		e.db.QueryRow(`SELECT payout_id FROM payouts LIMIT 1`).Scan(&payout)
		b.act("/billing", "/payouts/"+payout+"/status", url.Values{"status": {"completed"}})
		var st string
		e.db.QueryRow(`SELECT status FROM payouts WHERE payout_id = $1`, payout).Scan(&st)
		if st != "completed" {
			t.Fatalf("payout %s", st)
		}
	})

	t.Run("settings are validated and versioned", func(t *testing.T) {
		if _, final, _ := b.act("/settings", "/settings/payout_minimum_usd", url.Values{"value": {"abc"}}); !strings.Contains(final, "err=") {
			t.Fatal("non-numeric value accepted")
		}
		b.act("/settings", "/settings/payout_minimum_usd", url.Values{"value": {"7.5"}})
		var v, by string
		var version int
		e.db.QueryRow(`SELECT config_value, updated_by, version FROM system_config WHERE config_key = 'payout_minimum_usd'`).Scan(&v, &by, &version)
		if v != "7.5" || by != "root@example.com" || version != 2 {
			t.Fatalf("setting now %s by %s v%d", v, by, version)
		}
	})

	t.Run("actions are in the admin log", func(t *testing.T) {
		for _, a := range []string{"user.quota", "user.suspend", "user.role", "peer.drain", "peer.health",
			"repair.retry", "billing.run", "billing.approve", "payout.completed", "config.update"} {
			if n := e.count(`SELECT count(*) FROM admin_audit_log WHERE action = $1`, a); n == 0 {
				t.Errorf("no admin log entry for %s", a)
			}
		}
		code, body := b.get("/log?q=config.")
		if code != 200 || !strings.Contains(body, "payout_minimum_usd") {
			t.Fatal("log page does not show the config change")
		}
	})

	t.Run("security headers", func(t *testing.T) {
		resp, err := b.c.Get(base + "/")
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		for _, h := range []string{"Content-Security-Policy", "X-Frame-Options", "X-Content-Type-Options"} {
			if resp.Header.Get(h) == "" {
				t.Errorf("missing %s", h)
			}
		}
	})
}
