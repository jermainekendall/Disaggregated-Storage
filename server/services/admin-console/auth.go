package main

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"golang.org/x/crypto/bcrypt"
	"golang.org/x/time/rate"
)

const cookieName = "orch_admin"

// session is the signed payload of the session cookie.
type session struct {
	UserID  string `json:"u"`
	Expires int64  `json:"e"`
	Nonce   string `json:"n"` // per-login random value; the CSRF token derives from it
}

// admin is the signed-in operator, loaded from the database on every request.
type admin struct {
	UserID string
	Email  string
	CSRF   string
}

type ctxKey struct{}

func currentAdmin(r *http.Request) *admin {
	a, _ := r.Context().Value(ctxKey{}).(*admin)
	return a
}

func (s *server) sign(b []byte) string {
	m := hmac.New(sha256.New, s.cfg.SessionSecret)
	m.Write(b)
	return base64.RawURLEncoding.EncodeToString(m.Sum(nil))
}

func (s *server) encodeSession(ss session) string {
	raw, _ := json.Marshal(ss)
	p := base64.RawURLEncoding.EncodeToString(raw)
	return p + "." + s.sign([]byte(p))
}

func (s *server) decodeSession(v string) (session, bool) {
	var ss session
	p, sig, ok := strings.Cut(v, ".")
	if !ok || !hmac.Equal([]byte(sig), []byte(s.sign([]byte(p)))) {
		return ss, false
	}
	raw, err := base64.RawURLEncoding.DecodeString(p)
	if err != nil || json.Unmarshal(raw, &ss) != nil || time.Now().Unix() > ss.Expires {
		return ss, false
	}
	return ss, true
}

func (s *server) csrfToken(ss session) string {
	return s.sign([]byte("csrf:" + ss.Nonce))
}

func (s *server) setCookie(w http.ResponseWriter, value string, maxAge int) {
	http.SetCookie(w, &http.Cookie{
		Name: cookieName, Value: value, Path: "/", MaxAge: maxAge,
		HttpOnly: true, Secure: s.cfg.CookieSecure, SameSite: http.SameSiteStrictMode,
	})
}

// requireAdmin checks the session cookie, re-validates the user against the
// database, and verifies the CSRF token on state-changing requests.
func (s *server) requireAdmin(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := r.Cookie(cookieName)
		var ss session
		ok := err == nil
		if ok {
			ss, ok = s.decodeSession(c.Value)
		}
		var email string
		if ok {
			err := s.db.QueryRowContext(r.Context(), `
				SELECT email FROM users WHERE user_id = $1 AND role = 'admin' AND status = 'active'`,
				ss.UserID).Scan(&email)
			ok = err == nil
		}
		if !ok {
			s.setCookie(w, "", -1)
			if r.Method == http.MethodGet {
				http.Redirect(w, r, "/login?next="+urlQueryEscape(r.URL.RequestURI()), http.StatusSeeOther)
			} else {
				http.Error(w, "session expired, sign in again", http.StatusUnauthorized)
			}
			return
		}
		a := &admin{UserID: ss.UserID, Email: email, CSRF: s.csrfToken(ss)}
		if r.Method == http.MethodPost {
			if !hmac.Equal([]byte(r.PostFormValue("csrf")), []byte(a.CSRF)) {
				http.Error(w, "invalid form token; reload the page and try again", http.StatusForbidden)
				return
			}
		}
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), ctxKey{}, a)))
	})
}

// loginLimiter allows 10 sign-in attempts per minute per client address.
type loginLimiter struct {
	mu sync.Mutex
	m  map[string]*rate.Limiter
}

func newLoginLimiter() *loginLimiter { return &loginLimiter{m: map[string]*rate.Limiter{}} }

func (l *loginLimiter) allow(ip string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.m) > 10000 {
		l.m = map[string]*rate.Limiter{} // crude bound on memory
	}
	lim, ok := l.m[ip]
	if !ok {
		lim = rate.NewLimiter(rate.Every(6*time.Second), 10)
		l.m[ip] = lim
	}
	return lim.Allow()
}

func clientIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

func (s *server) handleLoginPage(w http.ResponseWriter, r *http.Request) {
	s.render(w, r, "login.html", map[string]interface{}{"Next": r.URL.Query().Get("next")})
}

func (s *server) handleLogin(w http.ResponseWriter, r *http.Request) {
	next := r.PostFormValue("next")
	if !strings.HasPrefix(next, "/") || strings.HasPrefix(next, "//") {
		next = "/"
	}
	fail := func(msg string) {
		w.WriteHeader(http.StatusUnauthorized)
		s.render(w, r, "login.html", map[string]interface{}{"Next": next, "Error": msg})
	}
	if !s.login.allow(clientIP(r)) {
		fail("Too many attempts. Wait a minute and try again.")
		return
	}
	email := strings.ToLower(strings.TrimSpace(r.PostFormValue("email")))
	var uid, hash, role, st string
	err := s.db.QueryRowContext(r.Context(),
		`SELECT user_id, password_hash, role, status FROM users WHERE email = $1`, email).Scan(&uid, &hash, &role, &st)
	if err == sql.ErrNoRows || (err == nil && bcrypt.CompareHashAndPassword([]byte(hash), []byte(r.PostFormValue("password"))) != nil) {
		fail("Wrong email or password.")
		return
	}
	if err != nil {
		fail("Sign-in is unavailable: " + err.Error())
		return
	}
	if role != "admin" || st != "active" {
		fail("This account does not have admin access.")
		return
	}
	nonce := make([]byte, 16)
	rand.Read(nonce)
	ss := session{UserID: uid, Expires: time.Now().Add(s.cfg.SessionTTL).Unix(), Nonce: hex.EncodeToString(nonce)}
	s.setCookie(w, s.encodeSession(ss), int(s.cfg.SessionTTL.Seconds()))
	s.db.ExecContext(r.Context(), `UPDATE users SET last_login_at = now() WHERE user_id = $1`, uid)
	s.audit(r, &admin{UserID: uid, Email: email}, "admin.login", "user", uid, nil)
	http.Redirect(w, r, next, http.StatusSeeOther)
}

func (s *server) handleLogout(w http.ResponseWriter, r *http.Request) {
	s.setCookie(w, "", -1)
	http.Redirect(w, r, "/login", http.StatusSeeOther)
}

// audit records an admin action. Failures are logged, not fatal.
func (s *server) audit(r *http.Request, a *admin, action, targetType, targetID string, details map[string]interface{}) {
	var raw interface{}
	if details != nil {
		b, _ := json.Marshal(details)
		raw = string(b)
	}
	if _, err := s.db.ExecContext(r.Context(), `
		INSERT INTO admin_audit_log (admin_id, admin_email, action, target_type, target_id, details, client_ip)
		VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		a.UserID, a.Email, action, targetType, targetID, raw, clientIP(r)); err != nil {
		fmt.Println("audit log:", err)
	}
}

// securityHeaders sets a strict content security policy and friends.
func securityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := w.Header()
		h.Set("Content-Security-Policy", "default-src 'self'; style-src 'self'; script-src 'self'; frame-ancestors 'none'; form-action 'self'; base-uri 'none'")
		h.Set("X-Frame-Options", "DENY")
		h.Set("X-Content-Type-Options", "nosniff")
		h.Set("Referrer-Policy", "same-origin")
		h.Set("Cache-Control", "no-store")
		next.ServeHTTP(w, r)
	})
}
