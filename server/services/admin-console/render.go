package main

import (
	"database/sql"
	"fmt"
	"html/template"
	"io/fs"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/mux"
)

type pageTemplate = template.Template

var funcs = template.FuncMap{
	"bytes": humanBytes,
	"gb":    func(f float64) string { return fmt.Sprintf("%.2f GB", f) },
	"money": func(f float64) string { return fmt.Sprintf("$%.2f", f) },
	"money4": func(f float64) string {
		return fmt.Sprintf("$%.4f", f)
	},
	"pct": func(a, b int) string {
		if b == 0 {
			return "-"
		}
		return fmt.Sprintf("%.0f%%", float64(a)*100/float64(b))
	},
	"f1":    func(f float64) string { return fmt.Sprintf("%.1f", f) },
	"time":  fmtTime,
	"ntime": func(t sql.NullTime) string { return fmtNullTime(t) },
	"ago":   ago,
	"nago": func(t sql.NullTime) string {
		if !t.Valid {
			return "never"
		}
		return ago(t.Time)
	},
	"short": func(s string) string {
		if len(s) > 8 {
			return s[:8]
		}
		return s
	},
	"badge": badge,
	"add":   func(a, b int) int { return a + b },
	"dict": func(kv ...interface{}) map[string]interface{} {
		m := map[string]interface{}{}
		for i := 0; i+1 < len(kv); i += 2 {
			m[kv[i].(string)] = kv[i+1]
		}
		return m
	},
	"sub": func(a, b int) int { return a - b },
}

func humanBytes(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(b)/float64(div), "KMGTPE"[exp])
}

func fmtTime(t time.Time) string {
	if t.IsZero() {
		return "-"
	}
	return t.UTC().Format("2006-01-02 15:04 UTC")
}

func fmtNullTime(t sql.NullTime) string {
	if !t.Valid {
		return "-"
	}
	return fmtTime(t.Time)
}

func ago(t time.Time) string {
	if t.IsZero() {
		return "never"
	}
	d := time.Since(t)
	switch {
	case d < time.Minute:
		return "just now"
	case d < time.Hour:
		return fmt.Sprintf("%dm ago", int(d.Minutes()))
	case d < 48*time.Hour:
		return fmt.Sprintf("%dh ago", int(d.Hours()))
	default:
		return fmt.Sprintf("%dd ago", int(d.Hours()/24))
	}
}

// badge maps a status word to a CSS class so states are colour coded.
func badge(s string) string {
	switch s {
	case "active", "good", "passed", "completed", "approved", "paid", "online", "admin":
		return "ok"
	case "degraded", "pending", "queued", "processing", "awaiting_data", "uploading", "warm", "retiring":
		return "warn"
	case "suspended", "failed", "missing", "timeout", "offline", "deleted", "deleting", "cancelled":
		return "bad"
	}
	return "neutral"
}

// loadTemplates parses each page together with the shared layout.
func loadTemplates() (map[string]*pageTemplate, error) {
	pages := map[string]*pageTemplate{}
	entries, err := fs.ReadDir(assets, "templates")
	if err != nil {
		return nil, err
	}
	for _, e := range entries {
		name := e.Name()
		if name == "layout.html" {
			continue
		}
		t, err := template.New(name).Funcs(funcs).ParseFS(assets, "templates/layout.html", "templates/"+name)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", name, err)
		}
		pages[name] = t
	}
	return pages, nil
}

// render executes a page. data may be nil; the layout gets the signed-in
// admin, the CSRF token and any flash message from the query string.
func (s *server) render(w http.ResponseWriter, r *http.Request, page string, data map[string]interface{}) {
	if data == nil {
		data = map[string]interface{}{}
	}
	data["Admin"] = currentAdmin(r)
	if a := currentAdmin(r); a != nil {
		data["CSRF"] = a.CSRF
	}
	data["Path"] = r.URL.Path
	data["Msg"] = r.URL.Query().Get("msg")
	data["Err"] = r.URL.Query().Get("err")
	t, ok := s.pages[page]
	if !ok {
		http.Error(w, "unknown page "+page, http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := t.ExecuteTemplate(w, "layout", data); err != nil {
		log.Printf("render %s: %v", page, err)
	}
}

// back redirects to path with a flash message (or error).
func back(w http.ResponseWriter, r *http.Request, path, msg string, failed bool) {
	key := "msg"
	if failed {
		key = "err"
	}
	sep := "?"
	if strings.Contains(path, "?") {
		sep = "&"
	}
	http.Redirect(w, r, path+sep+key+"="+url.QueryEscape(msg), http.StatusSeeOther)
}

func (s *server) fail(w http.ResponseWriter, r *http.Request, err error) {
	log.Printf("%s %s: %v", r.Method, r.URL.Path, err)
	http.Error(w, "internal error: "+err.Error(), http.StatusInternalServerError)
}

func urlQueryEscape(s string) string { return url.QueryEscape(s) }

func pageNum(r *http.Request) int {
	p, _ := strconv.Atoi(r.URL.Query().Get("page"))
	if p < 1 {
		p = 1
	}
	return p
}

const pageSize = 50

func (s *server) routes() http.Handler {
	r := mux.NewRouter()
	static, _ := fs.Sub(assets, "static")
	r.PathPrefix("/static/").Handler(http.StripPrefix("/static/", http.FileServer(http.FS(static))))
	r.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		if err := s.db.PingContext(r.Context()); err != nil {
			http.Error(w, "database: "+err.Error(), http.StatusServiceUnavailable)
			return
		}
		w.Write([]byte("ok"))
	}).Methods("GET")
	r.HandleFunc("/login", s.handleLoginPage).Methods("GET")
	r.HandleFunc("/login", s.handleLogin).Methods("POST")

	a := r.NewRoute().Subrouter()
	a.Use(s.requireAdmin)
	a.HandleFunc("/logout", s.handleLogout).Methods("POST")
	a.HandleFunc("/", s.handleDashboard).Methods("GET")

	a.HandleFunc("/users", s.handleUsers).Methods("GET")
	a.HandleFunc("/users/{id}", s.handleUser).Methods("GET")
	a.HandleFunc("/users/{id}/status", s.handleUserStatus).Methods("POST")
	a.HandleFunc("/users/{id}/tier", s.handleUserTier).Methods("POST")
	a.HandleFunc("/users/{id}/quota", s.handleUserQuota).Methods("POST")
	a.HandleFunc("/users/{id}/role", s.handleUserRole).Methods("POST")
	a.HandleFunc("/users/{id}/password", s.handleUserPassword).Methods("POST")

	a.HandleFunc("/peers", s.handlePeers).Methods("GET")
	a.HandleFunc("/peers/{id}", s.handlePeer).Methods("GET")
	a.HandleFunc("/peers/{id}/drain", s.handlePeerDrain).Methods("POST")
	a.HandleFunc("/peers/{id}/health", s.handlePeerHealth).Methods("POST")

	a.HandleFunc("/files", s.handleFiles).Methods("GET")
	a.HandleFunc("/files/{id}", s.handleFile).Methods("GET")
	a.HandleFunc("/files/{id}/repair", s.handleFileRepair).Methods("POST")

	a.HandleFunc("/repairs", s.handleRepairs).Methods("GET")
	a.HandleFunc("/repairs/{id}/retry", s.handleRepairRetry).Methods("POST")
	a.HandleFunc("/audits", s.handleAudits).Methods("GET")

	a.HandleFunc("/billing", s.handleBilling).Methods("GET")
	a.HandleFunc("/billing/run", s.handleBillingRun).Methods("POST")
	a.HandleFunc("/billing/approve", s.handleBillingApprove).Methods("POST")
	a.HandleFunc("/payouts/{id}/status", s.handlePayoutStatus).Methods("POST")

	a.HandleFunc("/settings", s.handleSettings).Methods("GET")
	a.HandleFunc("/settings/{key}", s.handleSettingUpdate).Methods("POST")
	a.HandleFunc("/log", s.handleLog).Methods("GET")

	r.NotFoundHandler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "not found", http.StatusNotFound)
	})
	return securityHeaders(r)
}

// idParam returns the {id} path variable if it is a UUID.
func idParam(w http.ResponseWriter, r *http.Request) (string, bool) {
	id := mux.Vars(r)["id"]
	if len(id) != 36 || strings.Count(id, "-") != 4 {
		http.Error(w, "bad id", http.StatusBadRequest)
		return "", false
	}
	return id, true
}
