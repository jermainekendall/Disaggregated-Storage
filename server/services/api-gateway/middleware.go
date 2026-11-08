package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"golang.org/x/time/rate"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ---------------------------------------------------------------- responses

type errorBody struct {
	Error   string `json:"error"`
	Message string `json:"message"`
}

func writeJSON(w http.ResponseWriter, code int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, code int, kind, msg string) {
	writeJSON(w, code, errorBody{Error: kind, Message: msg})
}

// writeGRPCError maps a backend gRPC error to an HTTP status.
func writeGRPCError(w http.ResponseWriter, err error) {
	st, _ := status.FromError(err)
	code := map[codes.Code]int{
		codes.InvalidArgument:    http.StatusBadRequest,
		codes.NotFound:           http.StatusNotFound,
		codes.AlreadyExists:      http.StatusConflict,
		codes.PermissionDenied:   http.StatusForbidden,
		codes.FailedPrecondition: http.StatusConflict,
		codes.Unavailable:        http.StatusServiceUnavailable,
		codes.DeadlineExceeded:   http.StatusGatewayTimeout,
	}[st.Code()]
	if code == 0 {
		code = http.StatusBadGateway
	}
	writeError(w, code, strings.ToLower(st.Code().String()), st.Message())
}

// decodeJSON reads a JSON body of at most 1 MiB and rejects unknown fields.
func decodeJSON(w http.ResponseWriter, r *http.Request, v interface{}) bool {
	dec := json.NewDecoder(io.LimitReader(r.Body, 1<<20))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", "invalid JSON body: "+err.Error())
		return false
	}
	return true
}

// ---------------------------------------------------------------- logging

type statusRecorder struct {
	http.ResponseWriter
	code int
}

func (s *statusRecorder) WriteHeader(code int) { s.code = code; s.ResponseWriter.WriteHeader(code) }

func logRequests(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, code: http.StatusOK}
		next.ServeHTTP(rec, r)
		if r.URL.Path != "/health" && r.URL.Path != "/ready" {
			log.Printf("%s %s %d %s", r.Method, r.URL.Path, rec.code, time.Since(start).Round(time.Millisecond))
		}
	})
}

// ---------------------------------------------------------------- rate limiting

// ipLimiter gives each client IP its own token bucket, so one noisy client
// cannot use up the budget for everyone.
type ipLimiter struct {
	mu      sync.Mutex
	perMin  int
	clients map[string]*clientLimit
}

type clientLimit struct {
	lim  *rate.Limiter
	seen time.Time
}

func newIPLimiter(perMinute int) *ipLimiter {
	if perMinute <= 0 {
		perMinute = 600
	}
	return &ipLimiter{perMin: perMinute, clients: map[string]*clientLimit{}}
}

func (l *ipLimiter) allow(ip string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	c, ok := l.clients[ip]
	if !ok {
		burst := l.perMin / 10
		if burst < 10 {
			burst = 10
		}
		c = &clientLimit{lim: rate.NewLimiter(rate.Limit(float64(l.perMin)/60), burst)}
		l.clients[ip] = c
	}
	c.seen = time.Now()
	return c.lim.Allow()
}

func (l *ipLimiter) cleanup(ctx context.Context) {
	t := time.NewTicker(5 * time.Minute)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			l.mu.Lock()
			for ip, c := range l.clients {
				if time.Since(c.seen) > 10*time.Minute {
					delete(l.clients, ip)
				}
			}
			l.mu.Unlock()
		}
	}
}

func clientIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

func (l *ipLimiter) middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !l.allow(clientIP(r)) {
			w.Header().Set("Retry-After", "1")
			writeError(w, http.StatusTooManyRequests, "rate_limited", "too many requests")
			return
		}
		next.ServeHTTP(w, r)
	})
}

// ---------------------------------------------------------------- auth

type ctxKey struct{}

func userID(r *http.Request) string { v, _ := r.Context().Value(ctxKey{}).(string); return v }

func (s *server) issueToken(uid string) (string, time.Time, error) {
	exp := time.Now().Add(s.cfg.TokenTTL)
	tok := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.RegisteredClaims{
		Subject:   uid,
		IssuedAt:  jwt.NewNumericDate(time.Now()),
		ExpiresAt: jwt.NewNumericDate(exp),
		Issuer:    "orchestrator-api-gateway",
	})
	signed, err := tok.SignedString(s.cfg.JWTSecret)
	return signed, exp, err
}

func (s *server) requireAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		if !ok || raw == "" {
			writeError(w, http.StatusUnauthorized, "unauthorized", "missing bearer token")
			return
		}
		claims := &jwt.RegisteredClaims{}
		_, err := jwt.ParseWithClaims(raw, claims, func(*jwt.Token) (interface{}, error) {
			return s.cfg.JWTSecret, nil
		}, jwt.WithValidMethods([]string{"HS256"}), jwt.WithExpirationRequired())
		if err != nil || claims.Subject == "" {
			msg := "invalid token"
			if errors.Is(err, jwt.ErrTokenExpired) {
				msg = "token expired"
			}
			writeError(w, http.StatusUnauthorized, "unauthorized", msg)
			return
		}
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), ctxKey{}, claims.Subject)))
	})
}
