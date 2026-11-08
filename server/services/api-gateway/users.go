package main

import (
	"context"
	"database/sql"
	"net/http"
	"net/mail"
	"strings"
	"time"

	"golang.org/x/crypto/bcrypt"

	"github.com/jermainekendall/disaggregated-storage/server/internal/common"
	billingpb "github.com/jermainekendall/disaggregated-storage/server/proto/billing"
)

type credentials struct {
	Email    string `json:"email"`
	Password string `json:"password"`
}

func (s *server) handleRegister(w http.ResponseWriter, r *http.Request) {
	var c credentials
	if !decodeJSON(w, r, &c) {
		return
	}
	c.Email = strings.ToLower(strings.TrimSpace(c.Email))
	if _, err := mail.ParseAddress(c.Email); err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", "a valid email is required")
		return
	}
	if len(c.Password) < 8 || len(c.Password) > 72 {
		writeError(w, http.StatusBadRequest, "bad_request", "password must be 8-72 characters")
		return
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(c.Password), bcrypt.DefaultCost)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal", "could not hash password")
		return
	}
	var uid string
	err = s.db.QueryRowContext(r.Context(),
		`INSERT INTO users (email, password_hash) VALUES ($1, $2) RETURNING user_id`, c.Email, string(hash)).Scan(&uid)
	if common.IsUniqueViolation(err) {
		writeError(w, http.StatusConflict, "conflict", "email already registered")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	token, exp, err := s.issueToken(uid)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal", "could not issue token")
		return
	}
	writeJSON(w, http.StatusCreated, map[string]interface{}{"user_id": uid, "token": token, "expires_at": exp.Unix()})
}

func (s *server) handleLogin(w http.ResponseWriter, r *http.Request) {
	var c credentials
	if !decodeJSON(w, r, &c) {
		return
	}
	var uid, hash, st string
	err := s.db.QueryRowContext(r.Context(),
		`SELECT user_id, password_hash, status FROM users WHERE email = $1`,
		strings.ToLower(strings.TrimSpace(c.Email))).Scan(&uid, &hash, &st)
	if err == sql.ErrNoRows || (err == nil && bcrypt.CompareHashAndPassword([]byte(hash), []byte(c.Password)) != nil) {
		writeError(w, http.StatusUnauthorized, "unauthorized", "wrong email or password")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	if st != "active" {
		writeError(w, http.StatusForbidden, "forbidden", "account is "+st)
		return
	}
	s.db.ExecContext(r.Context(), `UPDATE users SET last_login_at = now() WHERE user_id = $1`, uid)
	token, exp, err := s.issueToken(uid)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal", "could not issue token")
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"user_id": uid, "token": token, "expires_at": exp.Unix()})
}

func (s *server) handleMe(w http.ResponseWriter, r *http.Request) {
	var u struct {
		UserID    string    `json:"user_id"`
		Email     string    `json:"email"`
		Tier      string    `json:"tier"`
		Status    string    `json:"status"`
		QuotaGB   int       `json:"storage_quota_gb"`
		UsedBytes int64     `json:"storage_used_bytes"`
		CreatedAt time.Time `json:"created_at"`
	}
	err := s.db.QueryRowContext(r.Context(), `
		SELECT u.user_id, u.email, u.tier, u.status, u.storage_quota_gb, u.created_at,
		       (SELECT COALESCE(sum(file_size_bytes), 0) FROM files
		        WHERE owner_id = u.user_id AND status NOT IN ('deleting', 'deleted'))
		FROM users u WHERE u.user_id = $1`, userID(r)).
		Scan(&u.UserID, &u.Email, &u.Tier, &u.Status, &u.QuotaGB, &u.CreatedAt, &u.UsedBytes)
	if err == sql.ErrNoRows {
		writeError(w, http.StatusNotFound, "not_found", "user no longer exists")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, u)
}

func (s *server) handleEarnings(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	bal, err := s.billing.GetBalance(ctx, &billingpb.BalanceRequest{UserId: userID(r)})
	if err != nil {
		writeGRPCError(w, err)
		return
	}
	list, err := s.billing.ListEarnings(ctx, &billingpb.ListEarningsRequest{UserId: userID(r), Limit: 12})
	if err != nil {
		writeGRPCError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"balance": bal, "earnings": list.Earnings})
}
