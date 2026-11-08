package common

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"net/url"
	"regexp"
	"time"

	"github.com/lib/pq"
)

// DSN builds the connection string from DATABASE_URL, or from DB_HOST,
// DB_PORT, DB_NAME, DB_USER, DB_PASSWORD and DB_SSLMODE. The test cluster runs
// CockroachDB in insecure mode, so DB_SSLMODE defaults to "disable".
func DSN() string {
	if u := Env("DATABASE_URL", ""); u != "" {
		return u
	}
	u := url.URL{
		Scheme: "postgresql",
		Host:   Env("DB_HOST", "cockroachdb-public") + ":" + Env("DB_PORT", "26257"),
		Path:   "/" + Env("DB_NAME", "orchestrator"),
	}
	user, pass := Env("DB_USER", "root"), Env("DB_PASSWORD", "")
	if pass != "" {
		u.User = url.UserPassword(user, pass)
	} else {
		u.User = url.User(user)
	}
	q := url.Values{}
	q.Set("sslmode", Env("DB_SSLMODE", "disable"))
	q.Set("application_name", Env("SERVICE_NAME", "orchestrator"))
	u.RawQuery = q.Encode()
	return u.String()
}

// OpenDB connects to the database, retrying while it starts up (pods often
// start before CockroachDB is ready), and sets pool limits.
func OpenDB(ctx context.Context) (*sql.DB, error) {
	db, err := sql.Open("postgres", DSN())
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(EnvInt("DB_MAX_OPEN_CONNS", 10))
	db.SetMaxIdleConns(EnvInt("DB_MAX_IDLE_CONNS", 5))
	db.SetConnMaxLifetime(5 * time.Minute)

	deadline := time.Now().Add(EnvDuration("DB_CONNECT_TIMEOUT", 2*time.Minute))
	for {
		pctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		err = db.PingContext(pctx)
		cancel()
		if err == nil {
			return db, nil
		}
		if time.Now().After(deadline) || ctx.Err() != nil {
			db.Close()
			return nil, fmt.Errorf("database not reachable: %w", err)
		}
		log.Printf("waiting for database: %v", err)
		time.Sleep(2 * time.Second)
	}
}

// MustOpenDB opens the database and checks the schema, exiting on failure.
// required maps table names to the columns the caller uses.
func MustOpenDB(ctx context.Context, required map[string][]string) *sql.DB {
	db, err := OpenDB(ctx)
	if err != nil {
		log.Fatalf("database: %v", err)
	}
	if err := RequireSchema(ctx, db, required); err != nil {
		log.Fatalf("schema check failed (apply infrastructure/schema.sql): %v", err)
	}
	log.Println("connected to database, schema check passed")
	return db
}

// RunTx runs fn in a transaction and retries it when CockroachDB reports a
// serialization conflict (SQLSTATE 40001), as its docs require.
func RunTx(ctx context.Context, db *sql.DB, fn func(*sql.Tx) error) error {
	const maxAttempts = 5
	var err error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		var tx *sql.Tx
		tx, err = db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		if err = fn(tx); err == nil {
			if err = tx.Commit(); err == nil {
				return nil
			}
		} else {
			tx.Rollback()
		}
		if !IsRetryable(err) {
			return err
		}
		time.Sleep(time.Duration(attempt*attempt) * 20 * time.Millisecond)
	}
	return err
}

// IsRetryable reports whether err is a CockroachDB transaction retry error.
func IsRetryable(err error) bool {
	var pqErr *pq.Error
	return errors.As(err, &pqErr) && pqErr.Code == "40001"
}

// IsUniqueViolation reports whether err is a unique constraint violation.
func IsUniqueViolation(err error) bool {
	var pqErr *pq.Error
	return errors.As(err, &pqErr) && pqErr.Code == "23505"
}

var uuidRe = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

// IsUUID reports whether s is a canonical UUID string.
func IsUUID(s string) bool { return uuidRe.MatchString(s) }

// NullIfEmpty turns "" into a SQL NULL.
func NullIfEmpty(s string) sql.NullString {
	return sql.NullString{String: s, Valid: s != ""}
}

// Unix returns t as unix seconds, or 0 for a NULL time.
func Unix(t sql.NullTime) int64 {
	if !t.Valid {
		return 0
	}
	return t.Time.Unix()
}
