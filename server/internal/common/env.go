// Package common holds the helpers every Orchestrator service shares:
// configuration from the environment, database access, the schema
// self-check, transaction retries and the gRPC server wrapper.
package common

import (
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)

// Env returns the value of key, or def when it is unset or empty.
func Env(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

// EnvInt parses key as an integer. An unparsable value is logged and the
// default is used, so a typo in a manifest is visible instead of silent.
func EnvInt(key string, def int) int {
	v := Env(key, "")
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		log.Printf("config: %s=%q is not an integer, using %d", key, v, def)
		return def
	}
	return n
}

// EnvFloat parses key as a float64, falling back to def on error.
func EnvFloat(key string, def float64) float64 {
	v := Env(key, "")
	if v == "" {
		return def
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		log.Printf("config: %s=%q is not a number, using %g", key, v, def)
		return def
	}
	return f
}

// EnvBool parses key as a boolean (true/false/1/0/yes/no).
func EnvBool(key string, def bool) bool {
	v := strings.ToLower(Env(key, ""))
	switch v {
	case "":
		return def
	case "1", "true", "yes", "on":
		return true
	case "0", "false", "no", "off":
		return false
	}
	log.Printf("config: %s=%q is not a boolean, using %t", key, v, def)
	return def
}

// EnvDuration reads key as a Go duration ("90s", "5m"). A bare integer is
// taken as seconds, which matches the *_SECONDS variables in the manifests.
func EnvDuration(key string, def time.Duration) time.Duration {
	v := Env(key, "")
	if v == "" {
		return def
	}
	if n, err := strconv.Atoi(v); err == nil {
		return time.Duration(n) * time.Second
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		log.Printf("config: %s=%q is not a duration, using %s", key, v, def)
		return def
	}
	return d
}
