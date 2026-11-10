// Integration test: the real server services, a libp2p relay and seven peer
// agents on one machine, driven through the `orchestrator` CLI.
//
//	docker run -d --name crdb -p 26257:26257 cockroachdb/cockroach:v23.1.13 start-single-node --insecure
//	TEST_DATABASE_URL='postgresql://root@localhost:26257/defaultdb?sslmode=disable' go test ./test -v
//
// Skipped without TEST_DATABASE_URL.
package test

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"
	"testing"
	"time"

	_ "github.com/lib/pq"
	"gopkg.in/yaml.v3"
)

func editYAML(t *testing.T, path string, set map[string]interface{}) {
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	m := map[string]interface{}{}
	if err := yaml.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	for k, v := range set {
		m[k] = v
	}
	out, _ := yaml.Marshal(m)
	os.WriteFile(path, out, 0o600)
}

type env struct {
	t     *testing.T
	db    *sql.DB
	dbURL string
	bin   string // server binaries
	cli   string // orchestrator binary
	procs map[string]*exec.Cmd
	logs  map[string]string
	gw    string
	reg   string
	relay string
}

func freePort(t *testing.T) int {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func build(t *testing.T, dir, out, pkg string) {
	cmd := exec.Command("go", "build", "-o", out, pkg)
	cmd.Dir = dir
	if b, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build %s: %v\n%s", pkg, err, b)
	}
}

func setup(t *testing.T) *env {
	base := os.Getenv("TEST_DATABASE_URL")
	if base == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}
	admin, err := sql.Open("postgres", base)
	if err != nil {
		t.Fatal(err)
	}
	name := fmt.Sprintf("peer_it_%d", time.Now().UnixNano())
	if _, err := admin.Exec("CREATE DATABASE " + name); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { admin.Exec("DROP DATABASE " + name + " CASCADE"); admin.Close() })
	u, _ := url.Parse(base)
	u.Path = "/" + name
	e := &env{t: t, dbURL: u.String(), procs: map[string]*exec.Cmd{}, logs: map[string]string{}}
	e.db, _ = sql.Open("postgres", e.dbURL)
	t.Cleanup(func() { e.db.Close() })

	raw, err := os.ReadFile("../../server/infrastructure/schema.sql")
	if err != nil {
		t.Fatal(err)
	}
	var lines []string
	for _, l := range strings.Split(string(raw), "\n") {
		if !strings.HasPrefix(strings.TrimSpace(l), "--") {
			lines = append(lines, l)
		}
	}
	for _, stmt := range strings.Split(strings.Join(lines, "\n"), ";\n") {
		if strings.TrimSpace(stmt) != "" {
			if _, err := e.db.Exec(stmt); err != nil {
				t.Fatalf("schema: %v\n%s", err, stmt)
			}
		}
	}

	e.bin = t.TempDir()
	build(t, "../../server", e.bin+"/", "./services/...")
	e.cli = filepath.Join(t.TempDir(), "orchestrator")
	build(t, "..", e.cli, "./cmd/orchestrator")
	return e
}

func (e *env) start(name string, path string, args []string, envv ...string) {
	cmd := exec.Command(path, args...)
	cmd.Env = append(os.Environ(), envv...)
	logPath := filepath.Join(e.bin, name+".log")
	f, _ := os.Create(logPath)
	cmd.Stdout, cmd.Stderr = f, f
	if err := cmd.Start(); err != nil {
		e.t.Fatal(err)
	}
	e.procs[name] = cmd
	e.logs[name] = logPath
	e.t.Cleanup(func() {
		e.stop(name)
		f.Close()
		if e.t.Failed() {
			b, _ := os.ReadFile(logPath)
			if len(b) > 6000 {
				b = b[len(b)-6000:]
			}
			e.t.Logf("---- %s ----\n%s", name, b)
		}
	})
}

func (e *env) stop(name string) {
	if c := e.procs[name]; c != nil && c.ProcessState == nil {
		c.Process.Signal(os.Interrupt)
		done := make(chan struct{})
		go func() { c.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			c.Process.Kill()
		}
	}
}

func (e *env) service(name string, port int, extra ...string) {
	e.start(name, filepath.Join(e.bin, name), nil,
		append([]string{"DATABASE_URL=" + e.dbURL, fmt.Sprintf("GRPC_PORT=%d", port)}, extra...)...)
}

func (e *env) waitFor(what string, timeout time.Duration, cond func() bool) {
	e.t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			e.t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(300 * time.Millisecond)
	}
}

func (e *env) count(q string, args ...interface{}) int {
	var n int
	if err := e.db.QueryRow(q, args...).Scan(&n); err != nil {
		e.t.Fatalf("%s: %v", q, err)
	}
	return n
}

// cli runs the orchestrator binary with a config directory.
func (e *env) cli1(cfgDir string, args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, e.cli, append([]string{"--config", filepath.Join(cfgDir, "config.yaml")}, args...)...)
	// On timeout, SIGQUIT makes Go print every goroutine's stack.
	cmd.Cancel = func() error { return cmd.Process.Signal(syscall.SIGQUIT) }
	cmd.WaitDelay = 5 * time.Second
	cmd.Env = append(os.Environ(),
		"ORCHESTRATOR_EMAIL=it@example.com", "ORCHESTRATOR_PASSWORD=integration-pw",
		"ORCHESTRATOR_PASSPHRASE=integration passphrase")
	out, err := cmd.CombinedOutput()
	return string(out), err
}

func (e *env) mustCLI(cfgDir string, args ...string) string {
	e.t.Helper()
	out, err := e.cli1(cfgDir, args...)
	if err != nil {
		e.t.Fatalf("orchestrator %v: %v\n%s", args, err, out)
	}
	return out
}

func TestPeerNetwork(t *testing.T) {
	e := setup(t)

	// ---- server
	regPort, plPort, auPort, kmPort, blPort, gwPort := freePort(t), freePort(t), freePort(t), freePort(t), freePort(t), freePort(t)
	e.service("peer-registry", regPort)
	e.service("placement-service", plPort, "MIN_SHARD_COUNT=4", "PARITY_SHARDS=2", "MAX_PEERS_PER_ASN=0", "MIN_ASNS_PER_FILE=1")
	e.service("audit-service", auPort, "AUDIT_SCHEDULER_INTERVAL=2s", "AUDIT_FREQUENCY_HOURS=0", "CHALLENGE_TIMEOUT_SECONDS=60")
	e.service("key-metadata", kmPort)
	e.service("billing-service", blPort, "RUN_BACKGROUND_JOBS=false")
	e.start("api-gateway", filepath.Join(e.bin, "api-gateway"), nil,
		// Every agent here shares 127.0.0.1, so the per-client limit is raised.
		"DATABASE_URL="+e.dbURL, fmt.Sprintf("PORT=%d", gwPort), "JWT_SECRET=it", "RATE_LIMIT_REQUESTS_PER_MINUTE=100000",
		fmt.Sprintf("PEER_REGISTRY_ADDR=127.0.0.1:%d", regPort), fmt.Sprintf("PLACEMENT_ADDR=127.0.0.1:%d", plPort),
		fmt.Sprintf("AUDIT_ADDR=127.0.0.1:%d", auPort), fmt.Sprintf("KEY_METADATA_ADDR=127.0.0.1:%d", kmPort),
		fmt.Sprintf("BILLING_ADDR=127.0.0.1:%d", blPort))
	e.gw = fmt.Sprintf("http://127.0.0.1:%d", gwPort)
	e.reg = fmt.Sprintf("127.0.0.1:%d", regPort)
	e.waitFor("gateway", 60*time.Second, func() bool {
		r, err := http.Get(e.gw + "/ready")
		if err != nil {
			return false
		}
		r.Body.Close()
		return r.StatusCode == 200
	})

	// ---- relay with a fixed identity
	keyOut, err := exec.Command(filepath.Join(e.bin, "relay-service"), "-genkey").Output()
	if err != nil {
		t.Fatal(err)
	}
	keyFile := filepath.Join(t.TempDir(), "relay.key")
	os.WriteFile(keyFile, keyOut, 0o600)
	relayPort := freePort(t)
	e.start("relay", filepath.Join(e.bin, "relay-service"), nil, "KEY_FILE="+keyFile,
		fmt.Sprintf("PORT=%d", relayPort), fmt.Sprintf("ANNOUNCE_ADDRS=/ip4/127.0.0.1/tcp/%d", relayPort))
	e.waitFor("relay up", 20*time.Second, func() bool {
		b, _ := os.ReadFile(e.logs["relay"])
		m := regexp.MustCompile(`relay (12D3\w+) up`).FindSubmatch(b)
		if m != nil {
			e.relay = fmt.Sprintf("/ip4/127.0.0.1/tcp/%d/p2p/%s", relayPort, m[1])
		}
		return m != nil
	})

	// ---- seven agents; agent 6 is reachable only through the relay
	const nAgents = 7
	dirs := make([]string, nAgents)
	for i := range dirs {
		dirs[i] = t.TempDir()
		args := []string{"setup", "--non-interactive", "--gateway", e.gw, "--registry", e.reg, "--relay", e.relay,
			"--storage-dir", filepath.Join(dirs[i], "storage"), "--storage-gb", "1", "--port", "0", "--loopback"}
		e.mustCLI(dirs[i], args...)
		cfg := filepath.Join(dirs[i], "config.yaml")
		set := map[string]interface{}{"poll_interval": "1s", "heartbeat_interval": "10s"}
		if i == nAgents-1 {
			// Advertise only an unreachable address, so others must use the relay.
			set["announce_addrs"] = []string{"/ip4/192.0.2.123/tcp/9"}
		}
		editYAML(t, cfg, set)
		e.start(fmt.Sprintf("agent%d", i), e.cli, []string{"--config", cfg, "run"})
	}
	e.waitFor("agents registered", 60*time.Second, func() bool {
		return e.count(`SELECT count(*) FROM peers WHERE is_online`) == nAgents
	})
	e.waitFor("relay-only agent has a circuit address", 30*time.Second, func() bool {
		return e.count(`SELECT count(*) FROM peers WHERE multiaddr LIKE '/ip4/192.0.2.123%' AND array_to_string(multiaddrs, ' ') LIKE '%p2p-circuit%'`) == 1
	})

	// The client uses agent 0's config, so agent 0 must never get a shard.
	clientDir := dirs[0]
	t.Run("account", func(t *testing.T) {
		out := e.mustCLI(clientDir, "login", "--register")
		if !strings.Contains(out, "Logged in") {
			t.Fatal(out)
		}
	})

	payload := make([]byte, 3<<20+12345)
	rand.Read(payload)
	src := filepath.Join(t.TempDir(), "photos.tar")
	os.WriteFile(src, payload, 0o600)

	var fileID string
	t.Run("upload", func(t *testing.T) {
		out := e.mustCLI(clientDir, "upload", src)
		m := regexp.MustCompile(`as ([0-9a-f-]{36})`).FindStringSubmatch(out)
		if m == nil {
			t.Fatalf("no file id in output:\n%s", out)
		}
		fileID = m[1]
		if n := e.count(`SELECT count(*) FROM shards WHERE file_id = $1 AND health_status = 'good'`, fileID); n != 6 {
			t.Fatalf("%d good shards, want 6", n)
		}
		var status string
		e.db.QueryRow(`SELECT status FROM files WHERE file_id = $1`, fileID).Scan(&status)
		if status != "active" {
			t.Fatalf("file status %s", status)
		}
		var ownerShards int
		e.db.QueryRow(`SELECT count(*) FROM shards s JOIN peers p ON p.peer_id = s.peer_id
			WHERE s.file_id = $1 AND p.multiaddr LIKE '%' || (SELECT libp2p_peer_id FROM peers ORDER BY first_seen_at LIMIT 1) || '%'`, fileID).Scan(&ownerShards)
		if n := e.count(`SELECT count(*) FROM shards s JOIN peers p ON p.peer_id = s.peer_id
			WHERE s.file_id = $1 AND p.multiaddr LIKE '/ip4/192.0.2.123%'`, fileID); n != 1 {
			t.Fatalf("relay-only agent holds %d shards, want 1 (6 shards over 6 eligible peers)", n)
		}
		if n := e.count(`SELECT count(*) FROM audit_challenges c JOIN shards s ON s.shard_id = c.shard_id WHERE s.file_id = $1`, fileID); n != 48 {
			t.Fatalf("%d audit challenges registered, want 48", n)
		}
		out = e.mustCLI(clientDir, "ls")
		if !strings.Contains(out, "photos.tar") || !strings.Contains(out, fileID) {
			t.Fatalf("ls:\n%s", out)
		}
	})

	download := func(t *testing.T, label string) {
		dst := filepath.Join(t.TempDir(), label)
		out := e.mustCLI(clientDir, "download", fileID, "-o", dst)
		got, err := os.ReadFile(dst)
		if err != nil || !bytes.Equal(got, payload) {
			t.Fatalf("%s: downloaded file differs (%v)\n%s", label, err, out)
		}
	}
	t.Run("download", func(t *testing.T) { download(t, "copy1") })

	t.Run("wrong passphrase is refused", func(t *testing.T) {
		cmd := exec.Command(e.cli, "--config", filepath.Join(clientDir, "config.yaml"), "ls")
		cmd.Env = append(os.Environ(), "ORCHESTRATOR_PASSPHRASE=not the passphrase")
		out, err := cmd.CombinedOutput()
		if err == nil || !strings.Contains(string(out), "wrong passphrase") {
			t.Fatalf("expected wrong passphrase error, got %v\n%s", err, out)
		}
	})

	t.Run("agents answer audits", func(t *testing.T) {
		e.waitFor("passed audits on every shard", 60*time.Second, func() bool {
			return e.count(`SELECT count(DISTINCT a.shard_id) FROM audit_logs a JOIN shards s ON s.shard_id = a.shard_id
				WHERE s.file_id = $1 AND a.status = 'passed'`, fileID) == 6
		})
		if n := e.count(`SELECT count(*) FROM audit_logs WHERE status IN ('failed', 'timeout')`); n != 0 {
			t.Fatalf("%d audits failed", n)
		}
	})

	// Find the agent holding data shard 0 and stop it.
	var lostPeer, lostShard, lostHash string
	e.db.QueryRow(`SELECT peer_id, shard_id, shard_hash FROM shards WHERE file_id = $1 AND shard_index = 0`, fileID).
		Scan(&lostPeer, &lostShard, &lostHash)
	var lostLibp2p string
	e.db.QueryRow(`SELECT libp2p_peer_id FROM peers WHERE peer_id = $1`, lostPeer).Scan(&lostLibp2p)
	for i := range dirs {
		raw, _ := os.ReadFile(filepath.Join(dirs[i], "state.json"))
		if strings.Contains(string(raw), lostPeer) {
			e.stop(fmt.Sprintf("agent%d", i))
		}
	}

	t.Run("download with a data shard's peer gone", func(t *testing.T) { download(t, "copy2") })

	t.Run("repair rebuilds the lost shard on another agent", func(t *testing.T) {
		e.db.Exec(`UPDATE peers SET is_online = false, last_seen_at = now() - INTERVAL '8 days',
			last_heartbeat_at = now() - INTERVAL '8 days' WHERE peer_id = $1`, lostPeer)
		e.start("repair-worker", filepath.Join(e.bin, "repair-worker"), nil,
			"DATABASE_URL="+e.dbURL, "POLL_INTERVAL_SECONDS=1", "WORKER_ID=it")
		e.waitFor("rebuilt shard confirmed", 90*time.Second, func() bool {
			return e.count(`SELECT count(*) FROM shards WHERE file_id = $1 AND shard_index = 0
				AND health_status = 'good' AND peer_id != $2`, fileID, lostPeer) == 1
		})
		var newHash string
		e.db.QueryRow(`SELECT shard_hash FROM shards WHERE file_id = $1 AND shard_index = 0 AND health_status = 'good'`,
			fileID).Scan(&newHash)
		if newHash != lostHash {
			t.Fatalf("rebuilt shard hash %s differs from original %s", newHash, lostHash)
		}
		e.waitFor("repair job completed", 30*time.Second, func() bool {
			return e.count(`SELECT count(*) FROM repair_queue WHERE file_id = $1 AND status = 'completed'`, fileID) == 1
		})
		download(t, "copy3")
	})

	t.Run("delete removes shards from agents' disks", func(t *testing.T) {
		var shardIDs []string
		rows, _ := e.db.Query(`SELECT shard_id FROM shards WHERE file_id = $1 AND health_status = 'good'`, fileID)
		for rows.Next() {
			var id string
			rows.Scan(&id)
			shardIDs = append(shardIDs, id)
		}
		rows.Close()
		e.mustCLI(clientDir, "rm", fileID)
		e.waitFor("file deleted", 30*time.Second, func() bool {
			return e.count(`SELECT count(*) FROM files WHERE file_id = $1 AND status = 'deleted'`, fileID) == 1
		})
		for _, d := range dirs {
			for _, id := range shardIDs {
				if _, err := os.Stat(filepath.Join(d, "storage", "shards", id[:2], id)); err == nil {
					t.Fatalf("shard %s still on disk in %s", id, d)
				}
			}
		}
	})

	t.Run("status", func(t *testing.T) {
		out := e.mustCLI(dirs[1], "status")
		if !strings.Contains(out, "Server peer:") || !strings.Contains(out, "libp2p ID:") {
			t.Fatal(out)
		}
	})
}
