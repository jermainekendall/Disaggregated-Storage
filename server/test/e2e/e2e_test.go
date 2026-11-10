// End-to-end test: builds every service, starts them against a real
// CockroachDB, and drives the main flows (peer registration, upload and
// shard confirmation, audits, repair, replication, billing, keys, deletion).
//
// It needs a CockroachDB it may create databases on:
//
//	docker run -d --name crdb -p 26257:26257 cockroachdb/cockroach:v23.1.13 start-single-node --insecure
//	TEST_DATABASE_URL='postgresql://root@localhost:26257/defaultdb?sslmode=disable' go test ./test/e2e -v
//
// Without TEST_DATABASE_URL the test is skipped.
package e2e

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	_ "github.com/lib/pq"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"

	auditpb "github.com/jermainekendall/disaggregated-storage/server/proto/audit"
	billingpb "github.com/jermainekendall/disaggregated-storage/server/proto/billing"
	keypb "github.com/jermainekendall/disaggregated-storage/server/proto/keymeta"
	peerpb "github.com/jermainekendall/disaggregated-storage/server/proto/peer"
)

type env struct {
	t       *testing.T
	db      *sql.DB
	dbURL   string
	bin     string
	gateway string
	addrs   map[string]string
}

func freePort(t *testing.T) string {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return fmt.Sprint(l.Addr().(*net.TCPAddr).Port)
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
	dbName := fmt.Sprintf("e2e_%d", time.Now().UnixNano())
	if _, err := admin.Exec("CREATE DATABASE " + dbName); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { admin.Exec("DROP DATABASE " + dbName + " CASCADE"); admin.Close() })

	u, _ := url.Parse(base)
	u.Path = "/" + dbName
	e := &env{t: t, dbURL: u.String(), addrs: map[string]string{}}
	if e.db, err = sql.Open("postgres", e.dbURL); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { e.db.Close() })
	e.applySchema()

	e.bin = t.TempDir()
	build := exec.Command("go", "build", "-o", e.bin+"/", "./services/...")
	build.Dir = "../.."
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	return e
}

// applySchema runs schema.sql one statement at a time, as the cockroach CLI does.
func (e *env) applySchema() {
	raw, err := os.ReadFile("../../infrastructure/schema.sql")
	if err != nil {
		e.t.Fatal(err)
	}
	var lines []string
	for _, l := range strings.Split(string(raw), "\n") {
		if !strings.HasPrefix(strings.TrimSpace(l), "--") {
			lines = append(lines, l)
		}
	}
	for _, stmt := range strings.Split(strings.Join(lines, "\n"), ";\n") {
		if strings.TrimSpace(stmt) == "" {
			continue
		}
		if _, err := e.db.Exec(stmt); err != nil {
			e.t.Fatalf("schema statement failed: %v\n%s", err, stmt)
		}
	}
}

// start runs a service binary with the given env until the test ends.
func (e *env) start(name string, extra ...string) {
	cmd := exec.Command(filepath.Join(e.bin, name))
	cmd.Env = append(os.Environ(), "DATABASE_URL="+e.dbURL, "SERVICE_NAME="+name)
	cmd.Env = append(cmd.Env, extra...)
	logf, _ := os.Create(filepath.Join(e.bin, name+".log"))
	cmd.Stdout, cmd.Stderr = logf, logf
	if err := cmd.Start(); err != nil {
		e.t.Fatal(err)
	}
	e.t.Cleanup(func() {
		cmd.Process.Signal(os.Interrupt)
		cmd.Wait()
		logf.Close()
		if e.t.Failed() {
			b, _ := os.ReadFile(logf.Name())
			e.t.Logf("---- %s log ----\n%s", name, b)
		}
	})
}

func (e *env) startGRPC(name string, extra ...string) string {
	port := freePort(e.t)
	e.start(name, append(extra, "GRPC_PORT="+port)...)
	addr := "127.0.0.1:" + port
	conn := e.dial(addr)
	deadline := time.Now().Add(30 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		resp, err := healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{})
		cancel()
		if err == nil && resp.Status == healthpb.HealthCheckResponse_SERVING {
			break
		}
		if time.Now().After(deadline) {
			e.t.Fatalf("%s did not become healthy: %v", name, err)
		}
		time.Sleep(200 * time.Millisecond)
	}
	e.addrs[name] = addr
	return addr
}

func (e *env) dial(addr string) *grpc.ClientConn {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		e.t.Fatal(err)
	}
	e.t.Cleanup(func() { conn.Close() })
	return conn
}

func (e *env) http(method, path, token string, body interface{}, out interface{}) int {
	var r io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		r = bytes.NewReader(b)
	}
	req, _ := http.NewRequest(method, e.gateway+path, r)
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		e.t.Fatalf("%s %s: %v", method, path, err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if out != nil {
		if err := json.Unmarshal(raw, out); err != nil {
			e.t.Fatalf("%s %s: decode %q: %v", method, path, raw, err)
		}
	}
	return resp.StatusCode
}

// waitFor polls cond until it is true or the timeout passes.
func (e *env) waitFor(what string, timeout time.Duration, cond func() bool) {
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			e.t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(250 * time.Millisecond)
	}
}

func (e *env) count(q string, args ...interface{}) int {
	var n int
	if err := e.db.QueryRow(q, args...).Scan(&n); err != nil {
		e.t.Fatalf("%s: %v", q, err)
	}
	return n
}

func TestEndToEnd(t *testing.T) {
	e := setup(t)
	ctx := context.Background()

	registry := e.startGRPC("peer-registry", "HEARTBEAT_TIMEOUT_SECONDS=900")
	placement := e.startGRPC("placement-service",
		"MIN_SHARD_COUNT=4", "PARITY_SHARDS=2", "MAX_PEERS_PER_ASN=2", "MIN_ASNS_PER_FILE=3")
	auditAddr := e.startGRPC("audit-service", "RUN_BACKGROUND_JOBS=false", "CHALLENGE_TIMEOUT_SECONDS=300")
	billingAddr := e.startGRPC("billing-service", "RUN_BACKGROUND_JOBS=false", "PAYMENT_SIMULATION_DELAY=1")
	keysAddr := e.startGRPC("key-metadata", "RUN_BACKGROUND_JOBS=false")

	gwPort := freePort(t)
	e.start("api-gateway", "PORT="+gwPort, "JWT_SECRET=test-secret",
		"PEER_REGISTRY_ADDR="+registry, "PLACEMENT_ADDR="+placement, "BILLING_ADDR="+billingAddr,
		"AUDIT_ADDR="+auditAddr, "KEY_METADATA_ADDR="+keysAddr)
	e.gateway = "http://127.0.0.1:" + gwPort
	e.waitFor("gateway ready", 30*time.Second, func() bool {
		resp, err := http.Get(e.gateway + "/ready")
		if err != nil {
			return false
		}
		resp.Body.Close()
		return resp.StatusCode == 200
	})

	peers := peerpb.NewPeerRegistryClient(e.dial(registry))
	audits := auditpb.NewAuditServiceClient(e.dial(auditAddr))
	billing := billingpb.NewBillingServiceClient(e.dial(billingAddr))
	keys := keypb.NewKeyMetadataServiceClient(e.dial(keysAddr))

	// ---- peers: 9 peers over 4 ASNs, registered over HTTP (forwarded to gRPC)
	peerIDs := map[string]string{} // libp2p id -> uuid
	for i := 0; i < 9; i++ {
		lp := fmt.Sprintf("12D3KooWTestPeer%02d", i)
		var resp struct {
			Success bool   `json:"success"`
			PeerID  string `json:"peer_id"`
			Message string `json:"message"`
		}
		code := e.http("POST", "/api/v1/peers/register", "", map[string]interface{}{
			"libp2p_peer_id":           lp,
			"multiaddrs":               []string{fmt.Sprintf("/ip4/10.0.0.%d/tcp/4001/p2p/%s", i+1, lp)},
			"total_capacity_bytes":     int64(100) << 30,
			"available_capacity_bytes": int64(80) << 30,
			"asn":                      fmt.Sprintf("AS%d", 64500+i%4),
			"region":                   "test",
			"timezone":                 "America/Guyana",
		}, &resp)
		if code != 200 || !resp.Success {
			t.Fatalf("register peer %d: %d %+v", i, code, resp)
		}
		peerIDs[lp] = resp.PeerID
	}

	t.Run("re-registration keeps the peer id and heartbeat keeps addresses", func(t *testing.T) {
		lp := "12D3KooWTestPeer00"
		r, err := peers.RegisterPeer(ctx, &peerpb.PeerRegistration{Libp2PPeerId: lp, Asn: "AS64500",
			TotalCapacityBytes: 100 << 30, AvailableCapacityBytes: 80 << 30})
		if err != nil || !r.Success || r.PeerId != peerIDs[lp] {
			t.Fatalf("re-register: %v %+v (want %s)", err, r, peerIDs[lp])
		}
		hb, err := peers.Heartbeat(ctx, &peerpb.HeartbeatRequest{PeerId: lp, AvailableCapacityBytes: 79 << 30})
		if err != nil || !hb.Success {
			t.Fatalf("heartbeat by libp2p id: %v %+v", err, hb)
		}
		p, err := peers.GetPeer(ctx, &peerpb.PeerRequest{PeerId: lp})
		if err != nil {
			t.Fatal(err)
		}
		if len(p.Multiaddrs) != 1 || p.Multiaddr == "" || p.Timezone != "America/Guyana" {
			t.Fatalf("addresses lost or timezone wrong: %+v", p)
		}
		bad, _ := peers.RegisterPeer(ctx, &peerpb.PeerRegistration{PeerId: "not-a-uuid"})
		if bad.Success {
			t.Fatal("non-UUID peer_id accepted")
		}
		list, err := peers.ListPeers(ctx, &peerpb.ListPeersRequest{Limit: 2})
		if err != nil || len(list.Peers) != 2 || list.TotalCount != 9 {
			t.Fatalf("list: %v, %d peers, total %d", err, len(list.Peers), list.TotalCount)
		}
	})

	// ---- user and upload
	var auth struct {
		UserID string `json:"user_id"`
		Token  string `json:"token"`
	}
	if code := e.http("POST", "/api/v1/auth/register", "", map[string]string{"email": "tester@example.com", "password": "correct horse"}, &auth); code != 201 {
		t.Fatalf("register user: %d", code)
	}
	if code := e.http("POST", "/api/v1/auth/login", "", map[string]string{"email": "tester@example.com", "password": "wrong password"}, nil); code != 401 {
		t.Fatalf("bad password accepted: %d", code)
	}
	if code := e.http("GET", "/api/v1/files", "", nil, nil); code != 401 {
		t.Fatalf("unauthenticated request allowed: %d", code)
	}

	type assignment struct {
		ShardID    string `json:"shard_id"`
		ShardIndex int    `json:"shard_index"`
		PeerID     string `json:"peer_id"`
		Address    string `json:"peer_address"`
	}
	var up struct {
		FileID     string       `json:"file_id"`
		Data       int          `json:"data_shards"`
		Parity     int          `json:"parity_shards"`
		ShardBytes int64        `json:"shard_size_bytes"`
		Shards     []assignment `json:"shards"`
	}
	code := e.http("POST", "/api/v1/files/upload", auth.Token, map[string]interface{}{
		"file_size_bytes": int64(1) << 30, "file_name_encrypted": []byte("encrypted-name"),
		"owner_peer_id": "12D3KooWTestPeer08",
	}, &up)
	if code != 201 || up.Data != 4 || up.Parity != 2 || len(up.Shards) != 6 {
		t.Fatalf("upload: %d %+v", code, up)
	}
	asnCount := map[string]int{}
	for _, a := range up.Shards {
		if a.PeerID == peerIDs["12D3KooWTestPeer08"] {
			t.Fatal("owner peer was chosen to hold a shard")
		}
		if !strings.HasPrefix(a.Address, "/ip4/") {
			t.Fatalf("peer address is not a multiaddr: %q", a.Address)
		}
		var asn string
		e.db.QueryRow(`SELECT asn FROM peers WHERE peer_id = $1`, a.PeerID).Scan(&asn)
		asnCount[asn]++
	}
	for asn, n := range asnCount {
		if n > 2 {
			t.Fatalf("%s holds %d shards, cap is 2", asn, n)
		}
	}

	// Shard bytes, so audits have something real to prove.
	shardData := map[string][]byte{}
	for i, a := range up.Shards {
		b := make([]byte, 4096)
		rand.Read(b)
		shardData[a.ShardID] = b
		sum := sha256.Sum256(b)
		var conf struct {
			Status string `json:"file_status"`
		}
		if code := e.http("POST", "/api/v1/shards/"+a.ShardID+"/confirm", "", map[string]string{
			"peer_id": a.PeerID, "shard_hash": hex.EncodeToString(sum[:])}, &conf); code != 200 {
			t.Fatalf("confirm shard %d: %d", i, code)
		}
		want := "uploading"
		if i == len(up.Shards)-1 {
			want = "active"
		}
		if conf.Status != want {
			t.Fatalf("after %d confirms file is %s, want %s", i+1, conf.Status, want)
		}
	}
	if code := e.http("POST", "/api/v1/shards/"+up.Shards[0].ShardID+"/confirm", "", map[string]string{
		"peer_id": up.Shards[0].PeerID, "shard_hash": strings.Repeat("a", 64)}, nil); code != 409 {
		t.Fatalf("double confirm allowed: %d", code)
	}
	var files struct {
		Files []struct {
			FileID string `json:"file_id"`
		} `json:"files"`
	}
	if code := e.http("GET", "/api/v1/files", auth.Token, nil, &files); code != 200 || len(files.Files) != 1 {
		t.Fatalf("list files: %d %+v", code, files)
	}

	t.Run("audits", func(t *testing.T) {
		target := up.Shards[0]
		data := shardData[target.ShardID]
		var chs []*auditpb.Challenge
		for i := 0; i < 3; i++ {
			nonce := make([]byte, 32)
			rand.Read(nonce)
			proof := sha256.Sum256(append(append([]byte{}, nonce...), data...))
			chs = append(chs, &auditpb.Challenge{Challenge: nonce, ExpectedProof: proof[:]})
		}
		if _, err := audits.RegisterChallenges(ctx, &auditpb.RegisterChallengesRequest{ShardId: target.ShardID, Challenges: chs}); err != nil {
			t.Fatal(err)
		}
		answer := func(nonce []byte, correct bool) []byte {
			if !correct {
				return make([]byte, 32)
			}
			p := sha256.Sum256(append(append([]byte{}, nonce...), data...))
			return p[:]
		}

		ch, err := audits.ChallengeAudit(ctx, &auditpb.ChallengeRequest{ShardId: target.ShardID})
		if err != nil || ch.PeerId != target.PeerID {
			t.Fatalf("challenge: %v %+v", err, ch)
		}
		again, _ := audits.ChallengeAudit(ctx, &auditpb.ChallengeRequest{ShardId: target.ShardID})
		if again.AuditId != ch.AuditId {
			t.Fatal("a second challenge opened while one is pending")
		}
		pend, err := audits.GetPendingAudits(ctx, &auditpb.PendingAuditsRequest{PeerId: target.PeerID})
		if err != nil || len(pend.Audits) != 1 || !bytes.Equal(pend.Audits[0].Challenge, ch.Challenge) {
			t.Fatalf("pending audits: %v %+v", err, pend)
		}
		res, err := audits.VerifyResponse(ctx, &auditpb.VerifyRequest{AuditId: ch.AuditId, Proof: answer(ch.Challenge, true)})
		if err != nil || !res.Passed {
			t.Fatalf("correct proof rejected: %v %+v", err, res)
		}
		if _, err := audits.VerifyResponse(ctx, &auditpb.VerifyRequest{AuditId: ch.AuditId, Proof: answer(ch.Challenge, true)}); status.Code(err) != codes.FailedPrecondition {
			t.Fatalf("audit answered twice: %v", err)
		}

		ch2, err := audits.ChallengeAudit(ctx, &auditpb.ChallengeRequest{ShardId: target.ShardID})
		if err != nil {
			t.Fatal(err)
		}
		res, err = audits.VerifyResponse(ctx, &auditpb.VerifyRequest{AuditId: ch2.AuditId, Proof: answer(ch2.Challenge, false)})
		if err != nil || res.Passed || res.Status != "failed" {
			t.Fatalf("wrong proof passed: %v %+v", err, res)
		}
		var health string
		e.db.QueryRow(`SELECT health_status FROM shards WHERE shard_id = $1`, target.ShardID).Scan(&health)
		if health != "degraded" {
			t.Fatalf("shard health after failed audit = %s, want degraded", health)
		}
		hist, err := audits.GetAuditHistory(ctx, &auditpb.AuditHistoryRequest{ShardId: target.ShardID})
		if err != nil || hist.Total != 2 || hist.Audits[0].ChallengedAt == 0 {
			t.Fatalf("history: %v %+v", err, hist)
		}
		// Pass again so the shard is good for the repair test.
		ch3, _ := audits.ChallengeAudit(ctx, &auditpb.ChallengeRequest{ShardId: target.ShardID})
		audits.VerifyResponse(ctx, &auditpb.VerifyRequest{AuditId: ch3.AuditId, Proof: answer(ch3.Challenge, true)})
		if _, err := audits.ChallengeAudit(ctx, &auditpb.ChallengeRequest{ShardId: target.ShardID}); status.Code(err) != codes.FailedPrecondition {
			t.Fatalf("challenge with no unused challenges: %v", err)
		}
	})

	t.Run("repair rebuilds one lost shard on one new peer", func(t *testing.T) {
		lost := up.Shards[1]
		e.db.Exec(`UPDATE peers SET is_online = false, last_seen_at = now() - INTERVAL '8 days' WHERE peer_id = $1`, lost.PeerID)
		e.start("repair-worker", "POLL_INTERVAL_SECONDS=1", "INACTIVE_PEER_HOURS=168", "WORKER_ID=e2e")

		var newShard, newPeer string
		e.waitFor("rebuild assignment", 30*time.Second, func() bool {
			return e.db.QueryRow(`SELECT shard_id, peer_id FROM shards
				WHERE file_id = $1 AND shard_index = $2 AND health_status = 'pending'`,
				up.FileID, lost.ShardIndex).Scan(&newShard, &newPeer) == nil
		})
		if newPeer == lost.PeerID {
			t.Fatal("rebuilt shard placed on the lost peer")
		}
		if n := e.count(`SELECT count(*) FROM shards WHERE file_id = $1 AND health_status = 'pending'`, up.FileID); n != 1 {
			t.Fatalf("%d pending shards, want exactly 1 (one per lost index, not 14 copies)", n)
		}
		e.waitFor("job awaiting data", 10*time.Second, func() bool {
			return e.count(`SELECT count(*) FROM repair_queue WHERE file_id = $1 AND status = 'awaiting_data'`, up.FileID) == 1
		})

		var work struct {
			Store []struct {
				ShardID string `json:"shard_id"`
				Data    int    `json:"data_shards"`
				Sources []struct {
					ShardIndex int `json:"shard_index"`
				} `json:"sources"`
			} `json:"store"`
		}
		e.http("GET", "/api/v1/peers/"+newPeer+"/pending-shards", "", nil, &work)
		if len(work.Store) != 1 || work.Store[0].ShardID != newShard || len(work.Store[0].Sources) < work.Store[0].Data {
			t.Fatalf("pending-shards for rebuild peer: %+v", work)
		}
		var conf struct {
			Status string `json:"file_status"`
		}
		e.http("POST", "/api/v1/shards/"+newShard+"/confirm", "", map[string]string{
			"peer_id": newPeer, "shard_hash": strings.Repeat("b", 64)}, &conf)
		if conf.Status != "active" {
			t.Fatalf("file after rebuild = %s, want active", conf.Status)
		}
		e.waitFor("repair job completed", 10*time.Second, func() bool {
			return e.count(`SELECT count(*) FROM repair_queue WHERE file_id = $1 AND status = 'completed'`, up.FileID) == 1
		})
	})

	t.Run("replication adds copies for a warm file", func(t *testing.T) {
		for i := 0; i < 12; i++ {
			e.http("GET", "/api/v1/files/"+up.FileID, auth.Token, nil, nil)
		}
		e.start("replication-worker", "POLL_INTERVAL_SECONDS=1", "WARM_TIER_THRESHOLD=10", "HOT_TIER_THRESHOLD=1000")
		e.waitFor("two queued copies", 30*time.Second, func() bool {
			return e.count(`SELECT count(*) FROM replication_jobs WHERE file_id = $1 AND status = 'queued'`, up.FileID) == 2
		})
		var tier string
		e.db.QueryRow(`SELECT storage_tier FROM files WHERE file_id = $1`, up.FileID).Scan(&tier)
		if tier != "warm" {
			t.Fatalf("tier = %s, want warm", tier)
		}
		var copyShard, copyPeer string
		e.db.QueryRow(`SELECT target_shard_id, target_peer_id FROM replication_jobs WHERE file_id = $1 LIMIT 1`, up.FileID).Scan(&copyShard, &copyPeer)
		e.http("POST", "/api/v1/shards/"+copyShard+"/confirm", "", map[string]string{
			"peer_id": copyPeer, "shard_hash": strings.Repeat("c", 64)}, nil)
		e.waitFor("copy job completed", 10*time.Second, func() bool {
			return e.count(`SELECT count(*) FROM replication_jobs WHERE file_id = $1 AND status = 'completed'`, up.FileID) == 1
		})
	})

	t.Run("billing", func(t *testing.T) {
		holder := up.Shards[2].PeerID
		// Pretend the shard has been stored for 10 days and the peer served 2 GB.
		e.db.Exec(`UPDATE shards SET stored_at = now() - INTERVAL '10 days' WHERE peer_id = $1`, holder)
		e.db.Exec(`UPDATE peers SET user_id = $1, bytes_served = $2 WHERE peer_id = $3`, auth.UserID, int64(2)<<30, holder)
		now := time.Now().UTC()
		start := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC)
		rec, err := billing.CalculateEarnings(ctx, &billingpb.CalculateEarningsRequest{
			PeerId: holder, Year: int32(now.Year()), Month: int32(now.Month())})
		if err != nil {
			t.Fatal(err)
		}
		days := now.Sub(start).Hours() / 24
		if days > 10 {
			days = 10
		}
		wantGBDays := float64(up.ShardBytes) / (1 << 30) * days
		if d := rec.StorageGbDays - wantGBDays; d > 0.01 || d < -0.01 {
			t.Fatalf("storage_gb_days = %.3f, want %.3f", rec.StorageGbDays, wantGBDays)
		}
		if rec.BandwidthGb < 1.99 || rec.BandwidthGb > 2.01 {
			t.Fatalf("bandwidth_gb = %.3f, want 2", rec.BandwidthGb)
		}
		again, _ := billing.CalculateEarnings(ctx, &billingpb.CalculateEarningsRequest{
			PeerId: holder, Year: int32(now.Year()), Month: int32(now.Month())})
		if again.EarningId != rec.EarningId {
			t.Fatal("recalculation created a second row")
		}

		// Add earnings directly so a payout is possible, then check balance arithmetic.
		e.db.Exec(`INSERT INTO earnings (peer_id, user_id, period_start, period_end, storage_gb_days, base_rate_usd,
			gross_earnings_usd, net_earnings_usd) VALUES ($1, $2, '2025-01-01', '2025-02-01', 1, 0.0063, 20, 20)`,
			holder, auth.UserID)
		if _, err := billing.ProcessPayout(ctx, &billingpb.PayoutRequest{UserId: auth.UserID, AmountUsd: 10}); status.Code(err) != codes.FailedPrecondition {
			t.Fatalf("payout without payment method: %v", err)
		}
		if _, err := billing.UpdatePaymentMethod(ctx, &billingpb.UpdatePaymentRequest{UserId: auth.UserID,
			PaymentMethod: "paypal", PaymentEmail: "tester@example.com"}); err != nil {
			t.Fatal(err)
		}
		if _, err := billing.ProcessPayout(ctx, &billingpb.PayoutRequest{UserId: auth.UserID, AmountUsd: 15}); err != nil {
			t.Fatal(err)
		}
		// The pending $15 counts against the balance, so a second $15 must fail.
		if _, err := billing.ProcessPayout(ctx, &billingpb.PayoutRequest{UserId: auth.UserID, AmountUsd: 15}); status.Code(err) != codes.FailedPrecondition {
			t.Fatalf("second payout overspent the balance: %v", err)
		}
		bal, err := billing.GetBalance(ctx, &billingpb.BalanceRequest{UserId: auth.UserID})
		if err != nil || bal.PaidOutUsd != 15 || bal.AvailableUsd > bal.EarnedUsd-15+1e-6 {
			t.Fatalf("balance: %v %+v", err, bal)
		}
		e.waitFor("simulated payment", 15*time.Second, func() bool {
			return e.count(`SELECT count(*) FROM payouts WHERE user_id = $1 AND status = 'completed'`, auth.UserID) == 1
		})
		var earnings map[string]interface{}
		if code := e.http("GET", "/api/v1/users/me/earnings", auth.Token, nil, &earnings); code != 200 {
			t.Fatalf("earnings endpoint: %d %v", code, earnings)
		}
	})

	t.Run("keys", func(t *testing.T) {
		if _, err := keys.StoreMasterKey(ctx, &keypb.StoreMasterKeyRequest{UserId: auth.UserID, WrappedMasterKey: []byte("wrapped")}); err != nil {
			t.Fatal(err)
		}
		k1, err := keys.StoreFileKey(ctx, &keypb.StoreFileKeyRequest{FileId: up.FileID, UserId: auth.UserID, WrappedKey: []byte("k1")})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := keys.StoreFileKey(ctx, &keypb.StoreFileKeyRequest{FileId: up.FileID, UserId: auth.UserID, WrappedKey: []byte("dup")}); status.Code(err) != codes.AlreadyExists {
			t.Fatalf("second active key allowed: %v", err)
		}
		k2, err := keys.RotateFileKey(ctx, &keypb.RotateFileKeyRequest{FileId: up.FileID, UserId: auth.UserID, NewWrappedKey: []byte("k2")})
		if err != nil || k2.KeyVersion != 2 || k2.KeyId == k1.KeyId {
			t.Fatalf("rotate: %v %+v", err, k2)
		}
		got, err := keys.GetFileKey(ctx, &keypb.GetFileKeyRequest{FileId: up.FileID, UserId: auth.UserID})
		if err != nil || string(got.WrappedKey) != "k2" || got.RotationDue {
			t.Fatalf("get: %v %+v", err, got)
		}
		// A key past its rotation date still works.
		e.db.Exec(`UPDATE file_keys SET rotate_after = now() - INTERVAL '1 day' WHERE key_id = $1`, k2.KeyId)
		got, err = keys.GetFileKey(ctx, &keypb.GetFileKeyRequest{FileId: up.FileID, UserId: auth.UserID})
		if err != nil || !got.RotationDue {
			t.Fatalf("overdue key: %v %+v", err, got)
		}
		info, err := keys.RegisterRecoveryShares(ctx, &keypb.RegisterRecoveryRequest{UserId: auth.UserID, Threshold: 2,
			Holders: []*keypb.ShareHolder{{HolderId: "friend-a", ShareIndex: 1}, {HolderId: "friend-b", ShareIndex: 2}, {HolderId: "device-2", ShareIndex: 3}}})
		if err != nil || info.TotalShares != 3 {
			t.Fatalf("recovery: %v %+v", err, info)
		}
		back, err := keys.GetRecoveryInfo(ctx, &keypb.GetRecoveryInfoRequest{UserId: auth.UserID})
		if err != nil || len(back.Holders) != 3 || back.Threshold != 2 {
			t.Fatalf("recovery info: %v %+v", err, back)
		}
	})

	t.Run("analytics", func(t *testing.T) {
		e.start("analytics-worker", "POLL_INTERVAL_SECONDS=3600")
		e.waitFor("system stats event", 20*time.Second, func() bool {
			return e.count(`SELECT count(*) FROM analytics_events WHERE event_type = 'system_stats'`) >= 1
		})
		e.waitFor("churn scores", 10*time.Second, func() bool {
			return e.count(`SELECT count(*) FROM peers WHERE churn_prediction IS NOT NULL`) > 0
		})
	})

	t.Run("repeated audit failures mark the shard missing and queue a repair", func(t *testing.T) {
		var up2 struct {
			FileID string       `json:"file_id"`
			Shards []assignment `json:"shards"`
		}
		if code := e.http("POST", "/api/v1/files/upload", auth.Token, map[string]interface{}{
			"file_size_bytes": int64(64) << 20, "file_name_encrypted": []byte("second")}, &up2); code != 201 {
			t.Fatalf("upload: %d", code)
		}
		for _, a := range up2.Shards {
			e.http("POST", "/api/v1/shards/"+a.ShardID+"/confirm", "", map[string]string{
				"peer_id": a.PeerID, "shard_hash": strings.Repeat("d", 64)}, nil)
		}
		bad := up2.Shards[0]
		var chs []*auditpb.Challenge
		for i := 0; i < 3; i++ {
			nonce := make([]byte, 16)
			rand.Read(nonce)
			proof := sha256.Sum256(nonce)
			chs = append(chs, &auditpb.Challenge{Challenge: nonce, ExpectedProof: proof[:]})
		}
		audits.RegisterChallenges(ctx, &auditpb.RegisterChallengesRequest{ShardId: bad.ShardID, Challenges: chs})
		for i := 0; i < 3; i++ {
			ch, err := audits.ChallengeAudit(ctx, &auditpb.ChallengeRequest{ShardId: bad.ShardID})
			if err != nil {
				t.Fatal(err)
			}
			audits.VerifyResponse(ctx, &auditpb.VerifyRequest{AuditId: ch.AuditId, Proof: make([]byte, 32)})
		}
		var health string
		e.db.QueryRow(`SELECT health_status FROM shards WHERE shard_id = $1`, bad.ShardID).Scan(&health)
		if health != "missing" {
			t.Fatalf("shard after 3 failures = %s, want missing", health)
		}
		if n := e.count(`SELECT count(*) FROM repair_queue WHERE file_id = $1 AND reason = 'failed_audit'`, up2.FileID); n != 1 {
			t.Fatalf("%d repair jobs for the file, want 1", n)
		}
		// The running repair worker places exactly one rebuild for the lost index.
		e.waitFor("rebuild placed", 15*time.Second, func() bool {
			return e.count(`SELECT count(*) FROM shards WHERE file_id = $1 AND shard_index = $2 AND health_status = 'pending'`,
				up2.FileID, bad.ShardIndex) == 1
		})
	})

	t.Run("delete", func(t *testing.T) {
		if code := e.http("DELETE", "/api/v1/files/"+up.FileID, auth.Token, nil, nil); code != 202 {
			t.Fatalf("delete: %d", code)
		}
		rows, _ := e.db.Query(`SELECT shard_id, peer_id FROM shards WHERE file_id = $1 AND health_status = 'retiring'`, up.FileID)
		type sp struct{ shard, peer string }
		var list []sp
		for rows.Next() {
			var x sp
			rows.Scan(&x.shard, &x.peer)
			list = append(list, x)
		}
		rows.Close()
		if len(list) == 0 {
			t.Fatal("no shards marked retiring")
		}
		var work struct {
			Delete []string `json:"delete"`
		}
		e.http("GET", "/api/v1/peers/"+list[0].peer+"/pending-shards", "", nil, &work)
		if len(work.Delete) == 0 {
			t.Fatal("peer was not told to delete its shard")
		}
		for _, x := range list {
			e.http("POST", "/api/v1/shards/"+x.shard+"/deleted", "", map[string]string{"peer_id": x.peer}, nil)
		}
		var st string
		e.db.QueryRow(`SELECT status FROM files WHERE file_id = $1`, up.FileID).Scan(&st)
		if st != "deleted" {
			t.Fatalf("file status after all deletions = %s", st)
		}
	})
}
