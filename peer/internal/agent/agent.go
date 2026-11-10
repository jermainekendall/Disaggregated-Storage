// Package agent is the storage daemon (`orchestrator run`): it offers disk
// space to the network, stores shards pushed to it, answers audits, rebuilds
// or copies shards when the server asks, and deletes retired ones.
//
// Everything the agent does is driven by the server:
//   - registration at startup and heartbeats every heartbeat_interval
//   - GET /peers/{id}/pending-shards every poll_interval: shards to accept
//     from uploaders, to rebuild from other peers, to copy, or to delete
//   - GET /peers/{id}/audits: proof-of-storage challenges to answer
//
// It only accepts a pushed shard the server assigned to it, with the
// assigned size (and hash, when known).
package agent

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/jermainekendall/disaggregated-storage/peer/internal/api"
	"github.com/jermainekendall/disaggregated-storage/peer/internal/config"
	"github.com/jermainekendall/disaggregated-storage/peer/internal/crypt"
	"github.com/jermainekendall/disaggregated-storage/peer/internal/erasure"
	"github.com/jermainekendall/disaggregated-storage/peer/internal/identity"
	"github.com/jermainekendall/disaggregated-storage/peer/internal/p2p"
	"github.com/jermainekendall/disaggregated-storage/peer/internal/shardstore"
	"github.com/jermainekendall/disaggregated-storage/peer/internal/sysinfo"
)

// State is persisted in the config directory.
type State struct {
	PeerID string `json:"peer_id"` // server-assigned UUID
}

// StatePath is where the agent keeps its server peer ID.
func StatePath(cfg *config.Config) string { return filepath.Join(cfg.Dir(), "state.json") }

type Agent struct {
	cfg     *config.Config
	version string
	api     *api.Client
	host    *p2p.Host
	store   *shardstore.Store
	workDir string

	mu        sync.Mutex
	refreshMu sync.Mutex // serialises on-demand assignment refreshes
	peerID    string
	assigned  map[string]api.Work // shards we may accept, by shard ID
	lastPoll  time.Time
	busy      map[string]bool // shards being rebuilt or copied
}

// Run starts the agent and blocks until ctx is cancelled.
func Run(ctx context.Context, cfg *config.Config, version string) error {
	store, err := shardstore.Open(cfg.StoragePath(), cfg.CapacityBytes())
	if err != nil {
		return fmt.Errorf("storage: %w", err)
	}
	key, err := identity.LoadOrCreateKey(cfg.Resolve(cfg.KeyPath))
	if err != nil {
		return fmt.Errorf("identity: %w", err)
	}
	client, err := api.New(cfg.GatewayURL, cfg.RegistryAddr)
	if err != nil {
		return err
	}
	defer client.Close()

	h, err := p2p.NewHost(p2p.HostConfig{
		Key: key, ListenPort: cfg.ListenPort, RelayAddr: cfg.RelayAddr,
		Announce: cfg.AnnounceAddrs, AllowLoopback: cfg.AnnounceLoopback,
	})
	if err != nil {
		return fmt.Errorf("libp2p: %w", err)
	}
	defer h.Close()

	a := &Agent{
		cfg: cfg, version: version, api: client, host: h, store: store,
		workDir:  filepath.Join(cfg.Dir(), "work"),
		assigned: map[string]api.Work{}, busy: map[string]bool{},
	}
	os.RemoveAll(a.workDir)
	if err := os.MkdirAll(a.workDir, 0o700); err != nil {
		return err
	}
	var st State
	identity.ReadJSON(StatePath(cfg), &st)
	a.peerID = st.PeerID

	handlers := &p2p.Handlers{Store: store, Authorize: a.authorize, Stored: a.confirm, Version: version}
	handlers.Register(h)

	n, used, capacity := store.Stats()
	log.Printf("orchestrator agent %s: libp2p %s, %d shards stored (%.2f of %.2f GB)",
		version, h.ID(), n, gb(used), gb(capacity))
	for _, addr := range h.P2PAddrs() {
		log.Printf("  listening on %s", addr)
	}

	go h.KeepRelayReservation(ctx)
	if cfg.RelayAddr != "" {
		// Give the relay a moment so the first registration carries the
		// relay address; later heartbeats update it anyway.
		select {
		case <-time.After(3 * time.Second):
		case <-ctx.Done():
		}
	}

	if err := a.register(ctx); err != nil {
		return err
	}
	handlers.PeerID = a.id()

	var wg sync.WaitGroup
	run := func(name string, every time.Duration, fn func(context.Context)) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			t := time.NewTicker(every)
			defer t.Stop()
			for {
				func() {
					defer func() {
						if r := recover(); r != nil {
							log.Printf("%s: panic: %v", name, r)
						}
					}()
					fn(ctx)
				}()
				select {
				case <-ctx.Done():
					return
				case <-t.C:
				}
			}
		}()
	}
	run("heartbeat", cfg.HeartbeatInterval, a.heartbeat)
	run("work", cfg.PollInterval, a.pollWork)
	run("audits", cfg.PollInterval, a.answerAudits)
	run("scrub", 24*time.Hour, a.scrub)
	<-ctx.Done()
	log.Println("shutting down")
	wg.Wait()
	return nil
}

func (a *Agent) id() string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.peerID
}

func gb(b int64) float64 { return float64(b) / (1 << 30) }

// register (re-)registers with the server, retrying until it succeeds or ctx
// ends, and saves the server-assigned peer ID.
func (a *Agent) register(ctx context.Context) error {
	host, _ := os.Hostname()
	for attempt := 1; ; attempt++ {
		addrs := a.host.P2PAddrs()
		reg := api.Registration{
			PeerID:                 a.id(),
			Libp2pPeerID:           a.host.ID().String(),
			Multiaddrs:             addrs,
			TotalCapacityBytes:     a.cfg.CapacityBytes(),
			AvailableCapacityBytes: a.store.Available(),
			Region:                 a.cfg.Region,
			ASN:                    a.cfg.ASN,
			Timezone:               sysinfo.Timezone(),
			Version:                a.version,
			DeviceModel:            host,
			OSType:                 "desktop",
		}
		if len(addrs) > 0 {
			reg.Multiaddr = addrs[0]
		}
		id, err := a.api.Register(ctx, reg)
		if err == nil {
			a.mu.Lock()
			a.peerID = id
			a.mu.Unlock()
			if err := identity.WriteJSON(StatePath(a.cfg), State{PeerID: id}); err != nil {
				log.Printf("could not save state: %v", err)
			}
			if f := a.cfg.PeerIDFile; f != "" {
				// Public on purpose: the ID is not a secret, and local users'
				// uploads read it to avoid storing their own shards here.
				if err := writePublic(f, id+"\n"); err != nil {
					log.Printf("could not write %s: %v", f, err)
				}
			}
			log.Printf("registered with server as peer %s", id)
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		wait := time.Duration(min(attempt, 6)) * 10 * time.Second
		log.Printf("registration failed: %v (retrying in %s)", err, wait)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(wait):
		}
	}
}

func (a *Agent) heartbeat(ctx context.Context) {
	power := sysinfo.ReadPower()
	err := a.api.SendHeartbeat(ctx, api.Heartbeat{
		PeerID:         a.id(),
		AvailableBytes: a.store.Available(),
		Multiaddrs:     a.host.P2PAddrs(),
		Charging:       power.OnAC,
		BatteryLevel:   power.BatteryLevel,
		NetworkType:    sysinfo.NetworkType(),
	})
	if api.IsStatus(err, 404) {
		log.Println("server no longer knows this peer; registering again")
		a.register(ctx)
		return
	}
	if err != nil {
		log.Printf("heartbeat failed: %v", err)
	}
}

// authorize lets a pushed shard in only if the server assigned it to us.
// The assignment list is refreshed if the shard is not in it yet (an upload
// can start right after placement, before our next poll).
func (a *Agent) authorize(ctx context.Context, shardID string) (shardstore.Meta, error) {
	a.mu.Lock()
	w, ok := a.assigned[shardID]
	a.mu.Unlock()
	if !ok {
		// Not in our last list: the upload may have been placed after it.
		// Refreshes are serialised and at most every 200 ms, so a burst of
		// pushes (or junk IDs) costs at most a few server calls.
		a.refreshMu.Lock()
		a.mu.Lock()
		w, ok = a.assigned[shardID]
		wait := 200*time.Millisecond - time.Since(a.lastPoll)
		a.mu.Unlock()
		if !ok {
			if wait > 0 {
				time.Sleep(wait)
			}
			a.refreshAssignments(ctx)
			a.mu.Lock()
			w, ok = a.assigned[shardID]
			a.mu.Unlock()
		}
		a.refreshMu.Unlock()
	}
	if !ok {
		return shardstore.Meta{}, errors.New("this peer was not assigned that shard")
	}
	return shardstore.Meta{ShardID: w.ShardID, FileID: w.FileID, Index: w.ShardIndex,
		Size: w.SizeBytes, SHA256: w.ExpectedHash}, nil
}

func (a *Agent) refreshAssignments(ctx context.Context) (api.PendingWork, error) {
	work, err := a.api.PendingShards(ctx, a.id())
	if err != nil {
		return work, err
	}
	m := make(map[string]api.Work, len(work.Store))
	for _, w := range work.Store {
		m[w.ShardID] = w
	}
	a.mu.Lock()
	a.assigned = m
	a.lastPoll = time.Now()
	a.mu.Unlock()
	return work, nil
}

// confirm tells the server a shard is stored. A failure is retried by the
// next poll, which sees the shard still pending but already on disk.
func (a *Agent) confirm(ctx context.Context, m shardstore.Meta) {
	if err := a.api.ConfirmShard(ctx, m.ShardID, a.id(), m.SHA256); err != nil {
		log.Printf("confirm %s: %v", m.ShardID, err)
		return
	}
	log.Printf("stored shard %s (file %s, index %d, %d bytes)", m.ShardID, m.FileID, m.Index, m.Size)
}

func (a *Agent) pollWork(ctx context.Context) {
	work, err := a.refreshAssignments(ctx)
	if err != nil {
		log.Printf("poll: %v", err)
		return
	}
	for _, w := range work.Store {
		if m, ok := a.store.Get(w.ShardID); ok {
			a.confirm(ctx, m) // stored earlier but the confirmation was lost
			continue
		}
		if w.FileStatus == "uploading" {
			continue // the uploader pushes it to us
		}
		a.mu.Lock()
		if a.busy[w.ShardID] {
			a.mu.Unlock()
			continue
		}
		a.busy[w.ShardID] = true
		a.mu.Unlock()
		go func(w api.Work) {
			defer func() { a.mu.Lock(); delete(a.busy, w.ShardID); a.mu.Unlock() }()
			var err error
			if w.IsReplica {
				err = a.copyShard(ctx, w)
			} else {
				err = a.rebuildShard(ctx, w)
			}
			if err != nil {
				log.Printf("shard %s (file %s, index %d): %v; will retry", w.ShardID, w.FileID, w.ShardIndex, err)
			}
		}(w)
	}
	for _, id := range work.Delete {
		if err := a.store.Delete(id); err != nil {
			log.Printf("delete %s: %v", id, err)
			continue
		}
		if err := a.api.ShardDeleted(ctx, id, a.id()); err != nil {
			log.Printf("report deletion of %s: %v", id, err)
			continue
		}
		log.Printf("deleted retired shard %s", id)
	}
}

// fetch downloads one shard from a source into dir and verifies its hash
// when expected is set.
func (a *Agent) fetch(ctx context.Context, src api.Source, shardID, dir, expected string) (string, error) {
	id, err := a.host.Dial(ctx, src.Libp2pID, src.Multiaddrs)
	if err != nil {
		return "", err
	}
	p := filepath.Join(dir, fmt.Sprintf("src-%d", src.ShardIndex))
	f, err := os.Create(p)
	if err != nil {
		return "", err
	}
	_, reported, err := a.host.FetchShard(ctx, id, shardID, f)
	f.Close()
	if err != nil {
		return "", err
	}
	got, err := hashFile(p)
	if err != nil {
		return "", err
	}
	if got != reported || (expected != "" && got != expected) {
		return "", fmt.Errorf("shard from %s failed its hash check", src.PeerID)
	}
	return p, nil
}

// copyShard stores an extra copy of a shard index fetched from a peer that
// holds it (hot/warm tier replication).
func (a *Agent) copyShard(ctx context.Context, w api.Work) error {
	dir, err := os.MkdirTemp(a.workDir, "copy-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	for _, src := range w.Sources {
		if src.ShardIndex != w.ShardIndex {
			continue
		}
		p, err := a.fetch(ctx, src, src.ShardID, dir, w.ExpectedHash)
		if err != nil {
			log.Printf("copy from %s: %v", src.PeerID, err)
			continue
		}
		return a.storeFile(ctx, w, p)
	}
	return errors.New("no source could provide the shard")
}

// rebuildShard recreates a lost shard index from `data` other shards of the file.
func (a *Agent) rebuildShard(ctx context.Context, w api.Work) error {
	dir, err := os.MkdirTemp(a.workDir, "rebuild-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	have := map[int]string{}
	for _, src := range w.Sources {
		if len(have) == w.DataShards {
			break
		}
		if src.ShardIndex == w.ShardIndex || have[src.ShardIndex] != "" {
			continue
		}
		p, err := a.fetch(ctx, src, src.ShardID, dir, "")
		if err != nil {
			log.Printf("rebuild: source %s index %d: %v", src.PeerID, src.ShardIndex, err)
			continue
		}
		have[src.ShardIndex] = p
	}
	if len(have) < w.DataShards {
		return fmt.Errorf("only %d of %d source shards reachable", len(have), w.DataShards)
	}
	out, err := erasure.Reconstruct(have, []int{w.ShardIndex}, w.DataShards, w.ParityShards, dir)
	if err != nil {
		return err
	}
	return a.storeFile(ctx, w, out[w.ShardIndex])
}

func (a *Agent) storeFile(ctx context.Context, w api.Work, path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	m, err := a.store.Put(shardstore.Meta{ShardID: w.ShardID, FileID: w.FileID, Index: w.ShardIndex,
		Size: w.SizeBytes, SHA256: w.ExpectedHash}, f)
	if err != nil {
		return err
	}
	a.confirm(ctx, m)
	return nil
}

// answerAudits proves possession of the shards the server challenges.
func (a *Agent) answerAudits(ctx context.Context) {
	audits, err := a.api.PendingAudits(ctx, a.id())
	if err != nil {
		log.Printf("audits: %v", err)
		return
	}
	for _, au := range audits {
		path, err := a.store.Path(au.ShardID)
		if err != nil {
			log.Printf("audit %s: shard %s is not stored here", au.AuditID, au.ShardID)
			continue // the audit will time out and count as a failure
		}
		f, err := os.Open(path)
		if err != nil {
			continue
		}
		proof, err := crypt.AuditProof(au.Challenge, f)
		f.Close()
		if err != nil {
			continue
		}
		passed, err := a.api.AnswerAudit(ctx, au.AuditID, proof)
		if err != nil {
			log.Printf("audit %s: %v", au.AuditID, err)
			continue
		}
		log.Printf("audit of shard %s: passed=%t", au.ShardID, passed)
	}
}

// scrub re-reads every stored shard and drops corrupt ones, so audits and
// repair replace them instead of the agent serving bad data.
func (a *Agent) scrub(ctx context.Context) {
	bad := 0
	for _, m := range a.store.List() {
		if ctx.Err() != nil {
			return
		}
		if err := a.store.Verify(m.ShardID); err != nil {
			log.Printf("scrub: %v; deleting it", err)
			a.store.Delete(m.ShardID)
			bad++
		}
	}
	if bad > 0 {
		log.Printf("scrub: removed %d corrupt shards", bad)
	}
}

func writePublic(path, content string) error {
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, []byte(content), 0o600); err != nil {
		return err
	}
	if err := os.Chmod(tmp, 0o644); err != nil { // the service's umask is 077
		return err
	}
	return os.Rename(tmp, path)
}

func hashFile(p string) (string, error) {
	f, err := os.Open(p)
	if err != nil {
		return "", err
	}
	defer f.Close()
	sum, _, err := crypt.HashReader(f)
	return sum, err
}
