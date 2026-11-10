// Package client implements the user commands: login, upload, download,
// list and delete.
//
// Upload: compress + encrypt the file with a fresh file key (to a temp
// file), ask the server for a placement, erasure code the encrypted file
// into data + parity shards, push each shard to its peer over libp2p, store
// the wrapped file key, and register audit challenges for every shard.
//
// Download: fetch any `data` shards (verifying each against its recorded
// SHA-256), rebuild missing data shards from parity if needed, join,
// decrypt and decompress.
//
// Temporary space: about 2x the compressed file size during an upload or
// download, under the config directory's work/ folder.
package client

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
	"hash"
	"io"
	"mime"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/crypto"

	"github.com/jermainekendall/disaggregated-storage/peer/internal/agent"
	"github.com/jermainekendall/disaggregated-storage/peer/internal/api"
	"github.com/jermainekendall/disaggregated-storage/peer/internal/config"
	"github.com/jermainekendall/disaggregated-storage/peer/internal/crypt"
	"github.com/jermainekendall/disaggregated-storage/peer/internal/erasure"
	"github.com/jermainekendall/disaggregated-storage/peer/internal/identity"
	"github.com/jermainekendall/disaggregated-storage/peer/internal/p2p"
)

const challengesPerShard = 8

// Client is a logged-in user session.
type Client struct {
	cfg    *config.Config
	api    *api.Client
	master []byte
	host   *p2p.Host
	Log    func(format string, args ...interface{})
}

func sessionPath(cfg *config.Config) string { return filepath.Join(cfg.Dir(), "session.json") }

// Login authenticates (or registers) and saves the session token.
func Login(ctx context.Context, cfg *config.Config, email, password string, register bool) (api.Session, error) {
	c, err := api.New(cfg.GatewayURL, "")
	if err != nil {
		return api.Session{}, err
	}
	s, err := c.Login(ctx, email, password, register)
	if err != nil {
		return s, err
	}
	return s, identity.WriteJSON(sessionPath(cfg), s)
}

// Logout forgets the saved session.
func Logout(cfg *config.Config) error {
	err := os.Remove(sessionPath(cfg))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

// Open loads the saved session. passphrase is called (once) when the master
// key is needed; confirm is true when a new master key is being created.
func Open(ctx context.Context, cfg *config.Config, passphrase func(confirm bool) (string, error)) (*Client, error) {
	var s api.Session
	if err := identity.ReadJSON(sessionPath(cfg), &s); err != nil {
		return nil, err
	}
	if !s.Valid() {
		return nil, errors.New("not logged in or session expired; run `orchestrator login`")
	}
	base, err := api.New(cfg.GatewayURL, "")
	if err != nil {
		return nil, err
	}
	c := &Client{cfg: cfg, api: base.WithToken(s.Token), Log: func(string, ...interface{}) {}}

	wrapped, err := c.api.MasterKey(ctx)
	switch {
	case api.IsStatus(err, 404):
		// First use: create the master key.
		pp, err := passphrase(true)
		if err != nil {
			return nil, err
		}
		mk, err := crypt.NewKey()
		if err != nil {
			return nil, err
		}
		w, err := crypt.WrapMasterKey(mk, pp)
		if err != nil {
			return nil, err
		}
		if err := c.api.PutMasterKey(ctx, w); err != nil {
			return nil, fmt.Errorf("store master key: %w", err)
		}
		c.master = mk
	case err != nil:
		return nil, err
	default:
		pp, err := passphrase(false)
		if err != nil {
			return nil, err
		}
		if c.master, err = crypt.UnwrapMasterKey(wrapped, pp); err != nil {
			return nil, err
		}
	}
	return c, nil
}

// Close stops the libp2p host if one was started.
func (c *Client) Close() {
	if c.host != nil {
		c.host.Close()
	}
}

// p2pHost starts an outbound-only libp2p host with a throwaway identity
// (a running agent on this machine keeps its own).
func (c *Client) p2pHost() (*p2p.Host, error) {
	if c.host != nil {
		return c.host, nil
	}
	key, _, err := crypto.GenerateEd25519Key(nil)
	if err != nil {
		return nil, err
	}
	h, err := p2p.NewHost(p2p.HostConfig{Key: key, ListenPort: -1, RelayAddr: c.cfg.RelayAddr})
	if err != nil {
		return nil, err
	}
	c.host = h
	return h, nil
}

func (c *Client) workDir(prefix string) (string, error) {
	base := filepath.Join(c.cfg.Dir(), "work")
	if err := os.MkdirAll(base, 0o700); err != nil {
		return "", err
	}
	return os.MkdirTemp(base, prefix)
}

// UploadResult summarises a finished upload.
type UploadResult struct {
	FileID       string
	Name         string
	PlainBytes   int64
	StoredBytes  int64
	Data, Parity int
}

// Upload stores a local file on the network.
func (c *Client) Upload(ctx context.Context, path, tier string) (UploadResult, error) {
	res := UploadResult{Name: filepath.Base(path)}
	src, err := os.Open(path)
	if err != nil {
		return res, err
	}
	defer src.Close()
	if st, err := src.Stat(); err != nil || st.IsDir() {
		return res, fmt.Errorf("%s is not a regular file", path)
	}

	dir, err := c.workDir("upload-")
	if err != nil {
		return res, err
	}
	defer os.RemoveAll(dir)

	fileKey, err := crypt.NewKey()
	if err != nil {
		return res, err
	}
	encPath := filepath.Join(dir, "encrypted")
	enc, err := os.OpenFile(encPath, os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return res, err
	}
	c.Log("encrypting %s", path)
	res.PlainBytes, err = crypt.EncryptFile(fileKey, src, enc)
	if cerr := enc.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return res, err
	}
	st, _ := os.Stat(encPath)
	res.StoredBytes = st.Size()

	nameEnc, err := crypt.EncryptName(fileKey, res.Name)
	if err != nil {
		return res, err
	}
	owner := c.localAgentPeerID() // don't place shards on our own agent

	plan, err := c.api.StartUpload(ctx, api.UploadRequest{
		FileSizeBytes: res.StoredBytes, FileNameEncrypted: nameEnc,
		MimeType:    mime.TypeByExtension(filepath.Ext(path)),
		StorageTier: tier, OwnerPeerID: owner,
		IsCompressed: true, CompressionAlgorithm: "gzip", OriginalSizeBytes: res.PlainBytes,
	})
	if err != nil {
		return res, fmt.Errorf("placement: %w", err)
	}
	res.FileID, res.Data, res.Parity = plan.FileID, plan.Data, plan.Parity
	// From here on, an error leaves a half-uploaded file record: remove it.
	ok := false
	defer func() {
		if !ok {
			c.api.DeleteFile(context.Background(), plan.FileID)
		}
	}()

	wrapped, err := crypt.WrapFileKey(c.master, fileKey, plan.FileID)
	if err != nil {
		return res, err
	}
	if err := c.api.PutFileKey(ctx, plan.FileID, wrapped); err != nil {
		return res, fmt.Errorf("store file key: %w", err)
	}

	c.Log("erasure coding into %d data + %d parity shards of %d bytes", plan.Data, plan.Parity, plan.ShardBytes)
	shards, err := erasure.Encode(encPath, plan.Data, plan.Parity, dir)
	if err != nil {
		return res, err
	}
	os.Remove(encPath)
	if len(shards) != len(plan.Shards) {
		return res, fmt.Errorf("server planned %d shards, encoded %d", len(plan.Shards), len(shards))
	}

	h, err := c.p2pHost()
	if err != nil {
		return res, err
	}
	// Push shards (4 at a time), computing the audit challenges in the same
	// read as the hash.
	type pushed struct {
		a   api.Assignment
		chs []api.Challenge
		err error
	}
	results := make([]pushed, len(plan.Shards))
	sem := make(chan struct{}, 4)
	var wg sync.WaitGroup
	for i, a := range plan.Shards {
		wg.Add(1)
		go func(i int, a api.Assignment) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			p := shards[a.ShardIndex]
			sum, chs, err := hashWithChallenges(p, plan.ShardBytes)
			if err == nil {
				err = api.Retry(ctx, 3, func() error { return c.push(ctx, h, a, p, plan.ShardBytes, sum) })
			}
			if err == nil {
				c.Log("  shard %d -> peer %s", a.ShardIndex, short(a.PeerID))
			}
			results[i] = pushed{a, chs, err}
		}(i, a)
	}
	wg.Wait()
	for _, r := range results {
		if r.err != nil {
			return res, fmt.Errorf("shard %d to peer %s: %w", r.a.ShardIndex, r.a.PeerID, r.err)
		}
	}
	for _, r := range results {
		if err := c.api.RegisterChallenges(ctx, r.a.ShardID, r.chs); err != nil {
			return res, fmt.Errorf("register audit challenges: %w", err)
		}
	}
	ok = true
	return res, nil
}

// localAgentPeerID finds this machine's agent: in the same config directory
// (agent run as this user) or the file published by the system service.
func (c *Client) localAgentPeerID() string {
	var st agent.State
	identity.ReadJSON(agent.StatePath(c.cfg), &st)
	if st.PeerID != "" {
		return st.PeerID
	}
	if f := c.cfg.AgentPeerIDFile; f != "" {
		if b, err := os.ReadFile(f); err == nil {
			return strings.TrimSpace(string(b))
		}
	}
	return ""
}

func (c *Client) push(ctx context.Context, h *p2p.Host, a api.Assignment, path string, size int64, sum string) error {
	addrs := append([]string{a.PeerAddress}, a.Multiaddrs...)
	id, err := h.Dial(ctx, a.Libp2pID, addrs)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return h.PushShard(ctx, id, a.ShardID, size, sum, f)
}

// hashWithChallenges reads a shard once and returns its SHA-256 plus
// challengesPerShard proof-of-storage challenges (nonce, SHA-256(nonce||shard)).
func hashWithChallenges(path string, size int64) (string, []api.Challenge, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", nil, err
	}
	defer f.Close()
	whole := sha256.New()
	writers := []io.Writer{whole}
	nonces := make([][]byte, challengesPerShard)
	hashers := make([]hash.Hash, challengesPerShard)
	for i := range nonces {
		nonces[i] = make([]byte, 32)
		if _, err := rand.Read(nonces[i]); err != nil {
			return "", nil, err
		}
		hashers[i] = sha256.New()
		hashers[i].Write(nonces[i])
		writers = append(writers, hashers[i])
	}
	n, err := io.Copy(io.MultiWriter(writers...), f)
	if err != nil {
		return "", nil, err
	}
	if n != size {
		return "", nil, fmt.Errorf("shard is %d bytes, expected %d", n, size)
	}
	chs := make([]api.Challenge, challengesPerShard)
	for i := range chs {
		chs[i] = api.Challenge{Challenge: nonces[i], ExpectedProof: hashers[i].Sum(nil)}
	}
	return fmt.Sprintf("%x", whole.Sum(nil)), chs, nil
}

// Download restores a file to outPath (or, if outPath is a directory or
// empty, to its original name there). It returns the written path.
func (c *Client) Download(ctx context.Context, fileID, outPath string, overwrite bool) (string, error) {
	info, locs, err := c.api.GetFile(ctx, fileID)
	if err != nil {
		return "", err
	}
	if info.Status == "uploading" {
		return "", errors.New("the file's upload has not finished")
	}
	wrapped, err := c.api.FileKey(ctx, fileID)
	if err != nil {
		return "", fmt.Errorf("file key: %w", err)
	}
	fileKey, err := crypt.UnwrapFileKey(c.master, wrapped, fileID)
	if err != nil {
		return "", err
	}
	name, err := crypt.DecryptName(fileKey, info.FileNameEnc)
	if err != nil {
		return "", fmt.Errorf("file name: %w", err)
	}
	name = filepath.Base(name)
	if outPath == "" {
		outPath = name
	} else if st, err := os.Stat(outPath); err == nil && st.IsDir() {
		outPath = filepath.Join(outPath, name)
	}
	if _, err := os.Stat(outPath); err == nil && !overwrite {
		return "", fmt.Errorf("%s exists (use --force to overwrite)", outPath)
	}

	dir, err := c.workDir("download-")
	if err != nil {
		return "", err
	}
	defer os.RemoveAll(dir)
	h, err := c.p2pHost()
	if err != nil {
		return "", err
	}

	have, err := c.fetchShards(ctx, h, info, locs, dir)
	if err != nil {
		return "", err
	}
	var missing []int
	for i := 0; i < info.DataShards; i++ {
		if have[i] == "" {
			missing = append(missing, i)
		}
	}
	if len(missing) > 0 {
		c.Log("rebuilding data shards %v from parity", missing)
		rebuilt, err := erasure.Reconstruct(have, missing, info.DataShards, info.ParityShards, dir)
		if err != nil {
			return "", err
		}
		for i, p := range rebuilt {
			have[i] = p
		}
	}
	data := make([]string, info.DataShards)
	for i := range data {
		data[i] = have[i]
	}

	tmp := outPath + ".orchestrator-partial"
	out, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return "", err
	}
	pr, pw := io.Pipe()
	go func() { pw.CloseWithError(erasure.Join(data, info.FileSizeBytes, pw)) }()
	_, err = crypt.DecryptFile(fileKey, pr, out)
	pr.Close()
	if cerr := out.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		os.Remove(tmp)
		return "", fmt.Errorf("decrypt: %w", err)
	}
	return outPath, os.Rename(tmp, outPath)
}

// fetchShards downloads verified shards until `data` distinct indexes are
// present, data shards first.
func (c *Client) fetchShards(ctx context.Context, h *p2p.Host, info api.FileInfo, locs []api.ShardLocation, dir string) (map[int]string, error) {
	byIndex := map[int][]api.ShardLocation{}
	for _, l := range locs {
		if l.Health == "good" && l.ShardHash != "" {
			byIndex[l.ShardIndex] = append(byIndex[l.ShardIndex], l)
		}
	}
	indexes := make([]int, 0, len(byIndex))
	for i := range byIndex {
		indexes = append(indexes, i)
	}
	sort.Ints(indexes) // data shards (lowest indexes) first: no decoding if they all work
	for _, cands := range byIndex {
		sort.SliceStable(cands, func(a, b int) bool { return cands[a].PeerOnline && !cands[b].PeerOnline })
	}

	have := map[int]string{}
	var errs []string
	for _, i := range indexes {
		if len(have) == info.DataShards {
			break
		}
		for _, l := range byIndex[i] {
			p, err := c.fetchOne(ctx, h, l, dir)
			if err != nil {
				errs = append(errs, fmt.Sprintf("index %d from %s: %v", i, short(l.PeerID), err))
				continue
			}
			have[i] = p
			c.Log("  shard %d <- peer %s", i, short(l.PeerID))
			break
		}
	}
	if len(have) < info.DataShards {
		return nil, fmt.Errorf("only %d of the %d shards needed could be fetched:\n  %s",
			len(have), info.DataShards, strings.Join(errs, "\n  "))
	}
	return have, nil
}

func (c *Client) fetchOne(ctx context.Context, h *p2p.Host, l api.ShardLocation, dir string) (string, error) {
	cctx, cancel := context.WithTimeout(ctx, 30*time.Minute)
	defer cancel()
	id, err := h.Dial(cctx, l.Libp2pID, append([]string{l.PeerAddress}, l.Multiaddrs...))
	if err != nil {
		return "", err
	}
	p := filepath.Join(dir, fmt.Sprintf("shard-%d", l.ShardIndex))
	f, err := os.Create(p)
	if err != nil {
		return "", err
	}
	hw := sha256.New()
	_, _, err = h.FetchShard(cctx, id, l.ShardID, io.MultiWriter(f, hw))
	f.Close()
	if err == nil && fmt.Sprintf("%x", hw.Sum(nil)) != l.ShardHash {
		err = errors.New("hash mismatch (corrupt or wrong shard)")
	}
	if err != nil {
		os.Remove(p)
		return "", err
	}
	return p, nil
}

// Entry is one row of List.
type Entry struct {
	api.FileInfo
	Name string
}

// List returns the user's files with decrypted names.
func (c *Client) List(ctx context.Context) ([]Entry, error) {
	files, err := c.api.ListFiles(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]Entry, len(files))
	for i, f := range files {
		out[i] = Entry{FileInfo: f, Name: "(name unavailable)"}
		wrapped, err := c.api.FileKey(ctx, f.FileID)
		if err != nil {
			continue
		}
		fk, err := crypt.UnwrapFileKey(c.master, wrapped, f.FileID)
		if err != nil {
			continue
		}
		if n, err := crypt.DecryptName(fk, f.FileNameEnc); err == nil {
			out[i].Name = n
		}
	}
	return out, nil
}

// Delete removes a file from the network.
func (c *Client) Delete(ctx context.Context, fileID string) error {
	return c.api.DeleteFile(ctx, fileID)
}

func short(id string) string {
	if len(id) > 8 {
		return id[:8]
	}
	return id
}
