// Package api talks to the Orchestrator server: the HTTP gateway and the
// peer registry's gRPC heartbeat.
package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	peerpb "github.com/jermainekendall/disaggregated-storage/server/proto/peer"
)

// Client is safe for concurrent use.
type Client struct {
	base  string
	http  *http.Client
	token string // bearer token for user endpoints (empty for agent calls)

	registry peerpb.PeerRegistryClient
	conn     *grpc.ClientConn
}

// New creates a client. registryAddr may be empty for client-only use.
func New(gatewayURL, registryAddr string) (*Client, error) {
	c := &Client{base: strings.TrimRight(gatewayURL, "/"), http: &http.Client{Timeout: 60 * time.Second}}
	if registryAddr != "" {
		conn, err := grpc.NewClient(registryAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, err
		}
		c.conn = conn
		c.registry = peerpb.NewPeerRegistryClient(conn)
	}
	return c, nil
}

// WithToken returns a copy that authenticates as a user.
func (c *Client) WithToken(token string) *Client {
	cp := *c
	cp.token = token
	return &cp
}

func (c *Client) Close() {
	if c.conn != nil {
		c.conn.Close()
	}
}

// Error is a non-2xx gateway response.
type Error struct {
	Status  int
	Kind    string `json:"error"`
	Message string `json:"message"`
}

func (e *Error) Error() string {
	if e.Message != "" {
		return fmt.Sprintf("server: %s (%d %s)", e.Message, e.Status, e.Kind)
	}
	return fmt.Sprintf("server returned HTTP %d", e.Status)
}

// IsStatus reports whether err is a gateway error with the given HTTP status.
func IsStatus(err error, code int) bool {
	var e *Error
	return errors.As(err, &e) && e.Status == code
}

// retryable: network errors, 429 and 5xx. Client errors are final.
func retryable(err error) bool {
	var e *Error
	if errors.As(err, &e) {
		return e.Status == http.StatusTooManyRequests || e.Status >= 500
	}
	return err != nil && !errors.Is(err, context.Canceled)
}

// Retry runs fn up to attempts times with exponential backoff and jitter.
func Retry(ctx context.Context, attempts int, fn func() error) error {
	var err error
	delay := 500 * time.Millisecond
	for i := 0; i < attempts; i++ {
		if err = fn(); err == nil || !retryable(err) {
			return err
		}
		if i == attempts-1 {
			break
		}
		sleep := delay + time.Duration(rand.Int64N(int64(delay/2)))
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(sleep):
		}
		if delay < 30*time.Second {
			delay *= 2
		}
	}
	return err
}

// Do sends a JSON request and decodes a JSON response into out (if non-nil),
// retrying transient failures.
func (c *Client) Do(ctx context.Context, method, path string, in, out interface{}) error {
	var body []byte
	if in != nil {
		var err error
		if body, err = json.Marshal(in); err != nil {
			return err
		}
	}
	return Retry(ctx, 6, func() error {
		req, err := http.NewRequestWithContext(ctx, method, c.base+path, bytes.NewReader(body))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/json")
		if c.token != "" {
			req.Header.Set("Authorization", "Bearer "+c.token)
		}
		resp, err := c.http.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		raw, err := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
		if err != nil {
			return err
		}
		if resp.StatusCode >= 300 {
			e := &Error{Status: resp.StatusCode}
			json.Unmarshal(raw, e)
			return e
		}
		if out != nil && len(raw) > 0 {
			if err := json.Unmarshal(raw, out); err != nil {
				return fmt.Errorf("decode %s %s: %w", method, path, err)
			}
		}
		return nil
	})
}

// ---------------------------------------------------------------- agent calls

// Registration is the body of POST /api/v1/peers/register (proto field names).
type Registration struct {
	PeerID                 string   `json:"peer_id,omitempty"`
	Libp2pPeerID           string   `json:"libp2p_peer_id"`
	Multiaddr              string   `json:"multiaddr,omitempty"`
	Multiaddrs             []string `json:"multiaddrs"`
	TotalCapacityBytes     int64    `json:"total_capacity_bytes,string"`
	AvailableCapacityBytes int64    `json:"available_capacity_bytes,string"`
	Region                 string   `json:"region,omitempty"`
	ASN                    string   `json:"asn,omitempty"`
	Timezone               string   `json:"timezone,omitempty"`
	Version                string   `json:"version,omitempty"`
	UserID                 string   `json:"user_id,omitempty"`
	DeviceModel            string   `json:"device_model,omitempty"`
	OSType                 string   `json:"os_type,omitempty"`
}

// Register registers (or re-registers) the peer and returns its server UUID.
func (c *Client) Register(ctx context.Context, r Registration) (string, error) {
	var resp struct {
		Success bool   `json:"success"`
		Message string `json:"message"`
		PeerID  string `json:"peer_id"`
	}
	err := c.Do(ctx, "POST", "/api/v1/peers/register", r, &resp)
	if err == nil && !resp.Success {
		err = fmt.Errorf("registration rejected: %s", resp.Message)
	}
	var e *Error
	if errors.As(err, &e) && e.Status == http.StatusBadRequest {
		// The gateway returns 400 with the registry's message in the body.
		err = fmt.Errorf("registration rejected: %s", e.Message)
	}
	return resp.PeerID, err
}

// Heartbeat is the agent's periodic liveness report.
type Heartbeat struct {
	PeerID         string
	AvailableBytes int64
	Multiaddrs     []string
	Charging       bool
	BatteryLevel   int
	NetworkType    string
}

// SendHeartbeat reports liveness over gRPC to the peer registry.
func (c *Client) SendHeartbeat(ctx context.Context, h Heartbeat) error {
	if c.registry == nil {
		return errors.New("no registry address configured")
	}
	return Retry(ctx, 3, func() error {
		cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		resp, err := c.registry.Heartbeat(cctx, &peerpb.HeartbeatRequest{
			PeerId:                 h.PeerID,
			AvailableCapacityBytes: h.AvailableBytes,
			Multiaddrs:             h.Multiaddrs,
			IsCharging:             h.Charging,
			BatteryLevel:           int32(h.BatteryLevel),
			NetworkType:            h.NetworkType,
		})
		if err != nil {
			return err
		}
		if !resp.Success {
			return &Error{Status: http.StatusNotFound, Message: resp.Message}
		}
		return nil
	})
}

// Source is a peer holding a good copy of a shard index.
type Source struct {
	ShardID    string   `json:"shard_id"`
	ShardIndex int      `json:"shard_index"`
	PeerID     string   `json:"peer_id"`
	Libp2pID   string   `json:"libp2p_peer_id"`
	Multiaddrs []string `json:"multiaddrs"`
}

// Work is one shard the server wants this peer to store.
type Work struct {
	ShardID      string   `json:"shard_id"`
	FileID       string   `json:"file_id"`
	ShardIndex   int      `json:"shard_index"`
	ShardType    string   `json:"shard_type"`
	SizeBytes    int64    `json:"shard_size_bytes"`
	IsReplica    bool     `json:"is_replica"`
	DataShards   int      `json:"data_shards"`
	ParityShards int      `json:"parity_shards"`
	FileStatus   string   `json:"file_status"`
	ExpectedHash string   `json:"expected_hash"`
	Sources      []Source `json:"sources"`
}

// PendingWork is the response of GET /peers/{id}/pending-shards.
type PendingWork struct {
	Store  []Work   `json:"store"`
	Delete []string `json:"delete"`
}

func (c *Client) PendingShards(ctx context.Context, peerID string) (PendingWork, error) {
	var w PendingWork
	err := c.Do(ctx, "GET", "/api/v1/peers/"+peerID+"/pending-shards", nil, &w)
	return w, err
}

func (c *Client) ConfirmShard(ctx context.Context, shardID, peerID, sha256hex string) error {
	return c.Do(ctx, "POST", "/api/v1/shards/"+shardID+"/confirm",
		map[string]string{"peer_id": peerID, "shard_hash": sha256hex}, nil)
}

func (c *Client) ShardDeleted(ctx context.Context, shardID, peerID string) error {
	return c.Do(ctx, "POST", "/api/v1/shards/"+shardID+"/deleted", map[string]string{"peer_id": peerID}, nil)
}

// Audit is a challenge the server wants this peer to answer.
type Audit struct {
	AuditID   string `json:"audit_id"`
	ShardID   string `json:"shard_id"`
	Challenge []byte `json:"challenge"`
	ExpiresAt string `json:"expires_at"`
}

func (c *Client) PendingAudits(ctx context.Context, peerID string) ([]Audit, error) {
	var resp struct {
		Audits []Audit `json:"audits"`
	}
	err := c.Do(ctx, "GET", "/api/v1/peers/"+peerID+"/audits", nil, &resp)
	return resp.Audits, err
}

func (c *Client) AnswerAudit(ctx context.Context, auditID string, proof []byte) (bool, error) {
	var resp struct {
		Passed bool   `json:"passed"`
		Status string `json:"status"`
	}
	err := c.Do(ctx, "POST", "/api/v1/audits/"+auditID+"/response", map[string][]byte{"proof": proof}, &resp)
	return resp.Passed, err
}
