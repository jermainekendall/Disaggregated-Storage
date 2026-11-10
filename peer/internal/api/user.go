package api

import (
	"context"
	"time"
)

// ---------------------------------------------------------------- user calls

type Session struct {
	UserID    string `json:"user_id"`
	Token     string `json:"token"`
	ExpiresAt int64  `json:"expires_at"`
	Email     string `json:"email"`
}

// Valid reports whether the token is still usable for at least a minute.
func (s Session) Valid() bool {
	return s.Token != "" && time.Until(time.Unix(s.ExpiresAt, 0)) > time.Minute
}

func (c *Client) Login(ctx context.Context, email, password string, register bool) (Session, error) {
	path := "/api/v1/auth/login"
	if register {
		path = "/api/v1/auth/register"
	}
	var s Session
	err := c.Do(ctx, "POST", path, map[string]string{"email": email, "password": password}, &s)
	s.Email = email
	return s, err
}

// MasterKey returns the wrapped master key, or a 404 error before the first upload.
func (c *Client) MasterKey(ctx context.Context) ([]byte, error) {
	var resp struct {
		WrappedMasterKey []byte `json:"wrapped_master_key"`
	}
	err := c.Do(ctx, "GET", "/api/v1/keys/master", nil, &resp)
	return resp.WrappedMasterKey, err
}

func (c *Client) PutMasterKey(ctx context.Context, wrapped []byte) error {
	return c.Do(ctx, "PUT", "/api/v1/keys/master", map[string]interface{}{
		"wrapped_master_key": wrapped, "algorithm": "AES-256-GCM+Argon2id"}, nil)
}

func (c *Client) FileKey(ctx context.Context, fileID string) ([]byte, error) {
	var resp struct {
		WrappedKey []byte `json:"wrapped_key"`
	}
	err := c.Do(ctx, "GET", "/api/v1/files/"+fileID+"/key", nil, &resp)
	return resp.WrappedKey, err
}

func (c *Client) PutFileKey(ctx context.Context, fileID string, wrapped []byte) error {
	return c.Do(ctx, "PUT", "/api/v1/files/"+fileID+"/key", map[string]interface{}{
		"wrapped_key": wrapped, "algorithm": "AES-256-GCM-STREAM"}, nil)
}

// UploadRequest is the body of POST /files/upload.
type UploadRequest struct {
	FileSizeBytes        int64  `json:"file_size_bytes"`
	FileNameEncrypted    []byte `json:"file_name_encrypted"`
	MimeType             string `json:"mime_type,omitempty"`
	StorageTier          string `json:"storage_tier,omitempty"`
	OwnerPeerID          string `json:"owner_peer_id,omitempty"`
	IsCompressed         bool   `json:"is_compressed"`
	CompressionAlgorithm string `json:"compression_algorithm,omitempty"`
	OriginalSizeBytes    int64  `json:"original_size_bytes,omitempty"`
}

// Assignment says which peer stores which shard.
type Assignment struct {
	ShardID     string   `json:"shard_id"`
	ShardIndex  int      `json:"shard_index"`
	ShardType   string   `json:"shard_type"`
	PeerID      string   `json:"peer_id"`
	Libp2pID    string   `json:"libp2p_peer_id"`
	PeerAddress string   `json:"peer_address"`
	Multiaddrs  []string `json:"multiaddrs"`
}

type UploadPlan struct {
	FileID     string       `json:"file_id"`
	Data       int          `json:"data_shards"`
	Parity     int          `json:"parity_shards"`
	ShardBytes int64        `json:"shard_size_bytes"`
	Shards     []Assignment `json:"shards"`
}

func (c *Client) StartUpload(ctx context.Context, r UploadRequest) (UploadPlan, error) {
	var p UploadPlan
	err := c.Do(ctx, "POST", "/api/v1/files/upload", r, &p)
	return p, err
}

type FileInfo struct {
	FileID          string    `json:"file_id"`
	FileNameEnc     []byte    `json:"file_name_encrypted"`
	FileSizeBytes   int64     `json:"file_size_bytes"`
	StorageTier     string    `json:"storage_tier"`
	Status          string    `json:"status"`
	DataShards      int       `json:"data_shards"`
	ParityShards    int       `json:"parity_shards"`
	AvailableShards int       `json:"available_shards"`
	CreatedAt       time.Time `json:"created_at"`
}

type ShardLocation struct {
	ShardID     string   `json:"shard_id"`
	ShardIndex  int      `json:"shard_index"`
	SizeBytes   int64    `json:"shard_size_bytes"`
	ShardHash   string   `json:"shard_hash"`
	Health      string   `json:"health_status"`
	PeerID      string   `json:"peer_id"`
	Libp2pID    string   `json:"libp2p_peer_id"`
	PeerOnline  bool     `json:"peer_online"`
	PeerAddress string   `json:"peer_address"`
	Multiaddrs  []string `json:"multiaddrs"`
}

func (c *Client) ListFiles(ctx context.Context) ([]FileInfo, error) {
	var resp struct {
		Files []FileInfo `json:"files"`
	}
	err := c.Do(ctx, "GET", "/api/v1/files", nil, &resp)
	return resp.Files, err
}

func (c *Client) GetFile(ctx context.Context, fileID string) (FileInfo, []ShardLocation, error) {
	var resp struct {
		File   FileInfo        `json:"file"`
		Shards []ShardLocation `json:"shards"`
	}
	err := c.Do(ctx, "GET", "/api/v1/files/"+fileID, nil, &resp)
	return resp.File, resp.Shards, err
}

func (c *Client) DeleteFile(ctx context.Context, fileID string) error {
	return c.Do(ctx, "DELETE", "/api/v1/files/"+fileID, nil, nil)
}

type Challenge struct {
	Challenge     []byte `json:"challenge"`
	ExpectedProof []byte `json:"expected_proof"`
}

func (c *Client) RegisterChallenges(ctx context.Context, shardID string, chs []Challenge) error {
	return c.Do(ctx, "POST", "/api/v1/shards/"+shardID+"/challenges", map[string]interface{}{"challenges": chs}, nil)
}
