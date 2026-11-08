package main

import (
	"context"
	"database/sql"
	"net/http"
	"time"

	"github.com/gorilla/mux"
	"github.com/lib/pq"

	"github.com/jermainekendall/disaggregated-storage/server/internal/common"
	placementpb "github.com/jermainekendall/disaggregated-storage/server/proto/placement"
)

type fileSummary struct {
	FileID          string    `json:"file_id"`
	FileNameEnc     []byte    `json:"file_name_encrypted"` // base64 in JSON
	FileSizeBytes   int64     `json:"file_size_bytes"`
	MimeType        string    `json:"mime_type,omitempty"`
	StorageTier     string    `json:"storage_tier"`
	Status          string    `json:"status"`
	DataShards      int       `json:"data_shards"`
	ParityShards    int       `json:"parity_shards"`
	AvailableShards int       `json:"available_shards"`
	CreatedAt       time.Time `json:"created_at"`
}

const fileCols = `file_id, file_name_encrypted, file_size_bytes, COALESCE(mime_type, ''), storage_tier, status,
	shard_count, parity_shards, available_shards, created_at`

func scanFile(r interface{ Scan(...interface{}) error }) (fileSummary, error) {
	var f fileSummary
	err := r.Scan(&f.FileID, &f.FileNameEnc, &f.FileSizeBytes, &f.MimeType, &f.StorageTier, &f.Status,
		&f.DataShards, &f.ParityShards, &f.AvailableShards, &f.CreatedAt)
	return f, err
}

func (s *server) handleListFiles(w http.ResponseWriter, r *http.Request) {
	rows, err := s.db.QueryContext(r.Context(), `
		SELECT `+fileCols+` FROM files
		WHERE owner_id = $1 AND status NOT IN ('deleting', 'deleted')
		ORDER BY created_at DESC LIMIT 500`, userID(r))
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	defer rows.Close()
	files := []fileSummary{}
	for rows.Next() {
		f, err := scanFile(rows)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "internal", err.Error())
			return
		}
		files = append(files, f)
	}
	if err := rows.Err(); err != nil {
		writeError(w, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"files": files})
}

type shardLocation struct {
	ShardID     string   `json:"shard_id"`
	ShardIndex  int      `json:"shard_index"`
	ShardType   string   `json:"shard_type"`
	SizeBytes   int64    `json:"shard_size_bytes"`
	ShardHash   string   `json:"shard_hash,omitempty"`
	Health      string   `json:"health_status"`
	IsReplica   bool     `json:"is_replica"`
	PeerID      string   `json:"peer_id"`
	PeerOnline  bool     `json:"peer_online"`
	PeerAddress string   `json:"peer_address,omitempty"`
	Multiaddrs  []string `json:"multiaddrs,omitempty"`
}

// handleGetFile returns the file and where its shards are, and records the
// access (which drives hot/warm/cold tiering).
func (s *server) handleGetFile(w http.ResponseWriter, r *http.Request) {
	id := mux.Vars(r)["id"]
	if !common.IsUUID(id) {
		writeError(w, http.StatusBadRequest, "bad_request", "file id must be a UUID")
		return
	}
	f, err := scanFile(s.db.QueryRowContext(r.Context(), `
		SELECT `+fileCols+` FROM files WHERE file_id = $1 AND owner_id = $2 AND status != 'deleted'`, id, userID(r)))
	if err == sql.ErrNoRows {
		writeError(w, http.StatusNotFound, "not_found", "file not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal", err.Error())
		return
	}

	rows, err := s.db.QueryContext(r.Context(), `
		SELECT s.shard_id, s.shard_index, s.shard_type, s.shard_size_bytes, COALESCE(s.shard_hash, ''),
		       s.health_status, s.is_replica, s.peer_id, p.is_online, COALESCE(p.multiaddr, ''), p.multiaddrs
		FROM shards s JOIN peers p ON p.peer_id = s.peer_id
		WHERE s.file_id = $1 AND s.health_status IN ('good', 'pending', 'degraded')
		ORDER BY s.shard_index, s.is_replica`, id)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	defer rows.Close()
	shards := []shardLocation{}
	for rows.Next() {
		var l shardLocation
		if err := rows.Scan(&l.ShardID, &l.ShardIndex, &l.ShardType, &l.SizeBytes, &l.ShardHash, &l.Health,
			&l.IsReplica, &l.PeerID, &l.PeerOnline, &l.PeerAddress, pq.Array(&l.Multiaddrs)); err != nil {
			writeError(w, http.StatusInternalServerError, "internal", err.Error())
			return
		}
		shards = append(shards, l)
	}

	if _, err := s.db.ExecContext(r.Context(), `
		WITH logged AS (
			INSERT INTO file_access_logs (file_id, user_id, access_type, client_ip) VALUES ($1, $2, 'retrieve', $3)
			RETURNING file_id)
		UPDATE files SET access_count = access_count + 1, last_accessed_at = now()
		WHERE file_id = (SELECT file_id FROM logged)`, id, userID(r), clientIP(r)); err != nil {
		// Logging the access must not fail the download.
		_ = err
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"file": f, "shards": shards})
}

type uploadRequest struct {
	FileSizeBytes        int64  `json:"file_size_bytes"`
	FileNameEncrypted    []byte `json:"file_name_encrypted"` // base64 in JSON
	MimeType             string `json:"mime_type"`
	StorageTier          string `json:"storage_tier"`
	OwnerPeerID          string `json:"owner_peer_id"`
	IsCompressed         bool   `json:"is_compressed"`
	CompressionAlgorithm string `json:"compression_algorithm"`
	OriginalSizeBytes    int64  `json:"original_size_bytes"`
}

// handleUpload asks placement for one peer per shard, records the file as
// 'uploading' with one pending shard per peer, and returns where to send each
// shard. The client encrypts, erasure codes and sends shards to the peers;
// each peer confirms with /shards/{id}/confirm, and the file becomes active
// when all shards are confirmed.
func (s *server) handleUpload(w http.ResponseWriter, r *http.Request) {
	var req uploadRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	switch {
	case req.FileSizeBytes <= 0:
		writeError(w, http.StatusBadRequest, "bad_request", "file_size_bytes must be positive")
		return
	case len(req.FileNameEncrypted) == 0:
		writeError(w, http.StatusBadRequest, "bad_request", "file_name_encrypted is required")
		return
	}
	if req.StorageTier == "" {
		req.StorageTier = "cold"
	}
	if req.StorageTier != "cold" && req.StorageTier != "warm" && req.StorageTier != "hot" {
		writeError(w, http.StatusBadRequest, "bad_request", "storage_tier must be cold, warm or hot")
		return
	}
	switch req.CompressionAlgorithm {
	case "", "gzip", "zstd", "lz4":
	default:
		writeError(w, http.StatusBadRequest, "bad_request", "compression_algorithm must be gzip, zstd or lz4")
		return
	}

	uid := userID(r)
	var quotaGB int
	var usedBytes int64
	if err := s.db.QueryRowContext(r.Context(), `
		SELECT u.storage_quota_gb,
		       (SELECT COALESCE(sum(file_size_bytes), 0) FROM files
		        WHERE owner_id = u.user_id AND status NOT IN ('deleting', 'deleted'))
		FROM users u WHERE u.user_id = $1`, uid).Scan(&quotaGB, &usedBytes); err != nil {
		writeError(w, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	if usedBytes+req.FileSizeBytes > int64(quotaGB)<<30 {
		writeError(w, http.StatusForbidden, "quota_exceeded", "upload would exceed your storage quota")
		return
	}

	fileID := newUUID()
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	sel, err := s.placement.SelectPeersForUpload(ctx, &placementpb.UploadRequest{
		FileId: fileID, FileSizeBytes: req.FileSizeBytes, OwnerPeerId: req.OwnerPeerID, StorageTier: req.StorageTier,
	})
	if err != nil {
		writeGRPCError(w, err)
		return
	}

	type assignment struct {
		ShardID     string   `json:"shard_id"`
		ShardIndex  int32    `json:"shard_index"`
		ShardType   string   `json:"shard_type"`
		PeerID      string   `json:"peer_id"`
		PeerAddress string   `json:"peer_address"`
		Multiaddrs  []string `json:"multiaddrs,omitempty"`
	}
	var out []assignment
	err = common.RunTx(r.Context(), s.db, func(tx *sql.Tx) error {
		out = out[:0]
		var origSize sql.NullInt64
		if req.OriginalSizeBytes > 0 {
			origSize = sql.NullInt64{Int64: req.OriginalSizeBytes, Valid: true}
		}
		if _, err := tx.ExecContext(r.Context(), `
			INSERT INTO files (file_id, owner_id, file_name_encrypted, file_size_bytes, mime_type, storage_tier,
			                   shard_count, parity_shards, is_compressed, compression_algorithm,
			                   original_size_bytes, status)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, 'uploading')`,
			fileID, uid, req.FileNameEncrypted, req.FileSizeBytes, common.NullIfEmpty(req.MimeType),
			req.StorageTier, sel.ShardCount, sel.ParityShards, req.IsCompressed,
			common.NullIfEmpty(req.CompressionAlgorithm), origSize); err != nil {
			return err
		}
		for _, p := range sel.Peers {
			var shardID string
			if err := tx.QueryRowContext(r.Context(), `
				INSERT INTO shards (file_id, peer_id, shard_index, shard_type, shard_size_bytes, health_status)
				VALUES ($1, $2, $3, $4, $5, 'pending') RETURNING shard_id`,
				fileID, p.PeerId, p.ShardIndex, p.ShardType, sel.ShardSizeBytes).Scan(&shardID); err != nil {
				return err
			}
			out = append(out, assignment{shardID, p.ShardIndex, p.ShardType, p.PeerId, p.PeerAddress, p.Multiaddrs})
		}
		return nil
	})
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, map[string]interface{}{
		"file_id":          fileID,
		"data_shards":      sel.ShardCount,
		"parity_shards":    sel.ParityShards,
		"shard_size_bytes": sel.ShardSizeBytes,
		"shards":           out,
	})
}

// handleDeleteFile marks the file deleting and every copy retiring; peers
// delete retiring shards when they see them in pending-shards.
func (s *server) handleDeleteFile(w http.ResponseWriter, r *http.Request) {
	id := mux.Vars(r)["id"]
	if !common.IsUUID(id) {
		writeError(w, http.StatusBadRequest, "bad_request", "file id must be a UUID")
		return
	}
	found := false
	err := common.RunTx(r.Context(), s.db, func(tx *sql.Tx) error {
		res, err := tx.ExecContext(r.Context(), `
			UPDATE files SET status = 'deleting', deleted_at = now(), updated_at = now()
			WHERE file_id = $1 AND owner_id = $2 AND status NOT IN ('deleting', 'deleted')`, id, userID(r))
		if err != nil {
			return err
		}
		n, _ := res.RowsAffected()
		if found = n > 0; !found {
			return nil
		}
		if _, err := tx.ExecContext(r.Context(), `
			UPDATE shards SET health_status = 'retiring', removed_at = now(), updated_at = now()
			WHERE file_id = $1 AND health_status != 'missing'`, id); err != nil {
			return err
		}
		_, err = tx.ExecContext(r.Context(), `
			UPDATE repair_queue SET status = 'completed', completed_at = now(), last_error = 'file deleted'
			WHERE file_id = $1 AND status IN ('queued', 'processing', 'awaiting_data')`, id)
		return err
	})
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	if !found {
		writeError(w, http.StatusNotFound, "not_found", "file not found")
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]string{"file_id": id, "status": "deleting"})
}
