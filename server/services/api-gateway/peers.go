package main

import (
	"context"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"time"

	"github.com/gorilla/mux"
	"github.com/lib/pq"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/jermainekendall/disaggregated-storage/server/internal/common"
	"github.com/jermainekendall/disaggregated-storage/server/internal/storage"
	peerpb "github.com/jermainekendall/disaggregated-storage/server/proto/peer"
)

var jsonOut = protojson.MarshalOptions{UseProtoNames: true, EmitUnpopulated: true}

func writeProto(w http.ResponseWriter, code int, m proto.Message) {
	b, err := jsonOut.Marshal(m)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	w.Write(b)
}

// handleRegisterPeer forwards to PeerRegistry.RegisterPeer, so HTTP and gRPC
// registration share one implementation. The body uses the proto field names
// (peer_id, libp2p_peer_id, multiaddrs, total_capacity_bytes, ...).
func (s *server) handleRegisterPeer(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	var req peerpb.PeerRegistration
	if err := protojson.Unmarshal(body, &req); err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", "invalid registration: "+err.Error())
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	resp, err := s.peers.RegisterPeer(ctx, &req)
	if err != nil {
		writeGRPCError(w, err)
		return
	}
	code := http.StatusOK
	if !resp.Success {
		code = http.StatusBadRequest
	}
	writeProto(w, code, resp)
}

func (s *server) handleListPeers(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	limit, _ := strconv.Atoi(q.Get("limit"))
	offset, _ := strconv.Atoi(q.Get("offset"))
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	resp, err := s.peers.ListPeers(ctx, &peerpb.ListPeersRequest{
		Status: q.Get("status"), Limit: int32(limit), Offset: int32(offset)})
	if err != nil {
		writeGRPCError(w, err)
		return
	}
	writeProto(w, http.StatusOK, resp)
}

var sha256Hex = regexp.MustCompile(`^[0-9a-f]{64}$`)

// handleConfirmShard is called by a peer once it has stored a shard (new
// upload, rebuild or copy). The shard turns good and the file's health is
// recomputed; a file becomes active when all its shards are confirmed.
func (s *server) handleConfirmShard(w http.ResponseWriter, r *http.Request) {
	shardID := mux.Vars(r)["id"]
	var req struct {
		PeerID    string `json:"peer_id"`
		ShardHash string `json:"shard_hash"`
	}
	if !decodeJSON(w, r, &req) {
		return
	}
	if !common.IsUUID(shardID) || !common.IsUUID(req.PeerID) {
		writeError(w, http.StatusBadRequest, "bad_request", "shard id and peer_id must be UUIDs")
		return
	}
	if !sha256Hex.MatchString(req.ShardHash) {
		writeError(w, http.StatusBadRequest, "bad_request", "shard_hash must be a lowercase hex SHA-256")
		return
	}
	var fileID string
	err := s.db.QueryRowContext(r.Context(), `
		UPDATE shards SET health_status = 'good', shard_hash = $3, stored_at = now(), updated_at = now()
		WHERE shard_id = $1 AND peer_id = $2 AND health_status = 'pending'
		RETURNING file_id`, shardID, req.PeerID, req.ShardHash).Scan(&fileID)
	if err != nil {
		writeError(w, http.StatusConflict, "conflict", "no pending shard with this id for this peer")
		return
	}
	s.db.ExecContext(r.Context(), `UPDATE peers SET total_uploads = total_uploads + 1 WHERE peer_id = $1`, req.PeerID)
	h, err := storage.RefreshFile(r.Context(), s.db, fileID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"file_id": fileID, "file_status": h.Status, "available_shards": h.Available, "total_shards": h.Total(),
	})
}

type shardSource struct {
	ShardIndex int      `json:"shard_index"`
	PeerID     string   `json:"peer_id"`
	Multiaddrs []string `json:"multiaddrs"`
}

type pendingShard struct {
	ShardID      string        `json:"shard_id"`
	FileID       string        `json:"file_id"`
	ShardIndex   int           `json:"shard_index"`
	ShardType    string        `json:"shard_type"`
	SizeBytes    int64         `json:"shard_size_bytes"`
	IsReplica    bool          `json:"is_replica"`
	DataShards   int           `json:"data_shards"`
	ParityShards int           `json:"parity_shards"`
	FileStatus   string        `json:"file_status"`
	Sources      []shardSource `json:"sources"` // good copies on online peers
}

// handlePendingShards tells a peer agent what to do: shards it should store
// (from the uploader, rebuilt from `data_shards` sources, or copied from a
// source of the same index) and shards it should delete.
func (s *server) handlePendingShards(w http.ResponseWriter, r *http.Request) {
	peerID := mux.Vars(r)["peer_id"]
	if !common.IsUUID(peerID) {
		writeError(w, http.StatusBadRequest, "bad_request", "peer_id must be the server-assigned UUID")
		return
	}
	rows, err := s.db.QueryContext(r.Context(), `
		SELECT s.shard_id, s.file_id, s.shard_index, s.shard_type, s.shard_size_bytes, s.is_replica,
		       f.shard_count, f.parity_shards, f.status
		FROM shards s JOIN files f ON f.file_id = s.file_id
		WHERE s.peer_id = $1 AND s.health_status = 'pending'
		ORDER BY s.stored_at LIMIT 100`, peerID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	pending := []pendingShard{}
	for rows.Next() {
		var p pendingShard
		if err := rows.Scan(&p.ShardID, &p.FileID, &p.ShardIndex, &p.ShardType, &p.SizeBytes, &p.IsReplica,
			&p.DataShards, &p.ParityShards, &p.FileStatus); err != nil {
			rows.Close()
			writeError(w, http.StatusInternalServerError, "internal", err.Error())
			return
		}
		pending = append(pending, p)
	}
	rows.Close()

	for i := range pending {
		p := &pending[i]
		p.Sources = []shardSource{}
		if p.FileStatus == "uploading" {
			continue // the uploader sends this shard directly
		}
		src, err := s.db.QueryContext(r.Context(), `
			SELECT s.shard_index, s.peer_id, COALESCE(p.multiaddrs, ARRAY[p.multiaddr])
			FROM shards s JOIN peers p ON p.peer_id = s.peer_id
			WHERE s.file_id = $1 AND s.health_status = 'good' AND p.is_online AND s.peer_id != $2
			ORDER BY (s.shard_index = $3) DESC, random()
			LIMIT 50`, p.FileID, peerID, p.ShardIndex)
		if err != nil {
			continue
		}
		for src.Next() {
			var ss shardSource
			if src.Scan(&ss.ShardIndex, &ss.PeerID, pq.Array(&ss.Multiaddrs)) == nil {
				p.Sources = append(p.Sources, ss)
			}
		}
		src.Close()
	}

	toDelete := []string{}
	del, err := s.db.QueryContext(r.Context(), `
		SELECT shard_id FROM shards WHERE peer_id = $1 AND health_status = 'retiring' LIMIT 500`, peerID)
	if err == nil {
		for del.Next() {
			var id string
			if del.Scan(&id) == nil {
				toDelete = append(toDelete, id)
			}
		}
		del.Close()
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"store": pending, "delete": toDelete})
}

// handleShardDeleted is called by a peer after deleting a retiring shard.
// When every copy of a deleting file is gone, the file becomes deleted.
func (s *server) handleShardDeleted(w http.ResponseWriter, r *http.Request) {
	shardID := mux.Vars(r)["id"]
	var req struct {
		PeerID string `json:"peer_id"`
	}
	if !decodeJSON(w, r, &req) {
		return
	}
	if !common.IsUUID(shardID) || !common.IsUUID(req.PeerID) {
		writeError(w, http.StatusBadRequest, "bad_request", "shard id and peer_id must be UUIDs")
		return
	}
	var fileID string
	err := s.db.QueryRowContext(r.Context(), `
		UPDATE shards SET health_status = 'deleted', updated_at = now()
		WHERE shard_id = $1 AND peer_id = $2 AND health_status = 'retiring'
		RETURNING file_id`, shardID, req.PeerID).Scan(&fileID)
	if err != nil {
		writeError(w, http.StatusConflict, "conflict", "no retiring shard with this id for this peer")
		return
	}
	s.db.ExecContext(r.Context(), `
		UPDATE files SET status = 'deleted', updated_at = now()
		WHERE file_id = $1 AND status = 'deleting'
		  AND NOT EXISTS (SELECT 1 FROM shards WHERE file_id = $1 AND health_status = 'retiring')`, fileID)
	writeJSON(w, http.StatusOK, map[string]string{"shard_id": shardID, "status": "deleted"})
}
