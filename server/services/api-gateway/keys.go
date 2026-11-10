package main

import (
	"context"
	"net/http"
	"time"

	"github.com/gorilla/mux"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/jermainekendall/disaggregated-storage/server/internal/common"
	auditpb "github.com/jermainekendall/disaggregated-storage/server/proto/audit"
	keypb "github.com/jermainekendall/disaggregated-storage/server/proto/keymeta"
)

// Key routes. Keys arrive already wrapped by the client; the server never
// sees a plaintext key. user_id always comes from the token.

func (s *server) handleGetMasterKey(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	k, err := s.keys.GetMasterKey(ctx, &keypb.GetMasterKeyRequest{UserId: userID(r)})
	if err != nil {
		writeGRPCError(w, err)
		return
	}
	writeProto(w, http.StatusOK, k)
}

func (s *server) handlePutMasterKey(w http.ResponseWriter, r *http.Request) {
	var req struct {
		WrappedMasterKey []byte `json:"wrapped_master_key"`
		Algorithm        string `json:"algorithm"`
	}
	if !decodeJSON(w, r, &req) {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	resp, err := s.keys.StoreMasterKey(ctx, &keypb.StoreMasterKeyRequest{
		UserId: userID(r), WrappedMasterKey: req.WrappedMasterKey, Algorithm: req.Algorithm})
	if err != nil {
		writeGRPCError(w, err)
		return
	}
	writeProto(w, http.StatusOK, resp)
}

func (s *server) handleGetFileKey(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	k, err := s.keys.GetFileKey(ctx, &keypb.GetFileKeyRequest{FileId: mux.Vars(r)["id"], UserId: userID(r)})
	if err != nil {
		writeGRPCError(w, err)
		return
	}
	writeProto(w, http.StatusOK, k)
}

func (s *server) handlePutFileKey(w http.ResponseWriter, r *http.Request) {
	var req struct {
		WrappedKey     []byte `json:"wrapped_key"`
		DerivationSalt []byte `json:"derivation_salt"`
		Algorithm      string `json:"algorithm"`
	}
	if !decodeJSON(w, r, &req) {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	k, err := s.keys.StoreFileKey(ctx, &keypb.StoreFileKeyRequest{FileId: mux.Vars(r)["id"], UserId: userID(r),
		WrappedKey: req.WrappedKey, DerivationSalt: req.DerivationSalt, Algorithm: req.Algorithm})
	if err != nil {
		writeGRPCError(w, err)
		return
	}
	writeProto(w, http.StatusCreated, k)
}

// handleRegisterChallenges lets a file's owner register proof-of-storage
// challenges for one of its shards.
func (s *server) handleRegisterChallenges(w http.ResponseWriter, r *http.Request) {
	shardID := mux.Vars(r)["id"]
	if !common.IsUUID(shardID) {
		writeError(w, http.StatusBadRequest, "bad_request", "shard id must be a UUID")
		return
	}
	var req struct {
		Challenges []struct {
			Challenge     []byte `json:"challenge"`
			ExpectedProof []byte `json:"expected_proof"`
		} `json:"challenges"`
	}
	if !decodeJSON(w, r, &req) {
		return
	}
	var owner string
	if err := s.db.QueryRowContext(r.Context(), `
		SELECT f.owner_id FROM shards s JOIN files f ON f.file_id = s.file_id WHERE s.shard_id = $1`,
		shardID).Scan(&owner); err != nil || owner != userID(r) {
		writeError(w, http.StatusNotFound, "not_found", "shard not found")
		return
	}
	in := &auditpb.RegisterChallengesRequest{ShardId: shardID}
	for _, c := range req.Challenges {
		in.Challenges = append(in.Challenges, &auditpb.Challenge{Challenge: c.Challenge, ExpectedProof: c.ExpectedProof})
	}
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	resp, err := s.audits.RegisterChallenges(ctx, in)
	if err != nil {
		writeGRPCError(w, err)
		return
	}
	writeProto(w, http.StatusOK, resp)
}

// handlePeerAudits lists the audit challenges a peer must answer.
func (s *server) handlePeerAudits(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	resp, err := s.audits.GetPendingAudits(ctx, &auditpb.PendingAuditsRequest{PeerId: mux.Vars(r)["peer_id"]})
	if err != nil {
		writeGRPCError(w, err)
		return
	}
	writeProto(w, http.StatusOK, resp)
}

func (s *server) handleAuditResponse(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Proof []byte `json:"proof"`
	}
	if !decodeJSON(w, r, &req) {
		return
	}
	if len(req.Proof) != 32 {
		writeGRPCError(w, status.Error(codes.InvalidArgument, "proof must be a 32-byte SHA-256 (base64 in JSON)"))
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	resp, err := s.audits.VerifyResponse(ctx, &auditpb.VerifyRequest{AuditId: mux.Vars(r)["id"], Proof: req.Proof})
	if err != nil {
		writeGRPCError(w, err)
		return
	}
	writeProto(w, http.StatusOK, resp)
}
