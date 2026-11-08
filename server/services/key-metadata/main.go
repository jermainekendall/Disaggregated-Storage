// Key Metadata Service: stores client-wrapped keys (zero knowledge).
//
//   - key_metadata: one wrapped master key per user.
//   - file_keys: per-file keys wrapped with the owner's master key, with
//     rotation history (one active key per file and user).
//   - Recovery: clients split their recovery secret with Shamir's scheme and
//     give the shares to holders themselves. The server records only who
//     holds which share index, never share contents.
//
// Rotation dates are advisory. An old key keeps working (otherwise files
// would become unreadable); GetFileKey reports rotation_due so the client
// rotates it.
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/jermainekendall/disaggregated-storage/server/internal/common"
	pb "github.com/jermainekendall/disaggregated-storage/server/proto/keymeta"
)

type config struct {
	GRPCPort            string
	KeyRotationDays     int
	RecoveryThreshold   int
	RecoveryTotalShares int
	RunBackground       bool
}

func loadConfig() config {
	return config{
		GRPCPort:            common.Env("GRPC_PORT", "50055"),
		KeyRotationDays:     common.EnvInt("KEY_ROTATION_DAYS", 365),
		RecoveryThreshold:   common.EnvInt("RECOVERY_THRESHOLD", 3),
		RecoveryTotalShares: common.EnvInt("RECOVERY_TOTAL_SHARES", 5),
		RunBackground:       common.EnvBool("RUN_BACKGROUND_JOBS", true),
	}
}

var requiredSchema = map[string][]string{
	"key_metadata": {"user_id", "wrapped_master_key", "master_key_algorithm", "recovery_key_shares_metadata",
		"recovery_threshold", "recovery_total_shares", "key_version", "last_rotated_at", "next_rotation_at",
		"rotation_schedule_days", "updated_at"},
	"file_keys": {"key_id", "file_id", "user_id", "wrapped_key", "derivation_salt", "algorithm", "key_version",
		"is_active", "created_at", "rotate_after", "rotated_at", "last_accessed_at"},
	"files": {"file_id", "owner_id"},
}

type server struct {
	pb.UnimplementedKeyMetadataServiceServer
	db  *sql.DB
	cfg config
}

func defaultAlg(a string) string {
	if a == "" {
		return "AES-256-GCM"
	}
	return a
}

func (s *server) StoreMasterKey(ctx context.Context, req *pb.StoreMasterKeyRequest) (*pb.StoreMasterKeyResponse, error) {
	if !common.IsUUID(req.UserId) {
		return nil, status.Error(codes.InvalidArgument, "user_id must be a UUID")
	}
	if len(req.WrappedMasterKey) == 0 {
		return nil, status.Error(codes.InvalidArgument, "wrapped_master_key is required")
	}
	var version int32
	err := s.db.QueryRowContext(ctx, `
		INSERT INTO key_metadata (user_id, wrapped_master_key, master_key_algorithm, recovery_threshold,
		                          recovery_total_shares, next_rotation_at)
		VALUES ($1, $2, $3, $4, $5, now() + $6::INTERVAL)
		ON CONFLICT (user_id) DO UPDATE SET
			wrapped_master_key = excluded.wrapped_master_key,
			master_key_algorithm = excluded.master_key_algorithm,
			key_version = key_metadata.key_version + 1,
			last_rotated_at = now(),
			next_rotation_at = now() + (key_metadata.rotation_schedule_days || ' days')::INTERVAL,
			updated_at = now()
		RETURNING key_version`,
		req.UserId, req.WrappedMasterKey, defaultAlg(req.Algorithm), s.cfg.RecoveryThreshold,
		s.cfg.RecoveryTotalShares, "90 days").Scan(&version)
	if err != nil {
		return nil, grpcErr(err, "store master key")
	}
	return &pb.StoreMasterKeyResponse{KeyVersion: version}, nil
}

func (s *server) GetMasterKey(ctx context.Context, req *pb.GetMasterKeyRequest) (*pb.MasterKey, error) {
	if !common.IsUUID(req.UserId) {
		return nil, status.Error(codes.InvalidArgument, "user_id must be a UUID")
	}
	k := pb.MasterKey{UserId: req.UserId}
	var next sql.NullTime
	err := s.db.QueryRowContext(ctx, `
		SELECT wrapped_master_key, master_key_algorithm, key_version, next_rotation_at
		FROM key_metadata WHERE user_id = $1`, req.UserId).
		Scan(&k.WrappedMasterKey, &k.Algorithm, &k.KeyVersion, &next)
	if err == sql.ErrNoRows {
		return nil, status.Error(codes.NotFound, "no master key for user")
	}
	if err != nil {
		return nil, grpcErr(err, "get master key")
	}
	k.NextRotationAt = common.Unix(next)
	return &k, nil
}

const fileKeyCols = `key_id, file_id, wrapped_key, derivation_salt, algorithm, key_version, created_at, rotate_after`

func scanFileKey(r interface{ Scan(...interface{}) error }) (*pb.FileKey, error) {
	var k pb.FileKey
	var created, rotateAfter time.Time
	if err := r.Scan(&k.KeyId, &k.FileId, &k.WrappedKey, &k.DerivationSalt, &k.Algorithm, &k.KeyVersion,
		&created, &rotateAfter); err != nil {
		return nil, err
	}
	k.CreatedAt, k.RotateAfter = created.Unix(), rotateAfter.Unix()
	k.RotationDue = time.Now().After(rotateAfter)
	return &k, nil
}

func validateFileKey(fileID, userID string, wrapped []byte) error {
	switch {
	case !common.IsUUID(fileID):
		return status.Error(codes.InvalidArgument, "file_id must be a UUID")
	case !common.IsUUID(userID):
		return status.Error(codes.InvalidArgument, "user_id must be a UUID")
	case len(wrapped) == 0:
		return status.Error(codes.InvalidArgument, "wrapped key is required")
	}
	return nil
}

// insertFileKey adds a new active key. version is 1 for a new file.
func (s *server) insertFileKey(ctx context.Context, tx *sql.Tx, fileID, userID string, wrapped, salt []byte,
	alg string, version int32) (*pb.FileKey, error) {
	return scanFileKey(tx.QueryRowContext(ctx, `
		INSERT INTO file_keys (file_id, user_id, wrapped_key, derivation_salt, algorithm, key_version, rotate_after)
		VALUES ($1, $2, $3, $4, $5, $6, now() + $7::INTERVAL)
		RETURNING `+fileKeyCols,
		fileID, userID, wrapped, salt, defaultAlg(alg), version, intervalDays(s.cfg.KeyRotationDays)))
}

func (s *server) StoreFileKey(ctx context.Context, req *pb.StoreFileKeyRequest) (*pb.FileKey, error) {
	if err := validateFileKey(req.FileId, req.UserId, req.WrappedKey); err != nil {
		return nil, err
	}
	var key *pb.FileKey
	err := common.RunTx(ctx, s.db, func(tx *sql.Tx) error {
		var owner string
		err := tx.QueryRowContext(ctx, `SELECT owner_id FROM files WHERE file_id = $1`, req.FileId).Scan(&owner)
		if err == sql.ErrNoRows {
			return status.Error(codes.NotFound, "file not found")
		}
		if err != nil {
			return err
		}
		if owner != req.UserId {
			return status.Error(codes.PermissionDenied, "only the file owner can store its key")
		}
		key, err = s.insertFileKey(ctx, tx, req.FileId, req.UserId, req.WrappedKey, req.DerivationSalt, req.Algorithm, 1)
		if common.IsUniqueViolation(err) {
			return status.Error(codes.AlreadyExists, "file already has an active key; use RotateFileKey")
		}
		return err
	})
	if err != nil {
		return nil, grpcErr(err, "store file key")
	}
	return key, nil
}

func (s *server) GetFileKey(ctx context.Context, req *pb.GetFileKeyRequest) (*pb.FileKey, error) {
	if !common.IsUUID(req.FileId) || !common.IsUUID(req.UserId) {
		return nil, status.Error(codes.InvalidArgument, "file_id and user_id must be UUIDs")
	}
	key, err := scanFileKey(s.db.QueryRowContext(ctx, `
		UPDATE file_keys SET last_accessed_at = now()
		WHERE file_id = $1 AND user_id = $2 AND is_active = true
		RETURNING `+fileKeyCols, req.FileId, req.UserId))
	if err == sql.ErrNoRows {
		return nil, status.Error(codes.NotFound, "key not found")
	}
	if err != nil {
		return nil, grpcErr(err, "get file key")
	}
	return key, nil
}

// RotateFileKey replaces the active key in one transaction, so a failure
// never leaves the file without an active key.
func (s *server) RotateFileKey(ctx context.Context, req *pb.RotateFileKeyRequest) (*pb.FileKey, error) {
	if err := validateFileKey(req.FileId, req.UserId, req.NewWrappedKey); err != nil {
		return nil, err
	}
	var key *pb.FileKey
	err := common.RunTx(ctx, s.db, func(tx *sql.Tx) error {
		var version int32
		err := tx.QueryRowContext(ctx, `
			UPDATE file_keys SET is_active = false, rotated_at = now()
			WHERE file_id = $1 AND user_id = $2 AND is_active = true
			RETURNING key_version`, req.FileId, req.UserId).Scan(&version)
		if err == sql.ErrNoRows {
			return status.Error(codes.NotFound, "no active key to rotate")
		}
		if err != nil {
			return err
		}
		key, err = s.insertFileKey(ctx, tx, req.FileId, req.UserId, req.NewWrappedKey, req.NewDerivationSalt,
			req.Algorithm, version+1)
		return err
	})
	if err != nil {
		return nil, grpcErr(err, "rotate file key")
	}
	log.Printf("rotated key for file %s to version %d", req.FileId, key.KeyVersion)
	return key, nil
}

type recoveryMeta struct {
	Holders []holder `json:"holders"`
}

type holder struct {
	HolderID   string `json:"holder_id"`
	ShareIndex int32  `json:"share_index"`
}

// RegisterRecoveryShares records who holds each recovery share.
func (s *server) RegisterRecoveryShares(ctx context.Context, req *pb.RegisterRecoveryRequest) (*pb.RecoveryInfo, error) {
	if !common.IsUUID(req.UserId) {
		return nil, status.Error(codes.InvalidArgument, "user_id must be a UUID")
	}
	n := int32(len(req.Holders))
	if req.Threshold < 2 || req.Threshold > n {
		return nil, status.Errorf(codes.InvalidArgument, "threshold must be between 2 and the number of holders (%d)", n)
	}
	meta := recoveryMeta{}
	seen := map[int32]bool{}
	for _, h := range req.Holders {
		if h.HolderId == "" || h.ShareIndex < 1 || seen[h.ShareIndex] {
			return nil, status.Error(codes.InvalidArgument, "each holder needs an id and a unique share_index >= 1")
		}
		seen[h.ShareIndex] = true
		meta.Holders = append(meta.Holders, holder{h.HolderId, h.ShareIndex})
	}
	raw, _ := json.Marshal(meta)
	res, err := s.db.ExecContext(ctx, `
		UPDATE key_metadata SET recovery_key_shares_metadata = $1, recovery_threshold = $2,
		       recovery_total_shares = $3, updated_at = now()
		WHERE user_id = $4`, string(raw), req.Threshold, n, req.UserId)
	if err != nil {
		return nil, grpcErr(err, "register recovery")
	}
	if c, _ := res.RowsAffected(); c == 0 {
		return nil, status.Error(codes.FailedPrecondition, "store a master key before registering recovery shares")
	}
	return &pb.RecoveryInfo{UserId: req.UserId, Threshold: req.Threshold, TotalShares: n, Holders: req.Holders}, nil
}

func (s *server) GetRecoveryInfo(ctx context.Context, req *pb.GetRecoveryInfoRequest) (*pb.RecoveryInfo, error) {
	if !common.IsUUID(req.UserId) {
		return nil, status.Error(codes.InvalidArgument, "user_id must be a UUID")
	}
	var raw sql.NullString
	info := pb.RecoveryInfo{UserId: req.UserId}
	err := s.db.QueryRowContext(ctx, `
		SELECT recovery_key_shares_metadata::STRING, recovery_threshold, recovery_total_shares
		FROM key_metadata WHERE user_id = $1`, req.UserId).Scan(&raw, &info.Threshold, &info.TotalShares)
	if err == sql.ErrNoRows {
		return nil, status.Error(codes.NotFound, "no key metadata for user")
	}
	if err != nil {
		return nil, grpcErr(err, "recovery info")
	}
	if !raw.Valid {
		return nil, status.Error(codes.NotFound, "no recovery shares registered")
	}
	var meta recoveryMeta
	if err := json.Unmarshal([]byte(raw.String), &meta); err != nil {
		return nil, grpcErr(err, "recovery info")
	}
	for _, h := range meta.Holders {
		info.Holders = append(info.Holders, &pb.ShareHolder{HolderId: h.HolderID, ShareIndex: h.ShareIndex})
	}
	return &info, nil
}

// reportDueRotations logs how many active file keys are past their rotation date.
func (s *server) reportDueRotations(ctx context.Context) {
	var n int
	if err := s.db.QueryRowContext(ctx,
		`SELECT count(*) FROM file_keys WHERE is_active = true AND rotate_after < now()`).Scan(&n); err != nil {
		log.Printf("rotation check: %v", err)
		return
	}
	if n > 0 {
		log.Printf("%d file keys are due for rotation (clients are told via rotation_due)", n)
	}
}

func intervalDays(d int) string { return fmt.Sprintf("%d days", d) }

func grpcErr(err error, what string) error {
	if err == nil {
		return nil
	}
	if _, ok := status.FromError(err); ok {
		return err
	}
	return status.Errorf(codes.Internal, "%s: %v", what, err)
}

func main() {
	ctx, stop := common.SignalContext()
	defer stop()

	cfg := loadConfig()
	db := common.MustOpenDB(ctx, requiredSchema)
	defer db.Close()

	s := &server{db: db, cfg: cfg}
	if cfg.RunBackground {
		go common.RunEvery(ctx, "rotation-check", 24*time.Hour, s.reportDueRotations)
	}
	common.ServeGRPC(ctx, "key-metadata", cfg.GRPCPort, func(g *grpc.Server) {
		pb.RegisterKeyMetadataServiceServer(g, s)
	})
}
