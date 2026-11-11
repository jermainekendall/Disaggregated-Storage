-- CockroachDB schema for the Distributed Storage Orchestrator
-- Version: 3.0 (2026-11-08)
--
-- This file is the single source of truth for the database. Every service's
-- queries are written against it, and each service checks at startup that the
-- columns it uses exist (see internal/common/schema.go).
--
-- It is safe to run repeatedly: tables use IF NOT EXISTS, and the UPGRADES
-- section at the end brings a database created from an earlier version
-- (v2.0 + 001_add_libp2p_support) up to date.
--
-- Apply with:
--   cockroach sql --insecure --host=<host> --database=orchestrator < schema.sql
--
-- CHANGELOG
-- v3.1 (2026-11-11): users.role and admin_audit_log for the admin console
-- v3.0 (2026-11-08):
--   - peers: user_id nullable (peers can register before an account is linked);
--     libp2p_peer_id (unique), multiaddr, multiaddrs folded in from migration 001;
--     churn_prediction columns; updated_at
--   - shards is the one placement table (one row per shard copy on a peer).
--     Adds removed_at, is_replica, 'pending'/'retiring' health states;
--     shard_hash is nullable until the peer confirms the upload
--   - files: shard limits relaxed (data 2-100, parity 1-10) so small test
--     clusters can use smaller codes; production defaults remain 10+4
--   - new: audit_challenges, file_keys, analytics_events
--   - audit_logs: challenge_id, expires_at
--   - repair_queue: one open job per file, 'awaiting_data' state
--   - replication_jobs: target_shard_id
--   - earnings: user_id nullable, bandwidth columns, one row per peer per period
--   - migration 001 removed: its backfill read columns that never existed and
--     wrote undialable placeholder multiaddrs
-- v2.0 (2026-03-01): adaptive sharding, compression
-- v1.0 (2026-01-18): initial schema

-- ============================================================================
-- USERS & AUTHENTICATION
-- ============================================================================

CREATE TABLE IF NOT EXISTS users (
    user_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    email VARCHAR(255) UNIQUE NOT NULL,
    password_hash VARCHAR(255) NOT NULL, -- bcrypt
    tier VARCHAR(20) NOT NULL DEFAULT 'free', -- 'free', 'paid'
    role VARCHAR(20) NOT NULL DEFAULT 'user', -- 'user', 'admin' (admin console access)
    status VARCHAR(20) NOT NULL DEFAULT 'active', -- 'active', 'suspended', 'deleted'

    storage_contributed_gb INT NOT NULL DEFAULT 0,
    storage_used_gb INT NOT NULL DEFAULT 0,
    storage_quota_gb INT NOT NULL DEFAULT 25,

    current_balance_usd DECIMAL(10,4) NOT NULL DEFAULT 0.0000,
    lifetime_earnings_usd DECIMAL(12,4) NOT NULL DEFAULT 0.0000,

    payment_method VARCHAR(50), -- 'paypal', 'bank', 'crypto', 'gift_card', 'platform_credit'
    payment_email VARCHAR(255),
    payment_address TEXT,
    payout_threshold_usd DECIMAL(10,2) NOT NULL DEFAULT 5.00,

    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    last_login_at TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),

    INDEX idx_tier (tier),
    INDEX idx_status (status),
    INDEX idx_created_at (created_at)
);

-- ============================================================================
-- PEERS (storage nodes)
-- ============================================================================

CREATE TABLE IF NOT EXISTS peers (
    peer_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID REFERENCES users(user_id) ON DELETE CASCADE, -- NULL until linked

    -- libp2p identity and addresses
    libp2p_peer_id VARCHAR(100) UNIQUE,
    multiaddr TEXT,     -- primary address to dial
    multiaddrs TEXT[],  -- all known addresses, including relay circuits

    -- Network information
    asn VARCHAR(20) NOT NULL DEFAULT '',      -- e.g. "AS20057"
    timezone VARCHAR(50) NOT NULL DEFAULT 'UTC', -- IANA name, e.g. "America/New_York"
    region VARCHAR(50) NOT NULL DEFAULT '',   -- e.g. "us-east"
    country_code VARCHAR(2),
    isp VARCHAR(100),

    -- Device information
    device_model VARCHAR(100),
    os_type VARCHAR(20), -- 'ios', 'android', 'desktop'
    os_version VARCHAR(50),
    app_version VARCHAR(20),

    -- Capacity
    storage_total_gb INT NOT NULL DEFAULT 0,
    storage_free_gb DECIMAL(10,3) NOT NULL DEFAULT 0,
    bandwidth_max_mbps INT,

    -- Current state
    is_online BOOLEAN NOT NULL DEFAULT false,
    is_charging BOOLEAN NOT NULL DEFAULT false,
    battery_level INT CHECK (battery_level >= 0 AND battery_level <= 100),
    network_type VARCHAR(30), -- 'wifi', 'cellular_unmetered', 'cellular_metered'
    is_low_power_mode BOOLEAN NOT NULL DEFAULT false,

    -- Health & reliability
    health_score DECIMAL(5,2) NOT NULL DEFAULT 100.0 CHECK (health_score >= 0 AND health_score <= 200),
    reliability_score DECIMAL(4,3) NOT NULL DEFAULT 1.000 CHECK (reliability_score >= 0 AND reliability_score <= 1),
    uptime_percentage DECIMAL(5,2) NOT NULL DEFAULT 0.00,
    churn_prediction DECIMAL(4,3),
    churn_prediction_updated_at TIMESTAMP,

    -- Statistics
    total_uploads INT NOT NULL DEFAULT 0,
    total_downloads INT NOT NULL DEFAULT 0,
    failed_audits INT NOT NULL DEFAULT 0,
    successful_audits INT NOT NULL DEFAULT 0,
    bytes_served BIGINT NOT NULL DEFAULT 0,

    -- Timestamps
    first_seen_at TIMESTAMP NOT NULL DEFAULT NOW(),
    last_seen_at TIMESTAMP NOT NULL DEFAULT NOW(),
    last_heartbeat_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),

    INDEX idx_user_id (user_id),
    INDEX idx_asn (asn),
    INDEX idx_timezone (timezone),
    INDEX idx_region (region),
    INDEX idx_health_score (health_score),
    INDEX idx_is_online (is_online),
    INDEX idx_last_seen (last_seen_at),
    INDEX idx_composite_search (is_online, health_score, storage_free_gb)
);

-- ============================================================================
-- FILES
-- ============================================================================

CREATE TABLE IF NOT EXISTS files (
    file_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    owner_id UUID NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,

    file_name_encrypted BYTEA NOT NULL, -- AES-GCM encrypted by the client
    file_size_bytes BIGINT NOT NULL,    -- size after encryption (what is stored)
    mime_type VARCHAR(100),

    storage_tier VARCHAR(20) NOT NULL DEFAULT 'cold', -- 'cold', 'warm', 'hot'

    -- Erasure coding. Production default is 10 data + 4 parity; the limits
    -- allow smaller codes for small test clusters.
    shard_count INT NOT NULL DEFAULT 10,
    parity_shards INT NOT NULL DEFAULT 4,
    CONSTRAINT files_shard_count_range CHECK (shard_count >= 2 AND shard_count <= 100),
    CONSTRAINT files_parity_range CHECK (parity_shards >= 1 AND parity_shards <= 10),

    is_compressed BOOLEAN NOT NULL DEFAULT false,
    compression_algorithm VARCHAR(20) CHECK (compression_algorithm IN ('gzip', 'zstd', 'lz4')),
    original_size_bytes BIGINT,
    compressed_size_bytes BIGINT,

    access_count INT NOT NULL DEFAULT 0, -- lifetime; tiering uses file_access_logs
    last_accessed_at TIMESTAMP,
    popularity_score DECIMAL(10,4) NOT NULL DEFAULT 0.0000,

    -- 'uploading', 'active', 'degraded', 'deleting', 'deleted'
    status VARCHAR(20) NOT NULL DEFAULT 'uploading',
    available_shards INT NOT NULL DEFAULT 0,

    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    deleted_at TIMESTAMP,

    INDEX idx_owner_id (owner_id),
    INDEX idx_storage_tier (storage_tier),
    INDEX idx_status (status),
    INDEX idx_created_at (created_at),
    INDEX idx_last_accessed (last_accessed_at),
    INDEX idx_popularity (popularity_score),
    INDEX idx_degraded (status, available_shards) WHERE status = 'degraded',
    INDEX idx_shard_count (shard_count),
    INDEX idx_compressed (is_compressed)
);

-- ============================================================================
-- SHARDS: one row per shard copy held by a peer
-- ============================================================================
-- A file has shard_count + parity_shards distinct shards (shard_index
-- 0..n-1), each normally on a different peer. Hot files may get extra copies
-- of data shards (is_replica = true) for read throughput.
--
-- health_status:
--   pending   placed, waiting for the peer to confirm it stored the bytes
--   good      confirmed and passing audits
--   degraded  failing audits
--   missing   lost (peer gone, or too many failed audits)
--   retiring  surplus copy (or deleted file) the peer should delete
--   deleted   the peer confirmed it deleted the copy

CREATE TABLE IF NOT EXISTS shards (
    shard_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    file_id UUID NOT NULL REFERENCES files(file_id) ON DELETE CASCADE,
    peer_id UUID NOT NULL REFERENCES peers(peer_id) ON DELETE CASCADE,

    shard_index INT NOT NULL CHECK (shard_index >= 0),
    shard_type VARCHAR(10) NOT NULL, -- 'data', 'parity'
    shard_size_bytes BIGINT NOT NULL,
    shard_hash VARCHAR(64), -- SHA-256 hex, set when the peer confirms
    is_replica BOOLEAN NOT NULL DEFAULT false,

    is_anchor_stored BOOLEAN NOT NULL DEFAULT false,
    storage_path VARCHAR(500),

    health_status VARCHAR(20) NOT NULL DEFAULT 'pending',
    last_verified_at TIMESTAMP,
    verification_failures INT NOT NULL DEFAULT 0,

    stored_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    removed_at TIMESTAMP, -- set when the copy stops counting (missing / retiring)

    UNIQUE (file_id, peer_id, shard_index),

    INDEX idx_peer_id (peer_id),
    INDEX idx_health_status (health_status),
    INDEX idx_last_verified (last_verified_at),
    INDEX idx_composite_lookup (file_id, health_status)
);

-- ============================================================================
-- AUDITS
-- ============================================================================

-- Precomputed proof-of-storage challenges, registered by the uploader:
-- expected_proof = SHA-256(challenge || shard_bytes). Each is used once.
CREATE TABLE IF NOT EXISTS audit_challenges (
    challenge_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    shard_id UUID NOT NULL REFERENCES shards(shard_id) ON DELETE CASCADE,
    challenge BYTEA NOT NULL,
    expected_proof BYTEA NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    used_at TIMESTAMP,

    INDEX idx_unused (shard_id) WHERE used_at IS NULL
);

CREATE TABLE IF NOT EXISTS audit_logs (
    audit_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    shard_id UUID NOT NULL REFERENCES shards(shard_id) ON DELETE CASCADE,
    peer_id UUID NOT NULL REFERENCES peers(peer_id) ON DELETE CASCADE,
    challenge_id UUID REFERENCES audit_challenges(challenge_id) ON DELETE SET NULL,

    challenge BYTEA NOT NULL,
    response BYTEA,
    status VARCHAR(20) NOT NULL, -- 'pending', 'passed', 'failed', 'timeout'

    scheduled_at TIMESTAMP NOT NULL,
    challenged_at TIMESTAMP,
    responded_at TIMESTAMP,
    verified_at TIMESTAMP,
    expires_at TIMESTAMP,

    failure_reason TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),

    INDEX idx_shard_id (shard_id),
    INDEX idx_peer_id (peer_id),
    INDEX idx_status (status),
    INDEX idx_scheduled_at (scheduled_at)
);

-- ============================================================================
-- REPAIR QUEUE: one open job per file
-- ============================================================================

CREATE TABLE IF NOT EXISTS repair_queue (
    job_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    file_id UUID NOT NULL REFERENCES files(file_id) ON DELETE CASCADE,

    priority INT NOT NULL DEFAULT 5, -- 0 (critical) to 10 (low)
    missing_shards INT NOT NULL,
    target_shard_count INT NOT NULL DEFAULT 14,

    -- 'queued', 'processing', 'awaiting_data', 'completed', 'failed'
    status VARCHAR(20) NOT NULL DEFAULT 'queued',
    attempts INT NOT NULL DEFAULT 0,
    max_attempts INT NOT NULL DEFAULT 3,
    assigned_worker VARCHAR(100),
    reason VARCHAR(50),

    queued_at TIMESTAMP NOT NULL DEFAULT NOW(),
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),

    last_error TEXT,

    INDEX idx_status (status),
    INDEX idx_priority (priority),
    INDEX idx_queued_at (queued_at),
    INDEX idx_composite_queue (status, priority, queued_at)
);

-- ============================================================================
-- REPLICATION JOBS: copy one shard from a source peer to a target peer
-- ============================================================================

CREATE TABLE IF NOT EXISTS replication_jobs (
    job_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    file_id UUID NOT NULL REFERENCES files(file_id) ON DELETE CASCADE,
    source_peer_id UUID NOT NULL REFERENCES peers(peer_id) ON DELETE CASCADE,
    target_peer_id UUID NOT NULL REFERENCES peers(peer_id) ON DELETE CASCADE,
    target_shard_id UUID REFERENCES shards(shard_id) ON DELETE CASCADE,

    shard_index INT NOT NULL,
    priority VARCHAR(20) NOT NULL DEFAULT 'normal', -- 'critical', 'high', 'normal', 'low'

    -- 'queued' (waiting for peers to copy), 'completed', 'failed', 'cancelled'
    status VARCHAR(20) NOT NULL DEFAULT 'queued',

    requires_charging BOOLEAN NOT NULL DEFAULT true,
    requires_wifi BOOLEAN NOT NULL DEFAULT true,
    scheduled_window_start INT, -- hour of day (0-23)
    scheduled_window_end INT,

    queued_at TIMESTAMP NOT NULL DEFAULT NOW(),
    started_at TIMESTAMP,
    completed_at TIMESTAMP,

    last_error TEXT,

    INDEX idx_status (status),
    INDEX idx_priority (priority),
    INDEX idx_source_peer (source_peer_id),
    INDEX idx_target_peer (target_peer_id)
);

-- ============================================================================
-- KEY METADATA (zero knowledge: the server only stores wrapped keys)
-- ============================================================================

-- One row per user: the master key, wrapped with the device root key.
CREATE TABLE IF NOT EXISTS key_metadata (
    user_id UUID PRIMARY KEY REFERENCES users(user_id) ON DELETE CASCADE,

    wrapped_master_key BYTEA NOT NULL,
    master_key_algorithm VARCHAR(50) NOT NULL DEFAULT 'AES-256-GCM',

    -- Who holds each Shamir share. Never the shares themselves:
    -- {"holders": [{"holder_id": "...", "share_index": 1}, ...]}
    recovery_key_shares_metadata JSONB,
    recovery_threshold INT NOT NULL DEFAULT 3,
    recovery_total_shares INT NOT NULL DEFAULT 5,

    key_version INT NOT NULL DEFAULT 1,
    last_rotated_at TIMESTAMP,
    next_rotation_at TIMESTAMP,
    rotation_schedule_days INT NOT NULL DEFAULT 90,

    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Per-file keys, wrapped with the owner's master key.
CREATE TABLE IF NOT EXISTS file_keys (
    key_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    file_id UUID NOT NULL REFERENCES files(file_id) ON DELETE CASCADE,
    user_id UUID NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,

    wrapped_key BYTEA NOT NULL,
    derivation_salt BYTEA,
    algorithm VARCHAR(50) NOT NULL DEFAULT 'AES-256-GCM',
    key_version INT NOT NULL DEFAULT 1,

    is_active BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    rotate_after TIMESTAMP NOT NULL, -- rotation is due after this; the key keeps working
    rotated_at TIMESTAMP,
    last_accessed_at TIMESTAMP,

    INDEX idx_file_user (file_id, user_id),
    INDEX idx_rotate_after (rotate_after) WHERE is_active = true
);
-- At most one active key per file and user.
CREATE UNIQUE INDEX IF NOT EXISTS file_keys_one_active
    ON file_keys (file_id, user_id) WHERE is_active = true;

-- ============================================================================
-- PAYMENTS & BILLING
-- ============================================================================

CREATE TABLE IF NOT EXISTS earnings (
    earning_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    peer_id UUID NOT NULL REFERENCES peers(peer_id) ON DELETE CASCADE,
    user_id UUID REFERENCES users(user_id) ON DELETE CASCADE, -- owner at billing time

    period_start DATE NOT NULL,
    period_end DATE NOT NULL, -- exclusive

    storage_gb_days DECIMAL(12,3) NOT NULL,
    base_rate_usd DECIMAL(10,6) NOT NULL, -- USD per GB-month
    bandwidth_gb DECIMAL(12,3) NOT NULL DEFAULT 0,
    bandwidth_earnings_usd DECIMAL(10,4) NOT NULL DEFAULT 0,
    bytes_served_snapshot BIGINT NOT NULL DEFAULT 0, -- peers.bytes_served when billed
    gross_earnings_usd DECIMAL(10,4) NOT NULL,

    wake_up_bonus_usd DECIMAL(10,4) NOT NULL DEFAULT 0.0000,
    reliability_bonus_usd DECIMAL(10,4) NOT NULL DEFAULT 0.0000,
    audit_penalty_usd DECIMAL(10,4) NOT NULL DEFAULT 0.0000,

    net_earnings_usd DECIMAL(10,4) NOT NULL,

    status VARCHAR(20) NOT NULL DEFAULT 'pending', -- 'pending', 'approved', 'paid'

    calculated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    approved_at TIMESTAMP,

    UNIQUE (peer_id, period_start),
    INDEX idx_user_id (user_id),
    INDEX idx_period (period_start, period_end),
    INDEX idx_status (status)
);

CREATE TABLE IF NOT EXISTS payouts (
    payout_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,

    amount_usd DECIMAL(10,4) NOT NULL,

    period_start DATE NOT NULL,
    period_end DATE NOT NULL,

    payment_method VARCHAR(50) NOT NULL,
    payment_email VARCHAR(255),
    payment_address TEXT,

    status VARCHAR(20) NOT NULL DEFAULT 'pending', -- 'pending', 'processing', 'completed', 'failed'
    external_transaction_id VARCHAR(100),

    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    processed_at TIMESTAMP,
    completed_at TIMESTAMP,

    failure_reason TEXT,

    INDEX idx_user_id (user_id),
    INDEX idx_status (status),
    INDEX idx_created_at (created_at)
);

CREATE TABLE IF NOT EXISTS enterprise_usage (
    usage_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    customer_id UUID NOT NULL,

    period_start DATE NOT NULL,
    period_end DATE NOT NULL,

    cold_tier_gb_days DECIMAL(12,3) NOT NULL DEFAULT 0.000,
    warm_tier_gb_days DECIMAL(12,3) NOT NULL DEFAULT 0.000,
    hot_tier_gb_days DECIMAL(12,3) NOT NULL DEFAULT 0.000,

    api_requests_count BIGINT NOT NULL DEFAULT 0,
    bandwidth_gb DECIMAL(12,3) NOT NULL DEFAULT 0.000,

    cold_tier_charge_usd DECIMAL(12,4) NOT NULL DEFAULT 0.0000,
    warm_tier_charge_usd DECIMAL(12,4) NOT NULL DEFAULT 0.0000,
    hot_tier_charge_usd DECIMAL(12,4) NOT NULL DEFAULT 0.0000,
    api_charge_usd DECIMAL(12,4) NOT NULL DEFAULT 0.0000,
    bandwidth_charge_usd DECIMAL(12,4) NOT NULL DEFAULT 0.0000,

    total_charge_usd DECIMAL(12,4) NOT NULL,

    invoice_id UUID,
    invoice_status VARCHAR(20),

    calculated_at TIMESTAMP NOT NULL DEFAULT NOW(),

    INDEX idx_customer_id (customer_id),
    INDEX idx_period (period_start, period_end),
    INDEX idx_invoice_status (invoice_status)
);

-- ============================================================================
-- ACCESS LOGS & ANALYTICS
-- ============================================================================

CREATE TABLE IF NOT EXISTS file_access_logs (
    access_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    file_id UUID NOT NULL REFERENCES files(file_id) ON DELETE CASCADE,
    user_id UUID NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,

    access_type VARCHAR(20) NOT NULL, -- 'retrieve', 'delete', 'share'
    retrieval_time_ms INT,

    client_ip VARCHAR(45),
    client_region VARCHAR(50),

    accessed_at TIMESTAMP NOT NULL DEFAULT NOW(),

    INDEX idx_file_accessed (file_id, accessed_at),
    INDEX idx_user_id (user_id),
    INDEX idx_accessed_at (accessed_at)
);

CREATE TABLE IF NOT EXISTS analytics_events (
    event_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_type VARCHAR(50) NOT NULL,
    event_data JSONB NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),

    INDEX idx_type_created (event_type, created_at),
    INDEX idx_created_at (created_at)
);

-- Every change made through the admin console.
CREATE TABLE IF NOT EXISTS admin_audit_log (
    entry_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    admin_id UUID REFERENCES users(user_id) ON DELETE SET NULL,
    admin_email VARCHAR(255) NOT NULL,
    action VARCHAR(50) NOT NULL,       -- e.g. 'user.suspend', 'config.update'
    target_type VARCHAR(30),           -- 'user', 'peer', 'file', 'config', ...
    target_id VARCHAR(100),
    details JSONB,
    client_ip VARCHAR(45),
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),

    INDEX idx_created_at (created_at),
    INDEX idx_target (target_type, target_id)
);

-- ============================================================================
-- SYSTEM CONFIGURATION
-- ============================================================================

CREATE TABLE IF NOT EXISTS system_config (
    config_key VARCHAR(100) PRIMARY KEY,
    config_value TEXT NOT NULL,
    config_type VARCHAR(20) NOT NULL, -- 'string', 'int', 'float', 'json', 'bool'
    description TEXT,
    version INT NOT NULL DEFAULT 1,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_by VARCHAR(100)
);

INSERT INTO system_config (config_key, config_value, config_type, description) VALUES
    ('consumer_earnings_rate_cold', '0.0063', 'float', 'USD per GB per month for cold tier storage providers'),
    ('consumer_earnings_rate_warm', '0.0070', 'float', 'USD per GB per month for warm tier storage providers'),
    ('consumer_earnings_rate_hot', '0.0088', 'float', 'USD per GB per month for hot tier storage providers'),
    ('consumer_bandwidth_rate_gb', '0.0100', 'float', 'USD per GB served to clients'),
    ('enterprise_rate_cold', '0.018', 'float', 'USD per GB per month for cold tier enterprise storage'),
    ('enterprise_rate_warm', '0.025', 'float', 'USD per GB per month for warm tier enterprise storage'),
    ('enterprise_rate_hot', '0.035', 'float', 'USD per GB per month for hot tier enterprise storage'),
    ('payout_minimum_usd', '5.00', 'float', 'Minimum balance required for payout'),
    ('audit_failure_penalty_usd', '0.01', 'float', 'Deducted per failed audit in the billing period'),
    ('peer_inactive_threshold_hours', '168', 'int', '7 days - mark peer as inactive'),
    ('peer_churn_threshold_hours', '720', 'int', '30 days - trigger data migration'),
    ('repair_priority_threshold', '10', 'int', 'Available shards below this triggers high-priority repair'),
    ('audit_frequency_hours', '168', 'int', '7 days - how often to audit each shard'),
    ('max_replication_count', '28', 'int', 'Maximum shard placements for popular files'),
    ('min_replication_count', '14', 'int', 'Minimum shard placements for any file'),
    ('wake_up_bonus_usd', '0.05', 'float', 'Bonus paid for quick wake-up response'),
    ('platform_revenue_share', '0.65', 'float', 'Platform takes 65% of enterprise revenue'),
    ('consumer_revenue_share', '0.35', 'float', 'Consumers get 35% of enterprise revenue')
ON CONFLICT (config_key) DO NOTHING;

-- ============================================================================
-- UPGRADES from v2.0 / migration 001 (no-ops on a fresh database)
-- ============================================================================

ALTER TABLE users ADD COLUMN IF NOT EXISTS role VARCHAR(20) NOT NULL DEFAULT 'user';

ALTER TABLE peers ALTER COLUMN user_id DROP NOT NULL;
ALTER TABLE peers ADD COLUMN IF NOT EXISTS libp2p_peer_id VARCHAR(100);
ALTER TABLE peers ADD COLUMN IF NOT EXISTS multiaddr TEXT;
ALTER TABLE peers ADD COLUMN IF NOT EXISTS multiaddrs TEXT[];
ALTER TABLE peers ADD COLUMN IF NOT EXISTS churn_prediction DECIMAL(4,3);
ALTER TABLE peers ADD COLUMN IF NOT EXISTS churn_prediction_updated_at TIMESTAMP;
ALTER TABLE peers ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP NOT NULL DEFAULT NOW();
-- Migration 001 wrote placeholder addresses that cannot be dialed.
UPDATE peers SET multiaddr = NULL WHERE multiaddr LIKE '%/p2p/migration-pending';
DROP INDEX IF EXISTS peers@idx_peers_libp2p_id;
CREATE UNIQUE INDEX IF NOT EXISTS peers_libp2p_peer_id_key ON peers (libp2p_peer_id);

ALTER TABLE files DROP CONSTRAINT IF EXISTS check_shard_count;
ALTER TABLE files DROP CONSTRAINT IF EXISTS check_parity_shards;
ALTER TABLE files DROP CONSTRAINT IF EXISTS files_shard_count_range;
ALTER TABLE files DROP CONSTRAINT IF EXISTS files_parity_range;
ALTER TABLE files ADD CONSTRAINT files_shard_count_range CHECK (shard_count >= 2 AND shard_count <= 100);
ALTER TABLE files ADD CONSTRAINT files_parity_range CHECK (parity_shards >= 1 AND parity_shards <= 10);

ALTER TABLE shards ALTER COLUMN shard_hash DROP NOT NULL;
ALTER TABLE shards ADD COLUMN IF NOT EXISTS is_replica BOOLEAN NOT NULL DEFAULT false;
ALTER TABLE shards ADD COLUMN IF NOT EXISTS removed_at TIMESTAMP;

ALTER TABLE audit_logs ADD COLUMN IF NOT EXISTS challenge_id UUID REFERENCES audit_challenges(challenge_id) ON DELETE SET NULL;
ALTER TABLE audit_logs ADD COLUMN IF NOT EXISTS expires_at TIMESTAMP;

ALTER TABLE repair_queue ADD COLUMN IF NOT EXISTS reason VARCHAR(50);
ALTER TABLE repair_queue ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP NOT NULL DEFAULT NOW();

ALTER TABLE replication_jobs ADD COLUMN IF NOT EXISTS target_shard_id UUID REFERENCES shards(shard_id) ON DELETE CASCADE;

ALTER TABLE earnings ALTER COLUMN user_id DROP NOT NULL;
ALTER TABLE earnings ADD COLUMN IF NOT EXISTS bandwidth_gb DECIMAL(12,3) NOT NULL DEFAULT 0;
ALTER TABLE earnings ADD COLUMN IF NOT EXISTS bandwidth_earnings_usd DECIMAL(10,4) NOT NULL DEFAULT 0;
ALTER TABLE earnings ADD COLUMN IF NOT EXISTS bytes_served_snapshot BIGINT NOT NULL DEFAULT 0;
CREATE UNIQUE INDEX IF NOT EXISTS earnings_peer_period_key ON earnings (peer_id, period_start);

-- One open repair job per file (created after the upgrades so old databases get it too).
CREATE UNIQUE INDEX IF NOT EXISTS repair_queue_one_open_per_file
    ON repair_queue (file_id) WHERE status IN ('queued', 'processing', 'awaiting_data');

CREATE INDEX IF NOT EXISTS idx_peers_multiaddrs ON peers USING GIN (multiaddrs);
CREATE INDEX IF NOT EXISTS idx_shards_file_health ON shards (file_id, health_status) WHERE health_status != 'good';
CREATE INDEX IF NOT EXISTS idx_peers_online_score ON peers (is_online, health_score DESC) WHERE is_online = true;
CREATE INDEX IF NOT EXISTS idx_files_tier_status ON files (storage_tier, status) WHERE status = 'active';

-- ============================================================================
-- VIEWS
-- ============================================================================

DROP VIEW IF EXISTS peer_connectivity;
CREATE VIEW peer_connectivity AS
SELECT
    peer_id,
    libp2p_peer_id,
    multiaddr,
    multiaddrs,
    CASE WHEN libp2p_peer_id IS NOT NULL THEN 'libp2p' ELSE 'unregistered' END AS connection_type,
    last_heartbeat_at,
    is_online
FROM peers;

DROP VIEW IF EXISTS active_healthy_peers;
CREATE VIEW active_healthy_peers AS
SELECT peer_id, user_id, asn, timezone, region, health_score, storage_free_gb,
       is_charging, battery_level, network_type
FROM peers
WHERE is_online = true
  AND health_score >= 70
  AND storage_free_gb > 0.1;

-- Files that have lost shards but can still be rebuilt, worst first.
DROP VIEW IF EXISTS degraded_files;
CREATE VIEW degraded_files AS
SELECT
    f.file_id,
    f.owner_id,
    f.storage_tier,
    f.shard_count,
    f.parity_shards,
    (f.shard_count + f.parity_shards) AS total_shards,
    f.available_shards,
    (f.shard_count + f.parity_shards - f.available_shards) AS missing_shards,
    f.last_accessed_at,
    f.created_at
FROM files f
WHERE f.status = 'degraded'
ORDER BY f.available_shards ASC, f.last_accessed_at DESC;

-- LEFT JOIN so peers that are not linked to a user still appear.
DROP VIEW IF EXISTS peer_earnings_summary;
CREATE VIEW peer_earnings_summary AS
SELECT
    p.peer_id,
    p.user_id,
    u.email,
    COALESCE(SUM(e.net_earnings_usd), 0) AS total_earnings_usd,
    COUNT(e.earning_id) AS earning_periods,
    MAX(e.period_end) AS last_earning_period
FROM peers p
LEFT JOIN users u ON p.user_id = u.user_id
LEFT JOIN earnings e ON p.peer_id = e.peer_id AND e.status IN ('pending', 'approved', 'paid')
GROUP BY p.peer_id, p.user_id, u.email;

CREATE MATERIALIZED VIEW IF NOT EXISTS daily_stats AS
SELECT
    created_at::DATE AS date,
    COUNT(*) AS files_created,
    SUM(file_size_bytes) AS total_bytes_stored,
    AVG(file_size_bytes) AS avg_file_size
FROM files
GROUP BY created_at::DATE;
-- Refresh daily: REFRESH MATERIALIZED VIEW daily_stats;
