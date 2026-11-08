-- CockroachDB Schema for Distributed Storage Orchestrator
-- Version: 2.0 (Updated with Adaptive Sharding & Compression)
-- Date: 2026-03-01
--
-- CHANGELOG:
-- v2.0 (2026-03-01):
--   - Added adaptive sharding support (shard_count: 10-100 based on file size)
--   - Added compression support (is_compressed, compression_algorithm, size tracking)
--   - Removed fixed erasure_coding and replication_count columns
--   - Added indexes for shard_count and is_compressed
--   - Updated degraded_files view to use new columns
-- v1.0 (2026-01-18):
--   - Initial schema with 13 tables, 4 views
--   - Fixed replication at 10 data + 4 parity shards

-- Enable UUID extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ============================================================================
-- USERS & AUTHENTICATION
-- ============================================================================

CREATE TABLE users (
    user_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    email VARCHAR(255) UNIQUE NOT NULL,
    password_hash VARCHAR(255) NOT NULL, -- bcrypt
    tier VARCHAR(20) NOT NULL DEFAULT 'free', -- 'free', 'paid'
    status VARCHAR(20) NOT NULL DEFAULT 'active', -- 'active', 'suspended', 'deleted'
    
    -- Storage quotas
    storage_contributed_gb INT NOT NULL DEFAULT 0,
    storage_used_gb INT NOT NULL DEFAULT 0,
    storage_quota_gb INT NOT NULL DEFAULT 25, -- Free tier default
    
    -- Earnings
    current_balance_usd DECIMAL(10,4) NOT NULL DEFAULT 0.0000,
    lifetime_earnings_usd DECIMAL(12,4) NOT NULL DEFAULT 0.0000,
    
    -- Payment preferences
    payment_method VARCHAR(50), -- 'paypal', 'bank', 'crypto', 'gift_card', 'platform_credit'
    payment_email VARCHAR(255),
    payment_address TEXT, -- For crypto
    payout_threshold_usd DECIMAL(10,2) NOT NULL DEFAULT 5.00,
    
    -- Timestamps
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    last_login_at TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    
    -- Indexes
    INDEX idx_email (email),
    INDEX idx_tier (tier),
    INDEX idx_status (status),
    INDEX idx_created_at (created_at)
);

-- ============================================================================
-- PEERS (Storage Nodes)
-- ============================================================================

CREATE TABLE peers (
    peer_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    
    -- Network information
    asn VARCHAR(20) NOT NULL, -- AS number (e.g., "AS20057")
    timezone VARCHAR(50) NOT NULL, -- IANA timezone (e.g., "America/New_York")
    region VARCHAR(50) NOT NULL, -- Geo region (e.g., "us-east")
    country_code VARCHAR(2), -- ISO 3166-1 alpha-2
    isp VARCHAR(100), -- Internet service provider
    
    -- Device information
    device_model VARCHAR(100), -- e.g., "iPhone15,2"
    os_type VARCHAR(20), -- 'ios', 'android', 'desktop'
    os_version VARCHAR(50),
    app_version VARCHAR(20),
    
    -- Capacity
    storage_total_gb INT NOT NULL,
    storage_free_gb DECIMAL(10,3) NOT NULL,
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
    
    -- Indexes
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

CREATE TABLE files (
    file_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    owner_id UUID NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    
    -- File metadata (encrypted)
    file_name_encrypted BYTEA NOT NULL, -- AES-GCM encrypted filename
    file_size_bytes BIGINT NOT NULL, -- Size after encryption (what's actually stored)
    mime_type VARCHAR(100),
    
    -- Storage configuration
    storage_tier VARCHAR(20) NOT NULL DEFAULT 'cold', -- 'cold', 'warm', 'hot'
    
    -- Adaptive sharding (replaces erasure_coding and replication_count)
    shard_count INT NOT NULL DEFAULT 10 CHECK (shard_count >= 10 AND shard_count <= 100), -- Adaptive: 10-100 based on file size
    parity_shards INT NOT NULL DEFAULT 4 CHECK (parity_shards >= 2 AND parity_shards <= 10), -- Erasure coding parity
    
    -- Compression support
    is_compressed BOOLEAN NOT NULL DEFAULT false, -- Was file compressed before encryption?
    compression_algorithm VARCHAR(20) CHECK (compression_algorithm IN ('gzip', 'zstd', 'lz4', NULL)), -- Algorithm used
    original_size_bytes BIGINT, -- Size before compression (if compressed)
    compressed_size_bytes BIGINT, -- Size after compression, before encryption
    
    -- Access tracking
    access_count INT NOT NULL DEFAULT 0,
    last_accessed_at TIMESTAMP,
    popularity_score DECIMAL(10,4) NOT NULL DEFAULT 0.0000,
    
    -- Status
    status VARCHAR(20) NOT NULL DEFAULT 'active', -- 'active', 'degraded', 'deleting', 'deleted'
    available_shards INT NOT NULL DEFAULT 0, -- How many shards currently available
    
    -- Timestamps
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    deleted_at TIMESTAMP,
    
    -- Indexes
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
-- SHARDS (File Pieces)
-- ============================================================================

CREATE TABLE shards (
    shard_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    file_id UUID NOT NULL REFERENCES files(file_id) ON DELETE CASCADE,
    peer_id UUID NOT NULL REFERENCES peers(peer_id) ON DELETE CASCADE,
    
    -- Shard information
    shard_index INT NOT NULL CHECK (shard_index >= 0), -- 0-13 for 14-shard system
    shard_type VARCHAR(10) NOT NULL, -- 'data', 'parity'
    shard_size_bytes BIGINT NOT NULL,
    shard_hash VARCHAR(64) NOT NULL, -- SHA-256 hash for verification
    
    -- Storage location
    is_anchor_stored BOOLEAN NOT NULL DEFAULT false, -- True if on anchor node
    storage_path VARCHAR(500), -- Path on peer or anchor node
    
    -- Health
    health_status VARCHAR(20) NOT NULL DEFAULT 'good', -- 'good', 'degraded', 'missing'
    last_verified_at TIMESTAMP,
    verification_failures INT NOT NULL DEFAULT 0,
    
    -- Timestamps
    stored_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    
    -- Constraints
    UNIQUE (file_id, peer_id, shard_index),
    
    -- Indexes
    INDEX idx_file_id (file_id),
    INDEX idx_peer_id (peer_id),
    INDEX idx_health_status (health_status),
    INDEX idx_last_verified (last_verified_at),
    INDEX idx_composite_lookup (file_id, health_status)
);

-- ============================================================================
-- AUDIT LOGS
-- ============================================================================

CREATE TABLE audit_logs (
    audit_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    shard_id UUID NOT NULL REFERENCES shards(shard_id) ON DELETE CASCADE,
    peer_id UUID NOT NULL REFERENCES peers(peer_id) ON DELETE CASCADE,
    
    -- Audit details
    challenge BYTEA NOT NULL, -- Random challenge sent to peer
    response BYTEA, -- Peer's proof response
    status VARCHAR(20) NOT NULL, -- 'pending', 'passed', 'failed', 'timeout'
    
    -- Timing
    scheduled_at TIMESTAMP NOT NULL,
    challenged_at TIMESTAMP,
    responded_at TIMESTAMP,
    verified_at TIMESTAMP,
    
    -- Failure details
    failure_reason TEXT,
    
    -- Timestamps
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    
    -- Indexes
    INDEX idx_shard_id (shard_id),
    INDEX idx_peer_id (peer_id),
    INDEX idx_status (status),
    INDEX idx_scheduled_at (scheduled_at)
);

-- ============================================================================
-- REPAIR QUEUE
-- ============================================================================

CREATE TABLE repair_queue (
    job_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    file_id UUID NOT NULL REFERENCES files(file_id) ON DELETE CASCADE,
    
    -- Repair details
    priority INT NOT NULL DEFAULT 5, -- 0 (critical) to 10 (low)
    missing_shards INT NOT NULL,
    target_shard_count INT NOT NULL DEFAULT 14,
    
    -- Status
    status VARCHAR(20) NOT NULL DEFAULT 'queued', -- 'queued', 'processing', 'completed', 'failed'
    attempts INT NOT NULL DEFAULT 0,
    max_attempts INT NOT NULL DEFAULT 3,
    
    -- Worker assignment
    assigned_worker VARCHAR(100),
    
    -- Timestamps
    queued_at TIMESTAMP NOT NULL DEFAULT NOW(),
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    
    -- Error tracking
    last_error TEXT,
    
    -- Indexes
    INDEX idx_status (status),
    INDEX idx_priority (priority),
    INDEX idx_queued_at (queued_at),
    INDEX idx_composite_queue (status, priority, queued_at)
);

-- ============================================================================
-- REPLICATION JOBS
-- ============================================================================

CREATE TABLE replication_jobs (
    job_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    file_id UUID NOT NULL REFERENCES files(file_id) ON DELETE CASCADE,
    source_peer_id UUID NOT NULL REFERENCES peers(peer_id) ON DELETE CASCADE,
    target_peer_id UUID NOT NULL REFERENCES peers(peer_id) ON DELETE CASCADE,
    
    -- Job details
    shard_index INT NOT NULL,
    priority VARCHAR(20) NOT NULL DEFAULT 'normal', -- 'critical', 'high', 'normal', 'low'
    
    -- Status
    status VARCHAR(20) NOT NULL DEFAULT 'queued', -- 'queued', 'processing', 'completed', 'failed'
    
    -- Constraints (battery-aware)
    requires_charging BOOLEAN NOT NULL DEFAULT true,
    requires_wifi BOOLEAN NOT NULL DEFAULT true,
    scheduled_window_start INT, -- Hour of day (0-23)
    scheduled_window_end INT,
    
    -- Timestamps
    queued_at TIMESTAMP NOT NULL DEFAULT NOW(),
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    
    -- Error tracking
    last_error TEXT,
    
    -- Indexes
    INDEX idx_status (status),
    INDEX idx_priority (priority),
    INDEX idx_source_peer (source_peer_id),
    INDEX idx_target_peer (target_peer_id)
);

-- ============================================================================
-- KEY METADATA (Zero-Knowledge)
-- ============================================================================

CREATE TABLE key_metadata (
    user_id UUID PRIMARY KEY REFERENCES users(user_id) ON DELETE CASCADE,
    
    -- Wrapped keys (encrypted with device root key)
    wrapped_master_key BYTEA NOT NULL, -- Master key encrypted with DRK
    master_key_algorithm VARCHAR(50) NOT NULL DEFAULT 'AES-256-GCM',
    
    -- Recovery metadata (NOT the actual shares)
    recovery_key_shares_metadata JSONB, -- Locations of Shamir shares
    recovery_threshold INT NOT NULL DEFAULT 3,
    recovery_total_shares INT NOT NULL DEFAULT 5,
    
    -- Key rotation
    key_version INT NOT NULL DEFAULT 1,
    last_rotated_at TIMESTAMP,
    next_rotation_at TIMESTAMP,
    rotation_schedule_days INT NOT NULL DEFAULT 90,
    
    -- Timestamps
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- ============================================================================
-- PAYMENTS & BILLING
-- ============================================================================

CREATE TABLE earnings (
    earning_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    peer_id UUID NOT NULL REFERENCES peers(peer_id) ON DELETE CASCADE,
    user_id UUID NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    
    -- Calculation period
    period_start DATE NOT NULL,
    period_end DATE NOT NULL,
    
    -- Earnings breakdown
    storage_gb_days DECIMAL(12,3) NOT NULL, -- GB × days
    base_rate_usd DECIMAL(10,6) NOT NULL, -- Rate per GB/month
    gross_earnings_usd DECIMAL(10,4) NOT NULL,
    
    -- Bonuses
    wake_up_bonus_usd DECIMAL(10,4) NOT NULL DEFAULT 0.0000,
    reliability_bonus_usd DECIMAL(10,4) NOT NULL DEFAULT 0.0000,
    
    -- Penalties
    audit_penalty_usd DECIMAL(10,4) NOT NULL DEFAULT 0.0000,
    
    -- Net
    net_earnings_usd DECIMAL(10,4) NOT NULL,
    
    -- Status
    status VARCHAR(20) NOT NULL DEFAULT 'pending', -- 'pending', 'approved', 'paid'
    
    -- Timestamps
    calculated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    approved_at TIMESTAMP,
    
    -- Indexes
    INDEX idx_peer_id (peer_id),
    INDEX idx_user_id (user_id),
    INDEX idx_period (period_start, period_end),
    INDEX idx_status (status)
);

CREATE TABLE payouts (
    payout_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    
    -- Amount
    amount_usd DECIMAL(10,4) NOT NULL,
    
    -- Period covered
    period_start DATE NOT NULL,
    period_end DATE NOT NULL,
    
    -- Payment method
    payment_method VARCHAR(50) NOT NULL, -- 'paypal', 'bank', 'crypto', 'gift_card'
    payment_email VARCHAR(255),
    payment_address TEXT,
    
    -- Status
    status VARCHAR(20) NOT NULL DEFAULT 'pending', -- 'pending', 'processing', 'completed', 'failed'
    
    -- External reference
    external_transaction_id VARCHAR(100), -- Stripe/PayPal transaction ID
    
    -- Timestamps
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    processed_at TIMESTAMP,
    completed_at TIMESTAMP,
    
    -- Error tracking
    failure_reason TEXT,
    
    -- Indexes
    INDEX idx_user_id (user_id),
    INDEX idx_status (status),
    INDEX idx_created_at (created_at)
);

CREATE TABLE enterprise_usage (
    usage_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    customer_id UUID NOT NULL, -- Enterprise customer ID (from separate auth system)
    
    -- Usage period
    period_start DATE NOT NULL,
    period_end DATE NOT NULL,
    
    -- Storage usage by tier
    cold_tier_gb_days DECIMAL(12,3) NOT NULL DEFAULT 0.000,
    warm_tier_gb_days DECIMAL(12,3) NOT NULL DEFAULT 0.000,
    hot_tier_gb_days DECIMAL(12,3) NOT NULL DEFAULT 0.000,
    
    -- API usage
    api_requests_count BIGINT NOT NULL DEFAULT 0,
    bandwidth_gb DECIMAL(12,3) NOT NULL DEFAULT 0.000,
    
    -- Charges
    cold_tier_charge_usd DECIMAL(12,4) NOT NULL DEFAULT 0.0000,
    warm_tier_charge_usd DECIMAL(12,4) NOT NULL DEFAULT 0.0000,
    hot_tier_charge_usd DECIMAL(12,4) NOT NULL DEFAULT 0.0000,
    api_charge_usd DECIMAL(12,4) NOT NULL DEFAULT 0.0000,
    bandwidth_charge_usd DECIMAL(12,4) NOT NULL DEFAULT 0.0000,
    
    -- Total
    total_charge_usd DECIMAL(12,4) NOT NULL,
    
    -- Invoice
    invoice_id UUID,
    invoice_status VARCHAR(20), -- 'draft', 'sent', 'paid', 'overdue'
    
    -- Timestamps
    calculated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    
    -- Indexes
    INDEX idx_customer_id (customer_id),
    INDEX idx_period (period_start, period_end),
    INDEX idx_invoice_status (invoice_status)
);

-- ============================================================================
-- ACCESS LOGS (For analytics and ML)
-- ============================================================================

CREATE TABLE file_access_logs (
    access_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    file_id UUID NOT NULL REFERENCES files(file_id) ON DELETE CASCADE,
    user_id UUID NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    
    -- Access details
    access_type VARCHAR(20) NOT NULL, -- 'retrieve', 'delete', 'share'
    retrieval_time_ms INT, -- How long retrieval took
    
    -- Client information
    client_ip VARCHAR(45), -- IPv4 or IPv6
    client_region VARCHAR(50),
    
    -- Timestamp
    accessed_at TIMESTAMP NOT NULL DEFAULT NOW(),
    
    -- Indexes
    INDEX idx_file_id (file_id),
    INDEX idx_user_id (user_id),
    INDEX idx_accessed_at (accessed_at)
);

-- Partitioning for high-volume table (optional, for production)
-- ALTER TABLE file_access_logs PARTITION BY RANGE (accessed_at) (
--     PARTITION p2026_01 VALUES FROM ('2026-01-01') TO ('2026-02-01'),
--     PARTITION p2026_02 VALUES FROM ('2026-02-01') TO ('2026-03-01'),
--     ...
-- );

-- ============================================================================
-- SYSTEM CONFIGURATION
-- ============================================================================

CREATE TABLE system_config (
    config_key VARCHAR(100) PRIMARY KEY,
    config_value TEXT NOT NULL,
    config_type VARCHAR(20) NOT NULL, -- 'string', 'int', 'float', 'json', 'bool'
    description TEXT,
    
    -- Version control
    version INT NOT NULL DEFAULT 1,
    
    -- Timestamps
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_by VARCHAR(100) -- Admin user who made the change
);

-- Insert default configuration
INSERT INTO system_config (config_key, config_value, config_type, description) VALUES
    ('consumer_earnings_rate_cold', '0.0063', 'float', 'USD per GB per month for cold tier storage providers'),
    ('consumer_earnings_rate_warm', '0.0070', 'float', 'USD per GB per month for warm tier storage providers'),
    ('consumer_earnings_rate_hot', '0.0088', 'float', 'USD per GB per month for hot tier storage providers'),
    ('enterprise_rate_cold', '0.018', 'float', 'USD per GB per month for cold tier enterprise storage'),
    ('enterprise_rate_warm', '0.025', 'float', 'USD per GB per month for warm tier enterprise storage'),
    ('enterprise_rate_hot', '0.035', 'float', 'USD per GB per month for hot tier enterprise storage'),
    ('payout_minimum_usd', '5.00', 'float', 'Minimum balance required for payout'),
    ('peer_inactive_threshold_hours', '168', 'int', '7 days - mark peer as inactive'),
    ('peer_churn_threshold_hours', '720', 'int', '30 days - trigger data migration'),
    ('repair_priority_threshold', '10', 'int', 'Available shards below this triggers high-priority repair'),
    ('audit_frequency_hours', '168', 'int', '7 days - how often to audit each shard'),
    ('max_replication_count', '28', 'int', 'Maximum replicas for popular files'),
    ('min_replication_count', '14', 'int', 'Minimum replicas for any file'),
    ('wake_up_bonus_usd', '0.05', 'float', 'Bonus paid for quick wake-up response'),
    ('platform_revenue_share', '0.65', 'float', 'Platform takes 65% of enterprise revenue'),
    ('consumer_revenue_share', '0.35', 'float', 'Consumers get 35% of enterprise revenue');

-- ============================================================================
-- VIEWS (For common queries)
-- ============================================================================

-- Active peers with healthy status
CREATE VIEW active_healthy_peers AS
SELECT 
    peer_id,
    user_id,
    asn,
    timezone,
    region,
    health_score,
    storage_free_gb,
    is_charging,
    battery_level,
    network_type
FROM peers
WHERE is_online = true
  AND health_score >= 70
  AND storage_free_gb > 0.1
ORDER BY health_score DESC;

-- Files needing repair
CREATE VIEW degraded_files AS
SELECT 
    f.file_id,
    f.owner_id,
    f.storage_tier,
    f.shard_count,
    f.parity_shards,
    (f.shard_count + f.parity_shards) as total_shards,
    f.available_shards,
    (f.shard_count + f.parity_shards - f.available_shards) AS missing_shards,
    f.last_accessed_at,
    f.created_at
FROM files f
WHERE f.status = 'degraded'
  AND f.available_shards < f.shard_count -- Below minimum needed for reconstruction
ORDER BY f.available_shards ASC, f.last_accessed_at DESC;

-- Peer earnings summary
CREATE VIEW peer_earnings_summary AS
SELECT 
    p.peer_id,
    p.user_id,
    u.email,
    SUM(e.net_earnings_usd) AS total_earnings_usd,
    COUNT(e.earning_id) AS earning_periods,
    MAX(e.period_end) AS last_earning_period
FROM peers p
JOIN users u ON p.user_id = u.user_id
LEFT JOIN earnings e ON p.peer_id = e.peer_id AND e.status = 'approved'
GROUP BY p.peer_id, p.user_id, u.email;

-- ============================================================================

-- ============================================================================
-- GRANTS (RBAC)
-- ============================================================================


-- ============================================================================
-- INDEXES FOR PERFORMANCE
-- ============================================================================

-- Composite indexes for common queries
CREATE INDEX idx_shards_file_health ON shards(file_id, health_status) WHERE health_status != 'good';
CREATE INDEX idx_peers_online_score ON peers(is_online, health_score DESC) WHERE is_online = true;
CREATE INDEX idx_files_tier_status ON files(storage_tier, status) WHERE status = 'active';

-- ============================================================================
-- MATERIALIZED VIEWS (For analytics)
-- ============================================================================

CREATE MATERIALIZED VIEW daily_stats AS
SELECT 
    DATE(created_at) AS date,
    COUNT(*) AS files_created,
    SUM(file_size_bytes) AS total_bytes_stored,
    AVG(file_size_bytes) AS avg_file_size
FROM files
GROUP BY DATE(created_at);

-- Refresh schedule (run daily via cron)
-- REFRESH MATERIALIZED VIEW daily_stats;

-- ============================================================================
-- BACKUP & MAINTENANCE
-- ============================================================================

-- Comment for backup script
-- pg_dump -h cockroachdb-public -U root -d orchestrator > backup.sql

-- Vacuum analyze (run weekly)
-- VACUUM ANALYZE;

-- ============================================================================
-- END OF SCHEMA
-- ============================================================================
