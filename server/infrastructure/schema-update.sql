-- Database Migration for libp2p Support
-- File: 001_add_libp2p_support.sql
-- Apply with: cockroach sql --url="postgresql://root@localhost:26257/orchestrator" < 001_add_libp2p_support.sql

-- Add libp2p fields to peers table
ALTER TABLE IF EXISTS peers ADD COLUMN IF NOT EXISTS libp2p_peer_id VARCHAR(100);
ALTER TABLE IF EXISTS peers ADD COLUMN IF NOT EXISTS multiaddr TEXT;
ALTER TABLE IF EXISTS peers ADD COLUMN IF NOT EXISTS multiaddrs TEXT[];

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_peers_libp2p_id ON peers(libp2p_peer_id);
CREATE INDEX IF NOT EXISTS idx_peers_multiaddr ON peers USING GIN(multiaddrs);

-- Update existing peers with placeholder data (for migration)
-- In production, existing peers will re-register with new data
UPDATE peers 
SET multiaddr = '/ip4/' || ip_address || '/tcp/' || port || '/p2p/migration-pending'
WHERE multiaddr IS NULL AND ip_address IS NOT NULL;

-- Add comments
COMMENT ON COLUMN peers.libp2p_peer_id IS 'libp2p peer ID (cryptographic identity)';
COMMENT ON COLUMN peers.multiaddr IS 'Primary multiaddr for connecting to peer';
COMMENT ON COLUMN peers.multiaddrs IS 'All known multiaddrs for this peer';

-- Create view for peer connectivity info
CREATE OR REPLACE VIEW peer_connectivity AS
SELECT 
    peer_id,
    libp2p_peer_id,
    multiaddr,
    multiaddrs,
    ip_address,
    port,
    CASE 
        WHEN libp2p_peer_id IS NOT NULL THEN 'libp2p'
        WHEN ip_address IS NOT NULL THEN 'legacy'
        ELSE 'unknown'
    END AS connection_type,
    last_heartbeat,
    is_active
FROM peers;

COMMENT ON VIEW peer_connectivity IS 'Shows peer connection information (legacy vs libp2p)';

-- Success message
SELECT 'Migration 001_add_libp2p_support completed successfully' AS status;
