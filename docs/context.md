# Project context

Starting context for new sessions on this repository. It was imported on 2026-11-08 from
`SERVER_SIDE_TECHNICAL_REFERENCE.md` (dated 2026-11-07), which is kept in full next to
this file. Read that document for detail; this page is the short version.

## What the project is

An orchestrated peer-to-peer storage network, internally called "Orchestrator P2P
Storage". Files are encrypted, split into data shards plus parity shards, and spread
across many peer devices. It works like BitTorrent or IPFS, except that no peer holds a
whole file.

## Architecture in brief

- **Central layer (Kubernetes):** Go gRPC services that coordinate the network but never
  store file contents. They are the API Gateway (HTTP 8080), Peer Registry, Placement,
  Audit, Billing, Key Metadata, and the Repair, Replication and Analytics workers. Metadata
  lives in CockroachDB.
- **Peer layer (libp2p):** peers exchange shards directly, or through a libp2p circuit
  relay (v2) when NAT blocks inbound connections.
- **Placement:** starts at 10 data shards, doubling up to a maximum of 100 until each
  shard is 5 GB or smaller, plus 4 parity shards. A peer needs 20% free-space headroom
  beyond the shard size. Diversity rules: at least 5 ASNs per file, at most 3 shards in
  one ASN, and replicas grouped by timezone.
- **Security (design):** AES-256-GCM key per file, a master key and recovery secret,
  Shamir secret sharing for key escrow, and libp2p Noise encryption between peers.

## Current state (2026-11-08)

The server code is in `server/` (see `server/README.md`). A review of the uploaded code
found it compiled but could not work end to end: the code and schema disagreed, most
gRPC services were never registered, repair treated erasure-coded shards as 14 copies
each, the relay cut circuits at 1 MiB, and `install.sh` stopped before deploying. Those
issues were fixed on the `development` branch:

- One Go module, one Dockerfile, one set of protos, one idempotent `schema.sql` that
  also upgrades the old schema in place.
- Canonical ports: peer-registry 50051, placement 50052, billing 50053, audit 50054,
  key-metadata 50055, gateway 8080 (NodePort 30080), relay 4001 (NodePort 30001).
- Timezone is stored as an IANA name everywhere. The `shards` table is the placement
  table (one row per shard copy on a peer).
- Audits use precomputed proof-of-storage challenges. Repair rebuilds each missing
  shard index once on one new peer. Tiering uses a 30-day access window.
- The relay has a persistent identity from a Secret and no circuit data limit.
- An end-to-end test runs every flow against a real CockroachDB.

Still out of scope for the test deployment: TLS, authentication of peer agents,
real payment processing, health-score formulas, AS-path awareness and timezone
clustering in placement, and a peer agent implementation (the server endpoints it
needs exist).

## Related work

`jermainekendall/telepathi` is a separate Go project for block-level dedup and
compressed file transfer between machines. It may be useful for shard transfer, but
nothing connects the two projects yet.
