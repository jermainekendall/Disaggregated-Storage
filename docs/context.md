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

## Current state

- The source tree the reference describes (`production-complete/`, `server-updates/`,
  `outputs/relay-service/`, `orchestrator-k8s/`, `peer-registry-FIXED/`) is **not yet in
  this repository**. Only these docs and the README are here.
- No server component builds today. The three blockers:
  1. Three protobuf sources disagree. Pick one (recommended:
     `server-updates/proto/peer_registry.proto`) and regenerate the stubs with `protoc`.
  2. `peer-registry/main.go` does not compile against the proto types.
  3. `getEnvInt` and `getEnvFloat` in the peer registry ignore the environment.
- Other open gaps: the port mapping (50051 vs 50052), timezone stored both as text and as
  an int, migration 001 writing placeholder multiaddrs that can't be dialed, no
  `shard_locations` table, no TLS on the gateway or database, a global rather than
  per-client rate limit, the relay image needing a rebuild at v1.0.1, and workers that
  haven't been reviewed. The full table is in §12 of the reference.

## Next steps (from the reference, §13)

1. Choose the canonical proto, regenerate the stubs, and align the Go code with the
   generated names.
2. Fix the env getters, then run `go build ./...` in each service directory.
3. Resolve the port mapping and timezone representation, and update the manifests.
4. Correct migration 001 so placeholder addresses are never dialed.
5. Build each service and run a gRPC smoke test before deploying.

## Related work

`jermainekendall/telepathi` is a separate Go project for block-level dedup and
compressed file transfer between machines. It may be useful for shard transfer, but
nothing connects the two projects yet.
