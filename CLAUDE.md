# Project context

Disaggregated storage: a P2P storage network ("Orchestrator") that, like BitTorrent/IPFS,
spreads data across peers, but splits each file into erasure-coded shards
(data + parity) held by different peers instead of storing whole copies.

Full server-side design and known gaps: `docs/SERVER_SIDE_TECHNICAL_REFERENCE.md`
(state as of 2026-11-07). Read it before working on server components.

## Key facts

- Two tiers: central services on Kubernetes (Go, gRPC, CockroachDB) for coordination and
  metadata; peers on libp2p hold and exchange shard data, with a circuit relay for NAT.
- Services: API Gateway (HTTP 8080), Peer Registry, Placement, Audit, Billing,
  Key Metadata (gRPC), plus repair/replication/analytics workers and a libp2p relay.
- Placement: 10-100 data shards, each <= 5 GB, 4 parity by default; ASN diversity
  (>= 5 ASNs, <= 3 shards per ASN) and timezone grouping.
- Encryption: per-file AES-256-GCM keys; Shamir secret sharing for key escrow.

## Status (2026-11-08)

- Server code lives in `server/` (one Go module). Start with `server/README.md`.
- The peer agent and client live in `peer/` (one `orchestrator` binary for Ubuntu
  24.04; separate Go module that reuses `server/proto` via a replace directive).
  Start with `peer/README.md`. Telepathi integration is deliberately deferred.
- Tests for `peer/`: `make test` and `make integration` (seven agents, relay,
  server services; needs a CockroachDB).
- `server/infrastructure/schema.sql` is the single source of truth for the database;
  every service checks the columns it uses at startup.
- `server/proto/` holds the only .proto files; regenerate stubs with `make proto`.
- Tests: `go test ./...` (unit) and `server/test/e2e` (needs a CockroachDB, see README).
- The Kubernetes manifests in `server/deployments/` are the single, test-sized set
  (1 replica each, canonical ports 50051-50055). They are deployed on the user's
  single-node test cluster (192.168.1.201).
- Many gaps in section 12 of the technical reference are fixed (proto mismatch,
  env getters, ports, timezone, migration 001, shard locations, rate limiting,
  relay rebuild). TLS and production auth remain out of scope for the test deployment.
