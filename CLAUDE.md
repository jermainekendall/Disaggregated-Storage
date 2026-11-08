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

## Status

- The source tree the reference describes (`production-complete/`, `server-updates/`,
  `outputs/relay-service/`, `orchestrator-k8s/`, `peer-registry-FIXED/`) is **not yet
  in this repository**.
- The server side does not build yet. Fix these first: the three proto sources disagree,
  the peer registry does not compile against the generated types, and the peer registry's
  env getters ignore the environment. See section 12 of the reference for the full gap list
  and section 13 for next steps.
