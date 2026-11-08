# Orchestrator P2P Storage — Server-Side Technical Reference

Status: describes the design and the current state of the source tree as of 2026-11-07.
Items marked **[NOT YET BUILDABLE]** or **[MISMATCH]** are known gaps. This document does
not claim the server side compiles or has been tested end to end.

---

## 1. Purpose and scope

The server side is the set of central services that peers and clients depend on for
coordination. It does not store file contents. Shard data lives on peers (client side).
The server side handles:

- peer registration, liveness, and health scoring
- placement decisions (which peers hold which shards)
- client-facing HTTP API (uploads, downloads, listings, earnings)
- NAT traversal help via a libp2p circuit relay
- audit, repair, replication, billing, and key-metadata workers (see §9)

## 2. Service inventory

| Service | Location | Protocol / port (code default) | Backing store |
|---|---|---|---|
| API Gateway | `production-complete/services/api-gateway/` | HTTP 8080 (NodePort 30080) | CockroachDB (`lib/pq`) |
| Peer Registry | `production-complete/services/peer-registry/` | gRPC 50052 | CockroachDB |
| Placement Service | `production-complete/services/placement-service/` | gRPC 50051 | CockroachDB |
| Audit Service | `production-complete/services/audit-service/` | gRPC 50054 | CockroachDB |
| Billing Service | `production-complete/services/billing-service/` | gRPC 50053 | CockroachDB |
| Key Metadata | `production-complete/services/key-metadata/` | gRPC 50055 | CockroachDB |
| Repair / Replication / Analytics workers | `production-complete/services/*-worker/` | background | CockroachDB |
| Relay Service | `outputs/relay-service/main-FINAL.go` | libp2p circuit relay | none |
| Monitoring | Prometheus 30090, Grafana 30300 | | |

**[MISMATCH] Port assignment.** The Peer Registry default is 50052 in its code, but the
Placement Service default is 50051, and earlier design notes assign 50051 to the Peer
Registry. Pick one mapping and set it in the Kubernetes manifests and client config.

## 3. Architecture

Two tiers:

- **Central layer** (Kubernetes): gRPC services and SQL metadata. Authoritative for
  peers, placements, quotas, billing, and keys.
- **Peer layer** (libp2p): storage nodes exchange shard data directly when NAT allows,
  and through the relay when it does not.

Metadata store: CockroachDB, chosen for strongly consistent writes with horizontal scale.
Ephemeral placement caches and health signals are allowed to be eventually consistent.

```
 Client ──HTTP──► API Gateway ──gRPC──► Placement ──► CockroachDB
                      │                     ▲
                      └──gRPC──► Peer Registry ◄──gRPC── Peers (heartbeat/register)
                                                              │
                                  Relay (libp2p circuit) ◄────┤ when direct dial fails
                                                              ▼
                                                        Peer ↔ Peer shard transfer
```

## 4. Data model

### 4.1 `peers` table

Columns referenced by the code (not all are in every migration):

| Column | Type | Source |
|---|---|---|
| `peer_id` | UUID PK | generated on insert |
| `user_id`, `device_model`, `os_type`, `os_version`, `app_version` | text | registration |
| `asn`, `timezone`, `region`, `country_code`, `isp` | text | registration |
| `storage_total_gb`, `storage_free_gb`, `bandwidth_max_mbps` | number | registration / heartbeat |
| `is_online`, `is_charging`, `battery_level`, `network_type` | bool / int / text | heartbeat |
| `health_score` (0–100), `reliability_score` (0–1), `uptime_percentage` | number | scoring |
| `first_seen_at`, `last_seen_at`, `last_heartbeat_at` | timestamp | lifecycle |
| `libp2p_peer_id`, `multiaddr`, `multiaddrs[]` | varchar / text / text[] | migration 001 |

**[MISMATCH] Timezone.** The code stores `timezone` as text in one place and the placement
service reads it as an integer hour offset (`Timezone int`). Store one representation
(IANA name such as `America/Guyana` is preferable) and convert in the placement code.

### 4.2 Migration 001 (libp2p support)

`server-updates/migrations/001_add_libp2p_support_FIXED.sql` adds `libp2p_peer_id`,
`multiaddr`, and `multiaddrs`, then backfills `multiaddr`.

**[MISMATCH]** The backfill writes `/ip4/<ip>/tcp/<port>/p2p/migration-pending`. A
placeholder peer ID in a multiaddr is not a valid address for libp2p dialing. Either
exclude these rows from dialing until a peer re-registers, or set `multiaddr` to NULL
during backfill. The `ip_address` and `port` columns it reads are not defined in any
migration in this tree, so the backfill must be checked against the real schema first.

Run the backfill in batches, and confirm the `ALTER TABLE` job has finished (`SHOW JOBS`)
before the `UPDATE`.

### 4.3 Protobuf contracts

Three protobuf sources exist and they **do not agree**:

- `production-complete/proto/peer/peer.proto`: `PeerRegistration` has `ip_address`, `port`, `version`.
- `server-updates/proto/peer_registry.proto` and `peer-registry-FIXED/peer.proto`: add
  `libp2p_peer_id`, `multiaddr`, `multiaddrs`, `timezone` (the newer contract).
- The Go code in `peer-registry/main.go` uses `UserId`, `DeviceModel`, `Asn`, `Timezone`,
  `StorageFreeGb`, and similar names that match neither proto file.

**Rule:** regenerate Go stubs with `protoc` from **one** proto file, then update the Go
code to the generated names. Regeneration is a hard prerequisite (see
`technical-decisions.md`).

Services in the placement contract:

- `PlacementService.SelectPeersForUpload(UploadRequest) → PeerSelection`
- `PeerSelection` returns `shard_count`, `parity_shards`, and a list of `SelectedPeer`
  entries with `peer_address`, `health_score`, `asn`, `region`.

## 5. Peer Registry

### 5.1 RPCs (target contract)

| RPC | Purpose | Writes |
|---|---|---|
| `RegisterPeer` | create a peer, or update it if the same user and device already exist | `peers` insert/update |
| `Heartbeat` | liveness, storage free, battery, network type | `peers` update |
| `GetPeer` | fetch one peer | read |
| `ListPeers` | list by status (`active`, `inactive`, `all`) with a limit | read |

### 5.2 Registration

Lookup key is `(user_id, device_model)`. If a row exists, update its addresses and
capacity and set `is_online = true`. Otherwise insert with `health_score = 100`,
`reliability_score = 1.0`, `uptime_percentage = 0`. Both branches stamp `last_seen_at`.

Known issue: `RegisterPeer` in `peer-registry/main.go` does not match the proto types.
It returns `*RegisterPeerResponse` (undefined) and uses `req.PeerId` (absent from the
production proto). **[NOT YET BUILDABLE]**

### 5.3 Heartbeat and liveness

- Peers send a heartbeat about every 15 minutes (`HEARTBEAT_TIMEOUT_SECONDS=900`).
- A peer with no heartbeat for the timeout is marked offline.
- Peers with no contact for `INACTIVE_PEER_HOURS` (default 168) are treated as inactive.
- `CHURN_THRESHOLD_HOURS` (default 720) is used to detect repeated disconnects.
- `MIN_HEALTH_SCORE` (default 70) is the floor for peers offered new placements.

**[MISMATCH]** `getEnvInt` and `getEnvFloat` in the peer registry return the default
without reading the environment. The overrides above do not take effect until those
functions read `os.Getenv`. The placement service's `getEnvInt` is implemented correctly
and can be used as the reference.

### 5.4 Health scoring

`health_score` (0–100) and `reliability_score` (0–1) are computed from uptime, heartbeat
regularity, and recent failures. The formulas are not fully specified in the code tree.
Document them before relying on them for placement decisions.

## 6. Placement Service

### 6.1 Selection pipeline

1. Load the owner peer (`getPeerInfo`) and eligible peers (`getEligiblePeers`), excluding
   the owner.
2. Compute shard strategy from file size (`CalculateShardStrategy`).
3. Filter peers with `storage_free_gb >= shard_size × 1.2` (20% headroom).
4. Apply diversity constraints (below).
5. Score and select, then return `PeerSelection`.

### 6.2 Adaptive sharding

Constants: `MinShardCount=10`, `MaxShardCount=100`, `MaxShardSizeGB=5`,
`MinPeerCapacityGB=0.5`, `DefaultParityShards=4`.

- Start at 10 data shards. Double (capped at 100) until each shard is ≤ 5 GB.
- Require `data + parity` peers. If there are fewer peers than that, reduce the data
  shard count to `available − parity`. If the result is below 10, the request fails.

Example: a 20 GB file gives 10 shards of 2 GB each, plus 4 parity, so 14 peers are needed.

**Note:** the code calls `log.Printf` on the "not enough peers" path but still returns a
strategy. The caller then fails later with a capacity error. Return the error at the
point of detection.

### 6.3 Diversity constraints (configuration)

| Env var | Default | Meaning |
|---|---|---|
| `MIN_ASNS_PER_FILE` | 5 | distinct ASNs that must hold a file's shards |
| `MAX_PEERS_PER_ASN` | 3 | cap on shards in one ASN |
| `LOCAL_CLUSTER_SIZE` | 6 | size of the same-timezone group |
| `GLOBAL_CLUSTER_SIZE` | 8 | size of the cross-timezone group |
| `MIN_REPLICATION_COUNT` / `MAX_REPLICATION_COUNT` | 14 / 28 | total shard placement bounds |

The design calls for ASN diversity, AS-path awareness, timezone alignment for
availability, and nearby-ASN preference. Only the ASN cap and timezone grouping are
clearly implemented in the code read so far. AS-path awareness is not visible in the
code.

**Timezone alignment:** for availability, replicas should overlap during peak local
hours. Grouping by timezone (`LOCAL_CLUSTER_SIZE`) is the mechanism. Testing this needs
the validation matrix described in the design (§Testing & SLOs).

## 7. API Gateway

Base path `/api/v1`. Health at `/health` (liveness) and `/ready` (checks DB with a 2 s
timeout, returns 503 when not ready).

| Method | Path | Handler | Status |
|---|---|---|---|
| GET | `/health`, `/ready` | health checks | implemented |
| POST | `/auth/login`, `/auth/register` | auth | routes defined; logic to be confirmed |
| GET | `/files` | list files | route defined |
| GET | `/files/{id}` | get file | route defined |
| POST | `/files/upload` | upload | route defined |
| DELETE | `/files/{id}` | delete | route defined |
| GET | `/peers` | list peers | route defined |
| POST | `/peers/register` | register peer | route defined |
| GET | `/users/me`, `/users/me/earnings` | user info | route defined |

Rate limiting: a single global `rate.Limiter` at 1000 requests/minute. This is not per
client. Move to per-key limits before exposing this publicly.

**[MISMATCH]** The gateway's `/peers/register` path and the peer registry gRPC
`RegisterPeer` are two entry points for the same operation. Document which one peers
use, and make sure they share validation.

**Upload flow (intended):** client → gateway → placement (`SelectPeersForUpload`) →
gateway returns the peer list → client encrypts and sends shards to peers →
peers acknowledge → gateway records placements in `shard_locations`.

**[NOT YET IMPLEMENTED]** `shard_locations` table and `libp2p_client.go` integration in
the gateway (listed as pending in the project notes).

## 8. Relay Service

`outputs/relay-service/main-FINAL.go` runs a libp2p circuit relay (v2) for peers that
cannot accept inbound connections.

- It is a relay **server**. Do not call `EnableAutoRelay()` on it. That option is for
  clients looking for relays (recorded in `technical-decisions.md`).
- Deployed to Kubernetes with a multi-stage Dockerfile, two or more replicas, exposed on
  NodePort 30001 (per the design doc).
- Peers reserve a relay slot, then other peers dial them through the relay address.

Rebuild the image at v1.0.1 from `main-FINAL.go` before the next rollout.

## 9. Background workers

| Worker | Source | Role |
|---|---|---|
| Replication | `replication-worker/main.go` | re-create shards when a peer goes offline |
| Repair | `repair-worker/main.go` | detect and fix corrupt or missing shards |
| Audit | `audit-service/main.go` | periodic integrity and availability challenges |
| Analytics | `analytics-worker/main.go` | aggregate metrics |
| Billing | `billing-service/main.go` | usage and earnings accounting |
| Key metadata | `key-metadata/main.go` | key lifecycle endpoints (per-file keys, wrapping, escrow) |

These were not reviewed in detail for this document. Their interfaces and
correctness need the same compile-and-review pass as the peer registry before
they are described as working.

## 10. Security

- **Transport:** libp2p Noise for peer channels. Client ↔ gateway over HTTP today;
  TLS is required before production.
- **Database:** `sslmode=disable` is hard-coded in the gateway's connection string.
  Enable TLS for CockroachDB.
- **Encryption:** per-file AES-256-GCM keys, master key and recovery secret, Shamir
  secret sharing for escrow (per design). Key material is not stored in the peer
  registry.
- **Authentication:** MFA, rate limiting, key rotation are in the design. Only a
  global rate limit is in code today.
- **Sybil resistance:** identity friction, quotas, reputation, device attestation are
  in the design. Not implemented in the code read.
- **Uninstall guard / data-deletion abuse:** design only.

## 11. Deployment

- Kubernetes manifests in `server-updates/k8s/` and `orchestrator-k8s/`. Two versions
  of the relay manifest exist (`06-libp2p-relay.yaml`, `06-libp2p-relay-FIXED.yaml`).
  Use the FIXED one.
- Config is by environment variable (see each service's `loadConfig`).
- Build the proto stubs first, then the services, in this order:
  1. `protoc` on one proto source
  2. apply migration 001 (checked, batched)
  3. build and roll out peer registry, then placement, then gateway
  4. relay last, once peers can reach it
- Health: `/ready` on the gateway; Kubernetes probes on each gRPC service.

## 12. Known gaps (summary)

| # | Gap | Where |
|---|---|---|
| 1 | Three proto sources disagree | `proto/`, `server-updates/proto`, `peer-registry-FIXED` |
| 2 | Peer registry does not compile against the proto types | `peer-registry/main.go` |
| 3 | Env overrides ignored in peer registry | `getEnvInt`, `getEnvFloat` |
| 4 | Port mapping conflicts between docs and code | §2 |
| 5 | Timezone stored as text and as int | §4.1 |
| 6 | Migration writes non-dialable placeholder multiaddrs | §4.2 |
| 7 | AS-path awareness not in code | §6.3 |
| 8 | `shard_locations` and gateway libp2p client not implemented | §7 |
| 9 | Relay image needs rebuild at v1.0.1 | §8 |
| 10 | Workers not reviewed | §9 |
| 11 | No TLS on gateway or DB connection | §10 |
| 12 | Gateway rate limit is global, not per client | §7 |

Items 1–3 must be fixed before any server component will build. The earlier
build-fix work on the client did not touch these files.

## 13. Next steps

1. Pick the canonical proto (recommend `server-updates/proto/peer_registry.proto`, the
   newest), regenerate with `protoc`, and align the Go code with the generated names.
2. Fix the env getters in the peer registry, then run `go build ./...` in each service
   directory.
3. Resolve the port mapping and timezone representation, and update the manifests.
4. Correct migration 001 so placeholder addresses are never dialed.
5. Run each service's build and a basic gRPC smoke test before the next deploy.
