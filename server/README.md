# Orchestrator server (test deployment)

Central services for the disaggregated storage network. They coordinate peers
and keep metadata in CockroachDB; they never store file contents. Shard bytes
live on peers and move peer to peer over libp2p.

> **Test deployment.** No TLS anywhere, CockroachDB runs in insecure mode,
> peer-agent HTTP endpoints are not authenticated, and payouts are simulated.
> Do not expose this to the internet as is.

## Layout

```
server/
  go.mod                 one Go module for every service
  Dockerfile             one Dockerfile; pick the service with --build-arg SERVICE=<name>
  Makefile               proto / build / test / images
  docker-compose.yml     whole stack locally, no Kubernetes
  install.sh             single-node Kubernetes install on Ubuntu 24.04
  internal/common        env config, DB connection + retry, schema self-check, gRPC server
  internal/storage       shared file/shard bookkeeping (health, repair queue)
  proto/                 the only .proto files, with generated Go stubs
  services/<name>/       one main package per service
  infrastructure/        schema.sql (single source of truth) and cluster manifests
  deployments/           one manifest per service
  test/e2e/              end-to-end test against a real CockroachDB
```

## Services and ports

| Service | Port | Exposed on the node | Purpose |
|---|---|---|---|
| api-gateway | HTTP 8080 | NodePort 30080 | Client and peer-agent API |
| admin-console | HTTP 8080 | NodePort 30088 | Web interface for administrators |
| peer-registry | gRPC 50051 | NodePort 30051 | Registration, heartbeats, lookup |
| placement-service | gRPC 50052 | | Erasure code and peer choice per file |
| billing-service | gRPC 50053 | | Monthly earnings, payouts |
| audit-service | gRPC 50054 | | Proof-of-storage challenges |
| key-metadata | gRPC 50055 | | Wrapped master and file keys, recovery holders |
| repair-worker | | | Rebuilds lost shards |
| replication-worker | | | Tiering and extra copies for popular files |
| analytics-worker | | | Churn, reliability, system stats |
| libp2p-relay | libp2p 4001 | NodePort 30001 | Circuit relay v2 for peers behind NAT |

Every gRPC service also serves the standard gRPC health service (used by the
Kubernetes probes) and reflection, so `grpcurl -plaintext <host>:<port> list` works.

## Running it

**Locally with Docker Compose** (from `server/`):

```bash
docker compose up --build
```

The admin console is then at `http://localhost:8088` (sign in as
`admin@localhost.test` with `local-dev-admin-password`).

**On a single Ubuntu 24.04 machine with Kubernetes:**

```bash
sudo ./install.sh
```

The installer is safe to re-run. Use it again after code changes (`sudo ./install.sh -y`):
it rebuilds the images and restarts the pods.

**Tests:**

```bash
go test ./...
```

```bash
docker run -d --name crdb -p 26257:26257 cockroachdb/cockroach:v23.1.13 start-single-node --insecure
```

```bash
TEST_DATABASE_URL='postgresql://root@localhost:26257/defaultdb?sslmode=disable' go test ./test/e2e -v
```

The end-to-end test builds every service, starts them against the database
and runs peer registration, upload, audits, repair, replication, billing, keys,
analytics and deletion.

## Admin console

Open `http://<node-ip>:30088` and sign in with an administrator account.
`install.sh` creates the first one (set `ADMIN_EMAIL` before running it to choose
the address) and saves the sign-in to a root-only file:

```bash
sudo cat /root/orchestrator-admin-credentials.txt
```

From the console you can:

| Page | What it is for |
|---|---|
| Dashboard | users, peers online, capacity, stored data, missing shards, open repairs, audit pass rate, pending billing |
| Users | search; suspend or reactivate; change tier and storage quota; grant or remove admin; set a new password |
| Peers | health, capacity, shards held and audit record; **drain** a peer (no new shards, and its shards are rebuilt elsewhere); set its health score |
| Files | owner, code, shard map and health; queue a repair at top priority (names are encrypted and never shown) |
| Repairs | open, failed and completed repair jobs; retry a failed one; replication job counts |
| Audits | pass, fail and timeout counts; peers with failures; recent failures |
| Billing | earnings per month; run billing for a month; approve pending earnings; settle payouts |
| Settings | earnings rates, payout minimum and audit penalty (used by billing on its next run); reference values |
| Admin log | every change made in the console, with who, when and from where |

Protections:
- Admins cannot suspend themselves or remove their own admin role.
- Every request re-checks that the account is still an active admin, so revoking
  access takes effect at once.
- Forms carry CSRF tokens, sign-in attempts are rate-limited, and session cookies
  are `HttpOnly` and `SameSite=Strict`.
- Passwords are never written to the admin log.

It is served over plain HTTP in this test deployment, so keep it on a trusted
network. Before exposing it further, put it behind TLS (for example an ingress
with a certificate) and set `COOKIE_SECURE=true` in
`deployments/11-admin-console.yaml`.

More admins: create the account through the normal sign-up, then grant it the
admin role on its Users page.

## How data flows

1. **Peers register** (gRPC `RegisterPeer` or `POST /api/v1/peers/register`) with
   their libp2p ID and multiaddrs, get a server UUID back, then heartbeat.
2. **Upload:** the client calls `POST /api/v1/files/upload` with the file size.
   Placement picks the code (`MIN_SHARD_COUNT` data + `PARITY_SHARDS` parity) and one
   peer per shard. The client encrypts, erasure codes and sends each shard to its peer.
   Each peer confirms with `POST /api/v1/shards/{id}/confirm`. The file turns
   `active` when every shard is confirmed.
3. **Audits:** the uploader registers `(nonce, SHA-256(nonce || shard))` pairs with
   `AuditService.RegisterChallenges`. The audit service spends one per audit. Peers
   collect them with `GetPendingAudits` and answer with `VerifyResponse`. Repeated
   failures mark the shard missing and queue a repair.
4. **Repair:** when a shard is lost (peer offline past `INACTIVE_PEER_HOURS`, or failed
   audits), the repair worker assigns that shard index to one new peer. That peer
   finds its work and the surviving shards at `GET /api/v1/peers/{id}/pending-shards`,
   rebuilds the shard from any `data_shards` of them, and confirms it.
5. **Tiering:** accesses in the last 30 days decide cold, warm or hot. Warm and hot
   files get extra copies of data shards (copy jobs that the target peer fulfils and
   confirms). Copies are retired when a file cools down.
6. **Delete:** shards are marked retiring. Peers see them under `delete` in
   pending-shards and acknowledge with `POST /api/v1/shards/{id}/deleted`.

## Test-cluster settings

`deployments/03-placement-service.yaml` uses a 4 + 2 code with ASN rules off,
so six devices on one home network are enough. For production-like behaviour,
set `MIN_SHARD_COUNT=10`, `PARITY_SHARDS=4`, `MAX_PEERS_PER_ASN=3` and
`MIN_ASNS_PER_FILE=5`.

## Secrets

`install.sh` creates both of these if they are missing:

- `gateway-secrets`: `JWT_SECRET`, used to sign client tokens.
- `relay-identity`: `relay.key`, the relay's libp2p private key. The relay's peer ID is
  part of every relay address peers store, so keep this Secret. To make one by hand:

```bash
docker run --rm orchestrator/relay-service:dev -genkey > relay.key
```

```bash
kubectl create secret generic relay-identity -n orchestrator --from-file=relay.key
```

## Regenerating gRPC stubs

```bash
pip install grpcio-tools
```

```bash
go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.34.2
```

```bash
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.4.0
```

```bash
make proto
```
