# Orchestrator peer agent and client

One program, `orchestrator`, for Ubuntu 24.04 desktop:

- **Agent** (`orchestrator run`): offers disk space to the network. It stores
  shards pushed to it, answers proof-of-storage audits, rebuilds or copies
  shards when the server asks, and deletes retired ones.
- **Client** (`upload`, `download`, `ls`, `rm`): your own files. Encrypted on
  this machine, erasure coded, and spread over other peers.

The server side is in [`../server`](../server/README.md) and must be running.

## Install

You need Go 1.24 or newer to build. From this folder:

```bash
make install
```

That puts the binary in `~/.local/bin/orchestrator`. Then configure it:

```bash
orchestrator setup
```

Setup asks for the gateway URL (e.g. `http://192.168.1.201:30080`), the
registry address (`192.168.1.201:30051`), the relay address (printed at the end
of the server install), the storage directory and how much space to offer.

Run the agent in the foreground once to check it registers:

```bash
orchestrator run
```

Then run it as a systemd user service, so it starts with your session:

```bash
make install-service
```

```bash
journalctl --user -u orchestrator-agent -f
```

To keep it running while you are logged out:

```bash
sudo loginctl enable-linger $USER
```

## Use

```bash
orchestrator login --register
```

```bash
orchestrator upload ~/Pictures/holiday.tar
```

```bash
orchestrator ls
```

```bash
orchestrator download <file-id> -o ~/Downloads/
```

```bash
orchestrator rm <file-id>
```

```bash
orchestrator status
```

The first upload asks you to choose an **encryption passphrase**. It never
leaves this machine. If you lose it, your files cannot be recovered.

For scripts, `ORCHESTRATOR_EMAIL`, `ORCHESTRATOR_PASSWORD` and
`ORCHESTRATOR_PASSPHRASE` replace the prompts.

## How it works

**Upload**
1. gzip, then AES-256-GCM in 64 KiB authenticated chunks, with a random key for
   this file. The work happens in a temporary file, so file size is limited by
   disk, not memory.
2. The server picks the code (k data + m parity shards) and one peer per shard.
3. Reed-Solomon coding splits the encrypted file into k + m equal shards.
4. Each shard goes to its peer over libp2p (`/orchestrator/store/1.0.0`). The
   peer accepts only a shard the server assigned to it, checks the size and
   SHA-256, writes it to a temp file, fsyncs, renames it into place, and
   confirms it to the server.
5. The file key, wrapped with your master key, is stored on the server.
6. For every shard, 8 audit challenges `(nonce, SHA-256(nonce ‖ shard))` are
   registered. The server later sends one to the peer holding the shard, and
   only a peer that still has the bytes can answer.

**Download:** fetch any k shards (data shards first, each checked against its
recorded SHA-256), rebuild missing data shards from parity if needed, join,
decrypt, decompress.

**Keys**
- **Master key:** random, wrapped with Argon2id(passphrase, random salt) and
  stored on the server. The KDF parameters and salt are inside the wrapped blob.
- **File keys:** random per file, wrapped with the master key and bound to the
  file ID.
- The server never sees a plaintext key.

**Network:** libp2p over TCP and QUIC (IPv4 and IPv6), with a persistent Ed25519
identity, connection manager, UPnP/NAT-PMP port mapping and hole punching. The
agent keeps a reservation on the server's relay and advertises the relay
address, so peers behind NAT stay reachable.

## Files

| Path | What |
|---|---|
| `~/.config/orchestrator/config.yaml` | settings (0600) |
| `~/.config/orchestrator/identity.key` | libp2p identity (0600, created once) |
| `~/.config/orchestrator/state.json` | server-assigned peer ID |
| `~/.config/orchestrator/session.json` | login token (0600) |
| `~/.config/orchestrator/work/` | temporary space for uploads, downloads and rebuilds |
| `storage_dir` | shards held for others (0700; the agent refuses looser permissions) |

## Tests

```bash
make test
```

The full network test starts the server services, a relay and seven agents on
one machine. One agent is reachable only through the relay. The test then runs
upload, download, audits, download with a peer gone, repair and delete.

```bash
docker run -d --name crdb -p 26257:26257 cockroachdb/cockroach:v23.1.13 start-single-node --insecure
```

```bash
TEST_DATABASE_URL='postgresql://root@localhost:26257/defaultdb?sslmode=disable' make integration
```

## Feature list status

How each item of the earlier client's feature list is handled in this version.

| ID | Feature | Now |
|---|---|---|
| F001-F004 | Passphrase master key, AES-256-GCM, gzip, SHA-256 | Done |
| F005 | Key derivation verified | Argon2id with random salt, parameters stored with the key |
| F006 | Key escrow and recovery | Later (the server can record Shamir share holders) |
| F007-F010 | Identity, TCP v4/v6, connection manager, NAT features | Done, plus QUIC (F015) |
| F011-F012 | Kademlia DHT and bootstrap | Dropped: the server supplies peer addresses |
| F013 | Relay fallback | Done: reservation kept alive, circuit address advertised |
| F014 | Peer discovery and dialing | Done via server-supplied multiaddrs |
| F016, F018, F019 | Setup wizard, 0700/0600, refuse bad permissions | Done |
| F017 | Partition creation | Later (wizard explains how to use a mounted partition) |
| F020-F024 | Atomic store, hash check, index rebuilt on start, race-free space, delete removes files | Done |
| F025-F027 | Store, retrieve, status streams | Done; status no longer exposes paths |
| F028-F029 | Stream authorization, quotas | Agent accepts only server-assigned shards of the assigned size |
| F030-F032 | Registration, region/timezone, heartbeat loop | Done; timezone from the system |
| F033-F034 | Heartbeat route and interval | gRPC to the registry, every 5 min (server timeout 15 min) |
| F035 | Battery, charging, network type | Read from /sys and the default route |
| F036 | Uptime percentage | Computed by the server, not the agent |
| F037 | TLS to the gateway | Later (test deployment is plain HTTP) |
| F038, F046, F047 | Peer list, replication, rebalance | Server-driven: agents fulfil copy jobs from pending-shards |
| F039-F044 | Real sharding, placement, network send, persistence, download, checksums | Done (Reed-Solomon, server placement, libp2p) |
| F045 | Repair | Agents rebuild a lost shard from k others and verify it against the original hash |
| F048 | Audit responses | Done |
| F049-F052 | CLI modes, config validation, graceful shutdown, flag precedence | Done; old `--peer/--setup/--status/--version` flags kept |
| F053 | Metrics | Later |
| F054 | Retry with backoff | Done on every server call and shard push |
| F055 | Typed errors | Partly (API errors carry status codes) |
| F056 | Clean build | Done: `go vet` clean, unit and integration tests pass |
| F057 | GUI | Later |
