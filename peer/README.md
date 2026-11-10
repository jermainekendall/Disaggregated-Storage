# Orchestrator peer agent and client

One program, `orchestrator`, for Ubuntu 24.04 desktop:

- **Agent** (`orchestrator run`): offers disk space to the network. It stores
  shards pushed to it, answers proof-of-storage audits, rebuilds or copies
  shards when the server asks, and deletes retired ones.
- **Client** (`upload`, `download`, `ls`, `rm`): your own files. Encrypted on
  this machine, erasure coded, and spread over other peers.

The server side is in [`../server`](../server/README.md) and must be running.

## Install

From this folder, as your normal user (not root):

```bash
./install.sh
```

By default the storage agent runs as its own **`orchestrator` system account**,
not as you. The agent accepts data from other peers, so it is kept away from
your files:

- **The account:** no login shell and no home directory. The agent's config,
  identity key and stored shards live in `/var/lib/orchestrator`, readable only
  by that account.
- **The sandbox:** the systemd service hides `/home` from the agent, makes the
  rest of the filesystem read-only to it, drops all capabilities and blocks
  privilege escalation. `systemd-analyze security` rates it 1.5 ("OK", lower is
  safer).
- **Startup:** it starts at boot; no login is needed.
- **Your own commands** (`login`, `upload`, `download`, ...) run as you, with
  your settings in `~/.config/orchestrator`. The agent publishes its peer ID in
  `/run/orchestrator/peer-id`, so your uploads never place shards on your own
  machine.

The installer:
- installs the build tools with apt;
- installs Go 1.24 from go.dev if needed (Ubuntu 24.04's packaged Go is too old),
  checking the published SHA-256;
- builds `orchestrator` into `/usr/local/bin`;
- creates the account and configures the agent (setup asks for the server
  addresses and storage);
- writes your client settings;
- opens the agent's port in ufw if the firewall is active;
- installs and starts the `orchestrator-agent` service;
- checks that the server is reachable.

**Moving an earlier per-user install:** if this machine already ran the agent as
your user, the installer moves its identity, peer ID and stored shards to the
`orchestrator` account. The machine keeps its identity on the network.

**Storage location:** the default is `/var/lib/orchestrator/storage`. A
directory on another disk works too, and the installer grants the sandbox write
access to it. A directory under `/home` does not work, because the sandbox hides
`/home`.

Re-run the installer after pulling new code: it rebuilds and restarts the agent.
To skip the questions, pass the answers after `--`:

```bash
./install.sh -y -- --non-interactive --gateway http://192.168.1.201:30080 --registry 192.168.1.201:30051 --relay /ip4/192.168.1.201/tcp/30001/p2p/<relay-id> --storage-gb 50
```

`./install.sh --help` lists every option. `--user-service` runs the agent as your
own user instead (the earlier setup).

**Managing the agent:**

```bash
sudo systemctl status orchestrator-agent
```

```bash
sudo journalctl -u orchestrator-agent -f
```

```bash
sudo -u orchestrator orchestrator --config /var/lib/orchestrator/config.yaml status
```

To change the agent's settings:

```bash
sudo env HOME=/var/lib/orchestrator orchestrator --config /var/lib/orchestrator/config.yaml setup
```

```bash
sudo chown -R orchestrator:orchestrator /var/lib/orchestrator
```

```bash
sudo systemctl restart orchestrator-agent
```

Re-running `./install.sh -- <setup flags>` does the same three steps.

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
| `/var/lib/orchestrator/config.yaml` | agent settings (owned by `orchestrator`, 0600) |
| `/var/lib/orchestrator/identity.key` | agent's libp2p identity (0600, created once) |
| `/var/lib/orchestrator/state.json` | agent's server-assigned peer ID |
| `/var/lib/orchestrator/storage/` | shards held for others (0700; the agent refuses looser permissions) |
| `/run/orchestrator/peer-id` | the agent's peer ID, world-readable |
| `~/.config/orchestrator/config.yaml` | your client settings (server addresses) |
| `~/.config/orchestrator/session.json` | your login token (0600) |
| `~/.config/orchestrator/work/` | temporary space for your uploads and downloads |

With `--user-service`, the agent's files are in `~/.config/orchestrator` and
`~/.local/share/orchestrator/storage` instead.

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
