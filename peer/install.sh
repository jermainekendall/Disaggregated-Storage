#!/bin/bash
#
# Installer for the Orchestrator peer agent and client on Ubuntu 24.04 desktop.
#
# Run it as your normal user from the peer/ directory (it uses sudo for the
# system steps). By default the storage agent runs as its own `orchestrator`
# system account, sandboxed by systemd, so a fault in the agent (which accepts
# data from other peers) cannot reach your files. Your own commands
# (`orchestrator upload`, `ls`, ...) still run as you, with your login.
#
# Steps:
#   1. install build dependencies with apt (git, make, curl, jq, ...)
#   2. install Go 1.24 from go.dev if the installed Go is missing or too old
#      (Ubuntu 24.04's packaged Go is 1.22), verifying the published SHA-256
#   3. build `orchestrator` and install it to /usr/local/bin
#   4. create the `orchestrator` account and configure the agent in
#      /var/lib/orchestrator (moving over an earlier per-user install)
#   5. write your client settings to ~/.config/orchestrator
#   6. open the agent's libp2p port (TCP and UDP) if the ufw firewall is active
#   7. install and start the orchestrator-agent system service
#   8. check that the server's gateway and registry are reachable
#
# Usage:  ./install.sh [options] [-- setup flags]
#   -y, --yes        don't ask for confirmation
#   --user-service   run the agent as YOUR user (systemd user service, ~/.local/bin)
#                    instead of the dedicated account
#   --no-setup       skip agent configuration (configure later)
#   --no-service     don't install or start the service
#   --linger         (--user-service only) keep the agent running while logged out
#   --go-version V   Go version to install if needed (default 1.24.7)
# Anything after `--` is passed to `orchestrator setup` for the agent, e.g.
#   ./install.sh -y -- --non-interactive --gateway http://192.168.1.201:30080 \
#       --registry 192.168.1.201:30051 --relay /ip4/192.168.1.201/tcp/30001/p2p/12D3...
#
# Re-running is safe: it rebuilds, reinstalls and restarts the agent.

set -Eeuo pipefail

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; BLUE='\033[0;34m'; NC='\033[0m'
info() { echo -e "${BLUE}[INFO]${NC} $*"; }
warn() { echo -e "${YELLOW}[WARN]${NC} $*"; }
fail() { echo -e "${RED}[ERROR]${NC} $*" >&2; exit 1; }
ok()   { echo -e "${GREEN}[OK]${NC} $*"; }
step() { echo; echo -e "${BLUE}==== $* ====${NC}"; }
trap 'echo -e "${RED}[ERROR]${NC} failed at line $LINENO: $BASH_COMMAND" >&2' ERR

GO_MIN="1.24"
GO_VERSION="1.24.7"
ASSUME_YES=false
MODE=system
DO_SETUP=true
DO_SERVICE=true
LINGER=false
SETUP_ARGS=()

while [ $# -gt 0 ]; do
    case "$1" in
        -y|--yes) ASSUME_YES=true ;;
        --user-service) MODE=user ;;
        --no-setup) DO_SETUP=false ;;
        --no-service) DO_SERVICE=false ;;
        --linger) LINGER=true ;;
        --go-version) GO_VERSION="${2:?--go-version needs a value}"; shift ;;
        --) shift; SETUP_ARGS=("$@"); break ;;
        -h|--help) sed -n '2,36p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
        *) fail "unknown option $1 (see --help)" ;;
    esac
    shift
done

PEER_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
USER_CFG_DIR="${XDG_CONFIG_HOME:-$HOME/.config}/orchestrator"
USER_CFG="$USER_CFG_DIR/config.yaml"

SVC_USER=orchestrator
STATE_DIR=/var/lib/orchestrator
PEER_ID_FILE=/run/orchestrator/peer-id
if [ "$MODE" = system ]; then
    BIN=/usr/local/bin/orchestrator
    AGENT_CFG="$STATE_DIR/config.yaml"
else
    BIN="$HOME/.local/bin/orchestrator"
    AGENT_CFG="$USER_CFG"
fi

# ---------------------------------------------------------------- checks

[ "$EUID" -ne 0 ] || fail "run this as your normal user, not root (it uses sudo when needed)"
[ -f "$PEER_DIR/go.mod" ] && [ -d "$PEER_DIR/../server/proto" ] \
    || fail "run this from a checkout of the repository (peer/ and server/ are both needed)"
command -v sudo >/dev/null || fail "sudo is required"
command -v systemctl >/dev/null || fail "systemd is required"

if [ -r /etc/os-release ]; then
    . /etc/os-release
    if [ "${ID:-}" != "ubuntu" ]; then
        warn "this installer targets Ubuntu 24.04; detected ${PRETTY_NAME:-unknown}. Continuing anyway."
    elif [ "${VERSION_ID:-}" != "24.04" ]; then
        warn "tested on Ubuntu 24.04; detected $PRETTY_NAME. Continuing anyway."
    fi
fi

case "$(uname -m)" in
    x86_64) GOARCH=amd64 ;;
    aarch64|arm64) GOARCH=arm64 ;;
    *) fail "unsupported CPU architecture $(uname -m) (need x86_64 or arm64)" ;;
esac

info "Orchestrator peer installer: $PEER_DIR (agent mode: $MODE)"
if ! $ASSUME_YES; then
    echo "This installs build tools with apt, Go $GO_VERSION (if needed) under /usr/local/go and"
    if [ "$MODE" = system ]; then
        echo "orchestrator in $BIN, creates the '$SVC_USER' system account and the"
        echo "orchestrator-agent system service (data in $STATE_DIR)."
    else
        echo "orchestrator in $BIN and a systemd user service running as $USER."
    fi
    read -r -p "Continue? (yes/no): " REPLY
    [[ "$REPLY" =~ ^[Yy]([Ee][Ss])?$ ]] || fail "cancelled"
fi

# Read a top-level value from a config file (sudo for the agent's private config).
cfgval() {
    local file=$1 key=$2
    if [ "$file" = "$AGENT_CFG" ] && [ "$MODE" = system ]; then
        sudo test -f "$file" && sudo sed -n "s/^$key: *//p" "$file" | tr -d '"' | head -1 || true
    else
        [ -f "$file" ] && sed -n "s/^$key: *//p" "$file" | tr -d '"' | head -1 || true
    fi
}

# ---------------------------------------------------------------- 1. packages

step "1/8 System packages"
sudo apt-get update -qq
sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -qq \
    ca-certificates curl git make jq tar netcat-openbsd >/dev/null
ok "build dependencies installed"

# ---------------------------------------------------------------- 2. Go

step "2/8 Go toolchain"
# version_ge A B: true if version A >= version B (e.g. 1.24.7 >= 1.24)
version_ge() { [ "$(printf '%s\n%s\n' "$2" "$1" | sort -V | head -1)" = "$2" ]; }

GO=""
for candidate in "$(command -v go 2>/dev/null || true)" /usr/local/go/bin/go; do
    [ -n "$candidate" ] && [ -x "$candidate" ] || continue
    v=$("$candidate" env GOVERSION 2>/dev/null | sed 's/^go//')
    if [ -n "$v" ] && version_ge "$v" "$GO_MIN"; then
        GO="$candidate"
        ok "using Go $v at $candidate"
        break
    fi
    [ -n "$v" ] && info "Go $v at $candidate is older than $GO_MIN"
done

if [ -z "$GO" ]; then
    info "installing Go $GO_VERSION from go.dev"
    FILE="go${GO_VERSION}.linux-${GOARCH}.tar.gz"
    TMP=$(mktemp -d)
    trap 'rm -rf "$TMP"' EXIT
    # The official checksum, from go.dev's release index.
    SUM=$(curl -fsSL "https://go.dev/dl/?mode=json&include=all" \
        | jq -r --arg f "$FILE" '.[].files[] | select(.filename == $f) | .sha256' | head -1)
    [ -n "$SUM" ] && [ "$SUM" != "null" ] || fail "Go $GO_VERSION for $GOARCH not found on go.dev"
    curl -fsSL -o "$TMP/$FILE" "https://go.dev/dl/$FILE"
    echo "$SUM  $TMP/$FILE" | sha256sum -c --quiet - || fail "checksum mismatch for $FILE; not installing"
    sudo rm -rf /usr/local/go
    sudo tar -C /usr/local -xzf "$TMP/$FILE"
    # Put Go on PATH for future login shells.
    echo 'export PATH=$PATH:/usr/local/go/bin' | sudo tee /etc/profile.d/go.sh >/dev/null
    GO=/usr/local/go/bin/go
    ok "installed $("$GO" version)"
fi
GO_BIN_DIR="$(dirname "$GO")"
export PATH="$GO_BIN_DIR:$PATH"

# ---------------------------------------------------------------- 3. build

step "3/8 Build and install orchestrator"
cd "$PEER_DIR"
info "downloading Go modules and building (first run takes a few minutes)"
make build >/dev/null
if [ "$MODE" = system ]; then
    sudo install -D -m 0755 -o root -g root bin/orchestrator "$BIN"
else
    install -D -m 0755 bin/orchestrator "$BIN"
    case ":$PATH:" in
        *":$HOME/.local/bin:"*) ;;
        *) warn "$HOME/.local/bin is not on your PATH yet; open a new terminal" ;;
    esac
fi
ok "installed $("$BIN" version) to $BIN"

# ---------------------------------------------------------------- 4. agent account and config

step "4/8 Agent account and configuration"
if [ "$MODE" = user ]; then
    if [ -f "$AGENT_CFG" ] && [ ${#SETUP_ARGS[@]} -eq 0 ]; then
        ok "using existing config $AGENT_CFG"
    elif $DO_SETUP; then
        "$BIN" --config "$AGENT_CFG" setup "${SETUP_ARGS[@]}"
    else
        warn "skipping setup; run \`orchestrator setup\` before starting the agent"
    fi
else
    if ! getent passwd "$SVC_USER" >/dev/null; then
        sudo useradd --system --user-group --home-dir "$STATE_DIR" --no-create-home \
            --shell /usr/sbin/nologin --comment "Orchestrator storage agent" "$SVC_USER"
        ok "created system account '$SVC_USER' (no login, no home)"
    else
        ok "system account '$SVC_USER' exists"
    fi
    sudo install -d -m 0700 -o "$SVC_USER" -g "$SVC_USER" "$STATE_DIR"

    # Carry over an earlier per-user agent: same identity (so the network
    # still knows this peer), same peer ID and the shards it holds.
    MIGRATE_ARGS=()
    if [ -f "$USER_CFG_DIR/identity.key" ] && ! sudo test -f "$STATE_DIR/identity.key"; then
        info "found an agent installed for $USER; moving it to the '$SVC_USER' account"
        if systemctl --user is-enabled orchestrator-agent >/dev/null 2>&1 || \
           systemctl --user is-active orchestrator-agent >/dev/null 2>&1; then
            systemctl --user disable --now orchestrator-agent >/dev/null 2>&1 || true
            rm -f "${XDG_CONFIG_HOME:-$HOME/.config}/systemd/user/orchestrator-agent.service"
            systemctl --user daemon-reload 2>/dev/null || true
            ok "stopped and removed the user service"
        fi
        sudo install -m 0600 -o "$SVC_USER" -g "$SVC_USER" "$USER_CFG_DIR/identity.key" "$STATE_DIR/identity.key"
        [ -f "$USER_CFG_DIR/state.json" ] && \
            sudo install -m 0600 -o "$SVC_USER" -g "$SVC_USER" "$USER_CFG_DIR/state.json" "$STATE_DIR/state.json"
        OLD_STORAGE=$(cfgval "$USER_CFG" storage_dir)
        if [ -n "$OLD_STORAGE" ] && [ -d "$OLD_STORAGE/shards" ]; then
            sudo mkdir -p "$STATE_DIR/storage"
            sudo cp -a "$OLD_STORAGE/shards" "$STATE_DIR/storage/"
            sudo chown -R "$SVC_USER:$SVC_USER" "$STATE_DIR/storage"
            sudo chmod 0700 "$STATE_DIR/storage"
            rm -rf "$OLD_STORAGE/shards"
            ok "moved stored shards from $OLD_STORAGE"
        fi
        rm -f "$USER_CFG_DIR/identity.key" "$USER_CFG_DIR/state.json"
        for k in gateway_url:--gateway registry_addr:--registry relay_addr:--relay storage_gb:--storage-gb \
                 listen_port:--port region:--region asn:--asn; do
            v=$(cfgval "$USER_CFG" "${k%%:*}")
            [ -n "$v" ] && MIGRATE_ARGS+=("${k##*:}" "$v")
        done
        ok "identity and peer ID moved to $STATE_DIR"
    fi

    if sudo test -f "$AGENT_CFG" && [ ${#SETUP_ARGS[@]} -eq 0 ]; then
        ok "using existing agent config $AGENT_CFG"
    elif $DO_SETUP; then
        # Setup runs as root (so it can create a storage directory anywhere);
        # everything it writes is handed to the service account afterwards.
        INTERACTIVE=()
        if [ ${#MIGRATE_ARGS[@]} -gt 0 ] && [ ${#SETUP_ARGS[@]} -eq 0 ]; then
            INTERACTIVE=(--non-interactive)
        fi
        sudo env HOME="$STATE_DIR" "$BIN" --config "$AGENT_CFG" setup \
            --storage-dir "$STATE_DIR/storage" --peer-id-file "$PEER_ID_FILE" \
            "${INTERACTIVE[@]}" "${MIGRATE_ARGS[@]}" "${SETUP_ARGS[@]}"
    else
        warn "skipping setup; configure later with: sudo env HOME=$STATE_DIR $BIN --config $AGENT_CFG setup"
    fi

    if sudo test -f "$AGENT_CFG"; then
        STORAGE=$(cfgval "$AGENT_CFG" storage_dir)
        case "$STORAGE" in
            /home|/home/*|/root|/root/*|/run/user/*)
                fail "storage_dir $STORAGE is inside a home directory, which the sandboxed agent cannot access; choose e.g. $STATE_DIR/storage or a path on another disk" ;;
        esac
        sudo chown -R "$SVC_USER:$SVC_USER" "$STATE_DIR"
        sudo chown -R "$SVC_USER:$SVC_USER" "$STORAGE"
        sudo chmod -R go-rwx "$STORAGE"
        DROPIN=/etc/systemd/system/orchestrator-agent.service.d/storage.conf
        case "$STORAGE" in
            "$STATE_DIR"|"$STATE_DIR"/*) sudo rm -f "$DROPIN" ;;
            *)
                # The sandbox makes the filesystem read-only except listed paths.
                sudo install -d -m 0755 "$(dirname "$DROPIN")"
                printf '[Service]\nReadWritePaths=%s\n' "$STORAGE" | sudo tee "$DROPIN" >/dev/null
                ok "allowed the agent to write $STORAGE"
                ;;
        esac
        ok "agent files owned by '$SVC_USER' (storage: $STORAGE)"
    fi
fi

# Values for the remaining steps.
PORT=$(cfgval "$AGENT_CFG" listen_port); PORT=${PORT:-4001}
GATEWAY=$(cfgval "$AGENT_CFG" gateway_url)
REGISTRY=$(cfgval "$AGENT_CFG" registry_addr)
RELAY=$(cfgval "$AGENT_CFG" relay_addr)

# ---------------------------------------------------------------- 5. client config

step "5/8 Your client settings"
if [ "$MODE" = user ]; then
    ok "client and agent share $USER_CFG"
elif [ -f "$USER_CFG" ]; then
    ok "using existing $USER_CFG"
elif [ -n "$GATEWAY" ] && [ -n "$REGISTRY" ]; then
    CLIENT_ARGS=(--client-only --non-interactive --gateway "$GATEWAY" --registry "$REGISTRY")
    [ -n "$RELAY" ] && CLIENT_ARGS+=(--relay "$RELAY")
    "$BIN" --config "$USER_CFG" setup "${CLIENT_ARGS[@]}" >/dev/null
    ok "wrote $USER_CFG (server addresses for your uploads and downloads)"
else
    warn "no agent config to copy server addresses from; run \`orchestrator setup --client-only\` later"
fi

# ---------------------------------------------------------------- 6. firewall

step "6/8 Firewall"
if command -v ufw >/dev/null && sudo ufw status 2>/dev/null | grep -q '^Status: active'; then
    if [ "$PORT" != "0" ]; then
        sudo ufw allow "$PORT/tcp" comment 'orchestrator libp2p' >/dev/null
        sudo ufw allow "$PORT/udp" comment 'orchestrator libp2p (QUIC)' >/dev/null
        ok "ufw: allowed $PORT/tcp and $PORT/udp"
    else
        warn "listen_port is 0 (random); open a fixed port in ufw if peers cannot reach you"
    fi
else
    ok "ufw is not active; nothing to open"
fi
info "for peers outside your network, forward port $PORT (TCP and UDP) on your router,"
info "or rely on UPnP/NAT-PMP and the relay"

# ---------------------------------------------------------------- 7. service

step "7/8 Service"
if ! $DO_SERVICE; then
    warn "skipped"
elif [ "$MODE" = system ]; then
    if ! sudo test -f "$AGENT_CFG"; then
        warn "no agent config yet, so the service is not started"
    else
        sudo install -m 0644 packaging/orchestrator-agent-system.service /etc/systemd/system/orchestrator-agent.service
        sudo systemctl daemon-reload
        sudo systemctl enable orchestrator-agent >/dev/null 2>&1
        sudo systemctl restart orchestrator-agent
        sleep 3
        if systemctl is-active --quiet orchestrator-agent; then
            ok "orchestrator-agent is running as '$SVC_USER' (starts at boot)"
        else
            warn "the service did not stay up; recent log:"
            sudo journalctl -u orchestrator-agent -n 20 --no-pager || true
        fi
    fi
else
    if [ ! -f "$AGENT_CFG" ]; then
        warn "no config yet, so the service is not started"
    elif ! systemctl --user show-environment >/dev/null 2>&1; then
        warn "no systemd user session (e.g. over plain SSH); log in to the desktop and run \`make install-user-service\`"
    else
        UNIT_DIR="${XDG_CONFIG_HOME:-$HOME/.config}/systemd/user"
        install -D -m 0644 packaging/orchestrator-agent.service "$UNIT_DIR/orchestrator-agent.service"
        systemctl --user daemon-reload
        systemctl --user enable orchestrator-agent >/dev/null 2>&1
        systemctl --user restart orchestrator-agent
        sleep 3
        if systemctl --user is-active --quiet orchestrator-agent; then
            ok "orchestrator-agent is running as $USER"
        else
            warn "the service did not stay up; recent log:"
            journalctl --user -u orchestrator-agent -n 20 --no-pager || true
        fi
        if $LINGER; then
            sudo loginctl enable-linger "$USER"
            ok "agent keeps running while you are logged out"
        else
            info "to keep it running while logged out: sudo loginctl enable-linger $USER (or re-run with --linger)"
        fi
    fi
fi

# ---------------------------------------------------------------- 8. checks

step "8/8 Server connectivity"
if [ -n "$GATEWAY" ]; then
    if curl -fs --max-time 5 "$GATEWAY/health" >/dev/null; then
        ok "gateway reachable at $GATEWAY"
    else
        warn "gateway $GATEWAY is not answering; check the address and that the server is up"
    fi
fi
if [ -n "$REGISTRY" ]; then
    if nc -z -w 5 "${REGISTRY%:*}" "${REGISTRY##*:}"; then
        ok "peer registry reachable at $REGISTRY"
    else
        warn "peer registry $REGISTRY is not reachable"
    fi
fi

echo
ok "done. Next: \`orchestrator login --register\`, then \`orchestrator upload <file>\`"
if [ "$MODE" = system ]; then
    info "agent logs:   sudo journalctl -u orchestrator-agent -f"
    info "agent status: sudo -u $SVC_USER $BIN --config $AGENT_CFG status"
else
    info "agent logs:   journalctl --user -u orchestrator-agent -f"
fi
