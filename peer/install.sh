#!/bin/bash
#
# Installer for the Orchestrator peer agent and client on Ubuntu 24.04 desktop.
#
# Run it as your normal user from the peer/ directory (it uses sudo for the
# system steps). It:
#   1. installs build dependencies with apt (git, make, curl, jq, ...)
#   2. installs Go 1.24 from go.dev if the installed Go is missing or too old
#      (Ubuntu 24.04's packaged Go is 1.22), verifying the published SHA-256
#   3. builds `orchestrator` and installs it to ~/.local/bin
#   4. runs `orchestrator setup` if there is no config yet
#   5. opens the agent's libp2p port (TCP and UDP) if the ufw firewall is active
#   6. installs and starts the systemd user service
#   7. checks that the server's gateway and registry are reachable
#
# Usage:  ./install.sh [options] [-- setup flags]
#   -y, --yes        don't ask for confirmation
#   --no-setup       skip `orchestrator setup` (configure later)
#   --no-service     don't install the systemd user service
#   --linger         keep the agent running while you are logged out
#   --go-version V   Go version to install if needed (default 1.24.7)
# Anything after `--` is passed to `orchestrator setup`, e.g.
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
DO_SETUP=true
DO_SERVICE=true
LINGER=false
SETUP_ARGS=()

while [ $# -gt 0 ]; do
    case "$1" in
        -y|--yes) ASSUME_YES=true ;;
        --no-setup) DO_SETUP=false ;;
        --no-service) DO_SERVICE=false ;;
        --linger) LINGER=true ;;
        --go-version) GO_VERSION="${2:?--go-version needs a value}"; shift ;;
        --) shift; SETUP_ARGS=("$@"); break ;;
        -h|--help) sed -n '2,30p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
        *) fail "unknown option $1 (see --help)" ;;
    esac
    shift
done

PEER_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BIN_DIR="$HOME/.local/bin"
BIN="$BIN_DIR/orchestrator"
CONFIG="${XDG_CONFIG_HOME:-$HOME/.config}/orchestrator/config.yaml"

# ---------------------------------------------------------------- checks

[ "$EUID" -ne 0 ] || fail "run this as your normal user, not root (it uses sudo when needed); the agent runs as a user service"
[ -f "$PEER_DIR/go.mod" ] && [ -d "$PEER_DIR/../server/proto" ] \
    || fail "run this from a checkout of the repository (peer/ and server/ are both needed)"
command -v sudo >/dev/null || fail "sudo is required"

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

info "Orchestrator peer installer: $PEER_DIR"
if ! $ASSUME_YES; then
    echo "This installs build tools with apt, Go $GO_VERSION (if needed) under /usr/local/go,"
    echo "the orchestrator binary in $BIN, and a systemd user service."
    read -r -p "Continue? (yes/no): " REPLY
    [[ "$REPLY" =~ ^[Yy]([Ee][Ss])?$ ]] || fail "cancelled"
fi

# ---------------------------------------------------------------- 1. packages

step "1/7 System packages"
sudo apt-get update -qq
sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -qq \
    ca-certificates curl git make jq tar netcat-openbsd >/dev/null
ok "build dependencies installed"

# ---------------------------------------------------------------- 2. Go

step "2/7 Go toolchain"
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

step "3/7 Build and install orchestrator"
cd "$PEER_DIR"
info "downloading Go modules and building (first run takes a few minutes)"
make build >/dev/null
install -D -m 0755 bin/orchestrator "$BIN"
ok "installed $("$BIN" version) to $BIN"
case ":$PATH:" in
    *":$BIN_DIR:"*) ;;
    *) warn "$BIN_DIR is not on your PATH yet; open a new terminal (Ubuntu adds it at login once it exists)" ;;
esac

# ---------------------------------------------------------------- 4. setup

step "4/7 Configuration"
if [ -f "$CONFIG" ] && [ ${#SETUP_ARGS[@]} -eq 0 ]; then
    ok "using existing config $CONFIG (run \`orchestrator setup\` to change it)"
elif $DO_SETUP; then
    "$BIN" setup "${SETUP_ARGS[@]}"
else
    warn "skipping setup; run \`orchestrator setup\` before starting the agent"
fi

# Values for the remaining steps, read from the config if there is one.
cfg() { [ -f "$CONFIG" ] && sed -n "s/^$1: *//p" "$CONFIG" | tr -d '"' | head -1 || true; }
PORT=$(cfg listen_port); PORT=${PORT:-4001}
GATEWAY=$(cfg gateway_url)
REGISTRY=$(cfg registry_addr)

# ---------------------------------------------------------------- 5. firewall

step "5/7 Firewall"
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

# ---------------------------------------------------------------- 6. service

step "6/7 systemd user service"
if ! $DO_SERVICE; then
    warn "skipped; run the agent with \`orchestrator run\`"
elif [ ! -f "$CONFIG" ]; then
    warn "no config yet, so the service is not started; run \`orchestrator setup\` then \`make install-service\`"
elif ! systemctl --user show-environment >/dev/null 2>&1; then
    warn "no systemd user session (e.g. over plain SSH); log in to the desktop and run \`make install-service\`"
else
    UNIT_DIR="${XDG_CONFIG_HOME:-$HOME/.config}/systemd/user"
    install -D -m 0644 packaging/orchestrator-agent.service "$UNIT_DIR/orchestrator-agent.service"
    systemctl --user daemon-reload
    systemctl --user enable orchestrator-agent >/dev/null 2>&1
    systemctl --user restart orchestrator-agent
    sleep 3
    if systemctl --user is-active --quiet orchestrator-agent; then
        ok "orchestrator-agent is running (logs: journalctl --user -u orchestrator-agent -f)"
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

# ---------------------------------------------------------------- 7. checks

step "7/7 Server connectivity"
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
