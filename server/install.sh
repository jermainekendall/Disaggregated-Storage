#!/bin/bash
#
# TEST DEPLOYMENT installer: Distributed Storage Orchestrator
# Ubuntu 24.04 LTS, single node.
#
# Installs Docker, containerd and Kubernetes (kubeadm, single node, Flannel),
# CockroachDB (insecure, 1 node), Redis, NATS and monitoring; applies the
# database schema; builds the 10 service images and imports them into
# containerd; creates the JWT, relay-identity and admin console secrets; deploys everything;
# and checks that it came up.
#
# Run from the server/ directory:   sudo ./install.sh        (asks to confirm)
#                                   sudo ./install.sh -y     (no prompt)
# Re-running is safe: finished steps are skipped or re-applied idempotently.
#
# Rough requirements for the test cluster: 4 CPU cores, 8 GB RAM, 40 GB disk.
# This is not a production setup (no TLS, insecure database, simulated payments).

set -Eeuo pipefail

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; BLUE='\033[0;34m'; NC='\033[0m'
log_info()    { echo -e "${BLUE}[INFO]${NC} $*"; }
log_warn()    { echo -e "${YELLOW}[WARN]${NC} $*"; }
log_error()   { echo -e "${RED}[ERROR]${NC} $*"; }
log_success() { echo -e "${GREEN}[OK]${NC} $*"; }
log_step()    { echo; echo -e "${BLUE}==== $* ====${NC}"; }

trap 'log_error "failed at line $LINENO: $BASH_COMMAND (see $LOG_FILE)"' ERR

ASSUME_YES=false
[[ "${1:-}" == "-y" || "${1:-}" == "--yes" ]] && ASSUME_YES=true

if [ "$EUID" -ne 0 ]; then
    log_error "Please run as root: sudo ./install.sh"
    exit 1
fi

WORK_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CURRENT_USER="${SUDO_USER:-$(logname 2>/dev/null || echo root)}"
USER_HOME="$(getent passwd "$CURRENT_USER" | cut -d: -f6)"
LOG_FILE="/var/log/orchestrator-install.log"
NS=orchestrator
IMAGE_TAG=dev
SERVICES=(api-gateway admin-console peer-registry placement-service billing-service audit-service key-metadata
          repair-worker replication-worker analytics-worker relay-service)

exec > >(tee -a "$LOG_FILE") 2>&1

log_info "Distributed Storage Orchestrator - TEST deployment installer"
log_info "Date: $(date)   User: $CURRENT_USER   Source: $WORK_DIR   Log: $LOG_FILE"

if ! $ASSUME_YES; then
    echo "This will install Docker and Kubernetes on this machine and deploy the orchestrator."
    read -r -p "Continue? (yes/no): " REPLY
    [[ "$REPLY" =~ ^[Yy]([Ee][Ss])?$ ]] || { log_error "Cancelled"; exit 1; }
fi

START_TIME=$(date +%s)
k() { kubectl "$@"; }

# Wait until a resource exists (kubectl wait fails immediately if it does not).
wait_exists() {
    local what=$1 ns=$2 tries=${3:-60}
    for _ in $(seq "$tries"); do
        k get "$what" -n "$ns" >/dev/null 2>&1 && return 0
        sleep 5
    done
    log_error "$what did not appear in namespace $ns"
    return 1
}

# ============================================================================
log_step "1/8 System preparation"
# ============================================================================

apt-get update -qq
DEBIAN_FRONTEND=noninteractive apt-get install -y -qq \
    apt-transport-https ca-certificates curl gnupg lsb-release git jq netcat-openbsd

# The kubelet will not start while swap is on. Disable it now and on reboot.
# fstab fields may be separated by tabs (Ubuntu 24.04's /swap.img line is).
swapoff -a
sed -i -E '/^[^#].*[[:space:]]swap[[:space:]]/ s/^/#/' /etc/fstab
systemctl mask --now swap.target >/dev/null 2>&1 || true

cat > /etc/modules-load.d/k8s.conf <<EOF
overlay
br_netfilter
EOF
modprobe overlay
modprobe br_netfilter
cat > /etc/sysctl.d/k8s.conf <<EOF
net.bridge.bridge-nf-call-iptables  = 1
net.bridge.bridge-nf-call-ip6tables = 1
net.ipv4.ip_forward                 = 1
EOF
sysctl --system >/dev/null
log_success "System configured"

# ============================================================================
log_step "2/8 Docker and containerd"
# ============================================================================

if ! command -v docker >/dev/null; then
    install -m 0755 -d /etc/apt/keyrings
    curl -fsSL https://download.docker.com/linux/ubuntu/gpg | gpg --dearmor --yes -o /etc/apt/keyrings/docker.gpg
    chmod a+r /etc/apt/keyrings/docker.gpg
    echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu $(. /etc/os-release && echo "$VERSION_CODENAME") stable" \
        > /etc/apt/sources.list.d/docker.list
    apt-get update -qq
    DEBIAN_FRONTEND=noninteractive apt-get install -y -qq docker-ce docker-ce-cli containerd.io docker-buildx-plugin
fi

# Kubernetes needs containerd's CRI plugin with the systemd cgroup driver.
if ! grep -q 'SystemdCgroup = true' /etc/containerd/config.toml 2>/dev/null; then
    containerd config default > /etc/containerd/config.toml
    sed -i 's/SystemdCgroup = false/SystemdCgroup = true/' /etc/containerd/config.toml
    systemctl restart containerd
fi
systemctl enable --now docker >/dev/null
[ "$CURRENT_USER" != root ] && usermod -aG docker "$CURRENT_USER" || true
log_success "$(docker --version)"

# ============================================================================
log_step "3/8 Kubernetes (single node)"
# ============================================================================

if ! command -v kubeadm >/dev/null; then
    curl -fsSL https://pkgs.k8s.io/core:/stable:/v1.35/deb/Release.key | gpg --dearmor --yes -o /etc/apt/keyrings/kubernetes-apt-keyring.gpg
    echo 'deb [signed-by=/etc/apt/keyrings/kubernetes-apt-keyring.gpg] https://pkgs.k8s.io/core:/stable:/v1.35/deb/ /' \
        > /etc/apt/sources.list.d/kubernetes.list
    apt-get update -qq
    DEBIAN_FRONTEND=noninteractive apt-get install -y -qq kubelet kubeadm kubectl
    apt-mark hold kubelet kubeadm kubectl >/dev/null
fi

NODE_IP=$(hostname -I | awk '{print $1}')
if [ ! -f /etc/kubernetes/admin.conf ]; then
    kubeadm init --pod-network-cidr=10.244.0.0/16 --apiserver-advertise-address="$NODE_IP" \
        > /tmp/kubeadm-init.log 2>&1 || { cat /tmp/kubeadm-init.log; exit 1; }
else
    # Existing cluster: its certificates and config are tied to the IP it was
    # created with. If the machine's address changed (DHCP), the API server
    # cannot come back up on its own.
    API_IP=$(grep -m1 'server:' /etc/kubernetes/admin.conf | sed -E 's#.*https://([^:]+):.*#\1#')
    if ! hostname -I | tr ' ' '\n' | grep -qx "$API_IP"; then
        log_error "This cluster was created for IP $API_IP, but this machine now has: $(hostname -I)"
        log_error "Either give the machine its old address back (a DHCP reservation for $API_IP"
        log_error "is the easiest), or reset the cluster and re-run this script:"
        log_error "  sudo kubeadm reset -f && sudo rm -rf /etc/cni/net.d \$HOME/.kube && sudo ./install.sh -y"
        exit 1
    fi
    # After a reboot the kubelet may be in restart back-off (e.g. it found
    # swap on); restart it now that swap is off so the control plane returns.
    systemctl restart kubelet
fi
mkdir -p /root/.kube
cp -f /etc/kubernetes/admin.conf /root/.kube/config
if [ "$CURRENT_USER" != root ]; then
    mkdir -p "$USER_HOME/.kube"
    cp -f /etc/kubernetes/admin.conf "$USER_HOME/.kube/config"
    chown -R "$CURRENT_USER:$CURRENT_USER" "$USER_HOME/.kube"
fi
export KUBECONFIG=/root/.kube/config

# Wait for the API server before using it (it takes a while after kubeadm
# init, a kubelet restart or a reboot).
log_info "Waiting for the Kubernetes API server..."
for i in $(seq 60); do
    k get --raw=/readyz >/dev/null 2>&1 && break
    if [ "$i" -eq 60 ]; then
        log_error "The API server at $(grep -m1 'server:' /etc/kubernetes/admin.conf | awk '{print $2}') is not answering. Diagnostics:"
        swapon --show || true
        systemctl --no-pager --lines=0 status kubelet || true
        journalctl -u kubelet --no-pager -n 25 || true
        crictl --runtime-endpoint unix:///run/containerd/containerd.sock ps -a 2>/dev/null | grep -E 'kube-apiserver|etcd' || true
        exit 1
    fi
    sleep 5
done
log_success "API server is up"

# Single node: allow workloads on the control plane.
k taint nodes --all node-role.kubernetes.io/control-plane- >/dev/null 2>&1 || true

k apply -f https://github.com/flannel-io/flannel/releases/download/v0.26.1/kube-flannel.yml >/dev/null
wait_exists daemonset/kube-flannel-ds kube-flannel
k rollout status daemonset/kube-flannel-ds -n kube-flannel --timeout=180s
k wait --for=condition=Ready node --all --timeout=180s
log_success "Kubernetes node ready ($NODE_IP)"

# ============================================================================
log_step "4/8 Infrastructure: storage, namespace, CockroachDB, Redis, NATS, monitoring"
# ============================================================================

k apply -f "$WORK_DIR/infrastructure/00-storage.yaml" >/dev/null
k wait --namespace local-path-storage --for=condition=Ready pod --all --timeout=180s
k apply -f "$WORK_DIR/infrastructure/00-namespace.yaml" >/dev/null

# Earlier versions used an init Job that could never finish; remove any left over.
k delete job cockroachdb-init -n "$NS" --ignore-not-found >/dev/null
k apply -f "$WORK_DIR/infrastructure/01-cockroachdb.yaml" >/dev/null
# A StatefulSet never replaces a pod that is not Ready, so a pod from an
# older, broken spec would block the rollout forever. Delete it if it is not
# on the current revision; the StatefulSet recreates it from the new spec.
# (The data volume is kept.)
sleep 2
WANT_REV=$(k get statefulset cockroachdb -n "$NS" -o jsonpath='{.status.updateRevision}')
HAVE_REV=$(k get pod cockroachdb-0 -n "$NS" -o jsonpath='{.metadata.labels.controller-revision-hash}' 2>/dev/null || true)
if [ -n "$HAVE_REV" ] && [ -n "$WANT_REV" ] && [ "$HAVE_REV" != "$WANT_REV" ]; then
    log_info "Replacing cockroachdb-0 (revision $HAVE_REV) with the current spec ($WANT_REV)"
    k delete pod cockroachdb-0 -n "$NS" --wait=true >/dev/null
fi
# start-single-node initializes itself; the rollout finishes once the pod is Ready.
if ! k rollout status statefulset/cockroachdb -n "$NS" --timeout=600s; then
    log_error "CockroachDB did not become ready. Diagnostics:"
    k get pod,pvc -n "$NS" -l app=cockroachdb -o wide || true
    k get pvc -n "$NS" || true
    k describe pod cockroachdb-0 -n "$NS" | sed -n '/^Events:/,$p' || true
    k logs cockroachdb-0 -n "$NS" --tail=40 || true
    exit 1
fi
log_success "CockroachDB ready"

for f in 02-redis 03-nats; do
    k apply -f "$WORK_DIR/infrastructure/$f.yaml" >/dev/null
done
k create namespace monitoring --dry-run=client -o yaml | k apply -f - >/dev/null
k apply -f "$WORK_DIR/infrastructure/04-monitoring.yaml" >/dev/null
log_success "Redis, NATS and monitoring applied (not used by the services yet)"

# ============================================================================
log_step "5/8 Database schema"
# ============================================================================

crdb_sql() { k exec -i cockroachdb-0 -n "$NS" -- ./cockroach sql --insecure --host=localhost "$@"; }
crdb_sql -e "CREATE DATABASE IF NOT EXISTS orchestrator;"
# schema.sql is idempotent and upgrades older schemas in place. Errors stop the install.
crdb_sql --database=orchestrator < "$WORK_DIR/infrastructure/schema.sql" >/dev/null
TABLES=$(crdb_sql --database=orchestrator --format=csv -e \
    "SELECT count(*) FROM information_schema.tables WHERE table_schema='public' AND table_type='BASE TABLE';" | tail -1)
log_success "Schema applied ($TABLES tables)"

# ============================================================================
log_step "6/8 Build service images"
# ============================================================================

# Images are built with Docker, then imported into containerd, which is what
# the kubelet uses. The manifests use imagePullPolicy: Never, so no registry
# is needed.
for svc in "${SERVICES[@]}"; do
    log_info "Building $svc"
    docker build -q --build-arg SERVICE="$svc" -t "orchestrator/$svc:$IMAGE_TAG" "$WORK_DIR" >/dev/null
    docker save "orchestrator/$svc:$IMAGE_TAG" | ctr -n k8s.io images import - >/dev/null
done
log_success "Built and imported ${#SERVICES[@]} images"

# ============================================================================
log_step "7/8 Secrets and deployment"
# ============================================================================

if ! k get secret gateway-secrets -n "$NS" >/dev/null 2>&1; then
    k create secret generic gateway-secrets -n "$NS" --from-literal=JWT_SECRET="$(openssl rand -hex 32)" >/dev/null
    log_success "Created gateway-secrets (random JWT_SECRET)"
fi
ADMIN_CREDS=/root/orchestrator-admin-credentials.txt
if ! k get secret admin-console-secrets -n "$NS" >/dev/null 2>&1; then
    # The first admin account. The password is never printed to the install
    # log; it is saved to a root-only file instead. Set ADMIN_EMAIL before
    # running the installer to choose the address.
    ADMIN_EMAIL="${ADMIN_EMAIL:-admin@orchestrator.local}"
    ADMIN_PASSWORD="$(openssl rand -base64 18 | tr -d '/+=')"
    k create secret generic admin-console-secrets -n "$NS" \
        --from-literal=SESSION_SECRET="$(openssl rand -hex 32)" \
        --from-literal=ADMIN_BOOTSTRAP_EMAIL="$ADMIN_EMAIL" \
        --from-literal=ADMIN_BOOTSTRAP_PASSWORD="$ADMIN_PASSWORD" >/dev/null
    ( umask 077; printf 'Orchestrator admin console\nURL:      http://%s:30088\nEmail:    %s\nPassword: %s\n' \
        "$NODE_IP" "$ADMIN_EMAIL" "$ADMIN_PASSWORD" > "$ADMIN_CREDS" )
    unset ADMIN_PASSWORD
    log_success "Created admin-console-secrets (admin sign-in saved to $ADMIN_CREDS)"
fi
if ! k get secret relay-identity -n "$NS" >/dev/null 2>&1; then
    # A stable relay peer ID: peers store relay addresses that include it.
    KEY=$(docker run --rm "orchestrator/relay-service:$IMAGE_TAG" -genkey)
    k create secret generic relay-identity -n "$NS" --from-literal=relay.key="$KEY" >/dev/null
    log_success "Created relay-identity"
fi

k apply -f "$WORK_DIR/deployments/" >/dev/null
k patch configmap relay-config -n "$NS" --type merge \
    -p "{\"data\":{\"ANNOUNCE_ADDRS\":\"/ip4/$NODE_IP/tcp/30001\"}}" >/dev/null
# Restart so pods pick up rebuilt images and config on a re-run.
k rollout restart deployment -n "$NS" >/dev/null

DEPLOYMENTS=$(k get deployments -n "$NS" -o name)
for d in $DEPLOYMENTS; do
    k rollout status "$d" -n "$NS" --timeout=300s
done
log_success "All deployments rolled out"

# ============================================================================
log_step "8/8 Validation"
# ============================================================================

if curl -fsS "http://$NODE_IP:30080/ready" >/dev/null; then
    log_success "API gateway ready at http://$NODE_IP:30080"
else
    log_error "API gateway is not answering on http://$NODE_IP:30080/ready"
    exit 1
fi
if nc -z "$NODE_IP" 30051; then log_success "Peer registry (gRPC) on $NODE_IP:30051"; fi
if curl -fsS "http://$NODE_IP:30088/healthz" >/dev/null; then log_success "Admin console at http://$NODE_IP:30088"; fi
if nc -z "$NODE_IP" 30001; then log_success "libp2p relay on $NODE_IP:30001"; fi
RELAY_ID=$(k logs deployment/libp2p-relay -n "$NS" | grep -o 'relay 12D3[A-Za-z0-9]*' | head -1 | cut -d' ' -f2 || true)

DURATION=$(( $(date +%s) - START_TIME ))
echo
log_success "INSTALLATION COMPLETE in $((DURATION / 60))m $((DURATION % 60))s"
cat <<EOF

Endpoints (test deployment, no TLS):
  API gateway (HTTP)     http://$NODE_IP:30080        e.g. curl http://$NODE_IP:30080/health
  Peer registry (gRPC)   $NODE_IP:30051
  Admin console          http://$NODE_IP:30088   sign-in: sudo cat $ADMIN_CREDS
  libp2p relay           /ip4/$NODE_IP/tcp/30001/p2p/${RELAY_ID:-<see relay logs>}

Useful commands:
  kubectl get pods -n $NS
  kubectl logs -f deployment/<service> -n $NS
  kubectl exec -it cockroachdb-0 -n $NS -- ./cockroach sql --insecure --database=orchestrator

Rebuild and redeploy after code changes:  sudo ./install.sh -y
Install log: $LOG_FILE
EOF
