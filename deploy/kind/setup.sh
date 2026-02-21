#!/usr/bin/env bash
# ObjectIO Kind Cluster Setup
#
# Spins up a full ObjectIO cluster in Kind (Kubernetes in Docker):
#   - 4+2 EC (4 data + 2 parity shards)
#   - S3 API (gateway on localhost:9000 via port-forward)
#   - Iceberg REST Catalog (/iceberg/v1/)
#   - Delta Sharing (/delta-sharing/v1/)
#   - Prometheus + Grafana monitoring
#
# Prerequisites:
#   - kind        https://kind.sigs.k8s.io/docs/user/quick-start/
#   - kubectl     https://kubernetes.io/docs/tasks/tools/
#   - helm        https://helm.sh/docs/intro/install/
#   - docker
#
# Usage:
#   ./deploy/kind/setup.sh                  # create cluster + deploy
#   ./deploy/kind/setup.sh --skip-build     # skip docker build, use existing images
#   ./deploy/kind/setup.sh --registry ghcr.io/cloudomate  # pull from registry instead

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

CLUSTER_NAME="objectio"
NAMESPACE="objectio"
RELEASE="objectio"
REGISTRY=""
SKIP_BUILD=false
IMAGES=(objectio-gateway objectio-meta objectio-osd objectio-cli objectio-block-gateway)

# ── Parse args ────────────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
  case "$1" in
    --skip-build) SKIP_BUILD=true; shift ;;
    --registry)   REGISTRY="$2"; shift 2 ;;
    --teardown)
      kind delete cluster --name "${CLUSTER_NAME}" 2>/dev/null || true
      echo "Cluster '${CLUSTER_NAME}' deleted."
      exit 0
      ;;
    *) echo "Unknown option: $1"; exit 1 ;;
  esac
done

# ── Helpers ───────────────────────────────────────────────────────────────────
info()  { echo "▶ $*"; }
ok()    { echo "✓ $*"; }
die()   { echo "✗ $*" >&2; exit 1; }

check_prereqs() {
  for cmd in kind kubectl helm docker; do
    command -v "$cmd" &>/dev/null || die "Required tool not found: $cmd"
  done
}

# ── Create cluster ────────────────────────────────────────────────────────────
create_cluster() {
  if kind get clusters 2>/dev/null | grep -q "^${CLUSTER_NAME}$"; then
    info "Kind cluster '${CLUSTER_NAME}' already exists — skipping creation"
  else
    info "Creating Kind cluster '${CLUSTER_NAME}'..."
    kind create cluster \
      --name "${CLUSTER_NAME}" \
      --config "${SCRIPT_DIR}/kind-cluster.yaml"
    ok "Cluster created"
  fi
  kubectl config use-context "kind-${CLUSTER_NAME}"
}

# ── Build and load images ─────────────────────────────────────────────────────
build_and_load() {
  if [[ -n "${REGISTRY}" ]]; then
    info "Using registry '${REGISTRY}' — skipping local build"
    return
  fi

  if [[ "${SKIP_BUILD}" == "true" ]]; then
    info "Skipping build (--skip-build)"
  else
    info "Building Docker images (this takes a few minutes)..."
    docker build --target all -t objectio:latest "${REPO_ROOT}"
    # Extract individual images from the all-in-one build
    for img in "${IMAGES[@]}"; do
      docker build --target "${img#objectio-}" \
        -t "${img}:latest" \
        "${REPO_ROOT}" 2>/dev/null || \
      docker build --target "${img}" \
        -t "${img}:latest" \
        "${REPO_ROOT}"
    done
    ok "Images built"
  fi

  info "Loading images into Kind cluster..."
  for img in "${IMAGES[@]}"; do
    kind load docker-image "${img}:latest" --name "${CLUSTER_NAME}"
  done
  ok "Images loaded"
}

# ── Deploy Helm chart ─────────────────────────────────────────────────────────
deploy_helm() {
  kubectl create namespace "${NAMESPACE}" --dry-run=client -o yaml | kubectl apply -f -

  # If using a registry, create imagePullSecret if credentials are available
  if [[ -n "${REGISTRY}" ]]; then
    IMAGE_REGISTRY="${REGISTRY}"
    if [[ -n "${GITHUB_TOKEN:-}" ]]; then
      kubectl create secret docker-registry ghcr-secret \
        --namespace "${NAMESPACE}" \
        --docker-server=ghcr.io \
        --docker-username="${GITHUB_ACTOR:-objectio}" \
        --docker-password="${GITHUB_TOKEN}" \
        --dry-run=client -o yaml | kubectl apply -f -
    fi
  else
    IMAGE_REGISTRY="ghcr.io/cloudomate"   # placeholder — actual images are local
  fi

  info "Deploying ObjectIO Helm chart..."
  helm upgrade --install "${RELEASE}" \
    "${REPO_ROOT}/deploy/helm/objectio" \
    --namespace "${NAMESPACE}" \
    --values "${SCRIPT_DIR}/values.yaml" \
    --set "global.imageRegistry=${IMAGE_REGISTRY}" \
    --set "global.imagePullPolicy=$([ -n "${REGISTRY}" ] && echo Always || echo IfNotPresent)" \
    --wait=false \
    --timeout 10m

  ok "Helm chart installed"
}

# ── Wait for readiness ────────────────────────────────────────────────────────
wait_ready() {
  info "Waiting for meta StatefulSet (3 replicas)..."
  kubectl rollout status statefulset \
    -l app.kubernetes.io/component=meta \
    -n "${NAMESPACE}" --timeout=5m

  info "Waiting for OSD StatefulSet (6 replicas, may take a few minutes)..."
  kubectl rollout status statefulset \
    -l app.kubernetes.io/component=osd \
    -n "${NAMESPACE}" --timeout=10m

  info "Waiting for gateway Deployment..."
  kubectl rollout status deployment \
    -l app.kubernetes.io/component=gateway \
    -n "${NAMESPACE}" --timeout=3m

  ok "All components ready"
}

# ── Port-forward ──────────────────────────────────────────────────────────────
start_portforward() {
  # Kill any existing port-forwards
  pkill -f "kubectl port-forward.*${NAMESPACE}.*9000" 2>/dev/null || true
  pkill -f "kubectl port-forward.*${NAMESPACE}.*3000" 2>/dev/null || true

  GW_SVC=$(kubectl get svc -n "${NAMESPACE}" \
    -l app.kubernetes.io/component=gateway \
    -o jsonpath='{.items[0].metadata.name}')

  info "Starting port-forward: localhost:9000 → gateway S3"
  kubectl port-forward \
    -n "${NAMESPACE}" "svc/${GW_SVC}" 9000:9000 \
    &>/tmp/objectio-pf-gateway.log &
  echo $! > /tmp/objectio-pf-gateway.pid

  GF_SVC=$(kubectl get svc -n "${NAMESPACE}" \
    -l app.kubernetes.io/component=grafana \
    -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
  if [[ -n "${GF_SVC}" ]]; then
    info "Starting port-forward: localhost:3000 → Grafana"
    kubectl port-forward \
      -n "${NAMESPACE}" "svc/${GF_SVC}" 3000:3000 \
      &>/tmp/objectio-pf-grafana.log &
    echo $! > /tmp/objectio-pf-grafana.pid
  fi

  # Wait for gateway to respond
  info "Waiting for gateway to respond..."
  for i in $(seq 1 30); do
    if curl -sf http://localhost:9000/health &>/dev/null; then
      ok "Gateway is up"
      return
    fi
    sleep 2
  done
  die "Gateway did not respond after 60s — check logs: kubectl logs -n ${NAMESPACE} deploy/${RELEASE}-gateway"
}

# ── Smoke test ────────────────────────────────────────────────────────────────
smoke_test() {
  info "Running smoke tests..."

  # S3
  aws --endpoint-url http://localhost:9000 \
      --region us-east-1 \
      --no-sign-request \
      s3 mb s3://test-bucket 2>/dev/null || true
  echo "hello objectio" | aws --endpoint-url http://localhost:9000 \
      --region us-east-1 \
      --no-sign-request \
      s3 cp - s3://test-bucket/hello.txt
  RESULT=$(aws --endpoint-url http://localhost:9000 \
      --region us-east-1 \
      --no-sign-request \
      s3 cp s3://test-bucket/hello.txt -)
  [[ "${RESULT}" == "hello objectio" ]] || die "S3 round-trip failed"
  ok "S3: bucket create + put + get OK"

  # Iceberg REST Catalog
  HTTP=$(curl -sf -o /dev/null -w "%{http_code}" http://localhost:9000/iceberg/v1/config)
  [[ "${HTTP}" == "200" ]] || die "Iceberg /config returned ${HTTP}"
  ok "Iceberg REST Catalog: /config OK"

  # Delta Sharing
  HTTP=$(curl -sf -o /dev/null -w "%{http_code}" \
    http://localhost:9000/delta-sharing/v1/shares 2>/dev/null || echo "000")
  # 401 or 200 both mean the endpoint is alive
  [[ "${HTTP}" == "200" || "${HTTP}" == "401" ]] || die "Delta Sharing returned ${HTTP}"
  ok "Delta Sharing: /shares endpoint alive (HTTP ${HTTP})"

  ok "All smoke tests passed"
}

# ── Print summary ─────────────────────────────────────────────────────────────
print_summary() {
  cat <<EOF

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  ObjectIO is running on Kind cluster '${CLUSTER_NAME}'
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Endpoints (via kubectl port-forward):
  S3 / Iceberg / Delta Sharing  http://localhost:9000
  Grafana                       http://localhost:3000  (admin / objectio)

EC scheme: 4+2  |  Meta: 3 nodes  |  OSDs: 6 nodes

Quick tests:
  # S3
  aws --endpoint-url http://localhost:9000 --region us-east-1 --no-sign-request \\
    s3 ls

  # Iceberg
  curl -s http://localhost:9000/iceberg/v1/config | jq .

  # Create Iceberg namespace + table
  curl -s -X POST http://localhost:9000/iceberg/v1/namespaces \\
    -H 'Content-Type: application/json' \\
    -d '{"namespace":["analytics"],"properties":{}}' | jq .
  curl -s -X POST http://localhost:9000/iceberg/v1/namespaces/analytics/tables \\
    -H 'Content-Type: application/json' \\
    -d '{"name":"events","schema":{"type":"struct","fields":[{"id":1,"name":"id","type":"long","required":true}]}}' | jq .

  # Delta Sharing (create share + recipient via admin API)
  curl -s -X POST http://localhost:9000/_admin/delta-sharing/shares \\
    -d '{"name":"demo"}' | jq .

  # Cluster status
  kubectl get pods -n ${NAMESPACE}

Tear down:
  ./deploy/kind/setup.sh --teardown

EOF
}

# ── Main ──────────────────────────────────────────────────────────────────────
check_prereqs
create_cluster
build_and_load
deploy_helm
wait_ready
start_portforward
smoke_test
print_summary
