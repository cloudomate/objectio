# 008 - Deployment and Operations

## Overview

ObjectIO supports multiple deployment environments: local kind cluster for development, Helm charts for production Kubernetes, and Docker for container builds.

## Development: kind

### Setup

```bash
make kind-up              # Create kind cluster and deploy via Helm
make kind-up-registry     # Same, but pull images from GHCR
make kind-load            # Rebuild images and reload into cluster
make kind-down            # Tear down cluster
```

### Setup Script

- `deploy/kind/setup.sh` — Cluster creation and Helm deployment
- `deploy/kind/values.yaml` — Kind-specific values overlay
- `deploy/kind/kind-cluster.yaml` — Kind cluster configuration

### Testing

```bash
aws --endpoint-url http://localhost:9000 s3 mb s3://test-bucket
echo "hello" | aws --endpoint-url http://localhost:9000 s3 cp - s3://test-bucket/hello.txt
aws --endpoint-url http://localhost:9000 s3 cp s3://test-bucket/hello.txt -
curl -r 0-3 http://localhost:9000/test-bucket/hello.txt   # Range request → 206
```

## Production: Helm

### Chart Location

- `deploy/helm/objectio/` — Production Helm chart

### Chart Structure

```
deploy/helm/objectio/
├── Chart.yaml
├── values.yaml
├── templates/
│   ├── gateway-*.yaml
│   ├── meta-*.yaml
│   ├── osd-*.yaml
│   ├── block-gateway-*.yaml
│   └── *_service.yaml, *_deployment.yaml, *_pvc.yaml, etc.
├── dashboards/
│   └── *.json (Grafana dashboards)
└── dashboards/ (monitoring dashboards)
```

### Service Deployments

- **Gateway**: Deployment with HPA, Ingress, Service
- **Meta**: StatefulSet with Headless Service, PVC, PDB
- **OSD**: StatefulSet with Headless Service, PVC, ConfigMap
- **Block Gateway**: Deployment with Service, PVC

### Monitoring Stack

- Prometheus deployment with ConfigMap
- Grafana deployment with provisioning
- Dashboard ConfigMaps for:
  - cluster-overview.json
  - s3-operations.json
  - disk-health.json
  - osd-detail.json
  - block-storage.json

## Docker

### Multi-stage Build

```bash
docker build --target all     -t objectio .           # Universal image
docker build --target gateway -t objectio-gateway .   # Per-service (rare)
docker build --target meta    -t objectio-meta .
docker build --target osd     -t objectio-osd .
docker build --target cli     -t objectio-cli .
```

### Dockerfile Targets

- `all` — Single universal image (used by Helm)
- `gateway` — Gateway service only
- `meta` — Meta service only
- `osd` — OSD service only
- `cli` — CLI tool only

### Image Registry

- GHCR: `ghcr.io/cloudomate/objectio:<tag>`
- Multi-arch support (AMD64, ARM64)
- Published via `.github/workflows/release.yml`

## Monitoring

### Prometheus

- ServiceMonitor per service
- Metrics endpoints: :9000/metrics, :9101, :9201
- `deploy/monitoring/prometheus/prometheus.yml` — Prometheus config

### Grafana

- Dashboards provisioning via ConfigMaps
- Datasource: Prometheus
- `deploy/monitoring/docker-compose.monitoring.yml` — Local monitoring

### Metrics

| Service | Metrics Port | Endpoint |
|---------|--------------|----------|
| Gateway | 9000 | /metrics |
| Meta | 9101 | /metrics |
| OSD | 9201 | /metrics |

## CI/CD

### CI Pipeline

```bash
make ci   # Full CI: fmt + lint + test
```

### CI Steps

1. **fmt** — `cargo fmt --check`
2. **lint** — `cargo clippy --all-targets --all-features -- -D warnings`
3. **test** — `cargo test --workspace --features isal`

### Docker CI

```bash
docker compose run --rm build    # Build workspace
docker compose run --rm test     # Run tests
docker compose run --rm lint     # Run clippy
docker compose run --rm fmt      # Check formatting
docker compose run --rm dev      # Interactive dev shell
```

## All-in-One (aio)

For laptop-scale testing without Kubernetes:

- `bin/objectio-aio` — Single-process binary
- Runs meta + OSD + gateway in one tokio runtime
- `--admin-port`, `--ops-console-port`, `--tenant-console-port` flags

## Configuration

### TOML Config Files

- Service-specific TOML configuration
- CLI args override TOML config
- `--config` flag points to TOML file

### Example Configs

- `examples/config/single-node/osd.toml`
- `examples/config/three-nodes/node{1,2,3}-osd.toml`
- `examples/config/three-nodes/gateway.toml`
- `examples/config/multi-rack-lrc/osd-rack1-node1.toml`