# 008 - Deployment and Operations: Tasks

## Documentation Tasks

### kind Development

- [ ] Document make kind-up (create cluster + Helm deploy)
- [ ] Document make kind-up-registry (pull from GHCR)
- [ ] Document make kind-load (rebuild + reload images)
- [ ] Document make kind-down (tear down)
- [ ] Document deploy/kind/setup.sh script
- [ ] Document deploy/kind/values.yaml overlay
- [ ] Document deploy/kind/kind-cluster.yaml

### Helm Production

- [ ] Document deploy/helm/objectio/ chart structure
- [ ] Document gateway Deployment + HPA + Ingress + Service
- [ ] Document meta StatefulSet + Headless Service + PVC + PDB
- [ ] Document osd StatefulSet + Headless Service + PVC + ConfigMap
- [ ] Document block-gateway Deployment + Service + PVC
- [ ] Document deploy/helm/objectio/values.yaml production config

### Docker

- [ ] Document Dockerfile multi-stage build (all, gateway, meta, osd, cli)
- [ ] Document docker build --target commands
- [ ] Document GHCR image: ghcr.io/cloudomate/objectio:<tag>
- [ ] Document multi-arch support (AMD64, ARM64)
- [ ] Document .github/workflows/release.yml publishing

### Monitoring

- [ ] Document Prometheus metrics endpoints (9000/metrics, 9101, 9201)
- [ ] Document deploy/monitoring/prometheus/prometheus.yml
- [ ] Document Grafana dashboard provisioning
- [ ] Document cluster-overview.json dashboard
- [ ] Document s3-operations.json dashboard
- [ ] Document disk-health.json dashboard
- [ ] Document osd-detail.json dashboard
- [ ] Document block-storage.json dashboard
- [ ] Document deploy/monitoring/docker-compose.monitoring.yml

### CI/CD

- [ ] Document make ci (fmt + lint + test)
- [ ] Document make fmt and make fmt-fix
- [ ] Document make lint (clippy -D warnings)
- [ ] Document make test (cargo test --workspace --features isal)
- [ ] Document docker compose run build/test/lint/fmt/dev commands

### All-in-One

- [ ] Document bin/objectio-aio single-process binary
- [ ] Document --admin-port, --ops-console-port, --tenant-console-port flags
- [ ] Document use case: laptop-scale testing without K8s

### Configuration

- [ ] Document TOML config file pattern
- [ ] Document CLI args override TOML config
- [ ] Document --config flag usage
- [ ] Document example configs in examples/config/