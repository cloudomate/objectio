# 008 - Deployment and Operations: Plan

## Approach

Document the existing deployment and operations infrastructure, covering kind development setup, Helm production deployment, Docker builds, monitoring with Prometheus/Grafana, and CI pipeline.

## Technical Approach

### Development

1. **kind**: Document `make kind-up/kind-down/kind-load` commands
2. **Setup script**: Document `deploy/kind/setup.sh`
3. **Values overlay**: Document `deploy/kind/values.yaml`

### Production

1. **Helm chart**: Document `deploy/helm/objectio/` structure
2. **Templates**: Document StatefulSet/Deployment/Service templates
3. **Values**: Document production configuration

### Docker

1. **Multi-stage build**: Document Dockerfile targets (all, gateway, meta, osd, cli)
2. **Image publishing**: Document GHCR multi-arch publishing

### Monitoring

1. **Prometheus**: Document metrics endpoints and scraping
2. **Grafana**: Document dashboard provisioning and ConfigMaps
3. **Dashboards**: Document cluster-overview, s3-operations, disk-health, osd-detail, block-storage

### CI

1. **Pipeline**: Document fmt → lint → test
2. **Docker CI**: Document docker compose run commands
3. **Make targets**: Document make ci, make build, make test, etc.

### Source Files to Reference

- `Makefile` — Build and deploy targets
- `docker-compose.yml` — Docker CI configuration
- `Dockerfile` — Multi-stage build
- `deploy/kind/setup.sh` — Kind setup script
- `deploy/kind/values.yaml` — Kind values overlay
- `deploy/helm/objectio/Chart.yaml` — Helm chart
- `deploy/helm/objectio/values.yaml` — Helm values
- `deploy/helm/objectio/templates/*.yaml` — K8s templates
- `deploy/monitoring/docker-compose.monitoring.yml` — Local monitoring
- `deploy/monitoring/prometheus/prometheus.yml` — Prometheus config
- `.github/workflows/release.yml` — Release workflow

## Deliverables

- spec.md: Deployment methods, Helm chart, Docker, monitoring, CI
- plan.md: This document
- tasks.md: Actionable documentation tasks