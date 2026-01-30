# Deployment Guide

This section covers deploying ObjectIO in various configurations.

## Contents

- [Topologies](topologies.md) - Single-node to multi-DC deployments
- [Docker](docker.md) - Container-based deployment
- [Configuration](configuration.md) - Configuration reference

## Quick Start

### Docker Compose (Development)

```bash
# Clone the repository
git clone https://github.com/your-org/objectio.git
cd objectio

# Build Docker images
make docker

# Start a local cluster
make cluster-up

# Check status
make cluster-status
```

### Production Deployment

For production, use the installer:

```bash
# Download and install
sudo ./objectio-install

# Or with configuration file
sudo ./objectio-install --config install.toml
```

## Deployment Options

| Method | Best For | Guide |
|--------|----------|-------|
| Docker Compose | Development, testing | [Docker](docker.md) |
| Systemd | Bare-metal production | [Configuration](configuration.md) |
| Kubernetes | Cloud-native | Coming soon |

## Minimum Requirements

### Hardware

| Component | Minimum | Recommended |
|-----------|---------|-------------|
| CPU | 2 cores | 8+ cores |
| RAM | 4 GB | 16+ GB |
| Disk | 100 GB | 1+ TB per OSD |
| Network | 1 Gbps | 10+ Gbps |

### Software

- Linux (kernel 4.19+) or macOS (10.15+)
- Docker 24+ (for container deployment)
- Rust 1.92+ (for building from source)

## Cluster Sizing

### Metadata Service

| Cluster Size | Nodes | Failure Tolerance |
|--------------|-------|-------------------|
| Small | 3 | 1 node |
| Medium | 5 | 2 nodes |
| Large | 7 | 3 nodes |

### Storage Nodes (OSDs)

For 4+2 erasure coding:
- **Minimum**: 6 OSDs (no spare)
- **Recommended**: 7+ OSDs (with spare)

For 8+4 erasure coding:
- **Minimum**: 12 OSDs
- **Recommended**: 14+ OSDs

## Network Ports

| Port | Service | Protocol |
|------|---------|----------|
| 9000 | S3 Gateway | HTTP/HTTPS |
| 9100 | Metadata gRPC | gRPC |
| 9101 | Metadata Raft | gRPC |
| 9200 | OSD gRPC | gRPC |

## Security Checklist

- [ ] Enable TLS for S3 Gateway
- [ ] Configure firewall rules
- [ ] Enable authentication (`--no-auth` disabled)
- [ ] Set strong admin credentials
- [ ] Configure bucket policies
- [ ] Enable mTLS for internal communication (optional)

## Next Steps

1. Choose a [topology](topologies.md) based on your requirements
2. Follow the [Docker guide](docker.md) for container deployment
3. Review [configuration options](configuration.md)
