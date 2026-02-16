# Getting Started with ObjectIO

This guide will help you get ObjectIO running quickly. ObjectIO is a software-defined storage platform providing S3 object storage and distributed block storage.

## Prerequisites

- Rust 1.93+ (for building from source)
- Docker and Docker Compose (for containerized deployment)
- Linux or macOS

## Quick Start: Single-Node (Simplest)

The fastest way to try ObjectIO is a single-node setup with no redundancy:

```bash
# Terminal 1: Start metadata service (single-disk mode)
./target/release/objectio-meta --node-id 1 --listen 0.0.0.0:9100 --replication 1

# Terminal 2: Start OSD with a file-based disk (auto-created, 10GB default)
./target/release/objectio-osd \
  --listen 0.0.0.0:9200 \
  --disks /tmp/objectio-disk.img \
  --meta-endpoint http://localhost:9100

# Terminal 3: Start gateway (no auth for testing)
./target/release/objectio-gateway \
  --listen 0.0.0.0:9000 \
  --meta-endpoint http://localhost:9100 \
  --no-auth

# Test with AWS CLI
aws --endpoint-url http://localhost:9000 s3 mb s3://test
aws --endpoint-url http://localhost:9000 s3 cp myfile.txt s3://test/
aws --endpoint-url http://localhost:9000 s3 ls s3://test/
```

## Quick Start with Docker

The fastest way to try ObjectIO is using Docker Compose.

### 1. Clone and Build

```bash
git clone https://github.com/objectio/objectio.git
cd objectio

# Build Docker images
make docker
```

### 2. Start a Local Cluster

```bash
# Start a 3-meta + 6-OSD cluster
make cluster-up

# Check status
make cluster-status
```

### 3. Test with AWS CLI

```bash
# Configure endpoint
export AWS_ENDPOINT_URL=http://localhost:9000

# Create a bucket
aws s3 mb s3://test-bucket

# Upload a file
aws s3 cp myfile.txt s3://test-bucket/

# List objects
aws s3 ls s3://test-bucket/

# Download
aws s3 cp s3://test-bucket/myfile.txt downloaded.txt
```

## Building from Source

### Install Dependencies

```bash
# Ubuntu/Debian
sudo apt-get install build-essential nasm autoconf automake libtool libclang-dev protobuf-compiler

# macOS
brew install nasm autoconf automake libtool llvm protobuf
```

### Build

```bash
# Debug build
cargo build --workspace --features isal

# Release build
cargo build --workspace --release --features isal

# Run tests
cargo test --workspace --features isal
```

### Run Locally

```bash
# Terminal 1: Start metadata service
./target/release/objectio-meta --node-id 1 --listen 0.0.0.0:9100

# Terminal 2: Start OSD (with a test disk file)
dd if=/dev/zero of=/tmp/disk1.img bs=1M count=1024
./target/release/objectio-osd --listen 0.0.0.0:9200 --disks /tmp/disk1.img --meta-endpoint http://localhost:9100

# Terminal 3: Start gateway
./target/release/objectio-gateway --listen 0.0.0.0:9000 --meta-endpoint http://localhost:9100 --no-auth
```

## Configuration

ObjectIO uses TOML configuration files. See [Configuration Reference](deployment/configuration.md) for details.

### Gateway Configuration

```toml
# /etc/objectio/gateway.toml
[server]
listen = "0.0.0.0:9000"

[metadata]
endpoints = ["http://meta1:9100", "http://meta2:9100", "http://meta3:9100"]

[erasure_coding]
data_shards = 4
parity_shards = 2
```

### OSD Configuration

```toml
# /etc/objectio/osd.toml
[osd]
listen = "0.0.0.0:9200"
meta_endpoint = "http://meta1:9100"

[storage]
disks = ["/dev/vdb", "/dev/vdc"]
block_size = 4194304  # 4 MB
```

## Next Steps

- [Architecture Overview](architecture/README.md) - Understand the system design
- [Deployment Guide](deployment/README.md) - Production deployment
- [S3 API Reference](api/s3-operations.md) - Supported operations
- [Operations Guide](operations/README.md) - Monitoring and maintenance
