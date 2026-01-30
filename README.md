# ObjectIO

**S3-Compatible Distributed Object Storage in Rust**

ObjectIO is a high-performance, S3-compatible object storage system built in Rust. It provides erasure coding for data protection, CRUSH-like placement for fault tolerance, and AWS Signature V4 authentication.

## Features

- **S3 API Compatible** - Works with AWS CLI, SDKs, and existing S3 tools
- **Erasure Coding** - Configurable Reed-Solomon encoding (default 4+2)
- **CRUSH Placement** - Rack-aware data distribution for fault tolerance
- **AWS SigV4 Authentication** - Secure access with IAM-style users and policies
- **Multipart Uploads** - Support for large file uploads (>100MB)
- **Bucket Policies** - Fine-grained access control with JSON policies
- **Horizontal Scaling** - Stateless gateways, distributed metadata

## Architecture

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   S3 Clients    │     │   S3 Clients    │     │   S3 Clients    │
└────────┬────────┘     └────────┬────────┘     └────────┬────────┘
         │                       │                       │
         └───────────────────────┼───────────────────────┘
                                 │
                    ┌────────────▼────────────┐
                    │     Gateway (:9000)     │
                    │  - S3 API (Axum)        │
                    │  - SigV4 Auth           │
                    │  - Erasure Encoding     │
                    └────────────┬────────────┘
                                 │
              ┌──────────────────┼──────────────────┐
              │                  │                  │
    ┌─────────▼─────────┐       │       ┌─────────▼─────────┐
    │  Meta (:9100)     │◄──────┴──────►│  Meta (:9100)     │
    │  - Raft Consensus │               │  - Raft Consensus │
    │  - Object Metadata│               │  - Object Metadata│
    │  - IAM Users      │               │  - IAM Users      │
    └───────────────────┘               └───────────────────┘
              │
    ┌─────────┴─────────────────────────────────┐
    │                   │                       │
┌───▼───┐          ┌───▼───┐              ┌───▼───┐
│ OSD 1 │          │ OSD 2 │    ...       │ OSD N │
│:9200  │          │:9200  │              │:9200  │
└───────┘          └───────┘              └───────┘
```

## Quick Start

### Using Docker Compose

```bash
# Clone the repository
git clone https://github.com/example/objectio.git
cd objectio

# Start the stack
docker compose up -d

# Check logs for admin credentials
docker compose logs meta | grep -A3 "Admin credentials"
```

### Building from Source

```bash
# Build all binaries
cargo build --release

# Run metadata service (creates admin user on first start)
./target/release/objectio-meta --listen 0.0.0.0:9100

# Run OSD (storage node)
./target/release/objectio-osd --config /etc/objectio/osd.toml

# Run gateway
./target/release/objectio-gateway --listen 0.0.0.0:9000 --meta-endpoint http://localhost:9100
```

## Usage

### Configure AWS CLI

```bash
# Get credentials from meta service startup logs
export AWS_ACCESS_KEY_ID=AKIA...
export AWS_SECRET_ACCESS_KEY=...

# Create a bucket
aws --endpoint-url http://localhost:9000 s3 mb s3://my-bucket

# Upload a file
aws --endpoint-url http://localhost:9000 s3 cp file.txt s3://my-bucket/

# List objects
aws --endpoint-url http://localhost:9000 s3 ls s3://my-bucket/

# Download a file
aws --endpoint-url http://localhost:9000 s3 cp s3://my-bucket/file.txt -
```

### User Management

```bash
# Using the CLI (connects to metadata service)
objectio-cli user list
objectio-cli user create alice --email alice@example.com
objectio-cli key create <user_id>

# Using the Admin API (requires admin credentials)
curl -X POST http://localhost:9000/_admin/users \
  -H "Content-Type: application/json" \
  -d '{"display_name": "alice", "email": "alice@example.com"}' \
  --aws-sigv4 "aws:amz:us-east-1:s3" \
  --user "$AWS_ACCESS_KEY_ID:$AWS_SECRET_ACCESS_KEY"
```

## Configuration

### Gateway

```bash
objectio-gateway \
  --listen 0.0.0.0:9000 \
  --meta-endpoint http://meta:9100 \
  --ec-k 4 \                    # Data shards
  --ec-m 2                      # Parity shards
```

### OSD (Storage Node)

```toml
# /etc/objectio/osd.toml
[osd]
node_name = "osd1"
listen = "0.0.0.0:9200"
advertise_addr = "osd1:9200"
meta_endpoint = "http://meta:9100"

[osd.failure_domain]
region = "us-east-1"
datacenter = "dc1"
rack = "rack-01"

[storage]
disks = ["/dev/sdb"]
data_dir = "/var/lib/objectio/osd"
```

### Metadata Service

```bash
objectio-meta \
  --listen 0.0.0.0:9100 \
  --node-id 1 \
  --admin-user admin           # Default admin username
```

## Components

| Component | Port | Description |
|-----------|------|-------------|
| **Gateway** | 9000 | S3 API endpoint (stateless, horizontally scalable) |
| **Meta** | 9100 | Metadata service (Raft consensus for HA) |
| **OSD** | 9200 | Object Storage Daemon (one per disk or node) |
| **CLI** | - | Admin command-line tool |

## Documentation

- [Architecture](docs/architecture/README.md)
- [Deployment Guide](docs/deployment/README.md)
- [API Reference](docs/api/README.md)
- [Authentication](docs/api/authentication.md)
- [Operations](docs/operations/README.md)

## S3 Compatibility

### Supported Operations

| Category | Operations |
|----------|------------|
| **Bucket** | CreateBucket, DeleteBucket, HeadBucket, ListBuckets |
| **Object** | GetObject, PutObject, DeleteObject, HeadObject, CopyObject |
| **Listing** | ListObjectsV2, ListObjectVersions |
| **Multipart** | CreateMultipartUpload, UploadPart, CompleteMultipartUpload, AbortMultipartUpload, ListParts |
| **Policy** | GetBucketPolicy, PutBucketPolicy, DeleteBucketPolicy |

### Authentication

- AWS Signature Version 4 (SigV4)
- AWS Signature Version 2 (SigV2) - legacy support
- Bucket policies with conditions

## Development

```bash
# Run tests
cargo test

# Build with optimizations
cargo build --release

# Build Docker images
docker build --target gateway -t objectio-gateway .
docker build --target meta -t objectio-meta .
docker build --target osd -t objectio-osd .
```

## License

Apache 2.0
