# ObjectIO

**Software-Defined Storage with S3 and Block interfaces, built in Rust.**

ObjectIO is a high-performance software-defined storage (SDS) platform that provides both S3-compatible object storage and distributed block storage from a single unified infrastructure. Erasure coding protects data across failure domains, CRUSH placement distributes shards deterministically, and a Raft-ready metadata service persists state via redb.

## Key Features

**Object Storage (S3 API)**
- Full S3 API compatibility — works with AWS CLI, SDKs, boto3, s3cmd
- AWS Signature V4 authentication with IAM users and bucket policies
- Multipart uploads, range reads, copy operations
- Streaming upload/download with per-stripe erasure coding

**Block Storage**
- Distributed block volumes with thin provisioning
- Copy-on-write snapshots and writable clones
- Per-volume QoS — IOPS/bandwidth limits with token bucket rate limiting
- Write journal and write cache for crash consistency and low latency
- iSCSI, NVMe-oF, and NBD attachment targets

**Data Protection**
- Reed-Solomon erasure coding (4+2, 6+3, 8+4, configurable)
- Locally Repairable Codes (LRC) for large clusters
- Replication mode for small/dev deployments
- CRUSH 2.0 (HRW hashing) for rack/node/disk-aware placement
- Pluggable EC backends — pure Rust (portable) or ISA-L (x86, 2-5x faster)

**Iceberg REST Catalog**
- Apache Iceberg REST Catalog API hosted on the gateway
- Namespace and table management with metadata persisted via the Meta service
- IAM-style access control at namespace and table level
- Works with Spark, Trino, Flink, and other Iceberg-compatible engines

**Architecture**
- Stateless gateways — scale horizontally behind a load balancer
- Raft-ready metadata cluster with redb persistence
- Raw disk I/O (O_DIRECT / F_NOCACHE) with WAL, B-tree index, and ARC cache
- Pure Rust — no C/C++ dependencies for core functionality

## Architecture

```
                ┌───────────────────────────────────────────────┐
                │                    Clients                    │
                │  S3 (aws-cli, boto3)  Iceberg (Spark/Trino)  │
                │  Block (iSCSI/NVMe-oF)                       │
                └───────────────────────┬───────────────────────┘
                                        │
                       ┌────────────────▼────────────────┐
                       │        Gateway (:9000)          │
                       │  S3 API + Iceberg REST Catalog  │
                       │  Block Gateway + SigV4 Auth     │
                       │  Erasure Encoding               │
                       └────────────────┬────────────────┘
                                    │
                 ┌──────────────────┼──────────────────┐
                 │                  │                  │
       ┌─────────▼─────────┐       │        ┌─────────▼─────────┐
       │  Meta (:9100)     │◄──────┴───────►│  Meta (:9100)     │
       │  Raft Consensus   │                │  Raft Consensus   │
       │  redb Persistence │                │  redb Persistence │
       │  CRUSH Placement  │                │  CRUSH Placement  │
       │  IAM / Volumes    │                │  IAM / Volumes    │
       └───────────────────┘                └───────────────────┘
                 │
       ┌─────────┴─────────────────────────────────┐
       │                   │                       │
  ┌────▼────┐         ┌────▼────┐             ┌────▼────┐
  │  OSD 1  │         │  OSD 2  │    ...      │  OSD N  │
  │  :9200  │         │  :9200  │             │  :9200  │
  │  Shards │         │  Shards │             │  Shards │
  │  Blocks │         │  Blocks │             │  Blocks │
  └─────────┘         └─────────┘             └─────────┘
```

## Quick Start

### Docker Compose

```bash
git clone https://github.com/objectio/objectio.git
cd objectio

# Start a local cluster (3 meta + 6 OSD + 1 gateway, 4+2 EC)
make cluster-up

# Check status
make cluster-status
```

### Building from Source

```bash
# Prerequisites (Ubuntu/Debian)
sudo apt-get install build-essential nasm autoconf automake libtool libclang-dev protobuf-compiler

# Prerequisites (macOS)
brew install nasm autoconf automake libtool llvm protobuf

# Build
cargo build --workspace --release --features isal

# Run tests
cargo test --workspace --features isal
```

### Run Locally

```bash
# Terminal 1: Metadata service
./target/release/objectio-meta --node-id 1 --listen 0.0.0.0:9100 --replication 1

# Terminal 2: OSD (auto-creates a 10GB disk image)
./target/release/objectio-osd \
  --listen 0.0.0.0:9200 \
  --disks /tmp/objectio-disk.img \
  --meta-endpoint http://localhost:9100

# Terminal 3: Gateway (no auth for testing)
./target/release/objectio-gateway \
  --listen 0.0.0.0:9000 \
  --meta-endpoint http://localhost:9100 \
  --no-auth
```

## Usage

### S3 Object Storage

```bash
# Create a bucket
aws --endpoint-url http://localhost:9000 s3 mb s3://my-bucket

# Upload a file
aws --endpoint-url http://localhost:9000 s3 cp file.txt s3://my-bucket/

# List objects
aws --endpoint-url http://localhost:9000 s3 ls s3://my-bucket/

# Download a file
aws --endpoint-url http://localhost:9000 s3 cp s3://my-bucket/file.txt -

# Range read (returns 206 Partial Content)
curl -r 0-1023 http://localhost:9000/my-bucket/file.txt
```

### Block Storage

```bash
# Create a volume
objectio-cli -e http://localhost:9100 volume create my-volume --size 100G

# List volumes
objectio-cli -e http://localhost:9100 volume list

# Create a snapshot
objectio-cli -e http://localhost:9100 snapshot create <volume-id> --name snap1
```

### Iceberg REST Catalog

The gateway hosts an Apache Iceberg REST Catalog at `/iceberg/v1/`.

```bash
ENDPOINT=http://localhost:9000/iceberg/v1

# Get catalog config
curl -s $ENDPOINT/config | jq .

# Create a namespace
curl -s -X POST $ENDPOINT/namespaces \
  -H 'Content-Type: application/json' \
  -d '{"namespace":["analytics"],"properties":{"owner":"alice"}}' | jq .

# List namespaces
curl -s $ENDPOINT/namespaces | jq .

# Create a table
curl -s -X POST $ENDPOINT/namespaces/analytics/tables \
  -H 'Content-Type: application/json' \
  -d '{"name":"events","schema":{"type":"struct","fields":[{"id":1,"name":"id","type":"long","required":true}]}}' | jq .

# List tables
curl -s $ENDPOINT/namespaces/analytics/tables | jq .

# Load table metadata
curl -s $ENDPOINT/namespaces/analytics/tables/events | jq .
```

Use `--warehouse-location` on the gateway to set the S3 URL prefix for table data (default: `s3://objectio-warehouse`).

### User Management

```bash
# Admin credentials are printed in meta service logs on first start
objectio-cli -e http://localhost:9100 user list
objectio-cli -e http://localhost:9100 user create alice --email alice@example.com
objectio-cli -e http://localhost:9100 key create <user-id>
```

## Components

| Component | Binary | Port | Description |
|-----------|--------|------|-------------|
| **Gateway** | `objectio-gateway` | 9000 | S3 API + Iceberg REST Catalog + block gateway (stateless, horizontally scalable) |
| **Meta** | `objectio-meta` | 9100 | Metadata cluster — buckets, objects, volumes, IAM, CRUSH placement (Raft + redb) |
| **OSD** | `objectio-osd` | 9200 | Storage daemon — shard storage, block I/O, raw disk with WAL |
| **CLI** | `objectio-cli` | - | Admin CLI for user, volume, and cluster management |

## S3 Compatibility

| Category | Operations |
|----------|------------|
| **Bucket** | CreateBucket, DeleteBucket, HeadBucket, ListBuckets, GetBucketLocation |
| **Object** | GetObject, PutObject, DeleteObject, HeadObject, CopyObject |
| **Listing** | ListObjectsV2, ListObjectVersions |
| **Multipart** | CreateMultipartUpload, UploadPart, CompleteMultipartUpload, AbortMultipartUpload, ListParts, ListMultipartUploads |
| **Policy** | GetBucketPolicy, PutBucketPolicy, DeleteBucketPolicy |
| **Versioning** | PutBucketVersioning, GetBucketVersioning |
| **Auth** | AWS Signature V4 (SigV4) |

## Iceberg REST Catalog

| Endpoint | Description |
|----------|-------------|
| `GET /iceberg/v1/config` | Catalog configuration |
| `POST /iceberg/v1/namespaces` | Create namespace |
| `GET /iceberg/v1/namespaces` | List namespaces |
| `GET /iceberg/v1/namespaces/{ns}` | Get namespace |
| `DELETE /iceberg/v1/namespaces/{ns}` | Drop namespace |
| `POST /iceberg/v1/namespaces/{ns}/tables` | Create table |
| `GET /iceberg/v1/namespaces/{ns}/tables` | List tables |
| `GET /iceberg/v1/namespaces/{ns}/tables/{table}` | Load table |
| `DELETE /iceberg/v1/namespaces/{ns}/tables/{table}` | Drop table |
| `POST /iceberg/v1/tables/rename` | Rename table |
| `PUT /iceberg/v1/namespaces/{ns}/policy` | Set namespace policy |
| `PUT /iceberg/v1/namespaces/{ns}/tables/{table}/policy` | Set table policy |

Access control uses IAM-style policies with `iceberg:` action prefix (e.g., `iceberg:LoadTable`) and `arn:obio:iceberg:::` ARNs.

## Block Storage Features

| Feature | Description |
|---------|-------------|
| **Volumes** | Create, resize (grow), delete distributed block volumes |
| **Thin Provisioning** | Storage allocated only on write |
| **Snapshots** | Instant copy-on-write snapshots |
| **Clones** | Writable clones from snapshots |
| **QoS** | Per-volume IOPS and bandwidth limits with burst support |
| **Protocols** | iSCSI, NVMe-oF, NBD attachment targets |

## Data Protection Modes

| Mode | Overhead | Fault Tolerance | Use Case |
|------|----------|-----------------|----------|
| Replication 1 | 1x | None | Dev/test |
| Replication 3 | 3x | 2 failures | Small clusters, low latency |
| EC 4+2 | 1.5x | 2 failures | Production default |
| EC 6+3 | 1.5x | 3 failures | Multi-node production |
| LRC 6+2+2 | 1.67x | 1 rack + 1 disk | Large multi-rack clusters |

## Documentation

- [Architecture](docs/architecture/README.md) — system design, data flow, CRUSH placement
- [Block Storage](docs/architecture/block-storage.md) — volumes, snapshots, QoS, protocols
- [Data Protection](docs/architecture/data-protection.md) — erasure coding, LRC, replication
- [Storage Engine](docs/storage/README.md) — raw disk I/O, WAL, caching
- [Deployment Guide](docs/deployment/README.md) — Docker, bare-metal, topologies
- [API Reference](docs/api/README.md) — S3 operations, authentication
- [Operations](docs/operations/README.md) — monitoring, failure recovery

## Development

```bash
make build            # Debug build
make build-release    # Release build
make test             # Run all tests
make lint             # Clippy with -D warnings
make fmt              # Check formatting
make ci               # Full CI: fmt + lint + test

# Docker images
docker build --target gateway -t objectio-gateway .
docker build --target meta    -t objectio-meta .
docker build --target osd     -t objectio-osd .
docker build --target cli     -t objectio-cli .
```

## Requirements

- Rust 1.93+ (edition 2024)
- protobuf-compiler (protoc)
- ISA-L build deps for x86: nasm, autoconf, automake, libtool, libclang-dev
- On ARM, omit `--features isal` (uses portable pure-Rust EC backend)

## License

[Apache License 2.0](LICENSE)
