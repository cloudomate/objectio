# ObjectIO - S3-Compatible Distributed Object Storage

## Project Overview

ObjectIO is a pure Software Defined Storage (SDS) system providing S3 API compatibility with erasure coding for data protection, using raw disk access for maximum performance.

## Directory Structure

```
objectio/
├── crates/                     # Library crates
│   ├── objectio-common/        # Shared types, errors, config
│   ├── objectio-proto/         # Protobuf definitions (tonic/prost)
│   ├── objectio-placement/     # CRUSH-like placement algorithm
│   ├── objectio-erasure/       # Reed-Solomon erasure coding
│   ├── objectio-storage/       # Raw disk storage engine
│   ├── objectio-meta-store/    # Raft + metadata storage
│   ├── objectio-s3/            # S3 API handlers
│   ├── objectio-client/        # Internal RPC client
│   └── objectio-auth/          # Authentication & authorization
│
├── bin/                        # Binary crates
│   ├── objectio-gateway/       # S3 API gateway (stateless)
│   ├── objectio-meta/          # Metadata service (Raft cluster)
│   ├── objectio-osd/           # Object Storage Daemon
│   ├── objectio-cli/           # Admin CLI tool
│   └── objectio-install/       # Installation helper
│
├── docs/                       # Documentation
│   ├── architecture/           # System design docs
│   ├── deployment/             # Deployment guides
│   └── operations/             # Operational procedures
│
├── examples/config/            # Example configuration files
├── tests/                      # Integration tests
├── Dockerfile                  # Multi-stage Docker build
└── docker-compose.prod.yml     # Production deployment
```

## Key Components

| Component | Port | Description |
|-----------|------|-------------|
| **Gateway** | 9000 | S3 API endpoint (stateless, horizontally scalable) |
| **Meta** | 9100 | Metadata service (Raft consensus, 3+ nodes for HA) |
| **OSD** | 9200 | Object Storage Daemon (one per disk or node) |

## Data Write Path

```
S3 Client (PUT /bucket/key)
         │
         ▼
┌─────────────────────────────────────────────────────┐
│                   Gateway (:9000)                    │
│  1. Parse S3 request, authenticate (SigV4)          │
│  2. Query Meta for bucket policy & placement        │
│  3. Erasure encode data (default 4+2 Reed-Solomon)  │
│  4. Write shards to OSDs in parallel                │
│  5. Update object metadata in Meta                  │
│  6. Return S3 response                              │
└─────────────────────────────────────────────────────┘
         │
         ├──────────────────┐
         ▼                  ▼
┌─────────────────┐  ┌─────────────────┐
│  Meta (:9100)   │  │  OSDs (:9200)   │
│  - Bucket info  │  │  - Shard 0      │
│  - Object meta  │  │  - Shard 1      │
│  - Placement    │  │  - ...          │
│  - Raft log     │  │  - Shard k+m-1  │
└─────────────────┘  └─────────────────┘
```

**Write Flow Details:**
1. Gateway receives PUT request via S3 API
2. Gateway authenticates request (AWS SigV4 or no-auth mode)
3. Gateway queries Meta for placement decision (which OSDs for each shard)
4. Gateway erasure-encodes object into k data + m parity shards
5. Gateway writes shards to selected OSDs **in parallel**
6. Gateway waits for quorum (k shards) before returning success
7. Meta records object metadata (key, size, shard locations, checksums)

## Deployment

### Docker Compose (Production)

```bash
# Build all images
docker build --target gateway -t objectio-gateway .
docker build --target meta -t objectio-meta .
docker build --target osd -t objectio-osd .

# Deploy
docker compose -f docker-compose.prod.yml up -d
```

### Current Test Deployment

- **Host**: 192.168.4.225 (Ubuntu VM with 7 raw disks)
- **Topology**: 3 racks, 7 OSDs, 3 Meta nodes, 1 Gateway
- **EC Config**: 4+2 (4 data shards, 2 parity)

### OSD Configuration

Each OSD needs a config file with failure domain:

```toml
# /etc/objectio/osd.toml
[osd]
node_name = "osd1"
listen = "0.0.0.0:9200"
advertise_addr = "osd1:9200"      # How other services reach this OSD
meta_endpoint = "http://meta1:9100"

[osd.failure_domain]
region = "local"
datacenter = "dc1"
rack = "rack-01"                  # CRUSH uses this for shard placement

[storage]
disks = ["/dev/vdb"]
data_dir = "/var/lib/objectio/osd"
```

## Authentication & IAM

### Overview

ObjectIO uses AWS Signature V4 (SigV4) authentication, compatible with standard AWS SDKs and CLI tools.

| Mode | Flag | Description |
|------|------|-------------|
| **Authenticated** | (default) | SigV4 required for all requests |
| **Development** | `--no-auth` | No authentication (testing only) |

### Admin User Bootstrap

On first startup, the metadata service automatically creates an admin user:

```text
============================================
Admin credentials (save these!):
  Access Key ID:     AKIAXXXXXXXXXXXXXXXXXX
  Secret Access Key: xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
============================================
```

The admin username defaults to `admin` but can be changed:

```bash
objectio-meta --admin-user root
```

### Admin API

The Admin API (`/_admin/*`) uses the same SigV4 authentication as S3. Only the admin user can access these endpoints.

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/_admin/users` | GET | List all users |
| `/_admin/users` | POST | Create user (JSON: `{"display_name": "...", "email": "..."}`) |
| `/_admin/users/{user_id}` | DELETE | Delete user |
| `/_admin/users/{user_id}/access-keys` | GET | List user's access keys |
| `/_admin/users/{user_id}/access-keys` | POST | Create access key for user |
| `/_admin/access-keys/{access_key_id}` | DELETE | Delete access key |

#### Example: Create a new user

```bash
# Configure AWS CLI with admin credentials
export AWS_ACCESS_KEY_ID=AKIA...
export AWS_SECRET_ACCESS_KEY=...

# Create user via Admin API
curl -X POST https://s3.example.com/_admin/users \
  -H "Content-Type: application/json" \
  -d '{"display_name": "alice", "email": "alice@example.com"}' \
  --aws-sigv4 "aws:amz:us-east-1:s3" \
  --user "$AWS_ACCESS_KEY_ID:$AWS_SECRET_ACCESS_KEY"

# Or use the CLI
objectio-cli user create alice --email alice@example.com
objectio-cli key create <user_id>
```

### CLI Tool (`objectio-cli`)

```bash
# User management
objectio-cli user list
objectio-cli user create <display_name> [--email <email>]
objectio-cli user delete <user_id>

# Access key management
objectio-cli key list <user_id>
objectio-cli key create <user_id>
objectio-cli key delete <access_key_id>
```

## Testing

### Without Authentication (Development)

```bash
# Start gateway in no-auth mode
objectio-gateway --no-auth

# Create bucket
aws --endpoint-url http://localhost:9000 --no-sign-request s3 mb s3://test

# Upload file
aws --endpoint-url http://localhost:9000 --no-sign-request s3 cp file.txt s3://test/

# List objects
aws --endpoint-url http://localhost:9000 --no-sign-request s3 ls s3://test/

# Download file
aws --endpoint-url http://localhost:9000 --no-sign-request s3 cp s3://test/file.txt -
```

### With Authentication (Production)

```bash
# Configure credentials (from metadata service startup logs)
export AWS_ACCESS_KEY_ID=AKIA...
export AWS_SECRET_ACCESS_KEY=...

# Create bucket
aws --endpoint-url https://s3.example.com s3 mb s3://test

# Upload file
aws --endpoint-url https://s3.example.com s3 cp file.txt s3://test/

# List objects
aws --endpoint-url https://s3.example.com s3 ls s3://test/
```

## Building

```bash
# Debug build
cargo build

# Release build with ISA-L (x86_64 only)
cargo build --release --features isal

# Run tests
cargo test
```

## Key Technologies

- **Rust** - Systems programming language
- **Tokio** - Async runtime
- **Axum** - HTTP server for S3 API
- **Tonic/Prost** - gRPC for inter-service communication
- **OpenRaft** - Raft consensus for metadata
- **Redb** - Embedded database for metadata storage
- **reed-solomon-simd** - SIMD-optimized erasure coding
