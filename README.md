# ObjectIO

**Unified software-defined storage: S3, Iceberg tables, Delta Sharing, and block volumes on one erasure-coded backend. Built in Rust.**

ObjectIO delivers four storage paradigms on a single cluster — S3 object
storage, an Iceberg REST Catalog with Delta Sharing, and distributed block
volumes — backed by erasure-coded OSDs, topology-aware CRUSH placement, and
a Raft-consensus metadata service (openraft 0.9). Community features (S3, block, replication,
SSE-S3) are Apache-2.0 licensed; Enterprise features (Iceberg, Delta
Sharing, SSE-KMS external backends, multi-tenancy, OIDC registration, LRC)
are source-available under BUSL-1.1 and gated at runtime by a signed
license.

## Key Features

**Object Storage (S3 API)**
- Full S3 API compatibility — works with AWS CLI, SDKs, boto3, s3cmd
- AWS Signature V4 authentication with IAM users and bucket policies
- Multipart uploads, range reads, copy operations
- Versioning, object lock (WORM), lifecycle rules
- Streaming upload/download with per-stripe erasure coding
- Server-side encryption at rest — SSE-S3, SSE-KMS, and SSE-C; bucket defaults via `?encryption`; per-request headers honored; on-disk shards are always ciphertext when SSE is enabled

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
- IAM-style access control at namespace and table level with hierarchical policy evaluation
- Tags, quotas, encryption policies, and column/row-level data filters
- Built-in roles (CatalogAdmin, NamespaceOwner, TableWriter, TableReader) with role binding
- Policy simulation and effective policy introspection
- Works with Spark, Trino, Flink, and other Iceberg-compatible engines

**Delta Sharing**
- [Delta Sharing](https://delta.io/sharing/) open protocol for secure cross-organization data sharing
- Shares Iceberg tables via the Delta Sharing wire protocol (not Delta Lake format)
- Share, schema, and table management with admin API
- Bearer token authentication for recipients
- Presigned S3 URLs for direct data access

**Multi-Tenancy & Identity** *(Enterprise)*
- Tenants with per-tenant quotas, allowed pools, and admin users
- Tenant-scoped buckets, users, access keys, warehouses, shares
- Tenant-admin role: delegated admin within a tenant without system privileges
- OIDC / external SSO (Keycloak, Entra, Okta, Google) via `identity/openid/*` config

**Topology & Locality**
- 5-level failure domain model: region → zone → datacenter → rack → host
- Hard-enforced placement constraints (refuse writes when spread can't be satisfied)
- Locality-aware EC reads: gateway pulls shards from nearest OSDs first
- `objectio_read_locality_bytes_total` counter — see multi-DC bandwidth at a glance
- Interactive topology diagram in the web console

**Licensing**
- Ed25519-signed license files distinguish Community and Enterprise tiers
- Per-license node + raw-capacity caps enforced at meta `register_osd` (scale-up block on new hardware only)
- Runtime hot-swap: `PUT /_admin/license` activates Enterprise features without restart

**Web Console**
- React + Vite SPA at `/_console/` with 14 pages (Dashboard, Buckets, Objects, Users, Tenants, Identity, Tables, Table Sharing, Monitoring, Nodes & Drives, Policies, Encryption, Topology, License)
- Tenant-aware — admin-only pages hidden for tenant users
- Session cookies with HMAC, OIDC SSO support

**Monitoring**
- Prometheus metrics on all services (`/metrics` endpoint)
- Per-operation counters and latency histograms for S3, Iceberg, and block storage
- Locality metrics breaking reads down by topology distance (same-rack / same-zone / remote)
- Grafana dashboards for cluster overview, S3 operations, OSD detail, block storage, and disk health

**Architecture**
- Stateless gateways — scale horizontally behind a load balancer
- openraft 0.9 metadata cluster with redb persistence; single-pod or 3+-pod HA via `meta.replicas`
- Raw disk I/O (O_DIRECT / F_NOCACHE) with WAL, B-tree index, and ARC cache
- Pure Rust — no C/C++ dependencies for core functionality

## Architecture

```
                ┌───────────────────────────────────────────────┐
                │                    Clients                    │
                │  S3 (aws-cli, boto3)  Iceberg (Spark/Trino)   │
                │  Block (iSCSI/NVMe-oF)  Delta Sharing         │
                └───────────────────────┬───────────────────────┘
                                        │
                       ┌────────────────▼────────────────┐
                       │        Gateway (:9000)          │
                       │  S3 API + Iceberg REST Catalog  │
                       │  Delta Sharing + Block Gateway   │
                       │  SigV4 Auth + Erasure Encoding   │
                       └────────────────┬────────────────┘
                                    │
                 ┌──────────────────┼──────────────────┐
                 │                  │                  │
       ┌─────────▼─────────┐        │       ┌─────────▼─────────┐
       │  Meta (:9100)     │◄────── ┴──────►│  Meta (:9100)     │
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
git clone https://github.com/cloudomate/objectio.git
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

Every table operation must name an existing warehouse (via `?warehouse=X`
on `/iceberg/v1/config`). Warehouses are created via
`POST /_admin/warehouses {"name":"X"}` — meta auto-provisions a backing
bucket `iceberg-X` and records its S3 location. Omitting `?warehouse=` or
naming one that doesn't exist returns 400.

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
| **Gateway** | `objectio-gateway` | 9000 | S3 API + Iceberg REST Catalog + Delta Sharing + web console (stateless, horizontally scalable) |
| **Meta** | `objectio-meta` | 9100 | Metadata cluster — buckets, objects, volumes, IAM, tenants, CRUSH placement, license cap enforcement (Raft + redb) |
| **OSD** | `objectio-osd` | 9200 | Storage daemon — shard storage, block I/O, raw disk with WAL |
| **Block Gateway** | `objectio-block-gateway` | 9300 (gRPC) / 10809 (NBD) | Block service + NBD target; erasure-encodes block writes to OSDs |
| **CLI** | `objectio-cli` | — | Admin CLI (users, tenants, keys, volumes, snapshots, topology, license) |
| **Install** | `objectio-install` | — | Installation helper |

## S3 Compatibility

| Category | Operations |
|----------|------------|
| **Bucket** | CreateBucket, DeleteBucket, HeadBucket, ListBuckets, GetBucketLocation |
| **Object** | GetObject, PutObject, DeleteObject, DeleteObjects (batch), HeadObject, CopyObject |
| **Listing** | ListObjectsV2, ListObjectVersions |
| **Multipart** | CreateMultipartUpload, UploadPart, CompleteMultipartUpload, AbortMultipartUpload, ListParts, ListMultipartUploads |
| **Policy** | GetBucketPolicy, PutBucketPolicy, DeleteBucketPolicy |
| **Versioning** | PutBucketVersioning, GetBucketVersioning |
| **Auth** | AWS Signature V4 (SigV4) |

## Iceberg REST Catalog

**Core Operations**

| Endpoint | Description |
|----------|-------------|
| `GET /iceberg/v1/config` | Catalog configuration |
| `GET /iceberg/v1/namespaces` | List namespaces |
| `POST /iceberg/v1/namespaces` | Create namespace |
| `GET /iceberg/v1/namespaces/{ns}` | Load namespace |
| `HEAD /iceberg/v1/namespaces/{ns}` | Check namespace exists |
| `DELETE /iceberg/v1/namespaces/{ns}` | Drop namespace |
| `POST /iceberg/v1/namespaces/{ns}/properties` | Update namespace properties |
| `GET /iceberg/v1/namespaces/{ns}/tables` | List tables |
| `POST /iceberg/v1/namespaces/{ns}/tables` | Create table |
| `GET /iceberg/v1/namespaces/{ns}/tables/{table}` | Load table (applies data filters) |
| `POST /iceberg/v1/namespaces/{ns}/tables/{table}` | Commit table update (CAS) |
| `HEAD /iceberg/v1/namespaces/{ns}/tables/{table}` | Check table exists |
| `DELETE /iceberg/v1/namespaces/{ns}/tables/{table}` | Drop table |
| `POST /iceberg/v1/tables/rename` | Rename table |

**Access Control & Governance**

| Endpoint | Description |
|----------|-------------|
| `PUT /iceberg/v1/catalog/policy` | Set catalog-level default policy |
| `PUT /iceberg/v1/namespaces/{ns}/policy` | Set namespace policy |
| `PUT /iceberg/v1/namespaces/{ns}/tables/{table}/policy` | Set table policy |
| `GET /iceberg/v1/namespaces/{ns}/effective-policy` | Show merged hierarchical policy |
| `POST /iceberg/v1/simulate-policy` | Simulate policy decision |
| `PUT /iceberg/v1/namespaces/{ns}/role-binding` | Bind role to principals |

Built-in roles: `CatalogAdmin`, `NamespaceOwner`, `TableWriter`, `TableReader`.

**Tags, Quotas & Data Filters**

| Endpoint | Description |
|----------|-------------|
| `GET /iceberg/v1/namespaces/{ns}/tags` | Get namespace tags |
| `PUT /iceberg/v1/namespaces/{ns}/tags` | Set namespace tags |
| `GET /iceberg/v1/namespaces/{ns}/tables/{table}/tags` | Get table tags |
| `PUT /iceberg/v1/namespaces/{ns}/tables/{table}/tags` | Set table tags |
| `GET /iceberg/v1/namespaces/{ns}/quota` | Get namespace quota and usage |
| `PUT /iceberg/v1/namespaces/{ns}/quota` | Set namespace max table quota |
| `GET /iceberg/v1/namespaces/{ns}/encryption-policy` | Get encryption policy |
| `PUT /iceberg/v1/namespaces/{ns}/encryption-policy` | Set encryption location prefix |
| `POST /iceberg/v1/namespaces/{ns}/tables/{table}/data-filters` | Create column/row data filter |
| `GET /iceberg/v1/namespaces/{ns}/tables/{table}/data-filters` | List data filters |
| `DELETE /iceberg/v1/namespaces/{ns}/tables/{table}/data-filters/{id}` | Delete data filter |

Access control uses IAM-style policies with `iceberg:` action prefix (e.g., `iceberg:LoadTable`) and `arn:obio:iceberg:::` ARNs. Policies are evaluated hierarchically from catalog root through ancestor namespaces to the target.

## Block Storage Features

| Feature | Description |
|---------|-------------|
| **Volumes** | Create, resize (grow), delete distributed block volumes |
| **Thin Provisioning** | Storage allocated only on write |
| **Snapshots** | Instant copy-on-write snapshots |
| **Clones** | Writable clones from snapshots |
| **QoS** | Per-volume IOPS and bandwidth limits with burst support |
| **Protocols** | iSCSI, NVMe-oF, NBD attachment targets |

## Delta Sharing Protocol

Secure cross-organization data sharing via the [Delta Sharing](https://delta.io/sharing/) open protocol. Exposes Iceberg tables through the Delta Sharing wire protocol with bearer token authentication and presigned S3 URLs for direct data access.

**Client API** (`/delta-sharing/v1`)

| Endpoint | Description |
|----------|-------------|
| `GET /shares` | List shares accessible to recipient |
| `GET /shares/{share}/schemas` | List schemas in a share |
| `GET /shares/{share}/schemas/{schema}/tables` | List tables in schema |
| `GET /shares/{share}/all-tables` | List all tables in share |
| `GET /shares/{share}/schemas/{schema}/tables/{table}/version` | Get table version |
| `GET /shares/{share}/schemas/{schema}/tables/{table}/metadata` | Get table metadata (NDJSON) |
| `POST /shares/{share}/schemas/{schema}/tables/{table}/query` | Query table (returns presigned file URLs) |

**Admin API** (`/_admin/delta-sharing`)

| Endpoint | Description |
|----------|-------------|
| `POST /shares` | Create a share |
| `DELETE /shares/{share}` | Drop a share |
| `POST /shares/{share}/tables` | Add table to share |
| `DELETE /shares/{share}/schemas/{schema}/tables/{table}` | Remove table from share |
| `POST /recipients` | Create recipient (returns bearer token) |
| `DELETE /recipients/{name}` | Delete recipient |

## Admin API

Gateway admin endpoints at `/_admin/*`. All accept either SigV4 or the
console session cookie. System-admin-only endpoints are enforced in
code; tenant admins have a separate scope-limited variant.

**Identity**

| Endpoint | Description |
|---|---|
| `GET/POST /_admin/users` | List / create users |
| `DELETE /_admin/users/{id}` | Delete user |
| `GET/POST /_admin/users/{id}/access-keys` | List / create keys |
| `DELETE /_admin/access-keys/{id}` | Delete key |
| `GET/POST/PUT/DELETE /_admin/tenants[/{name}]` | Tenant CRUD *(Enterprise)* |
| `POST /_admin/tenants/{name}/admins` | Grant tenant-admin role *(Enterprise)* |
| `DELETE /_admin/tenants/{name}/admins/{user}` | Revoke tenant-admin *(Enterprise)* |
| `GET/POST/PUT/DELETE /_admin/policies[/{name}]` | IAM policy CRUD |
| `POST /_admin/policies/attach` \| `/detach` | Attach / detach policy |

**Cluster**

| Endpoint | Description |
|---|---|
| `GET /health` | Health check |
| `GET /metrics` | Prometheus metrics |
| `GET /_admin/cluster-info` | Gateway self-topology + per-OSD distance |
| `GET /_admin/topology` | Aggregated region→zone→dc→rack→host→OSDs tree |
| `GET /_admin/placement/validate?pool=NAME` | Is pool's failure_domain satisfiable? |
| `GET /_admin/nodes` | OSD / disk status |
| `GET/POST/PUT/DELETE /_admin/pools[/{name}]` | Storage pool CRUD |

**KMS & License**

| Endpoint | Description |
|---|---|
| `GET /_admin/kms/status` \| `GET /_admin/kms/keys` | KMS backend + key list |
| `POST /_admin/kms/keys` \| `DELETE /_admin/kms/keys/{id}` | Key lifecycle |
| `GET/PUT/DELETE /_admin/kms/config` | Select/swap KMS backend (local \| vault) |
| `GET /_admin/license` | Tier + features + live usage (nodes, raw capacity) |
| `PUT /_admin/license` | Install a signed license (hot-swap) |
| `DELETE /_admin/license` | Revert to Community |

## CLI Reference

The `objectio-cli` connects to the Meta service for cluster administration.

| Group | Commands |
|-------|----------|
| **cluster** | `status`, `topology` |
| **node** | `list`, `show`, `drain` |
| **disk** | `list`, `show` |
| **bucket** | `list`, `show` |
| **user** | `list`, `create`, `delete` (with `--tenant`) |
| **key** | `list`, `create`, `delete` |
| **group** | `list`, `create`, `delete`, `add-user`, `remove-user`, `user-groups` |
| **tenant** | `list`, `create`, `show`, `delete`, `admin add` / `remove` / `list` |
| **volume** | `list`, `create`, `show`, `resize`, `delete` |
| **snapshot** | `list`, `create`, `show`, `delete`, `clone` |
| **topology** | `show`, `validate <pool>` |
| **license** | `show`, `install <file>`, `verify <file>`, `remove` |

## Data Protection Modes

| Mode | Overhead | Fault Tolerance | Use Case |
|------|----------|-----------------|----------|
| Replication 1 | 1x | None | Dev/test |
| Replication 3 | 3x | 2 failures | Small clusters, low latency |
| EC 4+2 | 1.5x | 2 failures | Production default |
| EC 6+3 | 1.5x | 3 failures | Multi-node production |
| LRC 6+2+2 | 1.67x | 1 rack + 1 disk | Large multi-rack clusters |

## Monitoring

All services export Prometheus metrics at `/metrics`.

| Service | API Port | Metrics Port |
|---------|----------|--------------|
| Gateway | 9000 | 9000 (`/metrics`) |
| Meta | 9100 | 9101 |
| OSD | 9200 | 9201 |

Key metric families: `objectio_s3_requests_total`, `objectio_s3_request_duration_seconds`, `objectio_iceberg_requests_total`, `objectio_iceberg_request_duration_seconds`, `objectio_osd_*`, `objectio_block_*`.

## Documentation

- [Architecture](docs/architecture/README.md) — system design, data flow, CRUSH placement
- [Block Storage](docs/architecture/block-storage.md) — volumes, snapshots, QoS, protocols
- [Data Protection](docs/architecture/data-protection.md) — erasure coding, LRC, replication
- [Storage Engine](docs/storage/README.md) — raw disk I/O, WAL, caching
- [Deployment Guide](docs/deployment/README.md) — Docker, bare-metal, topologies
- [API Reference](docs/api/README.md) — S3 operations, Iceberg REST Catalog, authentication
- [Operations](docs/operations/README.md) — monitoring, failure recovery

## Development

```bash
make build            # Debug build
make build-release    # Release build
make test             # Run all tests
make lint             # Clippy with -D warnings
make fmt              # Check formatting
make ci               # Full CI: fmt + lint + test

# Docker images — `all` is the unified image used by helm
docker build --target all     -t objectio .              # one image, per-service entrypoints
docker build --target gateway -t objectio-gateway .      # gateway only
docker build --target meta    -t objectio-meta .         # meta only
docker build --target osd     -t objectio-osd .          # osd only
docker build --target cli     -t objectio-cli .          # admin CLI only
```

## Requirements

- Rust 1.93+ (edition 2024)
- protobuf-compiler (protoc)
- ISA-L build deps for x86: nasm, autoconf, automake, libtool, libclang-dev
- On ARM, omit `--features isal` (uses portable pure-Rust EC backend)

## Licensing

Dual-licensed:

- Everything outside `enterprise/` — [Apache License 2.0](./LICENSE)
- `enterprise/crates/objectio-iceberg/` + `enterprise/crates/objectio-delta-sharing/` — [Business Source License 1.1](./enterprise/LICENSE); converts to Apache-2.0 on **2030-04-18** under the BUSL change-license clause

BUSL permits reading, self-hosting, and modifying the code — it only
restricts offering the Enterprise features as a paid managed service that
competes with ObjectIO's own offerings. See the [NOTICE](./NOTICE) file
for the full split.

Enterprise features (Iceberg catalog, Delta Sharing, SSE-KMS external
backends, multi-tenancy, OIDC registration, LRC erasure coding) are also
gated at runtime by an Ed25519-signed license file. Without a valid
Enterprise license installed, those endpoints return `403
EnterpriseLicenseRequired`. See
[docs/FEATURES.md](docs/FEATURES.md#10-licensing--tiering) for the full
tier matrix and gate details.

## Roadmap

See [docs/TOPOLOGY-ROADMAP.md](docs/TOPOLOGY-ROADMAP.md) for the
multi-phase topology and maintenance-ergonomics roadmap:

- **Phase 1 ✓** — 5-level failure domains, hard-enforced placement, console topology page
- **Phase 2 ✓** — locality-aware EC reads, `/_admin/cluster-info`, cross-zone metrics
- **Phase 3** — domain drain, node-by-node rolling upgrade, repair throttling
- **Phase 4** — network fabric awareness, rack-local LRC, client-side hints
- **Phase 5** — cross-site async replication
- **Future (Enterprise track)** — immutable OS with A/B partitions + TPM-measured boot
