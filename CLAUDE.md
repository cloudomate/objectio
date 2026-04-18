# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build Commands

```bash
make build            # Debug build (all crates, with isal feature)
make build-release    # Release build
make test             # Run all tests (workspace + isal)
make lint             # Clippy with -D warnings
make fmt              # Check formatting
make fmt-fix          # Fix formatting
make ci               # Full CI: fmt + lint + test
make coverage         # Generate code coverage (cargo llvm-cov)
```

Single-crate operations:

```bash
cargo test --package objectio-erasure --features isal
cargo test --package objectio-auth -- test_name
cargo clippy --package objectio-s3 --features isal -- -D warnings
```

Build requires `protobuf-compiler` (protoc). The `isal` feature requires NASM + autoconf + automake + libtool + libclang-dev (x86_64 only; on ARM, omit `--features isal`).

### Docker-based builds (no local Rust needed)

```bash
docker compose run --rm build         # Build workspace
docker compose run --rm test          # Run tests
docker compose run --rm lint          # Run clippy
docker compose run --rm fmt           # Check formatting
docker compose run --rm dev           # Interactive dev shell
```

These use the `docker-compose.yml` in the repo root, building from the `builder-base` Dockerfile stage.

## Deploy & Test

### Local cluster

```bash
make cluster-up       # Start 3 meta + 6 OSD + 1 gateway (4+2 EC)
make cluster-down     # Stop cluster
make cluster-clean    # Stop + wipe all data
make cluster-logs     # Tail all logs
make cluster-status   # Show running services
```

Compose file: `deploy/local-cluster/docker-compose.yml`

Test with AWS CLI (gateway at `http://localhost:9000`):

```bash
aws --endpoint-url http://localhost:9000 s3 mb s3://test-bucket
echo "hello" | aws --endpoint-url http://localhost:9000 s3 cp - s3://test-bucket/hello.txt
aws --endpoint-url http://localhost:9000 s3 cp s3://test-bucket/hello.txt -
curl -r 0-3 http://localhost:9000/test-bucket/hello.txt   # Range request → 206
```

### Docker image builds

The multi-stage `Dockerfile` produces per-service images via `--target`:

```bash
docker build --target gateway -t objectio-gateway .
docker build --target meta    -t objectio-meta .
docker build --target osd     -t objectio-osd .
docker build --target cli     -t objectio-cli .
docker build --target all     -t objectio .       # all-in-one
```

### CI

GitHub Actions (`.github/workflows/ci.yml`) runs on a self-hosted runner:

1. **CI job**: Builds the `deps` Dockerfile stage, then runs `cargo fmt`, `cargo clippy`, `cargo test` inside Docker containers.
2. **Build & Push job** (main branch only): Builds all 5 Docker targets, tags `:latest` + `:$SHA_SHORT`, pushes to the container registry.

### Deploy directory layout

```
deploy/
  local-cluster/          # Local dev cluster (3 meta + 6 OSD + 1 gateway, 4+2 EC)
    docker-compose.yml
    docker-compose.monitoring.yml
  prod/                   # Production template (raw block devices, 7 OSDs)
    docker-compose.yml
  datacore/               # Datacore-specific deployment (3+2 EC, 5 OSDs)
    docker-compose.yml
    config/
  monitoring/             # Datacore monitoring stack
    docker-compose.monitoring.yml
    prometheus/
    grafana/
  single-node-7disk/      # Single-node 7-disk deployment (5+2 EC)
    docker-compose.yml
    config/
```

## Architecture

Four-service architecture: **Gateway** (:9000), **Meta** (:9100), **OSD** (:9200), **Block Gateway** (:9300/:10809).

**Gateway** (stateless, horizontally scalable): Receives S3 requests, authenticates via SigV4, queries Meta for placement, erasure-encodes data, writes shards to OSDs in parallel, returns S3 response. Also hosts the Iceberg REST Catalog at `/iceberg/v1/*` and the Delta Sharing API at `/delta-sharing/v1/*`.

**Meta** (Raft cluster, 3+ nodes): Stores bucket/object metadata, makes placement decisions via CRUSH algorithm, manages IAM users/access keys. Uses OpenRaft + Redb.

**OSD** (one per disk/node): Stores erasure-coded shards on raw disk with Direct I/O, maintains WAL for durability.

**Block Gateway** (stateless): Accepts block I/O over gRPC (BlockService) and NBD, buffers writes in an in-memory WriteCache, erasure-encodes 4MB chunks, and flushes them to OSDs. Bridges block storage volumes to the object layer.

### Block Storage

The `objectio-block` crate provides block storage on top of the distributed object layer. It maps logical block addresses (LBAs, 512-byte sectors) to erasure-coded 4MB chunks stored on OSDs. Key capabilities:

- **Thin provisioning**: Storage allocated only on write
- **Snapshots/Clones**: Copy-on-write snapshots and writable clones
- **QoS**: Per-volume IOPS/bandwidth limits with token bucket rate limiting and priority scheduling
- **Protocols**: iSCSI, NVMe-oF, NBD attachment targets
- **Write journal**: Crash-consistent writes via write-ahead journal
- **Write cache**: Coalesces small writes before flushing to chunks

The block gRPC service is defined in `crates/objectio-proto/proto/block.proto` (BlockService) and runs on the Block Gateway.

### Iceberg REST Catalog

The gateway hosts an Apache Iceberg REST Catalog API at `/iceberg/v1/*`, implemented in `crates/objectio-iceberg`. It provides namespace and table management with metadata persisted via the Meta service (Redb).

- **Endpoints**: `/iceberg/v1/config`, `/iceberg/v1/namespaces`, `/iceberg/v1/namespaces/{ns}`, `/iceberg/v1/namespaces/{ns}/tables`, `/iceberg/v1/namespaces/{ns}/tables/{table}`, `/iceberg/v1/tables/rename`
- **Access control**: IAM-style policies at namespace and table level, reusing `PolicyEvaluator`/`BucketPolicy` from `objectio-auth`. Actions use `iceberg:` prefix (e.g., `iceberg:LoadTable`), resources use `arn:obio:iceberg:::` ARNs. Namespace policies stored in properties under `__policy` key; table policies in proto `policy_json` field.
- **Policy management**: `PUT /iceberg/v1/namespaces/{ns}/policy` and `PUT /iceberg/v1/namespaces/{ns}/tables/{table}/policy` (admin only)
- **Metrics**: `objectio_iceberg_requests_total{operation,status}` counter and `objectio_iceberg_request_duration_seconds{operation}` histogram exported at `/metrics`
- **Gateway flag**: `--warehouse-location` sets the S3 URL prefix for table data (default: `s3://objectio-warehouse`)

### Delta Sharing

The `objectio-delta-sharing` crate implements the [Delta Sharing](https://delta.io/sharing/) open protocol as an Axum sub-router nested at `/delta-sharing` on the gateway. It shares Iceberg tables via the Delta Sharing wire protocol (not Delta Lake format). Key details:

- **Auth**: Uses bearer tokens, NOT SigV4 — the router is mounted **outside** the SigV4 auth middleware layer
- **Data access**: Generates presigned S3 URLs using admin credentials for recipients to download data directly
- **Admin API**: `/_admin/delta-sharing` endpoints for managing shares, schemas, tables, and recipients

### Crate Dependency Flow

```text
Gateway       → [objectio-s3, objectio-iceberg, objectio-delta-sharing, objectio-auth, objectio-erasure, objectio-client]
Block Gateway → [objectio-block, objectio-erasure, objectio-proto] (gRPC BlockService + NBD)
Meta          → [objectio-meta-store, objectio-placement, objectio-proto]
OSD           → [objectio-storage, objectio-block, objectio-erasure, objectio-proto]

objectio-iceberg       → [objectio-auth, objectio-proto] (Iceberg REST Catalog with policy evaluation)
objectio-delta-sharing → [objectio-auth, objectio-proto] (Delta Sharing with bearer token auth)
objectio-block         → [objectio-client, objectio-proto] (gRPC to OSDs for chunk I/O)
objectio-client → objectio-proto (gRPC stubs)
objectio-proto  → tonic/prost (generated from proto/{storage,metadata,cluster,block}.proto)
All crates      → objectio-common (error types, shared types, config)
```

### Key Crates

- **objectio-common**: Central `Error` enum (thiserror-based), `Result<T>` type alias, shared types. Errors map to HTTP status codes and S3 error codes. Helper methods: `is_retryable()`, `is_not_found()`, `http_status_code()`, `s3_error_code()`. Constructors: `Error::internal()`, `Error::not_implemented()`, `Error::invalid_request()`, etc.
- **objectio-proto**: gRPC stubs auto-generated via `crates/objectio-proto/build.rs` from `proto/{storage,metadata,cluster,block}.proto`. This is the only build script in the workspace; changing proto files triggers regeneration.
- **objectio-erasure**: Pluggable backends — `rust-simd` (default, portable) or `isal` (x86_64, 3-5x faster). Feature-flag selected.
- **objectio-placement**: CRUSH and CRUSH2 placement algorithms for shard distribution across failure domains. CRUSH2 (HRW hashing) is recommended; pre-built templates in `crush2::templates`.
- **objectio-storage**: Raw disk I/O engine with WAL, block allocation, ARC metadata cache, SMART monitoring. Uses `O_DIRECT`/`F_NOCACHE`. Key constants: 64KB block size, 4KB alignment, 1GB WAL, 1GB minimum disk.
- **objectio-block**: Block storage layer — VolumeManager, ChunkMapper, WriteCache, WriteJournal, QoS rate limiter. **Uses its own `BlockError`/`BlockResult` types**, not `objectio_common::Error`. Do not mix the two error types.
- **objectio-auth**: AWS SigV4 authentication. Feature flags: `builtin` (default), `oidc`, `openfga`, `full`. Also provides `PolicyEvaluator`/`BucketPolicy`/`RequestContext` used by both S3 bucket policies and Iceberg namespace/table policies.
- **objectio-s3**: Axum-based S3 API handlers (bucket ops, object ops, multipart upload). Admin API at `/_admin/*` endpoints for user/key management. Also exports `IcebergOperation` enum and Prometheus metrics for Iceberg operations.
- **objectio-iceberg**: Iceberg REST Catalog implementation — namespace/table CRUD, access control (`access.rs`), catalog client (`catalog.rs`), Axum handlers (`handlers.rs`). Uses `objectio-auth` for policy evaluation and `objectio-proto` for metadata persistence via gRPC.
- **objectio-delta-sharing**: Delta Sharing protocol server — Axum sub-router mounted at `/delta-sharing` outside SigV4 middleware (uses bearer tokens). Manages shares, schemas, tables, recipients; generates presigned S3 URLs for data access.

### Key Constants

- **Chunk/stripe size**: 4MB (4 × 1024 × 1024) — the fundamental EC unit across object and block storage
- **LBA size**: 512 bytes (block storage sector size)
- **LBAs per chunk**: 8192 (4MB / 512B)
- **Storage block size**: 64KB (internal allocation unit in objectio-storage)
- **O_DIRECT alignment**: 4KB
- **Default EC scheme**: 4+2 (4 data + 2 parity shards)

### Binary Crates

Binaries live in `bin/`, not `src/`:

- `bin/objectio-gateway` — S3 API gateway + Iceberg REST Catalog (Axum router + auth middleware)
- `bin/objectio-meta` — Metadata/Raft service
- `bin/objectio-osd` — Storage daemon (also hosts block storage gRPC)
- `bin/objectio-cli` — Admin CLI for user/volume/cluster management
- `bin/objectio-block-gateway` — Block storage gateway (gRPC BlockService :9300 + NBD :10809), EC-encodes block writes to OSDs
- `bin/objectio-install` — Installation helper

## Workspace Conventions

- **Rust edition 2024**, minimum rustc 1.93
- **Clippy**: `all`, `pedantic`, and `nursery` lints enabled workspace-wide (see `[workspace.lints.clippy]` in root Cargo.toml). No crate-specific overrides — all lint config is workspace-level.
- **unsafe_code**: warn level
- **Async runtime**: Tokio (full features) everywhere
- **HTTP framework**: Axum 0.8 with Tower middleware
- **gRPC**: Tonic 0.12 / Prost 0.13
- **Config pattern**: TOML config files + clap CLI args (CLI overrides config). Services use `--config` flag pointing to TOML.
- **Auth bypass**: `--no-auth` flag on gateway for development/testing
- **Testing**: All tests are colocated (`#[cfg(test)] mod tests`) within source files. Sync tests use `#[test]`; async tests use `#[tokio::test]`. No separate `tests/` integration test directory.
- **Type wrappers**: Newtypes use `derive_more` (e.g., `ObjectId(Uuid)` with `From`/`Into`). Types like `BucketName` validate on construction via `::new()` and offer `::new_unchecked()` for internal use.

## Handler Patterns

**Auth integration**: Axum handlers receive `Extension<AuthResult>` injected by the auth middleware. When `--no-auth` is active, no extension is present — handlers that need conditional auth checking take `Option<Extension<AuthResult>>` and skip policy evaluation when it is `None`. New routes that must be publicly accessible (health checks, etc.) must be registered on the router *before* the auth middleware layer, not after.

**State**: Shared service state is passed as `State<Arc<ServiceState>>`. gRPC clients to OSDs are wrapped in a connection pool (`OsdPool`) that lazily opens connections on first use, deduplicates by address, and sets `max_decoding_message_size`/`max_encoding_message_size` to 100 MB to accommodate large shard transfers.

**Iceberg policy hierarchy**: `access.rs` checks policies from the catalog root down through ancestor namespaces to the target namespace/table in order. Deny at any level wins; all levels must allow.

## Console (React admin UI)

The `console/` directory is a separate Vite + React 19 + TypeScript sub-project — its own npm toolchain, not part of the Cargo workspace. Built artifacts are served by the gateway as a static SPA at `/_console/`.

```bash
cd console
npm install
npm run build    # tsc -b && vite build → console/dist/
npm run dev      # Vite dev server (proxy /api to gateway)
npm run lint     # eslint
```

**Serving**: `bin/objectio-gateway/src/main.rs` mounts `tower_http::ServeDir` at `/_console` with SPA fallback to `index.html`. The directory is read from `$OBJECTIO_CONSOLE_DIR` (default `/usr/share/objectio/console`) — Dockerfile copies `console/dist/` there during image build. For local dev against a debug gateway, point `OBJECTIO_CONSOLE_DIR` at `console/dist/`.

**Console-specific backend routes** (distinct from S3/admin routes; registered on the gateway router): `/_console/api/login`, `/_console/api/session`, `/_console/api/logout`, `/_console/api/me/keys`, `/_console/api/oidc/{enabled,authorize,callback}`. Implementation lives in `bin/objectio-gateway/src/console_auth.rs`.

## Documentation

Design docs and user-facing reference live in `docs/` (`DESIGN.md`, `FEATURES.md`, `architecture/`, `api/`, `deployment/`, `operations/`, `storage/`, `getting-started.md`). Check these before inventing explanations — design rationale for erasure coding, placement, and storage layout is already written down. The `examples/` directory has pyiceberg client scripts and sample config files.

## Ports

| Service       | API         | Metrics          |
|---------------|-------------|------------------|
| Gateway       | 9000        | 9000 `/metrics`  |
| Meta          | 9100        | 9101             |
| OSD           | 9200        | 9201             |
| Block Gateway | 9300 (gRPC) | —                |
| Block Gateway | 10809 (NBD) | —                |
