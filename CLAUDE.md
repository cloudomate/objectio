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

## Architecture

Three-service architecture: **Gateway** (:9000), **Meta** (:9100), **OSD** (:9200).

**Gateway** (stateless, horizontally scalable): Receives S3 requests, authenticates via SigV4, queries Meta for placement, erasure-encodes data, writes shards to OSDs in parallel, returns S3 response.

**Meta** (Raft cluster, 3+ nodes): Stores bucket/object metadata, makes placement decisions via CRUSH algorithm, manages IAM users/access keys. Uses OpenRaft + Redb.

**OSD** (one per disk/node): Stores erasure-coded shards on raw disk with Direct I/O, maintains WAL for durability. Also hosts the block storage service.

### Block Storage

The `objectio-block` crate provides block storage on top of the distributed object layer. It maps logical block addresses (LBAs, 512-byte sectors) to erasure-coded 4MB chunks stored on OSDs. Key capabilities:

- **Thin provisioning**: Storage allocated only on write
- **Snapshots/Clones**: Copy-on-write snapshots and writable clones
- **QoS**: Per-volume IOPS/bandwidth limits with token bucket rate limiting and priority scheduling
- **Protocols**: iSCSI, NVMe-oF, NBD attachment targets
- **Write journal**: Crash-consistent writes via write-ahead journal
- **Write cache**: Coalesces small writes before flushing to chunks

The block gRPC service is defined in `crates/objectio-proto/proto/block.proto` (BlockService) and runs on the OSD.

### Crate Dependency Flow

```text
Gateway → [objectio-s3, objectio-auth, objectio-erasure, objectio-client]
Meta    → [objectio-meta-store, objectio-placement, objectio-proto]
OSD     → [objectio-storage, objectio-block, objectio-erasure, objectio-proto]

objectio-block  → [objectio-client, objectio-proto] (gRPC to OSDs for chunk I/O)
objectio-client → objectio-proto (gRPC stubs)
objectio-proto  → tonic/prost (generated from proto/{storage,metadata,cluster,block}.proto)
All crates      → objectio-common (error types, shared types, config)
```

### Key Crates

- **objectio-common**: Central `Error` enum (thiserror-based), `Result<T>` type alias, shared types. Errors map to HTTP status codes and S3 error codes. Check `is_retryable()` for network errors.
- **objectio-proto**: gRPC definitions auto-generated from `crates/objectio-proto/proto/{storage,metadata,cluster,block}.proto`.
- **objectio-erasure**: Pluggable backends — `rust-simd` (default, portable) or `isal` (x86_64, 3-5x faster). Feature-flag selected.
- **objectio-placement**: CRUSH and CRUSH2 placement algorithms for shard distribution across failure domains.
- **objectio-storage**: Raw disk I/O engine with WAL, block allocation, ARC metadata cache, SMART monitoring. Uses `O_DIRECT`/`F_NOCACHE`.
- **objectio-block**: Block storage layer — VolumeManager, ChunkMapper, WriteCache, WriteJournal, QoS rate limiter. Has its own `BlockError`/`BlockResult` types separate from common errors.
- **objectio-auth**: AWS SigV4 authentication. Feature flags: `builtin` (default), `oidc`, `openfga`, `full`.
- **objectio-s3**: Axum-based S3 API handlers (bucket ops, object ops, multipart upload).

## Workspace Conventions

- **Rust edition 2024**, minimum rustc 1.92
- **Clippy**: `all`, `pedantic`, and `nursery` lints enabled workspace-wide (see `[workspace.lints.clippy]` in root Cargo.toml)
- **unsafe_code**: warn level
- **Async runtime**: Tokio (full features) everywhere
- **HTTP framework**: Axum 0.8 with Tower middleware
- **gRPC**: Tonic 0.12 / Prost 0.13
- **Config pattern**: TOML config files + clap CLI args (CLI overrides config). Services use `--config` flag pointing to TOML.
- **Auth bypass**: `--no-auth` flag on gateway for development/testing

## Ports

| Service | API  | Metrics          |
|---------|------|------------------|
| Gateway | 9000 | 9000 `/metrics`  |
| Meta    | 9100 | 9101             |
| OSD     | 9200 | 9201             |
