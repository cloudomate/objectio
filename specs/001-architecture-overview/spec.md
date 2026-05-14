# 001 - Architecture Overview

## Overview

ObjectIO is a distributed object storage system built on a four-service architecture that provides S3-compatible object storage, block storage, and Iceberg/Delta Sharing integrations.

## System Architecture

### Services

| Service | Port | Description |
|---------|------|-------------|
| Gateway | 9000 | S3 API gateway, Iceberg REST Catalog, Delta Sharing, Console SPA |
| Meta | 9100 | Raft-based metadata store, IAM, placement decisions |
| OSD | 9200 | Object storage daemon, raw disk I/O |
| Block Gateway | 9300/10809 | Block storage gateway (gRPC/NBD) |

### Key Crates

- **objectio-common**: Error types (thiserror-based), Result<T>, shared types
- **objectio-proto**: gRPC stubs from proto/{storage,metadata,cluster,block}.proto
- **objectio-erasure**: EC backends — `rust-simd` (default) or `isal` (x86_64, 3-5x faster)
- **objectio-placement**: CRUSH and CRUSH2 placement algorithms
- **objectio-storage**: Raw disk I/O with WAL, ARC cache, SMART monitoring, O_DIRECT
- **objectio-block**: Block storage layer — VolumeManager, ChunkMapper, WriteCache, WriteJournal, QoS
- **objectio-auth**: AWS SigV4 authentication, PolicyEvaluator, BucketPolicy
- **objectio-s3**: Axum S3 API handlers, multipart upload, admin API
- **objectio-iceberg**: Iceberg REST Catalog at /iceberg/v1/*
- **objectio-delta-sharing**: Delta Sharing protocol at /delta-sharing/v1/*

### Crate Dependency Flow

```
Gateway       → [objectio-s3, objectio-iceberg, objectio-delta-sharing, objectio-auth, objectio-erasure, objectio-client]
Block Gateway → [objectio-block, objectio-erasure, objectio-proto]
Meta          → [objectio-meta-store, objectio-placement, objectio-proto]
OSD           → [objectio-storage, objectio-block, objectio-erasure, objectio-proto]

objectio-iceberg       → [objectio-auth, objectio-proto]
objectio-delta-sharing → [objectio-auth, objectio-proto]
objectio-block         → [objectio-client, objectio-proto]
objectio-client → objectio-proto
objectio-proto  → tonic/prost
All crates      → objectio-common
```

### Binary Crates

| Binary | Purpose |
|--------|---------|
| objectio-gateway | S3 API + Iceberg REST + Delta Sharing + Console SPA |
| objectio-meta | Metadata/Raft service |
| objectio-osd | Storage daemon |
| objectio-cli | Admin CLI |
| objectio-block-gateway | Block storage gateway (gRPC :9300 + NBD :10809) |
| objectio-aio | All-in-one binary for single-process smoke tests |
| objectio-install | Installation helper |

### Key Constants

| Constant | Value | Description |
|----------|-------|-------------|
| Chunk/stripe size | 4MB | Fundamental EC unit |
| LBA size | 512 bytes | Block storage sector |
| LBAs per chunk | 8192 | 4MB / 512B |
| Storage block size | 64KB | Internal allocation unit |
| O_DIRECT alignment | 4KB | Disk I/O alignment |
| Default EC scheme | 4+2 | 4 data + 2 parity shards |

### Ports

| Service | API | Metrics |
|---------|-----|---------|
| Gateway | 9000 | 9000 /metrics |
| Meta | 9100 | 9101 |
| OSD | 9200 | 9201 |
| Block Gateway | 9300 (gRPC) | — |
| Block Gateway | 10809 (NBD) | — |

## Console SPA

The `console/` directory is a Vite + React 19 + TypeScript sub-project with its own npm toolchain. Built artifacts served by gateway at `/_console/`. Two bundles: "ops" (full admin) and "tenant" (self-service).