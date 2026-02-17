# ObjectIO Documentation

ObjectIO is a software-defined storage (SDS) platform written in Rust that provides S3-compatible object storage and distributed block storage with erasure coding for data protection.

## Quick Links

| Topic | Description |
|-------|-------------|
| [Getting Started](getting-started.md) | Quick start guide for new users |
| [Architecture](architecture/README.md) | System design and components |
| [Block Storage](architecture/block-storage.md) | Volumes, snapshots, QoS |
| [Storage Engine](storage/README.md) | Disk layout, caching, erasure coding |
| [Deployment](deployment/README.md) | Installation and configuration |
| [API Reference](api/README.md) | S3 API, Iceberg REST Catalog, and authentication |
| [Operations](operations/README.md) | Monitoring, recovery, maintenance |

## Documentation Structure

```
docs/
├── README.md                    # This file
├── getting-started.md           # Quick start guide
├── DESIGN.md                    # Detailed design document
├── architecture/
│   ├── README.md                # Architecture overview
│   ├── components.md            # Component details
│   ├── block-storage.md         # Block storage design
│   ├── data-protection.md       # EC and replication
│   └── deployment-options.md    # Configuration by topology
├── storage/
│   ├── README.md                # Storage overview
│   ├── disk-layout.md           # Raw disk format
│   ├── caching.md               # Block and metadata caching
│   └── erasure-coding.md        # RS and LRC details
├── deployment/
│   ├── README.md                # Deployment overview
│   ├── topologies.md            # Single-node to multi-DC
│   ├── docker.md                # Container deployment
│   └── configuration.md         # Configuration reference
├── api/
│   ├── README.md                # API overview
│   ├── s3-operations.md         # S3 API reference
│   ├── iceberg-api.md           # Iceberg REST Catalog API
│   └── authentication.md        # SigV4 and security
└── operations/
    ├── README.md                # Operations overview
    ├── failure-recovery.md      # Failure handling
    └── monitoring.md            # Metrics and health
```

## Key Features

- **S3 API Compatibility**: Works with aws-cli, boto3, s3cmd, and any S3-compatible SDK
- **Iceberg REST Catalog**: Apache Iceberg table catalog for Spark, Trino, and Flink
- **Block Storage**: Distributed volumes with thin provisioning, snapshots, clones, and QoS
- **Erasure Coding**: Storage-efficient data protection (4+2, 8+4, LRC)
- **Raw Disk Access**: O_DIRECT/F_NOCACHE for maximum performance
- **Flexible Deployment**: Single-node to multi-datacenter scale
- **Pure Rust**: No C/C++ dependencies for core functionality

## System Components

| Component | Binary | Purpose |
|-----------|--------|---------|
| S3 Gateway | `objectio-gateway` | S3 REST API, Iceberg REST Catalog, authentication, erasure encoding |
| Metadata Service | `objectio-meta` | Bucket/object/volume metadata (redb persistence) |
| Storage Node (OSD) | `objectio-osd` | Raw disk storage, shard I/O, block storage gRPC |
| Admin CLI | `objectio-cli` | Cluster, user, and volume management |
| Installer | `objectio-install` | Automated deployment |

## Implementation Status

| Component | Status | Notes |
|-----------|--------|-------|
| Storage Engine | Complete | Raw disk I/O, B-tree + WAL + ARC cache |
| S3 API Gateway | Complete | HTTP server, S3 operations, XML responses, multipart |
| Metadata Persistence | Complete | redb-backed storage with in-memory cache |
| CRUSH 2.0 Placement | Complete | HRW hashing, rack/node/disk-aware, integrated |
| Erasure Coding | Complete | Reed-Solomon (rust-simd + ISA-L), LRC |
| Authentication | Complete | SigV4, bucket policies, Iceberg policies, IAM users |
| Iceberg REST Catalog | Complete | Namespace/table CRUD, access control, metrics |
| Block Storage | Complete | Volumes, snapshots, clones, QoS, write cache/journal |
| Raft Consensus | Placeholder | openraft dependency exists, state machine scaffolded |
| External IAM | Planned | OIDC, OpenFGA integration |

### Known Gaps

| Area | Current State | Impact |
|------|--------------|--------|
| Raft consensus | Placeholder — single meta node | No HA for metadata (redb provides persistence, not replication) |
| io_uring | Standard sync O_DIRECT | Uses blocking I/O on Tokio spawn_blocking |
| LRC in API | Backend implemented | LRC encoding works but not yet selectable via S3 API |
| Repair manager | Not implemented | Background scrubbing/repair not yet automated |

## Technology Stack

| Component | Crate | Status |
|-----------|-------|--------|
| Async Runtime | `tokio` | In use |
| HTTP Framework | `axum` 0.8 | In use |
| gRPC | `tonic` 0.12 / `prost` 0.13 | In use |
| Erasure Coding | `reed-solomon-simd`, `erasure-isa-l` | In use |
| Consensus | `openraft` 0.9 | Dependency exists, placeholder impl |
| KV Store | `redb` 2.4 | In use (meta persistence) |
| OSD Metadata | Custom B-tree + WAL + ARC | In use |
| Block Storage | `objectio-block` | In use |

## Contributing

See the [architecture documentation](architecture/README.md) for design details before contributing.

## License

[Apache License 2.0](../LICENSE)
