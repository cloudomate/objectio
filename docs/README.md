# ObjectIO Documentation

ObjectIO is a pure Software Defined Storage (SDS) system written in Rust that provides S3 API compatibility with erasure coding and replication for data protection.

## Quick Links

| Topic | Description |
|-------|-------------|
| [Getting Started](getting-started.md) | Quick start guide for new users |
| [Architecture](architecture/README.md) | System design and components |
| [Storage Engine](storage/README.md) | Disk layout, caching, erasure coding |
| [Deployment](deployment/README.md) | Installation and configuration |
| [API Reference](api/README.md) | S3 API and authentication |
| [Operations](operations/README.md) | Monitoring, recovery, maintenance |

## Documentation Structure

```
docs/
├── README.md                    # This file
├── getting-started.md           # Quick start guide
├── architecture/
│   ├── README.md                # Architecture overview
│   ├── components.md            # Component details
│   └── data-protection.md       # EC and replication
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
│   └── authentication.md        # SigV4 and security
└── operations/
    ├── README.md                # Operations overview
    ├── failure-recovery.md      # Failure handling
    └── monitoring.md            # Metrics and health
```

## Key Features

- **S3 API Compatibility**: Works with aws-cli, boto3, s3cmd, and any S3-compatible SDK
- **Erasure Coding**: Storage-efficient data protection (4+2, 8+4, LRC)
- **Raw Disk Access**: O_DIRECT/F_NOCACHE for maximum performance
- **Flexible Deployment**: Single-node to multi-datacenter scale
- **Pure Rust**: No C/C++ dependencies for core functionality

## System Components

| Component | Binary | Purpose |
|-----------|--------|---------|
| S3 Gateway | `objectio-gateway` | S3 REST API, authentication |
| Metadata Service | `objectio-meta` | Bucket/object metadata (in-memory) |
| Storage Node (OSD) | `objectio-osd` | Raw disk storage, erasure coding |
| Admin CLI | `objectio-cli` | Cluster management |
| Installer | `objectio-install` | Automated deployment |

## Implementation Status

| Phase | Status | Notes |
|-------|--------|-------|
| Phase 1: Foundation | ✅ Complete | Workspace structure, core types, protobuf |
| Phase 2: Storage Engine | ✅ Complete | Raw disk I/O, B-tree + WAL + ARC cache |
| Phase 3: Cluster Metadata | ⚠️ Partial | In-memory only; Raft + persistence pending |
| Phase 4: S3 API Gateway | ✅ Complete | HTTP server, S3 operations, XML responses |
| Phase 5: Reliability | ⚠️ Partial | Health checks done; repair manager pending |
| Phase 6: Auth & Installer | ✅ Complete | SigV4, bucket policies, installer |
| Phase 7: LRC & Backends | ✅ Complete | ISA-L backend, LRC codes (not wired to API) |
| Phase 8: External IAM | 📋 Planned | OIDC, OpenFGA integration |

### Implementation Gaps

| Component | Planned | Actual | Impact |
|-----------|---------|--------|--------|
| Cluster Metadata | redb + Raft | In-memory HashMap | Data lost on restart; single point of failure |
| Placement | CRUSH 2.0 | Simple hash rotation | Code exists but not wired up |
| LRC API | Exposed via config | Backend only | LRC encoding works but not selectable |
| io_uring | Async I/O | Sync O_DIRECT | Uses standard sync I/O |

> See [Architecture Components](architecture/components.md) for detailed implementation status.

## Technology Stack

| Component | Crate | Status |
|-----------|-------|--------|
| Async Runtime | `tokio` | ✅ Used |
| HTTP Framework | `axum` | ✅ Used |
| gRPC | `tonic` | ✅ Used |
| Erasure Coding | `reed-solomon-simd`, `erasure-isa-l` | ✅ Used |
| Consensus | `openraft` | ⚠️ Dependency exists, not integrated |
| KV Store | `redb` | ⚠️ Dependency exists, not integrated |
| OSD Metadata | Custom B-tree + WAL | ✅ Fully implemented |

## Contributing

See the [architecture documentation](architecture/README.md) for design details before contributing.

## License

See [LICENSE](../LICENSE) for details.
