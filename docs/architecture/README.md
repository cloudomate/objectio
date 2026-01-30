# ObjectIO Architecture

This section describes the system architecture of ObjectIO.

## Contents

- [Components](components.md) - Detailed component descriptions
- [Data Protection](data-protection.md) - Erasure coding and replication
- [Deployment Options](deployment-options.md) - Configuration by hardware topology

## Overview

ObjectIO is a distributed object storage system with three main layers:

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           S3 Clients                                    │
│              (aws-cli, boto3, s3cmd, any S3-compatible SDK)             │
└─────────────────────────────┬───────────────────────────────────────────┘
                              │ HTTPS (S3 REST API)
                              ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                       S3 Gateway Layer                                  │
│  ┌─────────────┐ ┌─────────────┐ ┌─────────────┐                        │
│  │  Gateway 1  │ │  Gateway 2  │ │  Gateway N  │  (stateless, scale)    │
│  └──────┬──────┘ └──────┬──────┘ └──────┬──────┘                        │
│         └───────────────┼───────────────┘                               │
│                         │                                               │
│  • AWS Signature V4 authentication                                      │
│  • Reed-Solomon / LRC erasure coding                                    │
│  • Streaming upload/download                                            │
│  • Multipart upload coordination                                        │
└─────────────────────────┬───────────────────────────────────────────────┘
                          │ gRPC
                          ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                      Metadata Service                                   │
│  ┌───────────────────────────────────────────────────────────────────┐  │
│  │                    objectio-meta                                  │  │
│  │                                                                   │  │
│  │  Stores ONLY cluster configuration (not object metadata):         │  │
│  │  • Bucket definitions (name, owner, storage_class)               │  │
│  │  • OSD topology (node_id, address, failure_domain)               │  │
│  │  • Bucket policies (access control)                              │  │
│  │  • CRUSH 2.0 placement algorithm                                 │  │
│  └───────────────────────────────────────────────────────────────────┘  │
└─────────────────────────┬───────────────────────────────────────────────┘
                          │ gRPC
                          ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                      Storage Node Layer (OSDs)                          │
│                                                                         │
│  ┌─────────────────────────────────────────────────────────────────┐    │
│  │                     Datacenter 1                                │    │
│  │  ┌───────────┐  ┌───────────┐  ┌───────────┐                    │    │
│  │  │   Rack A  │  │   Rack B  │  │   Rack C  │                    │    │
│  │  │ ┌───────┐ │  │ ┌───────┐ │  │ ┌───────┐ │                    │    │
│  │  │ │ OSD 1 │ │  │ │ OSD 3 │ │  │ │ OSD 5 │ │                    │    │
│  │  │ │D1 D2  │ │  │ │D1 D2  │ │  │ │D1 D2  │ │                    │    │
│  │  │ └───────┘ │  │ └───────┘ │  │ └───────┘ │                    │    │
│  │  └───────────┘  └───────────┘  └───────────┘                    │    │
│  └─────────────────────────────────────────────────────────────────┘    │
│                                                                         │
│  • Raw disk I/O (O_DIRECT / F_NOCACHE)                                  │
│  • B-tree + WAL metadata storage                                        │
│  • Object metadata on primary OSD                                       │
│  • Background scrubbing and repair                                      │
└─────────────────────────────────────────────────────────────────────────┘
```

## Distributed Metadata Architecture

ObjectIO uses a **distributed metadata model** (similar to Ceph) where:

- **Object metadata** is stored on the **primary OSD** (determined by CRUSH)
- **Metadata service** only stores **cluster configuration** (buckets, topology, policies)
- **CRUSH algorithm** provides deterministic placement - no central object index needed

### Why Distributed Metadata?

| Centralized (rejected) | Distributed (chosen) |
|------------------------|----------------------|
| Single point of failure | No SPOF for object lookups |
| Metadata service becomes bottleneck | Scales with OSD count |
| Requires Raft for HA | OSD already has persistence |
| Object index grows unbounded | Metadata distributed across OSDs |

### What's Stored Where

```
┌─────────────────────────────────────────────────────────────────────────┐
│                      Metadata Service (objectio-meta)                   │
│                                                                         │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐          │
│  │ Bucket Defs     │  │ OSD Topology    │  │ Bucket Policies │          │
│  │ name, owner,    │  │ node_id, addr,  │  │ bucket →        │          │
│  │ storage_class   │  │ failure_domain  │  │ policy_json     │          │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘          │
│                                                                         │
│  Small, rarely changes, simple persistence (redb or JSON snapshot)      │
└─────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────┐
│                      Primary OSD (per object)                           │
│                                                                         │
│  ┌─────────────────────────────────────────────────────────────────┐    │
│  │ Object Metadata (B-tree + WAL, crash-safe)                      │    │
│  │                                                                 │    │
│  │  bucket/key → {                                                 │    │
│  │    object_id: UUID,                                             │    │
│  │    size: u64,                                                   │    │
│  │    etag: String,                                                │    │
│  │    content_type: String,                                        │    │
│  │    created_at: u64,                                             │    │
│  │    stripes: [{                                                  │    │
│  │      ec_k, ec_m, ec_type,                                       │    │
│  │      shards: [{ position, node_id, disk_id, offset }]           │    │
│  │    }]                                                           │    │
│  │  }                                                              │    │
│  └─────────────────────────────────────────────────────────────────┘    │
│                                                                         │
│  Already has persistence: WAL + B-tree + ARC cache + snapshots          │
└─────────────────────────────────────────────────────────────────────────┘
```

### CRUSH Deterministic Placement

CRUSH (Controlled Replication Under Scalable Hashing) computes placement from:

```
CRUSH(bucket, key, topology) → [primary_osd, osd_1, osd_2, ..., osd_k+m-1]
```

- **Deterministic**: Same inputs always produce same outputs
- **No storage needed**: Placement is computed, not looked up
- **Failure-domain aware**: Spreads shards across racks/nodes
- **Stable under changes**: Adding/removing OSDs moves minimal data

## Data Flow

### Write Path (PUT Object)

```
┌────────┐     ┌─────────┐     ┌──────────────┐     ┌─────────────────┐
│ Client │────▶│ Gateway │────▶│ Meta Service │────▶│ CRUSH Placement │
└────────┘     └────┬────┘     └──────────────┘     └────────┬────────┘
                    │                                        │
                    │ 1. GetPlacement(bucket, key)           │
                    │◀───────────────────────────────────────┘
                    │    Returns: [primary, osd1, osd2, ...] + EC params
                    │
                    │ 2. Encode data with Reed-Solomon/LRC
                    │
                    ▼
            ┌───────────────────────────────────────────────────┐
            │           Parallel shard writes to OSDs           │
            │  ┌─────┐ ┌─────┐ ┌─────┐ ┌─────┐ ┌─────┐ ┌─────┐  │
            │  │OSD 0│ │OSD 1│ │OSD 2│ │OSD 3│ │OSD 4│ │OSD 5│  │
            │  │ D0  │ │ D1  │ │ D2  │ │ D3  │ │ P0  │ │ P1  │  │
            │  └─────┘ └─────┘ └─────┘ └─────┘ └─────┘ └─────┘  │
            └───────────────────────────────────────────────────┘
                    │
                    │ 3. PutObjectMeta to PRIMARY OSD
                    │    (stores bucket/key → ObjectMeta)
                    │
                    ▼
            ┌───────────────┐
            │  Primary OSD  │  ◀── Object metadata persisted here
            │  (WAL+B-tree) │
            └───────────────┘
```

1. Gateway requests placement from metadata service
2. Metadata service uses CRUSH to compute OSD list (no lookup, pure computation)
3. Gateway encodes data into k+m shards (Reed-Solomon or LRC)
4. Gateway writes shards to all OSDs in parallel
5. Gateway writes object metadata to **primary OSD** (position 0)
6. Primary OSD persists metadata via WAL + B-tree

### Read Path (GET Object)

```
┌────────┐     ┌─────────┐     ┌──────────────┐     ┌─────────────────┐
│ Client │────▶│ Gateway │────▶│ Meta Service │────▶│ CRUSH Placement │
└────────┘     └────┬────┘     └──────────────┘     └────────┬────────┘
                    │                                        │
                    │ 1. GetPlacement(bucket, key)           │
                    │◀───────────────────────────────────────┘
                    │    Returns: [primary, ...] (just need primary)
                    │
                    │ 2. GetObjectMeta from PRIMARY OSD
                    ▼
            ┌───────────────┐
            │  Primary OSD  │ ──▶ ObjectMeta { stripes, size, etag, ... }
            └───────────────┘
                    │
                    │ 3. Read k shards from OSDs (parallel)
                    ▼
            ┌───────────────────────────────────────────────────┐
            │           Parallel shard reads from OSDs          │
            │  ┌─────┐ ┌─────┐ ┌─────┐ ┌─────┐                  │
            │  │OSD 0│ │OSD 1│ │OSD 2│ │OSD 3│  (only need k)   │
            │  │ D0  │ │ D1  │ │ D2  │ │ D3  │                  │
            │  └─────┘ └─────┘ └─────┘ └─────┘                  │
            └───────────────────────────────────────────────────┘
                    │
                    │ 4. Decode with Reed-Solomon/LRC
                    │
                    ▼
            ┌────────┐
            │ Client │ ◀── Original data reconstructed
            └────────┘
```

1. Gateway requests placement (to find primary OSD)
2. Gateway fetches object metadata from primary OSD
3. Gateway reads k data shards (or uses parity if some unavailable)
4. Gateway decodes and streams to client

### List Path (ListObjects) - Scatter-Gather

Since object metadata is distributed across OSDs (each object on its primary OSD),
ListObjects uses **scatter-gather** to query all OSDs in parallel and merge results.

```
┌────────┐     ┌─────────┐     ┌──────────────┐
│ Client │────▶│ Gateway │────▶│ Meta Service │
└────────┘     └────┬────┘     └──────┬───────┘
                    │                  │
                    │ 1. GetListingNodes()
                    │◀─────────────────┘
                    │    Returns: [OSD 0, OSD 1, ..., OSD N]
                    │             + topology_version
                    │
                    │ 2. Fan out ListObjectsMeta(bucket, prefix) to all OSDs
                    ▼
    ┌───────────────────────────────────────────────────────────┐
    │              Parallel queries to all OSDs                  │
    │  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐       │
    │  │  OSD 0  │  │  OSD 1  │  │  OSD 2  │  │  OSD N  │       │
    │  │ prefix  │  │ prefix  │  │ prefix  │  │ prefix  │       │
    │  │ scan    │  │ scan    │  │ scan    │  │ scan    │       │
    │  └────┬────┘  └────┬────┘  └────┬────┘  └────┬────┘       │
    │       │            │            │            │             │
    │       └────────────┼────────────┼────────────┘             │
    │                    │            │                          │
    │                    ▼            ▼                          │
    │           ┌─────────────────────────────┐                  │
    │           │  K-way Merge (min-heap)     │                  │
    │           │  Sorted by key              │                  │
    │           │  Capped at max_keys         │                  │
    │           └─────────────────────────────┘                  │
    └───────────────────────────────────────────────────────────┘
                    │
                    │ 3. Return merged page + continuation token
                    ▼
            ┌────────┐
            │ Client │ ◀── Sorted object list + NextContinuationToken
            └────────┘
```

**Key Features:**

| Feature                 | Description                                               |
| ----------------------- | --------------------------------------------------------- |
| **Parallel queries**    | Up to 32 concurrent OSD queries with 10s timeout          |
| **K-way merge**         | Min-heap merges sorted results efficiently                |
| **Continuation token**  | Encodes per-shard cursors + HMAC signature                |
| **Failure handling**    | Returns partial results by default (configurable)         |
| **Topology validation** | Token includes topology_version to detect cluster changes |

**Continuation Token Structure:**

```json
{
  "bucket": "my-bucket",
  "prefix": "photos/",
  "shard_cursors": {
    "0": { "last_key": "photos/img_100.jpg", "exhausted": false },
    "1": { "last_key": "photos/img_099.jpg", "exhausted": false },
    "2": { "last_key": "", "exhausted": true }
  },
  "topology_version": 42,
  "signature": "base64-hmac-sha256"
}
```

The token is base64-encoded and signed with HMAC-SHA256 to prevent tampering.

## Design Principles

### 1. EC Within DC, Replication Across DCs

Erasure coding provides storage efficiency within a datacenter, while full replication provides availability across datacenters.

**Why not EC across DCs?**
- Read latency: EC needs k shards, cross-DC latency too high
- Availability: Partial DC failure = degraded reads
- Complexity: Cross-DC quorum is operationally complex
- Rebuild: EC reconstruction across WAN is slow and expensive

### 2. Failure Domain Awareness

CRUSH 2.0 respects the failure domain hierarchy:

```
Region
└── Datacenter
    └── Rack
        └── Node
            └── Disk
```

EC shards are spread across different failure domains to survive failures:
- 4+2 EC: Each shard on a different node (survives 2 node failures)
- LRC 8+2+2: Local parity within groups, global parity across all

### 3. Incremental Scalability

Start with a single node, scale to datacenter scale without data migration:
- Add new OSDs → CRUSH automatically includes them
- Remove OSDs → Data rebalances to remaining nodes
- Minimal data movement via consistent hashing

### 4. Pure Rust

No C/C++ dependencies for core functionality (ISA-L is optional for performance).

## Component Summary

| Component | Binary | Purpose | Status |
|-----------|--------|---------|--------|
| **S3 Gateway** | `objectio-gateway` | S3 REST API, auth, EC encode/decode | ✅ Complete |
| **Metadata Service** | `objectio-meta` | Buckets, topology, CRUSH placement | ✅ Complete |
| **Storage Node (OSD)** | `objectio-osd` | Shard storage, object metadata | ✅ Complete |
| **Admin CLI** | `objectio-cli` | Cluster management | ✅ Complete |
| **Installer** | `objectio-install` | Automated deployment | ✅ Complete |

## Metadata Distribution Summary

| Data | Location | Persistence | Notes |
|------|----------|-------------|-------|
| **Bucket definitions** | Meta service | In-memory (planned: redb) | Small, rarely changes |
| **OSD topology** | Meta service | In-memory (planned: redb) | Updated on OSD join/leave |
| **Bucket policies** | Meta service | In-memory (planned: redb) | Access control rules |
| **CRUSH map** | Meta service | Computed from topology | Not stored, computed |
| **Object metadata** | Primary OSD | WAL + B-tree | Persisted, crash-safe |
| **Shard data** | All k+m OSDs | Raw disk blocks | Checksummed (CRC32C) |

## OSD Metadata Storage

Each OSD has a production-ready metadata storage engine:

```
┌─────────────────────────────────────────────────────────┐
│                    OSD MetadataStore                     │
│  ┌─────────────────────────────────────────────────────┐│
│  │              ARC Cache (hot entries)                ││
│  │              Default: 10,000 entries                ││
│  └─────────────────────────────────────────────────────┘│
│                          │                               │
│  ┌─────────────────────────────────────────────────────┐│
│  │           B-tree Index (in-memory)                  ││
│  │  • Sorted by key for range scans                    ││
│  │  • Periodic snapshot to disk                        ││
│  └─────────────────────────────────────────────────────┘│
│                          │                               │
│  ┌─────────────────────────────────────────────────────┐│
│  │              WAL (append-only)                      ││
│  │  • All mutations logged first (fsync)              ││
│  │  • CRC32C per record                               ││
│  │  • Replay on crash recovery                        ││
│  │  • Truncated after snapshot                        ││
│  └─────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────┘
```

## Striping for Large Objects

Objects larger than a single block (~4 MB) are split into multiple **stripes**. Each stripe is independently erasure-coded.

### Why Striping?

| Problem | Solution |
|---------|----------|
| Block size limit (~4 MB) | Split large objects into stripes |
| Memory efficiency | Process one stripe at a time |
| Parallelism | All stripes processed concurrently |

### Stripe Size Calculation

```
max_shard_size = 4 MB - overhead ≈ 4 MB
max_stripe_data = max_shard_size × k

Example (4+2 EC):
  max_stripe_data = 4 MB × 4 = 16 MB
  50 MB object → ceil(50/16) = 4 stripes
```

### Write Path with Striping

```
┌───────────────────────────────────────────────────────────────────────┐
│                         50 MB Object                                   │
└───────────────────────────────────────────────────────────────────────┘
                              │
                    Split into stripes
                              │
        ┌─────────────────────┼─────────────────────┐
        ▼                     ▼                     ▼
  ┌──────────────┐     ┌──────────────┐     ┌──────────────┐
  │  Stripe 0    │     │  Stripe 1    │     │  Stripe 2    │
  │  (~16 MB)    │     │  (~16 MB)    │     │  (~18 MB)    │
  └──────────────┘     └──────────────┘     └──────────────┘
        │                     │                     │
   EC Encode             EC Encode             EC Encode
   (4+2)                 (4+2)                 (4+2)
        │                     │                     │
        ▼                     ▼                     ▼
  ┌──────────────┐     ┌──────────────┐     ┌──────────────┐
  │ 6 shards     │     │ 6 shards     │     │ 6 shards     │
  │ (~4 MB each) │     │ (~4 MB each) │     │ (~4.5 MB ea) │
  └──────────────┘     └──────────────┘     └──────────────┘
        │                     │                     │
        └─────────────────────┼─────────────────────┘
                              │
                     Write to OSDs in parallel
                     (all stripes concurrently)
```

### Object Metadata with Stripes

```rust
ObjectMeta {
    bucket: "photos",
    key: "vacation/large-video.mp4",
    size: 52_428_800,  // 50 MB
    stripes: [
        StripeMeta { stripe_id: 0, ec_k: 4, ec_m: 2, data_size: 16777216, shards: [...] },
        StripeMeta { stripe_id: 1, ec_k: 4, ec_m: 2, data_size: 16777216, shards: [...] },
        StripeMeta { stripe_id: 2, ec_k: 4, ec_m: 2, data_size: 18874368, shards: [...] },
    ],
    ...
}
```

### Quorum Writes

Writes wait for quorum before returning success:

| EC Profile | Total Shards | Write Quorum | Rationale |
|------------|--------------|--------------|-----------|
| 4+2 | 6 | 4 (k) | Can reconstruct from any k shards |
| 8+4 | 12 | 8 (k) | Can reconstruct from any k shards |
| Replication 3 | 3 | 1 | At least one copy must succeed |

## Centralized IAM

Credentials are managed by the **Metadata Service**, not individual gateways.

### Why Centralized?

| Gateway-based (old) | Meta Service (current) |
|---------------------|------------------------|
| Credentials regenerate on restart | Credentials persist |
| Each gateway has different creds | All gateways share same creds |
| No user management API | Full IAM RPCs available |

### Architecture

```
┌─────────────────┐                    ┌─────────────────────────────────┐
│    Gateway 1    │                    │        Metadata Service         │
│  ┌───────────┐  │     gRPC           │  ┌───────────────────────────┐  │
│  │  Cache    │◄─┼────────────────────┼─►│  IAM Store (in-memory)    │  │
│  │  (5 min)  │  │  GetAccessKey      │  │  - Users                  │  │
│  └───────────┘  │  ForAuth           │  │  - Access Keys            │  │
└─────────────────┘                    │  └───────────────────────────┘  │
                                       │                                 │
┌─────────────────┐                    │  IAM RPCs:                      │
│    Gateway 2    │                    │  - CreateUser                   │
│  ┌───────────┐  │     gRPC           │  - GetUser / ListUsers          │
│  │  Cache    │◄─┼────────────────────┤  - DeleteUser                   │
│  │  (5 min)  │  │                    │  - CreateAccessKey              │
│  └───────────┘  │                    │  - ListAccessKeys               │
└─────────────────┘                    │  - DeleteAccessKey              │
                                       │  - GetAccessKeyForAuth          │
                                       └─────────────────────────────────┘
```

### Admin Bootstrap

On first startup, the metadata service creates an admin user:

```
INFO objectio_meta: ============================================
INFO objectio_meta: Admin credentials (save these!):
INFO objectio_meta:   Access Key ID:     AKIAXXXXXXXXXXXXXXXX
INFO objectio_meta:   Secret Access Key: XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
INFO objectio_meta: ============================================
```

### Credential Caching

Gateways cache credentials for 5 minutes to reduce RPC calls:

```rust
struct CachedCredential {
    secret_access_key: String,
    user_id: String,
    fetched_at: Instant,
}

// Cache hit: use cached secret key
// Cache miss or expired: call GetAccessKeyForAuth RPC
```

## Next Steps

- [Components](components.md) - Detailed component descriptions
- [Data Protection](data-protection.md) - Erasure coding and replication strategies
