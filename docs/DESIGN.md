# ObjectIO Design Document

## S3-Compliant Distributed Object Storage System

**Version**: 0.1.0
**Status**: Draft
**Author**: Architecture Team

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [Goals and Non-Goals](#2-goals-and-non-goals)
3. [System Architecture](#3-system-architecture)
4. [Data Protection Model](#4-data-protection-model)
5. [Deployment Topologies](#5-deployment-topologies)
6. [Storage Engine](#6-storage-engine)
   - [Disk Layout](#disk-layout)
   - [Superblock Format](#superblock-format)
   - [Data Block Format](#data-block-format)
   - [Write-Ahead Log (WAL)](#write-ahead-log-wal)
   - [Raw Disk Access](#raw-disk-access)
   - [Caching Architecture](#caching-architecture)
7. [Metadata Service](#7-metadata-service)
8. [S3 API Gateway](#8-s3-api-gateway)
9. [Failure Handling and Recovery](#9-failure-handling-and-recovery)
10. [Security](#10-security)
11. [Project Structure](#11-project-structure)
12. [Implementation Phases](#12-implementation-phases)

---

## 1. Executive Summary

ObjectIO is a pure Software Defined Storage (SDS) system written in Rust that provides:

- **S3 API compatibility** for seamless integration with existing tools and applications
- **Erasure coding** for storage-efficient data protection within datacenters
- **Replication** for geo-redundancy across datacenters
- **Raw disk access** for maximum performance and control
- **Flexible deployment** from single-node to multi-datacenter scale

### Key Design Principles

1. **EC within DC, Replication across DCs** - Erasure coding provides efficiency within a datacenter; full replication provides availability across datacenters
2. **Failure domain awareness** - Data placement respects disk, node, rack, and datacenter boundaries
3. **Incremental scalability** - Start with a single node, scale to datacenter scale
4. **Pure Rust** - No C/C++ dependencies for core functionality

---

## 2. Goals and Non-Goals

### Goals

- Full S3 API compatibility (core operations, multipart upload, versioning)
- Support for single-node multi-disk deployments
- Support for multi-node deployments with rack awareness
- Support for multi-datacenter deployments with geo-replication
- Erasure coding with configurable k+m parameters
- Raw disk access bypassing filesystem overhead
- AWS Signature V4 authentication
- Server-side encryption (AES-256-GCM)
- Bucket policies with IAM-like permissions
- Background data integrity verification (scrubbing)
- Automatic failure detection and data repair

### Non-Goals (v1.0)

- S3 Select (query-in-place)
- S3 Object Lambda
- Cross-region replication with conflict resolution
- HDFS/POSIX filesystem interface
- Tiered storage with automatic migration
- Deduplication

---

## 3. System Architecture

### High-Level Overview

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
│  • Request routing and load balancing                                   │
│  • Streaming upload/download                                            │
│  • Multipart upload coordination                                        │
└─────────────────────────┬───────────────────────────────────────────────┘
                          │ gRPC
                          ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                      Metadata Service                                   │
│  ┌─────────────┐ ┌─────────────┐ ┌─────────────┐                        │
│  │   Meta 1    │◄┼►   Meta 2   │◄┼►   Meta 3   │  (Raft cluster)        │
│  │  (Leader)   │ │  (Follower) │ │  (Follower) │                        │
│  └─────────────┘ └─────────────┘ └─────────────┘                        │
│                                                                         │
│  • Bucket and object metadata                                           │
│  • CRUSH-like placement algorithm                                       │
│  • Cluster topology management                                          │
│  • Distributed locking                                                  │
└─────────────────────────┬───────────────────────────────────────────────┘
                          │ gRPC
                          ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                      Storage Node Layer                                 │
│                                                                         │
│  ┌─────────────────────────────────────────────────────────────────┐    │
│  │                     Datacenter 1                                │    │
│  │  ┌───────────┐  ┌───────────┐  ┌───────────┐                    │    │
│  │  │   Rack A  │  │   Rack B  │  │   Rack C  │                    │    │
│  │  │ ┌───────┐ │  │ ┌───────┐ │  │ ┌───────┐ │                    │    │
│  │  │ │ OSD 1 │ │  │ │ OSD 3 │ │  │ │ OSD 5 │ │                    │    │
│  │  │ │D1 D2  │ │  │ │D1 D2  │ │  │ │D1 D2  │ │                    │    │
│  │  │ └───────┘ │  │ └───────┘ │  │ └───────┘ │                    │    │
│  │  │ ┌───────┐ │  │ ┌───────┐ │  │ ┌───────┐ │                    │    │
│  │  │ │ OSD 2 │ │  │ │ OSD 4 │ │  │ │ OSD 6 │ │                    │    │
│  │  │ │D1 D2  │ │  │ │D1 D2  │ │  │ │D1 D2  │ │                    │    │
│  │  │ └───────┘ │  │ └───────┘ │  │ └───────┘ │                    │    │
│  │  └───────────┘  └───────────┘  └───────────┘                    │    │
│  └─────────────────────────────────────────────────────────────────┘    │
│                                                                         │
│  • Raw disk I/O (O_DIRECT / F_NOCACHE)                                  │
│  • Reed-Solomon erasure coding                                          │
│  • Write-ahead logging                                                  │
│  • Background scrubbing and repair                                      │
└─────────────────────────────────────────────────────────────────────────┘
```

### Component Summary

| Component | Binary | Purpose | Scaling |
|-----------|--------|---------|---------|
| **S3 Gateway** | `objectio-gateway` | S3 REST API, authentication, request routing | Horizontal (stateless) |
| **Metadata Service** | `objectio-meta` | Bucket/object metadata, placement, coordination | 3/5/7 node Raft cluster |
| **Storage Node (OSD)** | `objectio-osd` | Raw disk storage, erasure coding, repair | Add nodes/disks |
| **Admin CLI** | `objectio-cli` | Cluster management, monitoring | N/A |

---

## 4. Data Protection Model

### Core Principle: EC Within DC, Replication Across DCs

```
┌─────────────────────────────────────────────────────────────────────┐
│                        Data Protection Strategy                     │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│   WITHIN DATACENTER:                ACROSS DATACENTERS:             │
│   ═══════════════════               ════════════════════            │
│                                                                     │
│   ┌─────────────────┐               ┌─────────────────┐             │
│   │ Erasure Coding  │               │   Replication   │             │
│   │    (4+2, 8+4)   │               │  (Full Copies)  │             │
│   └─────────────────┘               └─────────────────┘             │
│                                                                     │
│   • Storage efficient (67%)         • Each DC has complete data     │
│   • Shards across racks             • Independent read serving      │
│   • Survives rack failures          • Survives DC failures          │
│   • Fast local rebuild              • Simple resync on recovery     │
│                                                                     │
│   WHY NOT EC across DCs?                                            │
│   ─────────────────────                                             │
│   • Read latency: EC needs k shards, cross-DC latency too high      │
│   • Availability: With EC, partial DC failure = degraded reads      │
│   • Complexity: Cross-DC quorum is operationally complex            │
│   • Rebuild: EC reconstruction across WAN is slow and expensive     │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

### Failure Domain Hierarchy

```rust
/// Failure domains from smallest to largest
pub enum FailureDomain {
    Disk,       // Single disk failure
    Node,       // Server/host failure (all disks on host)
    Rack,       // Rack failure (power, ToR switch)
    Datacenter, // DC/AZ failure (network partition, disaster)
    Region,     // Geographic region failure
}
```

### Placement Strategies

| Strategy | EC/Replication | Data Distribution | Failure Tolerance |
|----------|----------------|-------------------|-------------------|
| `disk` | EC | Shards on different disks, same node | Disk failures only |
| `node` | EC | Shards on different nodes, same rack | Node failures |
| `rack` | EC | Shards on different racks, same DC | Rack failures |
| `datacenter` | **Replication** | Full copy in each DC | DC failures |

### Storage Class Configuration

```yaml
storage_classes:
  # Single-node deployment: EC across local disks
  local:
    type: erasure_coding
    ec_profile: "4+2"    # 4 data + 2 parity shards
    placement: disk      # Spread across disks on same node
    fault_tolerance: 2   # Survives 2 disk failures

  # Single-DC deployment: EC across racks
  standard:
    type: erasure_coding
    ec_profile: "8+4"    # 8 data + 4 parity shards
    placement: rack      # Spread across racks
    fault_tolerance: 4   # Survives 4 rack failures

  # Multi-DC deployment: Replication across DCs
  geo:
    type: replication
    replicas: 3          # 3 full copies
    placement: datacenter
    sync_mode: async     # Async replication between DCs
    fault_tolerance: 2   # Survives 2 DC failures

  # Production hybrid: EC within DC + replication across DCs
  production:
    type: hybrid
    local_protection:
      type: erasure_coding
      ec_profile: "4+2"
      placement: rack
    geo_protection:
      type: replication
      replicas: 3
      sync_mode: async
```

### Erasure Coding Parameters

| Profile | Data (k) | Parity (m) | Total | Storage Efficiency | Fault Tolerance |
|---------|----------|------------|-------|-------------------|-----------------|
| 4+2 | 4 | 2 | 6 | 67% | 2 |
| 6+3 | 6 | 3 | 9 | 67% | 3 |
| 8+4 | 8 | 4 | 12 | 67% | 4 |
| 10+5 | 10 | 5 | 15 | 67% | 5 |
| 4+1 | 4 | 1 | 5 | 80% | 1 |

### Locally Repairable Codes (LRC)

In addition to standard MDS (Maximum Distance Separable) Reed-Solomon, ObjectIO supports **LRC (Locally Repairable Codes)** for reduced repair bandwidth in large clusters.

#### MDS vs LRC Comparison

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    MDS Reed-Solomon (4+2)                                   │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   Data:    ┌────┐ ┌────┐ ┌────┐ ┌────┐                                      │
│            │ D0 │ │ D1 │ │ D2 │ │ D3 │                                      │
│            └────┘ └────┘ └────┘ └────┘                                      │
│   Parity:  ┌────┐ ┌────┐                                                    │
│            │ P0 │ │ P1 │  (Reed-Solomon over all data)                      │
│            └────┘ └────┘                                                    │
│                                                                             │
│   Recovery: ANY shard failure → read k=4 shards, RS decode                  │
│   Repair bandwidth: High (must read 4 shards to recover 1)                  │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│                    LRC (12 data, 2 local, 2 global)                         │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   Group 0:    ┌────┬────┬────┬────┬────┬────┐  ┌────┐                       │
│               │ D0 │ D1 │ D2 │ D3 │ D4 │ D5 │  │LP0 │ (XOR of Group 0)      │
│               └────┴────┴────┴────┴────┴────┘  └────┘                       │
│                                                                             │
│   Group 1:    ┌────┬────┬────┬────┬────┬────┐  ┌────┐                       │
│               │ D6 │ D7 │ D8 │ D9 │D10 │D11 │  │LP1 │ (XOR of Group 1)      │
│               └────┴────┴────┴────┴────┴────┘  └────┘                       │
│                                                                             │
│   Global:     ┌────┬────┐                                                   │
│               │GP0 │GP1 │  (Reed-Solomon over ALL 12 data shards)           │
│               └────┴────┘                                                   │
│                                                                             │
│   Recovery (single failure in group):                                       │
│   • D3 lost → XOR(D0,D1,D2,D4,D5,LP0) = D3  (read 6 shards, not 12!)        │
│                                                                             │
│   Recovery (multiple failures):                                             │
│   • D3 + D9 lost → RS decode using global parity (read 12 shards)           │
│                                                                             │
│   Repair bandwidth: LOW for single failures (local group only)              │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

#### Why LRC?

| Aspect | MDS Reed-Solomon | LRC |
|--------|------------------|-----|
| **Repair bandwidth** | High (read k shards) | Low for single failures (read local group) |
| **Repair speed** | Slower | Faster for common case |
| **Storage overhead** | Lower (e.g., 67% for 4+2) | Slightly higher (e.g., 75% for 12+2+2) |
| **Best for** | Small clusters, max efficiency | Large clusters, fast repair |
| **Fault tolerance** | m failures | l+g failures (with constraints) |

#### LRC Configuration

```rust
/// LRC configuration parameters
pub struct LrcConfig {
    /// Number of data shards (k)
    pub data_shards: u8,         // e.g., 12
    /// Number of local parity shards (l) - one per group
    pub local_parity: u8,        // e.g., 2
    /// Number of global parity shards (g)
    pub global_parity: u8,       // e.g., 2
    /// Computed: data_shards / local_parity
    pub local_group_size: u8,    // e.g., 6
}
```

#### LRC Profiles

| Profile | Data (k) | Local (l) | Global (g) | Groups | Group Size | Total | Efficiency | Fault Tolerance |
|---------|----------|-----------|------------|--------|------------|-------|------------|-----------------|
| LRC 6+2+2 | 6 | 2 | 2 | 2 | 3 | 10 | 60% | 4 (with constraints) |
| LRC 12+2+2 | 12 | 2 | 2 | 2 | 6 | 16 | 75% | 4 (with constraints) |
| LRC 12+3+3 | 12 | 3 | 3 | 3 | 4 | 18 | 67% | 6 (with constraints) |

### Pluggable Erasure Coding Backends

ObjectIO supports multiple erasure coding backends, automatically selecting the optimal one for the platform.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                      Backend Architecture                                   │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   ┌────────────────────────────────────────────────────────────────────┐    │
│   │                     ErasureCodec (High-Level API)                  │    │
│   │  • encode(data) → shards                                           │    │
│   │  • decode(shards) → data                                           │    │
│   │  • try_local_recovery() (LRC only)                                 │    │
│   └────────────────────────────┬───────────────────────────────────────┘    │
│                                │                                            │
│                    ┌───────────┴───────────┐                                │
│                    │    BackendFactory     │                                │
│                    │  • create_mds()       │                                │
│                    │  • create_lrc()       │                                │
│                    │  • detect_best()      │                                │
│                    └───────────┬───────────┘                                │
│                                │                                            │
│              ┌─────────────────┼─────────────────┐                          │
│              │                 │                 │                          │
│       ┌──────▼──────┐   ┌──────▼──────┐   ┌─────▼──────┐                    │
│       │  rust_simd  │   │    ISA-L    │   │   Future   │                    │
│       │  (default)  │   │   (x86)     │   │  backends  │                    │
│       │             │   │             │   │            │                    │
│       │ • Portable  │   │ • AVX/AVX2  │   │ • CUDA     │                    │
│       │ • ARM NEON  │   │ • AVX-512   │   │ • etc.     │                    │
│       │ • Pure Rust │   │ • FFI       │   │            │                    │
│       └─────────────┘   └─────────────┘   └────────────┘                    │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

#### Backend Traits

```rust
/// Core trait for MDS erasure coding
pub trait ErasureBackend: Send + Sync {
    fn data_shards(&self) -> u8;
    fn parity_shards(&self) -> u8;
    fn total_shards(&self) -> u8;
    fn encode(&self, data: &[u8]) -> Result<Vec<Vec<u8>>, ErasureError>;
    fn decode(&self, shards: &mut [Option<Vec<u8>>]) -> Result<Vec<u8>, ErasureError>;
}

/// Extended trait for LRC backends
pub trait LrcBackend: ErasureBackend {
    fn lrc_config(&self) -> &LrcConfig;
    fn local_group(&self, shard_idx: usize) -> Option<usize>;
    fn can_recover_locally(&self, available: &[bool], missing: usize) -> bool;
    fn try_local_recovery(&self, shards: &mut [Option<Vec<u8>>], missing: usize) -> Result<bool, ErasureError>;
}
```

#### Backend Selection

| Architecture | Default Backend | Library | Features |
|--------------|-----------------|---------|----------|
| x86/x86_64 | ISA-L (if feature enabled) | Intel ISA-L FFI | AVX, AVX2, AVX-512 |
| ARM/AArch64 | rust_simd | reed-solomon-simd | NEON |
| Other | rust_simd | reed-solomon-simd | Portable |

```rust
// Automatic backend selection
let backend = BackendFactory::create_mds(&BackendConfig::mds(4, 2))?;

// Force specific backend
let backend = BackendFactory::create_mds(
    &BackendConfig::mds(4, 2).with_backend(BackendType::IsaL)
)?;
```

### Storage Classes with LRC

```yaml
storage_classes:
  # Standard MDS (small clusters, max efficiency)
  standard:
    type: erasure_coding
    ec_type: mds
    ec_profile: "4+2"
    placement: rack

  # LRC for large clusters (fast repair)
  lrc-standard:
    type: erasure_coding
    ec_type: lrc
    ec_profile: "12+2+2"      # 12 data, 2 local parity, 2 global parity
    local_group_size: 6        # 6 data shards per local group
    placement: rack

  # LRC with higher redundancy
  lrc-durable:
    type: erasure_coding
    ec_type: lrc
    ec_profile: "12+3+3"
    local_group_size: 4
    placement: rack

  # Production hybrid with LRC
  production-lrc:
    type: hybrid
    local_protection:
      type: erasure_coding
      ec_type: lrc
      ec_profile: "12+2+2"
      placement: rack
    geo_protection:
      type: replication
      replicas: 3
      sync_mode: async
```

---

## 5. Deployment Topologies

### Topology 1: Single-Node Multi-Disk

```
┌─────────────────────────────────────────────────────┐
│                  Single Server                      │
│                                                     │
│  ┌────────┐ ┌────────┐ ┌────────┐ ┌────────┐        │
│  │ Disk 1 │ │ Disk 2 │ │ Disk 3 │ │ Disk 4 │ ...    │
│  │ Shard1 │ │ Shard2 │ │ Shard3 │ │ Shard4 │        │
│  └────────┘ └────────┘ └────────┘ └────────┘        │
│                                                     │
│  EC 4+2: Spread 6 shards across 6+ disks            │
│                                                     │
│  Components:                                        │
│  • objectio-gateway (port 9000)                     │
│  • objectio-meta (embedded, single-node mode)       │
│  • objectio-osd (manages all disks)                 │
└─────────────────────────────────────────────────────┘

Use Case: Development, edge, small deployments
Survives: Disk failures (up to m disks)
Does NOT survive: Server failure
```

### Topology 2: Multi-Node Single-DC (Rack Aware)

```
┌─────────────────────────────────────────────────────────────────────┐
│                          Datacenter                                 │
│                                                                     │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐      │
│  │     Rack A      │  │     Rack B      │  │     Rack C      │      │
│  │  ┌───────────┐  │  │  ┌───────────┐  │  │  ┌───────────┐  │      │
│  │  │  Node 1   │  │  │  │  Node 3   │  │  │  │  Node 5   │  │      │
│  │  │ D1 D2 D3  │  │  │  │ D1 D2 D3  │  │  │  │ D1 D2 D3  │  │      │
│  │  │ Shards:   │  │  │  │ Shards:   │  │  │  │ Shards:   │  │      │
│  │  │ 1,4,7,10  │  │  │  │ 2,5,8,11  │  │  │  │ 3,6,9,12  │  │      │
│  │  └───────────┘  │  │  └───────────┘  │  │  └───────────┘  │      │
│  │  ┌───────────┐  │  │  ┌───────────┐  │  │  ┌───────────┐  │      │
│  │  │  Node 2   │  │  │  │  Node 4   │  │  │  │  Node 6   │  │      │
│  │  │ D1 D2 D3  │  │  │  │ D1 D2 D3  │  │  │  │ D1 D2 D3  │  │      │
│  │  └───────────┘  │  │  └───────────┘  │  │  └───────────┘  │      │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘      │
│                                                                     │
│  EC 8+4: Shards distributed across racks                            │
│  Metadata: 3-node Raft cluster (one per rack)                       │
└─────────────────────────────────────────────────────────────────────┘

Use Case: Production single-DC deployment
Survives: Multiple disk, node, and rack failures
Does NOT survive: DC-wide failure
```

### Topology 3: Multi-DC / Multi-AZ

```
┌───────────────────────┐    ┌───────────────────────┐    ┌───────────────────────┐
│    Datacenter 1       │    │    Datacenter 2       │    │    Datacenter 3       │
│    (Primary)          │    │    (Secondary)        │    │    (DR)               │
│                       │    │                       │    │                       │
│  ┌─────────────────┐  │    │  ┌─────────────────┐  │    │  ┌─────────────────┐  │
│  │   Full Copy 1   │  │    │  │   Full Copy 2   │  │    │  │   Full Copy 3   │  │
│  │   (EC within)   │◄─┼────┼─►│   (EC within)   │◄─┼────┼─►│   (EC within)   │  │
│  └─────────────────┘  │    │  └─────────────────┘  │    │  └─────────────────┘  │
│         │             │    │         │             │    │         │             │
│    ┌────┴────┐        │    │    ┌────┴────┐        │    │    ┌────┴────┐        │
│    │Racks/   │        │    │    │Racks/   │        │    │    │Racks/   │        │
│    │Nodes    │        │    │    │Nodes    │        │    │    │Nodes    │        │
│    └─────────┘        │    │    └─────────┘        │    │    └─────────┘        │
│                       │    │                       │    │                       │
│  Metadata: Local Raft │    │  Metadata: Local Raft │    │  Metadata: Local Raft │
└───────────────────────┘    └───────────────────────┘    └───────────────────────┘
         │                            │                            │
         └────────────────────────────┼────────────────────────────┘
                                      │
                            Async Replication
                            (configurable sync)

Data Flow:
1. Write arrives at DC1 gateway
2. EC encode + write to DC1 storage nodes
3. Async replicate full object to DC2, DC3
4. Read can be served from any DC

Use Case: Geo-distributed, disaster recovery, compliance
Survives: Complete DC/AZ failure (up to 2 DCs with 3x replication)
```

---

## 6. Storage Engine

### Disk Layout

```
Raw Block Device Layout
══════════════════════════════════════════════════════════════════

Offset 0                                                     End
├──────────┬──────────────┬─────────────┬────────────────────────┤
│Superblock│   WAL Region │Block Bitmap │     Data Region        │
│  (4 KB)  │   (1-4 GB)   │  (variable) │   (remaining space)    │
└──────────┴──────────────┴─────────────┴────────────────────────┘
```

### Superblock Format

```rust
#[repr(C, packed)]
pub struct Superblock {
    /// Magic number: "OBJIO001"
    pub magic: [u8; 8],
    /// On-disk format version
    pub version: u32,
    /// Unique disk identifier (UUID)
    pub disk_id: Uuid,
    /// Total disk size in bytes
    pub total_size: u64,
    /// Block size for data (default: 4 MB)
    pub data_block_size: u32,
    /// Block size for metadata (default: 4 KB)
    pub metadata_block_size: u32,

    // Region offsets
    pub wal_offset: u64,
    pub wal_size: u64,
    pub bitmap_offset: u64,
    pub bitmap_size: u64,
    pub data_offset: u64,

    // Timestamps
    pub created_at: u64,
    pub last_mount: u64,

    /// CRC32C of superblock
    pub checksum: u32,

    /// Reserved for future use
    pub _reserved: [u8; 3952],
}
```

### Data Block Format

```
Block Layout (4 MB default)
══════════════════════════════════════════════════════════════════

┌────────────────────────────────────────────────────────────────┐
│                      Block Header (64 bytes)                   │
├────────────────────────────────────────────────────────────────┤
│ magic[4]     │ "BLOK"                                          │
│ block_type   │ Data(0), Parity(1), Index(2)                    │
│ flags        │ Compressed, Encrypted, etc.                     │
│ block_id     │ UUID (16 bytes)                                 │
│ object_id    │ UUID (16 bytes)                                 │
│ stripe_id    │ u64 - which stripe this belongs to              │
│ stripe_pos   │ u8 - position within stripe (0..k+m-1)          │
│ ec_k, ec_m   │ Erasure coding parameters                       │
│ data_length  │ u32 - actual data size (may be < block size)    │
│ sequence     │ u64 - for ordering                              │
├────────────────────────────────────────────────────────────────┤
│                      Data Payload                              │
│              (block_size - 64 - 32 bytes)                      │
├────────────────────────────────────────────────────────────────┤
│                      Block Footer (32 bytes)                   │
├────────────────────────────────────────────────────────────────┤
│ crc32c       │ CRC32C of header + data                         │
│ xxhash64     │ xxHash64 for fast comparison                    │
│ sha256[20]   │ First 20 bytes of SHA256 (content addressing)   │
└────────────────────────────────────────────────────────────────┘
```

### Write-Ahead Log (WAL)

```rust
/// WAL entry types
pub enum WalEntry {
    /// Begin write transaction
    BeginTxn { txn_id: u64, object_id: ObjectId, timestamp: u64 },

    /// Write a block
    WriteBlock { txn_id: u64, block_id: BlockId, location: BlockLocation, crc: u32 },

    /// Commit transaction (durable)
    Commit { txn_id: u64, timestamp: u64 },

    /// Abort transaction (rollback)
    Abort { txn_id: u64, reason: String },

    /// Delete block
    Delete { txn_id: u64, block_id: BlockId },

    /// Checkpoint (WAL can be truncated before this point)
    Checkpoint { sequence: u64, timestamp: u64 },
}
```

**Write Path:**
1. Receive data from gateway
2. Append `BeginTxn` to WAL
3. Encode with Reed-Solomon (create k+m shards)
4. Write each shard: append `WriteBlock` to WAL, then write data block
5. After write quorum achieved, append `Commit` to WAL
6. Sync WAL to disk
7. Return success to gateway

**Recovery:**
1. Read WAL from last checkpoint
2. Replay committed transactions
3. Rollback uncommitted transactions
4. Truncate WAL at checkpoint

### Raw Disk Access

```rust
/// Platform-specific raw disk I/O
pub struct RawDiskAccess {
    fd: RawFd,
    alignment: usize,  // Typically 4096 bytes
}

impl RawDiskAccess {
    /// Open with direct I/O (bypass page cache)
    #[cfg(target_os = "linux")]
    pub fn open(path: &str) -> io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .custom_flags(libc::O_DIRECT | libc::O_SYNC)
            .open(path)?;
        Ok(Self { fd: file.into_raw_fd(), alignment: 4096 })
    }

    #[cfg(target_os = "macos")]
    pub fn open(path: &str) -> io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)?;
        // Disable page cache on macOS
        unsafe { libc::fcntl(file.as_raw_fd(), libc::F_NOCACHE, 1) };
        Ok(Self { fd: file.into_raw_fd(), alignment: 4096 })
    }

    /// Read aligned data (for direct I/O)
    pub async fn read(&self, offset: u64, buf: &mut AlignedBuffer) -> io::Result<usize>;

    /// Write aligned data (for direct I/O)
    pub async fn write(&self, offset: u64, buf: &AlignedBuffer) -> io::Result<usize>;
}
```

### Caching Architecture

Since ObjectIO uses direct I/O (`O_DIRECT`/`F_NOCACHE`) to bypass the OS page cache for predictable latency and to prevent double-caching, we implement application-level caching with full control over eviction and memory usage.

#### Cache Hierarchy

```
┌─────────────────────────────────────────────────────────────────────┐
│                         S3 Gateway                                  │
│  ┌───────────────────────────────────────────────────────────────┐  │
│  │                Object Cache (optional)                        │  │
│  │  • Hot object data (frequently accessed small objects)        │  │
│  │  • Configurable TTL and size limits                           │  │
│  └───────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────┘
                              │
┌─────────────────────────────▼───────────────────────────────────────┐
│                    Storage Node (OSD)                               │
│                                                                     │
│  ┌───────────────────────────────────────────────────────────────┐  │
│  │                    Block Cache (LRU)                          │  │
│  │  • Recently accessed data blocks                              │  │
│  │  • Write-through or write-back policies                       │  │
│  │  • Per-disk or shared pool configuration                      │  │
│  │  • Statistics: hit ratio, evictions, writebacks               │  │
│  └───────────────────────────────────────────────────────────────┘  │
│                              │                                      │
│  ┌───────────────────────────────────────────────────────────────┐  │
│  │                 Metadata Cache (in-memory)                    │  │
│  │  • Superblock (always cached after mount)                     │  │
│  │  • Block bitmap (lazy-loaded regions)                         │  │
│  │  • Block index entries (LRU)                                  │  │
│  └───────────────────────────────────────────────────────────────┘  │
│                              │                                      │
│  ┌───────────────────────────────────────────────────────────────┐  │
│  │              Raw Disk (O_DIRECT / F_NOCACHE)                  │  │
│  │  • Bypasses OS page cache entirely                            │  │
│  │  • 4KB aligned I/O operations                                 │  │
│  │  • Predictable latency, no double-caching                     │  │
│  └───────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────┘
```

#### Block Cache Design

```rust
/// Cache key uniquely identifies a block across all disks
#[derive(Hash, Eq, PartialEq, Clone, Copy)]
pub struct CacheKey {
    pub disk_id: DiskId,
    pub block_num: u64,
}

/// LRU block cache with dirty tracking for write-back support
pub struct BlockCache {
    /// Cached entries
    entries: RwLock<HashMap<CacheKey, CacheEntry>>,
    /// Maximum entries or bytes to cache
    capacity: CacheCapacity,
    /// LRU ordering (clock-based approximation)
    clock: AtomicU64,
    /// Cache statistics
    stats: CacheStats,
    /// Write policy
    policy: WritePolicy,
}

/// Individual cache entry
pub struct CacheEntry {
    /// Block data (aligned buffer)
    data: Bytes,
    /// Last access timestamp (for LRU)
    last_access: AtomicU64,
    /// True if modified but not yet flushed
    dirty: AtomicBool,
    /// Write timestamp (for write-back ordering)
    modified_at: Option<Instant>,
}

/// Cache capacity can be specified by count or memory
pub enum CacheCapacity {
    /// Maximum number of entries
    Entries(usize),
    /// Maximum memory in bytes
    Bytes(usize),
}

/// Cache statistics for monitoring
#[derive(Default)]
pub struct CacheStats {
    pub hits: AtomicU64,
    pub misses: AtomicU64,
    pub evictions: AtomicU64,
    pub writebacks: AtomicU64,
    pub dirty_bytes: AtomicU64,
}

impl CacheStats {
    /// Hit ratio from 0.0 to 1.0
    pub fn hit_ratio(&self) -> f64 {
        let hits = self.hits.load(Ordering::Relaxed);
        let misses = self.misses.load(Ordering::Relaxed);
        if hits + misses == 0 {
            return 0.0;
        }
        hits as f64 / (hits + misses) as f64
    }
}
```

#### Cache Write Policies

| Policy | Behavior | Use Case |
|--------|----------|----------|
| **Write-through** | Write to disk AND cache simultaneously | Default; strong durability guarantees |
| **Write-back** | Write to cache, async flush to disk | High write throughput, higher risk |
| **Write-around** | Write to disk only, skip cache | Large sequential writes, streaming |

```rust
pub enum WritePolicy {
    /// Write to disk and cache simultaneously (default)
    WriteThrough,
    /// Write to cache, flush asynchronously
    WriteBack {
        /// Max dirty entries before forced flush
        dirty_threshold: usize,
        /// Max time before dirty entry is flushed
        max_dirty_age: Duration,
    },
    /// Write to disk only, do not cache writes
    WriteAround,
}
```

#### Configuration

```yaml
storage:
  cache:
    # Block cache settings
    block_cache:
      enabled: true
      size_mb: 256              # Per-disk cache size
      policy: write_through     # write_through | write_back | write_around
      eviction: lru             # LRU eviction strategy

    # Metadata cache settings
    metadata_cache:
      enabled: true
      size_mb: 64               # Shared across all disks
      bitmap_preload: false     # Preload full bitmap on mount

    # Write-back specific settings (if policy: write_back)
    write_back:
      dirty_threshold: 1000     # Max dirty entries
      flush_interval_ms: 5000   # Background flush interval
      max_dirty_age_ms: 30000   # Force flush after this age
```

#### Cache Operations

```rust
impl BlockCache {
    /// Read a block, checking cache first
    pub async fn read(&self, key: CacheKey, disk: &RawDiskAccess) -> Result<Bytes> {
        // Check cache first
        if let Some(entry) = self.get(&key) {
            self.stats.hits.fetch_add(1, Ordering::Relaxed);
            return Ok(entry.data.clone());
        }

        self.stats.misses.fetch_add(1, Ordering::Relaxed);

        // Read from disk
        let data = disk.read_block(key.block_num).await?;

        // Insert into cache (may evict)
        self.insert(key, data.clone());

        Ok(data)
    }

    /// Write a block according to write policy
    pub async fn write(
        &self,
        key: CacheKey,
        data: Bytes,
        disk: &RawDiskAccess,
    ) -> Result<()> {
        match self.policy {
            WritePolicy::WriteThrough => {
                // Write to disk first
                disk.write_block(key.block_num, &data).await?;
                // Then update cache
                self.insert(key, data);
            }
            WritePolicy::WriteBack { .. } => {
                // Write to cache only, mark dirty
                self.insert_dirty(key, data);
                // Background task will flush
            }
            WritePolicy::WriteAround => {
                // Write to disk only
                disk.write_block(key.block_num, &data).await?;
                // Invalidate cache if present
                self.invalidate(&key);
            }
        }
        Ok(())
    }

    /// Evict least recently used entry
    fn evict_one(&self) -> Option<(CacheKey, CacheEntry)> {
        // Find entry with lowest last_access
        // If dirty, flush to disk first
        // ...
    }

    /// Flush all dirty entries (for write-back mode)
    pub async fn flush_all(&self, disk: &RawDiskAccess) -> Result<usize> {
        let mut flushed = 0;
        for (key, entry) in self.dirty_entries() {
            disk.write_block(key.block_num, &entry.data).await?;
            entry.dirty.store(false, Ordering::Release);
            flushed += 1;
        }
        self.stats.writebacks.fetch_add(flushed as u64, Ordering::Relaxed);
        Ok(flushed)
    }
}
```

#### Prometheus Metrics

```rust
// Exported metrics for monitoring
cache_hits_total{disk_id="..."}
cache_misses_total{disk_id="..."}
cache_hit_ratio{disk_id="..."}
cache_evictions_total{disk_id="..."}
cache_writebacks_total{disk_id="..."}
cache_dirty_bytes{disk_id="..."}
cache_entries{disk_id="..."}
cache_size_bytes{disk_id="..."}
```

### OSD Metadata Storage

Each OSD maintains local metadata about shards stored on its disks. This metadata is stored using a hybrid architecture optimized for the OSD workload pattern (write-heavy mutations, read-heavy lookups).

#### Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                        MetadataStore                                 │
│  ┌───────────────────────────────────────────────────────────────┐  │
│  │                   ARC Cache (hot entries)                     │  │
│  │  • Adaptive Replacement Cache                                 │  │
│  │  • Automatically tunes for recency vs frequency patterns      │  │
│  │  • Ghost entries track recently evicted keys                  │  │
│  └───────────────────────────────────────────────────────────────┘  │
│                              │                                       │
│  ┌───────────────────────────────────────────────────────────────┐  │
│  │                B-tree Index (in-memory)                       │  │
│  │  • Sorted keys for efficient range scans                      │  │
│  │  • Periodic snapshots to disk for durability                  │  │
│  │  • Fast point lookups O(log n)                                │  │
│  └───────────────────────────────────────────────────────────────┘  │
│                              │                                       │
│  ┌───────────────────────────────────────────────────────────────┐  │
│  │                   WAL (append-only)                           │  │
│  │  • All mutations logged before applying                       │  │
│  │  • CRC32C checksums per record                                │  │
│  │  • LSN (Log Sequence Number) for ordering                     │  │
│  │  • Truncated after successful snapshot                        │  │
│  └───────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────┘
```

#### Why Not redb/LMDB for OSD Metadata?

While redb is excellent for the global metadata service, OSD-local metadata has different requirements:

| Aspect | Global Metadata (redb) | OSD Metadata (Custom) |
|--------|------------------------|------------------------|
| **Scale** | Millions of objects | Millions of shards per disk |
| **Access** | Raft-replicated, strong consistency | Local-only, no replication |
| **Workload** | Mixed read/write | Write-heavy (shard creation/deletion) |
| **Recovery** | Raft log replay | WAL + snapshot replay |
| **Latency** | Network-bound | Disk I/O bound |
| **Control** | Generic KV store sufficient | Need shard-aware indexing |

The custom hybrid engine provides:
- **Predictable write latency**: Append-only WAL, no random writes during mutations
- **Fast recovery**: Snapshot + WAL replay, no full B-tree rebuild
- **Memory efficiency**: B-tree in memory, not mmap'd (better control)
- **Cache tuning**: ARC adapts to workload patterns automatically

#### Data Structures

```rust
/// Key for metadata entries (designed for prefix scanning)
pub struct MetadataKey(Vec<u8>);

impl MetadataKey {
    /// Shard key: s:{object_id}:{shard_pos}
    pub fn shard(object_id: &[u8; 16], shard_position: u8) -> Self;

    /// Object key: o:{object_id}
    pub fn object(object_id: &[u8; 16]) -> Self;

    /// Block key: b:{block_num} (big-endian for sorting)
    pub fn block(block_num: u64) -> Self;
}

/// Shard metadata stored on this OSD
pub struct ShardMeta {
    pub object_id: [u8; 16],
    pub shard_position: u8,
    pub block_num: u64,
    pub size: u64,
    pub checksum: u32,
    pub created_at: u64,
    pub last_verified: u64,
    pub shard_type: u8,      // 0=Data, 1=LocalParity, 2=GlobalParity
    pub local_group: u8,     // For LRC: group index, 255=global
}
```

#### WAL Record Format

```
+--------+------+--------+------+--------+
| Magic  | LSN  | Length | Data | CRC32C |
| 4B     | 8B   | 4B     | var  | 4B     |
+--------+------+--------+------+--------+

Magic: 0x4D57414C ("MWAL")
LSN: Monotonically increasing sequence number
Data: Serialized MetadataOp (Put, Delete, or Batch)
CRC32C: Checksum of magic + LSN + length + data
```

#### Write Path

```
1. Serialize operation → WAL record
2. Append record to WAL → fsync (configurable)
3. Apply to B-tree index (in-memory)
4. Update ARC cache (if key was cached)
5. Return LSN to caller

Durability: Once WAL is synced, mutation is durable.
             Index can be rebuilt from WAL if crash occurs.
```

#### Read Path

```
1. Check ARC cache
   └─ HIT: Return value, update access tracking
   └─ MISS: Continue to step 2

2. Lookup in B-tree index
   └─ FOUND: Populate cache, return value
   └─ NOT FOUND: Return None
```

#### Recovery

```
1. Find latest snapshot file: meta_{lsn}.snapshot
2. Load snapshot into B-tree (O(n) entries)
3. Open WAL, find entries with LSN > snapshot_lsn
4. Replay WAL entries sequentially:
   - Put → index.put(key, value, lsn)
   - Delete → index.delete(key, lsn)
5. Ready to serve requests

Recovery time: O(snapshot_size + WAL_since_snapshot)
```

#### Compaction

Background compaction runs periodically to:

1. **Snapshot B-tree** → Write all entries to new snapshot file
2. **Truncate WAL** → Remove entries before snapshot LSN
3. **Cleanup old snapshots** → Keep N most recent (default: 2)

```rust
pub struct MetadataStoreConfig {
    /// Snapshot after this many mutations
    pub snapshot_threshold: u64,      // Default: 10,000

    /// Max WAL size before forced snapshot
    pub max_wal_size_bytes: u64,      // Default: 64 MB

    /// Keep this many old snapshots
    pub snapshot_retention: usize,    // Default: 2

    /// Background compaction interval
    pub compaction_interval: Duration, // Default: 60s
}
```

#### ARC Cache (Adaptive Replacement Cache)

ARC improves on LRU by tracking both recency and frequency:

```
┌─────────────────────────────────────────────────────────────┐
│                      ARC Lists                               │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│   T1 (Recent):     Keys accessed once recently              │
│   T2 (Frequent):   Keys accessed multiple times             │
│   B1 (Ghost):      Recently evicted from T1 (keys only)     │
│   B2 (Ghost):      Recently evicted from T2 (keys only)     │
│                                                             │
│   Adaptation:                                               │
│   • Hit in B1 → Increase T1 target size (favor recency)     │
│   • Hit in B2 → Decrease T1 target size (favor frequency)   │
│                                                             │
└─────────────────────────────────────────────────────────────┘

Advantage over LRU:
- Scan-resistant: One-time accesses don't evict hot data
- Self-tuning: Adapts to workload without configuration
- Ghost entries: Zero-cost tracking of eviction history
```

#### Configuration

```yaml
storage:
  metadata:
    # WAL settings
    wal:
      sync_on_write: true           # fsync after each write
      max_size_mb: 64               # Trigger snapshot when exceeded

    # B-tree settings
    btree:
      snapshot_threshold: 10000     # Mutations before snapshot
      snapshot_retention: 2         # Keep N old snapshots

    # Cache settings
    cache:
      size: 10000                   # Number of entries to cache

    # Background compaction
    compaction:
      enabled: true
      interval_secs: 60
```

#### Metrics

```rust
// MetadataStore statistics
pub struct MetadataStoreStats {
    pub entry_count: u64,           // Total entries in index
    pub wal_size: u64,              // Current WAL size in bytes
    pub wal_lsn: u64,               // Current WAL LSN
    pub last_snapshot_lsn: u64,     // LSN of last snapshot
    pub cache_size: usize,          // Cached entries
    pub cache_hit_ratio: f64,       // 0.0 to 1.0
    pub cache_hits: u64,
    pub cache_misses: u64,
}
```

---

## 7. Metadata Service

### Raft Consensus

- **Implementation**: `openraft` crate (async-native Rust)
- **State machine storage**: `redb` (pure Rust, ACID, stable file format)
- **Cluster size**: 3, 5, or 7 nodes (odd number for quorum)

### Metadata Schema

```rust
/// Bucket metadata
pub struct BucketMeta {
    pub name: String,
    pub owner: UserId,
    pub created_at: u64,
    pub versioning: VersioningState,  // Disabled, Enabled, Suspended
    pub storage_class: String,
    pub policy: Option<BucketPolicy>,
    pub cors: Option<CorsConfig>,
    pub encryption: Option<EncryptionConfig>,
}

/// Object metadata
pub struct ObjectMeta {
    pub bucket: String,
    pub key: String,
    pub version_id: Option<String>,
    pub size: u64,
    pub etag: String,  // MD5 or composite for multipart
    pub content_type: String,
    pub created_at: u64,
    pub modified_at: u64,
    pub storage_class: String,
    pub user_metadata: HashMap<String, String>,

    /// Location information
    pub stripes: Vec<StripeMeta>,

    /// Encryption
    pub sse_algorithm: Option<String>,
    pub sse_key_id: Option<String>,
}

/// Stripe metadata (EC group)
pub struct StripeMeta {
    pub stripe_id: u64,
    pub ec_k: u8,
    pub ec_m: u8,
    pub shards: Vec<ShardLocation>,
}

/// Shard location
pub struct ShardLocation {
    pub shard_position: u8,  // 0..k-1 for data, k..k+m-1 for parity
    pub node_id: NodeId,
    pub disk_id: DiskId,
    pub block_offset: u64,
}
```

### Placement Algorithm (CRUSH-like)

```rust
/// Cluster topology
pub struct ClusterTopology {
    pub regions: HashMap<String, Region>,
}

pub struct Region {
    pub datacenters: HashMap<String, Datacenter>,
}

pub struct Datacenter {
    pub racks: HashMap<String, Rack>,
}

pub struct Rack {
    pub nodes: HashMap<NodeId, NodeInfo>,
}

pub struct NodeInfo {
    pub id: NodeId,
    pub address: SocketAddr,
    pub disks: Vec<DiskInfo>,
    pub weight: f64,
    pub status: NodeStatus,
}

impl PlacementPolicy {
    /// Compute placement for a new object
    pub fn place(
        &self,
        object_id: &ObjectId,
        storage_class: &StorageClass,
        topology: &ClusterTopology,
    ) -> Result<Vec<ShardPlacement>, PlacementError> {
        match storage_class.protection_type {
            ProtectionType::ErasureCoding { k, m, placement } => {
                self.place_ec_shards(object_id, k, m, placement, topology)
            }
            ProtectionType::Replication { replicas, placement } => {
                self.place_replicas(object_id, replicas, placement, topology)
            }
        }
    }

    fn place_ec_shards(
        &self,
        object_id: &ObjectId,
        k: u8,
        m: u8,
        placement: FailureDomain,
        topology: &ClusterTopology,
    ) -> Result<Vec<ShardPlacement>, PlacementError> {
        let total_shards = k + m;
        let mut placements = Vec::with_capacity(total_shards as usize);

        // Hash object ID to get deterministic but distributed placement
        let hash = xxhash64(object_id.as_bytes());

        match placement {
            FailureDomain::Disk => {
                // All shards on different disks, same node
                let node = self.select_node_by_hash(hash, topology)?;
                for i in 0..total_shards {
                    let disk = self.select_disk_by_hash(hash + i as u64, &node)?;
                    placements.push(ShardPlacement { node: node.id, disk: disk.id });
                }
            }
            FailureDomain::Rack => {
                // Shards on different racks
                let racks = self.select_racks_by_hash(hash, total_shards as usize, topology)?;
                for (i, rack) in racks.into_iter().enumerate() {
                    let node = self.select_node_in_rack_by_hash(hash + i as u64, &rack)?;
                    let disk = self.select_disk_by_hash(hash + i as u64, &node)?;
                    placements.push(ShardPlacement { node: node.id, disk: disk.id });
                }
            }
            // ... other failure domains
        }

        Ok(placements)
    }
}
```

---

## 8. S3 API Gateway

### Supported Operations

#### Bucket Operations
| Operation | Method | Path | Status |
|-----------|--------|------|--------|
| CreateBucket | PUT | /{bucket} | Phase 4 |
| DeleteBucket | DELETE | /{bucket} | Phase 4 |
| HeadBucket | HEAD | /{bucket} | Phase 4 |
| ListBuckets | GET | / | Phase 4 |
| GetBucketLocation | GET | /{bucket}?location | Phase 4 |
| PutBucketPolicy | PUT | /{bucket}?policy | Phase 4 |
| GetBucketPolicy | GET | /{bucket}?policy | Phase 4 |
| PutBucketVersioning | PUT | /{bucket}?versioning | Phase 4 |
| GetBucketVersioning | GET | /{bucket}?versioning | Phase 4 |

#### Object Operations
| Operation | Method | Path | Status |
|-----------|--------|------|--------|
| PutObject | PUT | /{bucket}/{key} | Phase 4 |
| GetObject | GET | /{bucket}/{key} | Phase 4 |
| HeadObject | HEAD | /{bucket}/{key} | Phase 4 |
| DeleteObject | DELETE | /{bucket}/{key} | Phase 4 |
| CopyObject | PUT | /{bucket}/{key} (with x-amz-copy-source) | Phase 4 |
| ListObjectsV2 | GET | /{bucket}?list-type=2 | Phase 4 |
| ListObjectVersions | GET | /{bucket}?versions | Phase 4 |

#### Multipart Upload
| Operation | Method | Path | Status |
|-----------|--------|------|--------|
| CreateMultipartUpload | POST | /{bucket}/{key}?uploads | Phase 4 |
| UploadPart | PUT | /{bucket}/{key}?partNumber=N&uploadId=ID | Phase 4 |
| CompleteMultipartUpload | POST | /{bucket}/{key}?uploadId=ID | Phase 4 |
| AbortMultipartUpload | DELETE | /{bucket}/{key}?uploadId=ID | Phase 4 |
| ListParts | GET | /{bucket}/{key}?uploadId=ID | Phase 4 |
| ListMultipartUploads | GET | /{bucket}?uploads | Phase 4 |

### AWS Signature V4 Authentication

```rust
/// Signature V4 authentication
pub struct SigV4Authenticator {
    credentials_store: Arc<CredentialsStore>,
}

impl SigV4Authenticator {
    /// Verify request signature
    pub async fn authenticate(&self, request: &S3Request) -> Result<AuthContext, AuthError> {
        // Extract auth info from Authorization header or query params
        let auth_info = self.extract_auth_info(request)?;

        // Look up credentials
        let secret_key = self.credentials_store
            .get_secret_key(&auth_info.access_key_id)
            .await?;

        // Compute expected signature
        let expected_signature = self.compute_signature(
            &secret_key,
            &auth_info.string_to_sign,
            &auth_info.credential_scope,
        )?;

        // Constant-time comparison
        if !constant_time_eq(&expected_signature, &auth_info.signature) {
            return Err(AuthError::SignatureDoesNotMatch);
        }

        Ok(AuthContext {
            user_id: auth_info.access_key_id,
            // ...
        })
    }

    fn compute_signature(
        &self,
        secret_key: &str,
        string_to_sign: &str,
        credential_scope: &CredentialScope,
    ) -> Result<String, AuthError> {
        // AWS4-HMAC-SHA256 signing
        let date_key = hmac_sha256(
            format!("AWS4{}", secret_key).as_bytes(),
            credential_scope.date.as_bytes(),
        );
        let region_key = hmac_sha256(&date_key, credential_scope.region.as_bytes());
        let service_key = hmac_sha256(&region_key, b"s3");
        let signing_key = hmac_sha256(&service_key, b"aws4_request");

        let signature = hmac_sha256(&signing_key, string_to_sign.as_bytes());
        Ok(hex::encode(signature))
    }
}
```

### Request Flow

```
┌─────────────────────────────────────────────────────────────────────┐
│                        S3 PUT Object Flow                           │
└─────────────────────────────────────────────────────────────────────┘

Client              Gateway              Metadata             Storage Nodes
   │                   │                    │                      │
   │─── PUT /bucket/key ──►                 │                      │
   │    [Headers + Body]                    │                      │
   │                   │                    │                      │
   │                   │─── Authenticate ───┤                      │
   │                   │◄── OK + UserCtx ───┤                      │
   │                   │                    │                      │
   │                   │─── GetPlacement ──►│                      │
   │                   │◄── Shard Targets ──│                      │
   │                   │                    │                      │
   │◄── 100 Continue ──│                    │                      │
   │                   │                    │                      │
   │═══ Stream Body ══►│                    │                      │
   │                   │                    │                      │
   │                   │─── EC Encode ──────┤                      │
   │                   │    (k+m shards)    │                      │
   │                   │                    │                      │
   │                   │─────────── WriteShard(1) ───────────────► │
   │                   │─────────── WriteShard(2) ───────────────► │
   │                   │─────────── WriteShard(k+m) ─────────────► │
   │                   │              (parallel)                   │
   │                   │                    │                      │
   │                   │◄────────── Ack(1) ────────────────────────│
   │                   │◄────────── Ack(2) ────────────────────────│
   │                   │    (wait for quorum)                      │
   │                   │                    │                      │
   │                   │─── CreateObject ──►│                      │
   │                   │    (metadata)      │                      │
   │                   │◄── OK ─────────────│                      │
   │                   │                    │                      │
   │◄── 200 OK ────────│                    │                      │
   │    [ETag]         │                    │                      │
   │                   │                    │                      │
```

---

## 9. Failure Handling and Recovery

### Failure Detection

```rust
/// Phi Accrual Failure Detector
pub struct PhiAccrualDetector {
    heartbeats: HashMap<NodeId, VecDeque<Instant>>,
    threshold: f64,       // Default: 8.0
    max_history: usize,   // Default: 100
}

impl PhiAccrualDetector {
    /// Record heartbeat from a node
    pub fn heartbeat(&mut self, node_id: NodeId);

    /// Calculate phi value (suspicion level)
    pub fn phi(&self, node_id: NodeId) -> f64;

    /// Check if node is considered failed
    pub fn is_failed(&self, node_id: NodeId) -> bool {
        self.phi(node_id) > self.threshold
    }
}
```

### Rebuild Priority Queue

| Priority | Condition | Action |
|----------|-----------|--------|
| Critical | available_shards <= k | Immediate rebuild, throttle other I/O |
| High | available_shards == k + 1 | High priority rebuild |
| Normal | available_shards < k + m | Normal background rebuild |
| Low | Proactive (health check found corruption) | Best-effort rebuild |

### Disk Failure Recovery

```
1. Detection:
   - Heartbeat timeout OR I/O error

2. Mark disk as FAILED in cluster map

3. For each shard on failed disk:
   a. Read k available shards from other locations
   b. Reconstruct missing shard via Reed-Solomon
   c. Allocate on:
      - Spare disk on same node (if available), OR
      - Different node in same failure domain
   d. Update shard location in metadata

4. Bandwidth limit: 100 MB/s default (configurable)
```

### Node Failure Recovery

```
1. Detection:
   - All disks on node report heartbeat failure

2. Mark node as DOWN

3. Priority queue by criticality:
   - Critical: Stripes with only k shards remaining
   - High: Stripes below redundancy threshold
   - Normal: All other degraded stripes

4. For each affected stripe:
   a. Read shards from surviving nodes
   b. Reconstruct all missing shards
   c. Write to nodes in DIFFERENT failure domains
      (prefer different rack if original was rack-spread)

5. Rebalance when node returns or is replaced
```

### Site/AZ Failure Recovery

```
1. Detection:
   - All nodes in AZ heartbeat fail

2. If 3-AZ replication:
   - 2 copies remain in surviving AZs
   - Reads: Served from surviving AZs
   - Writes:
     a. Sync mode: Write to 2 surviving AZs, queue for failed AZ
     b. Async mode: Write to primary, async replicate

3. If EC within AZ + cross-AZ replication:
   - One complete EC copy in each AZ
   - Surviving AZs continue serving independently

4. Recovery when AZ returns:
   - Resync all changed objects (delta sync)
   - Validate checksums
   - Resume normal replication
```

---

## 10. Security

### Authentication

- **AWS Signature V4**: Full implementation with presigned URL support
- **Access Keys**: Access Key ID + Secret Access Key pairs
- **Temporary Credentials**: Support for STS-style tokens (future)

### Authorization (Bucket Policies)

```rust
pub struct BucketPolicy {
    pub version: String,  // "2012-10-17"
    pub statements: Vec<PolicyStatement>,
}

pub struct PolicyStatement {
    pub sid: Option<String>,
    pub effect: Effect,           // Allow | Deny
    pub principal: Principal,     // "*" | { "OBIO": ["arn:..."] }
    pub action: Vec<String>,      // ["s3:GetObject", "s3:PutObject", ...]
    pub resource: Vec<String>,    // ["arn:obio:s3:::bucket/*", ...]
    pub condition: Option<Conditions>,
}

// Supported conditions
pub struct Conditions {
    pub ip_address: Option<IpCondition>,
    pub string_equals: Option<HashMap<String, String>>,
    pub string_like: Option<HashMap<String, String>>,
    // ...
}
```

### Encryption

```
┌─────────────────────────────────────────────────────────────────────┐
│                    Server-Side Encryption                           │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  SSE-S3 (Server-Managed Keys):                                      │
│  ─────────────────────────────                                      │
│  • ObjectIO manages keys                                            │
│  • Master key stored encrypted in metadata service                  │
│  • Per-object key derived from master key + object ID               │
│  • AES-256-GCM encryption                                           │
│                                                                     │
│  SSE-C (Customer-Provided Keys):                                    │
│  ────────────────────────────                                       │
│  • Customer provides key in request header                          │
│  • Key not stored (customer must provide on every request)          │
│  • AES-256-GCM encryption                                           │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

### Transport Security

- TLS 1.3 for all external connections (S3 API)
- mTLS optional for internal gRPC (between components)
- Certificate rotation support

---

## 11. Project Structure

```
objectio/
├── Cargo.toml                       # Workspace root
├── docs/
│   ├── DESIGN.md                    # This document
│   ├── DEPLOYMENT.md                # Deployment guide
│   └── API.md                       # API reference
│
├── crates/
│   ├── objectio-common/             # Shared types and utilities
│   │   └── src/
│   │       ├── lib.rs
│   │       ├── types.rs             # ObjectId, BucketName, etc.
│   │       ├── error.rs             # Common error types
│   │       ├── config.rs            # Configuration
│   │       └── checksum.rs          # CRC32C, xxHash, SHA256
│   │
│   ├── objectio-proto/              # Protocol definitions
│   │   ├── proto/
│   │   │   ├── storage.proto        # OSD RPC
│   │   │   ├── metadata.proto       # Metadata service RPC
│   │   │   └── cluster.proto        # Cluster management
│   │   └── build.rs
│   │
│   ├── objectio-erasure/            # Erasure coding
│   │   └── src/
│   │       ├── lib.rs
│   │       ├── reed_solomon.rs      # RS encoder/decoder
│   │       └── shard.rs             # Shard types
│   │
│   ├── objectio-placement/          # Placement algorithm
│   │   └── src/
│   │       ├── lib.rs
│   │       ├── crush.rs             # CRUSH algorithm
│   │       ├── topology.rs          # Cluster topology
│   │       └── policy.rs            # Placement policies
│   │
│   ├── objectio-storage/            # Storage engine
│   │   └── src/
│   │       ├── lib.rs
│   │       ├── disk.rs              # Disk manager
│   │       ├── raw_io.rs            # O_DIRECT / F_NOCACHE
│   │       ├── layout.rs            # Disk layout, superblock
│   │       ├── block.rs             # Block format, allocator
│   │       ├── cache.rs             # Block cache (LRU)
│   │       ├── wal.rs               # Block WAL
│   │       ├── repair.rs            # Failure detection, repair
│   │       └── metadata/            # OSD metadata storage
│   │           ├── mod.rs           # Module exports
│   │           ├── types.rs         # MetadataKey, ShardMeta
│   │           ├── wal.rs           # Metadata WAL (LSN-based)
│   │           ├── btree.rs         # In-memory B-tree + snapshots
│   │           ├── cache.rs         # ARC cache
│   │           └── store.rs         # Unified MetadataStore
│   │
│   ├── objectio-meta-store/         # Metadata service
│   │   └── src/
│   │       ├── lib.rs
│   │       ├── raft.rs              # Raft state machine
│   │       ├── store.rs             # redb-backed storage
│   │       ├── bucket.rs            # Bucket operations
│   │       └── object.rs            # Object metadata
│   │
│   ├── objectio-s3/                 # S3 API layer
│   │   └── src/
│   │       ├── lib.rs
│   │       ├── auth/
│   │       │   ├── mod.rs
│   │       │   ├── sigv4.rs         # Signature V4
│   │       │   └── presigned.rs     # Presigned URLs
│   │       ├── handlers/
│   │       │   ├── mod.rs
│   │       │   ├── bucket.rs        # Bucket operations
│   │       │   ├── object.rs        # Object operations
│   │       │   └── multipart.rs     # Multipart upload
│   │       ├── error.rs             # S3 error responses
│   │       └── xml.rs               # XML serialization
│   │
│   └── objectio-client/             # Internal RPC client
│       └── src/
│           ├── lib.rs
│           ├── meta.rs              # Metadata client
│           └── osd.rs               # OSD client
│
├── bin/
│   ├── objectio-gateway/            # S3 gateway binary
│   │   └── src/main.rs
│   ├── objectio-meta/               # Metadata service binary
│   │   └── src/main.rs
│   ├── objectio-osd/                # Storage node binary
│   │   └── src/main.rs
│   └── objectio-cli/                # Admin CLI
│       └── src/main.rs
│
└── tests/
    ├── integration/
    │   ├── s3_compat_test.rs        # S3 API tests
    │   ├── erasure_test.rs          # EC tests
    │   └── failover_test.rs         # Failure scenarios
    └── e2e/
        └── cluster_test.rs          # Full cluster tests
```

---

## 12. Implementation Phases

### Phase 1: Foundation ✅

**Goal**: Project structure and core types

- [x] Initialize Cargo workspace
- [x] Define core types in `objectio-common`
- [x] Set up protobuf definitions (`objectio-proto`)
- [x] Configure logging with `tracing`
- [x] Basic configuration loading

**Deliverable**: Compiling workspace with shared types

### Phase 2: Storage Engine ✅

**Goal**: Single-node storage with erasure coding

- [x] Raw disk access (O_DIRECT / F_NOCACHE)
- [x] Superblock and disk layout
- [x] Block allocator with extent-based free list
- [x] Write-ahead log (data WAL)
- [x] Block cache (LRU with write-through/write-back policies)
- [x] Reed-Solomon encoding/decoding (`objectio-erasure`)
- [x] OSD metadata storage (hybrid WAL + B-tree + ARC cache)
- [x] Single-node OSD binary

**Deliverable**: Working single-node storage engine

### Phase 3: Distributed Metadata ✅

**Goal**: Raft-based metadata service

- [x] Integrate `openraft`
- [x] Implement state machine with `redb`
- [x] Bucket and object metadata operations
- [x] CRUSH-like placement algorithm (`objectio-placement`)
- [x] Multi-node metadata cluster

**Deliverable**: 3-node metadata cluster

### Phase 4: S3 API ✅

**Goal**: S3-compatible HTTP API

- [x] AWS Signature V4 authentication (`objectio-auth`)
- [x] Core object operations (PUT, GET, DELETE, HEAD)
- [x] Bucket operations (CREATE, DELETE, LIST)
- [x] ListObjectsV2 with pagination
- [x] Multipart upload
- [x] Object versioning
- [x] Bucket policies

**Deliverable**: S3-compatible API passing basic compatibility tests

### Phase 5: Reliability & Operations 🔄

**Goal**: Production readiness

- [x] Health check endpoints
- [ ] Failure detection (phi accrual)
- [ ] Background repair manager
- [ ] Data integrity scrubbing
- [ ] Server-side encryption
- [ ] Prometheus metrics
- [ ] Admin CLI improvements
- [ ] Multi-DC replication

**Deliverable**: Production-ready system

### Phase 6: Multi-Node, Auth & Installer ✅

**Goal**: Production deployment support

- [x] OSD connection pool for multi-node gateway
- [x] Erasure coding integration in data path
- [x] User management and credential storage
- [x] Bucket ACLs and policy evaluation
- [x] Installation tool (`objectio-install`)
- [x] Systemd unit generation
- [x] Disk detection and preparation

**Deliverable**: Automated deployment with authentication

### Phase 7: Advanced Erasure Coding ✅

**Goal**: LRC support and pluggable EC backends

- [x] Backend abstraction trait (`ErasureBackend`)
- [x] ISA-L integration (x86 SIMD optimization)
- [x] LRC (Locally Repairable Codes) support
- [x] Striping for large objects (multi-stripe EC)
- [x] Scatter-gather listing across OSDs
- [ ] LRC-aware repair optimization
- [ ] Storage class configuration

**Deliverable**: Reduced repair bandwidth with LRC

### Phase 8: Centralized IAM ✅

**Goal**: Persistent credential management

- [x] IAM RPCs in metadata service (CreateUser, GetUser, CreateAccessKey, etc.)
- [x] Centralized credential storage in metadata service
- [x] Gateway credential lookup via gRPC (with 5-minute cache)
- [x] Admin user auto-creation on first startup
- [x] Credentials persist across gateway restarts

**Deliverable**: Credentials managed centrally, survive restarts

---

## Appendix A: Technology Stack

| Component | Crate | Version | Purpose |
|-----------|-------|---------|---------|
| Async Runtime | `tokio` | 1.x | Async I/O, timers |
| HTTP Framework | `axum` | 0.8+ | S3 REST API |
| gRPC | `tonic` | 0.12+ | Inter-service RPC |
| Protobuf | `prost` | 0.13+ | Protobuf codegen |
| Erasure Coding | `reed-solomon-simd` | latest | SIMD-optimized RS |
| Raft | `openraft` | 0.10+ | Distributed consensus |
| KV Store | `redb` | 2.x | Metadata storage |
| Checksums | `crc32c`, `xxhash-rust` | latest | Data integrity |
| Crypto | `ring` or `aws-lc-rs` | latest | SHA-256, HMAC, AES |
| XML | `quick-xml` | latest | S3 XML parsing |
| Tracing | `tracing` | 0.1+ | Structured logging |
| Metrics | `prometheus` | latest | Observability |

---

## Appendix B: References

- [Amazon S3 API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/Welcome.html)
- [AWS Signature Version 4](https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-authenticating-requests.html)
- [Ceph CRUSH Algorithm](https://docs.ceph.com/en/reef/rados/operations/crush-map/)
- [MinIO Architecture](https://min.io/docs/minio/linux/operations/concepts/erasure-coding.html)
- [Reed-Solomon Error Correction](https://en.wikipedia.org/wiki/Reed%E2%80%93Solomon_error_correction)
- [Raft Consensus Algorithm](https://raft.github.io/)
