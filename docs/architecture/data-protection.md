# Data Protection

ObjectIO provides data protection through erasure coding and replication.

## Core Principle: EC Within DC, Replication Across DCs

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
└─────────────────────────────────────────────────────────────────────┘
```

## Erasure Coding (Reed-Solomon)

### How It Works

Reed-Solomon encoding splits data into `k` data shards and generates `m` parity shards. Any `k` shards can reconstruct the original data.

```
Original Data:  ┌─────────────────────────────────────┐
                │            4 MB chunk               │
                └─────────────────────────────────────┘
                              │
                        RS Encode (4+2)
                              │
                              ▼
Data Shards:    ┌────┐ ┌────┐ ┌────┐ ┌────┐
                │ D0 │ │ D1 │ │ D2 │ │ D3 │  (1 MB each)
                └────┘ └────┘ └────┘ └────┘

Parity Shards:  ┌────┐ ┌────┐
                │ P0 │ │ P1 │  (computed from D0-D3)
                └────┘ └────┘
```

### Erasure Coding Profiles

| Profile | Data (k) | Parity (m) | Total | Storage Efficiency | Fault Tolerance |
|---------|----------|------------|-------|-------------------|-----------------|
| 4+2 | 4 | 2 | 6 | 67% | 2 failures |
| 6+3 | 6 | 3 | 9 | 67% | 3 failures |
| 8+4 | 8 | 4 | 12 | 67% | 4 failures |
| 10+5 | 10 | 5 | 15 | 67% | 5 failures |
| 4+1 | 4 | 1 | 5 | 80% | 1 failure |

### Placement Strategies

| Strategy | EC/Replication | Data Distribution | Failure Tolerance |
|----------|----------------|-------------------|-------------------|
| `disk` | EC | Shards on different disks, same node | Disk failures only |
| `node` | EC | Shards on different nodes, same rack | Node failures |
| `rack` | EC | Shards on different racks, same DC | Rack failures |
| `datacenter` | Replication | Full copy in each DC | DC failures |

---

## Locally Repairable Codes (LRC)

For large clusters, LRC reduces repair bandwidth by enabling local recovery.

### MDS vs LRC Comparison

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
│   Repair bandwidth: LOW for single failures (local group only)              │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### LRC Profiles

| Profile | Data (k) | Local (l) | Global (g) | Groups | Group Size | Total | Efficiency |
|---------|----------|-----------|------------|--------|------------|-------|------------|
| LRC 6+2+2 | 6 | 2 | 2 | 2 | 3 | 10 | 60% |
| LRC 12+2+2 | 12 | 2 | 2 | 2 | 6 | 16 | 75% |
| LRC 12+3+3 | 12 | 3 | 3 | 3 | 4 | 18 | 67% |

---

## Erasure Coding Backends

ObjectIO supports multiple EC backends with automatic selection.

```
┌────────────────────────────────────────────────────────────────────┐
│                     ErasureCodec (High-Level API)                  │
│  • encode(data) → shards                                           │
│  • decode(shards) → data                                           │
│  • try_local_recovery() (LRC only)                                 │
└────────────────────────────┬───────────────────────────────────────┘
                             │
              ┌──────────────┼──────────────┐
              │              │              │
       ┌──────▼──────┐ ┌─────▼──────┐ ┌─────▼──────┐
       │  rust_simd  │ │   ISA-L    │ │   Future   │
       │  (default)  │ │   (x86)    │ │  backends  │
       │             │ │            │ │            │
       │ • Portable  │ │ • AVX/AVX2 │ │ • CUDA     │
       │ • ARM NEON  │ │ • AVX-512  │ │ • etc.     │
       │ • Pure Rust │ │ • FFI      │ │            │
       └─────────────┘ └────────────┘ └────────────┘
```

### Backend Selection

| Architecture | Default Backend | Performance |
|--------------|-----------------|-------------|
| x86/x86_64 | ISA-L (if enabled) | 2-5x faster |
| ARM/AArch64 | rust_simd | NEON optimized |
| Other | rust_simd | Portable |

Enable ISA-L with the `isal` feature:

```bash
cargo build --features isal
```

---

## Storage Classes

Configure data protection per bucket or object.

```yaml
storage_classes:
  # Single-node deployment: EC across local disks
  local:
    type: erasure_coding
    ec_profile: "4+2"
    placement: disk
    fault_tolerance: 2

  # Single-DC deployment: EC across racks
  standard:
    type: erasure_coding
    ec_profile: "8+4"
    placement: rack
    fault_tolerance: 4

  # Multi-DC deployment: Replication across DCs
  geo:
    type: replication
    replicas: 3
    placement: datacenter
    sync_mode: async
    fault_tolerance: 2

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

---

## Failure Domain Hierarchy

ObjectIO respects failure domain boundaries when placing data.

```rust
pub enum FailureDomain {
    Disk,       // Single disk failure
    Node,       // Server/host failure (all disks on host)
    Rack,       // Rack failure (power, ToR switch)
    Datacenter, // DC/AZ failure (network partition, disaster)
    Region,     // Geographic region failure
}
```

### Placement Example (4+2 EC, Rack Placement)

```
Rack A          Rack B          Rack C
┌─────────┐     ┌─────────┐     ┌─────────┐
│ Node 1  │     │ Node 3  │     │ Node 5  │
│  D0, D3 │     │  D1, P0 │     │  D2, P1 │
└─────────┘     └─────────┘     └─────────┘

Survives: Complete failure of any 2 racks
```

Data shards (D0-D3) and parity shards (P0-P1) are distributed across racks so that a single rack failure doesn't lose more shards than can be tolerated.

---

## Replication Mode

For simpler deployments (single-node, dev/test), ObjectIO supports pure replication without erasure coding.

### How Replication Mode Works

```
┌───────────────────────────────────────────────────────────────────────┐
│                         Original Object                                │
└───────────────────────────────────────────────────────────────────────┘
                              │
                    (No EC encoding)
                              │
        ┌─────────────────────┼─────────────────────┐
        ▼                     ▼                     ▼
  ┌──────────────┐     ┌──────────────┐     ┌──────────────┐
  │   Replica 0  │     │   Replica 1  │     │   Replica 2  │
  │  (Full copy) │     │  (Full copy) │     │  (Full copy) │
  └──────────────┘     └──────────────┘     └──────────────┘
```

### Striping in Replication Mode

Large objects are still striped in replication mode (stripe size = ~4 MB):

```
┌───────────────────────────────────────────────────────────────────────┐
│                       12 MB Object (Replication 3)                     │
└───────────────────────────────────────────────────────────────────────┘
                              │
                    Split into stripes (~4 MB each)
                              │
        ┌─────────────────────┼─────────────────────┐
        ▼                     ▼                     ▼
  ┌──────────────┐     ┌──────────────┐     ┌──────────────┐
  │  Stripe 0    │     │  Stripe 1    │     │  Stripe 2    │
  │  (4 MB)      │     │  (4 MB)      │     │  (4 MB)      │
  └──────────────┘     └──────────────┘     └──────────────┘
        │                     │                     │
        │ Write to           │ Write to           │ Write to
        │ 3 replicas         │ 3 replicas         │ 3 replicas
        ▼                     ▼                     ▼
  ┌──────────────┐     ┌──────────────┐     ┌──────────────┐
  │ OSD0, OSD1,  │     │ OSD0, OSD1,  │     │ OSD0, OSD1,  │
  │ OSD2         │     │ OSD2         │     │ OSD2         │
  └──────────────┘     └──────────────┘     └──────────────┘
```

### Stripe Metadata for Replication

Replication uses special EC parameters (`ec_k=1, ec_m=0`):

```rust
StripeMeta {
    stripe_id: 0,
    ec_k: 1,           // Full data, not split
    ec_m: 0,           // No parity
    ec_type: ErasureReplication,
    data_size: 4194304,
    shards: [
        { position: 0, node_id: "osd0", ... },  // Replica 0
        { position: 1, node_id: "osd1", ... },  // Replica 1
        { position: 2, node_id: "osd2", ... },  // Replica 2
    ],
}
```

### Replication vs EC Comparison

| Aspect | Replication | Erasure Coding |
|--------|-------------|----------------|
| **Storage overhead** | N× (e.g., 3×) | 1.5× (e.g., 4+2) |
| **Read latency** | Any replica | Must read k shards |
| **Write latency** | Wait for 1 | Wait for k (quorum) |
| **Repair bandwidth** | Copy full object | Copy 1 shard |
| **Min nodes** | 1 | k+m (e.g., 6) |
| **Use case** | Dev/test, small clusters | Production |

### When to Use Replication

- **Single-node deployments** (replication=1)
- **Development and testing**
- **Small clusters** (< 6 nodes)
- **Low-latency requirements** (any replica can serve reads)
