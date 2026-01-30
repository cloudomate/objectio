# Block Storage Design

## Overview

ObjectIO Block provides distributed block storage by mapping logical block addresses (LBAs) to erasure-coded chunks stored on OSDs. This enables:

- **Virtual disks** for VMs and containers
- **iSCSI/NVMe-oF** access from any client
- **Thin provisioning** - allocate on write
- **Snapshots** - instant, space-efficient
- **Clones** - writable snapshots

## Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                         Block Client                              │
│  (Linux iSCSI initiator, NVMe-oF client, or NBD client)          │
└──────────────────────────────┬───────────────────────────────────┘
                               │
              ┌────────────────┼────────────────┐
              │                │                │
              ▼                ▼                ▼
┌─────────────────┐  ┌─────────────┐  ┌─────────────────┐
│  iSCSI Target   │  │ NVMe-oF    │  │  NBD Server     │
│  Port 3260      │  │ Port 4420  │  │  /dev/nbd0      │
└────────┬────────┘  └──────┬─────┘  └────────┬────────┘
         │                  │                  │
         └──────────────────┼──────────────────┘
                            │
              ┌─────────────▼─────────────┐
              │      Block Gateway        │
              │   (objectio-block)        │
              │                           │
              │  ┌─────────────────────┐  │
              │  │   Volume Manager    │  │
              │  │   - Volume CRUD     │  │
              │  │   - Snapshot mgmt   │  │
              │  │   - Access control  │  │
              │  └─────────────────────┘  │
              │                           │
              │  ┌─────────────────────┐  │
              │  │   Chunk Mapper      │  │
              │  │   LBA → chunk_id    │  │
              │  │   4MB chunks        │  │
              │  └─────────────────────┘  │
              │                           │
              │  ┌─────────────────────┐  │
              │  │   Write Cache       │  │
              │  │   - Write-back      │  │
              │  │   - Journal (WAL)   │  │
              │  │   - Flush policy    │  │
              │  └─────────────────────┘  │
              └─────────────┬─────────────┘
                            │ gRPC
         ┌──────────────────┼──────────────────┐
         │                  │                  │
         ▼                  ▼                  ▼
   ┌──────────┐       ┌──────────┐       ┌──────────┐
   │   Meta   │       │   OSD    │       │   OSD    │
   │          │       │          │       │          │
   └──────────┘       └──────────┘       └──────────┘
```

## Data Model

### Volume

A volume is a virtual block device with a fixed size.

```protobuf
message Volume {
    string volume_id = 1;           // UUID
    string name = 2;                // Human-readable name
    uint64 size_bytes = 3;          // Provisioned size
    uint64 used_bytes = 4;          // Actual used (thin provisioning)
    string pool = 5;                // Storage pool (EC config)
    VolumeState state = 6;          // available, attached, error
    repeated string snapshots = 7;  // Snapshot IDs
    string parent_snapshot = 8;     // For clones
    map<string, string> metadata = 9;
}

enum VolumeState {
    VOLUME_AVAILABLE = 0;
    VOLUME_ATTACHED = 1;
    VOLUME_ERROR = 2;
    VOLUME_DELETING = 3;
}
```

### Chunk Mapping

Volumes are divided into fixed-size chunks (default 4MB to match EC stripe).

```
Volume (100GB)
├── Chunk 0:  LBA 0 - 8191       → object: vol_{id}/chunk_00000000
├── Chunk 1:  LBA 8192 - 16383   → object: vol_{id}/chunk_00000001
├── Chunk 2:  LBA 16384 - 24575  → object: vol_{id}/chunk_00000002
│   ...
└── Chunk N:  ...                → object: vol_{id}/chunk_{N:08x}

LBA = Logical Block Address (512 bytes each)
Chunk = 4MB = 8192 LBAs
```

**Thin Provisioning**: Chunks are only created when first written. Reading an unallocated chunk returns zeros.

### Snapshot (Copy-on-Write)

```protobuf
message Snapshot {
    string snapshot_id = 1;
    string volume_id = 2;
    string name = 3;
    uint64 created_at = 4;
    uint64 size_bytes = 5;          // Logical size (same as volume)
    uint64 unique_bytes = 6;        // Data unique to this snapshot
    SnapshotState state = 7;
}
```

**COW Implementation**:
1. On snapshot create: Record current chunk list in snapshot metadata
2. On write to snapshotted volume: Copy chunk to snapshot before modifying
3. Snapshot chunks are immutable

```
Before Snapshot:
  Volume: [A][B][C][D]

After Snapshot (snap1):
  Volume:   [A][B][C][D]  ← current
  snap1:    [A][B][C][D]  ← same chunks (no copy yet)

After Write to chunk B:
  Volume:   [A][B'][C][D] ← B' is new chunk
  snap1:    [A][B][C][D]  ← B preserved in snapshot
```

## Block Gateway Components

### 1. Volume Manager

```rust
pub struct VolumeManager {
    meta_client: MetadataServiceClient,
    volumes: RwLock<HashMap<String, Volume>>,
    attachments: RwLock<HashMap<String, Attachment>>,
}

impl VolumeManager {
    /// Create a new volume
    pub async fn create_volume(&self, req: CreateVolumeRequest) -> Result<Volume>;

    /// Delete a volume (must be detached, no snapshots)
    pub async fn delete_volume(&self, volume_id: &str) -> Result<()>;

    /// Resize volume (grow only)
    pub async fn resize_volume(&self, volume_id: &str, new_size: u64) -> Result<()>;

    /// Create snapshot
    pub async fn create_snapshot(&self, volume_id: &str, name: &str) -> Result<Snapshot>;

    /// Clone from snapshot (creates new volume)
    pub async fn clone_volume(&self, snapshot_id: &str, name: &str) -> Result<Volume>;

    /// Attach volume to a target (iSCSI/NVMe-oF)
    pub async fn attach(&self, volume_id: &str, target: &str) -> Result<Attachment>;

    /// Detach volume
    pub async fn detach(&self, volume_id: &str) -> Result<()>;
}
```

### 2. Chunk Mapper

Maps LBAs to chunk objects:

```rust
pub struct ChunkMapper {
    chunk_size: u64,  // 4MB default
}

impl ChunkMapper {
    /// Convert LBA range to chunk IDs
    pub fn lba_to_chunks(&self, start_lba: u64, length: u64) -> Vec<ChunkRange> {
        // LBA is 512 bytes
        let start_byte = start_lba * 512;
        let end_byte = start_byte + length * 512;

        let start_chunk = start_byte / self.chunk_size;
        let end_chunk = (end_byte - 1) / self.chunk_size;

        (start_chunk..=end_chunk)
            .map(|chunk_id| ChunkRange {
                chunk_id,
                offset_in_chunk: ...,
                length: ...,
            })
            .collect()
    }

    /// Get object key for a chunk
    pub fn chunk_key(&self, volume_id: &str, chunk_id: u64) -> String {
        format!("vol_{}/chunk_{:08x}", volume_id, chunk_id)
    }
}
```

### 3. Write Cache

Block workloads require low latency. A write-back cache with journal is essential:

```rust
pub struct WriteCache {
    /// In-memory dirty pages
    dirty_pages: RwLock<BTreeMap<(VolumeId, ChunkId), DirtyChunk>>,

    /// Persistent journal for crash recovery
    journal: Journal,

    /// Cache size limit
    max_cache_bytes: u64,

    /// Flush policy
    flush_interval: Duration,
    max_dirty_age: Duration,
}

impl WriteCache {
    /// Write data (returns immediately after journal write)
    pub async fn write(&self, vol: &str, lba: u64, data: &[u8]) -> Result<()> {
        // 1. Write to journal (sync)
        self.journal.append(vol, lba, data).await?;

        // 2. Update in-memory cache
        self.update_dirty_page(vol, lba, data);

        // 3. Maybe trigger background flush
        if self.should_flush() {
            self.trigger_flush();
        }

        Ok(())
    }

    /// Read data (from cache or backend)
    pub async fn read(&self, vol: &str, lba: u64, len: u64) -> Result<Vec<u8>> {
        // Check cache first
        if let Some(data) = self.read_from_cache(vol, lba, len) {
            return Ok(data);
        }

        // Read from backend
        self.read_from_backend(vol, lba, len).await
    }

    /// Background flush to OSDs
    async fn flush_dirty_pages(&self) {
        // Coalesce writes to same chunk
        // EC encode and write to OSDs
        // Clear journal entries
    }
}
```

## Protocol Implementations

### iSCSI Target

Use `tgt` or `LIO` as the iSCSI target framework, with a custom backstore:

```rust
// Custom backstore that calls our block gateway
pub struct ObjectIOBackstore {
    volume_manager: Arc<VolumeManager>,
    chunk_mapper: Arc<ChunkMapper>,
    write_cache: Arc<WriteCache>,
}

impl Backstore for ObjectIOBackstore {
    async fn read(&self, lun: u32, lba: u64, length: u32) -> Result<Vec<u8>> {
        let volume = self.get_volume_for_lun(lun)?;
        self.write_cache.read(&volume.id, lba, length as u64).await
    }

    async fn write(&self, lun: u32, lba: u64, data: &[u8]) -> Result<()> {
        let volume = self.get_volume_for_lun(lun)?;
        self.write_cache.write(&volume.id, lba, data).await
    }

    async fn flush(&self, lun: u32) -> Result<()> {
        self.write_cache.flush().await
    }
}
```

### NVMe-oF Target

For higher performance, implement NVMe-oF using SPDK or kernel NVMe target:

```rust
// NVMe-oF namespace backed by ObjectIO
pub struct NvmeNamespace {
    nsid: u32,
    volume: Volume,
    // ... NVMe-specific fields
}
```

## Metadata Schema

### Volume Table (in Meta service)

```sql
-- Volumes
CREATE TABLE volumes (
    volume_id TEXT PRIMARY KEY,
    name TEXT UNIQUE,
    size_bytes INTEGER,
    pool TEXT,
    state INTEGER,
    parent_snapshot TEXT,
    created_at INTEGER,
    updated_at INTEGER
);

-- Chunks (sparse - only allocated chunks)
CREATE TABLE chunks (
    volume_id TEXT,
    chunk_id INTEGER,
    object_key TEXT,        -- Key in object storage
    etag TEXT,              -- For consistency check
    PRIMARY KEY (volume_id, chunk_id)
);

-- Snapshots
CREATE TABLE snapshots (
    snapshot_id TEXT PRIMARY KEY,
    volume_id TEXT,
    name TEXT,
    created_at INTEGER,
    chunk_manifest TEXT     -- JSON list of chunk references
);

-- Attachments
CREATE TABLE attachments (
    volume_id TEXT PRIMARY KEY,
    target_type TEXT,       -- iscsi, nvmeof, nbd
    target_address TEXT,    -- e.g., iqn.2024-01.com.objectio:vol1
    attached_at INTEGER
);
```

## API (gRPC)

```protobuf
service BlockService {
    // Volume operations
    rpc CreateVolume(CreateVolumeRequest) returns (Volume);
    rpc DeleteVolume(DeleteVolumeRequest) returns (Empty);
    rpc GetVolume(GetVolumeRequest) returns (Volume);
    rpc ListVolumes(ListVolumesRequest) returns (ListVolumesResponse);
    rpc ResizeVolume(ResizeVolumeRequest) returns (Volume);

    // Snapshot operations
    rpc CreateSnapshot(CreateSnapshotRequest) returns (Snapshot);
    rpc DeleteSnapshot(DeleteSnapshotRequest) returns (Empty);
    rpc ListSnapshots(ListSnapshotsRequest) returns (ListSnapshotsResponse);
    rpc CloneVolume(CloneVolumeRequest) returns (Volume);

    // Attachment operations
    rpc AttachVolume(AttachVolumeRequest) returns (Attachment);
    rpc DetachVolume(DetachVolumeRequest) returns (Empty);

    // I/O operations (for NBD/custom clients)
    rpc Read(ReadRequest) returns (ReadResponse);
    rpc Write(WriteRequest) returns (WriteResponse);
    rpc Flush(FlushRequest) returns (Empty);
}
```

## CLI Commands

```bash
# Volume management
objectio-cli volume create myvolume --size 100G --pool default
objectio-cli volume list
objectio-cli volume delete myvolume
objectio-cli volume resize myvolume --size 200G

# Snapshots
objectio-cli snapshot create myvolume --name snap1
objectio-cli snapshot list myvolume
objectio-cli snapshot delete snap1

# Cloning
objectio-cli volume clone snap1 --name myclone

# Attachments
objectio-cli volume attach myvolume --type iscsi --initiator iqn.2024-01.com.client:host1
objectio-cli volume detach myvolume

# iSCSI target info
objectio-cli iscsi list-targets
objectio-cli iscsi show-target iqn.2024-01.com.objectio:myvolume
```

## Quality of Service (QoS)

Block storage supports per-volume QoS guarantees for IOPS and bandwidth.

### QoS Configuration

```protobuf
message VolumeQos {
    uint64 max_iops = 1;            // Maximum IOPS (0 = unlimited)
    uint64 min_iops = 2;            // Guaranteed minimum IOPS (reserved)
    uint64 max_bandwidth_bps = 3;   // Maximum bandwidth (0 = unlimited)
    uint64 burst_iops = 4;          // Burst IOPS above max
    uint32 burst_seconds = 5;       // Burst duration
    QosPriority priority = 6;       // Scheduling priority
    uint64 target_latency_us = 7;   // Target latency SLO
}
```

### QoS Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                      Block Gateway                               │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │                    QoS Module                            │    │
│  │                                                          │    │
│  │  Per-Volume Rate Limiters:                               │    │
│  │  ┌─────────────────────────────────────────────────┐    │    │
│  │  │ vol-1: IOPS bucket [■■■■□□□□□□] 4000/10000      │    │    │
│  │  │        BW bucket   [■■■■■■□□□□] 300/500 MB/s    │    │    │
│  │  ├─────────────────────────────────────────────────┤    │    │
│  │  │ vol-2: IOPS bucket [■■□□□□□□□□] 2000/10000      │    │    │
│  │  │        BW bucket   [■□□□□□□□□□] 50/500 MB/s     │    │    │
│  │  └─────────────────────────────────────────────────┘    │    │
│  │                                                          │    │
│  │  I/O arrives → Check token bucket → Allow/Throttle       │    │
│  └─────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────┘
```

**Key Points:**
- QoS is enforced at the **block gateway**, not at OSDs
- OSDs report hardware IOPS capacity for admission control
- Guaranteed IOPS (min_iops) requires capacity reservation at volume creation

### Token Bucket Algorithm

```rust
pub struct TokenBucket {
    tokens: AtomicU64,      // Available tokens
    max_tokens: u64,        // Burst capacity
    refill_rate: u64,       // Tokens per second (sustained rate)
}

impl TokenBucket {
    fn try_acquire(&self, count: u64) -> bool {
        self.refill();  // Add tokens based on elapsed time
        // Atomic compare-and-swap to consume tokens
        // Returns false if insufficient tokens (throttled)
    }
}
```

### IOPS Guarantee Flow

```
1. Volume Creation with QoS:
   CreateVolume(name="db-vol", size=100GB, qos={min_iops=10000})
                    │
                    ▼
2. Metadata Service checks OSD capacity:
   - OSD1: 500K total, 400K reserved → 100K available ✓
   - OSD2: 500K total, 490K reserved → 10K available ✗
                    │
                    ▼
3. If sufficient capacity: Reserve IOPS, create volume
   If insufficient: Reject with "InsufficientIops" error
                    │
                    ▼
4. Runtime enforcement at gateway:
   - Token bucket initialized with min_iops rate
   - I/O proceeds if tokens available
   - I/O throttled (queued or rejected) if no tokens
```

### Priority Scheduling

When multiple volumes compete for resources:

| Priority | Behavior |
|----------|----------|
| Critical | Always processed first, never throttled |
| High | Processed before Normal/Low, minimal throttling |
| Normal | Fair share scheduling |
| Low | Background I/O, yields to higher priorities |

### Monitoring

```bash
# Get volume QoS stats
objectio-cli volume stats vol-1

Volume: vol-1
  IOPS:       4,523 / 10,000 (45.2% utilization)
  Bandwidth:  180 MB/s / 500 MB/s (36.0%)
  Latency:    0.8ms avg (target: 1ms)
  Throttled:  0 I/Os in last minute
```

## Performance Considerations

### Write Path Optimization

1. **Journal on fast storage**: NVMe for journal, can use slower storage for data
2. **Write coalescing**: Batch small writes into full chunks before EC encoding
3. **Parallel OSD writes**: Write EC shards in parallel

### Read Path Optimization

1. **Read cache**: Cache frequently accessed chunks in memory
2. **Prefetch**: Predict sequential reads and prefetch chunks
3. **Degraded reads**: If one shard is slow, reconstruct from other shards

### Latency Targets

| Operation | Target | Notes |
|-----------|--------|-------|
| 4KB random read (cached) | < 100μs | From memory cache |
| 4KB random read (uncached) | < 2ms | Single OSD read |
| 4KB random write | < 500μs | Journal write only |
| 4MB sequential write | < 10ms | Full EC encode + OSD writes |

## Comparison with Ceph RBD

| Feature | ObjectIO Block | Ceph RBD |
|---------|----------------|----------|
| Protocol | iSCSI, NVMe-oF, NBD | RBD (native), iSCSI |
| Chunk size | 4MB (configurable) | 4MB default |
| EC support | Yes (Reed-Solomon) | Yes (but often uses replication) |
| Snapshots | COW | COW |
| Thin provisioning | Yes | Yes |
| Client | Standard (iSCSI/NVMe) | Requires librbd or kernel module |

## Implementation Phases

### Phase 1: Core Volume Manager
- Volume CRUD operations
- Chunk mapper
- Integration with existing OSD layer

### Phase 2: Write Cache
- In-memory cache
- Journal for durability
- Background flush

### Phase 3: iSCSI Target
- LIO target integration
- Custom backstore

### Phase 4: Snapshots & Clones
- COW implementation
- Snapshot metadata

### Phase 5: NVMe-oF Target
- SPDK integration
- High-performance path

## Files to Create

```
objectio/
├── crates/
│   ├── objectio-block/           # Block storage library
│   │   ├── src/
│   │   │   ├── lib.rs
│   │   │   ├── volume.rs         # Volume manager
│   │   │   ├── chunk.rs          # Chunk mapper
│   │   │   ├── cache.rs          # Write cache
│   │   │   ├── journal.rs        # Write journal
│   │   │   ├── qos.rs            # QoS rate limiting
│   │   │   └── snapshot.rs       # Snapshot/clone logic
│   │   └── Cargo.toml
│   │
│   └── objectio-proto/
│       └── proto/
│           └── block.proto       # Block service proto
│
├── bin/
│   └── objectio-block/           # Block gateway binary
│       ├── src/
│       │   ├── main.rs
│       │   ├── iscsi.rs          # iSCSI target integration
│       │   ├── nvmeof.rs         # NVMe-oF target
│       │   └── nbd.rs            # NBD server
│       └── Cargo.toml
```
