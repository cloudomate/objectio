# Caching Architecture

ObjectIO implements application-level caching for predictable performance.

## Why Application-Level Caching?

Since ObjectIO uses O_DIRECT/F_NOCACHE to bypass the OS page cache, we implement our own caching layer with full control over:

- Memory allocation
- Eviction policies
- Write-back behavior
- Cache statistics

## Cache Hierarchy

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
│  │                 Metadata Cache (ARC)                          │  │
│  │  • Superblock (always cached after mount)                     │  │
│  │  • Block index entries (adaptive)                             │  │
│  │  • Shard metadata (hot entries)                               │  │
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

## Block Cache

### Design

```rust
/// Cache key uniquely identifies a block across all disks
#[derive(Hash, Eq, PartialEq, Clone, Copy)]
pub struct CacheKey {
    pub disk_id: DiskId,
    pub block_num: u64,
}

/// LRU block cache with dirty tracking
pub struct BlockCache {
    entries: RwLock<HashMap<CacheKey, CacheEntry>>,
    capacity: CacheCapacity,
    stats: CacheStats,
    policy: WritePolicy,
}

pub struct CacheEntry {
    data: Bytes,
    last_access: AtomicU64,
    dirty: AtomicBool,
    modified_at: Option<Instant>,
}
```

### Write Policies

| Policy | Behavior | Use Case |
|--------|----------|----------|
| **Write-through** | Write to disk AND cache simultaneously | Default; strong durability |
| **Write-back** | Write to cache, async flush to disk | High throughput, higher risk |
| **Write-around** | Write to disk only, skip cache | Large sequential writes |

```rust
pub enum WritePolicy {
    WriteThrough,
    WriteBack {
        dirty_threshold: usize,
        max_dirty_age: Duration,
    },
    WriteAround,
}
```

### Cache Operations

```rust
impl BlockCache {
    /// Read with cache
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

    /// Write with policy
    pub async fn write(&self, key: CacheKey, data: Bytes, disk: &RawDiskAccess) -> Result<()> {
        match self.policy {
            WritePolicy::WriteThrough => {
                disk.write_block(key.block_num, &data).await?;
                self.insert(key, data);
            }
            WritePolicy::WriteBack { .. } => {
                self.insert_dirty(key, data);
            }
            WritePolicy::WriteAround => {
                disk.write_block(key.block_num, &data).await?;
                self.invalidate(&key);
            }
        }
        Ok(())
    }
}
```

## Metadata Cache (ARC)

The metadata cache uses Adaptive Replacement Cache for better performance.

### ARC vs LRU

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
```

**Advantages over LRU:**
- **Scan-resistant**: One-time accesses don't evict hot data
- **Self-tuning**: Adapts to workload without configuration
- **Ghost entries**: Zero-cost tracking of eviction history

## Configuration

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

    # Write-back specific settings
    write_back:
      dirty_threshold: 1000     # Max dirty entries
      flush_interval_ms: 5000   # Background flush interval
      max_dirty_age_ms: 30000   # Force flush after this age
```

## Statistics

### Metrics Exported

```
cache_hits_total{disk_id="..."}
cache_misses_total{disk_id="..."}
cache_hit_ratio{disk_id="..."}
cache_evictions_total{disk_id="..."}
cache_writebacks_total{disk_id="..."}
cache_dirty_bytes{disk_id="..."}
cache_entries{disk_id="..."}
cache_size_bytes{disk_id="..."}
```

### Cache Stats API

```rust
pub struct CacheStats {
    pub hits: AtomicU64,
    pub misses: AtomicU64,
    pub evictions: AtomicU64,
    pub writebacks: AtomicU64,
    pub dirty_bytes: AtomicU64,
}

impl CacheStats {
    pub fn hit_ratio(&self) -> f64 {
        let hits = self.hits.load(Ordering::Relaxed);
        let misses = self.misses.load(Ordering::Relaxed);
        if hits + misses == 0 { 0.0 }
        else { hits as f64 / (hits + misses) as f64 }
    }
}
```

## Tuning Guidelines

### Cache Size

| Workload | Recommendation |
|----------|----------------|
| Read-heavy | 20-30% of working set |
| Write-heavy | 10-15% of working set |
| Mixed | 15-20% of working set |

### Write Policy Selection

| Scenario | Policy | Reason |
|----------|--------|--------|
| Database workloads | Write-through | Durability critical |
| Log ingestion | Write-back | Throughput priority |
| Large file uploads | Write-around | Avoid cache pollution |

### Monitoring

Watch these metrics:

1. **Hit ratio**: Should be > 90% for read-heavy workloads
2. **Evictions**: High rate may indicate undersized cache
3. **Dirty bytes**: For write-back, monitor to avoid data loss risk
