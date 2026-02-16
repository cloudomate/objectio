# Storage Engine

ObjectIO uses raw disk access for maximum performance and control.

## Contents

- [Disk Layout](disk-layout.md) - Raw disk format and structures
- [Caching](caching.md) - Block and metadata caching
- [Erasure Coding](erasure-coding.md) - Reed-Solomon implementation details

## Overview

The storage engine bypasses the filesystem entirely, using O_DIRECT (Linux) or F_NOCACHE (macOS) for direct disk I/O.

### Why Raw Disk Access?

| Aspect | Filesystem | Raw Disk |
|--------|------------|----------|
| **Overhead** | Filesystem metadata, journaling | None |
| **Caching** | Double-buffered (OS + app) | Single application cache |
| **Latency** | Variable | Predictable |
| **Control** | Limited | Full |
| **Recovery** | fsck required | Custom WAL recovery |

### Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        Storage Engine                                │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│   ┌───────────────────────────────────────────────────────────────┐ │
│   │                    Block Cache (LRU)                          │ │
│   │  • Recently accessed data blocks                              │ │
│   │  • Write-through or write-back policies                       │ │
│   └───────────────────────────────────────────────────────────────┘ │
│                              │                                      │
│   ┌───────────────────────────────────────────────────────────────┐ │
│   │                 Metadata Store                                │ │
│   │  • B-tree index (in-memory)                                   │ │
│   │  • WAL for durability                                         │ │
│   │  • ARC cache for hot entries                                  │ │
│   └───────────────────────────────────────────────────────────────┘ │
│                              │                                      │
│   ┌───────────────────────────────────────────────────────────────┐ │
│   │              Raw Disk I/O (O_DIRECT / F_NOCACHE)              │ │
│   │  • 4KB aligned I/O operations                                 │ │
│   │  • Bypasses OS page cache entirely                            │ │
│   └───────────────────────────────────────────────────────────────┘ │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

## Disk Layout

Each raw disk is divided into regions:

```
┌──────────┬──────────────┬─────────────┬────────────────────────┐
│Superblock│   WAL Region │Block Bitmap │     Data Region        │
│  (4 KB)  │   (1-4 GB)   │  (variable) │   (remaining space)    │
└──────────┴──────────────┴─────────────┴────────────────────────┘
```

See [Disk Layout](disk-layout.md) for detailed format specifications.

## OSD Metadata Store

Each OSD maintains a local metadata store (separate from the cluster metadata service) for tracking shards stored on its disks.

> **Implementation Status**: Fully implemented with custom B-tree index, WAL, and ARC cache.

### Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     OSD MetadataStore                            │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │                ARC Cache (hot entries)                     │  │
│  │  • Adaptive replacement for scan-resistant caching         │  │
│  │  • Configurable size (default: 10,000 entries)            │  │
│  └───────────────────────────────────────────────────────────┘  │
│                            │                                     │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │              B-tree Index (in-memory)                      │  │
│  │  • Rust BTreeMap for sorted key access                     │  │
│  │  • Range scans and prefix queries                          │  │
│  │  • Periodic snapshots to disk (configurable threshold)     │  │
│  └───────────────────────────────────────────────────────────┘  │
│                            │                                     │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │                WAL (append-only)                           │  │
│  │  • All mutations logged before applying                    │  │
│  │  • Configurable sync (fsync on write or batched)          │  │
│  │  • Replay on recovery after snapshot LSN                   │  │
│  │  • Truncated after snapshot                                │  │
│  └───────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

### Data Stored

| Key Type | Contents |
|----------|----------|
| Shard ID | Block location, size, checksum, stripe info |
| Disk ID | Superblock state, free space, health |
| Object → Shards | Mapping of objects to local shards |

### Write Path (Metadata)

1. Append operation to WAL (with fsync)
2. Update B-tree index (in-memory)
3. Update ARC cache (if entry cached)

### Read Path (Metadata)

1. Check ARC cache → return on hit
2. Query B-tree index
3. Populate cache on miss

### Recovery

1. Load latest snapshot into B-tree
2. Replay WAL entries after snapshot LSN
3. Ready to serve

---

## Write Path (Data)

1. Receive shard data from gateway
2. Append `BeginTxn` to WAL
3. Allocate blocks from bitmap
4. Append `WriteBlock` to WAL for each block
5. Write data to allocated blocks
6. Append `Commit` to WAL
7. Sync WAL to disk
8. Return success

## Read Path

1. Receive read request from gateway
2. Check block cache
   - **Hit**: Return cached data
   - **Miss**: Continue to step 3
3. Look up block location in metadata
4. Read block from disk (O_DIRECT)
5. Insert into cache
6. Return data

## Configuration

```toml
[storage]
# Disk paths (raw devices or files)
disks = ["/dev/vdb", "/dev/vdc"]

# Block size (default: 4 MB)
block_size = 4194304

[storage.cache]
# Block cache settings
block_cache:
  enabled = true
  size_mb = 256
  policy = "write_through"  # write_through | write_back | write_around

# Metadata cache settings
metadata_cache:
  enabled = true
  size_mb = 64

[storage.wal]
# WAL sync policy
sync_on_write = true
max_size_mb = 64
```

## Supported Platforms

| Platform | Direct I/O | Notes |
|----------|-----------|-------|
| Linux | `O_DIRECT` | Full support |
| macOS | `F_NOCACHE` | Full support |
| Windows | `FILE_FLAG_NO_BUFFERING` | Planned |

## Performance Tuning

### Block Size

- **4 MB (default)**: Good balance for most workloads
- **1 MB**: Better for small objects
- **16 MB**: Better for large sequential writes

### Cache Size

- Rule of thumb: 10-20% of working set
- Monitor cache hit ratio with metrics
- Adjust based on workload patterns

### WAL Size

- Larger WAL = less frequent snapshots
- Trade-off: recovery time vs write performance
