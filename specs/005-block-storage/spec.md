# 005 - Block Storage

## Overview

The ObjectIO Block Storage layer maps logical block addresses (LBAs, 512-byte sectors) to erasure-coded 4MB chunks stored on OSDs. It provides thin provisioning, snapshots/clones, QoS, and multiple attachment protocols.

## Core Components

- **objectio-block** crate: Block storage implementation
- **objectio-client** crate: gRPC client to OSDs
- **objectio-proto** crate: BlockService gRPC definitions
- **objectio-erasure** crate: EC encoding for chunk writes

## LBA to Chunk Mapping

### Addressing

- LBA size: 512 bytes (sector)
- Chunk size: 4MB (4 × 1024 × 1024)
- LBAs per chunk: 8192 (4MB / 512B)

### ChunkMapper

- Maps (volume_id, LBA) → (chunk_id, chunk_shards)
- Thin provisioning: chunks allocated only on first write
- Volume metadata tracks allocated chunks

## Thin Provisioning

- Storage allocated on demand (write)
- Zero chunks not allocated on disk
- Capacity reported as allocated vs total

## Snapshots and Clones

### Snapshots

- Read-only point-in-time view
- Copy-on-write: original chunks copied when modified
- Chain of snapshots possible

### Clones

- Writable copy of snapshot
- Shares chunks with parent until written
- COW on write for clone

## QoS Token Bucket

- Per-volume IOPS limit
- Per-volume bandwidth limit (MB/s)
- Token bucket algorithm
- Priority scheduling for volumes

## Write Cache and Journal

### Write Cache

- In-memory cache for writes
- Coalesces small writes before EC encoding
- Flushes full 4MB chunks to OSDs
- Reduces EC encoding overhead

### Write Journal

- Crash-consistent writes
- Journal记录 before data written
- Recovery replays journal entries
- Ensures no partial chunk writes

## Attachment Protocols

### gRPC (BlockService)

- Port 9300
- Defined in proto/block.proto
- BlockService API for volume attach/detach/read/write

### NBD (Network Block Device)

- Port 10809
- Linux NBD client support
- Standard block device interface

### iSCSI (planned)

- Standard iSCSI target
- CHAP authentication

### NVMe-oF (planned)

- NVMe over Fabrics
- RDMA or TCP transport

## Architecture

```
Block Gateway (:9300 gRPC, :10809 NBD)
    └── objectio-block (VolumeManager, ChunkMapper, WriteCache, WriteJournal, QoS)
        └── objectio-client (gRPC to OSDs)
            └── OSDs (raw disk, EC shards)
```

## Metrics

- `objectio_block_read_bytes` — Block read bytes
- `objectio_block_write_bytes` — Block write bytes
- `objectio_block_operations_total` — Operation counter
- `objectio_block_cache_hits` — Write cache hits
- Per-volume IOPS/bandwidth metrics