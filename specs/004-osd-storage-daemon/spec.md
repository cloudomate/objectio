# 004 - OSD Storage Daemon

## Overview

The ObjectIO OSD (Object Storage Daemon) is the storage backend that writes erasure-coded shards to raw disks using O_DIRECT, maintains a WAL for durability, and provides ARC caching.

## Core Components

- **objectio-storage** crate: Raw disk I/O engine
- **objectio-erasure** crate: EC encoding/decoding
- **objectio-block** crate: Block storage layer on top of OSD
- **objectio-proto** crate: gRPC definitions

## Disk I/O

### O_DIRECT Raw Disk

- Uses O_DIRECT/F_NOCACHE for direct I/O bypassing OS page cache
- 4KB alignment requirement for all I/O operations
- Raw device access, no filesystem

### Block Allocation

- 64KB block size as internal allocation unit
- B-tree metadata for block allocation tracking
- WAL (Write-Ahead Log) for crash recovery

### WAL (Write-Ahead Log)

- 1GB WAL size per OSD
- Durable write log before data hits main storage
- Recovery on restart from WAL replay

## Storage Structure

### Layout

```
/dev/sda (raw disk)
├── Superblock (at offset 0)
├── WAL (1GB)
└── Data blocks (64KB each)
```

### Metadata

- B-tree storing block allocation map
- WAL metadata for recovery
- ARC cache for hot metadata

## SMART Monitoring

- SMART data collection for disk health
- Attributes: temperature, reallocated sectors, pending errors
- Health status exposed via metrics

## Erasure Coding Integration

- Receives erasure-coded shards from Gateway
- 4+2 default (4 data + 2 parity)
- EC backend selection: `rust-simd` (default) or `isal` (x86_64 only)

## ARC Cache

- Adaptive Replacement Cache for metadata
- Caches block allocation metadata
- Reduces disk metadata reads

## Metrics

- `objectio_osd_disk_read_bytes` — Disk read bytes
- `objectio_osd_disk_write_bytes` — Disk write bytes
- `objectio_osd_operations_total` — Operation counter
- `objectio_osd_arc_hits` — ARC cache hits
- Exposed at port 9201