# 004 - OSD Storage Daemon: Plan

## Approach

Document the existing OSD implementation focusing on O_DIRECT raw disk I/O, WAL durability, SMART monitoring, and ARC cache.

## Technical Approach

### Disk I/O Stack

1. **O_DIRECT**: Document raw disk access bypassing OS page cache
2. **Block Layout**: Document superblock, WAL, data block regions
3. **B-tree Metadata**: Document block allocation tracking
4. **WAL**: Document write-ahead logging for crash recovery

### Source Files to Reference

- `crates/objectio-storage/src/lib.rs` — Module structure
- `crates/objectio-storage/src/disk.rs` — Raw disk I/O
- `crates/objectio-storage/src/raw_io.rs` — O_DIRECT operations
- `crates/objectio-storage/src/wal.rs` — WAL implementation
- `crates/objectio-storage/src/block.rs` — Block allocation
- `crates/objectio-storage/src/layout.rs` — Disk layout
- `crates/objectio-storage/src/smart.rs` — SMART monitoring
- `crates/objectio-storage/src/metadata/mod.rs` — Metadata B-tree
- `crates/objectio-storage/src/metadata/cache.rs` — ARC cache
- `crates/objectio-storage/src/metadata/store.rs` — Metadata store
- `crates/objectio-erasure/src/lib.rs` — EC backends

### Constants

- 64KB block size
- 4KB O_DIRECT alignment
- 1GB WAL size
- 1GB minimum disk

## Deliverables

- spec.md: OSD architecture, disk I/O, WAL, SMART, ARC
- plan.md: This document
- tasks.md: Actionable documentation tasks