# 005 - Block Storage: Plan

## Approach

Document the existing block storage implementation in objectio-block crate, focusing on LBA→chunk mapping, thin provisioning, snapshots/clones, QoS, and protocol support.

## Technical Approach

### Block Storage Architecture

1. **LBA Mapping**: Document 512B sector → 4MB chunk mapping
2. **Thin Provisioning**: Document on-demand chunk allocation
3. **Snapshots/Clones**: Document COW implementation
4. **QoS**: Document token bucket rate limiting
5. **Write Cache/Journal**: Document in-memory coalescing and crash recovery
6. **Protocols**: Document gRPC BlockService, NBD, iSCSI, NVMe-oF

### Source Files to Reference

- `crates/objectio-block/src/lib.rs` — Module structure
- `crates/objectio-block/src/` — (check actual source files)
- `crates/objectio-proto/proto/block.proto` — BlockService gRPC
- `bin/objectio-block-gateway/src/main.rs` — Block gateway binary
- `crates/objectio-client/src/lib.rs` — OSD gRPC client

### Key Error Types

- **IMPORTANT**: objectio-block uses its own `BlockError`/`BlockResult` types, NOT objectio_common::Error
- Do not mix the two error types

## Deliverables

- spec.md: Block storage architecture, LBA mapping, QoS, protocols
- plan.md: This document
- tasks.md: Actionable documentation tasks