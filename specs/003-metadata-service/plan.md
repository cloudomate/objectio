# 003 - Metadata Service: Plan

## Approach

Document the existing Meta service implementation focusing on OpenRaft consensus, Redb persistence, CRUSH placement, and IAM management.

## Technical Approach

### Data Structures

1. **IAM**: Document user and access key storage
2. **Buckets**: Document bucket metadata schema
3. **Objects**: Document object metadata with chunk mapping
4. **Placement**: Document CRUSH/CRUSH2 algorithm usage

### Raft Implementation

- OpenRaft for consensus
- Redb for durable storage
- Log replication across 3+ nodes
- Leader election

### Source Files to Reference

- `crates/objectio-meta-store/src/lib.rs` — Module structure
- `crates/objectio-meta-store/src/store.rs` — Bucket/object store
- `crates/objectio-meta-store/src/raft.rs` — Raft setup
- `crates/objectio-meta-store/src/raft_storage.rs` — Raft storage backend
- `crates/objectio-meta-store/src/types.rs` — Type definitions
- `crates/objectio-placement/src/lib.rs` — CRUSH/CRUSH2 algorithms
- `crates/objectio-placement/src/crush.rs` — CRUSH implementation
- `crates/objectio-placement/src/crush2.rs` — CRUSH2 (HRW) implementation
- `crates/objectio-proto/proto/metadata.proto` — gRPC definitions

### Placement Algorithm

- Document CRUSH algorithm for shard distribution
- Document CRUSH2 (HRW hashing) for better load balance
- Document failure domain awareness

## Deliverables

- spec.md: Meta service architecture, data model, Raft, placement
- plan.md: This document
- tasks.md: Actionable documentation tasks