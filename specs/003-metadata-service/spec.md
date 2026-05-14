# 003 - Metadata Service

## Overview

The ObjectIO Meta service is a Raft-based distributed metadata store that manages bucket/object metadata, IAM users/access keys, and placement decisions via CRUSH algorithms.

## Core Components

- **objectio-meta-store** crate: OpenRaft + Redb storage engine
- **objectio-placement** crate: CRUSH and CRUSH2 placement algorithms
- **objectio-proto** crate: gRPC definitions for Meta service

## Data Model

### IAM Users and Access Keys

- Users stored with unique identifier and metadata
- Access keys associated with users (access key ID + secret access key)
- Keys used for SigV4 authentication in S3 API

### Buckets

- Bucket name (globally unique)
- Owner (IAM user)
- Creation timestamp
- Policy (JSON, evaluated by PolicyEvaluator)
- Placement policy (CRUSH ruleset selection)

### Objects

- Object ID (UUID)
- Bucket reference
- Object name (key within bucket)
- Version ID (if versioning enabled)
- Size in bytes
- Chunk map: how object maps to erasure-coded shards
- Created/modified timestamps

### Placement

- CRUSH algorithm maps objects to OSDs
- CRUSH2 (HRW hashing) recommended for better distribution
- Pre-built templates in `crush2::templates`
- Failure domain awareness (rack, host, disk)

## Raft Consensus

- OpenRaft implementation for fault-tolerant metadata
- Redb (embedded Rust database) for persistence
- 3+ node cluster for HA
- Consensus for bucket creation, object metadata updates, IAM changes

## API

Meta service exposes gRPC API (defined in `proto/metadata.proto`) for:

- Bucket CRUD
- Object metadata CRUD
- User/key management
- Placement queries
- Cluster membership

## Metrics

- `objectio_meta_raft_leader` — Current leader gauge
- `objectio_meta_raft_log_entries` — Log entry count
- `objectio_meta_operations_total` — Operation counter