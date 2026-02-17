# ObjectIO Components

Detailed description of each ObjectIO component.

## S3 Gateway (`objectio-gateway`)

The S3 Gateway provides the S3-compatible REST API endpoint.

### Responsibilities

- **HTTP Server**: Axum-based HTTP/HTTPS server
- **Authentication**: AWS Signature V4 verification
- **Request Routing**: Parse S3 requests, route to appropriate handlers
- **Erasure Coding**: Encode data into shards for writes, decode for reads
- **Streaming**: Handle large file uploads/downloads efficiently
- **Multipart Upload**: Coordinate multipart upload sessions
- **Iceberg REST Catalog**: Apache Iceberg namespace/table management at `/iceberg/v1/*`
- **Iceberg Access Control**: IAM-style policies at namespace and table level

### Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                        S3 Gateway                                 │
├──────────────────────────────────────────────────────────────────┤
│                                                                  │
│   ┌─────────────┐  ┌─────────────┐  ┌─────────────┐              │
│   │   Axum      │  │   Auth      │  │   Handlers  │              │
│   │   Router    │──│   Middleware│──│  S3 + Iceberg│             │
│   └─────────────┘  └─────────────┘  └─────────────┘              │
│                                           │                      │
│                              ┌────────────┴────────────┐         │
│                              │                         │         │
│                    ┌─────────▼─────────┐  ┌───────────▼───────┐  │
│                    │   Metadata Client │  │   OSD Pool        │  │
│                    │   (gRPC)          │  │   (gRPC conns)    │  │
│                    └───────────────────┘  └───────────────────┘  │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

### Configuration

```toml
[server]
listen = "0.0.0.0:9000"
tls_cert = "/etc/objectio/tls/cert.pem"
tls_key = "/etc/objectio/tls/key.pem"

[auth]
enabled = true
region = "us-east-1"

[metadata]
endpoints = ["http://meta1:9100", "http://meta2:9100", "http://meta3:9100"]

[erasure_coding]
data_shards = 4
parity_shards = 2
```

### Scaling

Gateways are stateless and can be horizontally scaled. Use a load balancer (HAProxy, nginx, or cloud LB) in front of multiple gateway instances.

---

## Metadata Service (`objectio-meta`)

The Metadata Service manages all cluster metadata with redb persistence.

### Responsibilities

- **Bucket Metadata**: Create, delete, list buckets
- **Object Metadata**: Object locations, versions, user metadata
- **Volume Metadata**: Block storage volumes, snapshots, chunks
- **OSD Registration**: Track available storage nodes
- **Placement Decisions**: CRUSH 2.0 (HRW hashing) for shard placement
- **IAM**: Users, access keys, bucket policies
- **Bucket Policies**: JSON-based access control
- **Iceberg Catalog**: Namespace/table metadata and policies

### Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                     Metadata Service                              │
├──────────────────────────────────────────────────────────────────┤
│                                                                  │
│   ┌─────────────┐                                                │
│   │   gRPC      │                                                │
│   │   Server    │                                                │
│   └──────┬──────┘                                                │
│          │                                                       │
│          ▼                                                       │
│   ┌─────────────────────────────────────────────────────────────┐│
│   │              MetaService (in-memory cache + redb)           ││
│   │  ┌───────────────┐  ┌───────────────┐  ┌─────────────────┐  ││
│   │  │   buckets:    │  │   users:      │  │   osd_nodes:    │  ││
│   │  │   HashMap     │  │   HashMap     │  │   Vec<OsdNode>  │  ││
│   │  └───────────────┘  └───────────────┘  └─────────────────┘  ││
│   │  ┌───────────────┐  ┌───────────────┐  ┌─────────────────┐  ││
│   │  │  volumes:     │  │  access_keys: │  │  policies:      │  ││
│   │  │  HashMap      │  │  HashMap      │  │  HashMap        │  ││
│   │  └───────────────┘  └───────────────┘  └─────────────────┘  ││
│   │  ┌───────────────┐  ┌───────────────┐                       ││
│   │  │  iceberg_ns:  │  │  iceberg_tbl: │                       ││
│   │  │  HashMap      │  │  HashMap      │                       ││
│   │  └───────────────┘  └───────────────┘                       ││
│   └─────────────────────────────────────────────────────────────┘│
│          │                                                       │
│          ▼                                                       │
│   ┌─────────────────────────────────────────────────────────────┐│
│   │              redb (persistent KV store)                     ││
│   │  Tables: buckets, bucket_policies, multipart_uploads,       ││
│   │          osd_nodes, cluster_topology, users, access_keys,   ││
│   │          volumes, snapshots, volume_chunks,                 ││
│   │          iceberg_namespaces, iceberg_tables                 ││
│   └─────────────────────────────────────────────────────────────┘│
│                                                                  │
│   ✅ All writes persisted to redb                                │
│   ✅ Data loaded from redb on startup                            │
│   ✅ CRUSH 2.0 placement integrated                             │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

The service maintains in-memory HashMaps for fast reads and persists all writes to redb. On startup, all data is loaded from redb into memory.

### Planned: Raft HA

Raft consensus (via openraft) is scaffolded but not yet active. Currently the meta service runs as a single instance. When Raft is completed, deploy 3, 5, or 7 nodes:

```
3 nodes: Survives 1 failure
5 nodes: Survives 2 failures
7 nodes: Survives 3 failures
```

### Data Stored

| Category | Contents | Purpose |
|----------|----------|---------|
| **Buckets** | name, owner, storage_class, versioning_state, created_at | Bucket definitions |
| **Objects** | bucket/key → object_id, size, etag, content_type, timestamps, stripes | Object index |
| **Stripes** | ec_k, ec_m, ec_type (MDS/LRC), shard locations | Where data lives |
| **Shard Locations** | position, node_id, disk_id, offset, shard_type, local_group | Exact shard placement |
| **OSD Registry** | node_id, address, disk_ids | Available storage nodes |
| **Bucket Policies** | bucket → policy_json | Access control |
| **IAM Users** | user_id, display_name, email, ARN, status | User management |
| **Access Keys** | access_key_id, secret, user_id, status | S3 authentication |
| **Volumes** | volume_id, name, size, pool, state | Block storage volumes |
| **Snapshots** | snapshot_id, volume_id, name, chunk_manifest | COW snapshots |
| **Multipart Uploads** | upload_id, bucket, key, parts | In-progress uploads |

### Known Limitations

1. **Single point of failure**: Raft not yet active (data is persistent via redb, but no replication)
2. **No LRC in placement**: Only MDS (Reed-Solomon) placement exposed via API

---

## Storage Node (`objectio-osd`)

The OSD (Object Storage Daemon) manages raw disk storage.

> **Implementation Status**: ✅ Fully implemented with custom B-tree index, WAL, and ARC cache.

### Responsibilities

- **Raw Disk I/O**: O_DIRECT (Linux) / F_NOCACHE (macOS)
- **Block Storage**: 4MB blocks with checksums
- **Write-Ahead Log**: Durability for in-flight writes
- **Shard Storage**: Store erasure-coded shards
- **Local Metadata**: B-tree index with snapshot persistence
- **Background Tasks**: Compaction, scrubbing (repair pending)

### Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                        OSD Service                                │
├──────────────────────────────────────────────────────────────────┤
│                                                                  │
│   ┌─────────────┐        ┌─────────────┐                         │
│   │   gRPC      │        │   Shard     │                         │
│   │   Server    │◄──────►│   Handler   │                         │
│   └─────────────┘        └──────┬──────┘                         │
│                                 │                                │
│   ┌─────────────────────────────┴──────────────────────────────┐ │
│   │                     Storage Engine                          │ │
│   │  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐    │ │
│   │  │  Block   │  │  Block   │  │ Metadata │  │   WAL    │    │ │
│   │  │  Cache   │  │Allocator │  │  Store   │  │          │    │ │
│   │  └──────────┘  └──────────┘  └──────────┘  └──────────┘    │ │
│   └─────────────────────────────┬──────────────────────────────┘ │
│                                 │                                │
│   ┌─────────────────────────────┴──────────────────────────────┐ │
│   │                      Disk Manager                           │ │
│   │   ┌────────────┐   ┌────────────┐   ┌────────────┐         │ │
│   │   │  /dev/vdb  │   │  /dev/vdc  │   │  /dev/vdd  │   ...   │ │
│   │   │  (raw)     │   │  (raw)     │   │  (raw)     │         │ │
│   │   └────────────┘   └────────────┘   └────────────┘         │ │
│   └────────────────────────────────────────────────────────────┘ │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

### OSD Local Metadata Store

The OSD has a fully implemented metadata storage engine:

```
┌─────────────────────────────────────────────────────────────────┐
│                     MetadataStore                                │
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

**Write Path:**
1. Append operation to WAL (with fsync)
2. Update B-tree index (in-memory)
3. Update ARC cache (if entry cached)

**Read Path:**
1. Check ARC cache → return on hit
2. Query B-tree index
3. Populate cache on miss

**Recovery:**
1. Load latest snapshot into B-tree
2. Replay WAL entries after snapshot LSN
3. Ready to serve

### Disk Layout

Each raw disk has this layout:

```
┌──────────┬──────────────┬─────────────┬────────────────────────┐
│Superblock│   WAL Region │Block Bitmap │     Data Region        │
│  (4 KB)  │   (1-4 GB)   │  (variable) │   (remaining space)    │
└──────────┴──────────────┴─────────────┴────────────────────────┘
```

### Configuration

```toml
[osd]
listen = "0.0.0.0:9200"
advertise_addr = "osd1:9200"
meta_endpoint = "http://meta1:9100"

[storage]
disks = ["/dev/vdb", "/dev/vdc"]
block_size = 4194304  # 4 MB

[cache]
block_cache_size_mb = 256
write_policy = "write_through"

[metadata]
snapshot_threshold = 10000   # Entries between snapshots
snapshot_retention = 2       # Keep N old snapshots
cache_size = 10000           # ARC cache entries
```

---

## Admin CLI (`objectio-cli`)

Command-line tool for cluster management.

### Commands

```bash
# Cluster status
objectio-cli cluster status
objectio-cli cluster topology

# Bucket management
objectio-cli bucket list
objectio-cli bucket create my-bucket
objectio-cli bucket delete my-bucket

# User management
objectio-cli user list
objectio-cli user create --name alice
objectio-cli user delete alice

# OSD management
objectio-cli osd list
objectio-cli osd status osd-uuid
```

---

## Installer (`objectio-install`)

Automated installation and configuration tool.

### Features

- Disk detection and preparation
- Systemd unit generation
- Configuration file creation
- Initial cluster bootstrap

### Usage

```bash
# Interactive installation
sudo objectio-install

# Non-interactive with config
sudo objectio-install --config install.toml
```

See [Deployment Guide](../deployment/README.md) for details.
