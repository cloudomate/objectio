# 003 - Metadata Service: Tasks

## Documentation Tasks

### Data Model

- [ ] Document IAM user schema (ID, name, created, metadata)
- [ ] Document access key schema (access key ID, secret key hash, user association)
- [ ] Document bucket schema (name, owner, created, policy, placement)
- [ ] Document object schema (ID, bucket, key, version, size, chunk_map)
- [ ] Document chunk_map structure for EC shard mapping

### Raft Consensus

- [ ] Document OpenRaft usage for distributed consensus
- [ ] Document Redb embedded database for persistence
- [ ] Document 3+ node cluster configuration
- [ ] Document leader election and failover
- [ ] Document log replication mechanism

### Placement

- [ ] Document CRUSH algorithm for OSD selection
- [ ] Document CRUSH2 (HRW hashing) advantage over CRUSH
- [ ] Document failure domain awareness (rack, host, disk)
- [ ] Document crush2::templates pre-built rulesets
- [ ] Document placement policy per bucket

### API

- [ ] Document metadata.proto gRPC service definitions
- [ ] Document bucket CRUD gRPC calls
- [ ] Document object metadata CRUD gRPC calls
- [ ] Document user/key management gRPC calls
- [ ] Document placement query API

### Metrics

- [ ] Document objectio_meta_raft_leader gauge
- [ ] Document objectio_meta_raft_log_entries gauge
- [ ] Document objectio_meta_operations_total counter
- [ ] Document Meta metrics port 9101