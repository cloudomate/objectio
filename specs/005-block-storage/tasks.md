# 005 - Block Storage: Tasks

## Documentation Tasks

### LBA to Chunk Mapping

- [ ] Document 512-byte LBA (sector) size
- [ ] Document 4MB chunk size
- [ ] Document 8192 LBAs per chunk calculation
- [ ] Document ChunkMapper mapping (volume_id, LBA) → chunk
- [ ] Document chunk allocation tracking

### Thin Provisioning

- [ ] Document on-demand allocation (write triggers allocation)
- [ ] Document zero chunks not allocated
- [ ] Document capacity reporting (allocated vs total)

### Snapshots and Clones

- [ ] Document COW (copy-on-write) snapshot implementation
- [ ] Document read-only snapshot view
- [ ] Document writable clone from snapshot
- [ ] Document chunk sharing between clone and parent
- [ ] Document COW on write for clones

### QoS Token Bucket

- [ ] Document per-volume IOPS limit
- [ ] Document per-volume bandwidth limit (MB/s)
- [ ] Document token bucket algorithm
- [ ] Document priority scheduling for volumes

### Write Cache

- [ ] Document in-memory write coalescing
- [ ] Document cache flush to OSDs as full 4MB chunks
- [ ] Document cache size/configuration

### Write Journal

- [ ] Document crash-consistent write guarantee
- [ ] Document journal record before data write
- [ ] Document recovery from journal replay
- [ ] Document no partial chunk write guarantee

### Attachment Protocols

- [ ] Document gRPC BlockService on port 9300
- [ ] Document proto/block.proto BlockService API
- [ ] Document NBD on port 10809
- [ ] Document iSCSI target (if implemented)
- [ ] Document NVMe-oF (if implemented)

### Metrics

- [ ] Document objectio_block_read_bytes counter
- [ ] Document objectio_block_write_bytes counter
- [ ] Document objectio_block_operations_total counter
- [ ] Document objectio_block_cache_hits counter
- [ ] Document per-volume IOPS/bandwidth metrics

### Error Handling

- [ ] Document BlockError/BlockResult types (NOT objectio_common::Error)
- [ ] Document error type separation from object layer