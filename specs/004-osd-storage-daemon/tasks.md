# 004 - OSD Storage Daemon: Tasks

## Documentation Tasks

### Disk I/O

- [ ] Document O_DIRECT/F_NOCACHE raw disk access
- [ ] Document 4KB alignment requirement for all I/O
- [ ] Document no filesystem — raw device access
- [ ] Document read/write paths through objectio-storage

### Block Allocation

- [ ] Document 64KB block size as allocation unit
- [ ] Document B-tree metadata for block tracking
- [ ] Document block allocation algorithm
- [ ] Document minimum 1GB disk requirement

### WAL (Write-Ahead Log)

- [ ] Document 1GB WAL size configuration
- [ ] Document WAL durability guarantee before data write
- [ ] Document WAL recovery on restart (replay)
- [ ] Document WAL vs data block layout on disk

### SMART Monitoring

- [ ] Document SMART data collection implementation
- [ ] Document SMART attributes: temperature, reallocated sectors, pending errors
- [ ] Document SMART metrics exposure

### ARC Cache

- [ ] Document ARC (Adaptive Replacement Cache) for metadata
- [ ] Document what metadata is cached (block allocation)
- [ ] Document cache hit/miss metrics

### Erasure Coding

- [ ] Document EC backend integration (rust-simd vs isal)
- [ ] Document 4+2 default EC scheme
- [ ] Document feature flag selection for EC backend

### Metrics

- [ ] Document objectio_osd_disk_read_bytes counter
- [ ] Document objectio_osd_disk_write_bytes counter
- [ ] Document objectio_osd_operations_total counter
- [ ] Document objectio_osd_arc_hits counter
- [ ] Document OSD metrics port 9201