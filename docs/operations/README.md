# Operations Guide

Day-to-day operations, monitoring, and maintenance of ObjectIO clusters.

## Contents

- [Failure Recovery](failure-recovery.md) - Handling disk, node, and network failures
- [Monitoring](monitoring.md) - Metrics, alerting, and observability

---

## Quick Reference

### Service Management

```bash
# Start all services
sudo systemctl start objectio-gateway objectio-meta objectio-osd

# Stop all services
sudo systemctl stop objectio-gateway objectio-meta objectio-osd

# Restart a specific service
sudo systemctl restart objectio-gateway

# Check service status
sudo systemctl status objectio-gateway
```

### Health Checks

```bash
# Gateway health
curl http://localhost:9000/health

# Check cluster status via CLI
objectio-cli cluster status

# Check OSD status
objectio-cli osd list
```

### Common Operations

| Task | Command |
|------|---------|
| List buckets | `objectio-cli bucket list` |
| Check disk usage | `objectio-cli osd disk-usage` |
| View cluster map | `objectio-cli cluster map` |
| Trigger repair | `objectio-cli repair start --osd osd1` |
| Drain OSD | `objectio-cli osd drain osd1` |

---

## Cluster States

### Healthy

All components operational:

```
┌─────────────────────────────────────────────────────────┐
│ Cluster Status: HEALTHY                                 │
├─────────────────────────────────────────────────────────┤
│ Gateways:  2/2 healthy                                  │
│ Metadata:  3/3 healthy (leader: meta1)                  │
│ OSDs:      7/7 healthy                                  │
│ Objects:   1,234,567                                    │
│ Capacity:  45% used (4.5 TB / 10 TB)                   │
└─────────────────────────────────────────────────────────┘
```

### Degraded

Some redundancy lost but data accessible:

```
┌─────────────────────────────────────────────────────────┐
│ Cluster Status: DEGRADED                                │
├─────────────────────────────────────────────────────────┤
│ Gateways:  2/2 healthy                                  │
│ Metadata:  3/3 healthy (leader: meta2)                  │
│ OSDs:      6/7 healthy (osd3: DOWN)                     │
│ Objects:   1,234,567 (5,432 degraded)                   │
│ Repair:    In progress (ETA: 2h 15m)                    │
└─────────────────────────────────────────────────────────┘
```

### Critical

Data at risk of loss:

```
┌─────────────────────────────────────────────────────────┐
│ Cluster Status: CRITICAL                                │
├─────────────────────────────────────────────────────────┤
│ Gateways:  1/2 healthy                                  │
│ Metadata:  2/3 healthy (leader: meta1)                  │
│ OSDs:      4/7 healthy (osd2, osd3, osd5: DOWN)         │
│ Objects:   1,234,567 (50,000 at risk)                   │
│ Action:    RESTORE OSDs IMMEDIATELY                     │
└─────────────────────────────────────────────────────────┘
```

---

## Maintenance Windows

### Rolling Upgrades

1. **Upgrade OSDs first** (one at a time):
   ```bash
   # Drain OSD
   objectio-cli osd drain osd1

   # Wait for data migration
   objectio-cli osd wait-drained osd1

   # Stop, upgrade, start
   sudo systemctl stop objectio-osd@osd1
   # ... upgrade binary ...
   sudo systemctl start objectio-osd@osd1

   # Undrain
   objectio-cli osd undrain osd1
   ```

2. **Upgrade metadata nodes** (one at a time):
   ```bash
   # Check which node is leader
   objectio-cli meta status

   # Upgrade followers first, leader last
   sudo systemctl restart objectio-meta@meta2
   sudo systemctl restart objectio-meta@meta3
   sudo systemctl restart objectio-meta@meta1  # leader
   ```

3. **Upgrade gateways** (one at a time):
   ```bash
   sudo systemctl restart objectio-gateway
   ```

### Adding Capacity

```bash
# Add new OSD
objectio-cli osd add --address osd8:9200 --disks /dev/vdi

# Rebalance data (automatic)
objectio-cli cluster rebalance status
```

### Removing an OSD

```bash
# Drain all data from OSD
objectio-cli osd drain osd7

# Wait for completion
objectio-cli osd wait-drained osd7

# Remove from cluster
objectio-cli osd remove osd7
```

---

## Backup and Recovery

### Metadata Backup

Raft snapshots are automatic. For manual backup:

```bash
# Export metadata
objectio-cli meta export --output /backup/meta-$(date +%Y%m%d).json

# Import metadata (disaster recovery)
objectio-cli meta import --input /backup/meta-20240115.json
```

### Object Recovery

If erasure coding cannot recover an object:

```bash
# Check object health
objectio-cli object health bucket/key

# Force repair from available shards
objectio-cli object repair bucket/key --force
```

---

## Troubleshooting Quick Reference

| Symptom | Check | Solution |
|---------|-------|----------|
| Slow uploads | Network, OSD load | Scale gateways, add OSDs |
| High latency | Metadata service | Check Raft leader, disk I/O |
| Objects missing | OSD health | Check repair status |
| Auth failures | User credentials | Verify access keys |
| Connection refused | Service status | Restart services |

---

## Emergency Procedures

### Complete Cluster Recovery

If all metadata nodes fail:

1. Stop all services
2. Identify node with latest Raft log
3. Bootstrap from that node
4. Bring up remaining metadata nodes
5. Start OSDs and gateways

### Data Recovery from Raw Disks

If cluster metadata is lost but disks are intact:

```bash
# Scan disk for objects
objectio-cli disk scan /dev/vdb --output objects.json

# Rebuild metadata from scan
objectio-cli meta rebuild --from-scan objects.json
```

---

See [Failure Recovery](failure-recovery.md) for detailed recovery procedures.
