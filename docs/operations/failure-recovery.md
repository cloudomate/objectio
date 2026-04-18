# Failure Recovery

Handling failures in ObjectIO clusters.

## Failure Domains

ObjectIO is designed to tolerate failures at multiple levels:

```
┌─────────────────────────────────────────────────────────────────┐
│                    Failure Domain Hierarchy                      │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  Datacenter ──┬── Rack ──┬── Host ──┬── Disk                    │
│               │          │          │                            │
│               │          │          └── Single disk failure      │
│               │          │              (most common)            │
│               │          │                                       │
│               │          └── Node failure                        │
│               │              (power, OS, hardware)               │
│               │                                                  │
│               └── Rack failure                                   │
│                   (power, network switch)                        │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

---

## Disk Failures

### Detection

OSDs detect disk failures via:
- I/O errors during read/write
- SMART monitoring (if enabled)
- Scrubbing (background verification)

### Automatic Recovery

With 4+2 erasure coding, the cluster tolerates 2 disk failures:

```
Object: photo.jpg (split into 6 shards)

Before failure:
  Disk1: shard1 ✓    Disk4: shard4 ✓
  Disk2: shard2 ✓    Disk5: shard5 ✓
  Disk3: shard3 ✓    Disk6: shard6 ✓

After Disk3 fails:
  Disk1: shard1 ✓    Disk4: shard4 ✓
  Disk2: shard2 ✓    Disk5: shard5 ✓
  Disk3: shard3 ✗    Disk6: shard6 ✓

Recovery: shard3 reconstructed from any 4 remaining shards
          → placed on spare disk (Disk7)
```

### Manual Intervention

For failed disk replacement:

```bash
# 1. Identify failed disk
objectio-cli osd disk-status osd1

# Output:
# DISK       STATUS    USED     OBJECTS   ERRORS
# /dev/vdb   HEALTHY   45.2GB   12,345    0
# /dev/vdc   FAILED    -        -         I/O error

# 2. Mark disk as failed (triggers recovery)
objectio-cli osd disk-fail osd1 /dev/vdc

# 3. Replace physical disk
# (hardware operation)

# 4. Initialize new disk
objectio-cli osd disk-init osd1 /dev/vdc

# 5. Add disk back to cluster
objectio-cli osd disk-add osd1 /dev/vdc
```

---

## OSD Failures

### Types of OSD Failures

| Type | Detection | Impact |
|------|-----------|--------|
| Process crash | Heartbeat timeout | Temporary unavailability |
| Network partition | Heartbeat timeout | Partial cluster access |
| Hardware failure | No heartbeat | Requires repair |

### Recovery Process

```
1. Heartbeat timeout (default: 30s)
   │
   ▼
2. Metadata service marks OSD as DOWN
   │
   ▼
3. Placement recalculated for affected objects
   │
   ▼
4. Repair manager queues recovery operations
   │
   ▼
5. Missing shards reconstructed on healthy OSDs
```

### Manual Recovery

```bash
# Check OSD status
objectio-cli osd list

# Output:
# OSD    ADDRESS       STATUS   DISKS   OBJECTS   LAST_SEEN
# osd1   osd1:9200     UP       2       50,000    now
# osd2   osd2:9200     UP       2       48,500    now
# osd3   osd3:9200     DOWN     2       -         5m ago
# osd4   osd4:9200     UP       2       49,200    now

# View repair progress
objectio-cli repair status

# Output:
# REPAIR STATUS
# Queue:      5,432 objects
# In flight:  100 objects
# Completed:  2,100 objects
# ETA:        1h 45m

# Manually trigger repair for specific bucket
objectio-cli repair start --bucket critical-data
```

### Restarting a Failed OSD

```bash
# Check logs for failure reason
journalctl -u objectio-osd@osd3 -n 100

# Common issues:
# - Disk I/O errors → check disk health
# - Out of memory → increase limits or add RAM
# - Network issues → check connectivity

# Restart OSD
sudo systemctl restart objectio-osd@osd3

# Verify reconnection
objectio-cli osd status osd3
```

---

## Metadata Service Failures

### Raft Consensus

The metadata service uses Raft for consensus:

```
┌─────────────────────────────────────────────────────────────────┐
│                    Raft Cluster (3 nodes)                        │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│   meta1 (Leader)  ←───────→  meta2 (Follower)                   │
│         ↕                          ↕                             │
│         └────────────→ meta3 (Follower)                         │
│                                                                  │
│   Quorum: 2/3 nodes required for writes                         │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### Single Node Failure

Cluster continues operating with 2/3 nodes:

```bash
# Check Raft status
objectio-cli meta status

# Output:
# RAFT CLUSTER STATUS
# Leader:     meta1 (term: 42)
# Committed:  log index 1,234,567
#
# NODE    ROLE       STATE    LOG_INDEX   BEHIND
# meta1   Leader     Healthy  1,234,567   -
# meta2   Follower   Healthy  1,234,567   0
# meta3   Follower   Down     1,234,500   67 (5m ago)
```

### Leader Failover

If the leader fails, election occurs automatically:

```
1. Followers detect leader timeout (default: 1s)
   │
   ▼
2. Follower with most up-to-date log starts election
   │
   ▼
3. Majority vote elects new leader
   │
   ▼
4. New leader begins accepting writes
   │
   ▼
5. (Typically < 5 seconds total)
```

### Recovering a Failed Metadata Node

```bash
# Check why node failed
journalctl -u objectio-meta@meta3 -n 100

# Restart the node
sudo systemctl restart objectio-meta@meta3

# The node will:
# 1. Rejoin cluster as follower
# 2. Receive missing log entries from leader
# 3. Apply entries to catch up
# 4. Begin participating in consensus
```

### Complete Metadata Cluster Failure

**Scenario:** All 3 metadata nodes fail simultaneously.

**Recovery procedure:**

```bash
# 1. Identify node with latest data
# Check Raft log files for highest index
ls -la /var/lib/objectio/meta/raft/

# 2. Bootstrap from most recent node
objectio-meta --node-id 1 --bootstrap-single

# 3. Once leader is stable, add other nodes
objectio-cli meta add-peer meta2:9101
objectio-cli meta add-peer meta3:9101

# 4. Start other nodes in recovery mode
objectio-meta --node-id 2 --join meta1:9101
objectio-meta --node-id 3 --join meta1:9101
```

---

## Gateway Failures

### Stateless Recovery

Gateways are stateless - simply restart:

```bash
sudo systemctl restart objectio-gateway
```

### Load Balancer Health Checks

Configure health checks in your load balancer:

```nginx
# nginx example
upstream objectio {
    server gateway1:9000 max_fails=3 fail_timeout=30s;
    server gateway2:9000 max_fails=3 fail_timeout=30s;
}

server {
    location /health {
        proxy_pass http://objectio/health;
    }
}
```

---

## Network Partitions

### Split-Brain Prevention

Raft prevents split-brain by requiring majority quorum:

```
Partition scenario:
┌─────────────┐     X     ┌─────────────┐
│   meta1     │     X     │   meta2     │
│   (alone)   │     X     │   meta3     │
└─────────────┘     X     └─────────────┘
                    X
Minority (1/3)      X     Majority (2/3)
Cannot elect        X     Can elect leader
leader              X     and accept writes
```

### Client Retry Logic

Clients should retry on different gateways:

```python
import boto3
from botocore.config import Config

# Configure retries
config = Config(
    retries={
        'max_attempts': 3,
        'mode': 'adaptive'
    }
)

s3 = boto3.client('s3', config=config)
```

---

## Data Corruption

### Detection

- **Online:** CRC32C verification on every read
- **Offline:** Background scrubbing

### Recovery

If corruption is detected:

```bash
# Check object integrity
objectio-cli object verify bucket/key

# Output:
# Shard 1: OK (crc: 0xABCD1234)
# Shard 2: OK (crc: 0xBCDE2345)
# Shard 3: CORRUPT (expected: 0xCDEF3456, got: 0x00000000)
# Shard 4: OK (crc: 0xDEF04567)
# Shard 5: OK (crc: 0xEF015678)
# Shard 6: OK (crc: 0xF0126789)
#
# Object recoverable: YES (4 healthy shards available)

# Repair corrupted shard
objectio-cli object repair bucket/key

# Output:
# Reconstructed shard 3 from shards [1,2,4,5]
# Placed on OSD osd7
# Object now healthy
```

---

## Disaster Recovery

### Regular Backups

```bash
# Backup metadata daily
0 2 * * * objectio-cli meta export --output /backup/meta-$(date +\%Y\%m\%d).json

# Backup critical buckets
0 3 * * * objectio-cli bucket export critical-bucket --output /backup/
```

### Cross-Region Replication

For disaster recovery, replicate to a secondary site:

```toml
# /etc/objectio/replication.toml
[[replication.rule]]
source_bucket = "critical-data"
destination = "https://dr-site.example.com:9000"
destination_bucket = "critical-data-replica"
mode = "async"
```

### Recovery Time Objectives

| Failure Type | RTO | RPO |
|--------------|-----|-----|
| Single disk | Automatic | 0 (no data loss) |
| Single OSD | Automatic | 0 (no data loss) |
| Single metadata node | < 5 seconds | 0 |
| Complete metadata failure | 15-30 minutes | Last snapshot |
| Complete cluster loss | Hours | Last backup |

---

## Failure Simulation

Test your recovery procedures:

```bash
# Simulate disk failure
objectio-cli test fail-disk osd1 /dev/vdc --duration 5m

# Simulate OSD failure
objectio-cli test fail-osd osd3 --duration 5m

# Simulate network partition
objectio-cli test partition --nodes meta1 --duration 2m

# Verify data integrity after test
objectio-cli verify --full
```
