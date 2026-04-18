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

### Raft consensus (openraft 0.9)

The metadata service runs openraft. Every meta pod is a Raft member;
cluster size is a deployment decision via `meta.replicas` in the helm
chart:

| `meta.replicas` | Semantics | Survives |
|---|---|---|
| 1 | Single-voter cluster | Pod restart with PVC intact |
| 3 | 3-voter quorum | 1 pod loss |
| 5 | 5-voter quorum | 2 pod losses |

Phase R1 routes cluster config mutations (`SetConfig` / `DeleteConfig`)
through the Raft log. Other mutations (users, buckets, tenants, etc.)
still write to redb directly pending the R2+ migration — those are
quorum-safe only because they're leader-only in practice.

### Admin API

All endpoints are plain HTTP on the meta admin port (default 9102).
They're meta-local and not exposed via the gateway.

```bash
# Cluster state (leader id, term, voters, learners, last_log_index)
curl http://meta-0:9102/status | jq

# Force a node into the cluster as a learner (non-voting, catches up
# via log replication):
curl -X POST http://meta-0:9102/add-learner \
  -H 'Content-Type: application/json' \
  -d '{"node_id": 4, "addr": "meta-3.meta-headless:9100"}'

# Promote learners to voters (pass the full voter list, not a delta):
curl -X POST http://meta-0:9102/change-membership \
  -H 'Content-Type: application/json' \
  -d '{"voters": [1,2,3,4]}'
```

### Single pod failure in a 3-node cluster

Cluster continues with 2/3 nodes. Election is automatic; the old
leader's clients see `failed_precondition "not the raft leader —
forward to node X"` on the next write and retry there.

```bash
# Simulate a leader crash. Use `--grace-period=0 --force` — a graceful
# shutdown keeps sending heartbeats during the termination window,
# which suppresses the follower election until the container actually
# exits:
kubectl delete pod -n objectio objectio-meta-0 --grace-period=0 --force

# Watch followers elect a new leader (expect ~5s from kill to new
# leader with default heartbeat=250ms, election_timeout=500–1000ms,
# leader_lease=1s):
for i in 1 2; do
  curl -s http://objectio-meta-$i.objectio-meta-headless:9102/status
done
```

**Verified on the datacore kind cluster (2026-04-18):** leader (node 1)
force-deleted at T+0s; meta-1 (node 2) became leader at term=2, log
index 6 at ~T+5s. Meta-2 recognized the new leader in the same
interval. Writes continued against the new leader. When meta-0's
StatefulSet pod came back on its existing PVC ~30s later, it read
its Raft log, noticed the higher term, stepped down from its stored
leader state, received AppendEntries from the new leader, and
rejoined as Follower at term=2/log=6 without operator action.

**Bootstrap addresses matter.** The membership stores the peer
advertise address. Bootstrap must use the new helm chart (which sets
`--raft-advertise $HOSTNAME.objectio-meta-headless:9100`) — a
`kubectl set image` rollover on a pre-Raft StatefulSet misses the new
args and would register node 1 with listen address `0.0.0.0:9100`,
breaking future replication to that pod. Re-install fresh when
rolling up from a pre-R1 release.

### Complete metadata cluster failure

**Scenario:** all meta pods lose their PVCs simultaneously (rare —
typically requires a storage-layer incident).

```bash
# If all pods still have their PVCs, just wait — each pod's redb log
# is durable, so restarting the StatefulSet recovers the cluster.
kubectl rollout restart statefulset/objectio-meta -n objectio

# If PVCs are lost, the cluster is lost — there's no quorum to recover
# from. Treat it like a fresh install:
kubectl delete statefulset/objectio-meta pvc -l app.kubernetes.io/component=meta
helm upgrade objectio ./deploy/helm/objectio -f values.yaml
# Helm's post-install Job re-runs /init + /add-learner + /change-membership
# against the new meta-0.
```

### Network partitions

Raft's quorum requirement prevents split-brain. In a 3-node cluster
partitioned 1 + 2:

- Minority side (1 pod): can't elect a leader, refuses writes with
  `RaftError::Fatal("not ready")`.
- Majority side (2 pods): re-elects a leader if the old one was on the
  minority side; continues serving writes.

When the partition heals, the minority catches up via AppendEntries
and resumes as follower. No manual intervention needed.

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

Meta-side partition behavior is covered in the Metadata Service
Failures section above. Gateway/OSD partitions are handled by
client-side retry.

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
