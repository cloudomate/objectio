# ObjectIO Deployment Options

This guide covers deployment configurations based on your hardware topology.

## Quick Reference

| Nodes | Disks/Node | Recommended Config          | Efficiency | Survives              |
| ----- | ---------- | --------------------------- | ---------- | --------------------- |
| 1     | 12         | EC 4+2 (disk-level)         | 66%        | 2 disk failures       |
| 3     | 12         | EC 6+3 (node-level)         | 66%        | 1 node failure        |
| 3     | 12         | Replication 3-way           | 33%        | 2 node failures       |
| 6+    | 6+         | LRC 6+2+2 (rack-level)      | 60%        | 1 rack + 1 disk       |
| 10+   | 10+        | LRC 10+2+2 (rack-level)     | 71%        | 1 rack + 1 disk       |

## Failure Domain Hierarchy

```
Region (geo-redundancy)
└── Datacenter (power/network)
    └── Rack (ToR switch, PDU)
        └── Node (server)
            └── Disk (SSD/HDD)
```

CRUSH spreads shards across the **highest available failure domain** to maximize fault tolerance.

## Topology Configuration

Each OSD reports its failure domain on registration. Configure this in `/etc/objectio/osd.toml`:

```toml
[osd]
node_name = "osd-node1"
listen = "0.0.0.0:9002"
meta_endpoint = "http://meta.objectio.local:9001"
weight = 1.0

# Failure domain - where this OSD sits in the hierarchy
[osd.failure_domain]
region = "us-east"        # Geographic region
datacenter = "dc1"        # Data center or AZ
rack = "rack-01"          # Physical rack

[storage]
disks = ["/mnt/nvme0", "/mnt/nvme1", ...]
```

**How CRUSH uses this:**

1. OSDs register with their failure domain
2. Metadata service builds a topology tree
3. When placing shards, CRUSH ensures each shard lands in a **different** failure domain at the configured level
4. If `failure_domain = "rack"`, no two shards of the same object share a rack

See [examples/config/](../../examples/config/) for complete configuration examples.

---

## Scenario 1: Single Node (12 Disks)

**Hardware:** 1 server with 12 disks

**Failure Domain:** Disk-level only (can't survive node failure)

### Option 1A: EC 4+2 (Recommended)

```
┌─────────────────────────────────────────────────────────────────────────┐
│                            Single Node                                   │
│  ┌─────┐ ┌─────┐ ┌─────┐ ┌─────┐ ┌─────┐ ┌─────┐                        │
│  │ D0  │ │ D1  │ │ D2  │ │ D3  │ │ P0  │ │ P1  │  ← Object A (4+2)     │
│  │Disk0│ │Disk1│ │Disk2│ │Disk3│ │Disk4│ │Disk5│                        │
│  └─────┘ └─────┘ └─────┘ └─────┘ └─────┘ └─────┘                        │
│  ┌─────┐ ┌─────┐ ┌─────┐ ┌─────┐ ┌─────┐ ┌─────┐                        │
│  │ D0  │ │ D1  │ │ D2  │ │ D3  │ │ P0  │ │ P1  │  ← Object B (4+2)     │
│  │Disk6│ │Disk7│ │Disk8│ │Disk9│ │Dk10│ │Dk11│                        │
│  └─────┘ └─────┘ └─────┘ └─────┘ └─────┘ └─────┘                        │
└─────────────────────────────────────────────────────────────────────────┘

Configuration:
  ec_k: 4          # Data shards
  ec_m: 2          # Parity shards
  ec_type: MDS     # Reed-Solomon
  failure_domain: disk
```

| Metric              | Value                        |
| ------------------- | ---------------------------- |
| **Storage efficiency** | 66% (4/6)                 |
| **Fault tolerance** | 2 disk failures              |
| **Min disks**       | 6                            |
| **Read performance**| Parallel from 4 disks        |
| **Repair cost**     | Read 4 shards, write 1       |

### Option 1B: EC 8+4 (Maximum Fault Tolerance)

```
Configuration:
  ec_k: 8          # Data shards
  ec_m: 4          # Parity shards
  ec_type: MDS
  failure_domain: disk
```

| Metric              | Value                        |
| ------------------- | ---------------------------- |
| **Storage efficiency** | 66% (8/12)               |
| **Fault tolerance** | 4 disk failures              |
| **Min disks**       | 12                           |
| **Read performance**| Parallel from 8 disks        |
| **Repair cost**     | Read 8 shards, write 1       |

### Option 1C: Replication 3-way (Simple, Lower Efficiency)

```
Configuration:
  ec_k: 1          # Single data copy
  ec_m: 2          # Two additional replicas
  ec_type: MDS     # (Replication is k=1 EC)
  failure_domain: disk
```

| Metric              | Value                        |
| ------------------- | ---------------------------- |
| **Storage efficiency** | 33% (1/3)                |
| **Fault tolerance** | 2 disk failures              |
| **Min disks**       | 3                            |
| **Read performance**| Single disk (can load-balance)|
| **Repair cost**     | Read 1, write 1              |

**When to use replication:**
- Small objects (< 1MB) where EC overhead dominates
- Random read workloads where single-disk latency matters
- Simplest operational model

---

## Scenario 2: Three Nodes (12 Disks Each)

**Hardware:** 3 servers, each with 12 disks (36 total disks)

**Failure Domain Options:**
- **Node-level:** Survive 1 complete node failure
- **Disk-level:** Survive multiple disk failures across nodes

### Option 2A: EC 6+3 Node-Level (Recommended)

```
┌─────────────────────────────────────────────────────────────────────────┐
│   Node 0 (12 disks)    │   Node 1 (12 disks)    │   Node 2 (12 disks)  │
│  ┌────┐┌────┐┌────┐    │  ┌────┐┌────┐┌────┐    │  ┌────┐┌────┐┌────┐  │
│  │ D0 ││ D1 ││ D2 │    │  │ D3 ││ D4 ││ D5 │    │  │ P0 ││ P1 ││ P2 │  │
│  └────┘└────┘└────┘    │  └────┘└────┘└────┘    │  └────┘└────┘└────┘  │
│                        │                        │                      │
│  Each node gets 3      │  Each node gets 3      │  Each node gets 3    │
│  shards per object     │  shards per object     │  shards per object   │
└─────────────────────────────────────────────────────────────────────────┘

Configuration:
  ec_k: 6          # Data shards
  ec_m: 3          # Parity shards
  ec_type: MDS
  failure_domain: node
```

| Metric              | Value                        |
| ------------------- | ---------------------------- |
| **Storage efficiency** | 66% (6/9)                |
| **Fault tolerance** | 1 node OR 3 disks            |
| **Min nodes**       | 3                            |
| **Shards per node** | 3                            |
| **Read performance**| Parallel from 2+ nodes       |
| **Repair cost**     | Read 6 shards (cross-node)   |

**Math:** With 3 nodes and k=6, m=3:
- 9 shards ÷ 3 nodes = 3 shards per node
- Node failure loses 3 shards = exactly m
- Can reconstruct from remaining 6 shards ✓

### Option 2B: EC 4+2 Node-Level (Fewer Shards)

```
┌─────────────────────────────────────────────────────────────────────────┐
│   Node 0 (12 disks)    │   Node 1 (12 disks)    │   Node 2 (12 disks)  │
│  ┌────┐┌────┐          │  ┌────┐┌────┐          │  ┌────┐┌────┐        │
│  │ D0 ││ D1 │          │  │ D2 ││ D3 │          │  │ P0 ││ P1 │        │
│  └────┘└────┘          │  └────┘└────┘          │  └────┘└────┘        │
│                        │                        │                      │
│  2 shards per node     │  2 shards per node     │  2 shards per node   │
└─────────────────────────────────────────────────────────────────────────┘

Configuration:
  ec_k: 4
  ec_m: 2
  ec_type: MDS
  failure_domain: node
```

| Metric              | Value                        |
| ------------------- | ---------------------------- |
| **Storage efficiency** | 66% (4/6)                |
| **Fault tolerance** | 1 node OR 2 disks            |
| **Min nodes**       | 3                            |
| **Shards per node** | 2                            |

### Option 2C: Replication 3-way (Maximum Availability)

```
┌─────────────────────────────────────────────────────────────────────────┐
│   Node 0               │   Node 1               │   Node 2              │
│  ┌────────────────┐    │  ┌────────────────┐    │  ┌────────────────┐   │
│  │   Full Copy    │    │  │   Full Copy    │    │  │   Full Copy    │   │
│  │   (Primary)    │    │  │   (Replica 1)  │    │  │   (Replica 2)  │   │
│  └────────────────┘    │  └────────────────┘    │  └────────────────┘   │
└─────────────────────────────────────────────────────────────────────────┘

Configuration:
  ec_k: 1
  ec_m: 2
  ec_type: MDS
  failure_domain: node
```

| Metric              | Value                        |
| ------------------- | ---------------------------- |
| **Storage efficiency** | 33% (1/3)                |
| **Fault tolerance** | 2 node failures              |
| **Read performance**| Any single node (load-balance)|
| **Write performance**| Must write to all 3 nodes   |

**Use replication when:**
- Data durability is paramount (financial, medical)
- Random read latency is critical
- Simple operational model preferred over efficiency

---

## Scenario 3: Six+ Nodes (Multi-Rack with LRC)

**Hardware:** 6+ servers across multiple racks

**Failure Domain:** Rack-level (survive ToR switch or PDU failure)

### Option 3A: LRC 6+2+2 (Recommended for 6+ Nodes)

LRC (Locally Repairable Codes) adds **local parity** within groups for faster single-failure repair.

```
┌─────────────────────────────────────────────────────────────────────────┐
│                              LRC 6+2+2                                   │
│                                                                         │
│  ┌──────────────────────────────┐  ┌──────────────────────────────┐     │
│  │        Local Group 0         │  │        Local Group 1         │     │
│  │  ┌────┐┌────┐┌────┐┌────┐   │  │  ┌────┐┌────┐┌────┐┌────┐   │     │
│  │  │ D0 ││ D1 ││ D2 ││ LP0│   │  │  │ D3 ││ D4 ││ D5 ││ LP1│   │     │
│  │  │Rk0 ││Rk0 ││Rk0 ││Rk0 │   │  │  │Rk1 ││Rk1 ││Rk1 ││Rk1 │   │     │
│  │  └────┘└────┘└────┘└────┘   │  │  └────┘└────┘└────┘└────┘   │     │
│  └──────────────────────────────┘  └──────────────────────────────┘     │
│                                                                         │
│  ┌──────────────────────────────┐                                       │
│  │       Global Parity          │                                       │
│  │  ┌────┐┌────┐               │                                       │
│  │  │GP0 ││GP1 │               │  GP = Reed-Solomon over all data      │
│  │  │Rk2 ││Rk2 │               │                                       │
│  │  └────┘└────┘               │                                       │
│  └──────────────────────────────┘                                       │
└─────────────────────────────────────────────────────────────────────────┘

Configuration:
  ec_k: 6              # Data shards
  ec_type: LRC
  ec_local_parity: 2   # One per local group
  ec_global_parity: 2  # Reed-Solomon over all data
  local_group_size: 3  # Data shards per local group
  failure_domain: rack
```

| Metric              | Value                        |
| ------------------- | ---------------------------- |
| **Storage efficiency** | 60% (6/10)               |
| **Total shards**    | 10 (6 data + 2 LP + 2 GP)    |
| **Fault tolerance** | 1 rack + 1 disk, OR 4 disks  |
| **Local repair**    | Read 3 shards (within rack)  |
| **Global repair**   | Read 6 shards (cross-rack)   |

**LRC Repair Scenarios:**

| Failure Type          | Repair Method                | Network Cost        |
| --------------------- | ---------------------------- | ------------------- |
| 1 data disk           | XOR with local parity        | Read 3 (local)      |
| 1 local parity disk   | XOR with data shards         | Read 3 (local)      |
| 2 disks same group    | Reed-Solomon global          | Read 6 (cross-rack) |
| 1 disk + 1 LP (diff)  | Local + Local                | Read 6 (2×3 local)  |
| Full rack failure     | Reed-Solomon global          | Read 6 (cross-rack) |

### Option 3B: LRC 10+2+2 (Higher Efficiency)

```
Configuration:
  ec_k: 10             # Data shards
  ec_type: LRC
  ec_local_parity: 2   # One per local group
  ec_global_parity: 2
  local_group_size: 5  # 5 data shards per group
  failure_domain: rack
```

| Metric              | Value                        |
| ------------------- | ---------------------------- |
| **Storage efficiency** | 71% (10/14)              |
| **Total shards**    | 14 (10 data + 2 LP + 2 GP)   |
| **Min racks**       | 4 (5+1 per local group + GP) |
| **Local repair**    | Read 5 shards (within rack)  |

---

## Choosing Between EC and Replication

```
                    ┌─────────────────────────────────────────┐
                    │         Decision Flow                    │
                    └─────────────────────────────────────────┘
                                      │
                    ┌─────────────────┴─────────────────┐
                    ▼                                   ▼
         ┌─────────────────────┐           ┌─────────────────────┐
         │  Small objects      │           │  Large objects      │
         │  (< 1 MB)           │           │  (> 1 MB)           │
         └──────────┬──────────┘           └──────────┬──────────┘
                    │                                   │
                    ▼                                   ▼
         ┌─────────────────────┐           ┌─────────────────────┐
         │  Replication 3-way  │           │  Single DC?         │
         │  (EC overhead high) │           └──────────┬──────────┘
         └─────────────────────┘                      │
                                     ┌────────────────┴────────────────┐
                                     ▼                                 ▼
                          ┌─────────────────────┐          ┌─────────────────────┐
                          │  Yes: EC within DC  │          │  No: Replicate      │
                          │  (4+2 or 6+3)       │          │  across DCs         │
                          └──────────┬──────────┘          └─────────────────────┘
                                     │
                          ┌──────────┴──────────┐
                          ▼                     ▼
               ┌─────────────────────┐  ┌─────────────────────┐
               │  6+ nodes, racks?   │  │  3 nodes?           │
               │  → LRC 6+2+2        │  │  → EC 6+3 or 4+2    │
               └─────────────────────┘  └─────────────────────┘
```

---

## Configuration Examples

### Single Node (12 disks) - EC 4+2

```toml
# /etc/objectio/osd.toml
[osd]
node_name = "osd-single"
listen = "0.0.0.0:9002"
meta_endpoint = "http://localhost:9001"

# Single node: all OSDs share the same failure domain
# CRUSH spreads shards across different DISKS
[osd.failure_domain]
region = "default"
datacenter = "dc1"
rack = "rack1"

[storage]
data_dir = "/var/lib/objectio/osd"
disks = [
  "/mnt/nvme0", "/mnt/nvme1", "/mnt/nvme2", "/mnt/nvme3",
  "/mnt/nvme4", "/mnt/nvme5", "/mnt/nvme6", "/mnt/nvme7",
  "/mnt/nvme8", "/mnt/nvme9", "/mnt/nvme10", "/mnt/nvme11"
]
```

Gateway config:

```toml
# /etc/objectio/gateway.toml - EC 4+2 for single node
ec_k = 4
ec_m = 2
```

### Three Nodes - EC 6+3

Each node needs a **different rack** in its failure domain:

```toml
# Node 1: /etc/objectio/osd.toml
[osd]
node_name = "osd-node1"
listen = "0.0.0.0:9002"
meta_endpoint = "http://meta.objectio.local:9001"

[osd.failure_domain]
region = "us-east"
datacenter = "dc1"
rack = "rack-01"        # <-- Different rack per node!

[storage]
disks = ["/mnt/nvme0", "/mnt/nvme1", ..., "/mnt/nvme11"]
```

```toml
# Node 2: /etc/objectio/osd.toml
[osd.failure_domain]
region = "us-east"
datacenter = "dc1"
rack = "rack-02"        # <-- Different rack
```

```toml
# Node 3: /etc/objectio/osd.toml
[osd.failure_domain]
region = "us-east"
datacenter = "dc1"
rack = "rack-03"        # <-- Different rack
```

Gateway config:

```toml
# /etc/objectio/gateway.toml - EC 6+3 for three nodes
ec_k = 6
ec_m = 3
```

### Six+ Nodes - LRC 6+2+2

```toml
# /etc/objectio/gateway.toml
ec_k = 6
ec_local_parity = 2
ec_global_parity = 2
local_group_size = 3
```

See [examples/config/](../../examples/config/) for complete, ready-to-use configuration files.

---

## Capacity Planning

### Usable Capacity Formula

```
Usable = Raw × (k / (k + m))        # For MDS
Usable = Raw × (k / (k + l + g))    # For LRC
```

### Example: 3 Nodes × 12 Disks × 10TB

| Config           | Raw Capacity | Efficiency | Usable Capacity |
| ---------------- | ------------ | ---------- | --------------- |
| Replication 3    | 360 TB       | 33%        | 120 TB          |
| EC 4+2           | 360 TB       | 66%        | 240 TB          |
| EC 6+3           | 360 TB       | 66%        | 240 TB          |
| LRC 6+2+2        | 360 TB       | 60%        | 216 TB          |

### IOPS Considerations

| Config         | Read IOPS (per object)      | Write IOPS (per object)     |
| -------------- | --------------------------- | --------------------------- |
| Replication 3  | 1 disk (can load-balance)   | 3 disks (all replicas)      |
| EC 4+2         | 4 disks (parallel)          | 6 disks (parallel)          |
| EC 6+3         | 6 disks (parallel)          | 9 disks (parallel)          |
| LRC 6+2+2      | 6 disks (parallel)          | 10 disks (parallel)         |

---

## Migration Paths

### Starting Small, Growing Large

```
Phase 1: Single node, 6 disks
  └── EC 4+2 (disk-level)
         │
         ▼
Phase 2: 3 nodes, 12 disks each
  └── EC 6+3 (node-level)
         │
         ▼
Phase 3: 6+ nodes across racks
  └── LRC 6+2+2 (rack-level)
```

**Data migration:** ObjectIO supports online rebalancing when topology changes. Adding nodes triggers gradual data movement via background repair.

---

## Summary Recommendations

| Deployment Size | Recommended Config | Why |
| --------------- | ------------------ | --- |
| **Dev/Test**    | Single node, EC 4+2 | Simple, efficient, survives disk failures |
| **Small Prod**  | 3 nodes, EC 6+3     | Survives node failure, good efficiency |
| **Medium Prod** | 6 nodes, LRC 6+2+2  | Fast local repair, rack-level tolerance |
| **Large Prod**  | 10+ nodes, LRC 10+2+2 | Best efficiency (71%), scalable |
| **Critical**    | 3-way replication   | Maximum durability, simple operations |

---

## Real-World Example: Datacore Deployment

Current production deployment on `datacore` (192.168.4.102):

### Topology

- **Single node** with file-based storage
- **Replication count**: 1 (dev/test mode)
- **Services**: Meta + OSD + Gateway (all on same host)

### Docker Compose

```yaml
# /data/objectio/docker-compose.yml
services:
  meta:
    image: cr.imys.in/objectio/objectio-meta:latest
    container_name: objectio-meta
    command:
      - "--node-id"
      - "1"
      - "--listen"
      - "0.0.0.0:9100"
      - "--replication"
      - "1"
      - "--log-level"
      - "info"
    volumes:
      - ./meta:/var/lib/objectio
    ports:
      - "9100:9100"
    networks:
      - objectio-net

  osd:
    image: cr.imys.in/objectio/objectio-osd:latest
    container_name: objectio-osd
    user: root
    command:
      - "--config"
      - "/etc/objectio/osd.toml"
    volumes:
      - ./config/osd.toml:/etc/objectio/osd.toml:ro
      - ./storage:/data/objectio/storage
      - ./wal:/var/lib/objectio/wal
    ports:
      - "9200:9200"
    depends_on:
      meta:
        condition: service_healthy
    networks:
      - objectio-net

  gateway:
    image: cr.imys.in/objectio/objectio-gateway:latest
    container_name: objectio-gateway
    command:
      - "--listen"
      - "0.0.0.0:9000"
      - "--meta-endpoint"
      - "http://meta:9100"
      - "--region"
      - "us-east-1"
      - "--log-level"
      - "info"
    ports:
      - "9000:9000"
    depends_on:
      - meta
      - osd
    networks:
      - objectio-net

networks:
  objectio-net:
    driver: bridge
```

### OSD Configuration

```toml
# /data/objectio/config/osd.toml
[osd]
node_name = "osd1"
listen = "0.0.0.0:9200"
advertise_addr = "osd:9200"
meta_endpoint = "http://meta:9100"
weight = 1.0

[osd.failure_domain]
region = "local"
datacenter = "datacore"
rack = "rack-01"

[storage]
# File-based storage (auto-created, default 10GB)
disks = ["/data/objectio/storage/disk0.img"]
data_dir = "/var/lib/objectio/osd"

[logging]
level = "debug"
```

### Directory Structure

```
/data/objectio/
├── docker-compose.yml
├── config/
│   └── osd.toml
├── meta/              # Metadata service data
├── storage/           # OSD block storage
│   └── disk0.img      # File-based disk image
└── wal/               # OSD write-ahead log
```

### Usage

```bash
# Get admin credentials (shown in meta service logs)
docker logs objectio-meta | grep -A3 "Admin credentials"

# Use with AWS CLI
export AWS_ACCESS_KEY_ID=AKIA...
export AWS_SECRET_ACCESS_KEY=...
aws --endpoint-url http://192.168.4.102:9000 s3 ls
aws --endpoint-url http://192.168.4.102:9000 s3 mb s3://mybucket
aws --endpoint-url http://192.168.4.102:9000 s3 cp file.txt s3://mybucket/
```

### Upgrading

```bash
# On build server (192.168.4.225)
cd /home/ubuntu/objectio
make docker DOCKER_REGISTRY=cr.imys.in/objectio/
make push DOCKER_REGISTRY=cr.imys.in/objectio/

# On datacore (192.168.4.102)
cd /data/objectio
docker compose pull
docker compose down
docker compose up -d
```

---

## See Also

- [Data Protection](data-protection.md) - Deep dive into erasure coding math
- [CRUSH Algorithm](../internals/crush.md) - Placement algorithm details
- [Failure Recovery](../operations/recovery.md) - Repair and rebalancing procedures
