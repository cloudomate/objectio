# ObjectIO Configuration Examples

This directory contains example configurations for different deployment topologies.

## Topology Overview

ObjectIO uses CRUSH (Controlled Replication Under Scalable Hashing) for data placement.
Each OSD reports its **failure domain** on registration, and CRUSH spreads shards across
different failure domains to maximize fault tolerance.

### Failure Domain Hierarchy

```
region (us-east)
└── datacenter (dc1)
    └── rack (rack-01)
        └── node (osd-node1)
            └── disk (/mnt/nvme0)
```

### How Placement Works

1. OSD starts and reads its `[osd.failure_domain]` from config
2. OSD registers with metadata service, reporting its failure domain
3. Metadata service builds the cluster topology
4. When placing object shards, CRUSH ensures:
   - Each shard lands on a different failure domain (at the configured level)
   - If `failure_domain = "rack"`, no two shards of the same object share a rack

## Directory Structure

```
config/
├── single-node/          # Single server with multiple disks
│   └── osd.toml
│
├── three-nodes/          # Three servers in different racks
│   ├── node1-osd.toml
│   ├── node2-osd.toml
│   ├── node3-osd.toml
│   └── gateway.toml
│
└── multi-rack-lrc/       # 6+ nodes for LRC codes
    └── osd-rack1-node1.toml
```

## Deployment Scenarios

### Single Node (Development / Small Scale)

**Hardware:** 1 server with 6-12 NVMe drives

**Config:** `single-node/osd.toml`

```toml
[osd.failure_domain]
region = "default"
datacenter = "dc1"
rack = "rack1"
```

**Recommended EC:** `4+2` (disk-level fault tolerance)

- All disks share the same failure domain
- CRUSH spreads shards across different **disks**
- Survives: 2 disk failures
- Cannot survive: node failure

### Three Nodes (Production Small)

**Hardware:** 3 servers, each with 12 NVMe drives, in different racks

**Config:** `three-nodes/node{1,2,3}-osd.toml`

```toml
# node1-osd.toml
[osd.failure_domain]
region = "us-east"
datacenter = "dc1"
rack = "rack-01"   # Different rack per node!

# node2-osd.toml
[osd.failure_domain]
region = "us-east"
datacenter = "dc1"
rack = "rack-02"

# node3-osd.toml
[osd.failure_domain]
region = "us-east"
datacenter = "dc1"
rack = "rack-03"
```

**Recommended EC:** `6+3` (node-level fault tolerance)

- Each node is in a different rack
- CRUSH spreads 9 shards across 3 racks (3 per rack)
- Survives: 1 full node/rack failure
- Math: With 3 parity shards, can lose any 3 shards

### Multi-Rack LRC (Large Scale)

**Hardware:** 6+ servers across multiple racks

**Config:** `multi-rack-lrc/osd-*.toml`

**Recommended EC:** LRC `6+2+2`

- 6 data shards, 2 local parity, 2 global parity
- Faster single-failure repair (only read 3 shards vs 6)
- Survives: 1 full rack + 1 additional disk

## Key Configuration Fields

### `[osd.failure_domain]`

| Field        | Description                          | Example        |
|--------------|--------------------------------------|----------------|
| `region`     | Geographic region                    | `us-east`      |
| `datacenter` | Data center or availability zone     | `us-east-1a`   |
| `rack`       | Physical rack or network segment     | `rack-01`      |

### `weight`

Placement weight for this OSD. Higher weight = more data assigned.

Use this to balance data when mixing different disk sizes:
- 8TB disk: `weight = 1.0`
- 16TB disk: `weight = 2.0`

## Quick Start

1. Copy the appropriate example to `/etc/objectio/osd.toml`
2. Edit the failure domain to match your topology
3. Update the disk paths
4. Start the OSD: `objectio-osd --config /etc/objectio/osd.toml`

```bash
# Example: Node 2 in a three-node deployment
cp examples/config/three-nodes/node2-osd.toml /etc/objectio/osd.toml

# Edit failure domain and disks
vim /etc/objectio/osd.toml

# Start
objectio-osd --config /etc/objectio/osd.toml
```

## Verification

After starting OSDs, verify the topology:

```bash
# Check registered OSDs and their failure domains
objectio-cli cluster status

# Example output:
# Topology Version: 3
# Nodes:
#   osd-node1 (us-east/dc1/rack-01): 12 disks, ACTIVE
#   osd-node2 (us-east/dc1/rack-02): 12 disks, ACTIVE
#   osd-node3 (us-east/dc1/rack-03): 12 disks, ACTIVE
```
