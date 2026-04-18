# Deployment Topologies

ObjectIO supports various deployment configurations.

## Topology 1: Single-Node Multi-Disk

Best for development, edge, and small deployments.

```
┌─────────────────────────────────────────────────────┐
│                  Single Server                      │
│                                                     │
│  ┌────────┐ ┌────────┐ ┌────────┐ ┌────────┐        │
│  │ Disk 1 │ │ Disk 2 │ │ Disk 3 │ │ Disk 4 │ ...    │
│  │ Shard1 │ │ Shard2 │ │ Shard3 │ │ Shard4 │        │
│  └────────┘ └────────┘ └────────┘ └────────┘        │
│                                                     │
│  EC 4+2: Spread 6 shards across 6+ disks            │
│                                                     │
│  Components:                                        │
│  • objectio-gateway (port 9000)                     │
│  • objectio-meta (embedded, single-node mode)       │
│  • objectio-osd (manages all disks)                 │
└─────────────────────────────────────────────────────┘
```

**Characteristics:**
- Survives: Disk failures (up to m disks)
- Does NOT survive: Server failure
- Storage efficiency: 67% (4+2)

**Example Configuration:**

```yaml
# docker-compose.single-node.yml
services:
  objectio:
    image: objectio:latest
    ports:
      - "9000:9000"
    volumes:
      - /dev/vdb:/dev/vdb
      - /dev/vdc:/dev/vdc
      - /dev/vdd:/dev/vdd
      - /dev/vde:/dev/vde
      - /dev/vdf:/dev/vdf
      - /dev/vdg:/dev/vdg
    privileged: true
```

---

## Topology 2: Multi-Node Single-DC (Rack Aware)

Production single-datacenter deployment.

```
┌─────────────────────────────────────────────────────────────────────┐
│                          Datacenter                                 │
│                                                                     │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐      │
│  │     Rack A      │  │     Rack B      │  │     Rack C      │      │
│  │  ┌───────────┐  │  │  ┌───────────┐  │  │  ┌───────────┐  │      │
│  │  │  Node 1   │  │  │  │  Node 3   │  │  │  │  Node 5   │  │      │
│  │  │ D1 D2 D3  │  │  │  │ D1 D2 D3  │  │  │  │ D1 D2 D3  │  │      │
│  │  │ Shards:   │  │  │  │ Shards:   │  │  │  │ Shards:   │  │      │
│  │  │ 1,4,7,10  │  │  │  │ 2,5,8,11  │  │  │  │ 3,6,9,12  │  │      │
│  │  └───────────┘  │  │  └───────────┘  │  │  └───────────┘  │      │
│  │  ┌───────────┐  │  │  ┌───────────┐  │  │  ┌───────────┐  │      │
│  │  │  Node 2   │  │  │  │  Node 4   │  │  │  │  Node 6   │  │      │
│  │  │ D1 D2 D3  │  │  │  │ D1 D2 D3  │  │  │  │ D1 D2 D3  │  │      │
│  │  └───────────┘  │  │  └───────────┘  │  │  └───────────┘  │      │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘      │
│                                                                     │
│  EC 8+4: Shards distributed across racks                            │
│  Metadata: 3-node Raft cluster (one per rack)                       │
└─────────────────────────────────────────────────────────────────────┘
```

**Characteristics:**
- Survives: Multiple disk, node, and rack failures
- Does NOT survive: DC-wide failure
- Typical setup: 3+ racks, 6+ nodes

**Placement Strategy:**

```yaml
storage_class:
  type: erasure_coding
  ec_profile: "8+4"
  placement: rack  # Shards spread across racks
```

---

## Topology 3: Multi-DC / Multi-AZ

Geo-distributed deployment for disaster recovery.

```
┌───────────────────────┐    ┌───────────────────────┐    ┌───────────────────────┐
│    Datacenter 1       │    │    Datacenter 2       │    │    Datacenter 3       │
│    (Primary)          │    │    (Secondary)        │    │    (DR)               │
│                       │    │                       │    │                       │
│  ┌─────────────────┐  │    │  ┌─────────────────┐  │    │  ┌─────────────────┐  │
│  │   Full Copy 1   │  │    │  │   Full Copy 2   │  │    │  │   Full Copy 3   │  │
│  │   (EC within)   │◄─┼────┼─►│   (EC within)   │◄─┼────┼─►│   (EC within)   │  │
│  └─────────────────┘  │    │  └─────────────────┘  │    │  └─────────────────┘  │
│         │             │    │         │             │    │         │             │
│    ┌────┴────┐        │    │    ┌────┴────┐        │    │    ┌────┴────┐        │
│    │Racks/   │        │    │    │Racks/   │        │    │    │Racks/   │        │
│    │Nodes    │        │    │    │Nodes    │        │    │    │Nodes    │        │
│    └─────────┘        │    │    └─────────┘        │    │    └─────────┘        │
│                       │    │                       │    │                       │
│  Metadata: Local Raft │    │  Metadata: Local Raft │    │  Metadata: Local Raft │
└───────────────────────┘    └───────────────────────┘    └───────────────────────┘
         │                            │                            │
         └────────────────────────────┼────────────────────────────┘
                                      │
                            Async Replication
```

**Data Flow:**
1. Write arrives at DC1 gateway
2. EC encode + write to DC1 storage nodes
3. Async replicate full object to DC2, DC3
4. Read can be served from any DC

**Characteristics:**
- Survives: Complete DC/AZ failure (up to 2 DCs with 3x replication)
- Use case: Geo-distributed, disaster recovery, compliance

**Configuration:**

```yaml
storage_class:
  type: hybrid
  local_protection:
    type: erasure_coding
    ec_profile: "4+2"
    placement: rack
  geo_protection:
    type: replication
    replicas: 3
    sync_mode: async
```

---

## Comparison

| Topology | Failure Tolerance | Complexity | Use Case |
|----------|-------------------|------------|----------|
| Single-node | Disk only | Low | Dev, edge |
| Multi-node | Disk, node, rack | Medium | Production |
| Multi-DC | Disk, node, rack, DC | High | Enterprise |

## Choosing a Topology

### Single-Node Multi-Disk

Choose if:
- Development or testing
- Edge deployment with limited hardware
- Cost-sensitive deployments
- Acceptable single-node failure risk

### Multi-Node Single-DC

Choose if:
- Production workloads
- Node and rack fault tolerance required
- Single datacenter deployment
- No geo-redundancy needs

### Multi-DC / Multi-AZ

Choose if:
- Business-critical data
- Regulatory compliance (data locality)
- Disaster recovery requirements
- Global distribution needed
