# Topology & Fabric Awareness Roadmap

Status: planning. Phases land in order but work within a phase can parallelize.

## Where we are today

`OsdNode.failure_domain: Option<(region, datacenter, rack)>` is collected at
`register_osd` and persisted. CRUSH2 consumes it for placement — but as a
*soft* hint: if the cluster can't satisfy a rack-spread it still writes,
just with a weaker domain split. We have no view on top switches, zones,
or network latency, and clients have no way to advertise their location.

Goal of this roadmap: turn topology from "metadata we collect" into "a
first-class input to placement, reads, repair, and maintenance."

---

## Phase 1 — Tighten what we already have

**Outcome**: failure-domain constraints are honored as hard guarantees, the
domain model can describe 5 levels (not 3), and operators can see cluster
topology in the console without `kubectl exec` gymnastics.

### 1.1 Hard-enforce failure-domain constraints at placement

- Pool config already has `failure_domain: String` ("rack" | "datacenter" | …).
  Make it mean "placement MUST spread across ≥ `ec_k + ec_m` distinct values
  at this level". Today we only try.
- On a write, if the active topology can't satisfy the constraint, return
  `InsufficientCapacity` instead of a silent degraded placement.
- Admin endpoint `GET /_admin/placement/validate?pool=X` that explains
  exactly why a pool is satisfiable or not against the current topology.
- Tests: synthetic topologies with missing domains.

### 1.2 Extend `FailureDomainInfo`

Current:
```proto
message FailureDomainInfo { string region = 1; string datacenter = 2; string rack = 3; }
```
Target (additive, backward-compatible):
```proto
message FailureDomainInfo {
  string region     = 1;
  string zone       = 4;   // availability zone (cloud) or row (on-prem)
  string datacenter = 2;
  string rack       = 3;
  string host       = 5;   // physical host (below rack; above OSD process)
}
```
Empty string = inherit from enclosing level (matches current behavior).
Pools can use any of these as their `failure_domain`.

### 1.3 Console topology view

New page `/topology` renders the OSD tree:
```
region=us-east
  zone=us-east-1a
    datacenter=dc1
      rack=rack-01
        host=node-01  (OSD 7c:e6:d3…, 8 disks, 12 TiB raw)
        host=node-02  …
```
+ overlay: per-pool constraint satisfaction ("pool `hot-4+2` OK across 6 racks").
Pull data from `GET /_admin/topology` (new endpoint that aggregates OsdNode
+ computed distance groupings).

**Proto changes**: additive fields on `FailureDomainInfo`.
**Scope**: 1–2 weeks.
**Enterprise gate**: none — topology quality-of-life is table stakes.

---

## Phase 2 — Locality-aware reads

**Outcome**: the gateway prefers shards it's physically close to, collapsing
cross-DC bandwidth on reads in multi-site deployments.

### 2.1 Gateway self-positioning

New CLI flags / env / config keys:
- `--topology-region=us-east --topology-zone=us-east-1a --topology-dc=dc1 --topology-rack=rack-02`

Stored on `AppState.self_topology: FailureDomain`. Visible in
`GET /health` and `/_admin/cluster-info`.

### 2.2 Topology distance metric

```rust
// Both positions as (region, zone, dc, rack, host); distance = number of
// levels up we have to climb before they share a prefix. Same host = 0,
// same rack = 1, same DC = 2, same region = 4, different region = 5.
pub fn distance(a: &FailureDomain, b: &FailureDomain) -> u8;
```

### 2.3 Read-path routing by distance

In `scatter_gather::list_objects` and the EC read path:
- Build candidate shard set as today.
- Sort by `distance(gateway.self_topology, shard.host_topology)`.
- Fetch the nearer shards first; fall back to farther on error.

EC reads: for k+m codes, pick the k *cheapest* data shards rather than the
first k to respond. Parity reconstruction only kicks in when data shards
in the near set are unhealthy.

### 2.4 Metrics

Add `objectio_read_cross_zone_bytes_total` and
`objectio_read_cross_dc_bytes_total` so Prometheus can show the multi-DC
bandwidth savings.

**Proto changes**: `ListingNode` grows `FailureDomainInfo`.
**Scope**: 2–3 weeks.
**Enterprise gate**: none (benefits all tiers).

---

## Phase 3 — Maintenance ergonomics

**Outcome**: operators can take a rack or node out of service, run a
rolling OS / binary upgrade, and put it back without a 3 AM incident.

### 3.1 Domain drain

- `POST /_admin/domains/{kind}/{name}/drain` — e.g. `.../rack/rack-03/drain`
  or `.../host/node-07/drain`.
- Marks the domain *out*: no new placement, new reads/writes avoid it, but
  existing data stays readable. Background repair sources rebuilds from
  outside the drained domain.
- `POST /_admin/domains/{kind}/{name}/undrain` to put it back.
- CLI: `objectio-cli domain drain/undrain/list`.

### 3.2 Node-by-node rolling upgrade (user ask)

New admin workflow:
1. `objectio-cli upgrade start --target <image-tag>` records the intent.
2. For each OSD (ordered by health, least-loaded first):
   a. Drain the host domain.
   b. Wait until data on that host has repair-quorum elsewhere.
   c. Signal the host to restart onto the new binary/image.
   d. Wait for `register_osd` to reappear and health probes to pass.
   e. Undrain.
3. Repeat for meta (if StatefulSet > 1) and gateway.
4. Abort on any `degraded` health check; surface via
   `GET /_admin/upgrade/status`.

Kubernetes Operator integration: the same protocol drives a k8s Operator
that handles StatefulSet rollouts in the right order.

### 3.3 Repair budget per topology link

- `--repair-cross-rack-mbps` and `--repair-cross-dc-mbps` on OSD (or in
  meta config, dynamically configurable).
- Repair engine enforces a token bucket per *(source_domain, dest_domain)*
  pair. Prevents backfill from starving client I/O over the long-haul link.

### 3.4 Immutable OS (Enterprise, teaser)

Track separately — see "Future tracks" below. Rolling upgrade here is the
OS-agnostic version; the Enterprise track adds A/B partition OS image
swaps + signed rollbacks.

**Proto changes**: add `DrainStatus` enum to `OsdNode`;
`UpgradePlan` / `UpgradeProgress` messages.
**Scope**: 3–4 weeks for drain + upgrade; repair throttle is additive.
**Enterprise gate**: bulk drain/upgrade workflow is fine for Community;
operator mode may be Enterprise.

---

## Phase 4 — Network fabric

**Outcome**: placement understands that two hosts in the same rack but on
different TORs are not as close as two hosts on the same TOR. Enables
rack-local LRC and RDMA-aware EC rebuild.

### 4.1 Extend `FailureDomainInfo` with fabric fields

Additive:
```proto
string pod        = 6;   // network pod / cell
string tor_switch = 7;   // top-of-rack switch identifier
string rdma_group = 8;   // RDMA-connected island (empty = no RDMA)
```

### 4.2 Rack-local LRC

Already have LRC as an erasure type (Enterprise-gated). Make the stripe
builder place local-parity groups *within a single rack* so single-rack
loss rebuilds from in-rack survivors only.

### 4.3 Client-side hints

SDK / CLI / S3 client can send:
`x-objectio-client-region: us-east-1` (and zone/dc/rack if known).
Gateway uses this as its own position for that request when self-positioning
isn't sharp enough (e.g., a gateway serving many zones).

### 4.4 RDMA-aware rebuild

If source + dest share `rdma_group`, prefer RDMA for the rebuild transfer.
Requires OSDs to expose RDMA endpoints — separate infra work.

**Proto changes**: additive fields.
**Scope**: 2–3 weeks for fabric fields + rack-local LRC; RDMA is its own
epic once we have an RDMA transport.
**Enterprise gate**: rack-local LRC is already Enterprise; RDMA transport
also Enterprise.

---

## Phase 5 — Cross-site replication

**Outcome**: asynchronous bucket replication across regions with conflict
resolution; bucket-level region pinning for compliance.

Out of scope for this doc; tracked as its own epic. Shape:
- Replication policy per bucket (target site, filter, priority).
- Source-side WAL of pending replications; target-side apply with
  idempotence.
- Failover + failback drills. Conflict policy = last-write-wins initially.
- S3 API compatibility with AWS Cross-Region Replication where possible.

---

## Future tracks

### Immutable OS — Enterprise-only (user ask)

Ship cluster nodes on a signed, read-only base image (think Fedora CoreOS,
Talos, Bottlerocket). The Enterprise delta is:
- Reproducible builds, Ed25519-signed images
- A/B partition — boot on the new image, auto-rollback on fail probe
- TPM-measured boot; license ties to a signed attestation
- No runtime `apt install` possible; admin plane is entirely via our API
- Patches ship as OS image bumps, triggered by Phase 3's rolling upgrade

Requires: Phase 3 rolling upgrade as the delivery mechanism; a separate
image-build pipeline in the Enterprise repo; signing infra already in
place from the license work.

### Heterogeneous storage tiers

NVMe/HDD/tape awareness: pools carry a medium tag, placement picks by
pool, lifecycle moves objects between tiers. Complementary to topology.

### Topology-aware client SDK

Smart client that:
- Reads cluster topology once per session
- Caches shard locations
- Sends requests directly to nearest gateway or, eventually, directly to
  OSDs for reads (skip the gateway hop entirely for big objects).

---

## Immediate next step

Start Phase 1. Mechanical work with a clear end state: hard-enforced
constraints, 5-level domain model, and a topology page. The proto change
is additive and doesn't disturb Phase 2's locality work that comes next.

If you want, I'll open TaskCreate entries for 1.1 / 1.2 / 1.3 and kick off
the proto change + CRUSH placement enforcement first, since everything
else in Phase 1 sits on top of it.
