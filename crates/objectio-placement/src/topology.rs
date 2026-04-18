//! Cluster topology representation

use objectio_common::{DiskId, DiskStatus, FailureDomain, NodeId, NodeStatus};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::SocketAddr;

/// Cluster topology containing all nodes organized by failure domain
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ClusterTopology {
    /// Version number (incremented on changes)
    pub version: u64,
    /// All regions in the cluster
    pub regions: HashMap<String, RegionInfo>,
}

impl ClusterTopology {
    /// Create a new empty topology
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Get all nodes in the cluster
    pub fn all_nodes(&self) -> impl Iterator<Item = &NodeInfo> {
        self.regions.values().flat_map(|r| {
            r.datacenters
                .values()
                .flat_map(|dc| dc.racks.values().flat_map(|rack| rack.nodes.values()))
        })
    }

    /// Get a specific node by ID
    pub fn get_node(&self, node_id: NodeId) -> Option<&NodeInfo> {
        self.all_nodes().find(|n| n.id == node_id)
    }

    /// Get all active nodes
    pub fn active_nodes(&self) -> impl Iterator<Item = &NodeInfo> {
        self.all_nodes().filter(|n| n.status == NodeStatus::Active)
    }

    /// Add or update a node
    pub fn upsert_node(&mut self, node: NodeInfo) {
        let region = self
            .regions
            .entry(node.failure_domain.region.clone())
            .or_insert_with(|| RegionInfo::new(&node.failure_domain.region));

        let dc = region
            .datacenters
            .entry(node.failure_domain.datacenter.clone())
            .or_insert_with(|| DatacenterInfo::new(&node.failure_domain.datacenter));

        let rack = dc
            .racks
            .entry(node.failure_domain.rack.clone())
            .or_insert_with(|| RackInfo::new(&node.failure_domain.rack));

        rack.nodes.insert(node.id, node);
        self.version += 1;
    }

    /// Remove a node
    pub fn remove_node(&mut self, node_id: NodeId) -> Option<NodeInfo> {
        for region in self.regions.values_mut() {
            for dc in region.datacenters.values_mut() {
                for rack in dc.racks.values_mut() {
                    if let Some(node) = rack.nodes.remove(&node_id) {
                        self.version += 1;
                        return Some(node);
                    }
                }
            }
        }
        None
    }
}

/// Region information
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RegionInfo {
    /// Region name
    pub name: String,
    /// Datacenters in this region
    pub datacenters: HashMap<String, DatacenterInfo>,
}

impl RegionInfo {
    /// Create a new region
    #[must_use]
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            datacenters: HashMap::new(),
        }
    }
}

/// Datacenter information
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DatacenterInfo {
    /// Datacenter name
    pub name: String,
    /// Racks in this datacenter
    pub racks: HashMap<String, RackInfo>,
}

impl DatacenterInfo {
    /// Create a new datacenter
    #[must_use]
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            racks: HashMap::new(),
        }
    }
}

/// Rack information
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RackInfo {
    /// Rack name
    pub name: String,
    /// Nodes in this rack
    pub nodes: HashMap<NodeId, NodeInfo>,
}

impl RackInfo {
    /// Create a new rack
    #[must_use]
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            nodes: HashMap::new(),
        }
    }
}

/// Node information
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NodeInfo {
    /// Node unique identifier
    pub id: NodeId,
    /// Human-readable name
    pub name: String,
    /// gRPC endpoint address
    pub address: SocketAddr,
    /// Failure domain (region, datacenter, rack)
    pub failure_domain: FailureDomainInfo,
    /// Node status
    pub status: NodeStatus,
    /// Disks on this node
    pub disks: Vec<DiskInfo>,
    /// Weight for placement (higher = more data)
    pub weight: f64,
    /// Last heartbeat timestamp
    pub last_heartbeat: u64,
}

impl NodeInfo {
    /// Get total capacity across all disks
    #[must_use]
    pub fn total_capacity(&self) -> u64 {
        self.disks.iter().map(|d| d.total_capacity).sum()
    }

    /// Get used capacity across all disks
    #[must_use]
    pub fn used_capacity(&self) -> u64 {
        self.disks.iter().map(|d| d.used_capacity).sum()
    }

    /// Get available capacity
    #[must_use]
    pub fn available_capacity(&self) -> u64 {
        self.total_capacity().saturating_sub(self.used_capacity())
    }

    /// Check if node has capacity for a shard of given size
    #[must_use]
    pub fn has_capacity(&self, size: u64) -> bool {
        self.available_capacity() >= size
    }

    /// Get healthy disks
    pub fn healthy_disks(&self) -> impl Iterator<Item = &DiskInfo> {
        self.disks
            .iter()
            .filter(|d| d.status == DiskStatus::Healthy)
    }
}

/// Failure domain information for a node.
///
/// Empty string at any level means "inherit from the enclosing level".
/// The additive `zone` and `host` fields default to empty on old
/// serialized topologies so nothing in the persistent store breaks.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct FailureDomainInfo {
    /// Region name
    pub region: String,
    /// Availability zone (cloud) or row (on-prem) — sits between region
    /// and datacenter.
    #[serde(default)]
    pub zone: String,
    /// Datacenter name
    pub datacenter: String,
    /// Rack name
    pub rack: String,
    /// Physical host identifier — below rack, above the OSD process.
    /// Multiple OSDs on the same host share failure correlation.
    #[serde(default)]
    pub host: String,
}

impl FailureDomainInfo {
    /// Create a new failure domain info with region/datacenter/rack only.
    /// Fine for single-zone / single-host deployments; use
    /// [`Self::new_full`] to populate all five levels.
    #[must_use]
    pub fn new(region: &str, datacenter: &str, rack: &str) -> Self {
        Self {
            region: region.to_string(),
            zone: String::new(),
            datacenter: datacenter.to_string(),
            rack: rack.to_string(),
            host: String::new(),
        }
    }

    /// Populate all five levels. Empty string at a given level still means
    /// "inherit from enclosing".
    #[must_use]
    pub fn new_full(region: &str, zone: &str, datacenter: &str, rack: &str, host: &str) -> Self {
        Self {
            region: region.to_string(),
            zone: zone.to_string(),
            datacenter: datacenter.to_string(),
            rack: rack.to_string(),
            host: host.to_string(),
        }
    }

    /// Get the failure domain value at a specific level.
    #[must_use]
    pub fn at_level(&self, level: FailureDomain) -> &str {
        match level {
            FailureDomain::Disk | FailureDomain::Node => "", // finer than FailureDomainInfo
            FailureDomain::Host => &self.host,
            FailureDomain::Rack => &self.rack,
            FailureDomain::Datacenter => &self.datacenter,
            FailureDomain::Zone => &self.zone,
            FailureDomain::Region => &self.region,
        }
    }
}

/// Topological distance between two nodes, ranked so lower = closer.
///
/// Phase 2 read path sorts candidate shards by this value to collapse
/// cross-DC bandwidth. Ordering is total so callers can use it as a sort
/// key directly.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum TopologyDistance {
    SameHost = 0,
    SameRack = 1,
    SameDatacenter = 2,
    SameZone = 3,
    SameRegion = 4,
    Remote = 5,
    /// One or both sides lack enough topology info to rank. Treated as
    /// farther than a known remote so routing never accidentally prefers
    /// a partially-labeled peer over a known-close one.
    Unknown = 6,
}

impl TopologyDistance {
    /// Raw distance value — handy for metrics labels ("same-zone" / "remote").
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::SameHost => "same-host",
            Self::SameRack => "same-rack",
            Self::SameDatacenter => "same-datacenter",
            Self::SameZone => "same-zone",
            Self::SameRegion => "same-region",
            Self::Remote => "remote",
            Self::Unknown => "unknown",
        }
    }

    /// True iff the two peers are in the same datacenter or closer.
    /// Rough heuristic for "same broadcast domain" — used by repair-throttle
    /// (Phase 3) and by the UI to colorize locality.
    #[must_use]
    pub const fn is_local(self) -> bool {
        matches!(
            self,
            Self::SameHost | Self::SameRack | Self::SameDatacenter
        )
    }
}

/// Compute the topological distance between `a` and `b`.
///
/// Walks the hierarchy outside-in: peers must share every enclosing level
/// to be considered closer.
///
/// **Empty-field semantics** (both sides):
/// - Both empty at a level → treat as matching (both "inherit from
///   enclosing"), keep descending.
/// - One empty, other set → stop at the previous matched level; we can't
///   prove co-location we can't see.
/// - Both set, different → stop at the previous matched level.
#[must_use]
pub fn distance(a: &FailureDomainInfo, b: &FailureDomainInfo) -> TopologyDistance {
    // Region must match (both unknown counts as Unknown — we can't even
    // rule out opposite ends of the world).
    if a.region.is_empty() && b.region.is_empty() {
        return TopologyDistance::Unknown;
    }
    if a.region != b.region {
        return TopologyDistance::Remote;
    }
    if !field_match(&a.zone, &b.zone) {
        return TopologyDistance::SameRegion;
    }
    if !field_match(&a.datacenter, &b.datacenter) {
        return TopologyDistance::SameZone;
    }
    if !field_match(&a.rack, &b.rack) {
        return TopologyDistance::SameDatacenter;
    }
    if !field_match(&a.host, &b.host) {
        return TopologyDistance::SameRack;
    }
    // Can only claim SameHost when at least one side actually names a
    // host — otherwise we don't really know they're the same physical box.
    if a.host.is_empty() && b.host.is_empty() {
        TopologyDistance::SameRack
    } else {
        TopologyDistance::SameHost
    }
}

/// Two fields "match" when they're equal, or when both are empty (both
/// inherit from enclosing level).
fn field_match(a: &str, b: &str) -> bool {
    (a.is_empty() && b.is_empty()) || a == b
}

/// Disk information
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DiskInfo {
    /// Disk unique identifier
    pub id: DiskId,
    /// Path to the disk
    pub path: String,
    /// Total capacity in bytes
    pub total_capacity: u64,
    /// Used capacity in bytes
    pub used_capacity: u64,
    /// Disk status
    pub status: DiskStatus,
    /// Weight for placement
    pub weight: f64,
}

impl DiskInfo {
    /// Get available capacity
    #[must_use]
    pub fn available_capacity(&self) -> u64 {
        self.total_capacity.saturating_sub(self.used_capacity)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_topology_add_node() {
        let mut topology = ClusterTopology::new();

        let node = NodeInfo {
            id: NodeId::new(),
            name: "node1".to_string(),
            address: "127.0.0.1:9001".parse().unwrap(),
            failure_domain: FailureDomainInfo::new("us-east", "dc1", "rack1"),
            status: NodeStatus::Active,
            disks: vec![],
            weight: 1.0,
            last_heartbeat: 0,
        };

        topology.upsert_node(node.clone());

        assert_eq!(topology.version, 1);
        assert!(topology.get_node(node.id).is_some());
    }

    // ---- distance metric ---------------------------------------------------

    fn fd(region: &str, zone: &str, dc: &str, rack: &str, host: &str) -> FailureDomainInfo {
        FailureDomainInfo::new_full(region, zone, dc, rack, host)
    }

    #[test]
    fn distance_same_host() {
        let a = fd("us-east", "1a", "dc1", "rack1", "host-07");
        let b = fd("us-east", "1a", "dc1", "rack1", "host-07");
        assert_eq!(distance(&a, &b), TopologyDistance::SameHost);
    }

    #[test]
    fn distance_same_rack() {
        let a = fd("us-east", "1a", "dc1", "rack1", "host-07");
        let b = fd("us-east", "1a", "dc1", "rack1", "host-08");
        assert_eq!(distance(&a, &b), TopologyDistance::SameRack);
    }

    #[test]
    fn distance_same_datacenter() {
        let a = fd("us-east", "1a", "dc1", "rack1", "host-07");
        let b = fd("us-east", "1a", "dc1", "rack2", "host-01");
        assert_eq!(distance(&a, &b), TopologyDistance::SameDatacenter);
    }

    #[test]
    fn distance_same_zone() {
        let a = fd("us-east", "1a", "dc1", "rack1", "host-07");
        let b = fd("us-east", "1a", "dc2", "rack1", "host-07");
        assert_eq!(distance(&a, &b), TopologyDistance::SameZone);
    }

    #[test]
    fn distance_same_region() {
        let a = fd("us-east", "1a", "dc1", "rack1", "host-07");
        let b = fd("us-east", "1b", "dc1", "rack1", "host-07");
        assert_eq!(distance(&a, &b), TopologyDistance::SameRegion);
    }

    #[test]
    fn distance_remote() {
        let a = fd("us-east", "1a", "dc1", "rack1", "host-07");
        let b = fd("eu-west", "1a", "dc1", "rack1", "host-07");
        assert_eq!(distance(&a, &b), TopologyDistance::Remote);
    }

    #[test]
    fn distance_empty_region_is_unknown_only_when_both_empty() {
        // Both sides totally unlabeled: we truly can't rank — `Unknown`.
        let a = fd("", "", "", "", "");
        let b = fd("", "", "", "", "");
        assert_eq!(distance(&a, &b), TopologyDistance::Unknown);

        // One side unlabeled, the other in us-east: different region,
        // treat as Remote.
        let a = fd("", "", "", "", "");
        let b = fd("us-east", "1a", "dc1", "rack1", "host-07");
        assert_eq!(distance(&a, &b), TopologyDistance::Remote);
    }

    #[test]
    fn distance_empty_zone_on_both_descends() {
        // 3-level deployment (no zone/host). Both sides have matching
        // region+dc+rack; zone+host are empty on both. Expect SameRack,
        // not SameRegion — we don't penalize deployments that don't use
        // every level.
        let a = fd("local", "", "datacore", "rack1", "");
        let b = fd("local", "", "datacore", "rack1", "");
        assert_eq!(distance(&a, &b), TopologyDistance::SameRack);
    }

    #[test]
    fn distance_empty_host_capped_at_rack() {
        // Both in the same rack, one with empty host: can't claim SameHost.
        let a = fd("us-east", "1a", "dc1", "rack1", "host-07");
        let b = fd("us-east", "1a", "dc1", "rack1", "");
        assert_eq!(distance(&a, &b), TopologyDistance::SameRack);
    }

    #[test]
    fn distance_is_total_order_for_sort() {
        // A vector of distances round-trips through sort.
        let mut v = vec![
            TopologyDistance::Remote,
            TopologyDistance::SameRack,
            TopologyDistance::Unknown,
            TopologyDistance::SameHost,
            TopologyDistance::SameDatacenter,
        ];
        v.sort();
        assert_eq!(
            v,
            vec![
                TopologyDistance::SameHost,
                TopologyDistance::SameRack,
                TopologyDistance::SameDatacenter,
                TopologyDistance::Remote,
                TopologyDistance::Unknown,
            ]
        );
    }
}
