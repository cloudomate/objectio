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

/// Failure domain information for a node
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FailureDomainInfo {
    /// Region name
    pub region: String,
    /// Datacenter name
    pub datacenter: String,
    /// Rack name
    pub rack: String,
}

impl FailureDomainInfo {
    /// Create a new failure domain info
    #[must_use]
    pub fn new(region: &str, datacenter: &str, rack: &str) -> Self {
        Self {
            region: region.to_string(),
            datacenter: datacenter.to_string(),
            rack: rack.to_string(),
        }
    }

    /// Get the failure domain at a specific level
    #[must_use]
    pub fn at_level(&self, level: FailureDomain) -> &str {
        match level {
            FailureDomain::Disk | FailureDomain::Node => "", // Node-specific, not in domain info
            FailureDomain::Rack => &self.rack,
            FailureDomain::Datacenter => &self.datacenter,
            FailureDomain::Region => &self.region,
        }
    }
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
}
