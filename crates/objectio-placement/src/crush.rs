//! CRUSH-like placement algorithm
//!
//! Supports both MDS (Maximum Distance Separable) and LRC (Locally Repairable Codes)
//! placement strategies with failure domain awareness.

use crate::topology::{ClusterTopology, NodeInfo};
use objectio_common::{FailureDomain, NodeId, ObjectId};
use std::collections::{HashMap, HashSet};

/// Shard type for LRC placement
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ShardType {
    /// Data shard
    Data,
    /// Local parity shard (for local group recovery)
    LocalParity,
    /// Global parity shard (for cross-group recovery)
    GlobalParity,
}

/// LRC placement result for a single shard
#[derive(Clone, Debug)]
pub struct LrcShardPlacement {
    /// Shard position (0..total_shards-1)
    pub position: u8,
    /// Node to place this shard
    pub node_id: NodeId,
    /// Type of shard
    pub shard_type: ShardType,
    /// Local group index (0..num_groups-1 for data/local parity, None for global parity)
    pub local_group: Option<u8>,
}

/// LRC configuration for placement
#[derive(Clone, Debug)]
pub struct LrcPlacementConfig {
    /// Number of data shards (k)
    pub data_shards: u8,
    /// Number of local parity shards (l) - one per group
    pub local_parity: u8,
    /// Number of global parity shards (g)
    pub global_parity: u8,
    /// Failure domain for local group placement (shards in same group stay here)
    pub local_placement: FailureDomain,
    /// Failure domain for spreading groups apart
    pub group_placement: FailureDomain,
}

impl LrcPlacementConfig {
    /// Number of local groups
    pub fn num_groups(&self) -> u8 {
        self.local_parity
    }

    /// Size of each local group (data shards per group)
    pub fn group_size(&self) -> u8 {
        self.data_shards / self.local_parity
    }

    /// Total number of shards
    pub fn total_shards(&self) -> u8 {
        self.data_shards + self.local_parity + self.global_parity
    }
}

/// CRUSH map for data placement
pub struct CrushMap {
    topology: ClusterTopology,
}

impl CrushMap {
    /// Create a new CRUSH map from cluster topology
    #[must_use]
    pub fn new(topology: ClusterTopology) -> Self {
        Self { topology }
    }

    /// Update the topology
    pub fn update_topology(&mut self, topology: ClusterTopology) {
        self.topology = topology;
    }

    /// Select nodes for shard placement
    ///
    /// Returns `count` node IDs, ensuring that shards are distributed
    /// across the specified failure domain level.
    pub fn select_nodes(
        &self,
        object_id: &ObjectId,
        count: usize,
        failure_domain: FailureDomain,
    ) -> Vec<NodeId> {
        let hash = self.hash_object(object_id);
        let mut selected = Vec::with_capacity(count);
        let mut used_domains: HashSet<String> = HashSet::new();

        // Get all active nodes sorted by their weighted hash
        let mut candidates: Vec<(&NodeInfo, u64)> = self
            .topology
            .active_nodes()
            .map(|node| {
                let node_hash = self.weighted_hash(hash, node);
                (node, node_hash)
            })
            .collect();

        // Sort by hash (deterministic ordering)
        candidates.sort_by_key(|(_, h)| *h);

        // First pass: select nodes from different failure domains
        for (node, _) in &candidates {
            if selected.len() >= count {
                break;
            }

            let domain_key = self.get_domain_key(node, failure_domain);

            if !used_domains.contains(&domain_key) {
                selected.push(node.id);
                used_domains.insert(domain_key);
            }
        }

        // Second pass: fill remaining slots if we couldn't get enough diversity
        if selected.len() < count {
            for (node, _) in &candidates {
                if selected.len() >= count {
                    break;
                }

                if !selected.contains(&node.id) {
                    selected.push(node.id);
                }
            }
        }

        selected
    }

    /// Select nodes for LRC placement with local group awareness
    ///
    /// Places local groups (data shards + local parity) within the same failure domain
    /// for fast local repair, while spreading groups across different failure domains
    /// for fault tolerance. Global parity is placed in a separate domain.
    ///
    /// # Layout Example (LRC 12+2+2 with rack placement)
    /// ```text
    /// Rack A (Group 0): D0 D1 D2 D3 D4 D5 LP0
    /// Rack B (Group 1): D6 D7 D8 D9 D10 D11 LP1
    /// Rack C (Global):  GP0 GP1
    /// ```
    pub fn select_nodes_lrc(
        &self,
        object_id: &ObjectId,
        config: &LrcPlacementConfig,
    ) -> Vec<LrcShardPlacement> {
        let base_hash = self.hash_object(object_id);
        let num_groups = config.num_groups() as usize;
        let group_size = config.group_size() as usize;
        let mut placements = Vec::with_capacity(config.total_shards() as usize);

        // Group nodes by their failure domain (for group_placement level)
        let domain_nodes = self.group_nodes_by_domain(config.group_placement);

        // Get list of available domains
        let mut domain_keys: Vec<&String> = domain_nodes.keys().collect();
        // Sort deterministically based on hash
        domain_keys.sort_by_key(|k| {
            let domain_hash = xxhash_rust::xxh64::xxh64(k.as_bytes(), base_hash);
            domain_hash
        });

        let mut position: u8 = 0;
        let empty_string = String::new();

        // Place each local group
        for group_idx in 0..num_groups {
            // Select domain for this group (round-robin with hash rotation)
            let domain_idx = group_idx % domain_keys.len();
            let domain_key = domain_keys.get(domain_idx).cloned().unwrap_or(&empty_string);

            // Get nodes in this domain
            let group_nodes = domain_nodes.get(domain_key.as_str()).cloned().unwrap_or_default();

            // Sort nodes within domain by hash for deterministic selection
            let mut sorted_nodes: Vec<(&NodeInfo, u64)> = group_nodes
                .iter()
                .map(|n| {
                    let node_hash = self.weighted_hash(base_hash.wrapping_add(group_idx as u64), n);
                    (*n, node_hash)
                })
                .collect();
            sorted_nodes.sort_by_key(|(_, h)| *h);

            // Place data shards for this group
            for shard_in_group in 0..group_size {
                let node_idx = shard_in_group % sorted_nodes.len().max(1);
                let node_id = sorted_nodes
                    .get(node_idx)
                    .map(|(n, _)| n.id)
                    .unwrap_or_else(|| self.fallback_node(base_hash));

                placements.push(LrcShardPlacement {
                    position,
                    node_id,
                    shard_type: ShardType::Data,
                    local_group: Some(group_idx as u8),
                });
                position += 1;
            }

            // Place local parity for this group (same domain as data)
            let lp_node_idx = group_size % sorted_nodes.len().max(1);
            let lp_node_id = sorted_nodes
                .get(lp_node_idx)
                .map(|(n, _)| n.id)
                .unwrap_or_else(|| self.fallback_node(base_hash));

            placements.push(LrcShardPlacement {
                position,
                node_id: lp_node_id,
                shard_type: ShardType::LocalParity,
                local_group: Some(group_idx as u8),
            });
            position += 1;
        }

        // Place global parity in a different domain (for better fault tolerance)
        let global_domain_idx = num_groups % domain_keys.len();
        let global_domain_key = domain_keys
            .get(global_domain_idx)
            .cloned()
            .unwrap_or(&empty_string);
        let global_nodes = domain_nodes
            .get(global_domain_key.as_str())
            .cloned()
            .unwrap_or_default();

        let mut sorted_global: Vec<(&NodeInfo, u64)> = global_nodes
            .iter()
            .map(|n| {
                let node_hash = self.weighted_hash(base_hash.wrapping_add(1000), n);
                (*n, node_hash)
            })
            .collect();
        sorted_global.sort_by_key(|(_, h)| *h);

        for gp_idx in 0..config.global_parity {
            let node_idx = gp_idx as usize % sorted_global.len().max(1);
            let node_id = sorted_global
                .get(node_idx)
                .map(|(n, _)| n.id)
                .unwrap_or_else(|| self.fallback_node(base_hash));

            placements.push(LrcShardPlacement {
                position,
                node_id,
                shard_type: ShardType::GlobalParity,
                local_group: None,
            });
            position += 1;
        }

        placements
    }

    /// Group active nodes by their failure domain
    fn group_nodes_by_domain(&self, level: FailureDomain) -> HashMap<String, Vec<&NodeInfo>> {
        let mut groups: HashMap<String, Vec<&NodeInfo>> = HashMap::new();

        for node in self.topology.active_nodes() {
            let domain_key = self.get_domain_key(node, level);
            groups.entry(domain_key).or_default().push(node);
        }

        groups
    }

    /// Get a fallback node when no nodes are available in a domain
    fn fallback_node(&self, hash: u64) -> NodeId {
        self.topology
            .active_nodes()
            .next()
            .map(|n| n.id)
            .unwrap_or_else(|| {
                // Generate a deterministic placeholder ID if no nodes available
                let mut bytes = [0u8; 16];
                bytes[..8].copy_from_slice(&hash.to_le_bytes());
                NodeId::from_bytes(bytes)
            })
    }

    /// Hash an object ID to get a placement seed
    fn hash_object(&self, object_id: &ObjectId) -> u64 {
        xxhash_rust::xxh64::xxh64(object_id.as_bytes(), 0)
    }

    /// Compute weighted hash for a node
    fn weighted_hash(&self, base_hash: u64, node: &NodeInfo) -> u64 {
        // Combine base hash with node ID for deterministic but distributed placement
        let node_hash = xxhash_rust::xxh64::xxh64(node.id.as_bytes(), 0);

        // Weight affects the hash to give higher-weight nodes more data
        let weight_factor = (node.weight * 1000.0) as u64;

        base_hash.wrapping_mul(node_hash).wrapping_add(weight_factor)
    }

    /// Get the domain key for a node at the specified failure domain level
    fn get_domain_key(&self, node: &NodeInfo, level: FailureDomain) -> String {
        match level {
            FailureDomain::Disk => format!("{}:{}", node.id, "disk"), // Each disk is unique
            FailureDomain::Node => node.id.to_string(),
            FailureDomain::Rack => {
                format!(
                    "{}:{}:{}",
                    node.failure_domain.region,
                    node.failure_domain.datacenter,
                    node.failure_domain.rack
                )
            }
            FailureDomain::Datacenter => {
                format!(
                    "{}:{}",
                    node.failure_domain.region, node.failure_domain.datacenter
                )
            }
            FailureDomain::Region => node.failure_domain.region.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::topology::{FailureDomainInfo, NodeInfo};
    use objectio_common::NodeStatus;

    fn create_test_topology() -> ClusterTopology {
        let mut topology = ClusterTopology::new();

        // Add nodes in different racks
        for rack_num in 1..=3 {
            for node_num in 1..=2 {
                let node = NodeInfo {
                    id: NodeId::new(),
                    name: format!("node-{rack_num}-{node_num}"),
                    address: format!("127.0.0.{rack_num}:900{node_num}").parse().unwrap(),
                    failure_domain: FailureDomainInfo::new("us-east", "dc1", &format!("rack{rack_num}")),
                    status: NodeStatus::Active,
                    disks: vec![],
                    weight: 1.0,
                    last_heartbeat: 0,
                };
                topology.upsert_node(node);
            }
        }

        topology
    }

    #[test]
    fn test_select_nodes_rack_diversity() {
        let topology = create_test_topology();
        let crush = CrushMap::new(topology);

        let object_id = ObjectId::new();
        let nodes = crush.select_nodes(&object_id, 3, FailureDomain::Rack);

        assert_eq!(nodes.len(), 3);
        // All nodes should be unique
        let unique: HashSet<_> = nodes.iter().collect();
        assert_eq!(unique.len(), 3);
    }

    #[test]
    fn test_deterministic_placement() {
        let topology = create_test_topology();
        let crush = CrushMap::new(topology);

        let object_id = ObjectId::new();

        // Same object ID should always return same placement
        let nodes1 = crush.select_nodes(&object_id, 3, FailureDomain::Rack);
        let nodes2 = crush.select_nodes(&object_id, 3, FailureDomain::Rack);

        assert_eq!(nodes1, nodes2);
    }
}
