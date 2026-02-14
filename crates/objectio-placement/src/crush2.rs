//! CRUSH 2.0 - Enhanced placement algorithm
//!
//! Improvements over traditional CRUSH:
//! - Rendezvous/HRW (Highest Random Weight) hashing for node selection
//! - EC-aware placement templates
//! - Stripe group mapping for deterministic rack assignment
//! - SIMD/RDMA-friendly design
//!
//! # Algorithm Overview
//!
//! 1. **Object → Stripe Group**: `hash(object_id) % num_groups` determines participating racks
//! 2. **EC Placement Template**: Pre-defined shard layout (e.g., LRC groups)
//! 3. **HRW Hashing per Domain**: `score(node) = hash(object_id, node_id)`, pick top K
//! 4. **Optional Scoring**: For hot data, consider network distance and load

use crate::topology::{ClusterTopology, NodeInfo};
use objectio_common::{FailureDomain, NodeId, ObjectId};
use std::collections::HashMap;

/// Shard type for placement
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ShardRole {
    /// Data shard
    Data,
    /// Local parity (within group)
    LocalParity,
    /// Global parity (across groups)
    GlobalParity,
}

/// A single shard in the placement template
#[derive(Clone, Debug)]
pub struct TemplateShard {
    /// Position in the stripe (0-indexed)
    pub position: u8,
    /// Role of this shard
    pub role: ShardRole,
    /// Local group index (None for global parity)
    pub local_group: Option<u8>,
    /// Which domain slot this shard belongs to (for spreading)
    pub domain_slot: u8,
}

/// EC placement template - defines the layout for a specific EC configuration
#[derive(Clone, Debug)]
pub struct PlacementTemplate {
    /// Template name (e.g., "lrc_6_2_2")
    pub name: String,
    /// Number of data shards
    pub data_shards: u8,
    /// Number of local parity shards
    pub local_parity: u8,
    /// Number of global parity shards
    pub global_parity: u8,
    /// Shard definitions
    pub shards: Vec<TemplateShard>,
    /// Number of domain slots required
    pub domain_slots: u8,
    /// Minimum shards per domain slot
    pub shards_per_slot: u8,
}

impl PlacementTemplate {
    /// Create an MDS (Reed-Solomon) template
    ///
    /// All shards spread across different domains
    pub fn mds(k: u8, m: u8) -> Self {
        let total = k + m;
        let shards = (0..total)
            .map(|i| TemplateShard {
                position: i,
                role: if i < k {
                    ShardRole::Data
                } else {
                    ShardRole::GlobalParity
                },
                local_group: None,
                domain_slot: i, // Each shard in different domain
            })
            .collect();

        Self {
            name: format!("mds_{}_{}", k, m),
            data_shards: k,
            local_parity: 0,
            global_parity: m,
            shards,
            domain_slots: total,
            shards_per_slot: 1,
        }
    }

    /// Create an LRC template
    ///
    /// Example LRC(6,2,2):
    /// ```text
    /// Domain A (Group 0): D0 D1 D2 LP0
    /// Domain B (Group 1): D3 D4 D5 LP1
    /// Domain C (Global):  GP0 GP1
    /// ```
    pub fn lrc(k: u8, l: u8, g: u8) -> Self {
        let group_size = k / l;
        let mut shards = Vec::with_capacity((k + l + g) as usize);
        let mut position: u8 = 0;

        // Create local groups
        for group_idx in 0..l {
            // Data shards in this group
            for _ in 0..group_size {
                shards.push(TemplateShard {
                    position,
                    role: ShardRole::Data,
                    local_group: Some(group_idx),
                    domain_slot: group_idx,
                });
                position += 1;
            }
            // Local parity for this group
            shards.push(TemplateShard {
                position,
                role: ShardRole::LocalParity,
                local_group: Some(group_idx),
                domain_slot: group_idx,
            });
            position += 1;
        }

        // Global parity shards - in separate domain(s)
        let global_domain_start = l;
        for gp_idx in 0..g {
            shards.push(TemplateShard {
                position,
                role: ShardRole::GlobalParity,
                local_group: None,
                domain_slot: global_domain_start + (gp_idx / 2), // 2 global parity per domain
            });
            position += 1;
        }

        // Calculate domain slots needed
        let domain_slots = l + (g + 1).div_ceil(2); // local groups + global parity domains

        Self {
            name: format!("lrc_{}_{}_{}", k, l, g),
            data_shards: k,
            local_parity: l,
            global_parity: g,
            shards,
            domain_slots,
            shards_per_slot: group_size + 1, // data + local parity
        }
    }

    /// Total number of shards
    pub fn total_shards(&self) -> u8 {
        self.data_shards + self.local_parity + self.global_parity
    }
}

/// Stripe group - defines which domains participate for an object
#[derive(Clone, Debug)]
pub struct StripeGroup {
    /// Group ID
    pub id: u32,
    /// Domains participating in this group (ordered)
    pub domains: Vec<String>,
}

/// Result of HRW node selection
#[derive(Clone, Debug)]
pub struct HrwPlacement {
    /// Shard position
    pub position: u8,
    /// Selected node
    pub node_id: NodeId,
    /// Shard role
    pub role: ShardRole,
    /// Local group (if applicable)
    pub local_group: Option<u8>,
    /// HRW score (for debugging/logging)
    pub score: u64,
}

/// CRUSH 2.0 placement engine
pub struct Crush2 {
    topology: ClusterTopology,
    /// Pre-computed stripe groups (domains grouped by hash)
    stripe_groups: Vec<StripeGroup>,
    /// Number of stripe groups
    num_groups: u32,
}

impl Crush2 {
    /// Create a new CRUSH 2.0 engine
    pub fn new(topology: ClusterTopology, num_groups: u32) -> Self {
        let mut engine = Self {
            topology,
            stripe_groups: Vec::new(),
            num_groups,
        };
        engine.rebuild_stripe_groups();
        engine
    }

    /// Update topology and rebuild stripe groups
    pub fn update_topology(&mut self, topology: ClusterTopology) {
        self.topology = topology;
        self.rebuild_stripe_groups();
    }

    /// Rebuild stripe groups based on current topology
    fn rebuild_stripe_groups(&mut self) {
        // Group all nodes by their rack (or other domain level)
        let domains = self.group_nodes_by_domain(FailureDomain::Rack);
        let domain_keys: Vec<String> = domains.keys().cloned().collect();

        if domain_keys.is_empty() {
            self.stripe_groups = Vec::new();
            return;
        }

        // Create stripe groups by rotating through domains
        self.stripe_groups = (0..self.num_groups)
            .map(|group_id| {
                // Rotate domains based on group ID for even distribution
                let rotation = (group_id as usize) % domain_keys.len();
                let mut group_domains: Vec<String> = domain_keys.clone();
                group_domains.rotate_left(rotation);

                StripeGroup {
                    id: group_id,
                    domains: group_domains,
                }
            })
            .collect();
    }

    /// Select placement for an object using CRUSH 2.0 algorithm
    ///
    /// # Steps:
    /// 1. Map object to stripe group
    /// 2. Apply EC placement template
    /// 3. Use HRW hashing within each domain
    pub fn select_placement(
        &self,
        object_id: &ObjectId,
        template: &PlacementTemplate,
    ) -> Vec<HrwPlacement> {
        // Step 1: Object → stripe group
        let object_hash = self.hash_object(object_id);
        let group_idx = (object_hash as u32) % self.num_groups;
        let stripe_group = self
            .stripe_groups
            .get(group_idx as usize)
            .cloned()
            .unwrap_or_else(|| StripeGroup {
                id: group_idx,
                domains: Vec::new(),
            });

        // Group nodes by domain
        let domain_nodes = self.group_nodes_by_domain(FailureDomain::Rack);

        // Step 2 & 3: Apply template with HRW hashing per domain
        let mut placements = Vec::with_capacity(template.total_shards() as usize);

        // Track used nodes per domain to avoid duplicates
        let mut used_nodes: HashMap<u8, Vec<NodeId>> = HashMap::new();

        for shard in &template.shards {
            // Get the domain for this shard
            let domain_idx = shard.domain_slot as usize % stripe_group.domains.len().max(1);
            let domain_key = stripe_group
                .domains
                .get(domain_idx)
                .cloned()
                .unwrap_or_default();

            // Get nodes in this domain
            let nodes = domain_nodes.get(&domain_key).cloned().unwrap_or_default();

            // HRW selection: exclude already used nodes in this domain
            let used = used_nodes.entry(shard.domain_slot).or_default();
            let (node_id, score) = self.hrw_select(object_id, &nodes, used);

            used.push(node_id);

            placements.push(HrwPlacement {
                position: shard.position,
                node_id,
                role: shard.role,
                local_group: shard.local_group,
                score,
            });
        }

        placements
    }

    /// HRW (Highest Random Weight) / Rendezvous hashing
    ///
    /// `score(node) = hash(object_id, node_id) * weight`
    /// Pick the node with highest score, excluding already used nodes
    fn hrw_select(
        &self,
        object_id: &ObjectId,
        nodes: &[&NodeInfo],
        exclude: &[NodeId],
    ) -> (NodeId, u64) {
        if nodes.is_empty() {
            // Fallback: generate deterministic placeholder
            let hash = self.hash_object(object_id);
            let mut bytes = [0u8; 16];
            bytes[..8].copy_from_slice(&hash.to_le_bytes());
            return (NodeId::from_bytes(bytes), 0);
        }

        let object_hash = self.hash_object(object_id);

        // Calculate HRW score for each node
        let mut best_node = nodes[0];
        let mut best_score: u64 = 0;

        for node in nodes {
            // Skip excluded nodes
            if exclude.contains(&node.id) {
                continue;
            }

            // HRW score: hash(object_id || node_id) * weight
            let node_hash = xxhash_rust::xxh64::xxh64(node.id.as_bytes(), object_hash);

            // Apply weight (multiply by weight * 1000 for precision)
            let weight_factor = (node.weight * 1000.0) as u64;
            let score = node_hash.wrapping_mul(weight_factor);

            if score > best_score {
                best_score = score;
                best_node = node;
            }
        }

        (best_node.id, best_score)
    }

    /// Select multiple nodes using HRW (for simple replication/MDS)
    pub fn hrw_select_n(
        &self,
        object_id: &ObjectId,
        nodes: &[&NodeInfo],
        count: usize,
    ) -> Vec<(NodeId, u64)> {
        if nodes.is_empty() {
            return Vec::new();
        }

        let object_hash = self.hash_object(object_id);

        // Calculate scores for all nodes
        let mut scored: Vec<(NodeId, u64)> = nodes
            .iter()
            .map(|node| {
                let node_hash = xxhash_rust::xxh64::xxh64(node.id.as_bytes(), object_hash);
                let weight_factor = (node.weight * 1000.0) as u64;
                let score = node_hash.wrapping_mul(weight_factor);
                (node.id, score)
            })
            .collect();

        // Sort by score descending
        scored.sort_by(|a, b| b.1.cmp(&a.1));

        // Take top N
        scored.truncate(count);
        scored
    }

    /// Group active nodes by failure domain
    fn group_nodes_by_domain(&self, level: FailureDomain) -> HashMap<String, Vec<&NodeInfo>> {
        let mut groups: HashMap<String, Vec<&NodeInfo>> = HashMap::new();

        for node in self.topology.active_nodes() {
            let domain_key = self.get_domain_key(node, level);
            groups.entry(domain_key).or_default().push(node);
        }

        groups
    }

    /// Get domain key for a node
    fn get_domain_key(&self, node: &NodeInfo, level: FailureDomain) -> String {
        match level {
            FailureDomain::Disk => format!("{}:disk", node.id),
            FailureDomain::Node => node.id.to_string(),
            FailureDomain::Rack => format!(
                "{}:{}:{}",
                node.failure_domain.region,
                node.failure_domain.datacenter,
                node.failure_domain.rack
            ),
            FailureDomain::Datacenter => {
                format!(
                    "{}:{}",
                    node.failure_domain.region, node.failure_domain.datacenter
                )
            }
            FailureDomain::Region => node.failure_domain.region.clone(),
        }
    }

    /// Hash an object ID
    fn hash_object(&self, object_id: &ObjectId) -> u64 {
        xxhash_rust::xxh64::xxh64(object_id.as_bytes(), 0)
    }

    /// Get stripe group for an object (for debugging/inspection)
    pub fn get_stripe_group(&self, object_id: &ObjectId) -> Option<&StripeGroup> {
        let object_hash = self.hash_object(object_id);
        let group_idx = (object_hash as u32) % self.num_groups;
        self.stripe_groups.get(group_idx as usize)
    }
}

/// Common placement templates
pub mod templates {
    use super::PlacementTemplate;

    /// MDS 4+2 (standard Reed-Solomon)
    pub fn mds_4_2() -> PlacementTemplate {
        PlacementTemplate::mds(4, 2)
    }

    /// MDS 8+4
    pub fn mds_8_4() -> PlacementTemplate {
        PlacementTemplate::mds(8, 4)
    }

    /// LRC 6+2+2 (Azure-like, small)
    pub fn lrc_6_2_2() -> PlacementTemplate {
        PlacementTemplate::lrc(6, 2, 2)
    }

    /// LRC 12+2+2 (Azure-like, standard)
    pub fn lrc_12_2_2() -> PlacementTemplate {
        PlacementTemplate::lrc(12, 2, 2)
    }

    /// LRC 8+2+2
    pub fn lrc_8_2_2() -> PlacementTemplate {
        PlacementTemplate::lrc(8, 2, 2)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::topology::{ClusterTopology, FailureDomainInfo, NodeInfo};
    use objectio_common::NodeStatus;

    fn create_test_topology() -> ClusterTopology {
        let mut topology = ClusterTopology::new();

        // 3 racks, 4 nodes each
        for rack in 0..3 {
            for node in 0..4 {
                let info = NodeInfo {
                    id: NodeId::new(),
                    name: format!("node-r{}-n{}", rack, node),
                    address: format!("10.0.{}.{}:9002", rack, node).parse().unwrap(),
                    failure_domain: FailureDomainInfo::new(
                        "us-east",
                        "dc1",
                        &format!("rack{}", rack),
                    ),
                    status: NodeStatus::Active,
                    disks: vec![],
                    weight: 1.0,
                    last_heartbeat: 0,
                };
                topology.upsert_node(info);
            }
        }

        topology
    }

    #[test]
    fn test_hrw_deterministic() {
        let topology = create_test_topology();
        let crush = Crush2::new(topology, 64);
        let template = templates::mds_4_2();

        let object_id = ObjectId::new();

        // Same object should always get same placement
        let p1 = crush.select_placement(&object_id, &template);
        let p2 = crush.select_placement(&object_id, &template);

        assert_eq!(p1.len(), p2.len());
        for (a, b) in p1.iter().zip(p2.iter()) {
            assert_eq!(a.position, b.position);
            assert_eq!(a.node_id, b.node_id);
            assert_eq!(a.role, b.role);
        }
    }

    #[test]
    fn test_lrc_template() {
        let template = templates::lrc_6_2_2();

        assert_eq!(template.data_shards, 6);
        assert_eq!(template.local_parity, 2);
        assert_eq!(template.global_parity, 2);
        assert_eq!(template.total_shards(), 10);

        // Verify shard roles
        let data_count = template
            .shards
            .iter()
            .filter(|s| s.role == ShardRole::Data)
            .count();
        let lp_count = template
            .shards
            .iter()
            .filter(|s| s.role == ShardRole::LocalParity)
            .count();
        let gp_count = template
            .shards
            .iter()
            .filter(|s| s.role == ShardRole::GlobalParity)
            .count();

        assert_eq!(data_count, 6);
        assert_eq!(lp_count, 2);
        assert_eq!(gp_count, 2);

        // Verify local groups
        let group_0: Vec<_> = template
            .shards
            .iter()
            .filter(|s| s.local_group == Some(0))
            .collect();
        let group_1: Vec<_> = template
            .shards
            .iter()
            .filter(|s| s.local_group == Some(1))
            .collect();

        assert_eq!(group_0.len(), 4); // 3 data + 1 local parity
        assert_eq!(group_1.len(), 4);
    }

    #[test]
    fn test_lrc_placement() {
        let topology = create_test_topology();
        let crush = Crush2::new(topology, 64);
        let template = templates::lrc_6_2_2();

        let object_id = ObjectId::new();
        let placements = crush.select_placement(&object_id, &template);

        assert_eq!(placements.len(), 10);

        // Verify all positions are filled
        let positions: Vec<u8> = placements.iter().map(|p| p.position).collect();
        for i in 0..10 {
            assert!(positions.contains(&i), "Missing position {}", i);
        }
    }

    #[test]
    fn test_hrw_balance() {
        let topology = create_test_topology();
        let crush = Crush2::new(topology.clone(), 64);

        // Get all nodes
        let nodes: Vec<&NodeInfo> = topology.active_nodes().collect();

        // Count selections across many objects
        let mut selection_count: HashMap<NodeId, usize> = HashMap::new();
        for _i in 0..1000 {
            let object_id = ObjectId::new();
            let selected = crush.hrw_select_n(&object_id, &nodes, 1);
            if let Some((node_id, _)) = selected.first() {
                *selection_count.entry(*node_id).or_default() += 1;
            }
        }

        // With 12 nodes and 1000 objects, each should get ~83 objects
        // Allow 50% variance (42-125)
        for count in selection_count.values() {
            assert!(
                *count > 40 && *count < 130,
                "Unbalanced selection: {} (expected ~83)",
                count
            );
        }
    }
}
