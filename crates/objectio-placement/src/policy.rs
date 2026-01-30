//! Placement policy implementation

use crate::crush::{CrushMap, LrcPlacementConfig, ShardType};
use crate::topology::ClusterTopology;
use objectio_common::{
    DiskId, Error, FailureDomain, NodeId, ObjectId, ProtectionType, Result,
    StorageClass,
};
use parking_lot::RwLock;
use std::sync::Arc;

/// Result of a placement decision
#[derive(Clone, Debug)]
pub struct PlacementResult {
    /// Storage class used
    pub storage_class: String,
    /// Protection configuration
    pub protection: ProtectionType,
    /// Shard placements
    pub shards: Vec<ShardPlacement>,
}

/// Placement for a single shard
#[derive(Clone, Debug)]
pub struct ShardPlacement {
    /// Position in the stripe (0..k+m-1)
    pub position: u8,
    /// Primary node for this shard
    pub node_id: NodeId,
    /// Disk on the node (optional, can be selected by OSD)
    pub disk_id: Option<DiskId>,
    /// Shard type for LRC (Data, LocalParity, GlobalParity)
    pub shard_type: ShardType,
    /// Local group index for LRC (None for MDS or global parity)
    pub local_group: Option<u8>,
}

/// Placement policy for data distribution
pub struct PlacementPolicy {
    crush: Arc<RwLock<CrushMap>>,
    storage_classes: Vec<StorageClass>,
    default_storage_class: String,
}

impl PlacementPolicy {
    /// Create a new placement policy
    #[must_use]
    pub fn new(topology: ClusterTopology) -> Self {
        let default_class = StorageClass {
            name: "standard".to_string(),
            protection: ProtectionType::ErasureCoding {
                data_shards: 4,
                parity_shards: 2,
                placement: FailureDomain::Rack,
            },
            geo_replication: None,
        };

        Self {
            crush: Arc::new(RwLock::new(CrushMap::new(topology))),
            storage_classes: vec![default_class],
            default_storage_class: "standard".to_string(),
        }
    }

    /// Update the cluster topology
    pub fn update_topology(&self, topology: ClusterTopology) {
        self.crush.write().update_topology(topology);
    }

    /// Add a storage class
    pub fn add_storage_class(&mut self, class: StorageClass) {
        self.storage_classes.push(class);
    }

    /// Get a storage class by name
    pub fn get_storage_class(&self, name: &str) -> Option<&StorageClass> {
        self.storage_classes.iter().find(|c| c.name == name)
    }

    /// Compute placement for a new object
    pub fn place_object(
        &self,
        object_id: &ObjectId,
        storage_class: Option<&str>,
    ) -> Result<PlacementResult> {
        let class_name = storage_class.unwrap_or(&self.default_storage_class);
        let class = self
            .get_storage_class(class_name)
            .ok_or_else(|| Error::invalid_argument(format!("unknown storage class: {class_name}")))?;

        match &class.protection {
            ProtectionType::ErasureCoding {
                data_shards,
                parity_shards,
                placement,
            } => {
                let total_shards = *data_shards + *parity_shards;
                let nodes = self
                    .crush
                    .read()
                    .select_nodes(object_id, total_shards as usize, *placement);

                if nodes.len() < total_shards as usize {
                    return Err(Error::InsufficientNodes {
                        available: nodes.len(),
                        required: total_shards as usize,
                    });
                }

                // MDS: data shards first, then parity shards
                let shards = nodes
                    .into_iter()
                    .enumerate()
                    .map(|(i, node_id)| {
                        let shard_type = if (i as u8) < *data_shards {
                            ShardType::Data
                        } else {
                            ShardType::GlobalParity // MDS uses global parity for all parity shards
                        };
                        ShardPlacement {
                            position: i as u8,
                            node_id,
                            disk_id: None,
                            shard_type,
                            local_group: None, // MDS has no local groups
                        }
                    })
                    .collect();

                Ok(PlacementResult {
                    storage_class: class_name.to_string(),
                    protection: class.protection.clone(),
                    shards,
                })
            }
            ProtectionType::Lrc {
                data_shards,
                local_parity,
                global_parity,
                local_placement,
                group_placement,
            } => {
                let config = LrcPlacementConfig {
                    data_shards: *data_shards,
                    local_parity: *local_parity,
                    global_parity: *global_parity,
                    local_placement: *local_placement,
                    group_placement: *group_placement,
                };

                let lrc_placements = self.crush.read().select_nodes_lrc(object_id, &config);
                let total_shards = config.total_shards() as usize;

                if lrc_placements.len() < total_shards {
                    return Err(Error::InsufficientNodes {
                        available: lrc_placements.len(),
                        required: total_shards,
                    });
                }

                let shards = lrc_placements
                    .into_iter()
                    .map(|lrc| ShardPlacement {
                        position: lrc.position,
                        node_id: lrc.node_id,
                        disk_id: None,
                        shard_type: lrc.shard_type,
                        local_group: lrc.local_group,
                    })
                    .collect();

                Ok(PlacementResult {
                    storage_class: class_name.to_string(),
                    protection: class.protection.clone(),
                    shards,
                })
            }
            ProtectionType::Replication { replicas, placement } => {
                let nodes = self
                    .crush
                    .read()
                    .select_nodes(object_id, *replicas as usize, *placement);

                if nodes.len() < *replicas as usize {
                    return Err(Error::InsufficientNodes {
                        available: nodes.len(),
                        required: *replicas as usize,
                    });
                }

                let shards = nodes
                    .into_iter()
                    .enumerate()
                    .map(|(i, node_id)| ShardPlacement {
                        position: i as u8,
                        node_id,
                        disk_id: None,
                        shard_type: ShardType::Data, // Replicas are all data copies
                        local_group: None,
                    })
                    .collect();

                Ok(PlacementResult {
                    storage_class: class_name.to_string(),
                    protection: class.protection.clone(),
                    shards,
                })
            }
        }
    }

    /// Place a single shard (for repair operations)
    pub fn place_shard(
        &self,
        object_id: &ObjectId,
        position: u8,
        exclude_nodes: &[NodeId],
        failure_domain: FailureDomain,
        shard_type: ShardType,
        local_group: Option<u8>,
    ) -> Result<ShardPlacement> {
        // Get more candidates than needed to allow exclusion
        let candidates = self.crush.read().select_nodes(object_id, 10, failure_domain);

        for node_id in candidates {
            if !exclude_nodes.contains(&node_id) {
                return Ok(ShardPlacement {
                    position,
                    node_id,
                    disk_id: None,
                    shard_type,
                    local_group,
                });
            }
        }

        Err(Error::InsufficientNodes {
            available: 0,
            required: 1,
        })
    }

    /// Place a shard for LRC repair, preferring nodes in the same local group's domain
    pub fn place_lrc_shard(
        &self,
        object_id: &ObjectId,
        position: u8,
        local_group: Option<u8>,
        shard_type: ShardType,
        exclude_nodes: &[NodeId],
        local_placement: FailureDomain,
        group_placement: FailureDomain,
    ) -> Result<ShardPlacement> {
        // For local parity/data shards, prefer nodes in the same local group's domain
        // For global parity, use the group_placement domain
        let placement_domain = if local_group.is_some() {
            local_placement
        } else {
            group_placement
        };

        self.place_shard(
            object_id,
            position,
            exclude_nodes,
            placement_domain,
            shard_type,
            local_group,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::topology::{FailureDomainInfo, NodeInfo};
    use objectio_common::NodeStatus;

    fn create_test_topology() -> ClusterTopology {
        let mut topology = ClusterTopology::new();

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
    fn test_place_object() {
        let topology = create_test_topology();
        let policy = PlacementPolicy::new(topology);

        let object_id = ObjectId::new();
        let result = policy.place_object(&object_id, None).unwrap();

        assert_eq!(result.storage_class, "standard");
        assert_eq!(result.shards.len(), 6); // 4+2

        // Verify shard types: 4 data + 2 parity
        let data_count = result
            .shards
            .iter()
            .filter(|s| s.shard_type == ShardType::Data)
            .count();
        let parity_count = result
            .shards
            .iter()
            .filter(|s| s.shard_type == ShardType::GlobalParity)
            .count();
        assert_eq!(data_count, 4);
        assert_eq!(parity_count, 2);
    }

    #[test]
    fn test_place_object_lrc() {
        let topology = create_test_topology();
        let mut policy = PlacementPolicy::new(topology);

        // Add LRC storage class: 6 data, 2 local parity, 2 global parity
        // This creates 2 local groups of 3 data shards each
        let lrc_class = StorageClass {
            name: "lrc".to_string(),
            protection: ProtectionType::Lrc {
                data_shards: 6,
                local_parity: 2,
                global_parity: 2,
                local_placement: FailureDomain::Node,
                group_placement: FailureDomain::Rack,
            },
            geo_replication: None,
        };
        policy.add_storage_class(lrc_class);

        let object_id = ObjectId::new();
        let result = policy.place_object(&object_id, Some("lrc")).unwrap();

        assert_eq!(result.storage_class, "lrc");
        assert_eq!(result.shards.len(), 10); // 6 data + 2 local parity + 2 global parity

        // Count shard types
        let data_count = result
            .shards
            .iter()
            .filter(|s| s.shard_type == ShardType::Data)
            .count();
        let local_parity_count = result
            .shards
            .iter()
            .filter(|s| s.shard_type == ShardType::LocalParity)
            .count();
        let global_parity_count = result
            .shards
            .iter()
            .filter(|s| s.shard_type == ShardType::GlobalParity)
            .count();

        assert_eq!(data_count, 6, "should have 6 data shards");
        assert_eq!(local_parity_count, 2, "should have 2 local parity shards");
        assert_eq!(global_parity_count, 2, "should have 2 global parity shards");

        // Verify local groups
        let group_0_shards: Vec<_> = result
            .shards
            .iter()
            .filter(|s| s.local_group == Some(0))
            .collect();
        let group_1_shards: Vec<_> = result
            .shards
            .iter()
            .filter(|s| s.local_group == Some(1))
            .collect();
        let global_shards: Vec<_> = result
            .shards
            .iter()
            .filter(|s| s.local_group.is_none())
            .collect();

        // Group 0: 3 data + 1 local parity = 4 shards
        assert_eq!(group_0_shards.len(), 4, "group 0 should have 4 shards");
        // Group 1: 3 data + 1 local parity = 4 shards
        assert_eq!(group_1_shards.len(), 4, "group 1 should have 4 shards");
        // Global: 2 global parity = 2 shards
        assert_eq!(global_shards.len(), 2, "should have 2 global parity shards");
    }

    #[test]
    fn test_deterministic_lrc_placement() {
        let topology = create_test_topology();
        let mut policy = PlacementPolicy::new(topology);

        let lrc_class = StorageClass {
            name: "lrc".to_string(),
            protection: ProtectionType::Lrc {
                data_shards: 6,
                local_parity: 2,
                global_parity: 2,
                local_placement: FailureDomain::Node,
                group_placement: FailureDomain::Rack,
            },
            geo_replication: None,
        };
        policy.add_storage_class(lrc_class);

        let object_id = ObjectId::new();

        // Same object ID should produce same placement
        let result1 = policy.place_object(&object_id, Some("lrc")).unwrap();
        let result2 = policy.place_object(&object_id, Some("lrc")).unwrap();

        assert_eq!(result1.shards.len(), result2.shards.len());
        for (s1, s2) in result1.shards.iter().zip(result2.shards.iter()) {
            assert_eq!(s1.position, s2.position);
            assert_eq!(s1.node_id, s2.node_id);
            assert_eq!(s1.shard_type, s2.shard_type);
            assert_eq!(s1.local_group, s2.local_group);
        }
    }
}
