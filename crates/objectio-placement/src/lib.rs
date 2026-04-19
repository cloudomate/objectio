//! ObjectIO Placement - CRUSH-like data placement algorithm
//!
//! This crate implements data placement algorithms for deterministic
//! shard distribution with failure domain awareness. Supports both MDS
//! (Maximum Distance Separable) and LRC (Locally Repairable Codes)
//! placement strategies.
//!
//! # Algorithms
//!
//! ## CRUSH (Original)
//! Traditional CRUSH algorithm with straw bucket selection.
//!
//! ## CRUSH 2.0 (Recommended)
//! Enhanced placement using:
//! - **HRW (Rendezvous) hashing**: Better balance, minimal remapping
//! - **EC placement templates**: Pre-defined shard layouts
//! - **Stripe groups**: Deterministic domain assignment
//!
//! # Example
//! ```ignore
//! use objectio_placement::{Crush2, templates};
//!
//! let crush = Crush2::new(topology, 64);
//! let template = templates::lrc_6_2_2();
//! let placements = crush.select_placement(&object_id, &template);
//! ```

pub mod copyset;
pub mod crush;
pub mod crush2;
pub mod jump_hash;
pub mod policy;
pub mod topology;

// Original CRUSH exports
pub use crush::{CrushMap, LrcPlacementConfig, LrcShardPlacement, ShardType};
pub use policy::{PlacementPolicy, PlacementResult, ShardPlacement};
pub use topology::{
    ClusterTopology, FailureDomainInfo, NodeInfo, RackInfo, TopologyDistance, distance,
};

// CRUSH 2.0 exports
pub use crush2::templates;
pub use crush2::{
    Crush2, HrwPlacement, PlacementFeasibility, PlacementFeasibilityReport, PlacementTemplate,
    ShardRole, StripeGroup,
};

// Placement-group engine (Phase 2+). The jump-consistent-hash step
// routes `object_id → pg_id` in constant memory; the copyset pool
// provides the set of valid `(k+m)`-tuples the balancer assigns PGs
// from, respecting failure domains. CRUSH2 stays available as the
// fallback for pools with `pg_count = 0`.
pub use copyset::{Copyset, CopysetError, CopysetPool};
pub use jump_hash::jump_consistent_hash;
