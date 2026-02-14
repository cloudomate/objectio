//! Erasure coding backend abstraction
//!
//! This module provides a trait-based abstraction for different erasure coding
//! implementations, allowing runtime or compile-time backend selection.
//!
//! # Backends
//!
//! - `rust_simd`: Pure Rust using reed-solomon-simd (default, portable)
//! - `isal`: Intel ISA-L via FFI (x86/x86_64 only, feature-gated)
//!
//! # Code Types
//!
//! - MDS (Maximum Distance Separable): Standard Reed-Solomon, optimal but requires
//!   reading k shards for any reconstruction
//! - LRC (Locally Repairable Codes): Has local parity groups allowing faster
//!   single-shard repairs by reading only the local group

pub mod factory;
pub mod rust_simd;

#[cfg(all(feature = "isal", any(target_arch = "x86", target_arch = "x86_64")))]
pub mod isal;

use crate::ErasureError;

/// Result type for backend operations
pub type BackendResult<T> = Result<T, ErasureError>;

/// Capabilities of an erasure coding backend
#[derive(Clone, Debug, Default)]
pub struct BackendCapabilities {
    /// Backend name for identification
    pub name: &'static str,
    /// Supports LRC (Locally Repairable Codes)
    pub supports_lrc: bool,
    /// Supports SIMD acceleration
    pub supports_simd: bool,
    /// Maximum data shards supported
    pub max_data_shards: usize,
    /// Maximum parity shards supported
    pub max_parity_shards: usize,
}

/// Core trait for erasure coding backends
///
/// This trait provides the fundamental operations for MDS (Maximum Distance
/// Separable) erasure coding, typically Reed-Solomon.
pub trait ErasureBackend: Send + Sync {
    /// Get backend capabilities
    fn capabilities(&self) -> BackendCapabilities;

    /// Get the number of data shards (k)
    fn data_shards(&self) -> usize;

    /// Get the number of parity shards (m)
    fn parity_shards(&self) -> usize;

    /// Get total number of shards (k + m)
    fn total_shards(&self) -> usize {
        self.data_shards() + self.parity_shards()
    }

    /// Get minimum shards required for reconstruction (k)
    fn min_shards_for_decode(&self) -> usize {
        self.data_shards()
    }

    /// Encode data shards into parity shards
    ///
    /// Takes `k` data shards of equal size and produces `m` parity shards.
    /// Returns all shards (k data + m parity) in order.
    ///
    /// # Arguments
    /// * `data_shards` - Slice of k data shard references, all same size
    /// * `shard_size` - Size of each shard in bytes
    ///
    /// # Returns
    /// Vector of k+m shards (data shards copied, parity shards computed)
    fn encode(&self, data_shards: &[&[u8]], shard_size: usize) -> BackendResult<Vec<Vec<u8>>>;

    /// Decode/reconstruct missing shards
    ///
    /// Given available shards (some may be missing), reconstruct the specified
    /// missing shards. Requires at least k shards to be present.
    ///
    /// # Arguments
    /// * `shards` - Slice of k+m optional shards, `None` for missing
    /// * `shard_size` - Size of each shard in bytes
    /// * `missing_indices` - Indices of shards to reconstruct
    ///
    /// # Returns
    /// Vector of reconstructed shards in the order specified by `missing_indices`
    fn decode(
        &self,
        shards: &[Option<&[u8]>],
        shard_size: usize,
        missing_indices: &[usize],
    ) -> BackendResult<Vec<Vec<u8>>>;

    /// Verify that parity shards are consistent with data shards
    ///
    /// Re-encodes the data shards and compares with provided parity shards.
    fn verify(&self, shards: &[&[u8]]) -> BackendResult<bool>;
}

/// LRC (Locally Repairable Codes) configuration
///
/// LRC extends standard Reed-Solomon by adding local parity groups.
/// This reduces repair bandwidth for single-shard failures.
///
/// Example: LRC (12, 2, 2) has:
/// - 12 data shards split into 2 groups of 6
/// - 2 local parity shards (one per group, XOR of group)
/// - 2 global parity shards (Reed-Solomon over all data)
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LrcConfig {
    /// Number of data shards (k)
    pub data_shards: u8,
    /// Number of local parity shards (l) - one per group
    pub local_parity_shards: u8,
    /// Number of global parity shards (g)
    pub global_parity_shards: u8,
    /// Size of each local group (data shards per group = k / l)
    pub local_group_size: u8,
}

impl LrcConfig {
    /// Create a new LRC configuration
    ///
    /// # Arguments
    /// * `data_shards` - Number of data shards (k), must be divisible by `local_parity_shards`
    /// * `local_parity_shards` - Number of local groups/parities (l)
    /// * `global_parity_shards` - Number of global parity shards (g)
    ///
    /// # Panics
    /// Panics if `data_shards` is not divisible by `local_parity_shards`
    pub const fn new(data_shards: u8, local_parity_shards: u8, global_parity_shards: u8) -> Self {
        let local_group_size = data_shards / local_parity_shards;
        Self {
            data_shards,
            local_parity_shards,
            global_parity_shards,
            local_group_size,
        }
    }

    /// Total shards: k + l + g
    pub const fn total_shards(&self) -> u8 {
        self.data_shards + self.local_parity_shards + self.global_parity_shards
    }

    /// Total parity shards: l + g
    pub const fn total_parity(&self) -> u8 {
        self.local_parity_shards + self.global_parity_shards
    }

    /// Azure LRC (12, 2, 2) - 12 data, 2 local parity, 2 global parity
    pub const LRC_12_2_2: Self = Self::new(12, 2, 2);

    /// Smaller LRC (6, 2, 2) - 6 data, 2 local parity, 2 global parity
    pub const LRC_6_2_2: Self = Self::new(6, 2, 2);

    /// Compact LRC (8, 2, 2) - 8 data, 2 local parity, 2 global parity
    pub const LRC_8_2_2: Self = Self::new(8, 2, 2);
}

impl Default for LrcConfig {
    fn default() -> Self {
        Self::LRC_6_2_2
    }
}

/// Result of LRC encoding
#[derive(Clone, Debug)]
pub struct LrcEncodedData {
    /// Data shards (indices 0..k-1)
    pub data_shards: Vec<Vec<u8>>,
    /// Local parity shards (indices k..k+l-1)
    pub local_parity_shards: Vec<Vec<u8>>,
    /// Global parity shards (indices k+l..k+l+g-1)
    pub global_parity_shards: Vec<Vec<u8>>,
}

impl LrcEncodedData {
    /// Get all shards in order: data, local parity, global parity
    pub fn all_shards(&self) -> Vec<Vec<u8>> {
        let mut all = Vec::with_capacity(
            self.data_shards.len()
                + self.local_parity_shards.len()
                + self.global_parity_shards.len(),
        );
        all.extend(self.data_shards.iter().cloned());
        all.extend(self.local_parity_shards.iter().cloned());
        all.extend(self.global_parity_shards.iter().cloned());
        all
    }
}

/// Information about a local parity group
#[derive(Clone, Debug)]
pub struct LocalGroup {
    /// Group index (0..l-1)
    pub group_index: u8,
    /// Indices of data shards in this group
    pub data_shard_indices: Vec<usize>,
    /// Index of the local parity shard for this group
    pub local_parity_index: usize,
}

/// Extended trait for LRC-capable backends
///
/// LRC (Locally Repairable Codes) provides faster single-shard recovery
/// by using local parity groups. A missing shard in a group can be
/// recovered by XORing the other shards in that group with the local parity.
pub trait LrcBackend: ErasureBackend {
    /// Get the LRC configuration
    fn lrc_config(&self) -> &LrcConfig;

    /// Encode with local parity groups
    ///
    /// Produces data shards, local parity shards, and global parity shards.
    fn encode_lrc(&self, data_shards: &[&[u8]], shard_size: usize)
    -> BackendResult<LrcEncodedData>;

    /// Decode using local parity group (fast path for single-shard recovery)
    ///
    /// If the missing shard can be recovered using only its local group,
    /// this method is more efficient than full reconstruction.
    ///
    /// # Returns
    /// - `Ok(Some(data))` if local recovery succeeded
    /// - `Ok(None)` if local recovery is not possible (need global)
    /// - `Err(_)` on error
    fn decode_local(
        &self,
        shards: &[Option<&[u8]>],
        shard_size: usize,
        missing_index: usize,
    ) -> BackendResult<Option<Vec<u8>>>;

    /// Get the local group for a given shard index
    ///
    /// Returns `None` for global parity shards (they have no local group)
    fn local_group(&self, shard_index: usize) -> Option<LocalGroup>;

    /// Check if a shard can be recovered using local parity only
    ///
    /// Returns true if all other shards in the local group (including
    /// the local parity) are available.
    fn can_recover_locally(&self, available: &[bool], missing_index: usize) -> bool;
}

// Re-exports
pub use factory::{BackendConfig, BackendFactory, BackendType};
pub use rust_simd::{RustSimdBackend, RustSimdLrcBackend};
