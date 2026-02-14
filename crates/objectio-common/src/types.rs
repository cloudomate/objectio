//! Core type definitions for ObjectIO
//!
//! This module defines the fundamental types used throughout the system
//! including identifiers, metadata structures, and configuration types.

use derive_more::{Display, From, Into};
use serde::{Deserialize, Serialize};
use std::fmt;
use uuid::Uuid;

/// Unique identifier for an object
#[derive(Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, From, Into)]
pub struct ObjectId(Uuid);

impl ObjectId {
    /// Generate a new random object ID
    #[must_use]
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    /// Create from existing UUID
    #[must_use]
    pub const fn from_uuid(uuid: Uuid) -> Self {
        Self(uuid)
    }

    /// Get the underlying UUID
    #[must_use]
    pub const fn as_uuid(&self) -> Uuid {
        self.0
    }

    /// Get as bytes
    #[must_use]
    pub fn as_bytes(&self) -> &[u8; 16] {
        self.0.as_bytes()
    }
}

impl Default for ObjectId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for ObjectId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ObjectId({})", self.0)
    }
}

impl fmt::Display for ObjectId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Unique identifier for a bucket
#[derive(Clone, PartialEq, Eq, Hash, Serialize, Deserialize, Display)]
#[display("{_0}")]
pub struct BucketName(String);

impl BucketName {
    /// Create a new bucket name (validates S3 naming rules)
    pub fn new(name: impl Into<String>) -> Result<Self, BucketNameError> {
        let name = name.into();
        Self::validate(&name)?;
        Ok(Self(name))
    }

    /// Create without validation (internal use only)
    #[must_use]
    pub fn new_unchecked(name: impl Into<String>) -> Self {
        Self(name.into())
    }

    /// Get the bucket name as a string slice
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Validate bucket name according to S3 rules
    fn validate(name: &str) -> Result<(), BucketNameError> {
        // Length check: 3-63 characters
        if name.len() < 3 {
            return Err(BucketNameError::TooShort);
        }
        if name.len() > 63 {
            return Err(BucketNameError::TooLong);
        }

        // Must start with lowercase letter or number
        let first = name.chars().next().unwrap();
        if !first.is_ascii_lowercase() && !first.is_ascii_digit() {
            return Err(BucketNameError::InvalidStartChar);
        }

        // Must end with lowercase letter or number
        let last = name.chars().last().unwrap();
        if !last.is_ascii_lowercase() && !last.is_ascii_digit() {
            return Err(BucketNameError::InvalidEndChar);
        }

        // Only lowercase letters, numbers, hyphens, and periods allowed
        for c in name.chars() {
            if !c.is_ascii_lowercase() && !c.is_ascii_digit() && c != '-' && c != '.' {
                return Err(BucketNameError::InvalidChar(c));
            }
        }

        // No consecutive periods
        if name.contains("..") {
            return Err(BucketNameError::ConsecutivePeriods);
        }

        // Cannot be formatted as IP address
        if name.parse::<std::net::Ipv4Addr>().is_ok() {
            return Err(BucketNameError::LooksLikeIpAddress);
        }

        Ok(())
    }
}

impl fmt::Debug for BucketName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "BucketName({:?})", self.0)
    }
}

/// Errors that can occur when creating a bucket name
#[derive(Debug, Clone, thiserror::Error)]
pub enum BucketNameError {
    #[error("bucket name must be at least 3 characters")]
    TooShort,
    #[error("bucket name must be at most 63 characters")]
    TooLong,
    #[error("bucket name must start with a lowercase letter or number")]
    InvalidStartChar,
    #[error("bucket name must end with a lowercase letter or number")]
    InvalidEndChar,
    #[error("bucket name contains invalid character: {0}")]
    InvalidChar(char),
    #[error("bucket name cannot contain consecutive periods")]
    ConsecutivePeriods,
    #[error("bucket name cannot be formatted as an IP address")]
    LooksLikeIpAddress,
}

/// Object key (path within a bucket)
#[derive(Clone, PartialEq, Eq, Hash, Serialize, Deserialize, Display)]
#[display("{_0}")]
pub struct ObjectKey(String);

impl ObjectKey {
    /// Create a new object key
    pub fn new(key: impl Into<String>) -> Result<Self, ObjectKeyError> {
        let key = key.into();
        Self::validate(&key)?;
        Ok(Self(key))
    }

    /// Create without validation (internal use only)
    #[must_use]
    pub fn new_unchecked(key: impl Into<String>) -> Self {
        Self(key.into())
    }

    /// Get the object key as a string slice
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Validate object key
    fn validate(key: &str) -> Result<(), ObjectKeyError> {
        // Maximum length: 1024 bytes (UTF-8)
        if key.len() > 1024 {
            return Err(ObjectKeyError::TooLong);
        }

        // Cannot be empty
        if key.is_empty() {
            return Err(ObjectKeyError::Empty);
        }

        Ok(())
    }
}

impl fmt::Debug for ObjectKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ObjectKey({:?})", self.0)
    }
}

/// Errors that can occur when creating an object key
#[derive(Debug, Clone, thiserror::Error)]
pub enum ObjectKeyError {
    #[error("object key cannot be empty")]
    Empty,
    #[error("object key cannot exceed 1024 bytes")]
    TooLong,
}

/// Unique identifier for a storage node
#[derive(Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, From, Into)]
pub struct NodeId(Uuid);

impl NodeId {
    /// Generate a new random node ID
    #[must_use]
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    /// Create from existing UUID
    #[must_use]
    pub const fn from_uuid(uuid: Uuid) -> Self {
        Self(uuid)
    }

    /// Create from bytes
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(Uuid::from_bytes(bytes))
    }

    /// Get as bytes
    #[must_use]
    pub fn as_bytes(&self) -> &[u8; 16] {
        self.0.as_bytes()
    }
}

impl Default for NodeId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "NodeId({})", self.0)
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Unique identifier for a disk
#[derive(Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, From, Into)]
pub struct DiskId(Uuid);

impl DiskId {
    /// Generate a new random disk ID
    #[must_use]
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    /// Create from existing UUID
    #[must_use]
    pub const fn from_uuid(uuid: Uuid) -> Self {
        Self(uuid)
    }

    /// Get as bytes
    #[must_use]
    pub fn as_bytes(&self) -> &[u8; 16] {
        self.0.as_bytes()
    }

    /// Create from bytes
    #[must_use]
    pub fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(Uuid::from_bytes(bytes))
    }
}

impl Default for DiskId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for DiskId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DiskId({})", self.0)
    }
}

impl fmt::Display for DiskId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Unique identifier for a shard (part of an erasure-coded object)
#[derive(Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ShardId {
    /// The object this shard belongs to
    pub object_id: ObjectId,
    /// Stripe number within the object
    pub stripe_id: u64,
    /// Position within the stripe (0..k-1 for data, k..k+m-1 for parity)
    pub position: u8,
}

impl ShardId {
    /// Create a new shard ID
    #[must_use]
    pub const fn new(object_id: ObjectId, stripe_id: u64, position: u8) -> Self {
        Self {
            object_id,
            stripe_id,
            position,
        }
    }
}

impl fmt::Debug for ShardId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ShardId({}.{}.{})",
            self.object_id, self.stripe_id, self.position
        )
    }
}

impl fmt::Display for ShardId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}.{}", self.object_id, self.stripe_id, self.position)
    }
}

/// Unique identifier for a block on disk
#[derive(Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, From, Into)]
pub struct BlockId(Uuid);

impl BlockId {
    /// Generate a new random block ID
    #[must_use]
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    /// Create from existing UUID
    #[must_use]
    pub const fn from_uuid(uuid: Uuid) -> Self {
        Self(uuid)
    }

    /// Get as bytes
    #[must_use]
    pub fn as_bytes(&self) -> &[u8; 16] {
        self.0.as_bytes()
    }
}

impl Default for BlockId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for BlockId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "BlockId({})", self.0)
    }
}

impl fmt::Display for BlockId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Location of a block on a specific disk
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockLocation {
    /// Node where the block is stored
    pub node_id: NodeId,
    /// Disk where the block is stored
    pub disk_id: DiskId,
    /// Offset on the disk (in bytes)
    pub offset: u64,
    /// Size of the block (in bytes)
    pub size: u32,
}

/// Type of erasure coding algorithm
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum ErasureType {
    /// Maximum Distance Separable (standard Reed-Solomon)
    /// Any k shards can reconstruct the data
    #[default]
    Mds,
    /// Locally Repairable Codes
    /// Local parity groups enable faster single-shard repairs
    Lrc {
        /// Number of local parity shards (one per group)
        local_parity: u8,
        /// Number of global parity shards (Reed-Solomon over all data)
        global_parity: u8,
    },
}

impl ErasureType {
    /// Check if this is MDS
    pub const fn is_mds(&self) -> bool {
        matches!(self, Self::Mds)
    }

    /// Check if this is LRC
    pub const fn is_lrc(&self) -> bool {
        matches!(self, Self::Lrc { .. })
    }

    /// Get the LRC parameters if this is LRC type
    pub const fn lrc_params(&self) -> Option<(u8, u8)> {
        match self {
            Self::Lrc {
                local_parity,
                global_parity,
            } => Some((*local_parity, *global_parity)),
            Self::Mds => None,
        }
    }
}

/// Erasure coding configuration
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ErasureConfig {
    /// Number of data shards (k)
    pub data_shards: u8,
    /// Number of parity shards (m for MDS, l+g for LRC)
    pub parity_shards: u8,
    /// Type of erasure coding
    #[serde(default)]
    pub ec_type: ErasureType,
}

impl ErasureConfig {
    /// Create a new MDS erasure config (backward compatible)
    #[must_use]
    pub const fn new(data_shards: u8, parity_shards: u8) -> Self {
        Self {
            data_shards,
            parity_shards,
            ec_type: ErasureType::Mds,
        }
    }

    /// Create a new MDS erasure config (explicit)
    #[must_use]
    pub const fn mds(data_shards: u8, parity_shards: u8) -> Self {
        Self::new(data_shards, parity_shards)
    }

    /// Create a new LRC erasure config
    ///
    /// # Arguments
    /// * `data_shards` - Number of data shards (k), must be divisible by `local_parity`
    /// * `local_parity` - Number of local parity shards (l), one per group
    /// * `global_parity` - Number of global parity shards (g)
    #[must_use]
    pub fn lrc(data_shards: u8, local_parity: u8, global_parity: u8) -> Self {
        Self {
            data_shards,
            parity_shards: local_parity + global_parity,
            ec_type: ErasureType::Lrc {
                local_parity,
                global_parity,
            },
        }
    }

    /// Total number of shards (k + m)
    #[must_use]
    pub fn total_shards(&self) -> u8 {
        self.data_shards + self.parity_shards
    }

    /// Storage efficiency (k / (k + m))
    #[must_use]
    pub fn efficiency(&self) -> f64 {
        f64::from(self.data_shards) / f64::from(self.total_shards())
    }

    /// Check if this is MDS configuration
    pub const fn is_mds(&self) -> bool {
        self.ec_type.is_mds()
    }

    /// Check if this is LRC configuration
    pub const fn is_lrc(&self) -> bool {
        self.ec_type.is_lrc()
    }

    /// Get local group size for LRC (data shards per group)
    /// Returns None for MDS
    #[must_use]
    pub fn local_group_size(&self) -> Option<u8> {
        match self.ec_type {
            ErasureType::Lrc { local_parity, .. } => Some(self.data_shards / local_parity),
            ErasureType::Mds => None,
        }
    }

    // MDS presets (backward compatible)

    /// Default 4+2 MDS configuration
    pub const EC_4_2: Self = Self::mds(4, 2);

    /// 6+3 MDS configuration
    pub const EC_6_3: Self = Self::mds(6, 3);

    /// 8+4 MDS configuration
    pub const EC_8_4: Self = Self::mds(8, 4);

    /// 10+5 MDS configuration
    pub const EC_10_5: Self = Self::mds(10, 5);

    // LRC presets (use struct constructor with pre-computed parity_shards)

    /// Azure-style LRC: 12 data, 2 local parity (groups of 6), 2 global parity
    pub const LRC_12_2_2: Self = Self {
        data_shards: 12,
        parity_shards: 4, // 2 local + 2 global
        ec_type: ErasureType::Lrc {
            local_parity: 2,
            global_parity: 2,
        },
    };

    /// Smaller LRC: 6 data, 2 local parity (groups of 3), 2 global parity
    pub const LRC_6_2_2: Self = Self {
        data_shards: 6,
        parity_shards: 4, // 2 local + 2 global
        ec_type: ErasureType::Lrc {
            local_parity: 2,
            global_parity: 2,
        },
    };

    /// Compact LRC: 8 data, 2 local parity (groups of 4), 2 global parity
    pub const LRC_8_2_2: Self = Self {
        data_shards: 8,
        parity_shards: 4, // 2 local + 2 global
        ec_type: ErasureType::Lrc {
            local_parity: 2,
            global_parity: 2,
        },
    };
}

impl Default for ErasureConfig {
    fn default() -> Self {
        Self::EC_4_2
    }
}

/// Failure domain levels for placement
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum FailureDomain {
    /// Individual disk failure
    Disk = 0,
    /// Server/host failure
    Node = 1,
    /// Rack/power domain failure
    Rack = 2,
    /// Datacenter/AZ failure
    Datacenter = 3,
    /// Geographic region failure
    Region = 4,
}

impl fmt::Display for FailureDomain {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Disk => write!(f, "disk"),
            Self::Node => write!(f, "node"),
            Self::Rack => write!(f, "rack"),
            Self::Datacenter => write!(f, "datacenter"),
            Self::Region => write!(f, "region"),
        }
    }
}

/// Data protection type
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProtectionType {
    /// MDS Erasure coding (used within datacenter)
    ErasureCoding {
        /// Number of data shards
        data_shards: u8,
        /// Number of parity shards
        parity_shards: u8,
        /// Minimum failure domain for shard placement
        placement: FailureDomain,
    },
    /// LRC - Locally Repairable Codes (for large clusters with fast local repair)
    /// Local groups are placed within the same failure domain for fast repair
    Lrc {
        /// Number of data shards (k)
        data_shards: u8,
        /// Number of local parity shards (l) - one per group
        local_parity: u8,
        /// Number of global parity shards (g)
        global_parity: u8,
        /// Failure domain for local group placement (groups stay within this domain)
        local_placement: FailureDomain,
        /// Failure domain for spreading groups (groups spread across this domain)
        group_placement: FailureDomain,
    },
    /// Replication (used across datacenters)
    Replication {
        /// Number of copies
        replicas: u8,
        /// Minimum failure domain for replica placement
        placement: FailureDomain,
    },
}

impl ProtectionType {
    /// Create MDS erasure coding protection
    pub const fn mds(data_shards: u8, parity_shards: u8, placement: FailureDomain) -> Self {
        Self::ErasureCoding {
            data_shards,
            parity_shards,
            placement,
        }
    }

    /// Create LRC protection
    /// - Local groups are placed within `local_placement` domain (e.g., same rack for fast repair)
    /// - Groups are spread across `group_placement` domain (e.g., different racks for fault tolerance)
    pub const fn lrc(
        data_shards: u8,
        local_parity: u8,
        global_parity: u8,
        local_placement: FailureDomain,
        group_placement: FailureDomain,
    ) -> Self {
        Self::Lrc {
            data_shards,
            local_parity,
            global_parity,
            local_placement,
            group_placement,
        }
    }

    /// Get total number of shards
    #[must_use]
    pub fn total_shards(&self) -> u8 {
        match self {
            Self::ErasureCoding {
                data_shards,
                parity_shards,
                ..
            } => data_shards + parity_shards,
            Self::Lrc {
                data_shards,
                local_parity,
                global_parity,
                ..
            } => data_shards + local_parity + global_parity,
            Self::Replication { replicas, .. } => *replicas,
        }
    }

    /// Get number of data shards (k)
    pub const fn data_shards(&self) -> u8 {
        match self {
            Self::ErasureCoding { data_shards, .. } => *data_shards,
            Self::Lrc { data_shards, .. } => *data_shards,
            Self::Replication { replicas, .. } => *replicas,
        }
    }

    /// Check if this is LRC
    pub const fn is_lrc(&self) -> bool {
        matches!(self, Self::Lrc { .. })
    }

    /// Get local group size for LRC (data shards per group)
    #[must_use]
    pub fn local_group_size(&self) -> Option<u8> {
        match self {
            Self::Lrc {
                data_shards,
                local_parity,
                ..
            } => Some(*data_shards / *local_parity),
            _ => None,
        }
    }

    /// Get number of local groups for LRC
    pub const fn num_local_groups(&self) -> Option<u8> {
        match self {
            Self::Lrc { local_parity, .. } => Some(*local_parity),
            _ => None,
        }
    }
}

impl Default for ProtectionType {
    fn default() -> Self {
        Self::ErasureCoding {
            data_shards: 4,
            parity_shards: 2,
            placement: FailureDomain::Rack,
        }
    }
}

/// Storage class configuration
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageClass {
    /// Name of the storage class
    pub name: String,
    /// Protection type (EC or replication)
    pub protection: ProtectionType,
    /// Optional geo-replication configuration
    pub geo_replication: Option<GeoReplication>,
}

/// Geo-replication configuration (for cross-DC replication)
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GeoReplication {
    /// Number of datacenter copies
    pub replicas: u8,
    /// Synchronous or asynchronous replication
    pub sync_mode: SyncMode,
}

/// Replication synchronization mode
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum SyncMode {
    /// Wait for all replicas before acknowledging write
    Sync,
    /// Acknowledge write after local commit, replicate asynchronously
    #[default]
    Async,
}

/// Node status
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeStatus {
    /// Node is healthy and accepting requests
    #[default]
    Active,
    /// Node is being drained (no new data, serving reads)
    Draining,
    /// Node is down or unreachable
    Down,
    /// Node is being decommissioned
    Decommissioning,
}

/// Disk status
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum DiskStatus {
    /// Disk is healthy
    #[default]
    Healthy,
    /// Disk has errors but is still operational
    Degraded,
    /// Disk has failed
    Failed,
    /// Disk is being rebuilt
    Rebuilding,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_object_id() {
        let id = ObjectId::new();
        assert_eq!(id.as_bytes().len(), 16);
    }

    #[test]
    fn test_bucket_name_valid() {
        assert!(BucketName::new("my-bucket").is_ok());
        assert!(BucketName::new("bucket123").is_ok());
        assert!(BucketName::new("a.b.c").is_ok());
    }

    #[test]
    fn test_bucket_name_invalid() {
        assert!(BucketName::new("ab").is_err()); // Too short
        assert!(BucketName::new("-bucket").is_err()); // Invalid start
        assert!(BucketName::new("bucket-").is_err()); // Invalid end
        assert!(BucketName::new("Bucket").is_err()); // Uppercase
        assert!(BucketName::new("bucket..name").is_err()); // Consecutive periods
        assert!(BucketName::new("192.168.1.1").is_err()); // IP address
    }

    #[test]
    fn test_erasure_config() {
        let ec = ErasureConfig::EC_4_2;
        assert_eq!(ec.data_shards, 4);
        assert_eq!(ec.parity_shards, 2);
        assert_eq!(ec.total_shards(), 6);
        assert!((ec.efficiency() - 0.666_666_666_666_666_6).abs() < 0.001);
    }
}
