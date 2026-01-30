//! Configuration types for ObjectIO
//!
//! This module defines configuration structures used across components.

use crate::types::{ErasureConfig, FailureDomain, SyncMode};
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::path::PathBuf;

/// Root configuration for ObjectIO
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Config {
    /// Node configuration
    pub node: NodeConfig,
    /// Storage configuration
    pub storage: StorageConfig,
    /// Network configuration
    pub network: NetworkConfig,
    /// S3 API configuration
    pub s3: S3Config,
    /// Cluster configuration
    pub cluster: ClusterConfig,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            node: NodeConfig::default(),
            storage: StorageConfig::default(),
            network: NetworkConfig::default(),
            s3: S3Config::default(),
            cluster: ClusterConfig::default(),
        }
    }
}

/// Node identity and role configuration
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NodeConfig {
    /// Node name (human-readable identifier)
    pub name: String,
    /// Data directory for metadata and state
    pub data_dir: PathBuf,
    /// Failure domain information
    pub failure_domain: FailureDomainConfig,
}

impl Default for NodeConfig {
    fn default() -> Self {
        Self {
            name: "objectio-node".to_string(),
            data_dir: PathBuf::from("/var/lib/objectio"),
            failure_domain: FailureDomainConfig::default(),
        }
    }
}

/// Failure domain configuration for this node
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FailureDomainConfig {
    /// Region name (e.g., "us-east-1")
    pub region: String,
    /// Datacenter/AZ name (e.g., "us-east-1a")
    pub datacenter: String,
    /// Rack identifier (e.g., "rack-1")
    pub rack: String,
}

impl Default for FailureDomainConfig {
    fn default() -> Self {
        Self {
            region: "default".to_string(),
            datacenter: "default".to_string(),
            rack: "default".to_string(),
        }
    }
}

/// Storage configuration
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StorageConfig {
    /// Disks to use for storage
    pub disks: Vec<DiskConfig>,
    /// Default erasure coding configuration
    pub default_ec: ErasureConfig,
    /// Block size for data (default: 4 MB)
    pub block_size: usize,
    /// WAL configuration
    pub wal: WalConfig,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            disks: Vec::new(),
            default_ec: ErasureConfig::EC_4_2,
            block_size: 4 * 1024 * 1024, // 4 MB
            wal: WalConfig::default(),
        }
    }
}

/// Configuration for a single disk
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DiskConfig {
    /// Path to the disk device or directory
    pub path: PathBuf,
    /// Use direct I/O (O_DIRECT on Linux, F_NOCACHE on macOS)
    pub direct_io: bool,
    /// Weight for placement (higher = more data)
    pub weight: f64,
}

impl Default for DiskConfig {
    fn default() -> Self {
        Self {
            path: PathBuf::from("/var/lib/objectio/data"),
            direct_io: true,
            weight: 1.0,
        }
    }
}

/// Write-ahead log configuration
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WalConfig {
    /// Size of each WAL segment
    pub segment_size: usize,
    /// Total WAL size limit
    pub max_size: usize,
    /// Sync mode for WAL writes
    pub sync_mode: WalSyncMode,
}

impl Default for WalConfig {
    fn default() -> Self {
        Self {
            segment_size: 64 * 1024 * 1024,  // 64 MB
            max_size: 1024 * 1024 * 1024,    // 1 GB
            sync_mode: WalSyncMode::OnCommit,
        }
    }
}

/// WAL synchronization mode
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub enum WalSyncMode {
    /// Sync after every write (safest, slowest)
    EveryWrite,
    /// Sync only on commit (balanced)
    OnCommit,
    /// Batch syncs at interval (fastest, less durable)
    Batched,
}

impl Default for WalSyncMode {
    fn default() -> Self {
        Self::OnCommit
    }
}

/// Network configuration
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NetworkConfig {
    /// Address for S3 API (gateway)
    pub s3_listen: SocketAddr,
    /// Address for internal gRPC
    pub grpc_listen: SocketAddr,
    /// Address for metrics endpoint
    pub metrics_listen: SocketAddr,
}

impl Default for NetworkConfig {
    fn default() -> Self {
        Self {
            s3_listen: "0.0.0.0:9000".parse().unwrap(),
            grpc_listen: "0.0.0.0:9001".parse().unwrap(),
            metrics_listen: "0.0.0.0:9090".parse().unwrap(),
        }
    }
}

/// S3 API configuration
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct S3Config {
    /// Region name to return in responses
    pub region: String,
    /// Maximum object size (default: 5 TB)
    pub max_object_size: u64,
    /// Maximum part size for multipart upload (default: 5 GB)
    pub max_part_size: u64,
    /// Minimum part size for multipart upload (default: 5 MB)
    pub min_part_size: u64,
    /// Maximum number of parts per upload
    pub max_parts: u32,
    /// Enable virtual-hosted style requests
    pub virtual_host_style: bool,
}

impl Default for S3Config {
    fn default() -> Self {
        Self {
            region: "us-east-1".to_string(),
            max_object_size: 5 * 1024 * 1024 * 1024 * 1024, // 5 TB
            max_part_size: 5 * 1024 * 1024 * 1024,          // 5 GB
            min_part_size: 5 * 1024 * 1024,                  // 5 MB
            max_parts: 10_000,
            virtual_host_style: true,
        }
    }
}

/// Cluster configuration
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClusterConfig {
    /// Cluster name
    pub name: String,
    /// Metadata service endpoints (for joining cluster)
    pub meta_endpoints: Vec<String>,
    /// Number of metadata replicas
    pub meta_replicas: u8,
    /// Heartbeat interval (milliseconds)
    pub heartbeat_interval_ms: u64,
    /// Heartbeat timeout (milliseconds)
    pub heartbeat_timeout_ms: u64,
    /// Repair configuration
    pub repair: RepairConfig,
    /// Geo-replication configuration
    pub geo: Option<GeoConfig>,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            name: "objectio".to_string(),
            meta_endpoints: Vec::new(),
            meta_replicas: 3,
            heartbeat_interval_ms: 1000,
            heartbeat_timeout_ms: 5000,
            repair: RepairConfig::default(),
            geo: None,
        }
    }
}

/// Repair and rebuild configuration
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RepairConfig {
    /// Maximum concurrent repairs
    pub max_concurrent: usize,
    /// Bandwidth limit per disk (bytes/sec, 0 = unlimited)
    pub bandwidth_limit: u64,
    /// Scrubbing interval (seconds)
    pub scrub_interval_secs: u64,
}

impl Default for RepairConfig {
    fn default() -> Self {
        Self {
            max_concurrent: 4,
            bandwidth_limit: 100 * 1024 * 1024, // 100 MB/s
            scrub_interval_secs: 7 * 24 * 60 * 60, // 7 days
        }
    }
}

/// Geo-replication configuration
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GeoConfig {
    /// Number of datacenter copies
    pub replicas: u8,
    /// Synchronization mode
    pub sync_mode: SyncMode,
    /// Remote datacenter endpoints
    pub endpoints: Vec<RemoteDcConfig>,
}

/// Remote datacenter configuration
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RemoteDcConfig {
    /// Datacenter name
    pub name: String,
    /// Endpoint URL
    pub endpoint: String,
}

/// Storage class definition
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StorageClassConfig {
    /// Name of the storage class
    pub name: String,
    /// Erasure coding configuration (for local protection)
    pub erasure_coding: Option<EcConfig>,
    /// Replication configuration (alternative to EC)
    pub replication: Option<ReplicationConfig>,
    /// Placement constraint
    pub placement: FailureDomain,
    /// Geo-replication override
    pub geo: Option<GeoConfig>,
}

/// Erasure coding profile
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EcConfig {
    /// Number of data shards (k)
    pub data_shards: u8,
    /// Number of parity shards (m)
    pub parity_shards: u8,
}

/// Replication configuration
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReplicationConfig {
    /// Number of replicas
    pub replicas: u8,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = Config::default();
        assert_eq!(config.node.name, "objectio-node");
        assert_eq!(config.storage.default_ec, ErasureConfig::EC_4_2);
        assert_eq!(config.network.s3_listen.port(), 9000);
    }
}
