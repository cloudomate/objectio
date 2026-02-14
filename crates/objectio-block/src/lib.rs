#![allow(clippy::result_large_err)]
//! ObjectIO Block Storage
//!
//! This crate provides block storage capabilities on top of ObjectIO's
//! distributed object storage layer. It maps logical block addresses (LBAs)
//! to erasure-coded chunks stored on OSDs.
//!
//! # Features
//!
//! - **Thin provisioning**: Only allocate storage on write
//! - **Snapshots**: Copy-on-write snapshots
//! - **Clones**: Writable copies from snapshots
//! - **Multiple protocols**: iSCSI, NVMe-oF, NBD support
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────┐
//! │  Block Client   │  (iSCSI/NVMe-oF/NBD)
//! └────────┬────────┘
//!          │
//! ┌────────▼────────┐
//! │  Block Gateway  │
//! │  - VolumeManager│
//! │  - ChunkMapper  │
//! │  - WriteCache   │
//! └────────┬────────┘
//!          │
//! ┌────────▼────────┐
//! │  ObjectIO OSDs  │  (Erasure-coded storage)
//! └─────────────────┘
//! ```

pub mod cache;
pub mod chunk;
pub mod error;
pub mod journal;
pub mod metrics;
pub mod qos;
pub mod volume;

pub use cache::{CacheConfig, WriteCache};
pub use chunk::{ChunkId, ChunkMapper, ChunkRange};
pub use error::{BlockError, BlockResult};
pub use journal::{JournalEntry, WriteJournal};
pub use metrics::{
    ClusterMetrics, HealthStatus, MetricsCollector, OsdMetrics, PrometheusExporter, VolumeMetrics,
};
pub use qos::{
    IoStats, LatencyHistogram, LatencyPercentiles, Priority, TokenBucket, VolumeQosConfig,
    VolumeRateLimiter,
};
pub use volume::{Volume, VolumeManager, VolumeState};

/// Default chunk size: 4MB (matches EC stripe size)
pub const DEFAULT_CHUNK_SIZE: u64 = 4 * 1024 * 1024;

/// LBA size in bytes (standard 512-byte sectors)
pub const LBA_SIZE: u64 = 512;

/// LBAs per chunk at default chunk size
pub const LBAS_PER_CHUNK: u64 = DEFAULT_CHUNK_SIZE / LBA_SIZE; // 8192
