//! Block storage error types

use thiserror::Error;

/// Block storage error
#[derive(Error, Debug)]
pub enum BlockError {
    /// Volume not found
    #[error("Volume not found: {0}")]
    VolumeNotFound(String),

    /// Volume already exists
    #[error("Volume already exists: {0}")]
    VolumeExists(String),

    /// Volume is attached and cannot be modified
    #[error("Volume is attached: {0}")]
    VolumeAttached(String),

    /// Volume has snapshots and cannot be deleted
    #[error("Volume has snapshots: {0}")]
    VolumeHasSnapshots(String),

    /// Snapshot not found
    #[error("Snapshot not found: {0}")]
    SnapshotNotFound(String),

    /// Invalid volume size
    #[error("Invalid volume size: {0}")]
    InvalidSize(String),

    /// Cannot shrink volume
    #[error("Cannot shrink volume from {0} to {1} bytes")]
    CannotShrink(u64, u64),

    /// I/O error
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Offset out of bounds
    #[error("Offset {offset} + length {length} exceeds volume size {size}")]
    OutOfBounds { offset: u64, length: u64, size: u64 },

    /// Cache error
    #[error("Cache error: {0}")]
    Cache(String),

    /// Journal error
    #[error("Journal error: {0}")]
    Journal(String),

    /// Backend storage error
    #[error("Backend error: {0}")]
    Backend(String),

    /// gRPC error
    #[error("gRPC error: {0}")]
    Grpc(#[from] tonic::Status),

    /// Internal error
    #[error("Internal error: {0}")]
    Internal(String),
}

/// Result type for block operations
pub type BlockResult<T> = Result<T, BlockError>;
