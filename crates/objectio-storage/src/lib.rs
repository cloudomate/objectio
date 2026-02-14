//! ObjectIO Storage Engine - Raw disk storage
//!
//! This crate implements the storage engine for ObjectIO including:
//! - Raw disk access (O_DIRECT / F_NOCACHE)
//! - Write-ahead logging
//! - Block allocation and management
//! - Block caching (LRU with configurable write policies)
//! - Background repair and scrubbing
//! - Metadata storage (WAL + B-tree + ARC cache)

pub mod block;
pub mod cache;
pub mod disk;
pub mod layout;
pub mod metadata;
pub mod raw_io;
pub mod repair;
pub mod smart;
pub mod wal;

// Re-exports
pub use block::{Block, BlockAllocator, BlockBitmap, Extent};
pub use cache::{BlockCache, CacheCapacity, CacheKey, CacheStats, WritePolicy};
pub use disk::{DiskManager, DiskStats};
pub use layout::{
    ALIGNMENT, BlockFooter, BlockHeader, DEFAULT_BLOCK_SIZE, DEFAULT_WAL_SIZE, MIN_DISK_SIZE,
    SUPERBLOCK_SIZE, Superblock,
};
pub use metadata::{
    ArcCache, MetaCacheStats, MetadataEntry, MetadataKey, MetadataOp, MetadataStore, MetadataWal,
    ShardMeta,
};
pub use raw_io::{AlignedBuffer, RawFile};
pub use smart::{DiskSmartHealth, SmartAttribute, SmartMonitor};
pub use wal::{RecordType, SyncMode, WalRecord, WriteAheadLog, WriteOp};
