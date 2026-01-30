//! OSD Metadata Storage Engine
//!
//! Hybrid architecture for OSD local metadata:
//! - **WAL**: Append-only log for durability and crash recovery
//! - **B-tree Index**: In-memory index with periodic snapshots
//! - **ARC Cache**: Adaptive Replacement Cache for hot entries
//! - **Background Compaction**: Merges WAL into snapshots
//!
//! # Design
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────┐
//! │                  MetadataStore                           │
//! │  ┌─────────────────────────────────────────────────────┐│
//! │  │              ARC Cache (hot entries)                ││
//! │  └─────────────────────────────────────────────────────┘│
//! │                          │                               │
//! │  ┌─────────────────────────────────────────────────────┐│
//! │  │           B-tree Index (in-memory)                  ││
//! │  │  • Sorted by key for range scans                    ││
//! │  │  • Periodic snapshot to disk                        ││
//! │  └─────────────────────────────────────────────────────┘│
//! │                          │                               │
//! │  ┌─────────────────────────────────────────────────────┐│
//! │  │              WAL (append-only)                      ││
//! │  │  • All mutations logged first                       ││
//! │  │  • Replay on recovery                               ││
//! │  │  • Truncated after snapshot                         ││
//! │  └─────────────────────────────────────────────────────┘│
//! └─────────────────────────────────────────────────────────┘
//! ```
//!
//! # Write Path
//! 1. Write to WAL (fsync)
//! 2. Update B-tree index (in-memory)
//! 3. Update cache (if entry was cached)
//!
//! # Read Path
//! 1. Check cache (hit → return)
//! 2. Lookup in B-tree index
//! 3. Populate cache on miss
//!
//! # Recovery
//! 1. Load latest snapshot into B-tree
//! 2. Replay WAL entries after snapshot LSN
//! 3. Ready to serve

mod wal;
mod btree;
mod cache;
mod store;
mod types;

pub use cache::{ArcCache, CacheStats as MetaCacheStats};
pub use store::{MetadataStore, MetadataStoreConfig};
pub use types::{MetadataEntry, MetadataKey, MetadataOp, ShardMeta};
pub use wal::MetadataWal;
