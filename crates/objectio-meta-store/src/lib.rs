//! ObjectIO Metadata Store - Raft-based metadata service
//!
//! This crate implements the distributed metadata store using Raft consensus.

pub mod bucket;
pub mod object;
pub mod raft;
pub mod store;

// Re-exports
pub use bucket::BucketStore;
pub use object::ObjectStore;
pub use store::MetaStore;
