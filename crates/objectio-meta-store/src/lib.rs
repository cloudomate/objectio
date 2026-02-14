//! ObjectIO Metadata Store — persistent metadata backed by redb.

pub mod raft;
pub mod store;
pub mod tables;
pub mod types;

pub use store::{MetaStore, MetaStoreError, MetaStoreResult};
pub use types::{
    EcConfig, MultipartUploadState, OsdNode, PartState, StoredAccessKey, StoredAttachment,
    StoredChunkRef, StoredSnapshot, StoredUser, StoredVolume,
};
