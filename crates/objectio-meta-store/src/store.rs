//! Main metadata store

/// Metadata store backed by Raft + redb
pub struct MetaStore {
    _placeholder: (),
}

impl MetaStore {
    /// Create a new metadata store
    #[must_use]
    pub fn new() -> Self {
        Self { _placeholder: () }
    }
}

impl Default for MetaStore {
    fn default() -> Self {
        Self::new()
    }
}
