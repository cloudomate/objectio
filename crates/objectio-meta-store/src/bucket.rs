//! Bucket metadata operations

/// Bucket metadata store
pub struct BucketStore {
    _placeholder: (),
}

impl BucketStore {
    /// Create a new bucket store
    #[must_use]
    pub fn new() -> Self {
        Self { _placeholder: () }
    }
}

impl Default for BucketStore {
    fn default() -> Self {
        Self::new()
    }
}
