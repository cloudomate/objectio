//! Object metadata operations

/// Object metadata store
pub struct ObjectStore {
    _placeholder: (),
}

impl ObjectStore {
    /// Create a new object store
    #[must_use]
    pub fn new() -> Self {
        Self { _placeholder: () }
    }
}

impl Default for ObjectStore {
    fn default() -> Self {
        Self::new()
    }
}
