//! Metadata service client

/// Client for the metadata service
pub struct MetaClient {
    _placeholder: (),
}

impl MetaClient {
    /// Create a new metadata client
    #[must_use]
    pub fn new() -> Self {
        Self { _placeholder: () }
    }
}

impl Default for MetaClient {
    fn default() -> Self {
        Self::new()
    }
}
