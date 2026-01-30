//! OSD (Object Storage Daemon) client

/// Client for the storage node (OSD) service
pub struct OsdClient {
    _placeholder: (),
}

impl OsdClient {
    /// Create a new OSD client
    #[must_use]
    pub fn new() -> Self {
        Self { _placeholder: () }
    }
}

impl Default for OsdClient {
    fn default() -> Self {
        Self::new()
    }
}
