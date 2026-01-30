//! AWS Signature V4 authentication

/// Signature V4 authenticator
pub struct SigV4Authenticator {
    _placeholder: (),
}

impl SigV4Authenticator {
    /// Create a new authenticator
    #[must_use]
    pub fn new() -> Self {
        Self { _placeholder: () }
    }
}

impl Default for SigV4Authenticator {
    fn default() -> Self {
        Self::new()
    }
}
