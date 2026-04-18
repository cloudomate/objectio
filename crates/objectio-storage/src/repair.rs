//! Repair and recovery management

/// Repair manager placeholder
pub struct RepairManager {
    _placeholder: (),
}

impl RepairManager {
    /// Create a new repair manager
    #[must_use]
    pub fn new() -> Self {
        Self { _placeholder: () }
    }
}

impl Default for RepairManager {
    fn default() -> Self {
        Self::new()
    }
}
