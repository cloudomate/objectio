//! Raft consensus implementation

/// Raft state machine placeholder
pub struct RaftStateMachine {
    _placeholder: (),
}

impl RaftStateMachine {
    /// Create a new Raft state machine
    #[must_use]
    pub fn new() -> Self {
        Self { _placeholder: () }
    }
}

impl Default for RaftStateMachine {
    fn default() -> Self {
        Self::new()
    }
}
