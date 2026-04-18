//! Raft consensus configuration for the metadata service.
//!
//! This module declares the [`openraft`] type configuration that threads
//! through every consensus call: the command type, the response type, the
//! node identity, the log-entry wire format, and so on.
//!
//! ## One code path, cluster size is a deployment choice
//!
//! Meta always runs through Raft. A single-pod deployment boots a
//! single-voter cluster where there's no election drama and no network
//! RPCs; a 3+ pod deployment gets real quorum and leader election. The
//! code path through `Raft::client_write` is identical in both cases.
//!
//! ## Phase R1 scope
//!
//! Only [`MetaCommand::SetConfig`] and [`MetaCommand::DeleteConfig`] go
//! through consensus in the first phase. The rest of the 40+ mutation
//! handlers still write directly to redb and are not quorum-safe yet;
//! see the migration plan in `docs/FEATURES.md`.

use openraft::{BasicNode, declare_raft_types};
use serde::{Deserialize, Serialize};

/// A single replicated mutation against the metadata state.
///
/// Variants land in the Raft log exactly as encoded here; the state
/// machine's `apply` method is the only code path that turns them into
/// actual redb writes. Adding a new mutation means:
///
/// 1. add a variant here,
/// 2. handle it in the state machine's `apply`,
/// 3. swap the direct-redb-write site to `raft.client_write(...)`.
///
/// Because variants are persisted, **never change the wire shape of an
/// existing variant** — only append. Serde defaults handle additive field
/// changes; struct-to-enum or rename migrations need an explicit schema
/// bump.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum MetaCommand {
    /// Upsert a cluster config entry.
    SetConfig {
        key: String,
        value: Vec<u8>,
        updated_by: String,
    },
    /// Delete a cluster config entry. Idempotent — deleting a missing key
    /// applies successfully and returns `existed: false`.
    DeleteConfig { key: String },
}

/// Reply the state machine emits from `apply`, visible to the client that
/// proposed the command.
///
/// Kept small and typed rather than a serialized blob — response shape
/// changes shouldn't force a full log replay to interpret.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub enum MetaResponse {
    #[default]
    Ok,
    /// `SetConfig` committed; returns the monotonic version assigned by
    /// the state machine.
    ConfigSet { version: u64 },
    /// `DeleteConfig` committed; `existed` is true iff the key was
    /// present before deletion.
    ConfigDeleted { existed: bool },
}

declare_raft_types!(
    /// openraft type configuration for the metadata service.
    pub MetaTypeConfig:
        D            = MetaCommand,
        R            = MetaResponse,
        NodeId       = u64,
        Node         = BasicNode,
        Entry        = openraft::Entry<MetaTypeConfig>,
        SnapshotData = std::io::Cursor<Vec<u8>>,
        AsyncRuntime = openraft::TokioRuntime,
);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn commands_round_trip_through_serde() {
        // The log stores commands serialized; any format change breaks
        // replay, so pin the shape with a round-trip test.
        let cases = vec![
            MetaCommand::SetConfig {
                key: "license/active".into(),
                value: b"hello".to_vec(),
                updated_by: "console".into(),
            },
            MetaCommand::DeleteConfig {
                key: "license/active".into(),
            },
        ];
        for cmd in cases {
            let json = serde_json::to_vec(&cmd).unwrap();
            let back: MetaCommand = serde_json::from_slice(&json).unwrap();
            let back_json = serde_json::to_vec(&back).unwrap();
            assert_eq!(json, back_json, "round-trip mismatch: {cmd:?}");
        }
    }

    #[test]
    fn responses_round_trip_through_serde() {
        let cases = vec![
            MetaResponse::Ok,
            MetaResponse::ConfigSet { version: 42 },
            MetaResponse::ConfigDeleted { existed: true },
            MetaResponse::ConfigDeleted { existed: false },
        ];
        for resp in cases {
            let json = serde_json::to_vec(&resp).unwrap();
            let back: MetaResponse = serde_json::from_slice(&json).unwrap();
            let back_json = serde_json::to_vec(&back).unwrap();
            assert_eq!(json, back_json);
        }
    }
}
