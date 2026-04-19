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
    /// Operator-declared state for an OSD. Persisted on `OsdNode` and
    /// honoured by the placement topology: `Out` / `Draining` keep the
    /// node out of new placements regardless of heartbeat status.
    /// Idempotent — setting the same state again succeeds with
    /// `changed: false`.
    SetOsdAdminState {
        /// 16-byte node UUID.
        node_id: [u8; 16],
        /// New state.
        state: objectio_common::OsdAdminState,
        /// Audit trail — who requested this change (user id / "console" /
        /// "cli"). Stored in logs, not in the state machine.
        requested_by: String,
    },
    /// Atomic multi-key compare-and-swap across arbitrary meta tables.
    /// Every op's expected precondition must hold, or the whole command is
    /// rejected and no write lands. Intended for cross-table invariants —
    /// Iceberg transactions touching several tables, bucket renames that
    /// sweep every listing entry + bucket row in one shot, etc.
    ///
    /// Applied in a single redb write-txn so it's serialized with every
    /// other log entry and atomic on restart. Kept small (<= ~256 ops per
    /// command) to bound log-entry size and apply latency — callers with
    /// larger batches should chunk and rely on idempotent per-op retry.
    MultiCas {
        ops: Vec<CasOp>,
        /// Audit trail — caller identity. Not inspected by the state
        /// machine; landed in logs only.
        requested_by: String,
    },
}

/// A single conditional write inside a [`MetaCommand::MultiCas`].
///
/// `expected == None` means "key must NOT exist for this op to apply"
/// (create-if-absent). `new_value == None` means "delete this key"
/// (delete-if-matching).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CasOp {
    pub table: CasTable,
    pub key: String,
    pub expected: Option<Vec<u8>>,
    pub new_value: Option<Vec<u8>>,
}

/// Which meta table a [`CasOp`] targets. Serialized as a stable tag; the
/// state machine maps this to a `redb::TableDefinition`. Adding a new
/// table = appending a new variant here; **never remove or reorder** —
/// existing log entries carry these tags.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum CasTable {
    Buckets,
    BucketPolicies,
    IcebergNamespaces,
    IcebergTables,
    DeltaShares,
    DeltaTables,
    DeltaRecipients,
    Config,
    Pools,
    Tenants,
    IamPolicies,
    Volumes,
    Snapshots,
    Users,
    Groups,
    AccessKeys,
    /// Additive escape hatch for new tables introduced after this enum
    /// was frozen. Callers pass the redb table name directly; the state
    /// machine rejects unknown names rather than silently dropping the
    /// op. Prefer adding a named variant above when a table is
    /// long-lived.
    Named(String),
}

/// Event emitted by the state machine after a committed mutation has
/// landed in redb. Consumers — the live meta service on leader AND every
/// follower — use these to refresh their in-memory caches so reads
/// don't go stale until the next restart. The channel is unbounded and
/// best-effort: on slow consumers events accumulate, and on consumer
/// death the sender's send returns Err and the apply path drops the
/// event (the next promote-driven rebuild from redb catches up).
#[derive(Clone, Debug)]
pub enum ApplyEvent {
    /// One op from a committed `MultiCas`. A MultiCas that touches N
    /// keys produces N events, in the order the ops were declared. The
    /// state machine already applied them atomically — this event is a
    /// pure notification.
    MultiCasOp {
        table: CasTable,
        key: String,
        /// `None` = the op deleted the key.
        new_value: Option<Vec<u8>>,
    },
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
    /// `SetOsdAdminState` committed. `changed` is false if the OSD was
    /// already in the requested state; `found` is false if the node_id
    /// doesn't match any registered OSD (the command still applies
    /// successfully, but the caller can surface a warning).
    OsdAdminStateSet { changed: bool, found: bool },
    /// `MultiCas` committed — every op matched its expected value and
    /// every write/delete landed atomically.
    MultiCasOk,
    /// `MultiCas` rejected — at least one op's expected precondition
    /// didn't hold. `failed_indices` lists the op positions that
    /// mismatched so the caller can retry with refreshed expected
    /// values. No writes were applied.
    MultiCasConflict { failed_indices: Vec<u32> },
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
            MetaCommand::SetOsdAdminState {
                node_id: [7; 16],
                state: objectio_common::OsdAdminState::Out,
                requested_by: "console".into(),
            },
            MetaCommand::MultiCas {
                ops: vec![
                    CasOp {
                        table: CasTable::IcebergTables,
                        key: "ns/tbl".into(),
                        expected: Some(b"old".to_vec()),
                        new_value: Some(b"new".to_vec()),
                    },
                    CasOp {
                        table: CasTable::Named("custom_table".into()),
                        key: "k".into(),
                        expected: None,
                        new_value: Some(b"v".to_vec()),
                    },
                    CasOp {
                        table: CasTable::BucketPolicies,
                        key: "mybucket".into(),
                        expected: Some(b"policy1".to_vec()),
                        new_value: None, // delete
                    },
                ],
                requested_by: "iceberg-txn".into(),
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
            MetaResponse::OsdAdminStateSet {
                changed: true,
                found: true,
            },
            MetaResponse::OsdAdminStateSet {
                changed: false,
                found: false,
            },
            MetaResponse::MultiCasOk,
            MetaResponse::MultiCasConflict {
                failed_indices: vec![0, 2],
            },
        ];
        for resp in cases {
            let json = serde_json::to_vec(&resp).unwrap();
            let back: MetaResponse = serde_json::from_slice(&json).unwrap();
            let back_json = serde_json::to_vec(&back).unwrap();
            assert_eq!(json, back_json);
        }
    }
}
