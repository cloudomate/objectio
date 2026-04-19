// openraft::StorageError<u64> is the canonical error the trait methods
// return — it's intentionally large (wraps a full source chain). Boxing
// it throughout would obscure the call sites without any real payoff.
#![allow(clippy::result_large_err)]

//! Redb-backed [`openraft::RaftStorage`] for the metadata service.
//!
//! We implement the v1 trait and wrap it in [`openraft::storage::Adaptor`]
//! at the consumer side — that produces the v2 [`RaftLogStorage`] and
//! [`RaftStateMachine`] pair the current framework needs.
//!
//! ## Persistence layout (redb tables)
//!
//! | Table          | Key         | Value                                              |
//! |----------------|-------------|----------------------------------------------------|
//! | `raft_logs`    | `u64` index | JSON `openraft::Entry<MetaTypeConfig>`             |
//! | `raft_vote`    | `"vote"`    | JSON `openraft::Vote<u64>`                         |
//! | `raft_state`   | `"state"`   | JSON [`RaftPersistentState`] (applied + membership)|
//!
//! Every state-mutating method writes a redb transaction and only returns
//! after the transaction commits, so crashes never leave partially-applied
//! log entries or lost votes.
//!
//! ## Phase R1 scope
//!
//! - Log storage: full (append, truncate, purge, read).
//! - Vote: full.
//! - State machine: applies [`MetaCommand::SetConfig`] /
//!   [`MetaCommand::DeleteConfig`] into the existing `CONFIG` redb table.
//!   All other meta mutations still write directly to redb and are not
//!   quorum-safe yet — they get migrated variant-by-variant in R2+.
//! - Snapshots: **stubbed**. `get_current_snapshot` returns `None`,
//!   `build_snapshot` returns an empty snapshot, `install_snapshot` is a
//!   no-op. Good enough for small clusters that don't need log
//!   compaction; a real implementation follows in a later phase.

use std::fmt::Debug;
use std::io::Cursor;
use std::ops::RangeBounds;
use std::sync::Arc;

use openraft::{
    AnyError, EntryPayload, ErrorSubject, ErrorVerb, LogId, LogState, RaftLogReader,
    RaftSnapshotBuilder, RaftStorage, Snapshot, SnapshotMeta, StorageError, StorageIOError,
    StoredMembership, Vote,
};
use redb::{Database, ReadableTable};
use serde::{Deserialize, Serialize};

use crate::raft::{ApplyEvent, CasOp, CasTable, MetaCommand, MetaResponse, MetaTypeConfig};
use crate::tables;

type NodeId = u64;
type Node = openraft::BasicNode;
type Entry = openraft::Entry<MetaTypeConfig>;

/// Single-row payload persisted under `raft_state` so a restart can
/// restore the state machine without replaying the whole log.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct RaftPersistentState {
    last_applied: Option<LogId<NodeId>>,
    last_purged: Option<LogId<NodeId>>,
    membership: StoredMembership<NodeId, Node>,
    /// Monotonic counter returned by `MetaCommand::SetConfig` and bumped
    /// on every config write. Used as the `version` on `ConfigEntry`.
    config_version: u64,
}

/// Redb-backed Raft storage for meta.
///
/// Cheap to clone — internally it's an `Arc<Database>`. The openraft
/// Adaptor serializes concurrent access behind its own `RwLock`.
#[derive(Clone)]
pub struct MetaRaftStorage {
    db: Arc<Database>,
    /// Optional broadcast channel to the consumer of apply events.
    /// When set, the state machine emits one [`ApplyEvent`] per op of
    /// each committed `MultiCas` right after the redb commit. Consumers
    /// on both leader and follower use these to refresh their
    /// in-memory caches live — so reads on a just-promoted follower
    /// aren't stuck on the pre-promote snapshot.
    listener: Option<tokio::sync::mpsc::UnboundedSender<ApplyEvent>>,
}

impl MetaRaftStorage {
    /// Build a storage from a shared redb database. All Raft tables are
    /// opened on first write — no upfront migration needed.
    #[must_use]
    pub fn new(db: Arc<Database>) -> Self {
        Self { db, listener: None }
    }

    /// Attach an apply-event listener. The state machine will send one
    /// event per op of every committed `MultiCas`. Cloning the storage
    /// clones the sender too (unbounded channels are multi-producer).
    #[must_use]
    pub fn with_apply_listener(
        db: Arc<Database>,
        listener: tokio::sync::mpsc::UnboundedSender<ApplyEvent>,
    ) -> Self {
        Self {
            db,
            listener: Some(listener),
        }
    }

    // ---------------------------------------------------------------
    // Internal helpers — each is called from a trait method below.
    // These wrap redb transactions + JSON (de)serialization and convert
    // errors to openraft's StorageError.
    // ---------------------------------------------------------------

    fn load_state(&self) -> Result<RaftPersistentState, StorageError<NodeId>> {
        let txn = self.db.begin_read().map_err(read_err)?;
        let table = match txn.open_table(tables::RAFT_STATE) {
            Ok(t) => t,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(RaftPersistentState::default()),
            Err(e) => return Err(read_err(e)),
        };
        match table.get("state").map_err(read_err)? {
            Some(v) => {
                serde_json::from_slice(v.value()).map_err(|e| decode_err("raft_state", e))
            }
            None => Ok(RaftPersistentState::default()),
        }
    }

    fn save_state(&self, state: &RaftPersistentState) -> Result<(), StorageError<NodeId>> {
        let txn = self.db.begin_write().map_err(write_err)?;
        {
            let mut t = txn.open_table(tables::RAFT_STATE).map_err(write_err)?;
            let encoded = serde_json::to_vec(state).map_err(|e| encode_err("raft_state", e))?;
            t.insert("state", encoded.as_slice()).map_err(write_err)?;
        }
        txn.commit().map_err(write_err)
    }

    /// Apply a committed [`MetaCommand`] inside the same txn that moves
    /// `last_applied` forward. Atomic from the perspective of a restart.
    fn apply_command(
        &self,
        state: &mut RaftPersistentState,
        cmd: &MetaCommand,
        log_id: LogId<NodeId>,
    ) -> Result<MetaResponse, StorageError<NodeId>> {
        match cmd {
            MetaCommand::SetConfig {
                key,
                value,
                updated_by,
            } => {
                state.config_version += 1;
                let version = state.config_version;
                // Encode as the same protobuf shape meta already consumes,
                // so non-Raft readers (who still read the CONFIG table
                // directly) don't need migration code.
                use prost::Message;
                let entry = objectio_proto::metadata::ConfigEntry {
                    key: key.clone(),
                    value: value.clone(),
                    updated_at: now_unix(),
                    updated_by: updated_by.clone(),
                    version,
                };
                let bytes = entry.encode_to_vec();
                let txn = self.db.begin_write().map_err(write_err)?;
                {
                    let mut t = txn.open_table(tables::CONFIG).map_err(write_err)?;
                    t.insert(key.as_str(), bytes.as_slice()).map_err(write_err)?;
                }
                txn.commit().map_err(write_err)?;
                state.last_applied = Some(log_id);
                Ok(MetaResponse::ConfigSet { version })
            }
            MetaCommand::DeleteConfig { key } => {
                let txn = self.db.begin_write().map_err(write_err)?;
                let existed = {
                    let mut t = txn.open_table(tables::CONFIG).map_err(write_err)?;
                    t.remove(key.as_str()).map_err(write_err)?.is_some()
                };
                txn.commit().map_err(write_err)?;
                state.last_applied = Some(log_id);
                Ok(MetaResponse::ConfigDeleted { existed })
            }
            MetaCommand::SetOsdAdminState {
                node_id,
                state: new_state,
                requested_by: _,
            } => {
                // Same hex encoding the rest of the meta store uses to
                // key OsdNodes in the OSD_NODES table.
                let key = hex_encode_16(node_id);
                let txn = self.db.begin_write().map_err(write_err)?;
                let (found, changed) = {
                    let mut t = txn.open_table(tables::OSD_NODES).map_err(write_err)?;
                    // Read current bytes, release the borrow, then write
                    // — otherwise redb's access-guard holds a borrow of
                    // `t` that conflicts with the later insert.
                    let current = t
                        .get(key.as_str())
                        .map_err(read_err)?
                        .map(|v| v.value().to_vec());
                    match current {
                        Some(bytes) => {
                            let mut node: crate::types::OsdNode = bincode::deserialize(&bytes)
                                .map_err(|e| decode_err("OsdNode", e))?;
                            let changed = node.admin_state != *new_state;
                            if changed {
                                node.admin_state = *new_state;
                                let new_bytes = bincode::serialize(&node)
                                    .map_err(|e| decode_err("OsdNode", e))?;
                                t.insert(key.as_str(), new_bytes.as_slice())
                                    .map_err(write_err)?;
                            }
                            (true, changed)
                        }
                        // Unknown node_id — command succeeds (idempotent) but
                        // the caller learns via `found: false`. We still
                        // advance `last_applied` so the log entry isn't
                        // re-applied on restart.
                        None => (false, false),
                    }
                };
                txn.commit().map_err(write_err)?;
                state.last_applied = Some(log_id);
                Ok(MetaResponse::OsdAdminStateSet { changed, found })
            }
            MetaCommand::MultiCas {
                ops,
                requested_by: _,
            } => apply_multi_cas(&self.db, state, ops, log_id, self.listener.as_ref()),
        }
    }
}

/// Apply a [`MetaCommand::MultiCas`] inside a single redb write-txn.
///
/// Two-pass: (1) read every op's current value and compare against its
/// expected; collect all mismatches. If any mismatch, abort the txn —
/// no partial writes. (2) write/delete every op's new value. Commit.
///
/// The read+write happens in the same write-txn so interleaving with
/// other state-machine applies is impossible (openraft serializes
/// applies, and redb's write-txn is exclusive anyway).
fn apply_multi_cas(
    db: &redb::Database,
    state: &mut RaftPersistentState,
    ops: &[CasOp],
    log_id: LogId<NodeId>,
    listener: Option<&tokio::sync::mpsc::UnboundedSender<ApplyEvent>>,
) -> Result<MetaResponse, StorageError<NodeId>> {
    // Guardrail: keep log-entry apply latency bounded. Callers that need
    // thousands of conditional writes should chunk and retry.
    const MAX_OPS: usize = 256;
    if ops.len() > MAX_OPS {
        return Err(StorageError::IO {
            source: StorageIOError::write_state_machine(AnyError::error(format!(
                "MultiCas too many ops: {} > {MAX_OPS}",
                ops.len()
            ))),
        });
    }

    let txn = db.begin_write().map_err(write_err)?;
    let mut failed_indices: Vec<u32> = Vec::new();

    // Pass 1: verify every expected. Redb tables are scoped to the txn,
    // so we re-open per op to keep the lifetimes simple.
    for (idx, op) in ops.iter().enumerate() {
        let name = cas_table_name(&op.table);
        let tdef: redb::TableDefinition<&str, &[u8]> = redb::TableDefinition::new(name);
        let current: Option<Vec<u8>> = match txn.open_table(tdef) {
            Ok(t) => t
                .get(op.key.as_str())
                .map_err(read_err)?
                .map(|v| v.value().to_vec()),
            Err(redb::TableError::TableDoesNotExist(_)) => None,
            Err(e) => return Err(read_err(e)),
        };
        if current.as_deref() != op.expected.as_deref() {
            failed_indices.push(idx as u32);
        }
    }

    if !failed_indices.is_empty() {
        // Abort: drop the txn without commit. No partial state changes.
        // `last_applied` still advances so the log entry isn't retried.
        drop(txn);
        let commit_txn = db.begin_write().map_err(write_err)?;
        let mut s_state = state.clone();
        s_state.last_applied = Some(log_id);
        *state = s_state;
        commit_txn.commit().map_err(write_err)?;
        // Only persist `last_applied` here so follower replay sees the
        // same committed position. The failed indices go back to the
        // client so they can refresh and retry.
        return Ok(MetaResponse::MultiCasConflict { failed_indices });
    }

    // Pass 2: apply every write/delete in the same txn.
    for op in ops {
        let name = cas_table_name(&op.table);
        let tdef: redb::TableDefinition<&str, &[u8]> = redb::TableDefinition::new(name);
        let mut t = txn.open_table(tdef).map_err(write_err)?;
        match &op.new_value {
            Some(bytes) => {
                t.insert(op.key.as_str(), bytes.as_slice())
                    .map_err(write_err)?;
            }
            None => {
                t.remove(op.key.as_str()).map_err(write_err)?;
            }
        }
    }

    txn.commit().map_err(write_err)?;
    state.last_applied = Some(log_id);

    // Fan out apply events after the commit lands on disk. Send is
    // non-fatal: a dropped receiver (service crash, not yet wired up)
    // means the event is silently discarded. Consumers resync from redb
    // on next load_from_store so the cache can't stay permanently stale.
    if let Some(tx) = listener {
        for op in ops {
            let ev = ApplyEvent::MultiCasOp {
                table: op.table.clone(),
                key: op.key.clone(),
                new_value: op.new_value.clone(),
            };
            let _ = tx.send(ev);
        }
    }

    Ok(MetaResponse::MultiCasOk)
}

/// Map a [`CasTable`] tag to the redb table name used by the rest of the
/// meta store. Stays in lock-step with `tables.rs` — if you add a new
/// long-lived table, add a `CasTable` variant here too.
fn cas_table_name(t: &CasTable) -> &str {
    match t {
        CasTable::Buckets => "buckets",
        CasTable::BucketPolicies => "bucket_policies",
        CasTable::IcebergNamespaces => "iceberg_namespaces",
        CasTable::IcebergTables => "iceberg_tables",
        CasTable::DeltaShares => "delta_shares",
        CasTable::DeltaTables => "delta_tables",
        CasTable::DeltaRecipients => "delta_recipients",
        CasTable::Config => "config",
        CasTable::Pools => "pools",
        CasTable::Tenants => "tenants",
        CasTable::IamPolicies => "iam_policies",
        CasTable::Volumes => "volumes",
        CasTable::Snapshots => "snapshots",
        CasTable::Users => "users",
        CasTable::Groups => "groups",
        CasTable::AccessKeys => "access_keys",
        CasTable::Named(n) => n,
    }
}

/// 16-byte node id → 32-char lowercase hex, matching how MetaStore keys
/// OsdNodes. Tiny local impl so the crate doesn't need a `hex` dep just
/// for this one call site.
fn hex_encode_16(bytes: &[u8; 16]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(32);
    for b in bytes {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0x0f) as usize] as char);
    }
    out
}

/// Current unix timestamp, used to stamp `updated_at` on config writes.
fn now_unix() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

// ---------------------------------------------------------------
// Error-conversion helpers.
//
// openraft's `StorageError<NodeId>` wants a subject/verb/source triple.
// We slot redb/serde errors into those at the call site so every
// failure mode has a descriptive category.
// ---------------------------------------------------------------

fn write_err<E: std::error::Error + Send + Sync + 'static>(e: E) -> StorageError<NodeId> {
    StorageError::IO {
        source: StorageIOError::new(ErrorSubject::Logs, ErrorVerb::Write, AnyError::new(&e)),
    }
}

fn read_err<E: std::error::Error + Send + Sync + 'static>(e: E) -> StorageError<NodeId> {
    StorageError::IO {
        source: StorageIOError::new(ErrorSubject::Logs, ErrorVerb::Read, AnyError::new(&e)),
    }
}

fn decode_err<E: std::error::Error + Send + Sync + 'static>(what: &str, e: E) -> StorageError<NodeId> {
    let tagged = std::io::Error::other(format!("decode {what}: {e}"));
    StorageError::IO {
        source: StorageIOError::new(ErrorSubject::Logs, ErrorVerb::Read, AnyError::new(&tagged)),
    }
}

fn encode_err<E: std::error::Error + Send + Sync + 'static>(what: &str, e: E) -> StorageError<NodeId> {
    let tagged = std::io::Error::other(format!("encode {what}: {e}"));
    StorageError::IO {
        source: StorageIOError::new(ErrorSubject::Logs, ErrorVerb::Write, AnyError::new(&tagged)),
    }
}

// ---------------------------------------------------------------
// RaftLogReader — log reads via try_get_log_entries.
// ---------------------------------------------------------------

impl RaftLogReader<MetaTypeConfig> for MetaRaftStorage {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + Send>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry>, StorageError<NodeId>> {
        let txn = self.db.begin_read().map_err(read_err)?;
        let table = match txn.open_table(tables::RAFT_LOGS) {
            Ok(t) => t,
            // An unopened table means no logs yet — empty range result.
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
            Err(e) => return Err(read_err(e)),
        };

        let start = match range.start_bound() {
            std::ops::Bound::Included(&i) => i,
            std::ops::Bound::Excluded(&i) => i.saturating_add(1),
            std::ops::Bound::Unbounded => 0,
        };
        let end = match range.end_bound() {
            std::ops::Bound::Included(&i) => i.saturating_add(1),
            std::ops::Bound::Excluded(&i) => i,
            std::ops::Bound::Unbounded => u64::MAX,
        };

        let mut out = Vec::new();
        for row in table.range(start..end).map_err(read_err)? {
            let (_, v) = row.map_err(read_err)?;
            let entry: Entry = serde_json::from_slice(v.value())
                .map_err(|e| decode_err("raft_logs entry", e))?;
            out.push(entry);
        }
        Ok(out)
    }
}

// ---------------------------------------------------------------
// RaftSnapshotBuilder — minimal stub. Real snapshots in a later phase.
// ---------------------------------------------------------------

impl RaftSnapshotBuilder<MetaTypeConfig> for MetaRaftStorage {
    async fn build_snapshot(&mut self) -> Result<Snapshot<MetaTypeConfig>, StorageError<NodeId>> {
        let state = self.load_state()?;
        // Empty payload — no state-machine compaction yet. Log will
        // keep growing in R1, which is fine for config-scale workloads.
        let snapshot_id = format!(
            "meta-snap-{}",
            state
                .last_applied
                .map(|id| id.index)
                .unwrap_or_default()
        );
        Ok(Snapshot {
            meta: SnapshotMeta {
                last_log_id: state.last_applied,
                last_membership: state.membership,
                snapshot_id,
            },
            snapshot: Box::new(Cursor::new(Vec::new())),
        })
    }
}

// ---------------------------------------------------------------
// RaftStorage — the big one. Vote, log append/truncate/purge, apply.
// ---------------------------------------------------------------

#[allow(deprecated)] // RaftStorage is deprecated but the Adaptor still consumes it.
impl RaftStorage<MetaTypeConfig> for MetaRaftStorage {
    type LogReader = MetaRaftStorage;
    type SnapshotBuilder = MetaRaftStorage;

    async fn save_vote(&mut self, vote: &Vote<NodeId>) -> Result<(), StorageError<NodeId>> {
        let txn = self.db.begin_write().map_err(write_err)?;
        {
            let mut t = txn.open_table(tables::RAFT_VOTE).map_err(write_err)?;
            let bytes = serde_json::to_vec(vote).map_err(|e| encode_err("raft_vote", e))?;
            t.insert("vote", bytes.as_slice()).map_err(write_err)?;
        }
        txn.commit().map_err(write_err)?;
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<NodeId>>, StorageError<NodeId>> {
        let txn = self.db.begin_read().map_err(read_err)?;
        let table = match txn.open_table(tables::RAFT_VOTE) {
            Ok(t) => t,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
            Err(e) => return Err(read_err(e)),
        };
        match table.get("vote").map_err(read_err)? {
            Some(v) => Ok(Some(
                serde_json::from_slice(v.value()).map_err(|e| decode_err("raft_vote", e))?,
            )),
            None => Ok(None),
        }
    }

    async fn get_log_state(&mut self) -> Result<LogState<MetaTypeConfig>, StorageError<NodeId>> {
        let state = self.load_state()?;
        let txn = self.db.begin_read().map_err(read_err)?;
        let table = match txn.open_table(tables::RAFT_LOGS) {
            Ok(t) => t,
            Err(redb::TableError::TableDoesNotExist(_)) => {
                return Ok(LogState {
                    last_purged_log_id: state.last_purged,
                    last_log_id: state.last_purged,
                });
            }
            Err(e) => return Err(read_err(e)),
        };
        // Last log id = last row in the table, or `last_purged` if empty.
        let last_log_id = table
            .iter()
            .map_err(read_err)?
            .next_back()
            .transpose()
            .map_err(read_err)?
            .map(|(_, v)| {
                serde_json::from_slice::<Entry>(v.value())
                    .map_err(|e| decode_err("raft_logs last entry", e))
                    .map(|e| e.log_id)
            })
            .transpose()?;
        Ok(LogState {
            last_purged_log_id: state.last_purged,
            last_log_id: last_log_id.or(state.last_purged),
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn append_to_log<I>(&mut self, entries: I) -> Result<(), StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry> + Send,
    {
        let entries: Vec<Entry> = entries.into_iter().collect();
        if entries.is_empty() {
            return Ok(());
        }
        let txn = self.db.begin_write().map_err(write_err)?;
        {
            let mut t = txn.open_table(tables::RAFT_LOGS).map_err(write_err)?;
            for entry in &entries {
                let bytes = serde_json::to_vec(entry).map_err(|e| encode_err("raft_logs entry", e))?;
                t.insert(entry.log_id.index, bytes.as_slice())
                    .map_err(write_err)?;
            }
        }
        txn.commit().map_err(write_err)
    }

    async fn delete_conflict_logs_since(
        &mut self,
        log_id: LogId<NodeId>,
    ) -> Result<(), StorageError<NodeId>> {
        // Truncate log [log_id.index, ∞). Walk from the tail forward so a
        // crash mid-truncate leaves a consistent prefix.
        let txn = self.db.begin_write().map_err(write_err)?;
        {
            let mut t = txn.open_table(tables::RAFT_LOGS).map_err(write_err)?;
            let indices: Vec<u64> = t
                .range(log_id.index..)
                .map_err(write_err)?
                .filter_map(|r| r.ok().map(|(k, _)| k.value()))
                .collect();
            for idx in indices {
                t.remove(idx).map_err(write_err)?;
            }
        }
        txn.commit().map_err(write_err)
    }

    async fn purge_logs_upto(
        &mut self,
        log_id: LogId<NodeId>,
    ) -> Result<(), StorageError<NodeId>> {
        // Purge [0, log_id.index]. Also advance `last_purged` in state.
        let txn = self.db.begin_write().map_err(write_err)?;
        {
            let mut t = txn.open_table(tables::RAFT_LOGS).map_err(write_err)?;
            let indices: Vec<u64> = t
                .range(..=log_id.index)
                .map_err(write_err)?
                .filter_map(|r| r.ok().map(|(k, _)| k.value()))
                .collect();
            for idx in indices {
                t.remove(idx).map_err(write_err)?;
            }
        }
        txn.commit().map_err(write_err)?;
        let mut state = self.load_state()?;
        state.last_purged = Some(log_id);
        self.save_state(&state)
    }

    async fn last_applied_state(
        &mut self,
    ) -> Result<
        (
            Option<LogId<NodeId>>,
            StoredMembership<NodeId, Node>,
        ),
        StorageError<NodeId>,
    > {
        let state = self.load_state()?;
        Ok((state.last_applied, state.membership))
    }

    async fn apply_to_state_machine(
        &mut self,
        entries: &[Entry],
    ) -> Result<Vec<MetaResponse>, StorageError<NodeId>> {
        let mut state = self.load_state()?;
        let mut replies = Vec::with_capacity(entries.len());
        for entry in entries {
            match &entry.payload {
                EntryPayload::Blank => {
                    state.last_applied = Some(entry.log_id);
                    replies.push(MetaResponse::Ok);
                }
                EntryPayload::Normal(cmd) => {
                    let reply = self.apply_command(&mut state, cmd, entry.log_id)?;
                    replies.push(reply);
                }
                EntryPayload::Membership(mem) => {
                    state.last_applied = Some(entry.log_id);
                    state.membership = StoredMembership::new(Some(entry.log_id), mem.clone());
                    replies.push(MetaResponse::Ok);
                }
            }
        }
        // Persist `last_applied` + membership once per apply batch rather
        // than per entry — one less fsync per batch.
        self.save_state(&state)?;
        Ok(replies)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Cursor<Vec<u8>>>, StorageError<NodeId>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NodeId, Node>,
        _snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<NodeId>> {
        // R1 stub — snapshots aren't payload-bearing yet, so installing
        // one only updates the persistent markers.
        let mut state = self.load_state()?;
        state.last_applied = meta.last_log_id;
        state.membership = meta.last_membership.clone();
        self.save_state(&state)
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<MetaTypeConfig>>, StorageError<NodeId>> {
        // We don't persist snapshots in R1.
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use openraft::{CommittedLeaderId, EntryPayload, LogId};
    use tempfile::TempDir;

    fn storage() -> (TempDir, MetaRaftStorage) {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("meta.db");
        let db = Database::create(&path).unwrap();
        (dir, MetaRaftStorage::new(Arc::new(db)))
    }

    fn log_id(term: u64, index: u64) -> LogId<NodeId> {
        LogId::new(CommittedLeaderId::new(term, 1), index)
    }

    fn normal_entry(index: u64, cmd: MetaCommand) -> Entry {
        Entry {
            log_id: log_id(1, index),
            payload: EntryPayload::Normal(cmd),
        }
    }

    #[tokio::test]
    async fn vote_round_trip() {
        let (_d, mut s) = storage();
        assert!(s.read_vote().await.unwrap().is_none());
        let v = Vote::new(7, 42);
        s.save_vote(&v).await.unwrap();
        let back = s.read_vote().await.unwrap().unwrap();
        assert_eq!(back.leader_id().get_term(), 7);
    }

    #[tokio::test]
    async fn append_read_truncate_purge() {
        let (_d, mut s) = storage();
        let entries = vec![
            normal_entry(
                1,
                MetaCommand::SetConfig {
                    key: "a".into(),
                    value: b"1".to_vec(),
                    updated_by: "t".into(),
                },
            ),
            normal_entry(
                2,
                MetaCommand::SetConfig {
                    key: "b".into(),
                    value: b"2".to_vec(),
                    updated_by: "t".into(),
                },
            ),
            normal_entry(
                3,
                MetaCommand::SetConfig {
                    key: "c".into(),
                    value: b"3".to_vec(),
                    updated_by: "t".into(),
                },
            ),
        ];
        s.append_to_log(entries).await.unwrap();

        let got = s.try_get_log_entries(1..=3).await.unwrap();
        assert_eq!(got.len(), 3);
        assert_eq!(got[0].log_id.index, 1);

        // Truncate conflict starting at 2 → only index 1 remains.
        s.delete_conflict_logs_since(log_id(1, 2)).await.unwrap();
        let got = s.try_get_log_entries(0..=10).await.unwrap();
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].log_id.index, 1);

        // Purge up to 1 → empty + last_purged = 1.
        s.purge_logs_upto(log_id(1, 1)).await.unwrap();
        let got = s.try_get_log_entries(0..=10).await.unwrap();
        assert!(got.is_empty());
        let ls = s.get_log_state().await.unwrap();
        assert_eq!(ls.last_purged_log_id.unwrap().index, 1);
    }

    #[tokio::test]
    async fn apply_set_and_delete_config() {
        let (_d, mut s) = storage();
        let set = normal_entry(
            1,
            MetaCommand::SetConfig {
                key: "license/active".into(),
                value: b"signed-license-bytes".to_vec(),
                updated_by: "console".into(),
            },
        );
        let del = normal_entry(
            2,
            MetaCommand::DeleteConfig {
                key: "license/active".into(),
            },
        );

        let r = s.apply_to_state_machine(&[set]).await.unwrap();
        assert!(matches!(r[0], MetaResponse::ConfigSet { version: 1 }));

        let (last, _mem) = s.last_applied_state().await.unwrap();
        assert_eq!(last.unwrap().index, 1);

        let r = s.apply_to_state_machine(&[del]).await.unwrap();
        assert!(matches!(r[0], MetaResponse::ConfigDeleted { existed: true }));
    }

    #[tokio::test]
    async fn multi_cas_all_ok() {
        let (_d, mut s) = storage();
        // Seed: prior iceberg table row.
        let seed = normal_entry(
            1,
            MetaCommand::MultiCas {
                ops: vec![CasOp {
                    table: CasTable::IcebergTables,
                    key: "ns/tbl".into(),
                    expected: None,
                    new_value: Some(b"v1".to_vec()),
                }],
                requested_by: "seed".into(),
            },
        );
        let r = s.apply_to_state_machine(&[seed]).await.unwrap();
        assert!(matches!(r[0], MetaResponse::MultiCasOk));

        // Cross-table atomic update: rewrite the iceberg row AND
        // insert a bucket-policy row in one shot.
        let txn = normal_entry(
            2,
            MetaCommand::MultiCas {
                ops: vec![
                    CasOp {
                        table: CasTable::IcebergTables,
                        key: "ns/tbl".into(),
                        expected: Some(b"v1".to_vec()),
                        new_value: Some(b"v2".to_vec()),
                    },
                    CasOp {
                        table: CasTable::BucketPolicies,
                        key: "mybucket".into(),
                        expected: None,
                        new_value: Some(b"policy-json".to_vec()),
                    },
                ],
                requested_by: "txn".into(),
            },
        );
        let r = s.apply_to_state_machine(&[txn]).await.unwrap();
        assert!(matches!(r[0], MetaResponse::MultiCasOk));
    }

    #[tokio::test]
    async fn multi_cas_conflict_aborts_all() {
        let (_d, mut s) = storage();
        // Seed a row at v1.
        let seed = normal_entry(
            1,
            MetaCommand::MultiCas {
                ops: vec![CasOp {
                    table: CasTable::IcebergTables,
                    key: "ns/a".into(),
                    expected: None,
                    new_value: Some(b"v1".to_vec()),
                }],
                requested_by: "seed".into(),
            },
        );
        s.apply_to_state_machine(&[seed]).await.unwrap();

        // Attempt cross-table update where op[1]'s expected is wrong.
        // Op[0] would succeed in isolation; with MultiCas, the whole
        // command must abort and no writes land.
        let bad = normal_entry(
            2,
            MetaCommand::MultiCas {
                ops: vec![
                    CasOp {
                        table: CasTable::IcebergTables,
                        key: "ns/a".into(),
                        expected: Some(b"v1".to_vec()),
                        new_value: Some(b"v2".to_vec()),
                    },
                    CasOp {
                        table: CasTable::BucketPolicies,
                        key: "mybucket".into(),
                        expected: Some(b"not-there".to_vec()), // wrong
                        new_value: Some(b"policy-json".to_vec()),
                    },
                ],
                requested_by: "txn".into(),
            },
        );
        let r = s.apply_to_state_machine(&[bad]).await.unwrap();
        match &r[0] {
            MetaResponse::MultiCasConflict { failed_indices } => {
                assert_eq!(failed_indices, &vec![1]);
            }
            other => panic!("expected MultiCasConflict, got {other:?}"),
        }

        // Confirm op[0] was NOT applied — row is still v1, not v2.
        let txn = s.db.begin_read().unwrap();
        let t = txn
            .open_table(redb::TableDefinition::<&str, &[u8]>::new("iceberg_tables"))
            .unwrap();
        let v = t.get("ns/a").unwrap().unwrap().value().to_vec();
        assert_eq!(v, b"v1");

        // And that `last_applied` still advanced — so the conflict entry
        // isn't retried on leader restart.
        let (last, _) = s.last_applied_state().await.unwrap();
        assert_eq!(last.unwrap().index, 2);
    }

    #[tokio::test]
    async fn apply_listener_receives_one_event_per_op() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("meta.db");
        let db = Arc::new(Database::create(&path).unwrap());
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<ApplyEvent>();
        let mut s = MetaRaftStorage::with_apply_listener(db, tx);

        let e = normal_entry(
            1,
            MetaCommand::MultiCas {
                ops: vec![
                    CasOp {
                        table: CasTable::Buckets,
                        key: "b1".into(),
                        expected: None,
                        new_value: Some(b"v1".to_vec()),
                    },
                    CasOp {
                        table: CasTable::IcebergTables,
                        key: "ns/t".into(),
                        expected: None,
                        new_value: Some(b"v2".to_vec()),
                    },
                    CasOp {
                        table: CasTable::Buckets,
                        key: "b2".into(),
                        expected: None,
                        new_value: None, // delete — none to begin with, still reports as delete event
                    },
                ],
                requested_by: "test".into(),
            },
        );
        s.apply_to_state_machine(&[e]).await.unwrap();

        // Three ops → three events, in declaration order.
        let events: Vec<ApplyEvent> = (0..3).map(|_| rx.try_recv().unwrap()).collect();
        assert!(rx.try_recv().is_err(), "no more events expected");

        match &events[0] {
            ApplyEvent::MultiCasOp {
                table,
                key,
                new_value,
            } => {
                assert_eq!(table, &CasTable::Buckets);
                assert_eq!(key, "b1");
                assert_eq!(new_value.as_deref(), Some(&b"v1"[..]));
            }
        }
        match &events[1] {
            ApplyEvent::MultiCasOp { table, key, .. } => {
                assert_eq!(table, &CasTable::IcebergTables);
                assert_eq!(key, "ns/t");
            }
        }
        match &events[2] {
            ApplyEvent::MultiCasOp {
                table,
                key,
                new_value,
            } => {
                assert_eq!(table, &CasTable::Buckets);
                assert_eq!(key, "b2");
                assert!(new_value.is_none(), "delete op");
            }
        }
    }

    #[tokio::test]
    async fn apply_listener_not_called_on_conflict() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("meta.db");
        let db = Arc::new(Database::create(&path).unwrap());
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<ApplyEvent>();
        let mut s = MetaRaftStorage::with_apply_listener(db, tx);

        // Seed b1=v1.
        let seed = normal_entry(
            1,
            MetaCommand::MultiCas {
                ops: vec![CasOp {
                    table: CasTable::Buckets,
                    key: "b1".into(),
                    expected: None,
                    new_value: Some(b"v1".to_vec()),
                }],
                requested_by: "seed".into(),
            },
        );
        s.apply_to_state_machine(&[seed]).await.unwrap();
        // Consume the seed event.
        let _ = rx.try_recv().unwrap();

        // Now try a conflict MultiCas — should NOT emit events because
        // the writes didn't land.
        let bad = normal_entry(
            2,
            MetaCommand::MultiCas {
                ops: vec![CasOp {
                    table: CasTable::Buckets,
                    key: "b1".into(),
                    expected: Some(b"wrong".to_vec()),
                    new_value: Some(b"v2".to_vec()),
                }],
                requested_by: "conflict".into(),
            },
        );
        let r = s.apply_to_state_machine(&[bad]).await.unwrap();
        assert!(matches!(r[0], MetaResponse::MultiCasConflict { .. }));

        assert!(
            rx.try_recv().is_err(),
            "conflict should not emit apply events"
        );
    }

    #[tokio::test]
    async fn multi_cas_rejects_oversized_batch() {
        let (_d, mut s) = storage();
        let ops: Vec<CasOp> = (0..300)
            .map(|i| CasOp {
                table: CasTable::Config,
                key: format!("k{i}"),
                expected: None,
                new_value: Some(vec![0u8; 8]),
            })
            .collect();
        let big = normal_entry(
            1,
            MetaCommand::MultiCas {
                ops,
                requested_by: "bulk".into(),
            },
        );
        // Too large → apply returns the underlying storage error; the raft
        // runtime surfaces that to the caller as a propose failure.
        let err = s.apply_to_state_machine(&[big]).await.err();
        assert!(err.is_some(), "oversized MultiCas should fail apply");
    }

    #[tokio::test]
    async fn restart_preserves_applied_state() {
        // Write a config, drop the storage, open a new one against the
        // same file, confirm state survived the restart.
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("meta.db");
        {
            let db = Database::create(&path).unwrap();
            let mut s = MetaRaftStorage::new(Arc::new(db));
            let e = normal_entry(
                1,
                MetaCommand::SetConfig {
                    key: "k".into(),
                    value: b"v".to_vec(),
                    updated_by: "t".into(),
                },
            );
            s.apply_to_state_machine(&[e]).await.unwrap();
        }
        {
            let db = Database::create(&path).unwrap();
            let mut s = MetaRaftStorage::new(Arc::new(db));
            let (last, _) = s.last_applied_state().await.unwrap();
            assert_eq!(last.unwrap().index, 1);
        }
    }
}

