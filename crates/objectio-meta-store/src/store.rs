//! Persistent metadata store backed by redb.
//!
//! Provides typed put/get/delete/load methods for each table. All writes
//! are synchronous (write txn + commit). Reads go through the in-memory
//! HashMap cache in the service layer — this module only handles persistence.

use crate::tables;
use crate::types::{
    MultipartUploadState, OsdNode, StoredAccessKey, StoredChunkRef, StoredDataFilter, StoredGroup,
    StoredSnapshot, StoredUser, StoredVolume,
};
use objectio_proto::metadata::BucketMeta;
use prost::Message;
use redb::{Database, ReadableTable};
use std::path::Path;
use std::sync::Arc;
use tracing::error;

/// Error type for metadata store operations
#[derive(Debug, thiserror::Error)]
pub enum MetaStoreError {
    #[error("redb error: {0}")]
    Redb(#[from] redb::DatabaseError),
    #[error("redb storage error: {0}")]
    Storage(#[from] redb::StorageError),
    #[error("redb table error: {0}")]
    Table(#[from] redb::TableError),
    #[error("redb transaction error: {0}")]
    Transaction(Box<redb::TransactionError>),
    #[error("redb commit error: {0}")]
    Commit(#[from] redb::CommitError),
    #[error("bincode error: {0}")]
    Bincode(#[from] bincode::Error),
    #[error("prost decode error: {0}")]
    Decode(#[from] prost::DecodeError),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

impl From<redb::TransactionError> for MetaStoreError {
    fn from(e: redb::TransactionError) -> Self {
        Self::Transaction(Box::new(e))
    }
}

pub type MetaStoreResult<T> = Result<T, MetaStoreError>;

/// Persistent metadata store backed by redb.
///
/// Wraps an `Arc<Database>` so the same file can be shared with
/// [`crate::MetaRaftStorage`] — consensus needs to write to the same
/// `CONFIG` table that MetaStore reads from, and redb rejects multiple
/// handles to the same file, so handle-sharing is the only option.
pub struct MetaStore {
    db: Arc<Database>,
}

impl MetaStore {
    /// Borrow the underlying shared database handle. Exposed for
    /// [`crate::MetaRaftStorage`] so Raft's state-machine applies can
    /// write to the same tables the rest of meta reads.
    #[must_use]
    pub fn db(&self) -> Arc<Database> {
        self.db.clone()
    }

    /// Open (or create) the redb database at the given path.
    pub fn open(path: impl AsRef<Path>) -> MetaStoreResult<Self> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let db = Database::create(path)?;

        // Create all tables eagerly so later read txns don't fail
        let write_txn = db.begin_write()?;
        {
            let _t = write_txn.open_table(tables::BUCKETS)?;
            let _t = write_txn.open_table(tables::BUCKET_POLICIES)?;
            let _t = write_txn.open_table(tables::MULTIPART_UPLOADS)?;
            let _t = write_txn.open_table(tables::OSD_NODES)?;
            let _t = write_txn.open_table(tables::CLUSTER_TOPOLOGY)?;
            let _t = write_txn.open_table(tables::USERS)?;
            let _t = write_txn.open_table(tables::ACCESS_KEYS)?;
            let _t = write_txn.open_table(tables::VOLUMES)?;
            let _t = write_txn.open_table(tables::SNAPSHOTS)?;
            let _t = write_txn.open_table(tables::VOLUME_CHUNKS)?;
            let _t = write_txn.open_table(tables::ICEBERG_NAMESPACES)?;
            let _t = write_txn.open_table(tables::ICEBERG_TABLES)?;
            let _t = write_txn.open_table(tables::GROUPS)?;
            let _t = write_txn.open_table(tables::GROUP_MEMBERS)?;
            let _t = write_txn.open_table(tables::DATA_FILTERS)?;
            let _t = write_txn.open_table(tables::DELTA_SHARES)?;
            let _t = write_txn.open_table(tables::DELTA_TABLES)?;
            let _t = write_txn.open_table(tables::DELTA_RECIPIENTS)?;
            let _t = write_txn.open_table(tables::CONFIG)?;
            let _t = write_txn.open_table(tables::POOLS)?;
            let _t = write_txn.open_table(tables::TENANTS)?;
            let _t = write_txn.open_table(tables::IAM_POLICIES)?;
            let _t = write_txn.open_table(tables::POLICY_ATTACHMENTS)?;
            let _t = write_txn.open_table(tables::ICEBERG_WAREHOUSES)?;
            let _t = write_txn.open_table(tables::OBJECT_LOCK_CONFIGS)?;
            let _t = write_txn.open_table(tables::LIFECYCLE_CONFIGS)?;
        }
        write_txn.commit()?;

        Ok(Self { db: Arc::new(db) })
    }

    // ---- Buckets (prost-encoded) ----

    pub fn put_bucket(&self, name: &str, bucket: &BucketMeta) {
        let bytes = bucket.encode_to_vec();
        if let Err(e) = self.put_bytes(tables::BUCKETS, name, &bytes) {
            error!("Failed to persist bucket '{}': {}", name, e);
        }
    }

    pub fn delete_bucket(&self, name: &str) {
        if let Err(e) = self.delete_key(tables::BUCKETS, name) {
            error!("Failed to delete bucket '{}': {}", name, e);
        }
    }

    pub fn load_buckets(&self) -> MetaStoreResult<Vec<(String, BucketMeta)>> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(tables::BUCKETS)?;
        let mut result = Vec::new();
        for entry in table.iter()? {
            let entry = entry?;
            let key = entry.0.value().to_string();
            let bytes = entry.1.value();
            match BucketMeta::decode(bytes) {
                Ok(bucket) => result.push((key, bucket)),
                Err(e) => error!("Failed to decode bucket '{}': {}", key, e),
            }
        }
        Ok(result)
    }

    // ---- Bucket Policies (string values) ----

    pub fn put_bucket_policy(&self, bucket: &str, policy_json: &str) {
        if let Err(e) = (|| -> MetaStoreResult<()> {
            let write_txn = self.db.begin_write()?;
            {
                let mut table = write_txn.open_table(tables::BUCKET_POLICIES)?;
                table.insert(bucket, policy_json)?;
            }
            write_txn.commit()?;
            Ok(())
        })() {
            error!("Failed to persist bucket policy '{}': {}", bucket, e);
        }
    }

    pub fn delete_bucket_policy(&self, bucket: &str) {
        if let Err(e) = self.delete_key(tables::BUCKET_POLICIES, bucket) {
            error!("Failed to delete bucket policy '{}': {}", bucket, e);
        }
    }

    pub fn load_bucket_policies(&self) -> MetaStoreResult<Vec<(String, String)>> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(tables::BUCKET_POLICIES)?;
        let mut result = Vec::new();
        for entry in table.iter()? {
            let entry = entry?;
            result.push((entry.0.value().to_string(), entry.1.value().to_string()));
        }
        Ok(result)
    }

    // ---- Multipart Uploads (bincode) ----

    pub fn put_multipart_upload(&self, upload_id: &str, state: &MultipartUploadState) {
        if let Err(e) = self.put_bincode(tables::MULTIPART_UPLOADS, upload_id, state) {
            error!("Failed to persist multipart upload '{}': {}", upload_id, e);
        }
    }

    pub fn delete_multipart_upload(&self, upload_id: &str) {
        if let Err(e) = self.delete_key(tables::MULTIPART_UPLOADS, upload_id) {
            error!("Failed to delete multipart upload '{}': {}", upload_id, e);
        }
    }

    pub fn load_multipart_uploads(&self) -> MetaStoreResult<Vec<(String, MultipartUploadState)>> {
        self.load_bincode_table(tables::MULTIPART_UPLOADS)
    }

    // ---- OSD Nodes (bincode) ----

    pub fn put_osd_node(&self, node_id_hex: &str, node: &OsdNode) {
        if let Err(e) = self.put_bincode(tables::OSD_NODES, node_id_hex, node) {
            error!("Failed to persist OSD node '{}': {}", node_id_hex, e);
        }
    }

    pub fn load_osd_nodes(&self) -> MetaStoreResult<Vec<(String, OsdNode)>> {
        self.load_bincode_table(tables::OSD_NODES)
    }

    // ---- Cluster Topology (bincode, single key) ----

    pub fn put_topology(&self, topology: &objectio_placement::topology::ClusterTopology) {
        if let Err(e) = self.put_bincode(tables::CLUSTER_TOPOLOGY, "topology", topology) {
            error!("Failed to persist cluster topology: {}", e);
        }
    }

    pub fn load_topology(
        &self,
    ) -> MetaStoreResult<Option<objectio_placement::topology::ClusterTopology>> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(tables::CLUSTER_TOPOLOGY)?;
        match table.get("topology")? {
            Some(val) => {
                let topo = bincode::deserialize(val.value())?;
                Ok(Some(topo))
            }
            None => Ok(None),
        }
    }

    // ---- OSD Node + Topology batch write ----

    pub fn put_osd_and_topology(
        &self,
        node_id_hex: &str,
        node: &OsdNode,
        topology: &objectio_placement::topology::ClusterTopology,
    ) {
        if let Err(e) = (|| -> MetaStoreResult<()> {
            let node_bytes = bincode::serialize(node)?;
            let topo_bytes = bincode::serialize(topology)?;
            let write_txn = self.db.begin_write()?;
            {
                let mut t = write_txn.open_table(tables::OSD_NODES)?;
                t.insert(node_id_hex, node_bytes.as_slice())?;
                let mut t2 = write_txn.open_table(tables::CLUSTER_TOPOLOGY)?;
                t2.insert("topology", topo_bytes.as_slice())?;
            }
            write_txn.commit()?;
            Ok(())
        })() {
            error!("Failed to persist OSD + topology '{}': {}", node_id_hex, e);
        }
    }

    // ---- Users (bincode) ----

    pub fn put_user(&self, user_id: &str, user: &StoredUser) {
        if let Err(e) = self.put_bincode(tables::USERS, user_id, user) {
            error!("Failed to persist user '{}': {}", user_id, e);
        }
    }

    pub fn delete_user(&self, user_id: &str) {
        if let Err(e) = self.delete_key(tables::USERS, user_id) {
            error!("Failed to delete user '{}': {}", user_id, e);
        }
    }

    pub fn load_users(&self) -> MetaStoreResult<Vec<(String, StoredUser)>> {
        self.load_bincode_table(tables::USERS)
    }

    // ---- Access Keys (bincode) ----

    pub fn put_access_key(&self, access_key_id: &str, key: &StoredAccessKey) {
        if let Err(e) = self.put_bincode(tables::ACCESS_KEYS, access_key_id, key) {
            error!("Failed to persist access key '{}': {}", access_key_id, e);
        }
    }

    pub fn delete_access_key(&self, access_key_id: &str) {
        if let Err(e) = self.delete_key(tables::ACCESS_KEYS, access_key_id) {
            error!("Failed to delete access key '{}': {}", access_key_id, e);
        }
    }

    pub fn load_access_keys(&self) -> MetaStoreResult<Vec<(String, StoredAccessKey)>> {
        self.load_bincode_table(tables::ACCESS_KEYS)
    }

    // ---- Groups (bincode) ----

    pub fn put_group(&self, group_id: &str, group: &StoredGroup) {
        if let Err(e) = self.put_bincode(tables::GROUPS, group_id, group) {
            error!("Failed to persist group '{}': {}", group_id, e);
        }
    }

    pub fn delete_group(&self, group_id: &str) {
        if let Err(e) = self.delete_key(tables::GROUPS, group_id) {
            error!("Failed to delete group '{}': {}", group_id, e);
        }
    }

    pub fn load_groups(&self) -> MetaStoreResult<Vec<(String, StoredGroup)>> {
        self.load_bincode_table(tables::GROUPS)
    }

    // ---- Ensure admin (batch user + key write) ----

    pub fn put_user_and_key(&self, user: &StoredUser, key: &StoredAccessKey) {
        if let Err(e) = (|| -> MetaStoreResult<()> {
            let user_bytes = bincode::serialize(user)?;
            let key_bytes = bincode::serialize(key)?;
            let write_txn = self.db.begin_write()?;
            {
                let mut t = write_txn.open_table(tables::USERS)?;
                t.insert(user.user_id.as_str(), user_bytes.as_slice())?;
                let mut t2 = write_txn.open_table(tables::ACCESS_KEYS)?;
                t2.insert(key.access_key_id.as_str(), key_bytes.as_slice())?;
            }
            write_txn.commit()?;
            Ok(())
        })() {
            error!("Failed to persist admin user+key '{}': {}", user.user_id, e);
        }
    }

    // ---- Volumes (bincode) ----

    pub fn put_volume(&self, volume_id: &str, volume: &StoredVolume) {
        if let Err(e) = self.put_bincode(tables::VOLUMES, volume_id, volume) {
            error!("Failed to persist volume '{}': {}", volume_id, e);
        }
    }

    pub fn delete_volume(&self, volume_id: &str) {
        if let Err(e) = self.delete_key(tables::VOLUMES, volume_id) {
            error!("Failed to delete volume '{}': {}", volume_id, e);
        }
    }

    pub fn load_volumes(&self) -> MetaStoreResult<Vec<(String, StoredVolume)>> {
        self.load_bincode_table(tables::VOLUMES)
    }

    // ---- Snapshots (bincode) ----

    pub fn put_snapshot(&self, snapshot_id: &str, snapshot: &StoredSnapshot) {
        if let Err(e) = self.put_bincode(tables::SNAPSHOTS, snapshot_id, snapshot) {
            error!("Failed to persist snapshot '{}': {}", snapshot_id, e);
        }
    }

    pub fn delete_snapshot(&self, snapshot_id: &str) {
        if let Err(e) = self.delete_key(tables::SNAPSHOTS, snapshot_id) {
            error!("Failed to delete snapshot '{}': {}", snapshot_id, e);
        }
    }

    pub fn load_snapshots(&self) -> MetaStoreResult<Vec<(String, StoredSnapshot)>> {
        self.load_bincode_table(tables::SNAPSHOTS)
    }

    // ---- Volume Chunks (bincode, composite key "vol_id:chunk_id") ----

    pub fn put_chunk(&self, volume_id: &str, chunk_id: u64, chunk: &StoredChunkRef) {
        let key = format!("{volume_id}:{chunk_id}");
        if let Err(e) = self.put_bincode(tables::VOLUME_CHUNKS, &key, chunk) {
            error!("Failed to persist chunk '{}': {}", key, e);
        }
    }

    pub fn delete_chunks_for_volume(&self, volume_id: &str) {
        if let Err(e) =
            self.delete_keys_with_prefix(tables::VOLUME_CHUNKS, &format!("{volume_id}:"))
        {
            error!("Failed to delete chunks for volume '{}': {}", volume_id, e);
        }
    }

    pub fn delete_chunk(&self, volume_id: &str, chunk_id: u64) {
        let key = format!("{volume_id}:{chunk_id}");
        if let Err(e) = self.delete_key(tables::VOLUME_CHUNKS, &key) {
            error!("Failed to delete chunk '{}': {}", key, e);
        }
    }

    pub fn load_volume_chunks(&self) -> MetaStoreResult<Vec<(String, u64, StoredChunkRef)>> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(tables::VOLUME_CHUNKS)?;
        let mut result = Vec::new();
        for entry in table.iter()? {
            let entry = entry?;
            let composite_key = entry.0.value().to_string();
            let bytes = entry.1.value();
            // Parse "vol_id:chunk_id"
            if let Some((vol_id, chunk_str)) = composite_key.rsplit_once(':')
                && let Ok(chunk_id) = chunk_str.parse::<u64>()
            {
                match bincode::deserialize::<StoredChunkRef>(bytes) {
                    Ok(chunk) => result.push((vol_id.to_string(), chunk_id, chunk)),
                    Err(e) => error!("Failed to decode chunk '{}': {}", composite_key, e),
                }
            }
        }
        Ok(result)
    }

    // ---- Delete volume data ----

    pub fn delete_volume_all(&self, volume_id: &str) {
        if let Err(e) = self.delete_key(tables::VOLUMES, volume_id) {
            error!("Failed to delete volume '{}': {}", volume_id, e);
        }
        if let Err(e) =
            self.delete_keys_with_prefix(tables::VOLUME_CHUNKS, &format!("{volume_id}:"))
        {
            error!("Failed to delete chunks for volume '{}': {}", volume_id, e);
        }
    }

    // ---- Iceberg Namespaces (raw bytes, prost-encoded) ----

    pub fn put_iceberg_namespace(&self, key: &str, data: &[u8]) {
        if let Err(e) = self.put_bytes(tables::ICEBERG_NAMESPACES, key, data) {
            error!("Failed to persist iceberg namespace '{}': {}", key, e);
        }
    }

    pub fn get_iceberg_namespace(&self, key: &str) -> MetaStoreResult<Option<Vec<u8>>> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(tables::ICEBERG_NAMESPACES)?;
        Ok(table.get(key)?.map(|v| v.value().to_vec()))
    }

    pub fn delete_iceberg_namespace(&self, key: &str) {
        if let Err(e) = self.delete_key(tables::ICEBERG_NAMESPACES, key) {
            error!("Failed to delete iceberg namespace '{}': {}", key, e);
        }
    }

    pub fn list_iceberg_namespaces(&self, prefix: &str) -> MetaStoreResult<Vec<(String, Vec<u8>)>> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(tables::ICEBERG_NAMESPACES)?;
        let mut result = Vec::new();
        for entry in table.iter()? {
            let entry = entry?;
            let k = entry.0.value().to_string();
            if prefix.is_empty() || k.starts_with(prefix) {
                result.push((k, entry.1.value().to_vec()));
            }
        }
        Ok(result)
    }

    // ---- Iceberg Tables (raw bytes, prost-encoded) ----

    pub fn put_iceberg_table(&self, key: &str, data: &[u8]) {
        if let Err(e) = self.put_bytes(tables::ICEBERG_TABLES, key, data) {
            error!("Failed to persist iceberg table '{}': {}", key, e);
        }
    }

    pub fn get_iceberg_table(&self, key: &str) -> MetaStoreResult<Option<Vec<u8>>> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(tables::ICEBERG_TABLES)?;
        Ok(table.get(key)?.map(|v| v.value().to_vec()))
    }

    pub fn delete_iceberg_table(&self, key: &str) {
        if let Err(e) = self.delete_key(tables::ICEBERG_TABLES, key) {
            error!("Failed to delete iceberg table '{}': {}", key, e);
        }
    }

    pub fn list_iceberg_tables(
        &self,
        namespace_prefix: &str,
    ) -> MetaStoreResult<Vec<(String, Vec<u8>)>> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(tables::ICEBERG_TABLES)?;
        let mut result = Vec::new();
        for entry in table.iter()? {
            let entry = entry?;
            let k = entry.0.value().to_string();
            if k.starts_with(namespace_prefix) {
                result.push((k, entry.1.value().to_vec()));
            }
        }
        Ok(result)
    }

    /// Compare-and-swap for Iceberg table metadata location.
    /// Returns true if the swap succeeded (current value matched `expected`).
    pub fn cas_iceberg_table(
        &self,
        key: &str,
        expected: &[u8],
        new: &[u8],
    ) -> MetaStoreResult<bool> {
        let write_txn = self.db.begin_write()?;
        let swapped = {
            let mut table = write_txn.open_table(tables::ICEBERG_TABLES)?;
            // Read and compare, then drop the guard before mutating
            let matches = table.get(key)?.is_some_and(|val| val.value() == expected);
            if matches {
                table.insert(key, new)?;
                true
            } else {
                false
            }
        };
        if swapped {
            write_txn.commit()?;
        }
        Ok(swapped)
    }

    /// Atomic multi-key CAS across Iceberg tables in a single redb
    /// write-txn. Either every op's expected value matches and every
    /// write lands, or none of them do. On conflict returns the indices
    /// (positions in `ops`) whose expected value didn't match. On Raft
    /// deployments the MultiCas log command is the canonical path; this
    /// helper exists for single-pod / test builds that don't spin up the
    /// Raft state machine.
    pub fn cas_iceberg_tables_multi(
        &self,
        ops: &[(String, Vec<u8>, Vec<u8>)],
    ) -> MetaStoreResult<Vec<usize>> {
        let write_txn = self.db.begin_write()?;
        let mut failed: Vec<usize> = Vec::new();

        // Pass 1: verify every expected. Each open_table call is scoped
        // to this single write-txn so the read+write see the same state.
        {
            let table = write_txn.open_table(tables::ICEBERG_TABLES)?;
            for (i, (key, expected, _)) in ops.iter().enumerate() {
                let actual = table.get(key.as_str())?.map(|v| v.value().to_vec());
                if actual.as_deref() != Some(expected.as_slice()) {
                    failed.push(i);
                }
            }
        }

        if !failed.is_empty() {
            // Drop the txn without commit → no writes land.
            return Ok(failed);
        }

        // Pass 2: apply every write.
        {
            let mut table = write_txn.open_table(tables::ICEBERG_TABLES)?;
            for (key, _, new) in ops {
                table.insert(key.as_str(), new.as_slice())?;
            }
        }
        write_txn.commit()?;
        Ok(failed)
    }

    // ---- Object listings (prost, Raft-backed) ----

    pub fn read_object_listing(&self, key: &str) -> Option<Vec<u8>> {
        let read_txn = self.db.begin_read().ok()?;
        let table = match read_txn.open_table(tables::OBJECT_LISTINGS) {
            Ok(t) => t,
            Err(redb::TableError::TableDoesNotExist(_)) => return None,
            Err(_) => return None,
        };
        table.get(key).ok()?.map(|v| v.value().to_vec())
    }

    pub fn put_object_listing(&self, key: &str, bytes: &[u8]) {
        if let Err(e) = self.put_bytes(tables::OBJECT_LISTINGS, key, bytes) {
            error!("Failed to persist object listing '{key}': {e}");
        }
    }

    pub fn delete_object_listing(&self, key: &str) {
        if let Err(e) = self.delete_key(tables::OBJECT_LISTINGS, key) {
            error!("Failed to delete object listing '{key}': {e}");
        }
    }


    /// Range scan over OBJECT_LISTINGS for one bucket with an optional
    /// prefix filter. Returns up to `max_keys` entries (bucket/key/ver
    /// strings plus raw prost-encoded ObjectListingEntry bytes) in
    /// key-sorted order, and a continuation token for the next page.
    /// Keys are stored as `{bucket}\0{key}\0{version_id}` so a range
    /// scan starting at `{bucket}\0{prefix}` stops cleanly when the
    /// next bucket begins.
    pub fn list_object_listings(
        &self,
        bucket: &str,
        prefix: &str,
        start_after: &str,
        max_keys: usize,
    ) -> MetaStoreResult<(Vec<(String, Vec<u8>)>, bool, String)> {
        let read_txn = self.db.begin_read()?;
        let table = match read_txn.open_table(tables::OBJECT_LISTINGS) {
            Ok(t) => t,
            Err(redb::TableError::TableDoesNotExist(_)) => {
                return Ok((Vec::new(), false, String::new()));
            }
            Err(e) => return Err(e.into()),
        };

        let bucket_prefix = format!("{bucket}\0");
        let full_prefix = format!("{bucket_prefix}{prefix}");
        let start_exclusive = if start_after.is_empty() {
            None
        } else {
            Some(format!("{bucket_prefix}{start_after}"))
        };

        // Range: [full_prefix, bucket\0\xff) — iterate forward, stop on
        // either max_keys or when the key no longer starts with
        // full_prefix.
        let mut results: Vec<(String, Vec<u8>)> = Vec::with_capacity(max_keys.min(1024));
        let mut is_truncated = false;
        let mut next_token = String::new();

        let iter = table.range(full_prefix.as_str()..)?;
        for entry in iter {
            let (k, v) = entry?;
            let k_str = k.value().to_string();
            if !k_str.starts_with(&full_prefix) {
                break;
            }
            if let Some(ref after) = start_exclusive
                && k_str.as_str() <= after.as_str()
            {
                continue;
            }
            if results.len() >= max_keys {
                is_truncated = true;
                // Strip the bucket prefix from the token for caller
                // convenience — they pass it back as start_after which
                // is in bucket-relative form.
                next_token = results
                    .last()
                    .map(|(k, _)| k[bucket_prefix.len()..].to_string())
                    .unwrap_or_default();
                break;
            }
            results.push((k_str, v.value().to_vec()));
        }
        Ok((results, is_truncated, next_token))
    }

    // ---- Data Filters (bincode) ----

    pub fn put_data_filter(&self, filter_id: &str, filter: &StoredDataFilter) {
        if let Err(e) = self.put_bincode(tables::DATA_FILTERS, filter_id, filter) {
            error!("Failed to persist data filter '{}': {}", filter_id, e);
        }
    }

    pub fn delete_data_filter(&self, filter_id: &str) {
        if let Err(e) = self.delete_key(tables::DATA_FILTERS, filter_id) {
            error!("Failed to delete data filter '{}': {}", filter_id, e);
        }
    }

    pub fn load_data_filters(&self) -> MetaStoreResult<Vec<(String, StoredDataFilter)>> {
        self.load_bincode_table(tables::DATA_FILTERS)
    }

    // ---- Delta Sharing (raw bytes, prost-encoded) ----

    pub fn put_delta_share(&self, name: &str, data: &[u8]) {
        if let Err(e) = self.put_bytes(tables::DELTA_SHARES, name, data) {
            error!("Failed to persist delta share '{}': {}", name, e);
        }
    }

    pub fn delete_delta_share(&self, name: &str) {
        if let Err(e) = self.delete_key(tables::DELTA_SHARES, name) {
            error!("Failed to delete delta share '{}': {}", name, e);
        }
    }

    pub fn load_delta_shares(&self) -> MetaStoreResult<Vec<(String, Vec<u8>)>> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(tables::DELTA_SHARES)?;
        let mut result = Vec::new();
        for entry in table.iter()? {
            let entry = entry?;
            result.push((entry.0.value().to_string(), entry.1.value().to_vec()));
        }
        Ok(result)
    }

    pub fn put_delta_table(&self, key: &str, data: &[u8]) {
        if let Err(e) = self.put_bytes(tables::DELTA_TABLES, key, data) {
            error!("Failed to persist delta table '{}': {}", key, e);
        }
    }

    pub fn delete_delta_table(&self, key: &str) {
        if let Err(e) = self.delete_key(tables::DELTA_TABLES, key) {
            error!("Failed to delete delta table '{}': {}", key, e);
        }
    }

    pub fn delete_delta_tables_for_share(&self, share: &str) {
        let prefix = format!("{share}\x00");
        if let Err(e) = self.delete_keys_with_prefix(tables::DELTA_TABLES, &prefix) {
            error!("Failed to delete delta tables for share '{}': {}", share, e);
        }
    }

    pub fn load_delta_tables(&self) -> MetaStoreResult<Vec<(String, Vec<u8>)>> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(tables::DELTA_TABLES)?;
        let mut result = Vec::new();
        for entry in table.iter()? {
            let entry = entry?;
            result.push((entry.0.value().to_string(), entry.1.value().to_vec()));
        }
        Ok(result)
    }

    pub fn put_delta_recipient(&self, name: &str, data: &[u8]) {
        if let Err(e) = self.put_bytes(tables::DELTA_RECIPIENTS, name, data) {
            error!("Failed to persist delta recipient '{}': {}", name, e);
        }
    }

    pub fn delete_delta_recipient(&self, name: &str) {
        if let Err(e) = self.delete_key(tables::DELTA_RECIPIENTS, name) {
            error!("Failed to delete delta recipient '{}': {}", name, e);
        }
    }

    pub fn load_delta_recipients(&self) -> MetaStoreResult<Vec<(String, Vec<u8>)>> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(tables::DELTA_RECIPIENTS)?;
        let mut result = Vec::new();
        for entry in table.iter()? {
            let entry = entry?;
            result.push((entry.0.value().to_string(), entry.1.value().to_vec()));
        }
        Ok(result)
    }

    // ---- Generic helpers ----

    fn put_bytes(
        &self,
        table_def: redb::TableDefinition<&str, &[u8]>,
        key: &str,
        value: &[u8],
    ) -> MetaStoreResult<()> {
        let write_txn = self.db.begin_write()?;
        {
            let mut table = write_txn.open_table(table_def)?;
            table.insert(key, value)?;
        }
        write_txn.commit()?;
        Ok(())
    }

    fn put_bincode<T: serde::Serialize>(
        &self,
        table_def: redb::TableDefinition<&str, &[u8]>,
        key: &str,
        value: &T,
    ) -> MetaStoreResult<()> {
        let bytes = bincode::serialize(value)?;
        self.put_bytes(table_def, key, &bytes)
    }

    fn delete_key<V: redb::Value>(
        &self,
        table_def: redb::TableDefinition<&str, V>,
        key: &str,
    ) -> MetaStoreResult<()> {
        let write_txn = self.db.begin_write()?;
        {
            let mut table = write_txn.open_table(table_def)?;
            table.remove(key)?;
        }
        write_txn.commit()?;
        Ok(())
    }

    fn load_bincode_table<T: serde::de::DeserializeOwned>(
        &self,
        table_def: redb::TableDefinition<&str, &[u8]>,
    ) -> MetaStoreResult<Vec<(String, T)>> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(table_def)?;
        let mut result = Vec::new();
        for entry in table.iter()? {
            let entry = entry?;
            let key = entry.0.value().to_string();
            let bytes = entry.1.value();
            match bincode::deserialize::<T>(bytes) {
                Ok(val) => result.push((key, val)),
                Err(e) => error!("Failed to decode entry '{}': {}", key, e),
            }
        }
        Ok(result)
    }

    fn delete_keys_with_prefix(
        &self,
        table_def: redb::TableDefinition<&str, &[u8]>,
        prefix: &str,
    ) -> MetaStoreResult<()> {
        // Collect keys first (read txn)
        let keys_to_delete: Vec<String> = {
            let read_txn = self.db.begin_read()?;
            let table = read_txn.open_table(table_def)?;
            let mut keys = Vec::new();
            for entry in table.iter()? {
                let entry = entry?;
                let k = entry.0.value().to_string();
                if k.starts_with(prefix) {
                    keys.push(k);
                }
            }
            keys
        };
        // Delete in write txn
        if !keys_to_delete.is_empty() {
            let write_txn = self.db.begin_write()?;
            {
                let mut table = write_txn.open_table(table_def)?;
                for key in &keys_to_delete {
                    table.remove(key.as_str())?;
                }
            }
            write_txn.commit()?;
        }
        Ok(())
    }

    // ---- Cluster Config (raw bytes, prost-encoded ConfigEntry) ----

    pub fn put_config(&self, key: &str, data: &[u8]) {
        if let Err(e) = self.put_bytes(tables::CONFIG, key, data) {
            error!("Failed to persist config '{}': {}", key, e);
        }
    }

    pub fn delete_config(&self, key: &str) {
        if let Err(e) = self.delete_key(tables::CONFIG, key) {
            error!("Failed to delete config '{}': {}", key, e);
        }
    }

    pub fn load_all_config(&self) -> Vec<(String, Vec<u8>)> {
        let read_txn = match self.db.begin_read() {
            Ok(t) => t,
            Err(e) => {
                error!("Failed to begin read txn for config: {}", e);
                return Vec::new();
            }
        };
        let table = match read_txn.open_table(tables::CONFIG) {
            Ok(t) => t,
            Err(e) => {
                error!("Failed to open config table: {}", e);
                return Vec::new();
            }
        };
        let mut result = Vec::new();
        if let Ok(iter) = table.iter() {
            for entry in iter.flatten() {
                result.push((entry.0.value().to_string(), entry.1.value().to_vec()));
            }
        }
        result
    }

    // ---- Server Pools (prost-encoded PoolConfig) ----

    pub fn put_pool(&self, name: &str, data: &[u8]) {
        if let Err(e) = self.put_bytes(tables::POOLS, name, data) {
            error!("Failed to persist pool '{}': {}", name, e);
        }
    }

    pub fn delete_pool(&self, name: &str) {
        if let Err(e) = self.delete_key(tables::POOLS, name) {
            error!("Failed to delete pool '{}': {}", name, e);
        }
    }

    pub fn load_all_pools(&self) -> Vec<(String, Vec<u8>)> {
        let read_txn = match self.db.begin_read() {
            Ok(t) => t,
            Err(e) => {
                error!("Failed to begin read txn for pools: {}", e);
                return Vec::new();
            }
        };
        let table = match read_txn.open_table(tables::POOLS) {
            Ok(t) => t,
            Err(e) => {
                error!("Failed to open pools table: {}", e);
                return Vec::new();
            }
        };
        let mut result = Vec::new();
        if let Ok(iter) = table.iter() {
            for entry in iter.flatten() {
                result.push((entry.0.value().to_string(), entry.1.value().to_vec()));
            }
        }
        result
    }

    // ---- Tenants (prost-encoded TenantConfig) ----

    pub fn put_tenant(&self, name: &str, data: &[u8]) {
        if let Err(e) = self.put_bytes(tables::TENANTS, name, data) {
            error!("Failed to persist tenant '{}': {}", name, e);
        }
    }

    pub fn delete_tenant(&self, name: &str) {
        if let Err(e) = self.delete_key(tables::TENANTS, name) {
            error!("Failed to delete tenant '{}': {}", name, e);
        }
    }

    pub fn load_all_tenants(&self) -> Vec<(String, Vec<u8>)> {
        let read_txn = match self.db.begin_read() {
            Ok(t) => t,
            Err(e) => {
                error!("Failed to begin read txn for tenants: {}", e);
                return Vec::new();
            }
        };
        let table = match read_txn.open_table(tables::TENANTS) {
            Ok(t) => t,
            Err(e) => {
                error!("Failed to open tenants table: {}", e);
                return Vec::new();
            }
        };
        let mut result = Vec::new();
        if let Ok(iter) = table.iter() {
            for entry in iter.flatten() {
                result.push((entry.0.value().to_string(), entry.1.value().to_vec()));
            }
        }
        result
    }

    // ---- IAM Policies (prost-encoded) ----

    pub fn put_iam_policy(&self, name: &str, data: &[u8]) {
        if let Err(e) = self.put_bytes(tables::IAM_POLICIES, name, data) {
            error!("Failed to persist IAM policy '{}': {}", name, e);
        }
    }

    pub fn delete_iam_policy(&self, name: &str) {
        if let Err(e) = self.delete_key(tables::IAM_POLICIES, name) {
            error!("Failed to delete IAM policy '{}': {}", name, e);
        }
    }

    pub fn load_all_iam_policies(&self) -> Vec<(String, Vec<u8>)> {
        let read_txn = match self.db.begin_read() {
            Ok(t) => t,
            Err(e) => {
                error!("Failed to begin read txn for IAM policies: {}", e);
                return Vec::new();
            }
        };
        let table = match read_txn.open_table(tables::IAM_POLICIES) {
            Ok(t) => t,
            Err(e) => {
                error!("Failed to open IAM policies table: {}", e);
                return Vec::new();
            }
        };
        let mut result = Vec::new();
        if let Ok(iter) = table.iter() {
            for entry in iter.flatten() {
                result.push((entry.0.value().to_string(), entry.1.value().to_vec()));
            }
        }
        result
    }

    // ---- Policy Attachments (string values) ----

    pub fn put_policy_attachment(&self, key: &str, policies: &str) {
        if let Err(e) = (|| -> MetaStoreResult<()> {
            let write_txn = self.db.begin_write()?;
            {
                let mut table = write_txn.open_table(tables::POLICY_ATTACHMENTS)?;
                table.insert(key, policies)?;
            }
            write_txn.commit()?;
            Ok(())
        })() {
            error!("Failed to persist policy attachment '{}': {}", key, e);
        }
    }

    pub fn delete_policy_attachment(&self, key: &str) {
        if let Err(e) = (|| -> MetaStoreResult<()> {
            let write_txn = self.db.begin_write()?;
            {
                let mut table = write_txn.open_table(tables::POLICY_ATTACHMENTS)?;
                table.remove(key)?;
            }
            write_txn.commit()?;
            Ok(())
        })() {
            error!("Failed to delete policy attachment '{}': {}", key, e);
        }
    }

    pub fn load_all_policy_attachments(&self) -> Vec<(String, String)> {
        let read_txn = match self.db.begin_read() {
            Ok(t) => t,
            Err(e) => {
                error!("Failed to begin read txn for policy attachments: {}", e);
                return Vec::new();
            }
        };
        let table = match read_txn.open_table(tables::POLICY_ATTACHMENTS) {
            Ok(t) => t,
            Err(e) => {
                error!("Failed to open policy attachments table: {}", e);
                return Vec::new();
            }
        };
        let mut result = Vec::new();
        if let Ok(iter) = table.iter() {
            for entry in iter.flatten() {
                result.push((entry.0.value().to_string(), entry.1.value().to_string()));
            }
        }
        result
    }

    // ---- Iceberg Warehouses (prost-encoded) ----

    pub fn put_warehouse(&self, name: &str, data: &[u8]) {
        if let Err(e) = self.put_bytes(tables::ICEBERG_WAREHOUSES, name, data) {
            error!("Failed to persist warehouse '{}': {}", name, e);
        }
    }

    pub fn delete_warehouse(&self, name: &str) {
        if let Err(e) = self.delete_key(tables::ICEBERG_WAREHOUSES, name) {
            error!("Failed to delete warehouse '{}': {}", name, e);
        }
    }

    pub fn load_all_warehouses(&self) -> Vec<(String, Vec<u8>)> {
        let read_txn = match self.db.begin_read() {
            Ok(t) => t,
            Err(e) => {
                error!("Failed to begin read txn for warehouses: {}", e);
                return Vec::new();
            }
        };
        let table = match read_txn.open_table(tables::ICEBERG_WAREHOUSES) {
            Ok(t) => t,
            Err(e) => {
                error!("Failed to open warehouses table: {}", e);
                return Vec::new();
            }
        };
        let mut result = Vec::new();
        if let Ok(iter) = table.iter() {
            for entry in iter.flatten() {
                result.push((entry.0.value().to_string(), entry.1.value().to_vec()));
            }
        }
        result
    }

    // ---- Object Lock Configurations (prost-encoded) ----

    pub fn put_object_lock_config(&self, bucket: &str, data: &[u8]) {
        if let Err(e) = self.put_bytes(tables::OBJECT_LOCK_CONFIGS, bucket, data) {
            error!(
                "Failed to persist object lock config for '{}': {}",
                bucket, e
            );
        }
    }

    pub fn delete_object_lock_config(&self, bucket: &str) {
        if let Err(e) = self.delete_key(tables::OBJECT_LOCK_CONFIGS, bucket) {
            error!(
                "Failed to delete object lock config for '{}': {}",
                bucket, e
            );
        }
    }

    pub fn load_all_object_lock_configs(&self) -> Vec<(String, Vec<u8>)> {
        let read_txn = match self.db.begin_read() {
            Ok(t) => t,
            Err(e) => {
                error!("Failed to begin read txn for object lock configs: {}", e);
                return Vec::new();
            }
        };
        let table = match read_txn.open_table(tables::OBJECT_LOCK_CONFIGS) {
            Ok(t) => t,
            Err(e) => {
                error!("Failed to open object lock configs table: {}", e);
                return Vec::new();
            }
        };
        let mut result = Vec::new();
        if let Ok(iter) = table.iter() {
            for entry in iter.flatten() {
                result.push((entry.0.value().to_string(), entry.1.value().to_vec()));
            }
        }
        result
    }

    // ---- Lifecycle Configurations (prost-encoded) ----

    pub fn put_lifecycle_config(&self, bucket: &str, data: &[u8]) {
        if let Err(e) = self.put_bytes(tables::LIFECYCLE_CONFIGS, bucket, data) {
            error!("Failed to persist lifecycle config for '{}': {}", bucket, e);
        }
    }

    pub fn delete_lifecycle_config(&self, bucket: &str) {
        if let Err(e) = self.delete_key(tables::LIFECYCLE_CONFIGS, bucket) {
            error!("Failed to delete lifecycle config for '{}': {}", bucket, e);
        }
    }

    pub fn load_all_lifecycle_configs(&self) -> Vec<(String, Vec<u8>)> {
        let read_txn = match self.db.begin_read() {
            Ok(t) => t,
            Err(e) => {
                error!("Failed to begin read txn for lifecycle configs: {}", e);
                return Vec::new();
            }
        };
        let table = match read_txn.open_table(tables::LIFECYCLE_CONFIGS) {
            Ok(t) => t,
            Err(e) => {
                error!("Failed to open lifecycle configs table: {}", e);
                return Vec::new();
            }
        };
        let mut result = Vec::new();
        if let Ok(iter) = table.iter() {
            for entry in iter.flatten() {
                result.push((entry.0.value().to_string(), entry.1.value().to_vec()));
            }
        }
        result
    }

    // ---- Bucket default SSE configurations (prost-encoded) ----

    pub fn put_bucket_encryption_config(&self, bucket: &str, data: &[u8]) {
        if let Err(e) = self.put_bytes(tables::BUCKET_ENCRYPTION_CONFIGS, bucket, data) {
            error!(
                "Failed to persist bucket encryption config for '{}': {}",
                bucket, e
            );
        }
    }

    pub fn delete_bucket_encryption_config(&self, bucket: &str) {
        if let Err(e) = self.delete_key(tables::BUCKET_ENCRYPTION_CONFIGS, bucket) {
            error!(
                "Failed to delete bucket encryption config for '{}': {}",
                bucket, e
            );
        }
    }

    pub fn load_all_bucket_encryption_configs(&self) -> Vec<(String, Vec<u8>)> {
        let read_txn = match self.db.begin_read() {
            Ok(t) => t,
            Err(e) => {
                error!(
                    "Failed to begin read txn for bucket encryption configs: {}",
                    e
                );
                return Vec::new();
            }
        };
        let table = match read_txn.open_table(tables::BUCKET_ENCRYPTION_CONFIGS) {
            Ok(t) => t,
            Err(e) => {
                error!("Failed to open bucket encryption configs table: {}", e);
                return Vec::new();
            }
        };
        let mut result = Vec::new();
        if let Ok(iter) = table.iter() {
            for entry in iter.flatten() {
                result.push((entry.0.value().to_string(), entry.1.value().to_vec()));
            }
        }
        result
    }

    // ---- KMS keys (prost-encoded KmsKey; key material already wrapped) ----

    pub fn put_kms_key(&self, key_id: &str, data: &[u8]) {
        if let Err(e) = self.put_bytes(tables::KMS_KEYS, key_id, data) {
            error!("Failed to persist KMS key '{}': {}", key_id, e);
        }
    }

    pub fn delete_kms_key(&self, key_id: &str) {
        if let Err(e) = self.delete_key(tables::KMS_KEYS, key_id) {
            error!("Failed to delete KMS key '{}': {}", key_id, e);
        }
    }

    pub fn load_all_kms_keys(&self) -> Vec<(String, Vec<u8>)> {
        let read_txn = match self.db.begin_read() {
            Ok(t) => t,
            Err(e) => {
                error!("Failed to begin read txn for KMS keys: {}", e);
                return Vec::new();
            }
        };
        let table = match read_txn.open_table(tables::KMS_KEYS) {
            Ok(t) => t,
            Err(e) => {
                error!("Failed to open KMS keys table: {}", e);
                return Vec::new();
            }
        };
        let mut result = Vec::new();
        if let Ok(iter) = table.iter() {
            for entry in iter.flatten() {
                result.push((entry.0.value().to_string(), entry.1.value().to_vec()));
            }
        }
        result
    }
}
