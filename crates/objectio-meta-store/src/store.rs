//! Persistent metadata store backed by redb.
//!
//! Provides typed put/get/delete/load methods for each table. All writes
//! are synchronous (write txn + commit). Reads go through the in-memory
//! HashMap cache in the service layer — this module only handles persistence.

use crate::tables;
use crate::types::{
    MultipartUploadState, OsdNode, StoredAccessKey, StoredChunkRef, StoredSnapshot, StoredUser,
    StoredVolume,
};
use objectio_proto::metadata::BucketMeta;
use prost::Message;
use redb::{Database, ReadableTable};
use std::path::Path;
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
pub struct MetaStore {
    db: Database,
}

impl MetaStore {
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
        }
        write_txn.commit()?;

        Ok(Self { db })
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
}
