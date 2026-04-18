//! In-memory B-tree index with snapshot persistence
//!
//! Uses Rust's BTreeMap for the core index, with:
//! - Efficient point lookups and range scans
//! - Periodic snapshots to disk
//! - Fast recovery from snapshot + WAL replay

use super::types::{MetadataEntry, MetadataKey, SnapshotHeader};
use objectio_common::{Error, Result};
use parking_lot::RwLock;
use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

/// B-tree index configuration
#[derive(Clone, Debug)]
pub struct BTreeConfig {
    /// Snapshot directory
    pub snapshot_dir: PathBuf,
    /// Minimum entries between snapshots
    pub snapshot_threshold: u64,
    /// Keep this many old snapshots
    pub snapshot_retention: usize,
}

impl Default for BTreeConfig {
    fn default() -> Self {
        Self {
            snapshot_dir: PathBuf::from("."),
            snapshot_threshold: 10000,
            snapshot_retention: 2,
        }
    }
}

/// In-memory B-tree index
pub struct BTreeIndex {
    /// The actual B-tree
    tree: RwLock<BTreeMap<MetadataKey, StoredValue>>,
    /// Current LSN
    lsn: AtomicU64,
    /// Entry count
    entry_count: AtomicU64,
    /// Mutations since last snapshot
    mutations_since_snapshot: AtomicU64,
    /// Configuration
    config: BTreeConfig,
    /// Last snapshot LSN
    last_snapshot_lsn: AtomicU64,
}

/// Value stored in the B-tree
#[derive(Clone, Debug)]
struct StoredValue {
    /// Actual value bytes
    data: Vec<u8>,
    /// LSN when this value was written
    lsn: u64,
}

impl BTreeIndex {
    /// Create a new empty index
    pub fn new(config: BTreeConfig) -> Self {
        Self {
            tree: RwLock::new(BTreeMap::new()),
            lsn: AtomicU64::new(0),
            entry_count: AtomicU64::new(0),
            mutations_since_snapshot: AtomicU64::new(0),
            config,
            last_snapshot_lsn: AtomicU64::new(0),
        }
    }

    /// Load index from the latest snapshot
    pub fn load_snapshot(config: BTreeConfig) -> Result<Self> {
        let snapshot_path = Self::find_latest_snapshot(&config.snapshot_dir)?;

        match snapshot_path {
            Some(path) => {
                let (tree, header) = Self::read_snapshot(&path)?;
                Ok(Self {
                    entry_count: AtomicU64::new(tree.len() as u64),
                    tree: RwLock::new(tree),
                    lsn: AtomicU64::new(header.lsn),
                    mutations_since_snapshot: AtomicU64::new(0),
                    last_snapshot_lsn: AtomicU64::new(header.lsn),
                    config,
                })
            }
            None => Ok(Self::new(config)),
        }
    }

    /// Find the latest snapshot file
    fn find_latest_snapshot(dir: &Path) -> Result<Option<PathBuf>> {
        if !dir.exists() {
            return Ok(None);
        }

        let mut snapshots: Vec<(PathBuf, u64)> = std::fs::read_dir(dir)
            .map_err(|e| Error::Storage(format!("failed to read snapshot dir: {}", e)))?
            .filter_map(|entry| entry.ok())
            .filter(|entry| {
                entry
                    .path()
                    .extension()
                    .map(|ext| ext == "snapshot")
                    .unwrap_or(false)
            })
            .filter_map(|entry| {
                // Extract LSN from filename: meta_<lsn>.snapshot
                let name = entry.file_name();
                let name_str = name.to_string_lossy();
                if let Some(lsn_str) = name_str
                    .strip_prefix("meta_")
                    .and_then(|s| s.strip_suffix(".snapshot"))
                {
                    lsn_str.parse::<u64>().ok().map(|lsn| (entry.path(), lsn))
                } else {
                    None
                }
            })
            .collect();

        snapshots.sort_by_key(|(_, lsn)| std::cmp::Reverse(*lsn));
        Ok(snapshots.into_iter().next().map(|(path, _)| path))
    }

    /// Read a snapshot file
    fn read_snapshot(path: &Path) -> Result<(BTreeMap<MetadataKey, StoredValue>, SnapshotHeader)> {
        let file = File::open(path)
            .map_err(|e| Error::Storage(format!("failed to open snapshot: {}", e)))?;

        let mut reader = BufReader::new(file);

        // Read header
        let mut header_buf = [0u8; SnapshotHeader::SIZE];
        reader
            .read_exact(&mut header_buf)
            .map_err(|e| Error::Storage(format!("failed to read snapshot header: {}", e)))?;

        let header = SnapshotHeader::from_bytes(&header_buf)
            .ok_or_else(|| Error::Storage("invalid snapshot header".into()))?;

        // Read entries
        let mut tree = BTreeMap::new();
        let mut data_buf = Vec::new();
        reader
            .read_to_end(&mut data_buf)
            .map_err(|e| Error::Storage(format!("failed to read snapshot data: {}", e)))?;

        // Verify checksum
        let computed_crc = crc32c::crc32c(&data_buf);
        if computed_crc != header.checksum {
            return Err(Error::Storage("snapshot checksum mismatch".into()));
        }

        // Deserialize entries
        let entries: Vec<(MetadataKey, Vec<u8>, u64)> = bincode::deserialize(&data_buf)
            .map_err(|e| Error::Storage(format!("failed to deserialize snapshot: {}", e)))?;

        for (key, data, lsn) in entries {
            tree.insert(key, StoredValue { data, lsn });
        }

        Ok((tree, header))
    }

    /// Insert or update a key-value pair
    pub fn put(&self, key: MetadataKey, value: Vec<u8>, lsn: u64) {
        let mut tree = self.tree.write();
        let is_new = !tree.contains_key(&key);
        tree.insert(key, StoredValue { data: value, lsn });

        if is_new {
            self.entry_count.fetch_add(1, Ordering::Relaxed);
        }
        self.update_lsn(lsn);
        self.mutations_since_snapshot
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Delete a key
    pub fn delete(&self, key: &MetadataKey, lsn: u64) -> bool {
        let mut tree = self.tree.write();
        let removed = tree.remove(key).is_some();

        if removed {
            self.entry_count.fetch_sub(1, Ordering::Relaxed);
        }
        self.update_lsn(lsn);
        self.mutations_since_snapshot
            .fetch_add(1, Ordering::Relaxed);

        removed
    }

    /// Get a value by key
    pub fn get(&self, key: &MetadataKey) -> Option<Vec<u8>> {
        let tree = self.tree.read();
        tree.get(key).map(|v| v.data.clone())
    }

    /// Check if a key exists
    pub fn contains(&self, key: &MetadataKey) -> bool {
        let tree = self.tree.read();
        tree.contains_key(key)
    }

    /// Range scan with prefix
    pub fn scan_prefix(&self, prefix: &MetadataKey) -> Vec<(MetadataKey, Vec<u8>)> {
        let tree = self.tree.read();

        let start = Bound::Included(prefix.clone());
        let mut end_bytes = prefix.0.clone();

        // Increment last byte to get exclusive end bound
        // Handle overflow by extending the range
        let end = if let Some(last) = end_bytes.last_mut() {
            if *last < 255 {
                *last += 1;
                Bound::Excluded(MetadataKey(end_bytes))
            } else {
                Bound::Unbounded
            }
        } else {
            Bound::Unbounded
        };

        tree.range((start, end))
            .filter(|(k, _)| k.as_bytes().starts_with(prefix.as_bytes()))
            .map(|(k, v)| (k.clone(), v.data.clone()))
            .collect()
    }

    /// Range scan between two keys
    #[allow(dead_code)]
    pub fn scan_range(
        &self,
        start: &MetadataKey,
        end: &MetadataKey,
        limit: usize,
    ) -> Vec<(MetadataKey, Vec<u8>)> {
        let tree = self.tree.read();

        tree.range(start..end)
            .take(limit)
            .map(|(k, v)| (k.clone(), v.data.clone()))
            .collect()
    }

    /// Apply a batch of entries from WAL replay
    pub fn apply_entry(&self, entry: MetadataEntry) {
        if entry.deleted {
            self.delete(&entry.key, entry.lsn);
        } else {
            self.put(entry.key, entry.value, entry.lsn);
        }
    }

    /// Write a snapshot to disk
    pub fn write_snapshot(&self) -> Result<PathBuf> {
        let lsn = self.lsn.load(Ordering::SeqCst);
        let tree = self.tree.read();

        // Prepare entries for serialization
        let entries: Vec<(MetadataKey, Vec<u8>, u64)> = tree
            .iter()
            .map(|(k, v)| (k.clone(), v.data.clone(), v.lsn))
            .collect();

        let entry_count = entries.len() as u64;

        // Serialize entries
        let data = bincode::serialize(&entries)
            .map_err(|e| Error::Storage(format!("failed to serialize snapshot: {}", e)))?;

        drop(tree);

        // Compute checksum
        let checksum = crc32c::crc32c(&data);

        // Create header
        let mut header = SnapshotHeader::new(lsn, entry_count);
        header.checksum = checksum;

        // Ensure directory exists
        std::fs::create_dir_all(&self.config.snapshot_dir)
            .map_err(|e| Error::Storage(format!("failed to create snapshot dir: {}", e)))?;

        // Write to temporary file first
        let snapshot_name = format!("meta_{}.snapshot", lsn);
        let snapshot_path = self.config.snapshot_dir.join(&snapshot_name);
        let temp_path = self
            .config
            .snapshot_dir
            .join(format!("{}.tmp", snapshot_name));

        {
            let file = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&temp_path)
                .map_err(|e| Error::Storage(format!("failed to create snapshot file: {}", e)))?;

            let mut writer = BufWriter::new(file);

            // Write header
            writer
                .write_all(&header.to_bytes())
                .map_err(|e| Error::Storage(format!("failed to write snapshot header: {}", e)))?;

            // Write data
            writer
                .write_all(&data)
                .map_err(|e| Error::Storage(format!("failed to write snapshot data: {}", e)))?;

            writer
                .flush()
                .map_err(|e| Error::Storage(format!("failed to flush snapshot: {}", e)))?;

            writer
                .get_ref()
                .sync_all()
                .map_err(|e| Error::Storage(format!("failed to sync snapshot: {}", e)))?;
        }

        // Atomic rename
        std::fs::rename(&temp_path, &snapshot_path)
            .map_err(|e| Error::Storage(format!("failed to rename snapshot: {}", e)))?;

        // Update tracking
        self.last_snapshot_lsn.store(lsn, Ordering::SeqCst);
        self.mutations_since_snapshot.store(0, Ordering::Relaxed);

        // Cleanup old snapshots
        self.cleanup_old_snapshots()?;

        Ok(snapshot_path)
    }

    /// Clean up old snapshot files
    fn cleanup_old_snapshots(&self) -> Result<()> {
        let mut snapshots: Vec<(PathBuf, u64)> = std::fs::read_dir(&self.config.snapshot_dir)
            .map_err(|e| Error::Storage(format!("failed to read snapshot dir: {}", e)))?
            .filter_map(|entry| entry.ok())
            .filter(|entry| {
                entry
                    .path()
                    .extension()
                    .map(|ext| ext == "snapshot")
                    .unwrap_or(false)
            })
            .filter_map(|entry| {
                let name = entry.file_name();
                let name_str = name.to_string_lossy();
                if let Some(lsn_str) = name_str
                    .strip_prefix("meta_")
                    .and_then(|s| s.strip_suffix(".snapshot"))
                {
                    lsn_str.parse::<u64>().ok().map(|lsn| (entry.path(), lsn))
                } else {
                    None
                }
            })
            .collect();

        // Sort by LSN descending (newest first)
        snapshots.sort_by_key(|(_, lsn)| std::cmp::Reverse(*lsn));

        // Remove old snapshots beyond retention
        for (path, _) in snapshots.into_iter().skip(self.config.snapshot_retention) {
            let _ = std::fs::remove_file(path);
        }

        Ok(())
    }

    /// Check if snapshot is needed
    pub fn needs_snapshot(&self) -> bool {
        self.mutations_since_snapshot.load(Ordering::Relaxed) >= self.config.snapshot_threshold
    }

    /// Get current LSN
    pub fn current_lsn(&self) -> u64 {
        self.lsn.load(Ordering::SeqCst)
    }

    /// Get last snapshot LSN
    pub fn last_snapshot_lsn(&self) -> u64 {
        self.last_snapshot_lsn.load(Ordering::SeqCst)
    }

    /// Get entry count
    pub fn len(&self) -> u64 {
        self.entry_count.load(Ordering::Relaxed)
    }

    /// Check if empty
    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Update LSN if higher
    fn update_lsn(&self, lsn: u64) {
        let _ = self
            .lsn
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |current| {
                if lsn > current { Some(lsn) } else { None }
            });
    }

    /// Get all entries (for debugging/testing)
    #[cfg(test)]
    #[allow(dead_code)]
    pub fn entries(&self) -> Vec<(MetadataKey, Vec<u8>)> {
        let tree = self.tree.read();
        tree.iter()
            .map(|(k, v)| (k.clone(), v.data.clone()))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_btree_put_get() {
        let config = BTreeConfig::default();
        let index = BTreeIndex::new(config);

        let key = MetadataKey::block(42);
        index.put(key.clone(), b"test value".to_vec(), 1);

        let value = index.get(&key).unwrap();
        assert_eq!(value, b"test value");
    }

    #[test]
    fn test_btree_delete() {
        let config = BTreeConfig::default();
        let index = BTreeIndex::new(config);

        let key = MetadataKey::block(42);
        index.put(key.clone(), b"test value".to_vec(), 1);

        assert!(index.delete(&key, 2));
        assert!(index.get(&key).is_none());
    }

    #[test]
    fn test_btree_scan_prefix() {
        let config = BTreeConfig::default();
        let index = BTreeIndex::new(config);

        // Insert some block metadata
        for i in 1..=5 {
            index.put(
                MetadataKey::block(i),
                format!("block_{}", i).into_bytes(),
                i,
            );
        }

        // Insert some object metadata
        index.put(MetadataKey::object(&[1u8; 16]), b"object_1".to_vec(), 10);

        // Scan for blocks (prefix 'b')
        let block_prefix = MetadataKey(vec![b'b']);
        let blocks = index.scan_prefix(&block_prefix);
        assert_eq!(blocks.len(), 5);
    }

    #[test]
    fn test_btree_snapshot_roundtrip() {
        let dir = tempdir().unwrap();
        let config = BTreeConfig {
            snapshot_dir: dir.path().to_path_buf(),
            snapshot_threshold: 10,
            snapshot_retention: 2,
        };

        // Create and populate index
        let index = BTreeIndex::new(config.clone());
        for i in 1..=100 {
            index.put(
                MetadataKey::block(i),
                format!("value_{}", i).into_bytes(),
                i,
            );
        }

        // Write snapshot
        let snapshot_path = index.write_snapshot().unwrap();
        assert!(snapshot_path.exists());

        // Load snapshot
        let loaded = BTreeIndex::load_snapshot(config).unwrap();
        assert_eq!(loaded.len(), 100);

        // Verify values
        for i in 1..=100 {
            let value = loaded.get(&MetadataKey::block(i)).unwrap();
            assert_eq!(value, format!("value_{}", i).into_bytes());
        }
    }

    #[test]
    fn test_btree_entry_count() {
        let config = BTreeConfig::default();
        let index = BTreeIndex::new(config);

        assert_eq!(index.len(), 0);

        index.put(MetadataKey::block(1), b"v1".to_vec(), 1);
        assert_eq!(index.len(), 1);

        index.put(MetadataKey::block(2), b"v2".to_vec(), 2);
        assert_eq!(index.len(), 2);

        // Update existing key - count shouldn't change
        index.put(MetadataKey::block(1), b"v1_updated".to_vec(), 3);
        assert_eq!(index.len(), 2);

        index.delete(&MetadataKey::block(1), 4);
        assert_eq!(index.len(), 1);
    }
}
