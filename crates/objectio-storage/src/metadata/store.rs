//! Unified Metadata Store
//!
//! Combines WAL, B-tree index, and ARC cache into a single interface
//! with background compaction.

use super::btree::{BTreeConfig, BTreeIndex};
use super::cache::ArcCache;
use super::types::{MetadataKey, MetadataOp, ShardMeta};
use super::wal::{MetadataWal, WalConfig};
use objectio_common::{Error, Result};
use parking_lot::Mutex;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use tracing::{debug, error, info, warn};

/// Metadata store configuration
#[derive(Clone, Debug)]
pub struct MetadataStoreConfig {
    /// Base directory for metadata files
    pub data_dir: PathBuf,
    /// WAL configuration
    pub wal: WalConfig,
    /// B-tree configuration
    pub btree: BTreeConfig,
    /// Cache size (number of entries)
    pub cache_size: usize,
    /// Enable background compaction
    pub background_compaction: bool,
    /// Compaction check interval
    pub compaction_interval: Duration,
}

impl Default for MetadataStoreConfig {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("./metadata"),
            wal: WalConfig::default(),
            btree: BTreeConfig::default(),
            cache_size: 10000,
            background_compaction: true,
            compaction_interval: Duration::from_secs(60),
        }
    }
}

impl MetadataStoreConfig {
    /// Create config with data directory
    pub fn with_data_dir(data_dir: impl AsRef<Path>) -> Self {
        let data_dir = data_dir.as_ref().to_path_buf();
        Self {
            btree: BTreeConfig {
                snapshot_dir: data_dir.join("snapshots"),
                ..Default::default()
            },
            data_dir,
            ..Default::default()
        }
    }
}

/// Unified metadata store for OSD
pub struct MetadataStore {
    /// Write-ahead log
    wal: Arc<MetadataWal>,
    /// B-tree index
    index: Arc<BTreeIndex>,
    /// ARC cache
    cache: Arc<ArcCache>,
    /// Configuration
    config: MetadataStoreConfig,
    /// Compaction lock
    compaction_lock: Mutex<()>,
    /// Shutdown flag for background thread
    shutdown: Arc<AtomicBool>,
    /// Background compaction handle
    compaction_handle: Mutex<Option<thread::JoinHandle<()>>>,
}

impl MetadataStore {
    /// Create a new metadata store
    pub fn create(config: MetadataStoreConfig) -> Result<Self> {
        // Ensure directories exist
        std::fs::create_dir_all(&config.data_dir)
            .map_err(|e| Error::Storage(format!("failed to create data dir: {}", e)))?;
        std::fs::create_dir_all(&config.btree.snapshot_dir)
            .map_err(|e| Error::Storage(format!("failed to create snapshot dir: {}", e)))?;

        // Create WAL
        let wal_path = config.data_dir.join("metadata.wal");
        let wal = Arc::new(MetadataWal::create(&wal_path, config.wal.clone())?);

        // Create B-tree index
        let index = Arc::new(BTreeIndex::new(config.btree.clone()));

        // Create cache
        let cache = Arc::new(ArcCache::new(config.cache_size));

        let store = Self {
            wal,
            index,
            cache,
            config,
            compaction_lock: Mutex::new(()),
            shutdown: Arc::new(AtomicBool::new(false)),
            compaction_handle: Mutex::new(None),
        };

        // Start background compaction if enabled
        if store.config.background_compaction {
            store.start_background_compaction();
        }

        info!("Created new metadata store at {:?}", store.config.data_dir);
        Ok(store)
    }

    /// Open an existing metadata store
    pub fn open(config: MetadataStoreConfig) -> Result<Self> {
        // Open or create WAL
        let wal_path = config.data_dir.join("metadata.wal");
        let wal = Arc::new(if wal_path.exists() {
            MetadataWal::open(&wal_path, config.wal.clone())?
        } else {
            MetadataWal::create(&wal_path, config.wal.clone())?
        });

        // Load B-tree from snapshot
        let index = Arc::new(BTreeIndex::load_snapshot(config.btree.clone())?);
        let snapshot_lsn = index.last_snapshot_lsn();

        // Replay WAL entries after snapshot
        info!("Replaying WAL from LSN {}", snapshot_lsn + 1);
        let mut replay_count = 0u64;

        wal.iter_entries(snapshot_lsn + 1, |entry| {
            index.apply_entry(entry);
            replay_count += 1;
            Ok(())
        })?;

        info!("Replayed {} WAL entries", replay_count);

        // Create cache
        let cache = Arc::new(ArcCache::new(config.cache_size));

        let store = Self {
            wal,
            index,
            cache,
            config,
            compaction_lock: Mutex::new(()),
            shutdown: Arc::new(AtomicBool::new(false)),
            compaction_handle: Mutex::new(None),
        };

        // Start background compaction if enabled
        if store.config.background_compaction {
            store.start_background_compaction();
        }

        info!(
            "Opened metadata store at {:?} ({} entries)",
            store.config.data_dir,
            store.index.len()
        );
        Ok(store)
    }

    /// Open or create a metadata store
    pub fn open_or_create(config: MetadataStoreConfig) -> Result<Self> {
        if config.data_dir.join("metadata.wal").exists()
            || config.btree.snapshot_dir.exists()
        {
            Self::open(config)
        } else {
            Self::create(config)
        }
    }

    /// Put a key-value pair
    pub fn put(&self, key: MetadataKey, value: Vec<u8>) -> Result<u64> {
        // 1. Write to WAL
        let op = MetadataOp::Put {
            key: key.clone(),
            value: value.clone(),
        };
        let lsn = self.wal.append(&op)?;

        // 2. Update index
        self.index.put(key.clone(), value.clone(), lsn);

        // 3. Update cache
        self.cache.put(key, value);

        debug!("put: lsn={}", lsn);
        Ok(lsn)
    }

    /// Put shard metadata
    pub fn put_shard(&self, meta: &ShardMeta) -> Result<u64> {
        let key = MetadataKey::shard(&meta.object_id, meta.shard_position);
        let value = meta.to_bytes();
        self.put(key, value)
    }

    /// Delete a key
    pub fn delete(&self, key: &MetadataKey) -> Result<u64> {
        // 1. Write to WAL
        let op = MetadataOp::Delete { key: key.clone() };
        let lsn = self.wal.append(&op)?;

        // 2. Update index
        self.index.delete(key, lsn);

        // 3. Remove from cache
        self.cache.remove(key);

        debug!("delete: lsn={}", lsn);
        Ok(lsn)
    }

    /// Get a value by key
    pub fn get(&self, key: &MetadataKey) -> Option<Vec<u8>> {
        // 1. Check cache
        if let Some(value) = self.cache.get(key) {
            return Some(value);
        }

        // 2. Check index
        if let Some(value) = self.index.get(key) {
            // Populate cache
            self.cache.put(key.clone(), value.clone());
            return Some(value);
        }

        None
    }

    /// Get shard metadata
    pub fn get_shard(&self, object_id: &[u8; 16], shard_position: u8) -> Option<ShardMeta> {
        let key = MetadataKey::shard(object_id, shard_position);
        self.get(&key).and_then(|v| ShardMeta::from_bytes(&v))
    }

    /// Check if a key exists
    pub fn contains(&self, key: &MetadataKey) -> bool {
        self.cache.contains(key) || self.index.contains(key)
    }

    /// Batch write operations
    pub fn batch_put(&self, entries: Vec<(MetadataKey, Vec<u8>)>) -> Result<u64> {
        if entries.is_empty() {
            return Ok(self.wal.current_lsn());
        }

        // Build batch operation
        let ops: Vec<MetadataOp> = entries
            .iter()
            .map(|(k, v)| MetadataOp::Put {
                key: k.clone(),
                value: v.clone(),
            })
            .collect();

        // 1. Write to WAL atomically
        let lsn = self.wal.append_batch(&ops)?;

        // 2. Update index
        for (key, value) in &entries {
            self.index.put(key.clone(), value.clone(), lsn);
        }

        // 3. Update cache
        for (key, value) in entries {
            self.cache.put(key, value);
        }

        debug!("batch_put: {} entries, lsn={}", ops.len(), lsn);
        Ok(lsn)
    }

    /// Scan entries with a key prefix
    pub fn scan_prefix(&self, prefix: &MetadataKey) -> Vec<(MetadataKey, Vec<u8>)> {
        self.index.scan_prefix(prefix)
    }

    /// Scan shards for an object
    pub fn scan_object_shards(&self, object_id: &[u8; 16]) -> Vec<ShardMeta> {
        // Prefix is 's' + object_id (17 bytes total)
        let mut prefix_bytes = vec![b's'];
        prefix_bytes.extend_from_slice(object_id);
        let prefix_key = MetadataKey::from_bytes(prefix_bytes);

        self.index
            .scan_prefix(&prefix_key)
            .iter()
            .filter_map(|(_, v)| ShardMeta::from_bytes(v))
            .collect()
    }

    /// Force a snapshot to disk
    pub fn snapshot(&self) -> Result<PathBuf> {
        let _lock = self.compaction_lock.lock();
        self.do_snapshot()
    }

    /// Internal snapshot implementation
    fn do_snapshot(&self) -> Result<PathBuf> {
        // Write snapshot
        let path = self.index.write_snapshot()?;
        let snapshot_lsn = self.index.last_snapshot_lsn();

        info!("Wrote snapshot at LSN {}", snapshot_lsn);

        // Truncate WAL
        if snapshot_lsn > 0 {
            if let Err(e) = self.wal.truncate_before(snapshot_lsn) {
                warn!("Failed to truncate WAL: {}", e);
            } else {
                debug!("Truncated WAL before LSN {}", snapshot_lsn);
            }
        }

        Ok(path)
    }

    /// Trigger compaction if needed
    pub fn maybe_compact(&self) -> Result<Option<PathBuf>> {
        if self.needs_compaction() {
            Ok(Some(self.snapshot()?))
        } else {
            Ok(None)
        }
    }

    /// Check if compaction is needed
    pub fn needs_compaction(&self) -> bool {
        self.index.needs_snapshot() || self.wal.needs_compaction()
    }

    /// Start background compaction thread
    fn start_background_compaction(&self) {
        let wal = Arc::clone(&self.wal);
        let index = Arc::clone(&self.index);
        let shutdown = Arc::clone(&self.shutdown);
        let interval = self.config.compaction_interval;
        let compaction_lock = self.compaction_lock.lock();
        drop(compaction_lock); // Just checking it exists

        let handle = thread::spawn(move || {
            info!("Background compaction thread started");

            while !shutdown.load(Ordering::Relaxed) {
                thread::sleep(interval);

                if shutdown.load(Ordering::Relaxed) {
                    break;
                }

                // Check if compaction needed
                if index.needs_snapshot() || wal.needs_compaction() {
                    debug!("Starting background compaction");

                    match index.write_snapshot() {
                        Ok(path) => {
                            info!("Background snapshot completed: {:?}", path);

                            let snapshot_lsn = index.last_snapshot_lsn();
                            if snapshot_lsn > 0 {
                                if let Err(e) = wal.truncate_before(snapshot_lsn) {
                                    warn!("Failed to truncate WAL: {}", e);
                                }
                            }
                        }
                        Err(e) => {
                            error!("Background snapshot failed: {}", e);
                        }
                    }
                }
            }

            info!("Background compaction thread stopped");
        });

        *self.compaction_handle.lock() = Some(handle);
    }

    /// Stop background compaction and shutdown
    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Relaxed);

        if let Some(handle) = self.compaction_handle.lock().take() {
            let _ = handle.join();
        }

        // Final sync
        if let Err(e) = self.wal.sync() {
            error!("Failed to sync WAL on shutdown: {}", e);
        }
    }

    /// Sync WAL to disk
    pub fn sync(&self) -> Result<()> {
        self.wal.sync()
    }

    /// Get current LSN
    pub fn current_lsn(&self) -> u64 {
        self.wal.current_lsn()
    }

    /// Get entry count
    pub fn len(&self) -> u64 {
        self.index.len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Get cache hit ratio
    pub fn cache_hit_ratio(&self) -> f64 {
        self.cache.stats().hit_ratio()
    }

    /// Get statistics
    pub fn stats(&self) -> MetadataStoreStats {
        MetadataStoreStats {
            entry_count: self.index.len(),
            wal_size: self.wal.size(),
            wal_lsn: self.wal.current_lsn(),
            index_lsn: self.index.current_lsn(),
            last_snapshot_lsn: self.index.last_snapshot_lsn(),
            cache_size: self.cache.len(),
            cache_hit_ratio: self.cache.stats().hit_ratio(),
            cache_hits: self.cache.stats().hits.load(Ordering::Relaxed),
            cache_misses: self.cache.stats().misses.load(Ordering::Relaxed),
        }
    }
}

impl Drop for MetadataStore {
    fn drop(&mut self) {
        self.shutdown();
    }
}

/// Metadata store statistics
#[derive(Debug, Clone)]
pub struct MetadataStoreStats {
    /// Number of entries in the index
    pub entry_count: u64,
    /// WAL size in bytes
    pub wal_size: u64,
    /// Current WAL LSN
    pub wal_lsn: u64,
    /// Current index LSN
    pub index_lsn: u64,
    /// Last snapshot LSN
    pub last_snapshot_lsn: u64,
    /// Number of cached entries
    pub cache_size: usize,
    /// Cache hit ratio
    pub cache_hit_ratio: f64,
    /// Total cache hits
    pub cache_hits: u64,
    /// Total cache misses
    pub cache_misses: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn test_config(dir: &Path) -> MetadataStoreConfig {
        MetadataStoreConfig {
            data_dir: dir.to_path_buf(),
            wal: WalConfig {
                sync_on_write: false, // Faster tests
                max_size_bytes: 1024 * 1024,
                write_buffer_size: 4096,
            },
            btree: BTreeConfig {
                snapshot_dir: dir.join("snapshots"),
                snapshot_threshold: 100,
                snapshot_retention: 2,
            },
            cache_size: 100,
            background_compaction: false, // Manual for tests
            compaction_interval: Duration::from_secs(1),
        }
    }

    #[test]
    fn test_store_create_and_put_get() {
        let dir = tempdir().unwrap();
        let config = test_config(dir.path());

        let store = MetadataStore::create(config).unwrap();

        // Put
        let key = MetadataKey::block(42);
        store.put(key.clone(), b"test value".to_vec()).unwrap();

        // Get
        let value = store.get(&key).unwrap();
        assert_eq!(value, b"test value");
    }

    #[test]
    fn test_store_delete() {
        let dir = tempdir().unwrap();
        let config = test_config(dir.path());

        let store = MetadataStore::create(config).unwrap();

        let key = MetadataKey::block(42);
        store.put(key.clone(), b"test value".to_vec()).unwrap();

        store.delete(&key).unwrap();
        assert!(store.get(&key).is_none());
    }

    #[test]
    fn test_store_batch_put() {
        let dir = tempdir().unwrap();
        let config = test_config(dir.path());

        let store = MetadataStore::create(config).unwrap();

        let entries: Vec<(MetadataKey, Vec<u8>)> = (1..=10)
            .map(|i| (MetadataKey::block(i), format!("value_{}", i).into_bytes()))
            .collect();

        store.batch_put(entries).unwrap();

        assert_eq!(store.len(), 10);
        assert_eq!(
            store.get(&MetadataKey::block(5)),
            Some(b"value_5".to_vec())
        );
    }

    #[test]
    fn test_store_recovery() {
        let dir = tempdir().unwrap();
        let config = test_config(dir.path());

        // Create and populate
        {
            let store = MetadataStore::create(config.clone()).unwrap();

            for i in 1..=50 {
                store
                    .put(MetadataKey::block(i), format!("value_{}", i).into_bytes())
                    .unwrap();
            }

            store.sync().unwrap();
        }

        // Reopen and verify
        {
            let store = MetadataStore::open(config).unwrap();

            assert_eq!(store.len(), 50);

            for i in 1..=50 {
                let value = store.get(&MetadataKey::block(i)).unwrap();
                assert_eq!(value, format!("value_{}", i).into_bytes());
            }
        }
    }

    #[test]
    fn test_store_snapshot_and_recovery() {
        let dir = tempdir().unwrap();
        let config = test_config(dir.path());

        // Create, populate, and snapshot
        {
            let store = MetadataStore::create(config.clone()).unwrap();

            for i in 1..=100 {
                store
                    .put(MetadataKey::block(i), format!("value_{}", i).into_bytes())
                    .unwrap();
            }

            // Force snapshot
            store.snapshot().unwrap();

            // Add more entries after snapshot
            for i in 101..=150 {
                store
                    .put(MetadataKey::block(i), format!("value_{}", i).into_bytes())
                    .unwrap();
            }

            store.sync().unwrap();
        }

        // Reopen and verify (should recover from snapshot + WAL replay)
        {
            let store = MetadataStore::open(config).unwrap();

            assert_eq!(store.len(), 150);

            // Check entries from before snapshot
            assert_eq!(
                store.get(&MetadataKey::block(50)),
                Some(b"value_50".to_vec())
            );

            // Check entries from after snapshot (WAL replay)
            assert_eq!(
                store.get(&MetadataKey::block(125)),
                Some(b"value_125".to_vec())
            );
        }
    }

    #[test]
    fn test_store_shard_metadata() {
        let dir = tempdir().unwrap();
        let config = test_config(dir.path());

        let store = MetadataStore::create(config).unwrap();

        let meta = ShardMeta {
            object_id: [1u8; 16],
            shard_position: 0,
            block_num: 42,
            size: 1024,
            checksum: 0xDEADBEEF,
            created_at: 1234567890,
            last_verified: 1234567890,
            shard_type: 0,
            local_group: 255,
        };

        store.put_shard(&meta).unwrap();

        let retrieved = store.get_shard(&[1u8; 16], 0).unwrap();
        assert_eq!(retrieved.block_num, 42);
        assert_eq!(retrieved.checksum, 0xDEADBEEF);
    }

    #[test]
    fn test_store_scan_object_shards() {
        let dir = tempdir().unwrap();
        let config = test_config(dir.path());

        let store = MetadataStore::create(config).unwrap();

        let object_id = [1u8; 16];

        // Add 6 shards for the object
        for pos in 0..6 {
            let meta = ShardMeta {
                object_id,
                shard_position: pos,
                block_num: 100 + pos as u64,
                size: 1024,
                checksum: pos as u32,
                created_at: 1234567890,
                last_verified: 1234567890,
                shard_type: if pos < 4 { 0 } else { 2 }, // 4 data + 2 parity
                local_group: 255,
            };
            store.put_shard(&meta).unwrap();
        }

        // Scan
        let shards = store.scan_object_shards(&object_id);
        assert_eq!(shards.len(), 6);

        // Verify positions
        let positions: Vec<u8> = shards.iter().map(|s| s.shard_position).collect();
        assert_eq!(positions, vec![0, 1, 2, 3, 4, 5]);
    }

    #[test]
    fn test_store_cache_population() {
        let dir = tempdir().unwrap();
        let config = test_config(dir.path());

        let store = MetadataStore::create(config).unwrap();

        let key = MetadataKey::block(42);
        store.put(key.clone(), b"test value".to_vec()).unwrap();

        // First get - from index, populates cache
        store.cache.clear(); // Clear cache to test population
        let _ = store.get(&key);

        // Second get - should be from cache
        let stats_before = store.cache.stats().hits.load(Ordering::Relaxed);
        let _ = store.get(&key);
        let stats_after = store.cache.stats().hits.load(Ordering::Relaxed);

        assert!(stats_after > stats_before);
    }
}
