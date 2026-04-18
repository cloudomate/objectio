//! Write cache for block storage
//!
//! Provides a write-back cache with journaling for durability and low latency.

use crate::chunk::{ChunkId, ChunkMapper};
use crate::error::{BlockError, BlockResult};
use crate::journal::WriteJournal;

use bytes::Bytes;
use parking_lot::RwLock;
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

/// A dirty chunk that needs to be flushed to storage
#[derive(Debug, Clone)]
pub struct DirtyChunk {
    /// Full chunk data
    pub data: Bytes,
    /// When the chunk was first dirtied
    pub dirty_since: Instant,
    /// When the chunk was last modified
    pub last_modified: Instant,
}

/// Write cache configuration
#[derive(Debug, Clone)]
pub struct CacheConfig {
    /// Maximum cache size in bytes
    pub max_cache_bytes: u64,
    /// Flush interval for background flushing
    pub flush_interval: Duration,
    /// Maximum age before a dirty chunk must be flushed
    pub max_dirty_age: Duration,
    /// Journal directory path
    pub journal_path: Option<String>,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            max_cache_bytes: 256 * 1024 * 1024, // 256MB
            flush_interval: Duration::from_secs(5),
            max_dirty_age: Duration::from_secs(30),
            journal_path: None,
        }
    }
}

/// Cached data for a volume
struct VolumeCache {
    /// Dirty chunks (not yet flushed to storage)
    dirty_chunks: BTreeMap<ChunkId, DirtyChunk>,
    /// Clean chunks (read cache)
    clean_chunks: BTreeMap<ChunkId, Bytes>,
    /// Total dirty bytes
    dirty_bytes: u64,
    /// Total clean bytes
    clean_bytes: u64,
}

impl VolumeCache {
    fn new() -> Self {
        Self {
            dirty_chunks: BTreeMap::new(),
            clean_chunks: BTreeMap::new(),
            dirty_bytes: 0,
            clean_bytes: 0,
        }
    }
}

/// Write-back cache for block storage
///
/// Provides low-latency writes by caching data in memory and flushing
/// to storage in the background. A write-ahead journal ensures durability.
pub struct WriteCache {
    /// Per-volume caches
    caches: RwLock<BTreeMap<String, VolumeCache>>,
    /// Chunk mapper
    chunk_mapper: Arc<ChunkMapper>,
    /// Configuration
    config: CacheConfig,
    /// Total dirty bytes across all volumes
    total_dirty_bytes: RwLock<u64>,
    /// Write-ahead journal for durability (optional)
    journal: Option<Arc<WriteJournal>>,
    /// Shutdown signal sender
    _shutdown_tx: Option<mpsc::Sender<()>>,
}

/// Default maximum journal size: 256MB
const DEFAULT_MAX_JOURNAL_SIZE: u64 = 256 * 1024 * 1024;

impl WriteCache {
    /// Create a new write cache
    pub fn new(chunk_mapper: Arc<ChunkMapper>, config: CacheConfig) -> Self {
        let journal = config.journal_path.as_ref().map(|path| {
            Arc::new(
                WriteJournal::open(path, DEFAULT_MAX_JOURNAL_SIZE).expect("failed to open journal"),
            )
        });

        if journal.is_some() {
            info!(
                "Write cache initialized with journal at {:?}",
                config.journal_path
            );
        } else {
            warn!("Write cache initialized WITHOUT journal - data may be lost on crash");
        }

        Self {
            caches: RwLock::new(BTreeMap::new()),
            chunk_mapper,
            config,
            total_dirty_bytes: RwLock::new(0),
            journal,
            _shutdown_tx: None,
        }
    }

    /// Create a new write cache with default configuration
    pub fn with_defaults(chunk_mapper: Arc<ChunkMapper>) -> Self {
        Self::new(chunk_mapper, CacheConfig::default())
    }

    /// Create a new write cache with journaling enabled
    pub fn with_journal<P: AsRef<Path>>(chunk_mapper: Arc<ChunkMapper>, journal_path: P) -> Self {
        let config = CacheConfig {
            journal_path: Some(journal_path.as_ref().to_string_lossy().to_string()),
            ..CacheConfig::default()
        };
        Self::new(chunk_mapper, config)
    }

    /// Get the chunk mapper
    pub fn chunk_mapper(&self) -> Arc<ChunkMapper> {
        self.chunk_mapper.clone()
    }

    /// Initialize cache for a volume
    pub fn init_volume(&self, volume_id: &str) {
        let mut caches = self.caches.write();
        if !caches.contains_key(volume_id) {
            caches.insert(volume_id.to_string(), VolumeCache::new());
        }
    }

    /// Remove cache for a volume
    pub fn remove_volume(&self, volume_id: &str) {
        let mut caches = self.caches.write();
        if let Some(cache) = caches.remove(volume_id) {
            let mut total = self.total_dirty_bytes.write();
            *total = total.saturating_sub(cache.dirty_bytes);
        }
    }

    /// Write data to the cache
    ///
    /// This updates the in-memory cache and marks chunks as dirty.
    /// If journaling is enabled, the write is logged before acknowledging.
    /// The actual flush to storage happens asynchronously.
    pub fn write(&self, volume_id: &str, offset: u64, data: &[u8]) -> BlockResult<()> {
        if data.is_empty() {
            return Ok(());
        }

        // Log to journal first for durability (write-ahead logging)
        if let Some(ref journal) = self.journal {
            journal.log_write(
                volume_id,
                self.chunk_mapper.byte_offset_to_chunk_id(offset),
                offset % self.chunk_mapper.chunk_size(),
                Bytes::copy_from_slice(data),
            )?;
        }

        // Calculate affected chunks
        let chunk_ranges = self
            .chunk_mapper
            .byte_range_to_chunks(offset, data.len() as u64);
        let chunk_size = self.chunk_mapper.chunk_size() as usize;

        let mut caches = self.caches.write();
        let cache = caches
            .get_mut(volume_id)
            .ok_or_else(|| BlockError::VolumeNotFound(volume_id.to_string()))?;

        let mut data_offset = 0usize;
        let now = Instant::now();

        for range in chunk_ranges {
            let range_len = range.length as usize;

            // Get or create chunk data
            let chunk_data = if let Some(dirty) = cache.dirty_chunks.get(&range.chunk_id) {
                // Already dirty, update it
                dirty.data.to_vec()
            } else if let Some(clean) = cache.clean_chunks.remove(&range.chunk_id) {
                // Was clean, promote to dirty
                cache.clean_bytes = cache.clean_bytes.saturating_sub(clean.len() as u64);
                clean.to_vec()
            } else {
                // New chunk, initialize with zeros
                vec![0u8; chunk_size]
            };

            // Update chunk data
            let mut chunk_data = chunk_data;
            if chunk_data.len() < chunk_size {
                chunk_data.resize(chunk_size, 0);
            }

            let offset_in_chunk = range.offset_in_chunk as usize;
            chunk_data[offset_in_chunk..offset_in_chunk + range_len]
                .copy_from_slice(&data[data_offset..data_offset + range_len]);

            // Store as dirty
            let was_dirty = cache.dirty_chunks.contains_key(&range.chunk_id);
            let dirty_chunk = DirtyChunk {
                data: Bytes::from(chunk_data),
                dirty_since: if was_dirty {
                    cache.dirty_chunks.get(&range.chunk_id).unwrap().dirty_since
                } else {
                    now
                },
                last_modified: now,
            };

            if !was_dirty {
                cache.dirty_bytes += chunk_size as u64;
                *self.total_dirty_bytes.write() += chunk_size as u64;
            }

            cache.dirty_chunks.insert(range.chunk_id, dirty_chunk);
            data_offset += range_len;
        }

        // Check if we need to trigger a flush
        if self.should_flush() {
            debug!("Cache pressure high, should trigger flush");
            // In a real implementation, this would signal the background flusher
        }

        // Check if journal needs rotation
        if let Some(ref journal) = self.journal
            && journal.needs_rotation()
        {
            debug!("Journal needs rotation");
        }

        Ok(())
    }

    /// Read data from the cache (or return None if not cached)
    ///
    /// Returns the data if found in cache (dirty or clean), None otherwise.
    /// The caller should read from storage if None is returned.
    pub fn read(&self, volume_id: &str, offset: u64, length: u64) -> Option<Vec<u8>> {
        if length == 0 {
            return Some(Vec::new());
        }

        let chunk_ranges = self.chunk_mapper.byte_range_to_chunks(offset, length);
        let caches = self.caches.read();
        let cache = caches.get(volume_id)?;

        let mut result = Vec::with_capacity(length as usize);

        for range in &chunk_ranges {
            // Check dirty cache first
            if let Some(dirty) = cache.dirty_chunks.get(&range.chunk_id) {
                let offset_in_chunk = range.offset_in_chunk as usize;
                let range_len = range.length as usize;
                result.extend_from_slice(&dirty.data[offset_in_chunk..offset_in_chunk + range_len]);
            }
            // Check clean cache
            else if let Some(clean) = cache.clean_chunks.get(&range.chunk_id) {
                let offset_in_chunk = range.offset_in_chunk as usize;
                let range_len = range.length as usize;
                result.extend_from_slice(&clean[offset_in_chunk..offset_in_chunk + range_len]);
            } else {
                // Cache miss - caller needs to read from storage
                return None;
            }
        }

        Some(result)
    }

    /// Add a clean chunk to the read cache
    pub fn add_clean(&self, volume_id: &str, chunk_id: ChunkId, data: Bytes) {
        let mut caches = self.caches.write();
        if let Some(cache) = caches.get_mut(volume_id) {
            // Don't overwrite dirty data
            if !cache.dirty_chunks.contains_key(&chunk_id) {
                cache.clean_bytes += data.len() as u64;
                cache.clean_chunks.insert(chunk_id, data);
            }
        }
    }

    /// Get dirty chunks that should be flushed
    ///
    /// Returns chunks that are either:
    /// - Older than max_dirty_age
    /// - Need to be flushed due to cache pressure
    pub fn get_chunks_to_flush(&self, volume_id: &str) -> Vec<(ChunkId, Bytes)> {
        let caches = self.caches.read();
        let cache = match caches.get(volume_id) {
            Some(c) => c,
            None => return Vec::new(),
        };

        let now = Instant::now();
        let mut to_flush = Vec::new();

        for (chunk_id, dirty) in &cache.dirty_chunks {
            let age = now.duration_since(dirty.dirty_since);
            if age >= self.config.max_dirty_age || self.should_flush() {
                to_flush.push((*chunk_id, dirty.data.clone()));
            }
        }

        to_flush
    }

    /// Mark chunks as flushed (no longer dirty)
    ///
    /// Logs the flush completion to the journal if enabled.
    pub fn mark_flushed(&self, volume_id: &str, chunk_ids: &[ChunkId]) {
        // Log flush completion to journal
        if let Some(ref journal) = self.journal {
            for chunk_id in chunk_ids {
                if let Err(e) = journal.log_flush(volume_id, *chunk_id) {
                    warn!("Failed to log flush to journal: {}", e);
                }
            }
        }

        let mut caches = self.caches.write();
        if let Some(cache) = caches.get_mut(volume_id) {
            for chunk_id in chunk_ids {
                if let Some(dirty) = cache.dirty_chunks.remove(chunk_id) {
                    let chunk_size = dirty.data.len() as u64;
                    cache.dirty_bytes = cache.dirty_bytes.saturating_sub(chunk_size);
                    *self.total_dirty_bytes.write() -= chunk_size;

                    // Move to clean cache
                    cache.clean_bytes += chunk_size;
                    cache.clean_chunks.insert(*chunk_id, dirty.data);
                }
            }
        }
    }

    /// Create a checkpoint in the journal
    ///
    /// Entries before the checkpoint can be discarded on recovery.
    pub fn checkpoint(&self) -> BlockResult<()> {
        if let Some(ref journal) = self.journal {
            journal.checkpoint()?;
        }
        Ok(())
    }

    /// Recover unflushed writes from the journal
    ///
    /// Returns the writes that need to be replayed to restore cache state.
    pub fn recover(&self) -> BlockResult<Vec<(String, ChunkId, u64, Bytes)>> {
        let Some(ref journal) = self.journal else {
            return Ok(Vec::new());
        };

        let entries = journal.recover()?;
        let mut writes = Vec::new();

        for entry in entries {
            if let Some(data) = entry.data {
                writes.push((entry.volume_id, entry.chunk_id, entry.offset, data));
            }
        }

        info!("Recovered {} unflushed writes from journal", writes.len());
        Ok(writes)
    }

    /// Sync journal to disk
    pub fn sync(&self) -> BlockResult<()> {
        if let Some(ref journal) = self.journal {
            journal.sync()?;
        }
        Ok(())
    }

    /// Flush all dirty data for a volume
    pub fn flush_volume(&self, volume_id: &str) -> Vec<(ChunkId, Bytes)> {
        let mut caches = self.caches.write();
        if let Some(cache) = caches.get_mut(volume_id) {
            // Use std::mem::take to efficiently drain the BTreeMap
            let dirty_chunks = std::mem::take(&mut cache.dirty_chunks);
            let dirty: Vec<_> = dirty_chunks
                .into_iter()
                .map(|(id, d)| (id, d.data))
                .collect();

            let flushed_bytes = cache.dirty_bytes;
            *self.total_dirty_bytes.write() -= flushed_bytes;
            cache.dirty_bytes = 0;

            dirty
        } else {
            Vec::new()
        }
    }

    /// Check if we should trigger a flush due to cache pressure
    fn should_flush(&self) -> bool {
        *self.total_dirty_bytes.read() >= self.config.max_cache_bytes * 80 / 100
    }

    /// Get cache statistics
    pub fn stats(&self) -> CacheStats {
        let caches = self.caches.read();
        let mut stats = CacheStats::default();

        for cache in caches.values() {
            stats.dirty_bytes += cache.dirty_bytes;
            stats.clean_bytes += cache.clean_bytes;
            stats.dirty_chunks += cache.dirty_chunks.len();
            stats.clean_chunks += cache.clean_chunks.len();
        }

        stats.volume_count = caches.len();
        stats
    }
}

/// Cache statistics
#[derive(Debug, Default)]
pub struct CacheStats {
    /// Number of volumes with cached data
    pub volume_count: usize,
    /// Total dirty bytes
    pub dirty_bytes: u64,
    /// Total clean bytes
    pub clean_bytes: u64,
    /// Number of dirty chunks
    pub dirty_chunks: usize,
    /// Number of clean chunks
    pub clean_chunks: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_cache() -> WriteCache {
        let mapper = Arc::new(ChunkMapper::new(1024 * 1024)); // 1MB chunks for testing
        WriteCache::with_defaults(mapper)
    }

    #[test]
    fn test_write_single_chunk() {
        let cache = test_cache();
        cache.init_volume("vol1");

        let data = vec![0xABu8; 4096]; // 4KB
        cache.write("vol1", 0, &data).unwrap();

        // Should be able to read it back
        let read = cache.read("vol1", 0, 4096).unwrap();
        assert_eq!(read, data);

        // Stats should show dirty data
        let stats = cache.stats();
        assert_eq!(stats.dirty_chunks, 1);
        assert!(stats.dirty_bytes > 0);
    }

    #[test]
    fn test_write_spanning_chunks() {
        let cache = test_cache();
        cache.init_volume("vol1");

        // Write 2MB starting at 512KB (spans chunks 0 and 1)
        let data = vec![0xCDu8; 2 * 1024 * 1024];
        cache.write("vol1", 512 * 1024, &data).unwrap();

        // Should have 3 dirty chunks (512KB in chunk 0, 1MB in chunk 1, 512KB in chunk 2)
        let stats = cache.stats();
        assert_eq!(stats.dirty_chunks, 3);

        // Read back should work
        let read = cache.read("vol1", 512 * 1024, 2 * 1024 * 1024).unwrap();
        assert_eq!(read, data);
    }

    #[test]
    fn test_flush() {
        let cache = test_cache();
        cache.init_volume("vol1");

        let data = vec![0xEFu8; 4096];
        cache.write("vol1", 0, &data).unwrap();

        // Flush should return the dirty chunk
        let flushed = cache.flush_volume("vol1");
        assert_eq!(flushed.len(), 1);
        assert_eq!(flushed[0].0, 0); // Chunk ID 0

        // After flush, stats should show no dirty data
        let stats = cache.stats();
        assert_eq!(stats.dirty_chunks, 0);
        assert_eq!(stats.dirty_bytes, 0);
    }

    #[test]
    fn test_read_cache_miss() {
        let cache = test_cache();
        cache.init_volume("vol1");

        // Reading without writing should return None (cache miss)
        let read = cache.read("vol1", 0, 4096);
        assert!(read.is_none());
    }
}
