//! Block cache for storage engine
//!
//! Since ObjectIO uses direct I/O (O_DIRECT/F_NOCACHE) to bypass the OS page cache,
//! we implement application-level caching for frequently accessed blocks.

use bytes::Bytes;
use objectio_common::DiskId;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Cache key uniquely identifies a block across all disks
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CacheKey {
    pub disk_id: DiskId,
    pub block_num: u64,
}

impl CacheKey {
    pub fn new(disk_id: DiskId, block_num: u64) -> Self {
        Self { disk_id, block_num }
    }
}

/// Individual cache entry with LRU tracking and dirty flag
pub struct CacheEntry {
    /// Block data
    pub data: Bytes,
    /// Last access time for LRU eviction
    last_access: AtomicU64,
    /// True if modified but not yet flushed to disk
    dirty: bool,
    /// When the entry was marked dirty (for write-back age tracking)
    dirty_since: Option<Instant>,
}

impl CacheEntry {
    fn new(data: Bytes, clock: u64) -> Self {
        Self {
            data,
            last_access: AtomicU64::new(clock),
            dirty: false,
            dirty_since: None,
        }
    }

    fn new_dirty(data: Bytes, clock: u64) -> Self {
        Self {
            data,
            last_access: AtomicU64::new(clock),
            dirty: true,
            dirty_since: Some(Instant::now()),
        }
    }

    fn touch(&self, clock: u64) {
        self.last_access.store(clock, Ordering::Relaxed);
    }

    pub fn is_dirty(&self) -> bool {
        self.dirty
    }
}

/// Cache capacity can be specified by entry count or memory size
#[derive(Debug, Clone, Copy)]
pub enum CacheCapacity {
    /// Maximum number of entries
    Entries(usize),
    /// Maximum memory in bytes
    Bytes(usize),
}

impl Default for CacheCapacity {
    fn default() -> Self {
        CacheCapacity::Entries(1024)
    }
}

/// Write policy for the cache
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WritePolicy {
    /// Write to disk and cache simultaneously (default, strong durability)
    WriteThrough,
    /// Write to cache only, flush asynchronously (high throughput)
    WriteBack,
    /// Write to disk only, do not cache writes (for large sequential writes)
    WriteAround,
}

impl Default for WritePolicy {
    fn default() -> Self {
        WritePolicy::WriteThrough
    }
}

/// Cache statistics for monitoring
#[derive(Debug, Default)]
pub struct CacheStats {
    /// Number of cache hits
    pub hits: AtomicU64,
    /// Number of cache misses
    pub misses: AtomicU64,
    /// Number of entries evicted
    pub evictions: AtomicU64,
    /// Number of dirty entries written back to disk
    pub writebacks: AtomicU64,
    /// Current number of dirty entries
    pub dirty_count: AtomicU64,
}

impl CacheStats {
    /// Calculate hit ratio (0.0 to 1.0)
    pub fn hit_ratio(&self) -> f64 {
        let hits = self.hits.load(Ordering::Relaxed);
        let misses = self.misses.load(Ordering::Relaxed);
        let total = hits + misses;
        if total == 0 {
            return 0.0;
        }
        hits as f64 / total as f64
    }

    /// Reset all statistics
    pub fn reset(&self) {
        self.hits.store(0, Ordering::Relaxed);
        self.misses.store(0, Ordering::Relaxed);
        self.evictions.store(0, Ordering::Relaxed);
        self.writebacks.store(0, Ordering::Relaxed);
        self.dirty_count.store(0, Ordering::Relaxed);
    }
}

/// LRU block cache with configurable write policies
pub struct BlockCache {
    /// Cached entries protected by RwLock
    entries: RwLock<HashMap<CacheKey, CacheEntry>>,
    /// Cache capacity configuration
    capacity: CacheCapacity,
    /// Logical clock for LRU ordering
    clock: AtomicU64,
    /// Cache statistics
    stats: CacheStats,
    /// Write policy
    policy: WritePolicy,
    /// Block size (for memory-based capacity calculations)
    block_size: usize,
    /// Maximum age for dirty entries before forced flush (write-back only)
    max_dirty_age: Duration,
    /// Maximum dirty entries before forced flush (write-back only)
    dirty_threshold: usize,
}

impl BlockCache {
    /// Create a new block cache with the given capacity
    pub fn new(capacity: CacheCapacity, block_size: usize) -> Self {
        Self {
            entries: RwLock::new(HashMap::new()),
            capacity,
            clock: AtomicU64::new(0),
            stats: CacheStats::default(),
            policy: WritePolicy::default(),
            block_size,
            max_dirty_age: Duration::from_secs(30),
            dirty_threshold: 1000,
        }
    }

    /// Create a new block cache with a specific write policy
    pub fn with_policy(capacity: CacheCapacity, block_size: usize, policy: WritePolicy) -> Self {
        Self {
            entries: RwLock::new(HashMap::new()),
            capacity,
            clock: AtomicU64::new(0),
            stats: CacheStats::default(),
            policy,
            block_size,
            max_dirty_age: Duration::from_secs(30),
            dirty_threshold: 1000,
        }
    }

    /// Set the maximum age for dirty entries (write-back mode)
    pub fn set_max_dirty_age(&mut self, age: Duration) {
        self.max_dirty_age = age;
    }

    /// Set the dirty threshold (write-back mode)
    pub fn set_dirty_threshold(&mut self, threshold: usize) {
        self.dirty_threshold = threshold;
    }

    /// Get the write policy
    pub fn policy(&self) -> WritePolicy {
        self.policy
    }

    /// Get cache statistics
    pub fn stats(&self) -> &CacheStats {
        &self.stats
    }

    /// Get the current number of entries in the cache
    pub fn len(&self) -> usize {
        self.entries.read().len()
    }

    /// Check if the cache is empty
    pub fn is_empty(&self) -> bool {
        self.entries.read().is_empty()
    }

    /// Advance the logical clock and return the new value
    fn tick(&self) -> u64 {
        self.clock.fetch_add(1, Ordering::Relaxed) + 1
    }

    /// Get the maximum number of entries based on capacity configuration
    fn max_entries(&self) -> usize {
        match self.capacity {
            CacheCapacity::Entries(n) => n,
            CacheCapacity::Bytes(bytes) => bytes / self.block_size.max(1),
        }
    }

    /// Look up a block in the cache
    pub fn get(&self, key: &CacheKey) -> Option<Bytes> {
        let entries = self.entries.read();
        if let Some(entry) = entries.get(key) {
            let clock = self.tick();
            entry.touch(clock);
            self.stats.hits.fetch_add(1, Ordering::Relaxed);
            Some(entry.data.clone())
        } else {
            self.stats.misses.fetch_add(1, Ordering::Relaxed);
            None
        }
    }

    /// Insert a clean (non-dirty) block into the cache
    pub fn insert(&self, key: CacheKey, data: Bytes) {
        let clock = self.tick();
        let mut entries = self.entries.write();

        // Evict if at capacity
        while entries.len() >= self.max_entries() {
            if let Some(evict_key) = self.find_lru_entry(&entries) {
                entries.remove(&evict_key);
                self.stats.evictions.fetch_add(1, Ordering::Relaxed);
            } else {
                break;
            }
        }

        entries.insert(key, CacheEntry::new(data, clock));
    }

    /// Insert a dirty block into the cache (for write-back mode)
    pub fn insert_dirty(&self, key: CacheKey, data: Bytes) {
        let clock = self.tick();
        let mut entries = self.entries.write();

        // Evict if at capacity
        while entries.len() >= self.max_entries() {
            if let Some(evict_key) = self.find_lru_entry(&entries) {
                let entry = entries.remove(&evict_key);
                self.stats.evictions.fetch_add(1, Ordering::Relaxed);
                if entry.map(|e| e.dirty).unwrap_or(false) {
                    self.stats.dirty_count.fetch_sub(1, Ordering::Relaxed);
                }
            } else {
                break;
            }
        }

        // Check if replacing an existing dirty entry
        if let Some(old) = entries.get(&key) {
            if old.dirty {
                // Already dirty, don't increment count
                entries.insert(key, CacheEntry::new_dirty(data, clock));
                return;
            }
        }

        entries.insert(key, CacheEntry::new_dirty(data, clock));
        self.stats.dirty_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Invalidate (remove) a block from the cache
    pub fn invalidate(&self, key: &CacheKey) -> Option<Bytes> {
        let mut entries = self.entries.write();
        if let Some(entry) = entries.remove(key) {
            if entry.dirty {
                self.stats.dirty_count.fetch_sub(1, Ordering::Relaxed);
            }
            Some(entry.data)
        } else {
            None
        }
    }

    /// Check if a block is in the cache
    pub fn contains(&self, key: &CacheKey) -> bool {
        self.entries.read().contains_key(key)
    }

    /// Mark an entry as clean (after write-back flush)
    pub fn mark_clean(&self, key: &CacheKey) -> bool {
        let mut entries = self.entries.write();
        if let Some(entry) = entries.get_mut(key) {
            if entry.dirty {
                entry.dirty = false;
                entry.dirty_since = None;
                self.stats.dirty_count.fetch_sub(1, Ordering::Relaxed);
                self.stats.writebacks.fetch_add(1, Ordering::Relaxed);
                return true;
            }
        }
        false
    }

    /// Get all dirty entries that need to be flushed
    pub fn dirty_entries(&self) -> Vec<(CacheKey, Bytes)> {
        let entries = self.entries.read();
        entries
            .iter()
            .filter(|(_, entry)| entry.dirty)
            .map(|(key, entry)| (*key, entry.data.clone()))
            .collect()
    }

    /// Get dirty entries that exceed the max age threshold
    pub fn aged_dirty_entries(&self) -> Vec<(CacheKey, Bytes)> {
        let entries = self.entries.read();
        let now = Instant::now();
        entries
            .iter()
            .filter(|(_, entry)| {
                entry.dirty
                    && entry
                        .dirty_since
                        .map(|t| now.duration_since(t) > self.max_dirty_age)
                        .unwrap_or(false)
            })
            .map(|(key, entry)| (*key, entry.data.clone()))
            .collect()
    }

    /// Check if flush is needed based on dirty threshold
    pub fn needs_flush(&self) -> bool {
        self.stats.dirty_count.load(Ordering::Relaxed) as usize >= self.dirty_threshold
    }

    /// Find the LRU entry key (excluding dirty entries if possible)
    fn find_lru_entry(&self, entries: &HashMap<CacheKey, CacheEntry>) -> Option<CacheKey> {
        // First, try to find LRU among clean entries
        let mut lru_clean: Option<(CacheKey, u64)> = None;
        let mut lru_dirty: Option<(CacheKey, u64)> = None;

        for (key, entry) in entries.iter() {
            let access = entry.last_access.load(Ordering::Relaxed);
            if entry.dirty {
                match lru_dirty {
                    None => lru_dirty = Some((*key, access)),
                    Some((_, min_access)) if access < min_access => {
                        lru_dirty = Some((*key, access))
                    }
                    _ => {}
                }
            } else {
                match lru_clean {
                    None => lru_clean = Some((*key, access)),
                    Some((_, min_access)) if access < min_access => {
                        lru_clean = Some((*key, access))
                    }
                    _ => {}
                }
            }
        }

        // Prefer evicting clean entries over dirty ones
        lru_clean.or(lru_dirty).map(|(key, _)| key)
    }

    /// Clear all entries from the cache
    pub fn clear(&self) {
        let mut entries = self.entries.write();
        entries.clear();
        self.stats.dirty_count.store(0, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use objectio_common::DiskId;

    fn test_disk_id() -> DiskId {
        DiskId::from_bytes([1u8; 16])
    }

    #[test]
    fn test_cache_insert_and_get() {
        let cache = BlockCache::new(CacheCapacity::Entries(100), 4096);
        let key = CacheKey::new(test_disk_id(), 42);
        let data = Bytes::from(vec![1, 2, 3, 4]);

        cache.insert(key, data.clone());

        let retrieved = cache.get(&key);
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap(), data);
        assert_eq!(cache.stats().hits.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_cache_miss() {
        let cache = BlockCache::new(CacheCapacity::Entries(100), 4096);
        let key = CacheKey::new(test_disk_id(), 42);

        let result = cache.get(&key);
        assert!(result.is_none());
        assert_eq!(cache.stats().misses.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_cache_eviction() {
        let cache = BlockCache::new(CacheCapacity::Entries(2), 4096);
        let disk_id = test_disk_id();

        // Insert 3 entries into a cache of size 2
        cache.insert(CacheKey::new(disk_id, 1), Bytes::from(vec![1]));
        cache.insert(CacheKey::new(disk_id, 2), Bytes::from(vec![2]));
        cache.insert(CacheKey::new(disk_id, 3), Bytes::from(vec![3]));

        // Should have evicted the oldest entry
        assert_eq!(cache.len(), 2);
        assert!(cache.stats().evictions.load(Ordering::Relaxed) >= 1);
    }

    #[test]
    fn test_cache_lru_ordering() {
        let cache = BlockCache::new(CacheCapacity::Entries(2), 4096);
        let disk_id = test_disk_id();

        // Insert entries 1 and 2
        cache.insert(CacheKey::new(disk_id, 1), Bytes::from(vec![1]));
        cache.insert(CacheKey::new(disk_id, 2), Bytes::from(vec![2]));

        // Access entry 1 to make it more recent
        cache.get(&CacheKey::new(disk_id, 1));

        // Insert entry 3, should evict entry 2 (LRU)
        cache.insert(CacheKey::new(disk_id, 3), Bytes::from(vec![3]));

        // Entry 1 should still be present
        assert!(cache.contains(&CacheKey::new(disk_id, 1)));
        // Entry 2 should be evicted
        assert!(!cache.contains(&CacheKey::new(disk_id, 2)));
        // Entry 3 should be present
        assert!(cache.contains(&CacheKey::new(disk_id, 3)));
    }

    #[test]
    fn test_dirty_entries() {
        let cache =
            BlockCache::with_policy(CacheCapacity::Entries(100), 4096, WritePolicy::WriteBack);
        let disk_id = test_disk_id();

        // Insert dirty entries
        cache.insert_dirty(CacheKey::new(disk_id, 1), Bytes::from(vec![1]));
        cache.insert_dirty(CacheKey::new(disk_id, 2), Bytes::from(vec![2]));

        assert_eq!(cache.stats().dirty_count.load(Ordering::Relaxed), 2);

        let dirty = cache.dirty_entries();
        assert_eq!(dirty.len(), 2);

        // Mark one as clean
        cache.mark_clean(&CacheKey::new(disk_id, 1));
        assert_eq!(cache.stats().dirty_count.load(Ordering::Relaxed), 1);
        assert_eq!(cache.stats().writebacks.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_invalidate() {
        let cache = BlockCache::new(CacheCapacity::Entries(100), 4096);
        let key = CacheKey::new(test_disk_id(), 42);
        let data = Bytes::from(vec![1, 2, 3, 4]);

        cache.insert(key, data.clone());
        assert!(cache.contains(&key));

        let removed = cache.invalidate(&key);
        assert!(removed.is_some());
        assert_eq!(removed.unwrap(), data);
        assert!(!cache.contains(&key));
    }

    #[test]
    fn test_hit_ratio() {
        let cache = BlockCache::new(CacheCapacity::Entries(100), 4096);
        let key = CacheKey::new(test_disk_id(), 42);
        let data = Bytes::from(vec![1, 2, 3, 4]);

        cache.insert(key, data);

        // 2 hits
        cache.get(&key);
        cache.get(&key);

        // 2 misses
        cache.get(&CacheKey::new(test_disk_id(), 99));
        cache.get(&CacheKey::new(test_disk_id(), 100));

        let ratio = cache.stats().hit_ratio();
        assert!((ratio - 0.5).abs() < 0.01);
    }

    #[test]
    fn test_memory_based_capacity() {
        // 1 MB capacity with 4KB blocks = 256 entries max
        let cache = BlockCache::new(CacheCapacity::Bytes(1024 * 1024), 4096);
        assert_eq!(cache.max_entries(), 256);
    }

    #[test]
    fn test_clear() {
        let cache = BlockCache::new(CacheCapacity::Entries(100), 4096);
        let disk_id = test_disk_id();

        cache.insert(CacheKey::new(disk_id, 1), Bytes::from(vec![1]));
        cache.insert_dirty(CacheKey::new(disk_id, 2), Bytes::from(vec![2]));

        assert_eq!(cache.len(), 2);
        assert_eq!(cache.stats().dirty_count.load(Ordering::Relaxed), 1);

        cache.clear();

        assert!(cache.is_empty());
        assert_eq!(cache.stats().dirty_count.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn test_evict_clean_before_dirty() {
        let cache = BlockCache::new(CacheCapacity::Entries(2), 4096);
        let disk_id = test_disk_id();

        // Insert a dirty entry first
        cache.insert_dirty(CacheKey::new(disk_id, 1), Bytes::from(vec![1]));
        // Then a clean entry
        cache.insert(CacheKey::new(disk_id, 2), Bytes::from(vec![2]));

        // Insert a third entry, should evict the clean entry (2) not the dirty one (1)
        cache.insert(CacheKey::new(disk_id, 3), Bytes::from(vec![3]));

        // Dirty entry should still be present
        assert!(cache.contains(&CacheKey::new(disk_id, 1)));
        // Clean entry should be evicted
        assert!(!cache.contains(&CacheKey::new(disk_id, 2)));
        // New entry should be present
        assert!(cache.contains(&CacheKey::new(disk_id, 3)));
    }
}
