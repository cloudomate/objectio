//! Adaptive Replacement Cache (ARC) for metadata
//!
//! ARC combines recency and frequency to make eviction decisions,
//! automatically adapting to workload patterns. It maintains:
//!
//! - T1: Recently accessed entries (seen once recently)
//! - T2: Frequently accessed entries (seen multiple times)
//! - B1: Ghost entries recently evicted from T1
//! - B2: Ghost entries recently evicted from T2
//!
//! The algorithm adapts the target size of T1 vs T2 based on hit patterns.

use super::types::MetadataKey;
use parking_lot::Mutex;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};

/// Cache statistics
#[derive(Debug, Default)]
pub struct CacheStats {
    pub hits: AtomicU64,
    pub misses: AtomicU64,
    pub evictions: AtomicU64,
    pub t1_hits: AtomicU64,
    pub t2_hits: AtomicU64,
}

impl CacheStats {
    /// Calculate hit ratio (0.0 to 1.0)
    pub fn hit_ratio(&self) -> f64 {
        let hits = self.hits.load(Ordering::Relaxed) as f64;
        let misses = self.misses.load(Ordering::Relaxed) as f64;
        let total = hits + misses;
        if total == 0.0 { 0.0 } else { hits / total }
    }

    /// Reset all statistics
    pub fn reset(&self) {
        self.hits.store(0, Ordering::Relaxed);
        self.misses.store(0, Ordering::Relaxed);
        self.evictions.store(0, Ordering::Relaxed);
        self.t1_hits.store(0, Ordering::Relaxed);
        self.t2_hits.store(0, Ordering::Relaxed);
    }
}

/// Cached entry
#[derive(Clone)]
struct CacheEntry {
    value: Vec<u8>,
}

/// ARC cache internal state
struct ArcState {
    /// Recently accessed (seen once)
    t1: VecDeque<MetadataKey>,
    /// Frequently accessed (seen multiple times)
    t2: VecDeque<MetadataKey>,
    /// Ghost entries from T1
    b1: VecDeque<MetadataKey>,
    /// Ghost entries from T2
    b2: VecDeque<MetadataKey>,
    /// Actual cached values
    cache: HashMap<MetadataKey, CacheEntry>,
    /// Target size for T1 (adaptive parameter)
    p: usize,
    /// Maximum cache capacity
    capacity: usize,
}

impl ArcState {
    fn new(capacity: usize) -> Self {
        Self {
            t1: VecDeque::new(),
            t2: VecDeque::new(),
            b1: VecDeque::new(),
            b2: VecDeque::new(),
            cache: HashMap::with_capacity(capacity),
            p: 0,
            capacity,
        }
    }

    /// Check which list contains the key (if any)
    fn find_list(&self, key: &MetadataKey) -> Option<ListLocation> {
        if self.t1.contains(key) {
            Some(ListLocation::T1)
        } else if self.t2.contains(key) {
            Some(ListLocation::T2)
        } else if self.b1.contains(key) {
            Some(ListLocation::B1)
        } else if self.b2.contains(key) {
            Some(ListLocation::B2)
        } else {
            None
        }
    }

    /// Remove key from a specific list
    fn remove_from_list(&mut self, key: &MetadataKey, list: ListLocation) {
        match list {
            ListLocation::T1 => self.t1.retain(|k| k != key),
            ListLocation::T2 => self.t2.retain(|k| k != key),
            ListLocation::B1 => self.b1.retain(|k| k != key),
            ListLocation::B2 => self.b2.retain(|k| k != key),
        }
    }

    /// Current size of T1 + T2 (actual cached items)
    fn cache_size(&self) -> usize {
        self.t1.len() + self.t2.len()
    }

    /// Current size of B1 + B2 (ghost entries)
    fn ghost_size(&self) -> usize {
        self.b1.len() + self.b2.len()
    }
}

/// Which list a key is in
#[derive(Debug, Clone, Copy, PartialEq)]
enum ListLocation {
    T1,
    T2,
    B1,
    B2,
}

/// Adaptive Replacement Cache
pub struct ArcCache {
    state: Mutex<ArcState>,
    stats: CacheStats,
}

impl ArcCache {
    /// Create a new ARC cache with the given capacity
    pub fn new(capacity: usize) -> Self {
        Self {
            state: Mutex::new(ArcState::new(capacity)),
            stats: CacheStats::default(),
        }
    }

    /// Get a value from the cache
    pub fn get(&self, key: &MetadataKey) -> Option<Vec<u8>> {
        let mut state = self.state.lock();

        // Check T1
        if state.t1.contains(key) {
            // Move from T1 to T2 (now frequently accessed)
            state.t1.retain(|k| k != key);
            state.t2.push_back(key.clone());

            self.stats.hits.fetch_add(1, Ordering::Relaxed);
            self.stats.t1_hits.fetch_add(1, Ordering::Relaxed);

            return state.cache.get(key).map(|e| e.value.clone());
        }

        // Check T2
        if state.t2.contains(key) {
            // Move to back of T2 (most recently used)
            state.t2.retain(|k| k != key);
            state.t2.push_back(key.clone());

            self.stats.hits.fetch_add(1, Ordering::Relaxed);
            self.stats.t2_hits.fetch_add(1, Ordering::Relaxed);

            return state.cache.get(key).map(|e| e.value.clone());
        }

        // Miss
        self.stats.misses.fetch_add(1, Ordering::Relaxed);
        None
    }

    /// Put a value into the cache
    pub fn put(&self, key: MetadataKey, value: Vec<u8>) {
        let mut state = self.state.lock();
        let capacity = state.capacity;

        // Case 1: Key is in T1 or T2 (hit - update value and move to T2)
        if let Some(location) = state.find_list(&key) {
            match location {
                ListLocation::T1 | ListLocation::T2 => {
                    state.remove_from_list(&key, location);
                    state.t2.push_back(key.clone());
                    state.cache.insert(key, CacheEntry { value });
                    return;
                }
                _ => {}
            }
        }

        // Case 2: Key is in B1 (ghost - was recently evicted from T1)
        if state.b1.contains(&key) {
            // Adapt: increase target size for T1
            let delta = std::cmp::max(1, state.b2.len() / state.b1.len().max(1));
            state.p = std::cmp::min(capacity, state.p + delta);

            // Remove from B1
            state.b1.retain(|k| k != &key);

            // Make room if needed
            self.replace(&mut state, false);

            // Add to T2
            state.t2.push_back(key.clone());
            state.cache.insert(key, CacheEntry { value });
            return;
        }

        // Case 3: Key is in B2 (ghost - was recently evicted from T2)
        if state.b2.contains(&key) {
            // Adapt: decrease target size for T1
            let delta = std::cmp::max(1, state.b1.len() / state.b2.len().max(1));
            state.p = state.p.saturating_sub(delta);

            // Remove from B2
            state.b2.retain(|k| k != &key);

            // Make room if needed
            self.replace(&mut state, true);

            // Add to T2
            state.t2.push_back(key.clone());
            state.cache.insert(key, CacheEntry { value });
            return;
        }

        // Case 4: Key is not in any list (new entry)
        let l1_size = state.t1.len() + state.b1.len();

        if l1_size == capacity {
            // L1 (T1 + B1) is full
            if state.t1.len() < capacity {
                // Still room in T1, remove oldest from B1
                state.b1.pop_front();
                self.replace(&mut state, false);
            } else {
                // T1 is full (and B1 is empty), remove from T1 and add to B1
                if let Some(evicted) = state.t1.pop_front() {
                    state.cache.remove(&evicted);
                    state.b1.push_back(evicted);
                    self.stats.evictions.fetch_add(1, Ordering::Relaxed);
                }
            }
        } else if l1_size < capacity && state.cache_size() + state.ghost_size() >= capacity {
            // Cache is full but L1 has room
            if state.cache_size() + state.ghost_size() == 2 * capacity {
                // Ghost entries at max - remove from B2
                state.b2.pop_front();
            }
            self.replace(&mut state, false);
        }

        // Add new entry to T1
        state.t1.push_back(key.clone());
        state.cache.insert(key, CacheEntry { value });
    }

    /// Remove a value from the cache
    pub fn remove(&self, key: &MetadataKey) -> Option<Vec<u8>> {
        let mut state = self.state.lock();

        // Remove from whichever list it's in
        if let Some(location) = state.find_list(key) {
            state.remove_from_list(key, location);
        }

        state.cache.remove(key).map(|e| e.value)
    }

    /// Check if key is in cache
    pub fn contains(&self, key: &MetadataKey) -> bool {
        let state = self.state.lock();
        state.cache.contains_key(key)
    }

    /// Replace algorithm - evict from T1 or T2
    fn replace(&self, state: &mut ArcState, in_b2: bool) {
        if state.cache_size() == 0 {
            return;
        }

        let t1_len = state.t1.len();

        // Decide whether to evict from T1 or T2
        let evict_from_t1 = if t1_len > 0 {
            if in_b2 {
                t1_len >= state.p
            } else {
                t1_len > state.p
            }
        } else {
            false
        };

        if evict_from_t1 {
            // Evict from T1 -> B1
            if let Some(evicted) = state.t1.pop_front() {
                state.cache.remove(&evicted);
                state.b1.push_back(evicted);
                self.stats.evictions.fetch_add(1, Ordering::Relaxed);
            }
        } else {
            // Evict from T2 -> B2
            if let Some(evicted) = state.t2.pop_front() {
                state.cache.remove(&evicted);
                state.b2.push_back(evicted);
                self.stats.evictions.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// Clear the entire cache
    pub fn clear(&self) {
        let mut state = self.state.lock();
        state.t1.clear();
        state.t2.clear();
        state.b1.clear();
        state.b2.clear();
        state.cache.clear();
        state.p = 0;
    }

    /// Get cache statistics
    pub fn stats(&self) -> &CacheStats {
        &self.stats
    }

    /// Get current cache size
    pub fn len(&self) -> usize {
        let state = self.state.lock();
        state.cache.len()
    }

    /// Check if cache is empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Get cache capacity
    pub fn capacity(&self) -> usize {
        let state = self.state.lock();
        state.capacity
    }

    /// Get diagnostic info about internal state
    pub fn debug_info(&self) -> ArcDebugInfo {
        let state = self.state.lock();
        ArcDebugInfo {
            t1_len: state.t1.len(),
            t2_len: state.t2.len(),
            b1_len: state.b1.len(),
            b2_len: state.b2.len(),
            p: state.p,
            capacity: state.capacity,
        }
    }
}

/// Debug information about ARC state
#[derive(Debug)]
pub struct ArcDebugInfo {
    pub t1_len: usize,
    pub t2_len: usize,
    pub b1_len: usize,
    pub b2_len: usize,
    pub p: usize,
    pub capacity: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_arc_basic_put_get() {
        let cache = ArcCache::new(10);

        cache.put(MetadataKey::block(1), b"value1".to_vec());
        cache.put(MetadataKey::block(2), b"value2".to_vec());

        assert_eq!(cache.get(&MetadataKey::block(1)), Some(b"value1".to_vec()));
        assert_eq!(cache.get(&MetadataKey::block(2)), Some(b"value2".to_vec()));
        assert_eq!(cache.get(&MetadataKey::block(3)), None);
    }

    #[test]
    fn test_arc_eviction() {
        let cache = ArcCache::new(3);

        // Fill cache
        cache.put(MetadataKey::block(1), b"v1".to_vec());
        cache.put(MetadataKey::block(2), b"v2".to_vec());
        cache.put(MetadataKey::block(3), b"v3".to_vec());

        assert_eq!(cache.len(), 3);

        // Add one more - should evict
        cache.put(MetadataKey::block(4), b"v4".to_vec());

        // Should have evicted key 1 (oldest in T1)
        assert!(cache.get(&MetadataKey::block(1)).is_none());
        assert_eq!(cache.get(&MetadataKey::block(4)), Some(b"v4".to_vec()));
    }

    #[test]
    fn test_arc_frequency_adaptation() {
        let cache = ArcCache::new(4);

        // Add initial items
        cache.put(MetadataKey::block(1), b"v1".to_vec());
        cache.put(MetadataKey::block(2), b"v2".to_vec());

        // Access key 1 multiple times (moves to T2)
        cache.get(&MetadataKey::block(1));
        cache.get(&MetadataKey::block(1));

        // Key 1 should now be in T2 (frequently accessed)
        let info = cache.debug_info();
        assert_eq!(info.t2_len, 1); // key 1 in T2
        assert_eq!(info.t1_len, 1); // key 2 in T1
    }

    #[test]
    fn test_arc_remove() {
        let cache = ArcCache::new(10);

        cache.put(MetadataKey::block(1), b"value".to_vec());
        assert!(cache.contains(&MetadataKey::block(1)));

        let removed = cache.remove(&MetadataKey::block(1));
        assert_eq!(removed, Some(b"value".to_vec()));
        assert!(!cache.contains(&MetadataKey::block(1)));
    }

    #[test]
    fn test_arc_stats() {
        let cache = ArcCache::new(10);

        cache.put(MetadataKey::block(1), b"value".to_vec());

        // Hit
        cache.get(&MetadataKey::block(1));
        assert_eq!(cache.stats().hits.load(Ordering::Relaxed), 1);

        // Miss
        cache.get(&MetadataKey::block(2));
        assert_eq!(cache.stats().misses.load(Ordering::Relaxed), 1);

        assert!(cache.stats().hit_ratio() > 0.4 && cache.stats().hit_ratio() < 0.6);
    }

    #[test]
    fn test_arc_clear() {
        let cache = ArcCache::new(10);

        cache.put(MetadataKey::block(1), b"v1".to_vec());
        cache.put(MetadataKey::block(2), b"v2".to_vec());

        cache.clear();

        assert!(cache.is_empty());
        assert!(cache.get(&MetadataKey::block(1)).is_none());
    }

    #[test]
    fn test_arc_ghost_entries() {
        let cache = ArcCache::new(2);

        // Fill and evict
        cache.put(MetadataKey::block(1), b"v1".to_vec());
        cache.put(MetadataKey::block(2), b"v2".to_vec());
        cache.put(MetadataKey::block(3), b"v3".to_vec()); // Evicts key 1

        // Key 1 should be in ghost list B1
        let info = cache.debug_info();
        assert_eq!(info.b1_len, 1);

        // Re-insert key 1 - should go to T2 (was in B1)
        cache.put(MetadataKey::block(1), b"v1_new".to_vec());

        assert_eq!(cache.get(&MetadataKey::block(1)), Some(b"v1_new".to_vec()));
    }
}
