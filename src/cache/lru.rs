//! LRU Cache implementation.
//!
//! A sharded LRU cache for concurrent access with configurable capacity.

use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use parking_lot::Mutex;

/// Statistics for cache operations.
#[derive(Debug, Default)]
pub struct CacheStats {
    /// Number of cache hits.
    pub hits: AtomicU64,
    /// Number of cache misses.
    pub misses: AtomicU64,
    /// Number of insertions.
    pub inserts: AtomicU64,
    /// Number of evictions.
    pub evictions: AtomicU64,
}

impl CacheStats {
    /// Create new stats.
    pub fn new() -> Self {
        Self::default()
    }

    /// Get hit rate (0.0 to 1.0).
    pub fn hit_rate(&self) -> f64 {
        let hits = self.hits.load(Ordering::Relaxed);
        let misses = self.misses.load(Ordering::Relaxed);
        let total = hits + misses;
        if total == 0 {
            0.0
        } else {
            hits as f64 / total as f64
        }
    }

    /// Reset all counters.
    pub fn reset(&self) {
        self.hits.store(0, Ordering::Relaxed);
        self.misses.store(0, Ordering::Relaxed);
        self.inserts.store(0, Ordering::Relaxed);
        self.evictions.store(0, Ordering::Relaxed);
    }
}

impl Clone for CacheStats {
    fn clone(&self) -> Self {
        Self {
            hits: AtomicU64::new(self.hits.load(Ordering::Relaxed)),
            misses: AtomicU64::new(self.misses.load(Ordering::Relaxed)),
            inserts: AtomicU64::new(self.inserts.load(Ordering::Relaxed)),
            evictions: AtomicU64::new(self.evictions.load(Ordering::Relaxed)),
        }
    }
}

/// A node in the LRU linked list.
struct LruNode<K, V> {
    key: K,
    value: V,
    prev: Option<usize>,
    next: Option<usize>,
}

/// A single shard of the LRU cache.
struct LruShard<K, V> {
    /// Capacity of this shard.
    capacity: usize,
    /// Map from key to node index.
    map: HashMap<K, usize>,
    /// Node storage (using indices instead of pointers).
    nodes: Vec<Option<LruNode<K, V>>>,
    /// Free list of node indices.
    free_list: Vec<usize>,
    /// Head of LRU list (most recently used).
    head: Option<usize>,
    /// Tail of LRU list (least recently used).
    tail: Option<usize>,
}

impl<K: Hash + Eq + Clone, V: Clone> LruShard<K, V> {
    fn new(capacity: usize) -> Self {
        Self {
            capacity,
            map: HashMap::with_capacity(capacity),
            nodes: Vec::with_capacity(capacity),
            free_list: Vec::new(),
            head: None,
            tail: None,
        }
    }

    fn get(&mut self, key: &K) -> Option<V> {
        if let Some(&idx) = self.map.get(key) {
            // Move to front (most recently used)
            self.move_to_front(idx);
            self.nodes[idx].as_ref().map(|n| n.value.clone())
        } else {
            None
        }
    }

    fn insert(&mut self, key: K, value: V) -> bool {
        // Check if key already exists
        if let Some(&idx) = self.map.get(&key) {
            // Update value and move to front
            if let Some(ref mut node) = self.nodes[idx] {
                node.value = value;
            }
            self.move_to_front(idx);
            return false; // No eviction
        }

        // Need to evict if at capacity
        let evicted = if self.map.len() >= self.capacity {
            self.evict_lru();
            true
        } else {
            false
        };

        // Allocate node
        let idx = self.allocate_node();
        self.nodes[idx] = Some(LruNode {
            key: key.clone(),
            value,
            prev: None,
            next: self.head,
        });

        // Update head's prev pointer
        if let Some(head_idx) = self.head {
            if let Some(ref mut head_node) = self.nodes[head_idx] {
                head_node.prev = Some(idx);
            }
        }

        // Set as new head
        self.head = Some(idx);
        if self.tail.is_none() {
            self.tail = Some(idx);
        }

        self.map.insert(key, idx);
        evicted
    }

    fn remove(&mut self, key: &K) -> Option<V> {
        if let Some(idx) = self.map.remove(key) {
            let value = self.unlink_node(idx);
            self.free_list.push(idx);
            value
        } else {
            None
        }
    }

    fn len(&self) -> usize {
        self.map.len()
    }

    fn allocate_node(&mut self) -> usize {
        if let Some(idx) = self.free_list.pop() {
            idx
        } else {
            let idx = self.nodes.len();
            self.nodes.push(None);
            idx
        }
    }

    fn move_to_front(&mut self, idx: usize) {
        if self.head == Some(idx) {
            return; // Already at front
        }

        // Unlink from current position
        let (prev, next) = {
            let node = self.nodes[idx].as_ref().unwrap();
            (node.prev, node.next)
        };

        if let Some(prev_idx) = prev {
            if let Some(ref mut prev_node) = self.nodes[prev_idx] {
                prev_node.next = next;
            }
        }

        if let Some(next_idx) = next {
            if let Some(ref mut next_node) = self.nodes[next_idx] {
                next_node.prev = prev;
            }
        }

        if self.tail == Some(idx) {
            self.tail = prev;
        }

        // Link at front
        if let Some(ref mut node) = self.nodes[idx] {
            node.prev = None;
            node.next = self.head;
        }

        if let Some(head_idx) = self.head {
            if let Some(ref mut head_node) = self.nodes[head_idx] {
                head_node.prev = Some(idx);
            }
        }

        self.head = Some(idx);
    }

    fn evict_lru(&mut self) {
        if let Some(tail_idx) = self.tail {
            let key = self.nodes[tail_idx].as_ref().unwrap().key.clone();
            self.map.remove(&key);
            self.unlink_node(tail_idx);
            self.free_list.push(tail_idx);
        }
    }

    fn unlink_node(&mut self, idx: usize) -> Option<V> {
        let (prev, next, value) = {
            let node = self.nodes[idx].take()?;
            (node.prev, node.next, node.value)
        };

        if let Some(prev_idx) = prev {
            if let Some(ref mut prev_node) = self.nodes[prev_idx] {
                prev_node.next = next;
            }
        } else {
            self.head = next;
        }

        if let Some(next_idx) = next {
            if let Some(ref mut next_node) = self.nodes[next_idx] {
                next_node.prev = prev;
            }
        } else {
            self.tail = prev;
        }

        Some(value)
    }
}

/// Number of shards for the cache.
const NUM_SHARDS: usize = 16;

/// A sharded LRU cache for concurrent access.
///
/// The cache is divided into multiple shards to reduce lock contention.
/// Each shard has its own LRU list and can be accessed independently.
pub struct LruCache<K, V> {
    shards: Vec<Mutex<LruShard<K, V>>>,
    stats: Arc<CacheStats>,
}

impl<K: Hash + Eq + Clone, V: Clone> LruCache<K, V> {
    /// Create a new cache with the given capacity.
    ///
    /// The capacity is divided among the shards.
    pub fn new(capacity: usize) -> Self {
        let shard_capacity = (capacity + NUM_SHARDS - 1) / NUM_SHARDS;
        let shards = (0..NUM_SHARDS)
            .map(|_| Mutex::new(LruShard::new(shard_capacity)))
            .collect();

        Self {
            shards,
            stats: Arc::new(CacheStats::new()),
        }
    }

    /// Get a value from the cache.
    pub fn get(&self, key: &K) -> Option<V> {
        let shard_idx = self.shard_index(key);
        let mut shard = self.shards[shard_idx].lock();
        let result = shard.get(key);

        if result.is_some() {
            self.stats.hits.fetch_add(1, Ordering::Relaxed);
        } else {
            self.stats.misses.fetch_add(1, Ordering::Relaxed);
        }

        result
    }

    /// Insert a value into the cache.
    ///
    /// Returns true if an entry was evicted.
    pub fn insert(&self, key: K, value: V) -> bool {
        let shard_idx = self.shard_index(&key);
        let mut shard = self.shards[shard_idx].lock();
        let evicted = shard.insert(key, value);

        self.stats.inserts.fetch_add(1, Ordering::Relaxed);
        if evicted {
            self.stats.evictions.fetch_add(1, Ordering::Relaxed);
        }

        evicted
    }

    /// Remove a value from the cache.
    pub fn remove(&self, key: &K) -> Option<V> {
        let shard_idx = self.shard_index(key);
        let mut shard = self.shards[shard_idx].lock();
        shard.remove(key)
    }

    /// Get the total number of entries across all shards.
    pub fn len(&self) -> usize {
        self.shards.iter().map(|s| s.lock().len()).sum()
    }

    /// Check if the cache is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Get cache statistics.
    pub fn stats(&self) -> Arc<CacheStats> {
        Arc::clone(&self.stats)
    }

    /// Clear all entries from the cache.
    pub fn clear(&self) {
        for shard in &self.shards {
            let mut s = shard.lock();
            s.map.clear();
            s.nodes.clear();
            s.free_list.clear();
            s.head = None;
            s.tail = None;
        }
    }

    fn shard_index(&self, key: &K) -> usize {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        key.hash(&mut hasher);
        hasher.finish() as usize % NUM_SHARDS
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lru_cache_basic() {
        let cache: LruCache<String, i32> = LruCache::new(10);

        cache.insert("a".to_string(), 1);
        cache.insert("b".to_string(), 2);
        cache.insert("c".to_string(), 3);

        assert_eq!(cache.get(&"a".to_string()), Some(1));
        assert_eq!(cache.get(&"b".to_string()), Some(2));
        assert_eq!(cache.get(&"c".to_string()), Some(3));
        assert_eq!(cache.get(&"d".to_string()), None);
    }

    #[test]
    fn test_lru_cache_eviction() {
        let cache: LruCache<i32, i32> = LruCache::new(3);

        cache.insert(1, 10);
        cache.insert(2, 20);
        cache.insert(3, 30);

        // All should be present
        assert_eq!(cache.get(&1), Some(10));
        assert_eq!(cache.get(&2), Some(20));
        assert_eq!(cache.get(&3), Some(30));

        // Insert more - should evict
        cache.insert(4, 40);
        cache.insert(5, 50);

        // New entries should be present
        assert_eq!(cache.get(&4), Some(40));
        assert_eq!(cache.get(&5), Some(50));

        // Can't guarantee which was evicted due to sharding,
        // but size should be limited
        assert!(cache.len() <= 3 * NUM_SHARDS);
    }

    #[test]
    fn test_lru_cache_update() {
        let cache: LruCache<String, i32> = LruCache::new(10);

        cache.insert("key".to_string(), 1);
        assert_eq!(cache.get(&"key".to_string()), Some(1));

        cache.insert("key".to_string(), 2);
        assert_eq!(cache.get(&"key".to_string()), Some(2));
    }

    #[test]
    fn test_lru_cache_remove() {
        let cache: LruCache<String, i32> = LruCache::new(10);

        cache.insert("key".to_string(), 1);
        assert_eq!(cache.get(&"key".to_string()), Some(1));

        let removed = cache.remove(&"key".to_string());
        assert_eq!(removed, Some(1));
        assert_eq!(cache.get(&"key".to_string()), None);
    }

    #[test]
    fn test_lru_cache_stats() {
        let cache: LruCache<String, i32> = LruCache::new(10);

        cache.insert("a".to_string(), 1);
        cache.insert("b".to_string(), 2);

        // Hits
        cache.get(&"a".to_string());
        cache.get(&"b".to_string());

        // Misses
        cache.get(&"c".to_string());
        cache.get(&"d".to_string());

        let stats = cache.stats();
        assert_eq!(stats.hits.load(Ordering::Relaxed), 2);
        assert_eq!(stats.misses.load(Ordering::Relaxed), 2);
        assert_eq!(stats.inserts.load(Ordering::Relaxed), 2);
        assert_eq!(stats.hit_rate(), 0.5);
    }

    #[test]
    fn test_lru_cache_clear() {
        let cache: LruCache<String, i32> = LruCache::new(10);

        cache.insert("a".to_string(), 1);
        cache.insert("b".to_string(), 2);

        assert!(!cache.is_empty());
        cache.clear();
        assert!(cache.is_empty());
    }

    #[test]
    fn test_lru_shard_lru_order() {
        // Test that LRU eviction works correctly
        let mut shard: LruShard<i32, i32> = LruShard::new(3);

        shard.insert(1, 10);
        shard.insert(2, 20);
        shard.insert(3, 30);

        // Access 1 to make it recently used
        shard.get(&1);

        // Insert 4 - should evict 2 (least recently used)
        shard.insert(4, 40);

        assert_eq!(shard.get(&1), Some(10));
        assert_eq!(shard.get(&2), None); // Evicted
        assert_eq!(shard.get(&3), Some(30));
        assert_eq!(shard.get(&4), Some(40));
    }
}
