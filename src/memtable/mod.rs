//! MemTable - In-memory sorted storage for recent writes.
//!
//! The MemTable is the first destination for all writes. It uses a
//! concurrent skip list for efficient sorted storage that supports
//! multiple readers and writers.
//!
//! # Design
//!
//! - Uses `crossbeam-skiplist` for lock-free concurrent access
//! - Keys are `InternalKey` (user_key + sequence + type) for MVCC
//! - Values are raw bytes
//! - Iteration returns entries in sorted order
//!
//! # MVCC Semantics
//!
//! Multiple versions of the same user key can exist with different
//! sequence numbers. Reads at a specific sequence number see the
//! latest version at or before that sequence.

pub mod arena;

use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

use crate::types::{InternalKey, LookupResult, ValueType};

/// MemTable for in-memory sorted storage.
///
/// Thread-safe for concurrent reads and writes.
#[derive(Debug)]
pub struct MemTable {
    /// The underlying skip list.
    /// Key: encoded InternalKey, Value: raw bytes or empty for deletion
    table: SkipMap<Bytes, Bytes>,

    /// Approximate memory usage in bytes.
    approximate_memory_usage: AtomicUsize,

    /// Minimum sequence number in this memtable.
    min_sequence: AtomicU64,

    /// Maximum sequence number in this memtable.
    max_sequence: AtomicU64,

    /// Number of entries.
    entry_count: AtomicUsize,

    /// Unique ID for this memtable.
    id: u64,
}

impl MemTable {
    /// Create a new empty MemTable.
    pub fn new(id: u64) -> Self {
        Self {
            table: SkipMap::new(),
            approximate_memory_usage: AtomicUsize::new(0),
            min_sequence: AtomicU64::new(u64::MAX),
            max_sequence: AtomicU64::new(0),
            entry_count: AtomicUsize::new(0),
            id,
        }
    }

    /// Get the memtable ID.
    pub fn id(&self) -> u64 {
        self.id
    }

    /// Insert a key-value pair.
    ///
    /// The key should be an encoded InternalKey containing the user key,
    /// sequence number, and value type.
    pub fn put(&self, key: &InternalKey, value: &[u8]) {
        let encoded_key = key.encode();
        let value_bytes = Bytes::copy_from_slice(value);

        // Update memory usage estimate
        let entry_size = encoded_key.len() + value_bytes.len() + 64; // 64 bytes overhead estimate
        self.approximate_memory_usage
            .fetch_add(entry_size, Ordering::Relaxed);

        // Update sequence bounds
        let seq = key.sequence();
        self.update_sequence_bounds(seq);

        // Insert into skip list
        self.table.insert(encoded_key, value_bytes);
        self.entry_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Insert a deletion marker.
    pub fn delete(&self, key: &InternalKey) {
        debug_assert!(key.is_deletion());
        let encoded_key = key.encode();

        // Update memory usage estimate (no value for deletions)
        let entry_size = encoded_key.len() + 64;
        self.approximate_memory_usage
            .fetch_add(entry_size, Ordering::Relaxed);

        // Update sequence bounds
        let seq = key.sequence();
        self.update_sequence_bounds(seq);

        // Insert empty value as tombstone
        self.table.insert(encoded_key, Bytes::new());
        self.entry_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Look up a key at a specific sequence number.
    ///
    /// Returns the value for the latest version of the key that has
    /// a sequence number <= the given sequence.
    ///
    /// For MVCC, multiple versions exist with different sequence numbers.
    /// We need the highest sequence <= target (most recent visible version).
    /// Since encoded keys are sorted by (user_key, sequence) in ascending order,
    /// we iterate through all valid entries and keep the last one.
    pub fn get(&self, user_key: &[u8], sequence: u64) -> LookupResult {
        // Create bounds for this user key's entries
        // Lower bound: user_key with sequence 0 (start of this key's versions)
        let lower_key = InternalKey::new(
            Bytes::copy_from_slice(user_key),
            0,
            ValueType::Value,
        )
        .encode();

        // Upper bound: user_key with target sequence (include all versions <= target)
        // Use Deletion type to ensure we include deletions at this sequence
        let upper_key = InternalKey::new(
            Bytes::copy_from_slice(user_key),
            sequence,
            ValueType::Deletion, // Deletion has higher byte value, so it comes after Value
        )
        .encode();

        // Iterate through entries in range and keep the last valid one
        // (last = highest sequence <= target)
        let mut result = LookupResult::NotFound;

        for entry in self.table.range(lower_key..=upper_key) {
            let entry_key = entry.key();

            // Verify this is the same user key
            if let Some(entry_user_key) = InternalKey::parse_user_key(entry_key) {
                if entry_user_key != user_key {
                    // Different user key - skip
                    continue;
                }

                // Parse and verify sequence is valid
                if let Some(entry_seq) = InternalKey::parse_sequence(entry_key) {
                    if entry_seq <= sequence {
                        // Valid entry - update result (we want the last one)
                        if let Some(internal_key) = InternalKey::decode(entry_key) {
                            if internal_key.is_deletion() {
                                result = LookupResult::Deleted;
                            } else {
                                result = LookupResult::Found(entry.value().clone());
                            }
                        }
                    }
                }
            }
        }

        result
    }

    /// Get approximate memory usage in bytes.
    pub fn approximate_memory_usage(&self) -> usize {
        self.approximate_memory_usage.load(Ordering::Relaxed)
    }

    /// Get the number of entries.
    pub fn entry_count(&self) -> usize {
        self.entry_count.load(Ordering::Relaxed)
    }

    /// Check if the memtable is empty.
    pub fn is_empty(&self) -> bool {
        self.entry_count() == 0
    }

    /// Get the minimum sequence number.
    pub fn min_sequence(&self) -> u64 {
        let min = self.min_sequence.load(Ordering::Relaxed);
        if min == u64::MAX {
            0
        } else {
            min
        }
    }

    /// Get the maximum sequence number.
    pub fn max_sequence(&self) -> u64 {
        self.max_sequence.load(Ordering::Relaxed)
    }

    /// Create an iterator over all entries.
    pub fn iter(&self) -> MemTableIterator<'_> {
        MemTableIterator::new(self)
    }

    /// Create an iterator starting at a specific key.
    pub fn iter_from(&self, key: &[u8]) -> MemTableIterator<'_> {
        MemTableIterator::from_key(self, key)
    }

    /// Update sequence bounds.
    fn update_sequence_bounds(&self, seq: u64) {
        // Update min
        let mut current_min = self.min_sequence.load(Ordering::Relaxed);
        while seq < current_min {
            match self.min_sequence.compare_exchange_weak(
                current_min,
                seq,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(x) => current_min = x,
            }
        }

        // Update max
        let mut current_max = self.max_sequence.load(Ordering::Relaxed);
        while seq > current_max {
            match self.max_sequence.compare_exchange_weak(
                current_max,
                seq,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(x) => current_max = x,
            }
        }
    }
}

/// Iterator over MemTable entries.
pub struct MemTableIterator<'a> {
    /// Reference to the memtable.
    memtable: &'a MemTable,
    /// Current position in iteration.
    current: Option<(Bytes, Bytes)>,
    /// Whether we've started iterating.
    started: bool,
    /// Start key for iteration.
    start_key: Option<Bytes>,
}

impl<'a> MemTableIterator<'a> {
    /// Create a new iterator at the beginning.
    fn new(memtable: &'a MemTable) -> Self {
        Self {
            memtable,
            current: None,
            started: false,
            start_key: None,
        }
    }

    /// Create an iterator starting at a specific key.
    fn from_key(memtable: &'a MemTable, key: &[u8]) -> Self {
        Self {
            memtable,
            current: None,
            started: false,
            start_key: Some(Bytes::copy_from_slice(key)),
        }
    }

    /// Check if the iterator is valid (positioned at an entry).
    pub fn valid(&self) -> bool {
        self.current.is_some()
    }

    /// Get the current key.
    pub fn key(&self) -> Option<&Bytes> {
        self.current.as_ref().map(|(k, _)| k)
    }

    /// Get the current value.
    pub fn value(&self) -> Option<&Bytes> {
        self.current.as_ref().map(|(_, v)| v)
    }

    /// Move to the first entry.
    pub fn seek_to_first(&mut self) {
        self.started = true;
        if let Some(entry) = self.memtable.table.front() {
            self.current = Some((entry.key().clone(), entry.value().clone()));
        } else {
            self.current = None;
        }
    }

    /// Seek to the first entry with key >= target.
    pub fn seek(&mut self, target: &[u8]) {
        self.started = true;
        let target_bytes = Bytes::copy_from_slice(target);

        for entry in self.memtable.table.range(target_bytes..) {
            self.current = Some((entry.key().clone(), entry.value().clone()));
            return;
        }
        self.current = None;
    }

    /// Move to the next entry.
    pub fn next(&mut self) {
        if !self.started {
            self.seek_to_first();
            return;
        }

        if let Some((current_key, _)) = &self.current {
            let mut found_current = false;
            for entry in self.memtable.table.range(current_key.clone()..) {
                if found_current {
                    self.current = Some((entry.key().clone(), entry.value().clone()));
                    return;
                }
                if entry.key() == current_key {
                    found_current = true;
                }
            }
        }
        self.current = None;
    }

    /// Get the current entry as (InternalKey, value).
    pub fn entry(&self) -> Option<(InternalKey, &Bytes)> {
        self.current.as_ref().and_then(|(k, v)| {
            InternalKey::decode(k).map(|ik| (ik, v))
        })
    }
}

impl<'a> Iterator for MemTableIterator<'a> {
    type Item = (InternalKey, Bytes);

    fn next(&mut self) -> Option<Self::Item> {
        if !self.started {
            if let Some(start) = self.start_key.clone() {
                self.seek(&start);
            } else {
                self.seek_to_first();
            }
        } else {
            MemTableIterator::next(self);
        }

        self.current.as_ref().and_then(|(k, v)| {
            InternalKey::decode(k).map(|ik| (ik, v.clone()))
        })
    }
}

/// A handle to an immutable MemTable.
///
/// Once a MemTable is frozen (made immutable), it can be wrapped in this
/// type to indicate it's ready for flushing to an SSTable.
#[derive(Debug)]
pub struct ImmutableMemTable {
    inner: Arc<MemTable>,
}

impl ImmutableMemTable {
    /// Create from a MemTable.
    pub fn new(memtable: MemTable) -> Self {
        Self {
            inner: Arc::new(memtable),
        }
    }

    /// Get a reference to the inner MemTable.
    pub fn inner(&self) -> &MemTable {
        &self.inner
    }

    /// Get the memtable ID.
    pub fn id(&self) -> u64 {
        self.inner.id()
    }

    /// Look up a key.
    pub fn get(&self, user_key: &[u8], sequence: u64) -> LookupResult {
        self.inner.get(user_key, sequence)
    }

    /// Get approximate memory usage.
    pub fn approximate_memory_usage(&self) -> usize {
        self.inner.approximate_memory_usage()
    }

    /// Create an iterator.
    pub fn iter(&self) -> MemTableIterator<'_> {
        self.inner.iter()
    }
}

impl Clone for ImmutableMemTable {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_memtable_put_get() {
        let memtable = MemTable::new(1);

        // Insert a value
        let key = InternalKey::for_value(Bytes::from("hello"), 1);
        memtable.put(&key, b"world");

        // Read it back
        let result = memtable.get(b"hello", 1);
        assert!(matches!(result, LookupResult::Found(v) if v == Bytes::from("world")));

        // Read at higher sequence should also work
        let result = memtable.get(b"hello", 10);
        assert!(matches!(result, LookupResult::Found(v) if v == Bytes::from("world")));
    }

    #[test]
    fn test_memtable_delete() {
        let memtable = MemTable::new(1);

        // Insert a value
        let key1 = InternalKey::for_value(Bytes::from("hello"), 1);
        memtable.put(&key1, b"world");

        // Delete it
        let key2 = InternalKey::for_deletion(Bytes::from("hello"), 2);
        memtable.delete(&key2);

        // Read at sequence 1 should find the value
        let result = memtable.get(b"hello", 1);
        assert!(matches!(result, LookupResult::Found(_)));

        // Read at sequence 2+ should find deletion
        let result = memtable.get(b"hello", 2);
        assert!(matches!(result, LookupResult::Deleted));
    }

    #[test]
    fn test_memtable_mvcc() {
        let memtable = MemTable::new(1);

        // Insert multiple versions
        let key1 = InternalKey::for_value(Bytes::from("key"), 1);
        memtable.put(&key1, b"v1");

        let key2 = InternalKey::for_value(Bytes::from("key"), 5);
        memtable.put(&key2, b"v5");

        let key3 = InternalKey::for_value(Bytes::from("key"), 10);
        memtable.put(&key3, b"v10");

        // Read at different sequences
        let result = memtable.get(b"key", 1);
        assert!(matches!(result, LookupResult::Found(v) if v == Bytes::from("v1")));

        let result = memtable.get(b"key", 3);
        assert!(matches!(result, LookupResult::Found(v) if v == Bytes::from("v1")));

        let result = memtable.get(b"key", 5);
        assert!(matches!(result, LookupResult::Found(v) if v == Bytes::from("v5")));

        let result = memtable.get(b"key", 7);
        assert!(matches!(result, LookupResult::Found(v) if v == Bytes::from("v5")));

        let result = memtable.get(b"key", 10);
        assert!(matches!(result, LookupResult::Found(v) if v == Bytes::from("v10")));

        let result = memtable.get(b"key", 100);
        assert!(matches!(result, LookupResult::Found(v) if v == Bytes::from("v10")));
    }

    #[test]
    fn test_memtable_not_found() {
        let memtable = MemTable::new(1);

        let key = InternalKey::for_value(Bytes::from("hello"), 1);
        memtable.put(&key, b"world");

        // Different key should not be found
        let result = memtable.get(b"other", 1);
        assert!(matches!(result, LookupResult::NotFound));

        // Same key but sequence 0 (before any write)
        let result = memtable.get(b"hello", 0);
        assert!(matches!(result, LookupResult::NotFound));
    }

    #[test]
    fn test_memtable_iterator() {
        let memtable = MemTable::new(1);

        // Insert keys in random order
        for i in [5, 1, 3, 2, 4] {
            let key = InternalKey::for_value(Bytes::from(format!("key{}", i)), i);
            memtable.put(&key, format!("value{}", i).as_bytes());
        }

        // Iterate and collect
        let mut keys: Vec<String> = Vec::new();
        for (key, _) in memtable.iter() {
            keys.push(String::from_utf8_lossy(key.user_key()).to_string());
        }

        // Should be in sorted order
        assert_eq!(keys.len(), 5);
        // Note: The exact order depends on InternalKey comparison
    }

    #[test]
    fn test_memtable_memory_tracking() {
        let memtable = MemTable::new(1);
        assert_eq!(memtable.approximate_memory_usage(), 0);

        let key = InternalKey::for_value(Bytes::from("hello"), 1);
        memtable.put(&key, b"world");

        assert!(memtable.approximate_memory_usage() > 0);
    }

    #[test]
    fn test_memtable_sequence_bounds() {
        let memtable = MemTable::new(1);

        let key1 = InternalKey::for_value(Bytes::from("a"), 5);
        memtable.put(&key1, b"1");

        let key2 = InternalKey::for_value(Bytes::from("b"), 10);
        memtable.put(&key2, b"2");

        let key3 = InternalKey::for_value(Bytes::from("c"), 3);
        memtable.put(&key3, b"3");

        assert_eq!(memtable.min_sequence(), 3);
        assert_eq!(memtable.max_sequence(), 10);
    }

    #[test]
    fn test_immutable_memtable() {
        let memtable = MemTable::new(1);

        let key = InternalKey::for_value(Bytes::from("hello"), 1);
        memtable.put(&key, b"world");

        let immutable = ImmutableMemTable::new(memtable);

        let result = immutable.get(b"hello", 1);
        assert!(matches!(result, LookupResult::Found(v) if v == Bytes::from("world")));
    }

    #[test]
    fn test_memtable_multiple_keys() {
        let memtable = MemTable::new(1);

        // Insert different keys
        for i in 0..100 {
            let key = InternalKey::for_value(Bytes::from(format!("key{:04}", i)), i as u64);
            memtable.put(&key, format!("value{}", i).as_bytes());
        }

        assert_eq!(memtable.entry_count(), 100);

        // Verify we can read all of them
        for i in 0..100 {
            let result = memtable.get(format!("key{:04}", i).as_bytes(), 100);
            assert!(result.is_found(), "Key {} not found", i);
        }
    }
}
