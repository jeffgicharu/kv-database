//! DBIterator - unified database iterator.
//!
//! Merges entries from all data sources (memtables, SSTables) and provides
//! consistent iteration with snapshot isolation.

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::Arc;

use bytes::Bytes;

use crate::db::Database;
use crate::memtable::ImmutableMemTable;
use crate::sstable::SSTableReader;
use crate::types::InternalKey;
use crate::util::filename::table_file_path;
use crate::Result;

/// Direction for iterator traversal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IteratorDirection {
    /// Forward iteration (ascending key order).
    Forward,
    /// Reverse iteration (descending key order).
    Reverse,
}

/// An entry in the merged view.
#[derive(Debug, Clone)]
struct MergedEntry {
    /// User key.
    user_key: Bytes,
    /// Value (None if deleted).
    value: Option<Bytes>,
    /// Sequence number of this version.
    sequence: u64,
}

/// Builder for DBIterator with various options.
pub struct DBIteratorBuilder {
    /// Snapshot sequence number.
    sequence: u64,
    /// Start bound.
    start: Bound<Bytes>,
    /// End bound.
    end: Bound<Bytes>,
    /// Direction.
    direction: IteratorDirection,
}

impl DBIteratorBuilder {
    /// Create a new builder with default options.
    pub fn new(sequence: u64) -> Self {
        Self {
            sequence,
            start: Bound::Unbounded,
            end: Bound::Unbounded,
            direction: IteratorDirection::Forward,
        }
    }

    /// Set the start bound (inclusive).
    pub fn start(mut self, key: impl Into<Bytes>) -> Self {
        self.start = Bound::Included(key.into());
        self
    }

    /// Set the start bound (exclusive).
    pub fn start_exclusive(mut self, key: impl Into<Bytes>) -> Self {
        self.start = Bound::Excluded(key.into());
        self
    }

    /// Set the end bound (exclusive).
    pub fn end(mut self, key: impl Into<Bytes>) -> Self {
        self.end = Bound::Excluded(key.into());
        self
    }

    /// Set the end bound (inclusive).
    pub fn end_inclusive(mut self, key: impl Into<Bytes>) -> Self {
        self.end = Bound::Included(key.into());
        self
    }

    /// Set prefix for iteration.
    pub fn prefix(self, prefix: impl Into<Bytes>) -> Self {
        let prefix = prefix.into();
        let mut end_key = prefix.to_vec();
        
        // Increment last byte to create exclusive end bound
        // This handles prefix iteration correctly
        if let Some(last) = end_key.last_mut() {
            if *last < 255 {
                *last += 1;
                return self.start(prefix).end(Bytes::from(end_key));
            }
        }
        
        // If we can't increment (all 0xFF), just set start
        self.start(prefix)
    }

    /// Set the direction.
    pub fn direction(mut self, direction: IteratorDirection) -> Self {
        self.direction = direction;
        self
    }

    /// Build the iterator.
    pub fn build(self, db: &Arc<Database>) -> Result<DBIterator> {
        DBIterator::new(db, self.sequence, self.start, self.end, self.direction)
    }
}

/// Database iterator with snapshot isolation.
///
/// Provides ordered iteration over database contents as of a specific
/// sequence number. Handles merging of memtables and SSTables, and
/// properly filters out deleted keys.
pub struct DBIterator {
    /// Sorted entries visible at the snapshot.
    entries: Vec<(Bytes, Bytes)>,
    /// Current position.
    position: usize,
    /// Direction of iteration.
    direction: IteratorDirection,
    /// Whether iteration has started.
    started: bool,
}

impl DBIterator {
    /// Create a new iterator.
    fn new(
        db: &Arc<Database>,
        sequence: u64,
        start: Bound<Bytes>,
        end: Bound<Bytes>,
        direction: IteratorDirection,
    ) -> Result<Self> {
        // Collect all entries from all sources
        let mut entries_map: BTreeMap<Vec<u8>, MergedEntry> = BTreeMap::new();

        // 1. Collect from active memtable
        {
            let memtable = db.memtable_ref().read();
            for (internal_key, value) in memtable.iter() {
                Self::process_entry(&mut entries_map, &internal_key, &value, sequence);
            }
        }

        // 2. Collect from immutable memtables (newest first)
        {
            let imm_tables = db.imm_memtables_ref().read();
            for imm in imm_tables.iter().rev() {
                for (internal_key, value) in imm.iter() {
                    Self::process_entry(&mut entries_map, &internal_key, &value, sequence);
                }
            }
        }

        // 3. Collect from SSTables
        Self::collect_from_sstables(db, &mut entries_map, sequence)?;

        // 4. Filter by range and remove deleted entries
        let entries: Vec<(Bytes, Bytes)> = entries_map
            .into_iter()
            .filter_map(|(key, entry)| {
                // Skip deleted entries
                let value = entry.value?;
                
                // Check range bounds
                let key_bytes = Bytes::from(key);
                if !Self::in_range(&key_bytes, &start, &end) {
                    return None;
                }
                
                Some((key_bytes, value))
            })
            .collect();

        // Determine starting position based on direction
        let position = match direction {
            IteratorDirection::Forward => 0,
            IteratorDirection::Reverse => entries.len(),
        };

        Ok(Self {
            entries,
            position,
            direction,
            started: false,
        })
    }

    /// Process an entry and add to the map if visible.
    fn process_entry(
        map: &mut BTreeMap<Vec<u8>, MergedEntry>,
        internal_key: &InternalKey,
        value: &Bytes,
        snapshot_seq: u64,
    ) {
        // Skip entries newer than our snapshot
        if internal_key.sequence() > snapshot_seq {
            return;
        }

        let user_key = internal_key.user_key().to_vec();

        // Check if we already have a newer version of this key
        if let Some(existing) = map.get(&user_key) {
            if existing.sequence >= internal_key.sequence() {
                // Already have a newer or equal version
                return;
            }
        }

        // Determine value (None for deletions)
        let entry_value = if internal_key.is_deletion() {
            None
        } else {
            Some(value.clone())
        };

        map.insert(
            user_key.clone(),
            MergedEntry {
                user_key: Bytes::from(user_key),
                value: entry_value,
                sequence: internal_key.sequence(),
            },
        );
    }

    /// Collect entries from SSTables.
    fn collect_from_sstables(
        db: &Arc<Database>,
        map: &mut BTreeMap<Vec<u8>, MergedEntry>,
        snapshot_seq: u64,
    ) -> Result<()> {
        let version = db.versions_ref().current();
        let db_path = db.path();

        // Iterate through all levels
        for level in 0..7 {
            for file in version.files(level) {
                let table_path = table_file_path(db_path, file.file_number());
                let mut reader = SSTableReader::open(&table_path, file.file_number())?;
                
                // Use SSTableEntryIterator for simplicity
                let iter = crate::sstable::SSTableEntryIterator::new(&mut reader)?;
                
                for (key_bytes, value) in iter {
                    if let Some(internal_key) = InternalKey::decode(&key_bytes) {
                        Self::process_entry(map, &internal_key, &value, snapshot_seq);
                    }
                }
            }
        }

        Ok(())
    }

    /// Check if a key is within the range bounds.
    fn in_range(key: &Bytes, start: &Bound<Bytes>, end: &Bound<Bytes>) -> bool {
        // Check start bound
        let after_start = match start {
            Bound::Unbounded => true,
            Bound::Included(s) => key.as_ref() >= s.as_ref(),
            Bound::Excluded(s) => key.as_ref() > s.as_ref(),
        };

        if !after_start {
            return false;
        }

        // Check end bound
        match end {
            Bound::Unbounded => true,
            Bound::Included(e) => key.as_ref() <= e.as_ref(),
            Bound::Excluded(e) => key.as_ref() < e.as_ref(),
        }
    }

    /// Check if the iterator is valid (positioned at an entry).
    pub fn valid(&self) -> bool {
        match self.direction {
            IteratorDirection::Forward => self.position < self.entries.len(),
            IteratorDirection::Reverse => self.position > 0 && self.position <= self.entries.len(),
        }
    }

    /// Get the current key.
    pub fn key(&self) -> Option<&Bytes> {
        let idx = match self.direction {
            IteratorDirection::Forward => self.position,
            IteratorDirection::Reverse => self.position.checked_sub(1)?,
        };
        self.entries.get(idx).map(|(k, _)| k)
    }

    /// Get the current value.
    pub fn value(&self) -> Option<&Bytes> {
        let idx = match self.direction {
            IteratorDirection::Forward => self.position,
            IteratorDirection::Reverse => self.position.checked_sub(1)?,
        };
        self.entries.get(idx).map(|(_, v)| v)
    }

    /// Get the current entry as (key, value).
    pub fn entry(&self) -> Option<(&Bytes, &Bytes)> {
        let idx = match self.direction {
            IteratorDirection::Forward => self.position,
            IteratorDirection::Reverse => self.position.checked_sub(1)?,
        };
        self.entries.get(idx).map(|(k, v)| (k, v))
    }

    /// Move to the first entry.
    pub fn seek_to_first(&mut self) {
        self.started = true;
        self.position = 0;
    }

    /// Move to the last entry.
    pub fn seek_to_last(&mut self) {
        self.started = true;
        self.position = self.entries.len();
    }

    /// Seek to the first entry with key >= target.
    pub fn seek(&mut self, target: &[u8]) {
        self.started = true;
        self.position = self
            .entries
            .partition_point(|(k, _)| k.as_ref() < target);
    }

    /// Seek to the last entry with key <= target.
    pub fn seek_for_prev(&mut self, target: &[u8]) {
        self.started = true;
        let idx = self
            .entries
            .partition_point(|(k, _)| k.as_ref() <= target);
        self.position = idx; // For reverse, position points past the current entry
    }

    /// Move to the next entry.
    pub fn next(&mut self) {
        if !self.started {
            self.seek_to_first();
            return;
        }

        match self.direction {
            IteratorDirection::Forward => {
                if self.position < self.entries.len() {
                    self.position += 1;
                }
            }
            IteratorDirection::Reverse => {
                if self.position > 0 {
                    self.position -= 1;
                }
            }
        }
    }

    /// Move to the previous entry.
    pub fn prev(&mut self) {
        if !self.started {
            self.seek_to_last();
            return;
        }

        match self.direction {
            IteratorDirection::Forward => {
                if self.position > 0 {
                    self.position -= 1;
                }
            }
            IteratorDirection::Reverse => {
                if self.position < self.entries.len() {
                    self.position += 1;
                }
            }
        }
    }

    /// Get the number of entries.
    pub fn count(&self) -> usize {
        self.entries.len()
    }
}

impl Iterator for DBIterator {
    type Item = (Bytes, Bytes);

    fn next(&mut self) -> Option<Self::Item> {
        if !self.started {
            match self.direction {
                IteratorDirection::Forward => self.seek_to_first(),
                IteratorDirection::Reverse => self.seek_to_last(),
            }
        }

        let result = match self.direction {
            IteratorDirection::Forward => {
                if self.position < self.entries.len() {
                    let entry = self.entries[self.position].clone();
                    self.position += 1;
                    Some(entry)
                } else {
                    None
                }
            }
            IteratorDirection::Reverse => {
                if self.position > 0 {
                    self.position -= 1;
                    Some(self.entries[self.position].clone())
                } else {
                    None
                }
            }
        };

        if result.is_some() {
            self.started = true;
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn create_test_db() -> Arc<Database> {
        let dir = tempdir().unwrap();
        Database::open(dir.path()).unwrap()
    }

    #[test]
    fn test_iterator_empty() {
        let db = create_test_db();
        let iter = DBIteratorBuilder::new(u64::MAX)
            .build(&db)
            .unwrap();

        assert!(!iter.valid());
        assert_eq!(iter.count(), 0);
    }

    #[test]
    fn test_iterator_basic() {
        let db = create_test_db();

        // Insert some data
        db.put(b"key1", b"value1").unwrap();
        db.put(b"key2", b"value2").unwrap();
        db.put(b"key3", b"value3").unwrap();

        let mut iter = DBIteratorBuilder::new(u64::MAX)
            .build(&db)
            .unwrap();

        iter.seek_to_first();
        assert!(iter.valid());
        assert_eq!(iter.key().unwrap().as_ref(), b"key1");
        assert_eq!(iter.value().unwrap().as_ref(), b"value1");

        iter.next();
        assert!(iter.valid());
        assert_eq!(iter.key().unwrap().as_ref(), b"key2");

        iter.next();
        assert!(iter.valid());
        assert_eq!(iter.key().unwrap().as_ref(), b"key3");

        iter.next();
        assert!(!iter.valid());
    }

    #[test]
    fn test_iterator_seek() {
        let db = create_test_db();

        db.put(b"key1", b"value1").unwrap();
        db.put(b"key2", b"value2").unwrap();
        db.put(b"key3", b"value3").unwrap();

        let mut iter = DBIteratorBuilder::new(u64::MAX)
            .build(&db)
            .unwrap();

        // Seek to existing key
        iter.seek(b"key2");
        assert!(iter.valid());
        assert_eq!(iter.key().unwrap().as_ref(), b"key2");

        // Seek to non-existing key (should find next)
        iter.seek(b"key1x");
        assert!(iter.valid());
        assert_eq!(iter.key().unwrap().as_ref(), b"key2");

        // Seek past end
        iter.seek(b"zzz");
        assert!(!iter.valid());
    }

    #[test]
    fn test_iterator_range() {
        let db = create_test_db();

        db.put(b"key1", b"value1").unwrap();
        db.put(b"key2", b"value2").unwrap();
        db.put(b"key3", b"value3").unwrap();
        db.put(b"key4", b"value4").unwrap();
        db.put(b"key5", b"value5").unwrap();

        let iter = DBIteratorBuilder::new(u64::MAX)
            .start(Bytes::from("key2"))
            .end(Bytes::from("key4"))
            .build(&db)
            .unwrap();

        let keys: Vec<_> = iter.map(|(k, _)| k).collect();
        assert_eq!(keys.len(), 2);
        assert_eq!(keys[0].as_ref(), b"key2");
        assert_eq!(keys[1].as_ref(), b"key3");
    }

    #[test]
    fn test_iterator_prefix() {
        let db = create_test_db();

        db.put(b"user:1", b"alice").unwrap();
        db.put(b"user:2", b"bob").unwrap();
        db.put(b"user:3", b"charlie").unwrap();
        db.put(b"post:1", b"hello").unwrap();
        db.put(b"post:2", b"world").unwrap();

        let iter = DBIteratorBuilder::new(u64::MAX)
            .prefix(Bytes::from("user:"))
            .build(&db)
            .unwrap();

        let keys: Vec<_> = iter.map(|(k, _)| k).collect();
        assert_eq!(keys.len(), 3);
        assert!(keys.iter().all(|k| k.starts_with(b"user:")));
    }

    #[test]
    fn test_iterator_reverse() {
        let db = create_test_db();

        db.put(b"key1", b"value1").unwrap();
        db.put(b"key2", b"value2").unwrap();
        db.put(b"key3", b"value3").unwrap();

        let iter = DBIteratorBuilder::new(u64::MAX)
            .direction(IteratorDirection::Reverse)
            .build(&db)
            .unwrap();

        let keys: Vec<_> = iter.map(|(k, _)| k).collect();
        assert_eq!(keys.len(), 3);
        assert_eq!(keys[0].as_ref(), b"key3");
        assert_eq!(keys[1].as_ref(), b"key2");
        assert_eq!(keys[2].as_ref(), b"key1");
    }

    #[test]
    fn test_iterator_deleted_keys() {
        let db = create_test_db();

        db.put(b"key1", b"value1").unwrap();
        db.put(b"key2", b"value2").unwrap();
        db.put(b"key3", b"value3").unwrap();
        db.delete(b"key2").unwrap();

        let iter = DBIteratorBuilder::new(u64::MAX)
            .build(&db)
            .unwrap();

        let keys: Vec<_> = iter.map(|(k, _)| k).collect();
        assert_eq!(keys.len(), 2);
        assert_eq!(keys[0].as_ref(), b"key1");
        assert_eq!(keys[1].as_ref(), b"key3");
    }

    #[test]
    fn test_iterator_snapshot_isolation() {
        let db = create_test_db();

        // Write initial data
        db.put(b"key1", b"value1").unwrap();
        db.put(b"key2", b"value2").unwrap();

        // Get current sequence
        let snapshot_seq = db.sequence();

        // Write more data
        db.put(b"key3", b"value3").unwrap();
        db.put(b"key1", b"updated1").unwrap();

        // Iterator at old snapshot should see old data
        let iter = DBIteratorBuilder::new(snapshot_seq)
            .build(&db)
            .unwrap();

        let entries: Vec<_> = iter.collect();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].0.as_ref(), b"key1");
        assert_eq!(entries[0].1.as_ref(), b"value1"); // Old value
        assert_eq!(entries[1].0.as_ref(), b"key2");

        // Iterator at current sequence should see new data
        let iter = DBIteratorBuilder::new(u64::MAX)
            .build(&db)
            .unwrap();

        let entries: Vec<_> = iter.collect();
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].0.as_ref(), b"key1");
        assert_eq!(entries[0].1.as_ref(), b"updated1"); // New value
    }

    #[test]
    fn test_iterator_as_rust_iterator() {
        let db = create_test_db();

        db.put(b"a", b"1").unwrap();
        db.put(b"b", b"2").unwrap();
        db.put(b"c", b"3").unwrap();

        let iter = DBIteratorBuilder::new(u64::MAX).build(&db).unwrap();

        // Use standard iterator methods
        let sum: usize = iter
            .map(|(_, v)| {
                std::str::from_utf8(v.as_ref())
                    .unwrap()
                    .parse::<usize>()
                    .unwrap()
            })
            .sum();

        assert_eq!(sum, 6);
    }
}
