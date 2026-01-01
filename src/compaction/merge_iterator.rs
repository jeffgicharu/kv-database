//! Merge iterator for combining multiple sorted iterators.
//!
//! The merge iterator maintains a min-heap of iterators and always
//! returns entries in sorted order. When multiple iterators have
//! the same key, it returns them in order (newest first based on
//! sequence number in the internal key).

use std::cmp::Ordering;
use std::collections::BinaryHeap;

use bytes::Bytes;

use crate::types::InternalKey;
use crate::Result;

/// A single entry from an iterator.
#[derive(Debug, Clone)]
pub struct MergeEntry {
    /// The encoded internal key.
    pub key: Bytes,
    /// The value.
    pub value: Bytes,
    /// Iterator index (for tracking which iterator this came from).
    pub iterator_index: usize,
}

impl MergeEntry {
    /// Create a new merge entry.
    pub fn new(key: Bytes, value: Bytes, iterator_index: usize) -> Self {
        Self {
            key,
            value,
            iterator_index,
        }
    }

    /// Get the user key (without sequence/type suffix).
    pub fn user_key(&self) -> &[u8] {
        InternalKey::parse_user_key(&self.key).unwrap_or(&self.key)
    }

    /// Get the sequence number.
    pub fn sequence(&self) -> u64 {
        InternalKey::parse_sequence(&self.key).unwrap_or(0)
    }

    /// Check if this is a deletion marker.
    pub fn is_deletion(&self) -> bool {
        InternalKey::decode(&self.key)
            .map(|k| k.is_deletion())
            .unwrap_or(false)
    }
}

// For the min-heap, we need reverse ordering (smallest first)
impl PartialEq for MergeEntry {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}

impl Eq for MergeEntry {}

impl PartialOrd for MergeEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for MergeEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        // Reverse ordering for min-heap (BinaryHeap is a max-heap)
        // We want smaller keys first, and for same user key, higher sequences first
        match other.key.cmp(&self.key) {
            Ordering::Equal => {
                // Same key, use iterator index as tiebreaker
                // Lower index = newer data (for L0 files)
                other.iterator_index.cmp(&self.iterator_index)
            }
            ord => ord,
        }
    }
}

/// Trait for iterators that can be merged.
pub trait MergeSource {
    /// Check if the iterator is valid.
    fn valid(&self) -> bool;

    /// Get the current key.
    fn key(&self) -> Option<Bytes>;

    /// Get the current value.
    fn value(&self) -> Option<Bytes>;

    /// Move to the next entry.
    fn next(&mut self) -> Result<()>;

    /// Seek to the first entry.
    fn seek_to_first(&mut self) -> Result<()>;
}

/// A wrapper that holds an iterator and its current entry.
struct IteratorWrapper<I: MergeSource> {
    /// The underlying iterator.
    iterator: I,
    /// Iterator index.
    index: usize,
}

impl<I: MergeSource> IteratorWrapper<I> {
    fn new(iterator: I, index: usize) -> Self {
        Self { iterator, index }
    }

    fn current_entry(&self) -> Option<MergeEntry> {
        if self.iterator.valid() {
            Some(MergeEntry::new(
                self.iterator.key()?,
                self.iterator.value()?,
                self.index,
            ))
        } else {
            None
        }
    }
}

/// Merge iterator that combines multiple sorted iterators.
///
/// Entries are returned in sorted order. For entries with the same key,
/// they are returned in iterator order (lower index first, which typically
/// means newer data for L0 files).
pub struct MergeIterator<I: MergeSource> {
    /// Iterator wrappers.
    iterators: Vec<IteratorWrapper<I>>,
    /// Min-heap of current entries.
    heap: BinaryHeap<MergeEntry>,
    /// Current entry.
    current: Option<MergeEntry>,
}

impl<I: MergeSource> MergeIterator<I> {
    /// Create a new merge iterator from multiple sources.
    pub fn new(sources: Vec<I>) -> Self {
        let iterators: Vec<_> = sources
            .into_iter()
            .enumerate()
            .map(|(i, it)| IteratorWrapper::new(it, i))
            .collect();

        Self {
            iterators,
            heap: BinaryHeap::new(),
            current: None,
        }
    }

    /// Seek to the first entry.
    pub fn seek_to_first(&mut self) -> Result<()> {
        self.heap.clear();
        self.current = None;

        // Seek all iterators to first
        for wrapper in &mut self.iterators {
            wrapper.iterator.seek_to_first()?;
            if let Some(entry) = wrapper.current_entry() {
                self.heap.push(entry);
            }
        }

        // Pop the first entry
        self.advance()?;

        Ok(())
    }

    /// Check if the iterator is valid.
    pub fn valid(&self) -> bool {
        self.current.is_some()
    }

    /// Get the current entry.
    pub fn current(&self) -> Option<&MergeEntry> {
        self.current.as_ref()
    }

    /// Get the current key.
    pub fn key(&self) -> Option<&Bytes> {
        self.current.as_ref().map(|e| &e.key)
    }

    /// Get the current value.
    pub fn value(&self) -> Option<&Bytes> {
        self.current.as_ref().map(|e| &e.value)
    }

    /// Move to the next entry.
    pub fn next(&mut self) -> Result<()> {
        self.advance()
    }

    /// Advance to the next entry.
    fn advance(&mut self) -> Result<()> {
        // If we had a current entry, advance its source iterator
        if let Some(ref entry) = self.current {
            let idx = entry.iterator_index;
            let wrapper = &mut self.iterators[idx];
            wrapper.iterator.next()?;

            // Add next entry from this iterator to heap
            if let Some(new_entry) = wrapper.current_entry() {
                self.heap.push(new_entry);
            }
        }

        // Pop the next smallest entry
        self.current = self.heap.pop();

        Ok(())
    }
}

/// A simple in-memory merge source for testing.
pub struct VecMergeSource {
    entries: Vec<(Bytes, Bytes)>,
    position: usize,
}

impl VecMergeSource {
    /// Create a new vector-based merge source.
    pub fn new(entries: Vec<(Bytes, Bytes)>) -> Self {
        Self {
            entries,
            position: 0,
        }
    }
}

impl MergeSource for VecMergeSource {
    fn valid(&self) -> bool {
        self.position < self.entries.len()
    }

    fn key(&self) -> Option<Bytes> {
        self.entries.get(self.position).map(|(k, _)| k.clone())
    }

    fn value(&self) -> Option<Bytes> {
        self.entries.get(self.position).map(|(_, v)| v.clone())
    }

    fn next(&mut self) -> Result<()> {
        if self.position < self.entries.len() {
            self.position += 1;
        }
        Ok(())
    }

    fn seek_to_first(&mut self) -> Result<()> {
        self.position = 0;
        Ok(())
    }
}

/// Deduplicating merge iterator.
///
/// This iterator wraps a MergeIterator and handles duplicate keys,
/// keeping only the newest version of each key.
pub struct DedupMergeIterator<I: MergeSource> {
    /// Inner merge iterator.
    inner: MergeIterator<I>,
    /// Last user key seen.
    last_user_key: Option<Bytes>,
    /// Whether we've started iterating.
    started: bool,
}

impl<I: MergeSource> DedupMergeIterator<I> {
    /// Create a new deduplicating merge iterator.
    pub fn new(sources: Vec<I>) -> Self {
        Self {
            inner: MergeIterator::new(sources),
            last_user_key: None,
            started: false,
        }
    }

    /// Seek to the first entry.
    pub fn seek_to_first(&mut self) -> Result<()> {
        self.inner.seek_to_first()?;
        self.last_user_key = None;
        self.started = true;
        Ok(())
    }

    /// Check if the iterator is valid.
    pub fn valid(&self) -> bool {
        self.inner.valid()
    }

    /// Get the current entry.
    pub fn current(&self) -> Option<&MergeEntry> {
        self.inner.current()
    }

    /// Move to the next unique key.
    ///
    /// This skips over duplicate versions of the same user key,
    /// keeping only the newest (first encountered) version.
    pub fn next(&mut self) -> Result<()> {
        if !self.started {
            return self.seek_to_first();
        }

        // Remember the current user key
        if let Some(entry) = self.inner.current() {
            self.last_user_key = Some(Bytes::copy_from_slice(entry.user_key()));
        }

        // Advance and skip entries with the same user key
        loop {
            self.inner.next()?;

            match self.inner.current() {
                None => break,
                Some(entry) => {
                    let user_key = entry.user_key();
                    if let Some(ref last) = self.last_user_key {
                        if user_key != last.as_ref() {
                            // Different user key, stop here
                            break;
                        }
                        // Same user key, continue skipping
                    } else {
                        // No last key, this is a new key
                        break;
                    }
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_entries(keys: &[&str]) -> Vec<(Bytes, Bytes)> {
        keys.iter()
            .map(|k| {
                (
                    Bytes::copy_from_slice(k.as_bytes()),
                    Bytes::from(format!("value_{}", k)),
                )
            })
            .collect()
    }

    fn create_internal_entries(keys: &[(u64, &str)]) -> Vec<(Bytes, Bytes)> {
        keys.iter()
            .map(|(seq, k)| {
                let internal_key =
                    InternalKey::for_value(Bytes::copy_from_slice(k.as_bytes()), *seq);
                (internal_key.encode(), Bytes::from(format!("value_{}_{}", k, seq)))
            })
            .collect()
    }

    #[test]
    fn test_merge_iterator_single_source() {
        let source = VecMergeSource::new(create_entries(&["a", "b", "c"]));
        let mut iter = MergeIterator::new(vec![source]);

        iter.seek_to_first().unwrap();

        let mut keys = Vec::new();
        while iter.valid() {
            keys.push(iter.key().unwrap().clone());
            iter.next().unwrap();
        }

        assert_eq!(keys.len(), 3);
        assert_eq!(keys[0].as_ref(), b"a");
        assert_eq!(keys[1].as_ref(), b"b");
        assert_eq!(keys[2].as_ref(), b"c");
    }

    #[test]
    fn test_merge_iterator_multiple_sources() {
        let source1 = VecMergeSource::new(create_entries(&["a", "c", "e"]));
        let source2 = VecMergeSource::new(create_entries(&["b", "d", "f"]));
        let mut iter = MergeIterator::new(vec![source1, source2]);

        iter.seek_to_first().unwrap();

        let mut keys = Vec::new();
        while iter.valid() {
            keys.push(iter.key().unwrap().clone());
            iter.next().unwrap();
        }

        assert_eq!(keys.len(), 6);
        assert_eq!(keys[0].as_ref(), b"a");
        assert_eq!(keys[1].as_ref(), b"b");
        assert_eq!(keys[2].as_ref(), b"c");
        assert_eq!(keys[3].as_ref(), b"d");
        assert_eq!(keys[4].as_ref(), b"e");
        assert_eq!(keys[5].as_ref(), b"f");
    }

    #[test]
    fn test_merge_iterator_overlapping_keys() {
        let source1 = VecMergeSource::new(create_entries(&["a", "b", "c"]));
        let source2 = VecMergeSource::new(create_entries(&["b", "c", "d"]));
        let mut iter = MergeIterator::new(vec![source1, source2]);

        iter.seek_to_first().unwrap();

        let mut keys = Vec::new();
        while iter.valid() {
            keys.push(iter.key().unwrap().clone());
            iter.next().unwrap();
        }

        // Should have duplicates: a, b, b, c, c, d
        assert_eq!(keys.len(), 6);
    }

    #[test]
    fn test_merge_iterator_empty_sources() {
        let source1 = VecMergeSource::new(vec![]);
        let source2 = VecMergeSource::new(create_entries(&["a"]));
        let mut iter = MergeIterator::new(vec![source1, source2]);

        iter.seek_to_first().unwrap();
        assert!(iter.valid());
        assert_eq!(iter.key().unwrap().as_ref(), b"a");

        iter.next().unwrap();
        assert!(!iter.valid());
    }

    #[test]
    fn test_dedup_merge_iterator() {
        // Create entries with internal keys (user_key, sequence)
        // Higher sequence = newer, should be kept
        let source1 = VecMergeSource::new(create_internal_entries(&[
            (10, "a"),
            (10, "c"),
        ]));
        let source2 = VecMergeSource::new(create_internal_entries(&[
            (5, "a"),  // Older version of 'a'
            (15, "b"), // Only version of 'b'
            (5, "c"),  // Older version of 'c'
        ]));

        let mut iter = DedupMergeIterator::new(vec![source1, source2]);
        iter.seek_to_first().unwrap();

        let mut keys = Vec::new();
        while iter.valid() {
            if let Some(entry) = iter.current() {
                keys.push((entry.user_key().to_vec(), entry.sequence()));
            }
            iter.next().unwrap();
        }

        // Should have: a@10, b@15, c@10 (newest version of each)
        assert_eq!(keys.len(), 3);
    }

    #[test]
    fn test_merge_entry_ordering() {
        let entry1 = MergeEntry::new(Bytes::from("a"), Bytes::from("v1"), 0);
        let entry2 = MergeEntry::new(Bytes::from("b"), Bytes::from("v2"), 0);

        // Min-heap ordering: smaller key should come first (be "greater" in max-heap terms)
        assert!(entry1 > entry2);
    }
}
