//! SSTable iterator for efficient range scans.
//!
//! Provides two-level iteration: index block → data blocks.

use std::sync::Arc;

use bytes::Bytes;

use crate::Result;

use super::block::{Block, BlockHandle, BlockIterator};
use super::reader::SSTableReader;

/// Iterator over an SSTable.
///
/// Uses two-level iteration: the index block points to data blocks,
/// and we iterate through data blocks lazily.
pub struct SSTableIterator<'a> {
    /// Reference to the SSTable reader.
    reader: &'a mut SSTableReader,
    /// Index block iterator.
    index_iter: BlockIterator<'static>,
    /// Current data block.
    data_block: Option<Block>,
    /// Current data block iterator.
    data_iter: Option<BlockIterator<'static>>,
    /// Whether the iterator is valid.
    valid: bool,
    /// Shared reference to index block (to extend lifetime).
    _index_block: Arc<Block>,
}

impl<'a> SSTableIterator<'a> {
    /// Create a new SSTable iterator.
    pub fn new(reader: &'a mut SSTableReader) -> Self {
        let index_block = reader.index_block().clone();

        // Safety: We're holding an Arc<Block> that keeps the block alive,
        // and we'll only access the iterator while holding the SSTableIterator.
        let index_iter = unsafe {
            std::mem::transmute::<BlockIterator<'_>, BlockIterator<'static>>(index_block.iter())
        };

        Self {
            reader,
            index_iter,
            data_block: None,
            data_iter: None,
            valid: false,
            _index_block: index_block,
        }
    }

    /// Check if the iterator is valid.
    pub fn valid(&self) -> bool {
        self.valid
    }

    /// Get the current key.
    pub fn key(&self) -> Option<&[u8]> {
        self.data_iter.as_ref().and_then(|iter| {
            if iter.valid() {
                Some(iter.key())
            } else {
                None
            }
        })
    }

    /// Get the current value.
    pub fn value(&self) -> Option<&Bytes> {
        self.data_iter.as_ref().and_then(|iter| {
            if iter.valid() {
                Some(iter.value())
            } else {
                None
            }
        })
    }

    /// Seek to the first entry.
    pub fn seek_to_first(&mut self) -> Result<()> {
        self.index_iter.seek_to_first();
        self.init_data_block()?;
        self.skip_empty_data_blocks()?;
        Ok(())
    }

    /// Seek to the first entry with key >= target.
    pub fn seek(&mut self, target: &[u8]) -> Result<()> {
        // Seek in index
        self.index_iter.seek(target);

        if !self.index_iter.valid() {
            self.valid = false;
            return Ok(());
        }

        // Load the data block
        self.init_data_block()?;

        // Seek within data block
        if let Some(ref mut data_iter) = self.data_iter {
            data_iter.seek(target);
            self.valid = data_iter.valid();

            // If not found in this block, try next blocks
            if !self.valid {
                self.next_block()?;
            }
        }

        Ok(())
    }

    /// Move to the next entry.
    pub fn next(&mut self) -> Result<()> {
        if !self.valid {
            return Ok(());
        }

        if let Some(ref mut data_iter) = self.data_iter {
            data_iter.next();

            if data_iter.valid() {
                return Ok(());
            }
        }

        // Need to move to next data block
        self.next_block()
    }

    /// Initialize the data block from the current index entry.
    fn init_data_block(&mut self) -> Result<()> {
        if !self.index_iter.valid() {
            self.valid = false;
            self.data_block = None;
            self.data_iter = None;
            return Ok(());
        }

        // Parse block handle from index entry
        let handle_data = self.index_iter.value();
        let mut cursor = handle_data.as_ref();
        let handle = BlockHandle::decode(&mut cursor)?;

        // Read the data block
        let block = self.reader.read_block(&handle)?;
        self.data_block = Some(block);

        // Create iterator for the data block
        // Safety: data_block is stored in self and will live as long as data_iter
        if let Some(ref block) = self.data_block {
            let iter = unsafe {
                std::mem::transmute::<BlockIterator<'_>, BlockIterator<'static>>(block.iter())
            };
            self.data_iter = Some(iter);

            if let Some(ref mut data_iter) = self.data_iter {
                data_iter.seek_to_first();
                self.valid = data_iter.valid();
            }
        }

        Ok(())
    }

    /// Move to the next data block.
    fn next_block(&mut self) -> Result<()> {
        self.index_iter.next();
        self.init_data_block()?;
        self.skip_empty_data_blocks()
    }

    /// Skip empty data blocks.
    fn skip_empty_data_blocks(&mut self) -> Result<()> {
        while self.index_iter.valid() {
            if let Some(ref data_iter) = self.data_iter {
                if data_iter.valid() {
                    self.valid = true;
                    return Ok(());
                }
            }

            self.index_iter.next();
            if self.index_iter.valid() {
                self.init_data_block()?;
            }
        }

        self.valid = false;
        Ok(())
    }
}

/// A simpler iterator that collects all entries.
///
/// This is less memory efficient but safer and easier to use.
pub struct SSTableEntryIterator {
    /// All entries from the SSTable.
    entries: Vec<(Bytes, Bytes)>,
    /// Current position.
    position: usize,
}

impl SSTableEntryIterator {
    /// Create a new iterator by reading all entries.
    pub fn new(reader: &mut SSTableReader) -> Result<Self> {
        let mut entries = Vec::new();
        let index_block = reader.index_block().clone();

        for (_, handle_data) in index_block.iter() {
            let mut cursor = handle_data.as_ref();
            let handle = BlockHandle::decode(&mut cursor)?;

            let block = reader.read_block(&handle)?;
            for (key, value) in block.iter() {
                entries.push((key, value));
            }
        }

        Ok(Self {
            entries,
            position: 0,
        })
    }

    /// Check if the iterator is valid.
    pub fn valid(&self) -> bool {
        self.position < self.entries.len()
    }

    /// Get the current entry.
    pub fn current(&self) -> Option<(&Bytes, &Bytes)> {
        self.entries
            .get(self.position)
            .map(|(k, v)| (k, v))
    }

    /// Move to the next entry.
    pub fn next(&mut self) {
        if self.position < self.entries.len() {
            self.position += 1;
        }
    }

    /// Seek to the first entry with key >= target.
    pub fn seek(&mut self, target: &[u8]) {
        self.position = self
            .entries
            .partition_point(|(k, _)| k.as_ref() < target);
    }

    /// Seek to the first entry.
    pub fn seek_to_first(&mut self) {
        self.position = 0;
    }

    /// Get all entries.
    pub fn entries(&self) -> &[(Bytes, Bytes)] {
        &self.entries
    }
}

impl Iterator for SSTableEntryIterator {
    type Item = (Bytes, Bytes);

    fn next(&mut self) -> Option<Self::Item> {
        if self.position < self.entries.len() {
            let entry = self.entries[self.position].clone();
            self.position += 1;
            Some(entry)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sstable::{CompressionType, SSTableWriter};
    use tempfile::tempdir;

    fn create_test_sstable(path: &std::path::Path, count: usize) -> Result<()> {
        let mut writer = SSTableWriter::new(path, 1, CompressionType::None, 10)?;
        for i in 0..count {
            let key = format!("key_{:04}", i);
            let value = format!("value_{}", i);
            writer.add(key.as_bytes(), value.as_bytes())?;
        }
        writer.finish()?;
        Ok(())
    }

    #[test]
    fn test_sstable_entry_iterator_empty() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.sst");

        create_test_sstable(&path, 0).unwrap();

        let mut reader = SSTableReader::open(&path, 1).unwrap();
        let iter = SSTableEntryIterator::new(&mut reader).unwrap();

        assert!(!iter.valid());
        assert_eq!(iter.entries().len(), 0);
    }

    #[test]
    fn test_sstable_entry_iterator_all_entries() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.sst");

        create_test_sstable(&path, 100).unwrap();

        let mut reader = SSTableReader::open(&path, 1).unwrap();
        let iter = SSTableEntryIterator::new(&mut reader).unwrap();

        let entries: Vec<_> = iter.collect();
        assert_eq!(entries.len(), 100);

        // Verify order and content
        for (i, (key, value)) in entries.iter().enumerate() {
            let expected_key = format!("key_{:04}", i);
            let expected_value = format!("value_{}", i);
            assert_eq!(key.as_ref(), expected_key.as_bytes());
            assert_eq!(value.as_ref(), expected_value.as_bytes());
        }
    }

    #[test]
    fn test_sstable_entry_iterator_seek() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.sst");

        create_test_sstable(&path, 100).unwrap();

        let mut reader = SSTableReader::open(&path, 1).unwrap();
        let mut iter = SSTableEntryIterator::new(&mut reader).unwrap();

        // Seek to existing key
        iter.seek(b"key_0050");
        assert!(iter.valid());
        let (key, _) = iter.current().unwrap();
        assert_eq!(key.as_ref(), b"key_0050");

        // Seek to non-existing key (should find next)
        iter.seek(b"key_0050x");
        assert!(iter.valid());
        let (key, _) = iter.current().unwrap();
        assert_eq!(key.as_ref(), b"key_0051");

        // Seek past end
        iter.seek(b"zzz");
        assert!(!iter.valid());
    }

    #[test]
    fn test_sstable_iterator_basic() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.sst");

        create_test_sstable(&path, 10).unwrap();

        let mut reader = SSTableReader::open(&path, 1).unwrap();
        let mut iter = SSTableIterator::new(&mut reader);

        iter.seek_to_first().unwrap();
        assert!(iter.valid());

        let mut count = 0;
        while iter.valid() {
            assert!(iter.key().is_some());
            assert!(iter.value().is_some());
            count += 1;
            iter.next().unwrap();
        }

        assert_eq!(count, 10);
    }

    #[test]
    fn test_sstable_iterator_seek() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.sst");

        create_test_sstable(&path, 100).unwrap();

        let mut reader = SSTableReader::open(&path, 1).unwrap();
        let mut iter = SSTableIterator::new(&mut reader);

        // Seek to specific key
        iter.seek(b"key_0050").unwrap();
        assert!(iter.valid());
        assert_eq!(iter.key().unwrap(), b"key_0050");

        // Continue iterating
        iter.next().unwrap();
        assert!(iter.valid());
        assert_eq!(iter.key().unwrap(), b"key_0051");
    }
}
