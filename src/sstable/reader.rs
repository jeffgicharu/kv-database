//! SSTable reader for reading immutable sorted files.

use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;
use std::sync::Arc;

use bytes::Bytes;

use crate::Result;

use super::block::Block;
use super::filter::BloomFilter;
use super::{BlockHandle, Footer, BLOCK_TRAILER_SIZE, FOOTER_SIZE};

/// Reader for SSTable files.
///
/// Provides efficient key lookups and iteration using the index
/// and bloom filter.
pub struct SSTableReader {
    /// File handle.
    file: File,
    /// File size.
    file_size: u64,
    /// Parsed footer.
    footer: Footer,
    /// Index block.
    index_block: Arc<Block>,
    /// Bloom filter (if present).
    filter: Option<BloomFilter>,
    /// File number.
    file_number: u64,
}

impl SSTableReader {
    /// Open an SSTable file for reading.
    pub fn open(path: &Path, file_number: u64) -> Result<Self> {
        let mut file = File::open(path)?;
        let file_size = file.metadata()?.len();

        if file_size < FOOTER_SIZE as u64 {
            return Err(crate::Error::corruption("file too small for footer"));
        }

        // Read footer
        let mut footer_buf = vec![0u8; FOOTER_SIZE];
        file.seek(SeekFrom::End(-(FOOTER_SIZE as i64)))?;
        file.read_exact(&mut footer_buf)?;
        let footer = Footer::decode(&footer_buf)?;

        // Read index block
        let index_data = Self::read_block_data(&mut file, &footer.index_handle)?;
        let index_block = Arc::new(Block::new_with_trailer(&index_data)?);

        // Read filter block (if present)
        let filter = if footer.filter_handle.size() > 0 {
            let filter_data = Self::read_block_data(&mut file, &footer.filter_handle)?;
            BloomFilter::from_bytes(Bytes::from(filter_data))
        } else {
            None
        };

        Ok(Self {
            file,
            file_size,
            footer,
            index_block,
            filter,
            file_number,
        })
    }

    /// Read raw block data from the file.
    fn read_block_data(file: &mut File, handle: &BlockHandle) -> Result<Vec<u8>> {
        let total_size = handle.size() as usize;
        let mut data = vec![0u8; total_size];

        file.seek(SeekFrom::Start(handle.offset()))?;
        file.read_exact(&mut data)?;

        Ok(data)
    }

    /// Get the file number.
    pub fn file_number(&self) -> u64 {
        self.file_number
    }

    /// Get the file size.
    pub fn file_size(&self) -> u64 {
        self.file_size
    }

    /// Check if a key might exist using the bloom filter.
    pub fn may_contain(&self, key: &[u8]) -> bool {
        match &self.filter {
            Some(filter) => filter.may_contain(key),
            None => true, // No filter, assume it might contain
        }
    }

    /// Get a value by exact key match.
    ///
    /// Returns None if the key doesn't exist.
    pub fn get(&mut self, key: &[u8]) -> Result<Option<Bytes>> {
        // Check bloom filter first
        if !self.may_contain(key) {
            return Ok(None);
        }

        // Find the data block that might contain this key
        let mut index_iter = self.index_block.iter();
        index_iter.seek(key);

        if !index_iter.valid() {
            return Ok(None);
        }

        // Parse the block handle from the index entry value
        let handle_data = index_iter.value();
        let mut cursor = handle_data.as_ref();
        let handle = BlockHandle::decode(&mut cursor)?;

        // Read and search the data block
        let block_data = Self::read_block_data(&mut self.file, &handle)?;
        let block = Block::new_with_trailer(&block_data)?;

        let mut block_iter = block.iter();
        block_iter.seek(key);

        if block_iter.valid() && block_iter.key() == key {
            Ok(Some(block_iter.value().clone()))
        } else {
            Ok(None)
        }
    }

    /// MVCC-aware lookup: find value for user_key at given sequence.
    ///
    /// This method handles internal keys with sequence numbers. It searches
    /// for entries with the same user_key and returns the value with the
    /// highest sequence number <= target_sequence.
    ///
    /// Returns:
    /// - Ok(Some(value)) if a valid value was found
    /// - Ok(None) if the key doesn't exist or was deleted
    /// - Err if there was a read error
    pub fn get_user_key(
        &mut self,
        user_key: &[u8],
        target_sequence: u64,
    ) -> Result<Option<Bytes>> {
        use crate::types::{InternalKey, ValueType};

        // Check bloom filter first
        if !self.may_contain(user_key) {
            return Ok(None);
        }

        // Create a lookup key with sequence 0 to find the first entry for this user_key
        // (since lower sequences have lower encoded values)
        let start_key = InternalKey::new(
            Bytes::copy_from_slice(user_key),
            0,
            ValueType::Value,
        );
        let start_encoded = start_key.encode();

        // Find the data block that might contain this key
        let mut index_iter = self.index_block.iter();
        index_iter.seek(&start_encoded);

        // We may need to search multiple blocks
        let mut best_value: Option<Bytes> = None;
        let mut best_sequence: u64 = 0;
        let mut found_deletion = false;

        while index_iter.valid() {
            // Parse the block handle from the index entry value
            let handle_data = index_iter.value();
            let mut cursor = handle_data.as_ref();
            let handle = BlockHandle::decode(&mut cursor)?;

            // Read and search the data block
            let block_data = Self::read_block_data(&mut self.file, &handle)?;
            let block = Block::new_with_trailer(&block_data)?;

            let mut block_iter = block.iter();
            block_iter.seek(&start_encoded);

            while block_iter.valid() {
                let key_bytes = block_iter.key();

                // Parse the internal key
                if let Some(found_user_key) = InternalKey::parse_user_key(&key_bytes) {
                    // Check if this is still the same user key
                    if found_user_key != user_key {
                        // Moved past our user key, we're done
                        if let Some(value) = best_value {
                            if found_deletion {
                                return Ok(None);
                            }
                            return Ok(Some(value));
                        }
                        return Ok(None);
                    }

                    // Check sequence number
                    if let Some(seq) = InternalKey::parse_sequence(&key_bytes) {
                        if seq <= target_sequence && seq > best_sequence {
                            // This is a better match
                            best_sequence = seq;

                            // Check if it's a deletion
                            if let Some(internal_key) = InternalKey::decode(&key_bytes) {
                                if internal_key.is_deletion() {
                                    found_deletion = true;
                                    best_value = None;
                                } else {
                                    found_deletion = false;
                                    best_value = Some(block_iter.value().clone());
                                }
                            }
                        }
                    }
                }

                block_iter.next();
            }

            index_iter.next();
        }

        if found_deletion {
            Ok(None)
        } else {
            Ok(best_value)
        }
    }

    /// Read a data block by handle.
    pub fn read_block(&mut self, handle: &BlockHandle) -> Result<Block> {
        let block_data = Self::read_block_data(&mut self.file, handle)?;
        Block::new_with_trailer(&block_data)
    }

    /// Get the index block for iteration.
    pub fn index_block(&self) -> &Arc<Block> {
        &self.index_block
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sstable::{CompressionType, SSTableWriter};
    use tempfile::tempdir;

    fn create_test_sstable(
        path: &Path,
        entries: &[(Vec<u8>, Vec<u8>)],
    ) -> crate::Result<()> {
        let mut writer = SSTableWriter::new(path, 1, CompressionType::None, 10)?;
        for (key, value) in entries {
            writer.add(key, value)?;
        }
        writer.finish()?;
        Ok(())
    }

    #[test]
    fn test_sstable_reader_empty() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.sst");

        create_test_sstable(&path, &[]).unwrap();

        let mut reader = SSTableReader::open(&path, 1).unwrap();
        assert!(reader.get(b"key").unwrap().is_none());
    }

    #[test]
    fn test_sstable_reader_single_entry() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.sst");

        create_test_sstable(&path, &[(b"key".to_vec(), b"value".to_vec())]).unwrap();

        let mut reader = SSTableReader::open(&path, 1).unwrap();
        let value = reader.get(b"key").unwrap().unwrap();
        assert_eq!(value.as_ref(), b"value");
    }

    #[test]
    fn test_sstable_reader_multiple_entries() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.sst");

        let entries: Vec<(Vec<u8>, Vec<u8>)> = (0..100)
            .map(|i| {
                (
                    format!("key_{:04}", i).into_bytes(),
                    format!("value_{}", i).into_bytes(),
                )
            })
            .collect();

        create_test_sstable(&path, &entries).unwrap();

        let mut reader = SSTableReader::open(&path, 1).unwrap();

        // Test all entries
        for (key, expected_value) in &entries {
            let value = reader.get(key).unwrap().unwrap();
            assert_eq!(value.as_ref(), expected_value.as_slice());
        }

        // Test non-existent key
        assert!(reader.get(b"nonexistent").unwrap().is_none());
    }

    #[test]
    fn test_sstable_reader_bloom_filter() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.sst");

        let entries: Vec<(Vec<u8>, Vec<u8>)> = (0..100)
            .map(|i| {
                (
                    format!("key_{:04}", i).into_bytes(),
                    format!("value_{}", i).into_bytes(),
                )
            })
            .collect();

        create_test_sstable(&path, &entries).unwrap();

        let reader = SSTableReader::open(&path, 1).unwrap();

        // Keys that exist should pass bloom filter
        for (key, _) in &entries {
            assert!(reader.may_contain(key));
        }

        // Some non-existent keys should fail bloom filter (not all due to false positives)
        let mut filtered_count = 0;
        for i in 100..200 {
            let key = format!("key_{:04}", i);
            if !reader.may_contain(key.as_bytes()) {
                filtered_count += 1;
            }
        }
        // Expect most to be filtered (bloom filter should catch most)
        assert!(filtered_count > 80, "Bloom filter should filter most non-existent keys");
    }

    #[test]
    fn test_sstable_reader_with_compression() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.sst");

        // Create SSTable with Snappy compression
        let mut writer = SSTableWriter::new(&path, 1, CompressionType::Snappy, 10).unwrap();
        for i in 0..50 {
            let key = format!("key_{:04}", i);
            let value = "x".repeat(100); // Compressible data
            writer.add(key.as_bytes(), value.as_bytes()).unwrap();
        }
        writer.finish().unwrap();

        // Read it back
        let mut reader = SSTableReader::open(&path, 1).unwrap();
        for i in 0..50 {
            let key = format!("key_{:04}", i);
            let value = reader.get(key.as_bytes()).unwrap().unwrap();
            assert_eq!(value.len(), 100);
            assert!(value.iter().all(|&b| b == b'x'));
        }
    }
}
