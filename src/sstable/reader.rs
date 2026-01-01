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

    /// Get a value by key.
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
