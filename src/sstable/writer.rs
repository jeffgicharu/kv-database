//! SSTable writer for building immutable sorted files.

use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;

use bytes::{BufMut, Bytes, BytesMut};

use crate::Result;

use super::block_builder::BlockBuilder;
use super::filter::BloomFilterBuilder;
use super::{BlockHandle, CompressionType, Footer, DEFAULT_BLOCK_SIZE, FOOTER_SIZE};

/// Writer for building SSTable files.
///
/// Writes key-value pairs in sorted order, building data blocks,
/// a Bloom filter, and an index.
pub struct SSTableWriter {
    /// Buffered file writer.
    writer: BufWriter<File>,
    /// Current offset in the file.
    offset: u64,
    /// Block builder for data blocks.
    data_block: BlockBuilder,
    /// Index block builder.
    index_block: BlockBuilder,
    /// Bloom filter builder.
    filter_builder: BloomFilterBuilder,
    /// Last key written (for index).
    last_key: Vec<u8>,
    /// Pending index entry (written after data block is flushed).
    pending_index_entry: Option<BlockHandle>,
    /// Compression type.
    compression: CompressionType,
    /// Target block size.
    block_size: usize,
    /// Number of entries written.
    entry_count: u64,
    /// File number.
    file_number: u64,
    /// Smallest key in the SSTable.
    smallest_key: Option<Bytes>,
    /// Largest key in the SSTable.
    largest_key: Option<Bytes>,
}

impl SSTableWriter {
    /// Create a new SSTable writer.
    pub fn new(
        path: &Path,
        file_number: u64,
        compression: CompressionType,
        bits_per_key: usize,
    ) -> Result<Self> {
        Self::with_block_size(path, file_number, compression, bits_per_key, DEFAULT_BLOCK_SIZE)
    }

    /// Create a new SSTable writer with custom block size.
    pub fn with_block_size(
        path: &Path,
        file_number: u64,
        compression: CompressionType,
        bits_per_key: usize,
        block_size: usize,
    ) -> Result<Self> {
        let file = File::create(path)?;

        Ok(Self {
            writer: BufWriter::with_capacity(block_size * 4, file),
            offset: 0,
            data_block: BlockBuilder::with_options(block_size, 16),
            index_block: BlockBuilder::with_options(block_size, 1), // No compression for index
            filter_builder: BloomFilterBuilder::new(bits_per_key),
            last_key: Vec::new(),
            pending_index_entry: None,
            compression,
            block_size,
            entry_count: 0,
            file_number,
            smallest_key: None,
            largest_key: None,
        })
    }

    /// Get the file number.
    pub fn file_number(&self) -> u64 {
        self.file_number
    }

    /// Get the number of entries written.
    pub fn entry_count(&self) -> u64 {
        self.entry_count
    }

    /// Get the smallest key.
    pub fn smallest_key(&self) -> Option<&Bytes> {
        self.smallest_key.as_ref()
    }

    /// Get the largest key.
    pub fn largest_key(&self) -> Option<&Bytes> {
        self.largest_key.as_ref()
    }

    /// Add a key-value pair.
    ///
    /// Keys must be added in sorted order.
    pub fn add(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        debug_assert!(
            self.last_key.is_empty() || key > self.last_key.as_slice(),
            "keys must be added in sorted order"
        );

        // Write pending index entry for previous block
        if let Some(handle) = self.pending_index_entry.take() {
            // Use separator between last key of prev block and first key of new block
            let separator = find_short_separator(&self.last_key, key);
            self.write_index_entry(&separator, handle)?;
        }

        // Track smallest/largest keys
        if self.smallest_key.is_none() {
            self.smallest_key = Some(Bytes::copy_from_slice(key));
        }
        self.largest_key = Some(Bytes::copy_from_slice(key));

        // Add user key to bloom filter (internal key is user_key + 8 bytes of seq/type)
        // This allows bloom filter lookups by user key
        // Only strip the 8-byte suffix if the key is long enough to be an internal key
        // (i.e., has at least 1 byte of user key + 8 bytes of seq/type)
        if key.len() > 8 {
            self.filter_builder.add(&key[..key.len() - 8]);
        } else {
            // For short keys or raw keys (not internal keys), add as-is
            self.filter_builder.add(key);
        }

        // Add to data block
        self.data_block.add(key, value);
        self.last_key.clear();
        self.last_key.extend_from_slice(key);
        self.entry_count += 1;

        // Flush block if it's large enough
        if self.data_block.should_flush() {
            self.flush_data_block()?;
        }

        Ok(())
    }

    /// Flush the current data block to disk.
    fn flush_data_block(&mut self) -> Result<()> {
        if self.data_block.is_empty() {
            return Ok(());
        }

        let block_data = self.data_block.finish_with_trailer(self.compression);
        let handle = self.write_raw_block(&block_data)?;

        // Store handle for index entry (written when we know the separator)
        self.pending_index_entry = Some(handle);

        self.data_block.reset();
        Ok(())
    }

    /// Write a raw block to the file.
    fn write_raw_block(&mut self, data: &[u8]) -> Result<BlockHandle> {
        let handle = BlockHandle::new(self.offset, data.len() as u64);
        self.writer.write_all(data)?;
        self.offset += data.len() as u64;
        Ok(handle)
    }

    /// Write an index entry.
    fn write_index_entry(&mut self, key: &[u8], handle: BlockHandle) -> Result<()> {
        let mut value = BytesMut::with_capacity(16);
        handle.encode_to(&mut value);
        self.index_block.add(key, &value);
        Ok(())
    }

    /// Finish writing the SSTable.
    ///
    /// Writes the filter block, index block, and footer.
    pub fn finish(mut self) -> Result<SSTableInfo> {
        // Flush any remaining data block
        self.flush_data_block()?;

        // Write final index entry
        if let Some(handle) = self.pending_index_entry.take() {
            // Use successor of last key for final entry
            let successor = find_short_successor(&self.last_key);
            self.write_index_entry(&successor, handle)?;
        }

        // Write filter block
        let filter_data = self.filter_builder.finish();
        let filter_handle = if !filter_data.is_empty() {
            self.write_raw_block(&filter_data)?
        } else {
            BlockHandle::new(0, 0)
        };

        // Write index block
        let index_data = self.index_block.finish_with_trailer(CompressionType::None);
        let index_handle = self.write_raw_block(&index_data)?;

        // Write footer
        let footer = Footer::new(index_handle, filter_handle);
        let footer_data = footer.encode();
        self.writer.write_all(&footer_data)?;
        self.offset += FOOTER_SIZE as u64;

        // Sync and close
        self.writer.flush()?;
        self.writer.get_ref().sync_all()?;

        Ok(SSTableInfo {
            file_number: self.file_number,
            file_size: self.offset,
            entry_count: self.entry_count,
            smallest_key: self.smallest_key,
            largest_key: self.largest_key,
        })
    }

    /// Get the current file size.
    pub fn file_size(&self) -> u64 {
        self.offset
    }
}

/// Information about a completed SSTable.
#[derive(Debug, Clone)]
pub struct SSTableInfo {
    /// File number.
    pub file_number: u64,
    /// Total file size in bytes.
    pub file_size: u64,
    /// Number of entries.
    pub entry_count: u64,
    /// Smallest key (if any entries).
    pub smallest_key: Option<Bytes>,
    /// Largest key (if any entries).
    pub largest_key: Option<Bytes>,
}

/// Find a short separator between two keys.
///
/// Returns a key that is >= start and < limit.
fn find_short_separator(start: &[u8], limit: &[u8]) -> Vec<u8> {
    // Find common prefix
    let min_len = std::cmp::min(start.len(), limit.len());
    let mut diff_index = 0;

    while diff_index < min_len && start[diff_index] == limit[diff_index] {
        diff_index += 1;
    }

    if diff_index < min_len {
        // Try to increment the differing byte
        let diff_byte = start[diff_index];
        if diff_byte < 0xFF && diff_byte + 1 < limit[diff_index] {
            let mut result = start[..=diff_index].to_vec();
            result[diff_index] += 1;
            return result;
        }
    }

    // Can't shorten, return start
    start.to_vec()
}

/// Find a short successor to a key.
///
/// Returns the shortest key that is > input.
fn find_short_successor(key: &[u8]) -> Vec<u8> {
    // Find first byte that can be incremented
    for i in 0..key.len() {
        if key[i] < 0xFF {
            let mut result = key[..=i].to_vec();
            result[i] += 1;
            return result;
        }
    }

    // Key is all 0xFF bytes, can't shorten
    key.to_vec()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_sstable_writer_empty() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.sst");

        let writer = SSTableWriter::new(&path, 1, CompressionType::None, 10).unwrap();
        let info = writer.finish().unwrap();

        assert_eq!(info.entry_count, 0);
        assert!(info.smallest_key.is_none());
        assert!(info.largest_key.is_none());
    }

    #[test]
    fn test_sstable_writer_single_entry() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.sst");

        let mut writer = SSTableWriter::new(&path, 1, CompressionType::None, 10).unwrap();
        writer.add(b"key", b"value").unwrap();
        let info = writer.finish().unwrap();

        assert_eq!(info.entry_count, 1);
        assert_eq!(info.smallest_key.as_deref(), Some(b"key".as_slice()));
        assert_eq!(info.largest_key.as_deref(), Some(b"key".as_slice()));
    }

    #[test]
    fn test_sstable_writer_multiple_entries() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.sst");

        let mut writer = SSTableWriter::new(&path, 1, CompressionType::None, 10).unwrap();

        for i in 0..100 {
            let key = format!("key_{:04}", i);
            let value = format!("value_{}", i);
            writer.add(key.as_bytes(), value.as_bytes()).unwrap();
        }

        let info = writer.finish().unwrap();

        assert_eq!(info.entry_count, 100);
        assert_eq!(
            info.smallest_key.as_deref(),
            Some(b"key_0000".as_slice())
        );
        assert_eq!(info.largest_key.as_deref(), Some(b"key_0099".as_slice()));
    }

    #[test]
    fn test_sstable_writer_multiple_blocks() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.sst");

        // Use small block size to force multiple blocks
        let mut writer =
            SSTableWriter::with_block_size(&path, 1, CompressionType::None, 10, 256).unwrap();

        for i in 0..100 {
            let key = format!("key_{:04}", i);
            let value = "x".repeat(50);
            writer.add(key.as_bytes(), value.as_bytes()).unwrap();
        }

        let info = writer.finish().unwrap();
        assert_eq!(info.entry_count, 100);
        // With small blocks, file should be larger due to block overhead
        assert!(info.file_size > 5000);
    }

    #[test]
    fn test_find_short_separator() {
        // When diff_byte + 1 < limit[diff_index], we can shorten
        assert_eq!(find_short_separator(b"abc", b"abz"), b"abd");
        assert_eq!(find_short_separator(b"abc", b"xyz"), b"b");

        // When diff_byte + 1 >= limit[diff_index], we can't shorten
        assert_eq!(find_short_separator(b"abc", b"abd"), b"abc");

        // Same prefix, can't shorten
        assert_eq!(find_short_separator(b"abc", b"abcd"), b"abc");
    }

    #[test]
    fn test_find_short_successor() {
        assert_eq!(find_short_successor(b"abc"), b"b");
        assert_eq!(find_short_successor(b"a\xff\xff"), b"b");
        assert_eq!(find_short_successor(b"\xff\xff"), b"\xff\xff");
    }
}
