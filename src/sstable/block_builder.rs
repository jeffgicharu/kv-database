//! Block builder for constructing SSTable data blocks.
//!
//! Builds blocks with prefix compression and restart points.

use bytes::{BufMut, Bytes, BytesMut};

use crate::util::crc::{crc32, mask_crc};

use super::{CompressionType, BLOCK_TRAILER_SIZE, DEFAULT_BLOCK_SIZE};

/// Default number of entries between restart points.
const DEFAULT_RESTART_INTERVAL: usize = 16;

/// Builder for SSTable data blocks.
///
/// Uses prefix compression to reduce key storage overhead,
/// with restart points for efficient binary search.
pub struct BlockBuilder {
    /// Buffer for block data.
    buffer: BytesMut,
    /// Restart point offsets.
    restarts: Vec<u32>,
    /// Number of entries since last restart.
    counter: usize,
    /// Restart interval.
    restart_interval: usize,
    /// Last key added (for prefix compression).
    last_key: Vec<u8>,
    /// Whether the builder has any entries.
    finished: bool,
    /// Target block size.
    block_size: usize,
}

impl BlockBuilder {
    /// Create a new block builder with default settings.
    pub fn new() -> Self {
        Self::with_options(DEFAULT_BLOCK_SIZE, DEFAULT_RESTART_INTERVAL)
    }

    /// Create a new block builder with custom options.
    pub fn with_options(block_size: usize, restart_interval: usize) -> Self {
        let mut restarts = Vec::new();
        restarts.push(0); // First restart point at beginning

        Self {
            buffer: BytesMut::with_capacity(block_size),
            restarts,
            counter: 0,
            restart_interval,
            last_key: Vec::new(),
            finished: false,
            block_size,
        }
    }

    /// Add a key-value pair to the block.
    ///
    /// Keys must be added in sorted order.
    pub fn add(&mut self, key: &[u8], value: &[u8]) {
        debug_assert!(!self.finished);
        debug_assert!(
            self.last_key.is_empty() || key > self.last_key.as_slice(),
            "keys must be added in sorted order"
        );

        // Calculate prefix compression
        let shared = if self.counter < self.restart_interval {
            // Find common prefix with last key
            let mut shared = 0;
            let min_len = std::cmp::min(self.last_key.len(), key.len());
            while shared < min_len && self.last_key[shared] == key[shared] {
                shared += 1;
            }
            shared
        } else {
            // Start a new restart point - no prefix compression
            self.restarts.push(self.buffer.len() as u32);
            self.counter = 0;
            0
        };

        let unshared = key.len() - shared;

        // Encode entry: shared_len | unshared_len | value_len | key_delta | value
        encode_varint(&mut self.buffer, shared as u64);
        encode_varint(&mut self.buffer, unshared as u64);
        encode_varint(&mut self.buffer, value.len() as u64);
        self.buffer.put_slice(&key[shared..]);
        self.buffer.put_slice(value);

        // Update state
        self.last_key.clear();
        self.last_key.extend_from_slice(key);
        self.counter += 1;
    }

    /// Check if the block is empty.
    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    /// Get the current estimated size of the block.
    pub fn current_size(&self) -> usize {
        self.buffer.len() + self.restarts.len() * 4 + 4 + BLOCK_TRAILER_SIZE
    }

    /// Check if adding more entries would exceed the target size.
    pub fn should_flush(&self) -> bool {
        self.current_size() >= self.block_size
    }

    /// Finish building the block and return the data.
    ///
    /// Returns the block contents without trailer.
    pub fn finish(&mut self) -> Bytes {
        debug_assert!(!self.finished);
        self.finished = true;

        // Append restart points
        for &restart in &self.restarts {
            self.buffer.put_u32_le(restart);
        }

        // Append number of restart points
        self.buffer.put_u32_le(self.restarts.len() as u32);

        self.buffer.clone().freeze()
    }

    /// Finish and add trailer with CRC and compression type.
    pub fn finish_with_trailer(&mut self, compression: CompressionType) -> Bytes {
        let content = self.finish();

        // Optionally compress
        let (final_content, final_compression) = match compression {
            CompressionType::None => (content, CompressionType::None),
            CompressionType::Snappy => {
                match snap::raw::Encoder::new().compress_vec(&content) {
                    Ok(compressed) if compressed.len() < content.len() => {
                        (Bytes::from(compressed), CompressionType::Snappy)
                    }
                    _ => (content, CompressionType::None), // Compression didn't help
                }
            }
            CompressionType::Lz4 => {
                let compressed = lz4_flex::compress_prepend_size(&content);
                if compressed.len() < content.len() {
                    (Bytes::from(compressed), CompressionType::Lz4)
                } else {
                    (content, CompressionType::None)
                }
            }
        };

        // Build trailer: CRC (4) + Type (1)
        let mut result = BytesMut::with_capacity(final_content.len() + BLOCK_TRAILER_SIZE);
        result.put_slice(&final_content);

        // CRC is over content + type
        let mut crc_data = Vec::with_capacity(final_content.len() + 1);
        crc_data.extend_from_slice(&final_content);
        crc_data.push(final_compression.to_byte());
        let crc = mask_crc(crc32(&crc_data));

        result.put_u32_le(crc);
        result.put_u8(final_compression.to_byte());

        result.freeze()
    }

    /// Reset the builder for reuse.
    pub fn reset(&mut self) {
        self.buffer.clear();
        self.restarts.clear();
        self.restarts.push(0);
        self.counter = 0;
        self.last_key.clear();
        self.finished = false;
    }
}

impl Default for BlockBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Encode a varint to the buffer.
fn encode_varint(buf: &mut BytesMut, mut value: u64) {
    while value >= 0x80 {
        buf.put_u8((value as u8) | 0x80);
        value >>= 7;
    }
    buf.put_u8(value as u8);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sstable::Block;

    #[test]
    fn test_block_builder_empty() {
        let builder = BlockBuilder::new();
        assert!(builder.is_empty());
    }

    #[test]
    fn test_block_builder_single_entry() {
        let mut builder = BlockBuilder::new();
        builder.add(b"key", b"value");
        assert!(!builder.is_empty());

        let data = builder.finish();
        let block = Block::new(data).unwrap();
        assert_eq!(block.num_restarts(), 1);

        // Verify we can read the entry using Iterator trait
        let entries: Vec<_> = block.iter().collect();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].0.as_ref(), b"key");
        assert_eq!(entries[0].1.as_ref(), b"value");
    }

    #[test]
    fn test_block_builder_multiple_entries() {
        let mut builder = BlockBuilder::new();

        let entries = vec![
            (b"aaa".to_vec(), b"value1".to_vec()),
            (b"aab".to_vec(), b"value2".to_vec()),
            (b"abc".to_vec(), b"value3".to_vec()),
            (b"bbb".to_vec(), b"value4".to_vec()),
        ];

        for (key, value) in &entries {
            builder.add(key, value);
        }

        let data = builder.finish();
        let block = Block::new(data).unwrap();

        // Verify all entries
        let read_entries: Vec<_> = block.iter().collect();
        assert_eq!(read_entries.len(), entries.len());
        for (i, (key, value)) in read_entries.iter().enumerate() {
            assert_eq!(key.as_ref(), entries[i].0.as_slice());
            assert_eq!(value.as_ref(), entries[i].1.as_slice());
        }
    }

    #[test]
    fn test_block_builder_prefix_compression() {
        let mut builder = BlockBuilder::with_options(4096, 16);

        // Keys with common prefix should compress well
        for i in 0..10 {
            let key = format!("prefix_{:04}", i);
            let value = format!("value_{}", i);
            builder.add(key.as_bytes(), value.as_bytes());
        }

        let data = builder.finish();
        // With prefix compression, the data should be smaller than raw keys
        // Each key is 11 bytes, value is ~7 bytes = ~18 bytes per entry = ~180 bytes
        // With compression, should be significantly less
        assert!(data.len() < 180);
    }

    #[test]
    fn test_block_builder_restart_points() {
        // Use small restart interval for testing
        let mut builder = BlockBuilder::with_options(4096, 2);

        for i in 0..6 {
            let key = format!("key_{:02}", i);
            builder.add(key.as_bytes(), b"value");
        }

        let data = builder.finish();
        let block = Block::new(data).unwrap();

        // With interval of 2 and 6 entries, we should have 3 restart points
        // (entries 0-1, 2-3, 4-5)
        assert_eq!(block.num_restarts(), 3);
    }

    #[test]
    fn test_block_builder_seek() {
        let mut builder = BlockBuilder::with_options(4096, 2);

        let keys: Vec<String> = (0..10).map(|i| format!("key_{:02}", i)).collect();
        for key in &keys {
            builder.add(key.as_bytes(), b"value");
        }

        let data = builder.finish();
        let block = Block::new(data).unwrap();

        // Test seek to existing key
        let mut iter = block.iter();
        iter.seek(b"key_05");
        assert!(iter.valid());
        assert_eq!(iter.key(), b"key_05");

        // Test seek to non-existing key (should find next)
        iter.seek(b"key_04x");
        assert!(iter.valid());
        assert_eq!(iter.key(), b"key_05");

        // Test seek past end
        iter.seek(b"zzz");
        assert!(!iter.valid());
    }

    #[test]
    fn test_block_builder_with_trailer() {
        let mut builder = BlockBuilder::new();
        builder.add(b"key1", b"value1");
        builder.add(b"key2", b"value2");

        let data = builder.finish_with_trailer(CompressionType::None);

        // Should be able to parse with trailer
        let block = Block::new_with_trailer(&data).unwrap();
        let entries: Vec<_> = block.iter().collect();
        assert_eq!(entries.len(), 2);
    }

    #[test]
    fn test_block_builder_snappy_compression() {
        let mut builder = BlockBuilder::new();

        // Add compressible data
        for i in 0..100 {
            let key = format!("key_{:04}", i);
            let value = "x".repeat(100); // Highly compressible
            builder.add(key.as_bytes(), value.as_bytes());
        }

        let uncompressed = builder.current_size();
        let data = builder.finish_with_trailer(CompressionType::Snappy);

        // Compressed should be smaller
        assert!(data.len() < uncompressed);

        // Should still be readable
        let block = Block::new_with_trailer(&data).unwrap();
        let entries: Vec<_> = block.iter().collect();
        assert_eq!(entries.len(), 100);
    }

    #[test]
    fn test_block_builder_reset() {
        let mut builder = BlockBuilder::new();
        builder.add(b"key1", b"value1");
        builder.finish();

        builder.reset();
        assert!(builder.is_empty());

        builder.add(b"key2", b"value2");
        let data = builder.finish();
        let block = Block::new(data).unwrap();

        let mut iter = block.iter();
        iter.seek_to_first();
        assert_eq!(iter.key(), b"key2");
    }
}
