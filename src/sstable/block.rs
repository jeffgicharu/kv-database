//! Block format for SSTable data storage.
//!
//! A block contains a sequence of key-value entries with prefix compression,
//! followed by restart points for efficient binary search.

use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::util::crc::{crc32, mask_crc};
use crate::{Error, Result};

use super::{CompressionType, BLOCK_TRAILER_SIZE};

/// Handle to a block within an SSTable file.
///
/// Contains the offset and size needed to read the block.
#[derive(Debug, Clone, Copy, Default)]
pub struct BlockHandle {
    /// Offset within the file.
    offset: u64,
    /// Size of the block (not including trailer).
    size: u64,
}

impl BlockHandle {
    /// Create a new block handle.
    pub fn new(offset: u64, size: u64) -> Self {
        Self { offset, size }
    }

    /// Get the offset.
    pub fn offset(&self) -> u64 {
        self.offset
    }

    /// Get the size.
    pub fn size(&self) -> u64 {
        self.size
    }

    /// Encode to buffer (fixed 16 bytes: offset + size as u64).
    pub fn encode_to(&self, buf: &mut BytesMut) {
        buf.put_u64_le(self.offset);
        buf.put_u64_le(self.size);
    }

    /// Decode from buffer.
    pub fn decode(data: &mut &[u8]) -> Result<Self> {
        if data.len() < 16 {
            return Err(Error::corruption("block handle too short"));
        }
        let offset = data.get_u64_le();
        let size = data.get_u64_le();
        Ok(Self { offset, size })
    }

    /// Encoded size in bytes.
    pub const fn encoded_size() -> usize {
        16
    }
}

/// A block of data from an SSTable.
///
/// Contains key-value entries with prefix compression and restart points
/// for efficient binary search.
#[derive(Debug)]
pub struct Block {
    /// The raw block data.
    data: Bytes,
    /// Offset to the restart array.
    restart_offset: usize,
    /// Number of restart points.
    num_restarts: u32,
}

impl Block {
    /// Create a block from raw data.
    ///
    /// The data should include entries and restart points but NOT the trailer.
    pub fn new(data: Bytes) -> Result<Self> {
        if data.len() < 4 {
            return Err(Error::corruption("block too short"));
        }

        // Read number of restarts from end
        let num_restarts =
            u32::from_le_bytes(data[data.len() - 4..].try_into().map_err(|_| {
                Error::corruption("failed to read restart count")
            })?);

        // Calculate restart offset
        let restart_array_size = num_restarts as usize * 4;
        if data.len() < 4 + restart_array_size {
            return Err(Error::corruption("block too short for restart array"));
        }

        let restart_offset = data.len() - 4 - restart_array_size;

        Ok(Self {
            data,
            restart_offset,
            num_restarts,
        })
    }

    /// Create a block from raw data with trailer verification.
    pub fn new_with_trailer(data: &[u8]) -> Result<Self> {
        if data.len() < BLOCK_TRAILER_SIZE {
            return Err(Error::corruption("block too short for trailer"));
        }

        let content_len = data.len() - BLOCK_TRAILER_SIZE;
        let content = &data[..content_len];
        let trailer = &data[content_len..];

        // Read and verify CRC
        let stored_crc = u32::from_le_bytes(trailer[..4].try_into().unwrap());
        let compression_type = trailer[4];

        // CRC is computed over content + compression type
        let mut crc_data = Vec::with_capacity(content_len + 1);
        crc_data.extend_from_slice(content);
        crc_data.push(compression_type);
        let computed_crc = mask_crc(crc32(&crc_data));

        if stored_crc != computed_crc {
            return Err(Error::corruption("block checksum mismatch"));
        }

        // Handle decompression
        let block_data = match CompressionType::from_byte(compression_type) {
            Some(CompressionType::None) => Bytes::copy_from_slice(content),
            Some(CompressionType::Snappy) => {
                // Decompress with snappy
                let decompressed = snap::raw::Decoder::new()
                    .decompress_vec(content)
                    .map_err(|e| Error::corruption(format!("snappy decompress failed: {}", e)))?;
                Bytes::from(decompressed)
            }
            Some(CompressionType::Lz4) => {
                // Decompress with LZ4
                let decompressed = lz4_flex::decompress_size_prepended(content)
                    .map_err(|e| Error::corruption(format!("lz4 decompress failed: {}", e)))?;
                Bytes::from(decompressed)
            }
            None => {
                return Err(Error::corruption("unknown compression type"));
            }
        };

        Self::new(block_data)
    }

    /// Get the raw data.
    pub fn data(&self) -> &Bytes {
        &self.data
    }

    /// Get the number of restart points.
    pub fn num_restarts(&self) -> u32 {
        self.num_restarts
    }

    /// Get a restart point offset.
    pub fn restart_point(&self, index: u32) -> u32 {
        let offset = self.restart_offset + (index as usize * 4);
        u32::from_le_bytes(self.data[offset..offset + 4].try_into().unwrap())
    }

    /// Create an iterator over the block.
    pub fn iter(&self) -> BlockIterator<'_> {
        BlockIterator::new(self)
    }

    /// Find the restart point for binary search.
    ///
    /// Returns the index of the restart point at or before the target key.
    pub fn find_restart_point(&self, target: &[u8]) -> u32 {
        let mut left = 0u32;
        let mut right = self.num_restarts;

        while left < right {
            let mid = (left + right + 1) / 2;
            let restart_offset = self.restart_point(mid);

            // Decode key at restart point (no prefix compression)
            if let Some(key) = self.decode_key_at(restart_offset as usize) {
                if key.as_ref() <= target {
                    left = mid;
                } else {
                    right = mid - 1;
                }
            } else {
                break;
            }
        }

        left
    }

    /// Decode the key at a given offset.
    fn decode_key_at(&self, offset: usize) -> Option<Bytes> {
        if offset >= self.restart_offset {
            return None;
        }
        let mut cursor = &self.data[offset..self.restart_offset];
        if cursor.is_empty() {
            return None;
        }

        let shared = decode_varint(&mut cursor)?;
        let unshared = decode_varint(&mut cursor)?;
        let _value_len = decode_varint(&mut cursor)?;

        if shared != 0 {
            // At restart points, shared should be 0
            return None;
        }

        if cursor.len() < unshared as usize {
            return None;
        }

        Some(Bytes::copy_from_slice(&cursor[..unshared as usize]))
    }
}

/// Iterator over entries in a block.
pub struct BlockIterator<'a> {
    /// Reference to the block.
    block: &'a Block,
    /// Current position in the data.
    offset: usize,
    /// Current key (accumulated with prefix compression).
    current_key: Vec<u8>,
    /// Current value.
    current_value: Bytes,
    /// Whether we're at a valid entry.
    valid: bool,
}

impl<'a> BlockIterator<'a> {
    /// Create a new iterator at the beginning.
    fn new(block: &'a Block) -> Self {
        Self {
            block,
            offset: 0,
            current_key: Vec::new(),
            current_value: Bytes::new(),
            valid: false,
        }
    }

    /// Check if the iterator is valid.
    pub fn valid(&self) -> bool {
        self.valid
    }

    /// Get the current key.
    pub fn key(&self) -> &[u8] {
        &self.current_key
    }

    /// Get the current value.
    pub fn value(&self) -> &Bytes {
        &self.current_value
    }

    /// Move to the first entry.
    pub fn seek_to_first(&mut self) {
        self.offset = 0;
        self.current_key.clear();
        self.parse_next_entry();
    }

    /// Seek to the first entry with key >= target.
    pub fn seek(&mut self, target: &[u8]) {
        // Use restart points for binary search
        let restart_index = self.block.find_restart_point(target);
        self.offset = self.block.restart_point(restart_index) as usize;
        self.current_key.clear();

        // Linear search from restart point
        loop {
            self.parse_next_entry();
            if !self.valid {
                break;
            }
            if self.current_key.as_slice() >= target {
                break;
            }
        }
    }

    /// Move to the next entry.
    pub fn next(&mut self) {
        if !self.valid {
            return;
        }
        self.parse_next_entry();
    }

    /// Parse the next entry from the current offset.
    fn parse_next_entry(&mut self) {
        if self.offset >= self.block.restart_offset {
            self.valid = false;
            return;
        }

        let mut cursor = &self.block.data[self.offset..self.block.restart_offset];
        let start_len = cursor.len();

        // Decode entry header
        let shared = match decode_varint(&mut cursor) {
            Some(v) => v as usize,
            None => {
                self.valid = false;
                return;
            }
        };

        let unshared = match decode_varint(&mut cursor) {
            Some(v) => v as usize,
            None => {
                self.valid = false;
                return;
            }
        };

        let value_len = match decode_varint(&mut cursor) {
            Some(v) => v as usize,
            None => {
                self.valid = false;
                return;
            }
        };

        // Check bounds
        if cursor.len() < unshared + value_len {
            self.valid = false;
            return;
        }

        // Build the key using prefix compression
        self.current_key.truncate(shared);
        self.current_key.extend_from_slice(&cursor[..unshared]);

        // Extract value
        self.current_value = Bytes::copy_from_slice(&cursor[unshared..unshared + value_len]);

        // Update offset
        let consumed = start_len - cursor.len() + unshared + value_len;
        self.offset += consumed;
        self.valid = true;
    }
}

impl<'a> Iterator for BlockIterator<'a> {
    type Item = (Bytes, Bytes);

    fn next(&mut self) -> Option<Self::Item> {
        if !self.valid {
            self.seek_to_first();
        } else {
            BlockIterator::next(self);
        }

        if self.valid {
            Some((
                Bytes::copy_from_slice(&self.current_key),
                self.current_value.clone(),
            ))
        } else {
            None
        }
    }
}

/// Decode a varint from the cursor.
fn decode_varint(cursor: &mut &[u8]) -> Option<u64> {
    let mut result = 0u64;
    let mut shift = 0;

    loop {
        if cursor.is_empty() {
            return None;
        }

        let byte = cursor[0];
        *cursor = &cursor[1..];

        result |= ((byte & 0x7F) as u64) << shift;

        if byte & 0x80 == 0 {
            return Some(result);
        }

        shift += 7;
        if shift >= 64 {
            return None;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_block_handle() {
        let handle = BlockHandle::new(100, 200);
        assert_eq!(handle.offset(), 100);
        assert_eq!(handle.size(), 200);

        let mut buf = BytesMut::new();
        handle.encode_to(&mut buf);
        assert_eq!(buf.len(), 16);

        let mut cursor = &buf[..];
        let decoded = BlockHandle::decode(&mut cursor).unwrap();
        assert_eq!(decoded.offset(), 100);
        assert_eq!(decoded.size(), 200);
    }

    #[test]
    fn test_empty_block() {
        // Minimum valid block: just num_restarts = 0
        let mut data = BytesMut::new();
        data.put_u32_le(0); // num_restarts

        let block = Block::new(data.freeze()).unwrap();
        assert_eq!(block.num_restarts(), 0);
    }

    #[test]
    fn test_block_too_short() {
        let data = Bytes::from_static(&[0, 1, 2]); // Less than 4 bytes
        assert!(Block::new(data).is_err());
    }
}
