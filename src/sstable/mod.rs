//! SSTable - Sorted String Table for on-disk storage.
//!
//! SSTables are immutable, sorted files that store key-value pairs.
//! They are the persistent storage format for the LSM tree.
//!
//! # File Format
//!
//! ```text
//! +------------------+
//! | Data Block 1     |
//! +------------------+
//! | Data Block 2     |
//! +------------------+
//! | ...              |
//! +------------------+
//! | Data Block N     |
//! +------------------+
//! | Filter Block     |  (Bloom filter)
//! +------------------+
//! | Index Block      |  (Block handles for data blocks)
//! +------------------+
//! | Footer           |  (Index and filter block handles)
//! +------------------+
//! ```
//!
//! # Data Block Format
//!
//! ```text
//! +------------------+
//! | Entry 1          |  shared_len | unshared_len | value_len | key_delta | value
//! +------------------+
//! | Entry 2          |
//! +------------------+
//! | ...              |
//! +------------------+
//! | Restart Point 0  |  (4 bytes, offset)
//! +------------------+
//! | Restart Point 1  |
//! +------------------+
//! | ...              |
//! +------------------+
//! | Num Restarts     |  (4 bytes)
//! +------------------+
//! | CRC + Type       |  (5 bytes)
//! +------------------+
//! ```

mod block;
mod block_builder;
mod filter;
mod iterator;
mod reader;
mod writer;

pub use block::{Block, BlockHandle};
pub use block_builder::BlockBuilder;
pub use filter::BloomFilter;
pub use iterator::{SSTableEntryIterator, SSTableIterator};
pub use reader::SSTableReader;
pub use writer::SSTableWriter;

use bytes::{Buf, BufMut, Bytes, BytesMut};

/// Magic number for SSTable footer (8 bytes).
/// This helps identify valid SSTable files.
pub const FOOTER_MAGIC: u64 = 0x88e241b785f4cff7;

/// Footer size: index handle (16) + filter handle (16) + magic (8) = 40 bytes.
pub const FOOTER_SIZE: usize = 40;

/// Default block size (4KB).
pub const DEFAULT_BLOCK_SIZE: usize = 4 * 1024;

/// Block trailer size: CRC (4) + Type (1) = 5 bytes.
pub const BLOCK_TRAILER_SIZE: usize = 5;

/// Compression type for blocks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(u8)]
pub enum CompressionType {
    /// No compression.
    #[default]
    None = 0,
    /// Snappy compression.
    Snappy = 1,
    /// LZ4 compression.
    Lz4 = 2,
}

impl CompressionType {
    /// Create from byte.
    pub fn from_byte(b: u8) -> Option<Self> {
        match b {
            0 => Some(CompressionType::None),
            1 => Some(CompressionType::Snappy),
            2 => Some(CompressionType::Lz4),
            _ => None,
        }
    }

    /// Convert to byte.
    pub fn to_byte(self) -> u8 {
        self as u8
    }
}

/// Footer of an SSTable file.
///
/// Contains handles to the index and filter blocks.
#[derive(Debug, Clone)]
pub struct Footer {
    /// Handle to the index block.
    pub index_handle: BlockHandle,
    /// Handle to the filter block.
    pub filter_handle: BlockHandle,
}

impl Footer {
    /// Create a new footer.
    pub fn new(index_handle: BlockHandle, filter_handle: BlockHandle) -> Self {
        Self {
            index_handle,
            filter_handle,
        }
    }

    /// Encode the footer to bytes.
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(FOOTER_SIZE);
        self.index_handle.encode_to(&mut buf);
        self.filter_handle.encode_to(&mut buf);
        buf.put_u64_le(FOOTER_MAGIC);
        buf.freeze()
    }

    /// Decode a footer from bytes.
    pub fn decode(data: &[u8]) -> crate::Result<Self> {
        if data.len() < FOOTER_SIZE {
            return Err(crate::Error::corruption("footer too short"));
        }

        let footer_start = data.len() - FOOTER_SIZE;
        let mut cursor = &data[footer_start..];

        let index_handle = BlockHandle::decode(&mut cursor)?;
        let filter_handle = BlockHandle::decode(&mut cursor)?;

        let magic = cursor.get_u64_le();
        if magic != FOOTER_MAGIC {
            return Err(crate::Error::corruption("invalid footer magic"));
        }

        Ok(Self {
            index_handle,
            filter_handle,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compression_type_roundtrip() {
        for ct in [
            CompressionType::None,
            CompressionType::Snappy,
            CompressionType::Lz4,
        ] {
            assert_eq!(CompressionType::from_byte(ct.to_byte()), Some(ct));
        }
    }

    #[test]
    fn test_footer_encode_decode() {
        let index_handle = BlockHandle::new(100, 200);
        let filter_handle = BlockHandle::new(300, 400);
        let footer = Footer::new(index_handle, filter_handle);

        let encoded = footer.encode();
        assert_eq!(encoded.len(), FOOTER_SIZE);

        let decoded = Footer::decode(&encoded).unwrap();
        assert_eq!(decoded.index_handle.offset(), 100);
        assert_eq!(decoded.index_handle.size(), 200);
        assert_eq!(decoded.filter_handle.offset(), 300);
        assert_eq!(decoded.filter_handle.size(), 400);
    }

    #[test]
    fn test_footer_invalid_magic() {
        let mut data = vec![0u8; FOOTER_SIZE];
        // Don't write correct magic
        let result = Footer::decode(&data);
        assert!(result.is_err());
    }
}
