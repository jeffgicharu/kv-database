//! Write-Ahead Log (WAL) for durability.
//!
//! The WAL ensures durability by logging all writes before they're applied
//! to the MemTable. On crash recovery, the WAL is replayed to restore
//! any writes that weren't flushed to SSTables.
//!
//! # Format
//!
//! The WAL uses a block-based format with 32KB blocks. Each record has:
//! - CRC32 checksum (4 bytes)
//! - Length (2 bytes)
//! - Record type (1 byte): FULL, FIRST, MIDDLE, LAST
//! - Payload (variable)
//!
//! Large records that don't fit in a single block are fragmented across
//! multiple blocks using FIRST, MIDDLE, and LAST record types.

mod reader;
mod writer;

pub use reader::WalReader;
pub use writer::WalWriter;

/// Block size for WAL (32KB).
pub const BLOCK_SIZE: usize = 32 * 1024;

/// Header size: CRC (4) + Length (2) + Type (1) = 7 bytes.
pub const HEADER_SIZE: usize = 7;

/// Maximum payload size per record in a block.
pub const MAX_RECORD_SIZE: usize = BLOCK_SIZE - HEADER_SIZE;

/// Record types for WAL entries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum RecordType {
    /// Zero is reserved for pre-allocated files.
    Zero = 0,
    /// Complete record in a single fragment.
    Full = 1,
    /// First fragment of a record.
    First = 2,
    /// Middle fragment(s) of a record.
    Middle = 3,
    /// Last fragment of a record.
    Last = 4,
}

impl RecordType {
    /// Create from byte value.
    pub fn from_byte(b: u8) -> Option<Self> {
        match b {
            0 => Some(RecordType::Zero),
            1 => Some(RecordType::Full),
            2 => Some(RecordType::First),
            3 => Some(RecordType::Middle),
            4 => Some(RecordType::Last),
            _ => None,
        }
    }

    /// Convert to byte.
    pub fn to_byte(self) -> u8 {
        self as u8
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record_type_roundtrip() {
        for rt in [
            RecordType::Zero,
            RecordType::Full,
            RecordType::First,
            RecordType::Middle,
            RecordType::Last,
        ] {
            assert_eq!(RecordType::from_byte(rt.to_byte()), Some(rt));
        }
    }

    #[test]
    fn test_invalid_record_type() {
        assert_eq!(RecordType::from_byte(5), None);
        assert_eq!(RecordType::from_byte(255), None);
    }

    #[test]
    fn test_constants() {
        assert_eq!(BLOCK_SIZE, 32768);
        assert_eq!(HEADER_SIZE, 7);
        assert_eq!(MAX_RECORD_SIZE, BLOCK_SIZE - HEADER_SIZE);
    }
}
