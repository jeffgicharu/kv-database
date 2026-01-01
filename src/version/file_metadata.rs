//! File metadata for SSTable files.

use bytes::{BufMut, Bytes, BytesMut};
use std::cmp::Ordering;

use crate::types::InternalKey;

/// Metadata about an SSTable file.
///
/// Contains all information needed to locate and identify an SSTable,
/// including its key range for efficient lookups.
#[derive(Debug, Clone)]
pub struct FileMetadata {
    /// Unique file number.
    file_number: u64,
    /// File size in bytes.
    file_size: u64,
    /// Smallest key in the file.
    smallest: InternalKey,
    /// Largest key in the file.
    largest: InternalKey,
    /// Number of entries in the file.
    num_entries: u64,
    /// Whether this file is being compacted.
    being_compacted: bool,
    /// Number of times this file has been seeked.
    /// Used to trigger compaction for frequently-seeked files.
    allowed_seeks: i64,
}

impl FileMetadata {
    /// Create new file metadata.
    pub fn new(
        file_number: u64,
        file_size: u64,
        smallest: InternalKey,
        largest: InternalKey,
    ) -> Self {
        // Calculate allowed seeks based on file size.
        // Larger files get more allowed seeks before triggering compaction.
        let allowed_seeks = std::cmp::max(100, (file_size / 16384) as i64);

        Self {
            file_number,
            file_size,
            smallest,
            largest,
            num_entries: 0,
            being_compacted: false,
            allowed_seeks,
        }
    }

    /// Create with entry count.
    pub fn with_entries(
        file_number: u64,
        file_size: u64,
        smallest: InternalKey,
        largest: InternalKey,
        num_entries: u64,
    ) -> Self {
        let mut meta = Self::new(file_number, file_size, smallest, largest);
        meta.num_entries = num_entries;
        meta
    }

    /// Get the file number.
    pub fn file_number(&self) -> u64 {
        self.file_number
    }

    /// Get the file size.
    pub fn file_size(&self) -> u64 {
        self.file_size
    }

    /// Get the smallest key.
    pub fn smallest(&self) -> &InternalKey {
        &self.smallest
    }

    /// Get the largest key.
    pub fn largest(&self) -> &InternalKey {
        &self.largest
    }

    /// Get the number of entries.
    pub fn num_entries(&self) -> u64 {
        self.num_entries
    }

    /// Check if file is being compacted.
    pub fn being_compacted(&self) -> bool {
        self.being_compacted
    }

    /// Mark file as being compacted.
    pub fn set_being_compacted(&mut self, value: bool) {
        self.being_compacted = value;
    }

    /// Get allowed seeks remaining.
    pub fn allowed_seeks(&self) -> i64 {
        self.allowed_seeks
    }

    /// Decrement allowed seeks and return whether it dropped to zero.
    pub fn decrement_allowed_seeks(&mut self) -> bool {
        self.allowed_seeks -= 1;
        self.allowed_seeks <= 0
    }

    /// Check if the file's key range overlaps with the given range.
    pub fn overlaps(&self, smallest: &[u8], largest: &[u8]) -> bool {
        // File overlaps if:
        // - file.largest >= smallest AND file.smallest <= largest
        self.largest.user_key() >= smallest && self.smallest.user_key() <= largest
    }

    /// Check if a user key might be in this file.
    pub fn may_contain_key(&self, user_key: &[u8]) -> bool {
        user_key >= self.smallest.user_key() && user_key <= self.largest.user_key()
    }

    /// Encode file metadata for manifest.
    pub fn encode(&self) -> Bytes {
        let smallest_encoded = self.smallest.encode();
        let largest_encoded = self.largest.encode();

        let mut buf = BytesMut::with_capacity(
            8 + 8 + 8 + 4 + smallest_encoded.len() + 4 + largest_encoded.len(),
        );

        buf.put_u64_le(self.file_number);
        buf.put_u64_le(self.file_size);
        buf.put_u64_le(self.num_entries);

        // Encode smallest key
        buf.put_u32_le(smallest_encoded.len() as u32);
        buf.put_slice(&smallest_encoded);

        // Encode largest key
        buf.put_u32_le(largest_encoded.len() as u32);
        buf.put_slice(&largest_encoded);

        buf.freeze()
    }

    /// Decode file metadata from manifest.
    pub fn decode(data: &[u8]) -> crate::Result<(Self, usize)> {
        if data.len() < 24 {
            return Err(crate::Error::corruption("file metadata too short"));
        }

        let file_number = u64::from_le_bytes(data[0..8].try_into().unwrap());
        let file_size = u64::from_le_bytes(data[8..16].try_into().unwrap());
        let num_entries = u64::from_le_bytes(data[16..24].try_into().unwrap());

        let mut offset = 24;

        // Decode smallest key
        if data.len() < offset + 4 {
            return Err(crate::Error::corruption("file metadata truncated"));
        }
        let smallest_len = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;

        if data.len() < offset + smallest_len {
            return Err(crate::Error::corruption("smallest key truncated"));
        }
        let smallest = InternalKey::decode(&data[offset..offset + smallest_len])
            .ok_or_else(|| crate::Error::corruption("invalid smallest key"))?;
        offset += smallest_len;

        // Decode largest key
        if data.len() < offset + 4 {
            return Err(crate::Error::corruption("file metadata truncated"));
        }
        let largest_len = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;

        if data.len() < offset + largest_len {
            return Err(crate::Error::corruption("largest key truncated"));
        }
        let largest = InternalKey::decode(&data[offset..offset + largest_len])
            .ok_or_else(|| crate::Error::corruption("invalid largest key"))?;
        offset += largest_len;

        Ok((
            Self::with_entries(file_number, file_size, smallest, largest, num_entries),
            offset,
        ))
    }
}

impl PartialEq for FileMetadata {
    fn eq(&self, other: &Self) -> bool {
        self.file_number == other.file_number
    }
}

impl Eq for FileMetadata {}

impl PartialOrd for FileMetadata {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for FileMetadata {
    fn cmp(&self, other: &Self) -> Ordering {
        // Sort by smallest key first, then by file number for stability
        match self.smallest.user_key().cmp(other.smallest.user_key()) {
            Ordering::Equal => self.file_number.cmp(&other.file_number),
            ord => ord,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::ValueType;

    fn make_key(user_key: &[u8], seq: u64) -> InternalKey {
        InternalKey::new(Bytes::copy_from_slice(user_key), seq, ValueType::Value)
    }

    #[test]
    fn test_file_metadata_basic() {
        let meta = FileMetadata::new(
            1,
            1024,
            make_key(b"aaa", 1),
            make_key(b"zzz", 100),
        );

        assert_eq!(meta.file_number(), 1);
        assert_eq!(meta.file_size(), 1024);
        assert_eq!(meta.smallest().user_key(), b"aaa");
        assert_eq!(meta.largest().user_key(), b"zzz");
    }

    #[test]
    fn test_file_metadata_overlaps() {
        let meta = FileMetadata::new(
            1,
            1024,
            make_key(b"bbb", 1),
            make_key(b"ddd", 100),
        );

        // Overlapping ranges
        assert!(meta.overlaps(b"aaa", b"ccc")); // Left overlap
        assert!(meta.overlaps(b"ccc", b"eee")); // Right overlap
        assert!(meta.overlaps(b"aaa", b"eee")); // Contains file
        assert!(meta.overlaps(b"bbb", b"ddd")); // Exact match
        assert!(meta.overlaps(b"ccc", b"ccc")); // Point within

        // Non-overlapping ranges
        assert!(!meta.overlaps(b"aaa", b"aaz")); // Before
        assert!(!meta.overlaps(b"eee", b"zzz")); // After
    }

    #[test]
    fn test_file_metadata_may_contain() {
        let meta = FileMetadata::new(
            1,
            1024,
            make_key(b"bbb", 1),
            make_key(b"ddd", 100),
        );

        assert!(meta.may_contain_key(b"bbb"));
        assert!(meta.may_contain_key(b"ccc"));
        assert!(meta.may_contain_key(b"ddd"));
        assert!(!meta.may_contain_key(b"aaa"));
        assert!(!meta.may_contain_key(b"eee"));
    }

    #[test]
    fn test_file_metadata_encode_decode() {
        let meta = FileMetadata::with_entries(
            42,
            8192,
            make_key(b"start", 10),
            make_key(b"end", 50),
            1000,
        );

        let encoded = meta.encode();
        let (decoded, bytes_read) = FileMetadata::decode(&encoded).unwrap();

        assert_eq!(bytes_read, encoded.len());
        assert_eq!(decoded.file_number(), 42);
        assert_eq!(decoded.file_size(), 8192);
        assert_eq!(decoded.num_entries(), 1000);
        assert_eq!(decoded.smallest().user_key(), b"start");
        assert_eq!(decoded.largest().user_key(), b"end");
    }

    #[test]
    fn test_file_metadata_ordering() {
        let meta1 = FileMetadata::new(1, 100, make_key(b"aaa", 1), make_key(b"bbb", 1));
        let meta2 = FileMetadata::new(2, 100, make_key(b"ccc", 1), make_key(b"ddd", 1));
        let meta3 = FileMetadata::new(3, 100, make_key(b"aaa", 1), make_key(b"ccc", 1));

        assert!(meta1 < meta2); // aaa < ccc
        assert!(meta1 < meta3); // Same smallest, file_number 1 < 3
        assert!(meta3 < meta2); // aaa < ccc
    }

    #[test]
    fn test_allowed_seeks() {
        let mut meta = FileMetadata::new(
            1,
            100000, // ~6 allowed seeks (100000/16384)
            make_key(b"a", 1),
            make_key(b"z", 1),
        );

        assert!(meta.allowed_seeks() >= 6);

        // Decrement until it hits zero
        while !meta.decrement_allowed_seeks() {
            assert!(meta.allowed_seeks() > 0);
        }

        assert!(meta.allowed_seeks() <= 0);
    }
}
