//! VersionEdit - describes changes between versions.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::collections::{HashMap, HashSet};

use crate::options::MAX_LEVELS;
use crate::types::InternalKey;
use crate::{Error, Result};

use super::{EditTag, FileMetadata};

/// A VersionEdit describes the changes between two Versions.
///
/// It records:
/// - New files to add
/// - Files to delete
/// - Updated sequence numbers
/// - Compaction pointers
#[derive(Debug, Clone, Default)]
pub struct VersionEdit {
    /// Comparator name (set on first edit).
    pub comparator: Option<String>,
    /// Log file number.
    pub log_number: Option<u64>,
    /// Previous log number (deprecated).
    pub prev_log_number: Option<u64>,
    /// Next file number to allocate.
    pub next_file_number: Option<u64>,
    /// Last sequence number used.
    pub last_sequence: Option<u64>,
    /// Compaction pointers by level.
    pub compact_pointers: HashMap<usize, InternalKey>,
    /// Files to delete: (level, file_number).
    pub deleted_files: HashSet<(usize, u64)>,
    /// New files to add: (level, FileMetadata).
    pub new_files: Vec<(usize, FileMetadata)>,
}

impl VersionEdit {
    /// Create a new empty edit.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the comparator name.
    pub fn set_comparator(&mut self, name: impl Into<String>) {
        self.comparator = Some(name.into());
    }

    /// Set the log number.
    pub fn set_log_number(&mut self, num: u64) {
        self.log_number = Some(num);
    }

    /// Set the previous log number.
    pub fn set_prev_log_number(&mut self, num: u64) {
        self.prev_log_number = Some(num);
    }

    /// Set the next file number.
    pub fn set_next_file_number(&mut self, num: u64) {
        self.next_file_number = Some(num);
    }

    /// Set the last sequence number.
    pub fn set_last_sequence(&mut self, seq: u64) {
        self.last_sequence = Some(seq);
    }

    /// Set a compaction pointer.
    pub fn set_compact_pointer(&mut self, level: usize, key: InternalKey) {
        self.compact_pointers.insert(level, key);
    }

    /// Add a file to delete.
    pub fn delete_file(&mut self, level: usize, file_number: u64) {
        self.deleted_files.insert((level, file_number));
    }

    /// Add a new file.
    pub fn add_file(&mut self, level: usize, file: FileMetadata) {
        self.new_files.push((level, file));
    }

    /// Add a new file with explicit parameters.
    pub fn add_file_info(
        &mut self,
        level: usize,
        file_number: u64,
        file_size: u64,
        smallest: InternalKey,
        largest: InternalKey,
    ) {
        self.new_files.push((
            level,
            FileMetadata::new(file_number, file_size, smallest, largest),
        ));
    }

    /// Check if the edit is empty.
    pub fn is_empty(&self) -> bool {
        self.comparator.is_none()
            && self.log_number.is_none()
            && self.prev_log_number.is_none()
            && self.next_file_number.is_none()
            && self.last_sequence.is_none()
            && self.compact_pointers.is_empty()
            && self.deleted_files.is_empty()
            && self.new_files.is_empty()
    }

    /// Encode the edit to bytes.
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(256);

        // Comparator
        if let Some(ref name) = self.comparator {
            buf.put_u8(EditTag::Comparator.to_byte());
            encode_length_prefixed(&mut buf, name.as_bytes());
        }

        // Log number
        if let Some(num) = self.log_number {
            buf.put_u8(EditTag::LogNumber.to_byte());
            encode_varint(&mut buf, num);
        }

        // Prev log number
        if let Some(num) = self.prev_log_number {
            buf.put_u8(EditTag::PrevLogNumber.to_byte());
            encode_varint(&mut buf, num);
        }

        // Next file number
        if let Some(num) = self.next_file_number {
            buf.put_u8(EditTag::NextFileNumber.to_byte());
            encode_varint(&mut buf, num);
        }

        // Last sequence
        if let Some(seq) = self.last_sequence {
            buf.put_u8(EditTag::LastSequence.to_byte());
            encode_varint(&mut buf, seq);
        }

        // Compact pointers
        for (&level, key) in &self.compact_pointers {
            buf.put_u8(EditTag::CompactPointer.to_byte());
            encode_varint(&mut buf, level as u64);
            let key_bytes = key.encode();
            encode_length_prefixed(&mut buf, &key_bytes);
        }

        // Deleted files
        for &(level, file_number) in &self.deleted_files {
            buf.put_u8(EditTag::DeletedFile.to_byte());
            encode_varint(&mut buf, level as u64);
            encode_varint(&mut buf, file_number);
        }

        // New files
        for (level, file) in &self.new_files {
            buf.put_u8(EditTag::NewFile.to_byte());
            encode_varint(&mut buf, *level as u64);
            encode_varint(&mut buf, file.file_number());
            encode_varint(&mut buf, file.file_size());

            let smallest_bytes = file.smallest().encode();
            encode_length_prefixed(&mut buf, &smallest_bytes);

            let largest_bytes = file.largest().encode();
            encode_length_prefixed(&mut buf, &largest_bytes);
        }

        buf.freeze()
    }

    /// Decode an edit from bytes.
    pub fn decode(data: &[u8]) -> Result<Self> {
        let mut edit = VersionEdit::new();
        let mut cursor = data;

        while !cursor.is_empty() {
            let tag = cursor.get_u8();
            let tag = EditTag::from_byte(tag)
                .ok_or_else(|| Error::corruption(format!("unknown edit tag: {}", tag)))?;

            match tag {
                EditTag::Comparator => {
                    let name = decode_length_prefixed(&mut cursor)?;
                    edit.comparator = Some(
                        String::from_utf8(name.to_vec())
                            .map_err(|_| Error::corruption("invalid comparator name"))?,
                    );
                }
                EditTag::LogNumber => {
                    edit.log_number = Some(decode_varint(&mut cursor)?);
                }
                EditTag::PrevLogNumber => {
                    edit.prev_log_number = Some(decode_varint(&mut cursor)?);
                }
                EditTag::NextFileNumber => {
                    edit.next_file_number = Some(decode_varint(&mut cursor)?);
                }
                EditTag::LastSequence => {
                    edit.last_sequence = Some(decode_varint(&mut cursor)?);
                }
                EditTag::CompactPointer => {
                    let level = decode_varint(&mut cursor)? as usize;
                    if level >= MAX_LEVELS {
                        return Err(Error::corruption("invalid level for compact pointer"));
                    }
                    let key_bytes = decode_length_prefixed(&mut cursor)?;
                    let key = InternalKey::decode(&key_bytes)
                        .ok_or_else(|| Error::corruption("invalid compact pointer key"))?;
                    edit.compact_pointers.insert(level, key);
                }
                EditTag::DeletedFile => {
                    let level = decode_varint(&mut cursor)? as usize;
                    if level >= MAX_LEVELS {
                        return Err(Error::corruption("invalid level for deleted file"));
                    }
                    let file_number = decode_varint(&mut cursor)?;
                    edit.deleted_files.insert((level, file_number));
                }
                EditTag::NewFile => {
                    let level = decode_varint(&mut cursor)? as usize;
                    if level >= MAX_LEVELS {
                        return Err(Error::corruption("invalid level for new file"));
                    }
                    let file_number = decode_varint(&mut cursor)?;
                    let file_size = decode_varint(&mut cursor)?;

                    let smallest_bytes = decode_length_prefixed(&mut cursor)?;
                    let smallest = InternalKey::decode(&smallest_bytes)
                        .ok_or_else(|| Error::corruption("invalid smallest key"))?;

                    let largest_bytes = decode_length_prefixed(&mut cursor)?;
                    let largest = InternalKey::decode(&largest_bytes)
                        .ok_or_else(|| Error::corruption("invalid largest key"))?;

                    edit.new_files.push((
                        level,
                        FileMetadata::new(file_number, file_size, smallest, largest),
                    ));
                }
            }
        }

        Ok(edit)
    }
}

/// Encode a varint.
fn encode_varint(buf: &mut BytesMut, mut value: u64) {
    while value >= 0x80 {
        buf.put_u8((value as u8) | 0x80);
        value >>= 7;
    }
    buf.put_u8(value as u8);
}

/// Decode a varint.
fn decode_varint(cursor: &mut &[u8]) -> Result<u64> {
    let mut result = 0u64;
    let mut shift = 0;

    loop {
        if cursor.is_empty() {
            return Err(Error::corruption("truncated varint"));
        }

        let byte = cursor.get_u8();
        result |= ((byte & 0x7F) as u64) << shift;

        if byte & 0x80 == 0 {
            return Ok(result);
        }

        shift += 7;
        if shift >= 64 {
            return Err(Error::corruption("varint overflow"));
        }
    }
}

/// Encode length-prefixed bytes.
fn encode_length_prefixed(buf: &mut BytesMut, data: &[u8]) {
    encode_varint(buf, data.len() as u64);
    buf.put_slice(data);
}

/// Decode length-prefixed bytes.
fn decode_length_prefixed<'a>(cursor: &mut &'a [u8]) -> Result<&'a [u8]> {
    let len = decode_varint(cursor)? as usize;
    if cursor.len() < len {
        return Err(Error::corruption("truncated length-prefixed data"));
    }
    let data = &cursor[..len];
    *cursor = &cursor[len..];
    Ok(data)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::ValueType;

    fn make_key(user_key: &[u8], seq: u64) -> InternalKey {
        InternalKey::new(Bytes::copy_from_slice(user_key), seq, ValueType::Value)
    }

    #[test]
    fn test_version_edit_empty() {
        let edit = VersionEdit::new();
        assert!(edit.is_empty());
    }

    #[test]
    fn test_version_edit_set_fields() {
        let mut edit = VersionEdit::new();

        edit.set_comparator("leveldb.BytewiseComparator");
        edit.set_log_number(10);
        edit.set_next_file_number(20);
        edit.set_last_sequence(100);

        assert!(!edit.is_empty());
        assert_eq!(edit.comparator.as_deref(), Some("leveldb.BytewiseComparator"));
        assert_eq!(edit.log_number, Some(10));
        assert_eq!(edit.next_file_number, Some(20));
        assert_eq!(edit.last_sequence, Some(100));
    }

    #[test]
    fn test_version_edit_add_delete_files() {
        let mut edit = VersionEdit::new();

        edit.add_file_info(
            0,
            1,
            1024,
            make_key(b"aaa", 1),
            make_key(b"zzz", 100),
        );
        edit.delete_file(1, 5);

        assert_eq!(edit.new_files.len(), 1);
        assert_eq!(edit.deleted_files.len(), 1);
        assert!(edit.deleted_files.contains(&(1, 5)));
    }

    #[test]
    fn test_version_edit_encode_decode_empty() {
        let edit = VersionEdit::new();
        let encoded = edit.encode();
        let decoded = VersionEdit::decode(&encoded).unwrap();

        assert!(decoded.is_empty());
    }

    #[test]
    fn test_version_edit_encode_decode_full() {
        let mut edit = VersionEdit::new();

        edit.set_comparator("bytewise");
        edit.set_log_number(10);
        edit.set_prev_log_number(9);
        edit.set_next_file_number(100);
        edit.set_last_sequence(5000);

        edit.set_compact_pointer(1, make_key(b"compact", 50));

        edit.delete_file(0, 1);
        edit.delete_file(0, 2);

        edit.add_file_info(0, 3, 2048, make_key(b"a", 1), make_key(b"z", 100));
        edit.add_file_info(1, 4, 4096, make_key(b"aa", 10), make_key(b"zz", 200));

        let encoded = edit.encode();
        let decoded = VersionEdit::decode(&encoded).unwrap();

        assert_eq!(decoded.comparator.as_deref(), Some("bytewise"));
        assert_eq!(decoded.log_number, Some(10));
        assert_eq!(decoded.prev_log_number, Some(9));
        assert_eq!(decoded.next_file_number, Some(100));
        assert_eq!(decoded.last_sequence, Some(5000));

        assert_eq!(decoded.compact_pointers.len(), 1);
        assert!(decoded.compact_pointers.contains_key(&1));

        assert_eq!(decoded.deleted_files.len(), 2);
        assert!(decoded.deleted_files.contains(&(0, 1)));
        assert!(decoded.deleted_files.contains(&(0, 2)));

        assert_eq!(decoded.new_files.len(), 2);
        assert_eq!(decoded.new_files[0].0, 0); // level
        assert_eq!(decoded.new_files[0].1.file_number(), 3);
        assert_eq!(decoded.new_files[1].0, 1);
        assert_eq!(decoded.new_files[1].1.file_number(), 4);
    }

    #[test]
    fn test_version_edit_compact_pointer() {
        let mut edit = VersionEdit::new();
        edit.set_compact_pointer(2, make_key(b"middle", 100));

        let encoded = edit.encode();
        let decoded = VersionEdit::decode(&encoded).unwrap();

        assert_eq!(decoded.compact_pointers.len(), 1);
        let key = decoded.compact_pointers.get(&2).unwrap();
        assert_eq!(key.user_key(), b"middle");
    }

    #[test]
    fn test_varint_encoding() {
        let test_values = [0, 1, 127, 128, 255, 16383, 16384, u64::MAX >> 1];

        for &val in &test_values {
            let mut buf = BytesMut::new();
            encode_varint(&mut buf, val);

            let mut cursor: &[u8] = &buf;
            let decoded = decode_varint(&mut cursor).unwrap();

            assert_eq!(val, decoded);
            assert!(cursor.is_empty());
        }
    }
}
