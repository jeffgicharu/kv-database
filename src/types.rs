//! Core types for rustdb.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::cmp::Ordering;

/// Value type indicator in internal keys.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ValueType {
    /// Normal value.
    Value = 1,
    /// Deletion marker (tombstone).
    Deletion = 2,
}

impl ValueType {
    /// Create from byte.
    pub fn from_byte(b: u8) -> Option<Self> {
        match b {
            1 => Some(ValueType::Value),
            2 => Some(ValueType::Deletion),
            _ => None,
        }
    }

    /// Convert to byte.
    pub fn to_byte(self) -> u8 {
        self as u8
    }

    /// Check if this is a deletion marker.
    pub fn is_deletion(&self) -> bool {
        matches!(self, ValueType::Deletion)
    }
}

/// Internal key format used for storage.
///
/// An internal key combines:
/// - User key (the key provided by the user)
/// - Sequence number (version for MVCC)
/// - Value type (Value or Deletion)
///
/// Encoded format:
/// ```text
/// [user_key][sequence (7 bytes)][value_type (1 byte)]
/// ```
///
/// The sequence and value_type are packed into 8 bytes with sequence
/// in the high 56 bits and value_type in the low 8 bits.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InternalKey {
    /// The user-provided key.
    user_key: Bytes,
    /// Sequence number (version).
    sequence: u64,
    /// Value type.
    value_type: ValueType,
}

impl InternalKey {
    /// Maximum sequence number (56 bits).
    pub const MAX_SEQUENCE: u64 = (1 << 56) - 1;

    /// Create a new internal key.
    pub fn new(user_key: impl Into<Bytes>, sequence: u64, value_type: ValueType) -> Self {
        debug_assert!(sequence <= Self::MAX_SEQUENCE);
        Self {
            user_key: user_key.into(),
            sequence,
            value_type,
        }
    }

    /// Create an internal key for a put operation.
    pub fn for_value(user_key: impl Into<Bytes>, sequence: u64) -> Self {
        Self::new(user_key, sequence, ValueType::Value)
    }

    /// Create an internal key for a delete operation.
    pub fn for_deletion(user_key: impl Into<Bytes>, sequence: u64) -> Self {
        Self::new(user_key, sequence, ValueType::Deletion)
    }

    /// Get the user key.
    pub fn user_key(&self) -> &[u8] {
        &self.user_key
    }

    /// Get the sequence number.
    pub fn sequence(&self) -> u64 {
        self.sequence
    }

    /// Get the value type.
    pub fn value_type(&self) -> ValueType {
        self.value_type
    }

    /// Check if this is a deletion marker.
    pub fn is_deletion(&self) -> bool {
        self.value_type.is_deletion()
    }

    /// Encode the internal key to bytes.
    ///
    /// Format: [user_key][packed_sequence_type (8 bytes)]
    /// Where packed = (sequence << 8) | value_type
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(self.user_key.len() + 8);
        buf.put_slice(&self.user_key);
        let packed = (self.sequence << 8) | (self.value_type.to_byte() as u64);
        buf.put_u64(packed);
        buf.freeze()
    }

    /// Encode into an existing buffer.
    pub fn encode_to(&self, buf: &mut BytesMut) {
        buf.put_slice(&self.user_key);
        let packed = (self.sequence << 8) | (self.value_type.to_byte() as u64);
        buf.put_u64(packed);
    }

    /// Decode an internal key from bytes.
    pub fn decode(data: &[u8]) -> Option<Self> {
        if data.len() < 8 {
            return None;
        }

        let user_key_len = data.len() - 8;
        let user_key = Bytes::copy_from_slice(&data[..user_key_len]);

        let mut packed_bytes = &data[user_key_len..];
        let packed = packed_bytes.get_u64();

        let value_type = ValueType::from_byte((packed & 0xFF) as u8)?;
        let sequence = packed >> 8;

        Some(Self {
            user_key,
            sequence,
            value_type,
        })
    }

    /// Get the encoded length.
    pub fn encoded_len(&self) -> usize {
        self.user_key.len() + 8
    }

    /// Parse user key and sequence from encoded bytes without full decode.
    pub fn parse_user_key(encoded: &[u8]) -> Option<&[u8]> {
        if encoded.len() < 8 {
            return None;
        }
        Some(&encoded[..encoded.len() - 8])
    }

    /// Parse sequence from encoded bytes.
    pub fn parse_sequence(encoded: &[u8]) -> Option<u64> {
        if encoded.len() < 8 {
            return None;
        }
        let packed_bytes = &encoded[encoded.len() - 8..];
        let packed = u64::from_be_bytes(packed_bytes.try_into().ok()?);
        Some(packed >> 8)
    }
}

impl Ord for InternalKey {
    fn cmp(&self, other: &Self) -> Ordering {
        // First compare user keys
        match self.user_key.cmp(&other.user_key) {
            Ordering::Equal => {
                // For same user key, newer sequence comes first (descending)
                // This ensures we see the latest version first during reads
                other.sequence.cmp(&self.sequence)
            }
            ord => ord,
        }
    }
}

impl PartialOrd for InternalKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// A batch of write operations to be applied atomically.
#[derive(Debug, Clone, Default)]
pub struct WriteBatch {
    /// The entries in this batch.
    entries: Vec<BatchEntry>,
    /// Approximate size in bytes.
    approximate_size: usize,
}

/// A single entry in a write batch.
#[derive(Debug, Clone)]
pub struct BatchEntry {
    /// The key to write.
    pub key: Bytes,
    /// The value (None for deletion).
    pub value: Option<Bytes>,
}

impl WriteBatch {
    /// Create a new empty write batch.
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a write batch with pre-allocated capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            entries: Vec::with_capacity(capacity),
            approximate_size: 0,
        }
    }

    /// Add a put operation to the batch.
    pub fn put(&mut self, key: impl Into<Bytes>, value: impl Into<Bytes>) {
        let key = key.into();
        let value = value.into();
        self.approximate_size += key.len() + value.len() + 16; // overhead estimate
        self.entries.push(BatchEntry {
            key,
            value: Some(value),
        });
    }

    /// Add a delete operation to the batch.
    pub fn delete(&mut self, key: impl Into<Bytes>) {
        let key = key.into();
        self.approximate_size += key.len() + 8; // overhead estimate
        self.entries.push(BatchEntry { key, value: None });
    }

    /// Clear the batch.
    pub fn clear(&mut self) {
        self.entries.clear();
        self.approximate_size = 0;
    }

    /// Check if the batch is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Get the number of entries.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Get approximate size in bytes.
    pub fn approximate_size(&self) -> usize {
        self.approximate_size
    }

    /// Get the entries.
    pub fn entries(&self) -> &[BatchEntry] {
        &self.entries
    }

    /// Iterate over entries.
    pub fn iter(&self) -> impl Iterator<Item = &BatchEntry> {
        self.entries.iter()
    }

    /// Encode the batch for WAL.
    ///
    /// Format:
    /// ```text
    /// [count (4 bytes)]
    /// [entry1: type (1) | key_len (varint) | key | value_len (varint) | value]
    /// [entry2: ...]
    /// ...
    /// ```
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(self.approximate_size + 4);

        // Write count
        buf.put_u32(self.entries.len() as u32);

        for entry in &self.entries {
            if let Some(ref value) = entry.value {
                // Put operation
                buf.put_u8(ValueType::Value.to_byte());
                encode_varint(&mut buf, entry.key.len() as u64);
                buf.put_slice(&entry.key);
                encode_varint(&mut buf, value.len() as u64);
                buf.put_slice(value);
            } else {
                // Delete operation
                buf.put_u8(ValueType::Deletion.to_byte());
                encode_varint(&mut buf, entry.key.len() as u64);
                buf.put_slice(&entry.key);
            }
        }

        buf.freeze()
    }

    /// Decode a batch from WAL data.
    pub fn decode(mut data: &[u8]) -> crate::Result<Self> {
        if data.len() < 4 {
            return Err(crate::Error::corruption("batch too short"));
        }

        let count = data.get_u32() as usize;
        let mut batch = WriteBatch::with_capacity(count);

        for _ in 0..count {
            if data.is_empty() {
                return Err(crate::Error::corruption("unexpected end of batch"));
            }

            let value_type = ValueType::from_byte(data.get_u8())
                .ok_or_else(|| crate::Error::corruption("invalid value type"))?;

            let key_len = decode_varint(&mut data)
                .ok_or_else(|| crate::Error::corruption("invalid key length"))?
                as usize;

            if data.len() < key_len {
                return Err(crate::Error::corruption("key truncated"));
            }
            let key = Bytes::copy_from_slice(&data[..key_len]);
            data.advance(key_len);

            match value_type {
                ValueType::Value => {
                    let value_len = decode_varint(&mut data)
                        .ok_or_else(|| crate::Error::corruption("invalid value length"))?
                        as usize;

                    if data.len() < value_len {
                        return Err(crate::Error::corruption("value truncated"));
                    }
                    let value = Bytes::copy_from_slice(&data[..value_len]);
                    data.advance(value_len);

                    batch.put(key, value);
                }
                ValueType::Deletion => {
                    batch.delete(key);
                }
            }
        }

        Ok(batch)
    }
}

/// Result of a lookup operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LookupResult {
    /// Value found.
    Found(Bytes),
    /// Key was deleted (tombstone found).
    Deleted,
    /// Key not found.
    NotFound,
}

impl LookupResult {
    /// Check if a value was found.
    pub fn is_found(&self) -> bool {
        matches!(self, LookupResult::Found(_))
    }

    /// Get the value if found.
    pub fn value(&self) -> Option<&Bytes> {
        match self {
            LookupResult::Found(v) => Some(v),
            _ => None,
        }
    }

    /// Convert to Option<Bytes>.
    pub fn into_option(self) -> Option<Bytes> {
        match self {
            LookupResult::Found(v) => Some(v),
            _ => None,
        }
    }
}

// Helper functions for varint encoding (simple implementation)

fn encode_varint(buf: &mut BytesMut, mut value: u64) {
    while value >= 0x80 {
        buf.put_u8((value as u8) | 0x80);
        value >>= 7;
    }
    buf.put_u8(value as u8);
}

fn decode_varint(buf: &mut &[u8]) -> Option<u64> {
    let mut result = 0u64;
    let mut shift = 0;

    loop {
        if buf.is_empty() {
            return None;
        }

        let byte = buf.get_u8();
        result |= ((byte & 0x7F) as u64) << shift;

        if byte & 0x80 == 0 {
            return Some(result);
        }

        shift += 7;
        if shift >= 64 {
            return None; // Overflow
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_value_type() {
        assert_eq!(ValueType::from_byte(1), Some(ValueType::Value));
        assert_eq!(ValueType::from_byte(2), Some(ValueType::Deletion));
        assert_eq!(ValueType::from_byte(0), None);
        assert!(ValueType::Deletion.is_deletion());
        assert!(!ValueType::Value.is_deletion());
    }

    #[test]
    fn test_internal_key_encode_decode() {
        let key = InternalKey::new(Bytes::from("hello"), 12345, ValueType::Value);
        let encoded = key.encode();
        let decoded = InternalKey::decode(&encoded).unwrap();

        assert_eq!(key.user_key(), decoded.user_key());
        assert_eq!(key.sequence(), decoded.sequence());
        assert_eq!(key.value_type(), decoded.value_type());
    }

    #[test]
    fn test_internal_key_ordering() {
        let key1 = InternalKey::new(Bytes::from("aaa"), 100, ValueType::Value);
        let key2 = InternalKey::new(Bytes::from("aaa"), 200, ValueType::Value);
        let key3 = InternalKey::new(Bytes::from("bbb"), 100, ValueType::Value);

        // Same user key: higher sequence comes first
        assert!(key2 < key1); // 200 comes before 100 for same user key

        // Different user keys: lexicographic order
        assert!(key1 < key3);
        assert!(key2 < key3);
    }

    #[test]
    fn test_write_batch() {
        let mut batch = WriteBatch::new();
        batch.put(b"key1".as_slice(), b"value1".as_slice());
        batch.put(b"key2".as_slice(), b"value2".as_slice());
        batch.delete(b"key3".as_slice());

        assert_eq!(batch.len(), 3);
        assert!(!batch.is_empty());
    }

    #[test]
    fn test_write_batch_encode_decode() {
        let mut batch = WriteBatch::new();
        batch.put(b"key1".as_slice(), b"value1".as_slice());
        batch.put(b"key2".as_slice(), b"value2".as_slice());
        batch.delete(b"key3".as_slice());

        let encoded = batch.encode();
        let decoded = WriteBatch::decode(&encoded).unwrap();

        assert_eq!(batch.len(), decoded.len());

        for (orig, dec) in batch.entries().iter().zip(decoded.entries().iter()) {
            assert_eq!(orig.key, dec.key);
            assert_eq!(orig.value, dec.value);
        }
    }

    #[test]
    fn test_lookup_result() {
        let found = LookupResult::Found(Bytes::from("value"));
        assert!(found.is_found());
        assert_eq!(found.value(), Some(&Bytes::from("value")));

        let deleted = LookupResult::Deleted;
        assert!(!deleted.is_found());

        let not_found = LookupResult::NotFound;
        assert!(!not_found.is_found());
    }

    #[test]
    fn test_varint() {
        let test_values = [0u64, 1, 127, 128, 255, 256, 16383, 16384, u64::MAX >> 1];

        for &val in &test_values {
            let mut buf = BytesMut::new();
            encode_varint(&mut buf, val);

            let mut slice: &[u8] = &buf;
            let decoded = decode_varint(&mut slice).unwrap();

            assert_eq!(val, decoded, "Failed for value {}", val);
        }
    }
}
