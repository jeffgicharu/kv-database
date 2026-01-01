//! Encoding utilities for variable-length integers and fixed-width values.

use bytes::{Buf, BufMut, Bytes, BytesMut};

/// Maximum bytes needed to encode a varint64.
pub const MAX_VARINT64_LEN: usize = 10;

/// Maximum bytes needed to encode a varint32.
pub const MAX_VARINT32_LEN: usize = 5;

/// Encode a 32-bit unsigned integer as a varint.
///
/// Returns the number of bytes written.
pub fn encode_varint32(buf: &mut BytesMut, mut value: u32) -> usize {
    let mut count = 0;
    while value >= 0x80 {
        buf.put_u8((value as u8) | 0x80);
        value >>= 7;
        count += 1;
    }
    buf.put_u8(value as u8);
    count + 1
}

/// Encode a 64-bit unsigned integer as a varint.
///
/// Returns the number of bytes written.
pub fn encode_varint64(buf: &mut BytesMut, mut value: u64) -> usize {
    let mut count = 0;
    while value >= 0x80 {
        buf.put_u8((value as u8) | 0x80);
        value >>= 7;
        count += 1;
    }
    buf.put_u8(value as u8);
    count + 1
}

/// Encode a varint to a fixed-size array, returning the slice used.
pub fn encode_varint64_to_array(value: u64) -> ([u8; MAX_VARINT64_LEN], usize) {
    let mut buf = [0u8; MAX_VARINT64_LEN];
    let mut v = value;
    let mut i = 0;
    while v >= 0x80 {
        buf[i] = (v as u8) | 0x80;
        v >>= 7;
        i += 1;
    }
    buf[i] = v as u8;
    (buf, i + 1)
}

/// Decode a 32-bit varint from a buffer.
///
/// Returns None if the buffer is too short or the varint is malformed.
pub fn decode_varint32(buf: &mut &[u8]) -> Option<u32> {
    let mut result = 0u32;
    let mut shift = 0;

    for _ in 0..MAX_VARINT32_LEN {
        if buf.is_empty() {
            return None;
        }

        let byte = buf.get_u8();
        result |= ((byte & 0x7F) as u32) << shift;

        if byte & 0x80 == 0 {
            return Some(result);
        }

        shift += 7;
    }

    None // Varint too long
}

/// Decode a 64-bit varint from a buffer.
///
/// Returns None if the buffer is too short or the varint is malformed.
pub fn decode_varint64(buf: &mut &[u8]) -> Option<u64> {
    let mut result = 0u64;
    let mut shift = 0;

    for _ in 0..MAX_VARINT64_LEN {
        if buf.is_empty() {
            return None;
        }

        let byte = buf.get_u8();
        result |= ((byte & 0x7F) as u64) << shift;

        if byte & 0x80 == 0 {
            return Some(result);
        }

        shift += 7;
    }

    None // Varint too long
}

/// Get the number of bytes needed to encode a varint.
pub fn varint_length(value: u64) -> usize {
    let mut len = 1;
    let mut v = value;
    while v >= 0x80 {
        v >>= 7;
        len += 1;
    }
    len
}

/// Encode a length-prefixed byte slice.
pub fn encode_length_prefixed(buf: &mut BytesMut, data: &[u8]) {
    encode_varint64(buf, data.len() as u64);
    buf.put_slice(data);
}

/// Decode a length-prefixed byte slice.
pub fn decode_length_prefixed(buf: &mut &[u8]) -> Option<Bytes> {
    let len = decode_varint64(buf)? as usize;
    if buf.len() < len {
        return None;
    }
    let data = Bytes::copy_from_slice(&buf[..len]);
    buf.advance(len);
    Some(data)
}

/// Encode a fixed 32-bit little-endian integer.
pub fn encode_fixed32(buf: &mut BytesMut, value: u32) {
    buf.put_u32_le(value);
}

/// Decode a fixed 32-bit little-endian integer.
pub fn decode_fixed32(buf: &mut &[u8]) -> Option<u32> {
    if buf.len() < 4 {
        return None;
    }
    Some(buf.get_u32_le())
}

/// Encode a fixed 64-bit little-endian integer.
pub fn encode_fixed64(buf: &mut BytesMut, value: u64) {
    buf.put_u64_le(value);
}

/// Decode a fixed 64-bit little-endian integer.
pub fn decode_fixed64(buf: &mut &[u8]) -> Option<u64> {
    if buf.len() < 8 {
        return None;
    }
    Some(buf.get_u64_le())
}

/// Read a fixed 32-bit value from a slice without consuming.
pub fn read_fixed32(data: &[u8]) -> Option<u32> {
    if data.len() < 4 {
        return None;
    }
    Some(u32::from_le_bytes([data[0], data[1], data[2], data[3]]))
}

/// Read a fixed 64-bit value from a slice without consuming.
pub fn read_fixed64(data: &[u8]) -> Option<u64> {
    if data.len() < 8 {
        return None;
    }
    Some(u64::from_le_bytes([
        data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
    ]))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_varint32_roundtrip() {
        let test_values = [0u32, 1, 127, 128, 255, 256, 16383, 16384, u32::MAX];

        for &val in &test_values {
            let mut buf = BytesMut::new();
            encode_varint32(&mut buf, val);

            let mut slice: &[u8] = &buf;
            let decoded = decode_varint32(&mut slice).unwrap();

            assert_eq!(val, decoded, "Failed for value {}", val);
            assert!(slice.is_empty(), "Buffer not fully consumed");
        }
    }

    #[test]
    fn test_varint64_roundtrip() {
        let test_values = [
            0u64,
            1,
            127,
            128,
            255,
            256,
            16383,
            16384,
            (1 << 21) - 1,
            1 << 21,
            (1 << 28) - 1,
            1 << 28,
            (1 << 35) - 1,
            1 << 35,
            u64::MAX >> 1,
            u64::MAX,
        ];

        for &val in &test_values {
            let mut buf = BytesMut::new();
            encode_varint64(&mut buf, val);

            let mut slice: &[u8] = &buf;
            let decoded = decode_varint64(&mut slice).unwrap();

            assert_eq!(val, decoded, "Failed for value {}", val);
            assert!(slice.is_empty(), "Buffer not fully consumed");
        }
    }

    #[test]
    fn test_varint_length() {
        assert_eq!(varint_length(0), 1);
        assert_eq!(varint_length(127), 1);
        assert_eq!(varint_length(128), 2);
        assert_eq!(varint_length(16383), 2);
        assert_eq!(varint_length(16384), 3);
    }

    #[test]
    fn test_fixed32_roundtrip() {
        let test_values = [0u32, 1, 255, 256, u32::MAX];

        for &val in &test_values {
            let mut buf = BytesMut::new();
            encode_fixed32(&mut buf, val);

            assert_eq!(buf.len(), 4);

            let mut slice: &[u8] = &buf;
            let decoded = decode_fixed32(&mut slice).unwrap();

            assert_eq!(val, decoded);
        }
    }

    #[test]
    fn test_fixed64_roundtrip() {
        let test_values = [0u64, 1, 255, 256, u64::MAX];

        for &val in &test_values {
            let mut buf = BytesMut::new();
            encode_fixed64(&mut buf, val);

            assert_eq!(buf.len(), 8);

            let mut slice: &[u8] = &buf;
            let decoded = decode_fixed64(&mut slice).unwrap();

            assert_eq!(val, decoded);
        }
    }

    #[test]
    fn test_length_prefixed() {
        let data = b"hello world";

        let mut buf = BytesMut::new();
        encode_length_prefixed(&mut buf, data);

        let mut slice: &[u8] = &buf;
        let decoded = decode_length_prefixed(&mut slice).unwrap();

        assert_eq!(&decoded[..], data);
        assert!(slice.is_empty());
    }

    #[test]
    fn test_decode_truncated() {
        let mut empty: &[u8] = &[];
        assert!(decode_varint32(&mut empty).is_none());
        assert!(decode_fixed32(&mut empty).is_none());

        let short: &[u8] = &[0x80, 0x80]; // Incomplete varint
        let mut slice = short;
        assert!(decode_varint32(&mut slice).is_none());
    }
}
