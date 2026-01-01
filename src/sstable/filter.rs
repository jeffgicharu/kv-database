//! Bloom filter for SSTable key filtering.
//!
//! A Bloom filter is a probabilistic data structure that can quickly
//! determine if a key is definitely NOT in a set (no false negatives)
//! or might be in the set (possible false positives).

use bytes::{BufMut, Bytes, BytesMut};

/// Bloom filter for efficient key lookups.
///
/// Uses multiple hash functions to set bits in a bit array.
/// False positive rate depends on bits per key.
#[derive(Debug, Clone)]
pub struct BloomFilter {
    /// The bit array.
    bits: Vec<u8>,
    /// Number of hash functions to use.
    k: u32,
}

impl BloomFilter {
    /// Create a new Bloom filter builder.
    pub fn new(bits_per_key: usize) -> BloomFilterBuilder {
        BloomFilterBuilder::new(bits_per_key)
    }

    /// Create a Bloom filter from encoded data.
    pub fn from_bytes(data: Bytes) -> Option<Self> {
        if data.is_empty() {
            return None;
        }

        // Last byte is k (number of hash probes)
        let k = data[data.len() - 1] as u32;
        if k > 30 {
            // k is too large, probably corrupted
            return None;
        }

        let bits = data[..data.len() - 1].to_vec();
        Some(Self { bits, k })
    }

    /// Check if a key might be in the filter.
    ///
    /// Returns true if the key might exist (possible false positive).
    /// Returns false if the key definitely does not exist.
    pub fn may_contain(&self, key: &[u8]) -> bool {
        if self.bits.is_empty() {
            return false;
        }

        let bits_len = self.bits.len() * 8;
        let mut h = bloom_hash(key);

        let delta = (h >> 17) | (h << 15); // Rotate right 17 bits

        for _ in 0..self.k {
            let bit_pos = (h as usize) % bits_len;
            if self.bits[bit_pos / 8] & (1 << (bit_pos % 8)) == 0 {
                return false;
            }
            h = h.wrapping_add(delta);
        }

        true
    }

    /// Get the raw bytes of the filter.
    pub fn as_bytes(&self) -> &[u8] {
        &self.bits
    }

    /// Get k (number of hash probes).
    pub fn k(&self) -> u32 {
        self.k
    }
}

/// Builder for Bloom filters.
pub struct BloomFilterBuilder {
    /// Bits per key.
    bits_per_key: usize,
    /// Number of hash probes.
    k: u32,
    /// Keys to add to the filter.
    keys: Vec<Bytes>,
}

impl BloomFilterBuilder {
    /// Create a new builder with specified bits per key.
    pub fn new(bits_per_key: usize) -> Self {
        // Calculate optimal k = bits_per_key * ln(2) ≈ bits_per_key * 0.69
        // Clamp to reasonable range
        let k = std::cmp::max(1, std::cmp::min(30, (bits_per_key as f64 * 0.69) as u32));

        Self {
            bits_per_key,
            k,
            keys: Vec::new(),
        }
    }

    /// Add a key to the filter.
    pub fn add(&mut self, key: &[u8]) {
        self.keys.push(Bytes::copy_from_slice(key));
    }

    /// Get the number of keys added.
    pub fn len(&self) -> usize {
        self.keys.len()
    }

    /// Check if the builder is empty.
    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    /// Build the Bloom filter.
    pub fn finish(&self) -> Bytes {
        if self.keys.is_empty() {
            return Bytes::new();
        }

        // Calculate bit array size
        let mut bits_count = self.keys.len() * self.bits_per_key;
        // Round up to next multiple of 8
        bits_count = ((bits_count + 7) / 8) * 8;
        // Minimum size
        if bits_count < 64 {
            bits_count = 64;
        }

        let bytes_count = bits_count / 8;
        let mut bits = vec![0u8; bytes_count];

        // Add each key
        for key in &self.keys {
            let mut h = bloom_hash(key);
            let delta = (h >> 17) | (h << 15);

            for _ in 0..self.k {
                let bit_pos = (h as usize) % bits_count;
                bits[bit_pos / 8] |= 1 << (bit_pos % 8);
                h = h.wrapping_add(delta);
            }
        }

        // Append k as the last byte
        let mut result = BytesMut::with_capacity(bytes_count + 1);
        result.put_slice(&bits);
        result.put_u8(self.k as u8);

        result.freeze()
    }

    /// Build and return a BloomFilter.
    pub fn build(&self) -> BloomFilter {
        let data = self.finish();
        BloomFilter::from_bytes(data).unwrap_or(BloomFilter {
            bits: Vec::new(),
            k: self.k,
        })
    }

    /// Reset the builder for reuse.
    pub fn reset(&mut self) {
        self.keys.clear();
    }
}

/// Hash function for Bloom filter.
///
/// Uses a simple but effective hash function similar to MurmurHash.
fn bloom_hash(key: &[u8]) -> u32 {
    // Simple hash function based on MurmurHash
    const SEED: u32 = 0xbc9f1d34;
    const M: u32 = 0xc6a4a793;

    let mut h = SEED ^ (key.len() as u32).wrapping_mul(M);

    // Process 4 bytes at a time
    let mut i = 0;
    while i + 4 <= key.len() {
        let w = u32::from_le_bytes([key[i], key[i + 1], key[i + 2], key[i + 3]]);
        h = h.wrapping_add(w);
        h = h.wrapping_mul(M);
        h ^= h >> 16;
        i += 4;
    }

    // Process remaining bytes
    let remaining = key.len() - i;
    if remaining >= 3 {
        h = h.wrapping_add((key[i + 2] as u32) << 16);
    }
    if remaining >= 2 {
        h = h.wrapping_add((key[i + 1] as u32) << 8);
    }
    if remaining >= 1 {
        h = h.wrapping_add(key[i] as u32);
        h = h.wrapping_mul(M);
        h ^= h >> 24;
    }

    h
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bloom_filter_empty() {
        let builder = BloomFilterBuilder::new(10);
        let filter = builder.build();
        assert!(!filter.may_contain(b"hello"));
    }

    #[test]
    fn test_bloom_filter_single_key() {
        let mut builder = BloomFilterBuilder::new(10);
        builder.add(b"hello");
        let filter = builder.build();

        assert!(filter.may_contain(b"hello"));
        // False positives are possible but unlikely for a single key
    }

    #[test]
    fn test_bloom_filter_multiple_keys() {
        let mut builder = BloomFilterBuilder::new(10);
        let keys: Vec<String> = (0..100).map(|i| format!("key_{:04}", i)).collect();

        for key in &keys {
            builder.add(key.as_bytes());
        }

        let filter = builder.build();

        // All added keys should be found
        for key in &keys {
            assert!(
                filter.may_contain(key.as_bytes()),
                "Key {} not found",
                key
            );
        }
    }

    #[test]
    fn test_bloom_filter_false_positive_rate() {
        let mut builder = BloomFilterBuilder::new(10);

        // Add 1000 keys
        for i in 0..1000 {
            let key = format!("key_{:06}", i);
            builder.add(key.as_bytes());
        }

        let filter = builder.build();

        // Test 10000 keys that were NOT added
        let mut false_positives = 0;
        for i in 1000..11000 {
            let key = format!("key_{:06}", i);
            if filter.may_contain(key.as_bytes()) {
                false_positives += 1;
            }
        }

        // With 10 bits per key, expected false positive rate is ~1%
        // Allow some margin for randomness
        let fp_rate = false_positives as f64 / 10000.0;
        assert!(
            fp_rate < 0.02,
            "False positive rate too high: {:.2}%",
            fp_rate * 100.0
        );
    }

    #[test]
    fn test_bloom_filter_encode_decode() {
        let mut builder = BloomFilterBuilder::new(10);
        builder.add(b"key1");
        builder.add(b"key2");
        builder.add(b"key3");

        let data = builder.finish();
        let filter = BloomFilter::from_bytes(data).unwrap();

        assert!(filter.may_contain(b"key1"));
        assert!(filter.may_contain(b"key2"));
        assert!(filter.may_contain(b"key3"));
    }

    #[test]
    fn test_bloom_filter_builder_reset() {
        let mut builder = BloomFilterBuilder::new(10);
        builder.add(b"key1");
        assert_eq!(builder.len(), 1);

        builder.reset();
        assert!(builder.is_empty());

        builder.add(b"key2");
        let filter = builder.build();
        assert!(filter.may_contain(b"key2"));
        // key1 should NOT be found (may have false positive but unlikely)
    }

    #[test]
    fn test_bloom_hash() {
        // Just verify hash produces different values for different keys
        let h1 = bloom_hash(b"key1");
        let h2 = bloom_hash(b"key2");
        assert_ne!(h1, h2);

        // Same key should produce same hash
        let h3 = bloom_hash(b"key1");
        assert_eq!(h1, h3);
    }

    #[test]
    fn test_bloom_filter_bits_per_key() {
        // More bits per key should result in lower false positive rate
        let keys: Vec<String> = (0..100).map(|i| format!("key_{:04}", i)).collect();

        let fp_rates: Vec<f64> = [5, 10, 15, 20]
            .iter()
            .map(|&bits_per_key| {
                let mut builder = BloomFilterBuilder::new(bits_per_key);
                for key in &keys {
                    builder.add(key.as_bytes());
                }
                let filter = builder.build();

                let mut false_positives = 0;
                for i in 100..1100 {
                    let key = format!("key_{:04}", i);
                    if filter.may_contain(key.as_bytes()) {
                        false_positives += 1;
                    }
                }
                false_positives as f64 / 1000.0
            })
            .collect();

        // Higher bits per key should have lower false positive rate
        for i in 1..fp_rates.len() {
            assert!(
                fp_rates[i] <= fp_rates[i - 1] + 0.05, // Allow small margin
                "False positive rate should decrease with more bits"
            );
        }
    }
}
