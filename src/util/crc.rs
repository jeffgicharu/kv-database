//! CRC32 checksum utilities.

use crc32fast::Hasher;

/// Compute CRC32 checksum of the given data.
pub fn crc32(data: &[u8]) -> u32 {
    let mut hasher = Hasher::new();
    hasher.update(data);
    hasher.finalize()
}

/// Compute CRC32 checksum of multiple data slices.
pub fn crc32_multi(slices: &[&[u8]]) -> u32 {
    let mut hasher = Hasher::new();
    for slice in slices {
        hasher.update(slice);
    }
    hasher.finalize()
}

/// Extend an existing CRC32 with more data.
pub fn crc32_extend(crc: u32, data: &[u8]) -> u32 {
    let mut hasher = Hasher::new_with_initial(crc);
    hasher.update(data);
    hasher.finalize()
}

/// Mask a CRC value for storage.
///
/// This helps avoid problems with CRCs that happen to contain
/// the same bytes as common data patterns.
pub fn mask_crc(crc: u32) -> u32 {
    // Rotate right by 15 bits and add a constant.
    ((crc >> 15) | (crc << 17)).wrapping_add(0xa282ead8)
}

/// Unmask a masked CRC value.
pub fn unmask_crc(masked: u32) -> u32 {
    let rot = masked.wrapping_sub(0xa282ead8);
    (rot >> 17) | (rot << 15)
}

/// Verify that data matches expected CRC.
pub fn verify_crc(data: &[u8], expected: u32) -> bool {
    crc32(data) == expected
}

/// Verify that data matches expected masked CRC.
pub fn verify_masked_crc(data: &[u8], masked: u32) -> bool {
    crc32(data) == unmask_crc(masked)
}

/// CRC32 builder for incremental computation.
#[derive(Clone)]
pub struct Crc32Builder {
    hasher: Hasher,
}

impl Crc32Builder {
    /// Create a new CRC32 builder.
    pub fn new() -> Self {
        Self {
            hasher: Hasher::new(),
        }
    }

    /// Create a builder with an initial CRC value.
    pub fn with_initial(crc: u32) -> Self {
        Self {
            hasher: Hasher::new_with_initial(crc),
        }
    }

    /// Update the CRC with more data.
    pub fn update(&mut self, data: &[u8]) {
        self.hasher.update(data);
    }

    /// Finalize and get the CRC value.
    pub fn finalize(self) -> u32 {
        self.hasher.finalize()
    }

    /// Get the current CRC value without consuming the builder.
    pub fn clone_finalize(&self) -> u32 {
        self.hasher.clone().finalize()
    }

    /// Reset the builder.
    pub fn reset(&mut self) {
        self.hasher.reset();
    }
}

impl Default for Crc32Builder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_crc32_empty() {
        assert_eq!(crc32(&[]), 0);
    }

    #[test]
    fn test_crc32_data() {
        let data = b"hello world";
        let crc = crc32(data);
        assert_ne!(crc, 0);

        // Same data should produce same CRC
        assert_eq!(crc32(data), crc);
    }

    #[test]
    fn test_crc32_different_data() {
        let crc1 = crc32(b"hello");
        let crc2 = crc32(b"world");
        assert_ne!(crc1, crc2);
    }

    #[test]
    fn test_crc32_multi() {
        let data = b"hello world";
        let crc1 = crc32(data);
        let crc2 = crc32_multi(&[b"hello ", b"world"]);
        assert_eq!(crc1, crc2);
    }

    #[test]
    fn test_crc32_extend() {
        let crc1 = crc32(b"hello world");

        let crc_partial = crc32(b"hello ");
        let crc2 = crc32_extend(crc_partial, b"world");

        assert_eq!(crc1, crc2);
    }

    #[test]
    fn test_mask_unmask() {
        let original = 0x12345678u32;
        let masked = mask_crc(original);
        let unmasked = unmask_crc(masked);

        assert_ne!(masked, original);
        assert_eq!(unmasked, original);
    }

    #[test]
    fn test_verify_crc() {
        let data = b"test data";
        let crc = crc32(data);

        assert!(verify_crc(data, crc));
        assert!(!verify_crc(data, crc + 1));
        assert!(!verify_crc(b"other data", crc));
    }

    #[test]
    fn test_verify_masked_crc() {
        let data = b"test data";
        let crc = crc32(data);
        let masked = mask_crc(crc);

        assert!(verify_masked_crc(data, masked));
        assert!(!verify_masked_crc(data, masked + 1));
    }

    #[test]
    fn test_crc32_builder() {
        let data = b"hello world";

        let mut builder = Crc32Builder::new();
        builder.update(b"hello ");
        builder.update(b"world");
        let crc1 = builder.finalize();

        let crc2 = crc32(data);
        assert_eq!(crc1, crc2);
    }

    #[test]
    fn test_crc32_builder_clone_finalize() {
        let mut builder = Crc32Builder::new();
        builder.update(b"hello");

        let intermediate = builder.clone_finalize();
        builder.update(b" world");
        let final_crc = builder.finalize();

        assert_ne!(intermediate, final_crc);
        assert_eq!(intermediate, crc32(b"hello"));
        assert_eq!(final_crc, crc32(b"hello world"));
    }
}
