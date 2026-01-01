//! Key comparison utilities.

use std::cmp::Ordering;

/// Trait for comparing keys.
pub trait Comparator: Send + Sync {
    /// Compare two keys.
    fn compare(&self, a: &[u8], b: &[u8]) -> Ordering;

    /// Get the name of this comparator.
    fn name(&self) -> &str;

    /// Find the shortest separator between two keys.
    ///
    /// Returns a key `sep` such that `start <= sep < limit`.
    /// This is used to find separator keys for index blocks.
    fn find_shortest_separator(&self, start: &[u8], limit: &[u8]) -> Vec<u8>;

    /// Find the shortest successor for a key.
    ///
    /// Returns a key `succ` such that `key < succ`.
    fn find_short_successor(&self, key: &[u8]) -> Vec<u8>;
}

/// Default bytewise comparator (lexicographic ordering).
#[derive(Debug, Clone, Copy, Default)]
pub struct BytewiseComparator;

impl BytewiseComparator {
    /// Create a new bytewise comparator.
    pub fn new() -> Self {
        Self
    }
}

impl Comparator for BytewiseComparator {
    fn compare(&self, a: &[u8], b: &[u8]) -> Ordering {
        a.cmp(b)
    }

    fn name(&self) -> &str {
        "leveldb.BytewiseComparator"
    }

    fn find_shortest_separator(&self, start: &[u8], limit: &[u8]) -> Vec<u8> {
        // Find the length of the common prefix
        let min_len = std::cmp::min(start.len(), limit.len());
        let mut diff_index = 0;

        while diff_index < min_len && start[diff_index] == limit[diff_index] {
            diff_index += 1;
        }

        if diff_index >= min_len {
            // One key is a prefix of the other, or they are equal
            return start.to_vec();
        }

        // We found a differing byte
        let diff_byte = start[diff_index];

        // If we can increment the byte and it's still less than limit,
        // we have a shorter separator
        if diff_byte < 0xFF && diff_byte + 1 < limit[diff_index] {
            let mut result = start[..=diff_index].to_vec();
            result[diff_index] += 1;
            return result;
        }

        start.to_vec()
    }

    fn find_short_successor(&self, key: &[u8]) -> Vec<u8> {
        // Find the first byte that can be incremented
        for (i, &byte) in key.iter().enumerate() {
            if byte != 0xFF {
                let mut result = key[..=i].to_vec();
                result[i] += 1;
                return result;
            }
        }

        // All bytes are 0xFF, return the key as-is
        key.to_vec()
    }
}

/// Compare internal keys (user_key + sequence + type).
///
/// Internal keys are compared by:
/// 1. User key in ascending order
/// 2. Sequence number in descending order (newer first)
/// 3. Type in descending order
#[derive(Debug, Clone, Copy, Default)]
pub struct InternalKeyComparator {
    user_comparator: BytewiseComparator,
}

impl InternalKeyComparator {
    /// Create a new internal key comparator.
    pub fn new() -> Self {
        Self {
            user_comparator: BytewiseComparator::new(),
        }
    }

    /// Extract the user key from an internal key.
    pub fn user_key<'a>(&self, internal_key: &'a [u8]) -> &'a [u8] {
        if internal_key.len() < 8 {
            internal_key
        } else {
            &internal_key[..internal_key.len() - 8]
        }
    }

    /// Get the user comparator.
    pub fn user_comparator(&self) -> &BytewiseComparator {
        &self.user_comparator
    }
}

impl Comparator for InternalKeyComparator {
    fn compare(&self, a: &[u8], b: &[u8]) -> Ordering {
        // Extract user keys
        let user_key_a = self.user_key(a);
        let user_key_b = self.user_key(b);

        // First compare user keys
        match self.user_comparator.compare(user_key_a, user_key_b) {
            Ordering::Equal => {
                // For same user key, compare by sequence number (descending)
                // The last 8 bytes contain packed (sequence << 8 | type)
                if a.len() >= 8 && b.len() >= 8 {
                    let num_a = u64::from_be_bytes(a[a.len() - 8..].try_into().unwrap());
                    let num_b = u64::from_be_bytes(b[b.len() - 8..].try_into().unwrap());
                    // Descending order: larger sequence number comes first
                    num_b.cmp(&num_a)
                } else {
                    // Malformed keys, fall back to length comparison
                    a.len().cmp(&b.len())
                }
            }
            ord => ord,
        }
    }

    fn name(&self) -> &str {
        "leveldb.InternalKeyComparator"
    }

    fn find_shortest_separator(&self, start: &[u8], limit: &[u8]) -> Vec<u8> {
        // Use the user key portions
        let user_start = self.user_key(start);
        let user_limit = self.user_key(limit);

        let separator = self
            .user_comparator
            .find_shortest_separator(user_start, user_limit);

        if separator.len() < user_start.len()
            && self.user_comparator.compare(user_start, &separator) == Ordering::Less
        {
            // We found a shorter separator at the user key level.
            // Append the max sequence number to make it an internal key.
            let mut result = separator;
            // Append (max_sequence << 8 | type_for_seek)
            // This ensures this separator is >= any key with this user key
            let packed = (u64::MAX >> 8) << 8 | 1u64; // max sequence, type = Value
            result.extend_from_slice(&packed.to_be_bytes());
            return result;
        }

        start.to_vec()
    }

    fn find_short_successor(&self, key: &[u8]) -> Vec<u8> {
        let user_key = self.user_key(key);
        let successor = self.user_comparator.find_short_successor(user_key);

        if successor.len() < user_key.len()
            && self.user_comparator.compare(user_key, &successor) == Ordering::Less
        {
            // Found shorter successor
            let mut result = successor;
            let packed = (u64::MAX >> 8) << 8 | 1u64;
            result.extend_from_slice(&packed.to_be_bytes());
            return result;
        }

        key.to_vec()
    }
}

/// Reverse comparator - inverts the ordering of another comparator.
#[derive(Debug, Clone)]
pub struct ReverseComparator<C: Comparator> {
    inner: C,
}

impl<C: Comparator> ReverseComparator<C> {
    /// Create a new reverse comparator.
    pub fn new(inner: C) -> Self {
        Self { inner }
    }
}

impl<C: Comparator> Comparator for ReverseComparator<C> {
    fn compare(&self, a: &[u8], b: &[u8]) -> Ordering {
        self.inner.compare(b, a)
    }

    fn name(&self) -> &str {
        "reverse"
    }

    fn find_shortest_separator(&self, start: &[u8], limit: &[u8]) -> Vec<u8> {
        self.inner.find_shortest_separator(limit, start)
    }

    fn find_short_successor(&self, key: &[u8]) -> Vec<u8> {
        // For reverse, we want the predecessor, but that's complex
        // Just return the key as-is
        key.to_vec()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bytewise_compare() {
        let cmp = BytewiseComparator::new();

        assert_eq!(cmp.compare(b"abc", b"abc"), Ordering::Equal);
        assert_eq!(cmp.compare(b"abc", b"abd"), Ordering::Less);
        assert_eq!(cmp.compare(b"abd", b"abc"), Ordering::Greater);
        assert_eq!(cmp.compare(b"ab", b"abc"), Ordering::Less);
        assert_eq!(cmp.compare(b"abc", b"ab"), Ordering::Greater);
        assert_eq!(cmp.compare(b"", b""), Ordering::Equal);
        assert_eq!(cmp.compare(b"", b"a"), Ordering::Less);
    }

    #[test]
    fn test_find_shortest_separator() {
        let cmp = BytewiseComparator::new();

        // "abc" and "xyz" - can use "b" as separator
        let sep = cmp.find_shortest_separator(b"abc", b"xyz");
        assert!(sep.as_slice() >= b"abc".as_slice());
        assert!(sep.as_slice() < b"xyz".as_slice());

        // "abc" and "abd" - can use "abc" followed by something
        let sep = cmp.find_shortest_separator(b"abc", b"abd");
        assert!(sep.as_slice() >= b"abc".as_slice());
        assert!(sep.as_slice() < b"abd".as_slice());

        // Same key
        let sep = cmp.find_shortest_separator(b"abc", b"abc");
        assert_eq!(sep.as_slice(), b"abc".as_slice());
    }

    #[test]
    fn test_find_short_successor() {
        let cmp = BytewiseComparator::new();

        let succ = cmp.find_short_successor(b"abc");
        assert!(succ.as_slice() > b"abc".as_slice());

        // All 0xFF bytes
        let succ = cmp.find_short_successor(&[0xFF, 0xFF, 0xFF]);
        assert_eq!(succ.as_slice(), &[0xFF, 0xFF, 0xFF]);
    }

    #[test]
    fn test_internal_key_comparator() {
        let cmp = InternalKeyComparator::new();

        // Create internal keys: user_key + (sequence << 8 | type)
        let make_key = |user_key: &[u8], seq: u64| {
            let mut key = user_key.to_vec();
            let packed = (seq << 8) | 1u64; // type = Value
            key.extend_from_slice(&packed.to_be_bytes());
            key
        };

        // Same user key, different sequences
        let key1 = make_key(b"user", 100);
        let key2 = make_key(b"user", 200);

        // Higher sequence should come first (descending)
        assert_eq!(cmp.compare(&key2, &key1), Ordering::Less);
        assert_eq!(cmp.compare(&key1, &key2), Ordering::Greater);

        // Different user keys
        let key3 = make_key(b"aaa", 100);
        let key4 = make_key(b"bbb", 100);
        assert_eq!(cmp.compare(&key3, &key4), Ordering::Less);
    }

    #[test]
    fn test_reverse_comparator() {
        let cmp = ReverseComparator::new(BytewiseComparator::new());

        assert_eq!(cmp.compare(b"abc", b"abd"), Ordering::Greater);
        assert_eq!(cmp.compare(b"abd", b"abc"), Ordering::Less);
        assert_eq!(cmp.compare(b"abc", b"abc"), Ordering::Equal);
    }
}
