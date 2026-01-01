//! Block cache for SSTable data blocks.
//!
//! Caches decompressed data blocks to avoid repeated disk reads
//! and decompression.

use std::sync::Arc;

use bytes::Bytes;

use super::lru::{CacheStats, LruCache};

/// Key for block cache: (file_number, block_offset).
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
pub struct BlockCacheKey {
    /// SSTable file number.
    pub file_number: u64,
    /// Offset of the block within the file.
    pub block_offset: u64,
}

impl BlockCacheKey {
    /// Create a new block cache key.
    pub fn new(file_number: u64, block_offset: u64) -> Self {
        Self {
            file_number,
            block_offset,
        }
    }
}

/// Cached block data.
#[derive(Debug, Clone)]
pub struct CachedBlock {
    /// Decompressed block data.
    pub data: Bytes,
    /// Size in bytes (for memory tracking).
    pub size: usize,
}

impl CachedBlock {
    /// Create a new cached block.
    pub fn new(data: Bytes) -> Self {
        let size = data.len();
        Self { data, size }
    }
}

/// Block cache for SSTable data blocks.
///
/// Caches decompressed blocks keyed by (file_number, block_offset).
/// Uses LRU eviction when the cache is full.
pub struct BlockCache {
    cache: LruCache<BlockCacheKey, CachedBlock>,
}

impl BlockCache {
    /// Create a new block cache with the given capacity (in number of blocks).
    pub fn new(capacity: usize) -> Self {
        Self {
            cache: LruCache::new(capacity),
        }
    }

    /// Get a block from the cache.
    pub fn get(&self, file_number: u64, block_offset: u64) -> Option<CachedBlock> {
        let key = BlockCacheKey::new(file_number, block_offset);
        self.cache.get(&key)
    }

    /// Insert a block into the cache.
    pub fn insert(&self, file_number: u64, block_offset: u64, block: CachedBlock) {
        let key = BlockCacheKey::new(file_number, block_offset);
        self.cache.insert(key, block);
    }

    /// Invalidate all blocks for a specific file.
    ///
    /// This is called when a file is deleted during compaction.
    pub fn invalidate_file(&self, _file_number: u64) {
        // Note: Our current LRU implementation doesn't support efficient
        // prefix deletion. For now, we rely on natural LRU eviction.
        // A more sophisticated implementation would track blocks per file.
    }

    /// Get the number of cached blocks.
    pub fn len(&self) -> usize {
        self.cache.len()
    }

    /// Check if the cache is empty.
    pub fn is_empty(&self) -> bool {
        self.cache.is_empty()
    }

    /// Get cache statistics.
    pub fn stats(&self) -> Arc<CacheStats> {
        self.cache.stats()
    }

    /// Clear all cached blocks.
    pub fn clear(&self) {
        self.cache.clear();
    }
}

impl Default for BlockCache {
    fn default() -> Self {
        // Default: cache up to 1000 blocks
        Self::new(1000)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_block_cache_basic() {
        let cache = BlockCache::new(10);

        let block = CachedBlock::new(Bytes::from("test data"));
        cache.insert(1, 0, block.clone());

        let cached = cache.get(1, 0).unwrap();
        assert_eq!(cached.data.as_ref(), b"test data");
    }

    #[test]
    fn test_block_cache_miss() {
        let cache = BlockCache::new(10);

        assert!(cache.get(1, 0).is_none());
    }

    #[test]
    fn test_block_cache_multiple_files() {
        let cache = BlockCache::new(10);

        cache.insert(1, 0, CachedBlock::new(Bytes::from("file1_block0")));
        cache.insert(1, 100, CachedBlock::new(Bytes::from("file1_block100")));
        cache.insert(2, 0, CachedBlock::new(Bytes::from("file2_block0")));

        assert_eq!(cache.get(1, 0).unwrap().data.as_ref(), b"file1_block0");
        assert_eq!(cache.get(1, 100).unwrap().data.as_ref(), b"file1_block100");
        assert_eq!(cache.get(2, 0).unwrap().data.as_ref(), b"file2_block0");
    }

    #[test]
    fn test_block_cache_stats() {
        let cache = BlockCache::new(10);

        cache.insert(1, 0, CachedBlock::new(Bytes::from("data")));

        // Hit
        cache.get(1, 0);
        // Miss
        cache.get(2, 0);

        let stats = cache.stats();
        assert_eq!(stats.hits.load(std::sync::atomic::Ordering::Relaxed), 1);
        assert_eq!(stats.misses.load(std::sync::atomic::Ordering::Relaxed), 1);
    }
}
