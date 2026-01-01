//! Caching infrastructure for the database.
//!
//! This module provides caching to improve read performance:
//!
//! - **LRU Cache**: Generic sharded LRU cache for concurrent access
//! - **Block Cache**: Caches decompressed SSTable data blocks
//! - **Table Cache**: Caches open SSTable reader handles

mod block_cache;
mod lru;
mod table_cache;

pub use block_cache::{BlockCache, BlockCacheKey, CachedBlock};
pub use lru::{CacheStats, LruCache};
pub use table_cache::{CachedTable, TableCache};
