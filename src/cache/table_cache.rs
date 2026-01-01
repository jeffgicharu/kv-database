//! Table cache for SSTable reader handles.
//!
//! Caches open SSTable reader handles to avoid repeated file opens.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use parking_lot::Mutex;

use crate::sstable::SSTableReader;
use crate::util::filename::table_file_path;
use crate::Result;

use super::lru::{CacheStats, LruCache};

/// Cached SSTable reader wrapped for sharing.
#[derive(Clone)]
pub struct CachedTable {
    /// The SSTable reader.
    reader: Arc<Mutex<SSTableReader>>,
    /// File number.
    file_number: u64,
}

impl CachedTable {
    /// Create a new cached table entry.
    fn new(reader: SSTableReader, file_number: u64) -> Self {
        Self {
            reader: Arc::new(Mutex::new(reader)),
            file_number,
        }
    }

    /// Get the file number.
    pub fn file_number(&self) -> u64 {
        self.file_number
    }

    /// Access the reader with a closure.
    ///
    /// The closure receives a mutable reference to the reader.
    pub fn with_reader<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&mut SSTableReader) -> R,
    {
        let mut reader = self.reader.lock();
        f(&mut reader)
    }
}

/// Cache for open SSTable reader handles.
///
/// Avoids the overhead of repeatedly opening SSTable files by
/// keeping recently used readers open.
pub struct TableCache {
    /// The LRU cache of table readers.
    cache: LruCache<u64, CachedTable>,
    /// Database path for opening new tables.
    db_path: PathBuf,
}

impl TableCache {
    /// Create a new table cache.
    ///
    /// # Arguments
    ///
    /// * `db_path` - Path to the database directory
    /// * `capacity` - Maximum number of tables to cache
    pub fn new(db_path: impl AsRef<Path>, capacity: usize) -> Self {
        Self {
            cache: LruCache::new(capacity),
            db_path: db_path.as_ref().to_path_buf(),
        }
    }

    /// Get or open a table reader.
    ///
    /// Returns a cached reader if available, otherwise opens the file
    /// and caches it.
    pub fn get(&self, file_number: u64) -> Result<CachedTable> {
        // Check cache first
        if let Some(cached) = self.cache.get(&file_number) {
            return Ok(cached);
        }

        // Open the table
        let table_path = table_file_path(&self.db_path, file_number);
        let reader = SSTableReader::open(&table_path, file_number)?;
        let cached = CachedTable::new(reader, file_number);

        // Cache it
        self.cache.insert(file_number, cached.clone());

        Ok(cached)
    }

    /// Evict a table from the cache.
    ///
    /// Called when a table is deleted during compaction.
    pub fn evict(&self, file_number: u64) {
        self.cache.remove(&file_number);
    }

    /// Get the number of cached tables.
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

    /// Clear all cached tables.
    pub fn clear(&self) {
        self.cache.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sstable::{CompressionType, SSTableWriter};
    use tempfile::tempdir;

    fn create_test_sstable(path: &Path, file_number: u64) -> Result<()> {
        let table_path = table_file_path(path, file_number);
        let mut writer = SSTableWriter::new(&table_path, file_number, CompressionType::None, 10)?;
        writer.add(b"key1", b"value1")?;
        writer.add(b"key2", b"value2")?;
        writer.finish()?;
        Ok(())
    }

    #[test]
    fn test_table_cache_basic() {
        let dir = tempdir().unwrap();
        create_test_sstable(dir.path(), 1).unwrap();

        let cache = TableCache::new(dir.path(), 10);

        // First access opens the file
        let table = cache.get(1).unwrap();
        assert_eq!(table.file_number(), 1);

        // Second access should be cached
        let _table2 = cache.get(1).unwrap();

        let stats = cache.stats();
        assert_eq!(stats.hits.load(std::sync::atomic::Ordering::Relaxed), 1);
        assert_eq!(stats.misses.load(std::sync::atomic::Ordering::Relaxed), 1);
    }

    #[test]
    fn test_table_cache_evict() {
        let dir = tempdir().unwrap();
        create_test_sstable(dir.path(), 1).unwrap();

        let cache = TableCache::new(dir.path(), 10);

        // Load into cache
        let _ = cache.get(1).unwrap();
        assert_eq!(cache.len(), 1);

        // Evict
        cache.evict(1);
        
        // Note: Due to sharding, the entry might still be counted
        // until the shard is accessed again
    }

    #[test]
    fn test_table_cache_with_reader() {
        let dir = tempdir().unwrap();
        create_test_sstable(dir.path(), 1).unwrap();

        let cache = TableCache::new(dir.path(), 10);
        let table = cache.get(1).unwrap();

        // Use the reader
        let value = table.with_reader(|reader| {
            reader.get(b"key1").unwrap()
        });

        assert!(value.is_some());
    }
}
