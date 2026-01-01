//! Database - Core database implementation.
//!
//! The Database struct is the main entry point for all operations.
//! It coordinates:
//! - MemTable for in-memory writes
//! - WAL for durability
//! - SSTables for persistent storage
//! - Version management for file tracking
//!
//! # Thread Safety
//!
//! The Database is thread-safe and can be shared across threads using Arc.
//! Multiple readers can access concurrently, and writers are serialized.

use std::collections::VecDeque;
use std::fs::{File, OpenOptions};
use std::io::Write as IoWrite;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use parking_lot::{Mutex, RwLock};

use crate::memtable::{ImmutableMemTable, MemTable};
use crate::options::Options;
use crate::sstable::{CompressionType, SSTableReader, SSTableWriter};
use crate::types::{InternalKey, LookupResult, WriteBatch};
use crate::util::filename::{
    create_dir_if_missing, lock_file_path, log_file_path, table_file_path,
};
use crate::version::{database_exists, FileMetadata, VersionEdit, VersionSet};
use crate::wal::{WalReader, WalWriter};
use crate::{Error, Result};

/// The core database struct.
///
/// Thread-safe database that supports concurrent reads and serialized writes.
pub struct Database {
    /// Database directory path.
    db_path: PathBuf,
    /// Database options.
    options: Arc<Options>,
    /// Version set for file management.
    versions: Arc<VersionSet>,
    /// Active memtable for writes.
    memtable: RwLock<Arc<MemTable>>,
    /// Immutable memtables waiting to be flushed.
    imm_memtables: RwLock<VecDeque<ImmutableMemTable>>,
    /// Current WAL writer.
    wal: Mutex<Option<WalWriter>>,
    /// Current WAL file number.
    wal_number: AtomicU64,
    /// Next sequence number to assign.
    sequence: AtomicU64,
    /// Lock file handle (kept open to hold the lock).
    _lock_file: File,
    /// Whether the database is shutting down.
    shutting_down: AtomicBool,
    /// Write mutex for serializing writes.
    write_mutex: Mutex<()>,
    /// Background error (if any).
    bg_error: RwLock<Option<Error>>,
}

impl Database {
    /// Open a database at the given path.
    ///
    /// If the database doesn't exist and `create_if_missing` is true,
    /// a new database will be created.
    pub fn open(path: impl AsRef<Path>) -> Result<Arc<Self>> {
        Self::open_with_options(path, Options::default())
    }

    /// Open a database with custom options.
    pub fn open_with_options(path: impl AsRef<Path>, options: Options) -> Result<Arc<Self>> {
        let db_path = path.as_ref().to_path_buf();
        let options = Arc::new(options);

        // Create directory if needed
        if !db_path.exists() {
            if options.create_if_missing {
                create_dir_if_missing(&db_path)?;
            } else {
                return Err(Error::NotFound(format!(
                    "Database directory does not exist: {}",
                    db_path.display()
                )));
            }
        } else if options.error_if_exists && database_exists(&db_path) {
            return Err(Error::AlreadyExists(format!(
                "Database already exists: {}",
                db_path.display()
            )));
        }

        // Acquire lock file
        let lock_file = Self::acquire_lock(&db_path)?;

        // Create version set
        let versions = Arc::new(VersionSet::new(&db_path, Arc::clone(&options)));

        // Check if this is a new database or recovery
        let is_new_db = !database_exists(&db_path);

        let (memtable, wal, wal_number, last_sequence) = if is_new_db {
            // New database: create initial manifest
            let wal_number = versions.new_file_number();
            let memtable_id = versions.new_file_number();

            // Create initial manifest
            let mut init_edit = VersionEdit::new();
            init_edit.set_comparator("leveldb.BytewiseComparator");
            init_edit.set_log_number(wal_number);
            init_edit.set_next_file_number(versions.next_file_number());
            init_edit.set_last_sequence(0);
            versions.create_new_manifest(&init_edit)?;

            // Create WAL
            let wal_path = log_file_path(&db_path, wal_number);
            let wal = WalWriter::new(&wal_path, wal_number, options.sync_mode)?;

            // Create memtable
            let memtable = Arc::new(MemTable::new(memtable_id));

            (memtable, Some(wal), wal_number, 0u64)
        } else {
            // Existing database: recover
            versions.recover()?;

            let last_sequence = versions.last_sequence();
            let log_number = versions.log_number();

            // Replay WAL to recover memtable
            let wal_path = log_file_path(&db_path, log_number);
            let memtable_id = versions.new_file_number();
            let memtable = Arc::new(MemTable::new(memtable_id));

            if wal_path.exists() {
                Self::replay_wal(&wal_path, log_number, &memtable, last_sequence)?;
            }

            // Open WAL for appending
            let wal = if wal_path.exists() {
                Some(WalWriter::open_for_append(
                    &wal_path,
                    log_number,
                    options.sync_mode,
                )?)
            } else {
                // Create new WAL
                Some(WalWriter::new(&wal_path, log_number, options.sync_mode)?)
            };

            (memtable, wal, log_number, last_sequence)
        };

        let db = Arc::new(Self {
            db_path,
            options,
            versions,
            memtable: RwLock::new(memtable),
            imm_memtables: RwLock::new(VecDeque::new()),
            wal: Mutex::new(wal),
            wal_number: AtomicU64::new(wal_number),
            sequence: AtomicU64::new(last_sequence),
            _lock_file: lock_file,
            shutting_down: AtomicBool::new(false),
            write_mutex: Mutex::new(()),
            bg_error: RwLock::new(None),
        });

        Ok(db)
    }

    /// Acquire the database lock file.
    fn acquire_lock(db_path: &Path) -> Result<File> {
        let lock_path = lock_file_path(db_path);

        // Try to create/open the lock file exclusively
        let lock_file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&lock_path)
            .map_err(|e| {
                Error::LockError(format!(
                    "Failed to open lock file {}: {}",
                    lock_path.display(),
                    e
                ))
            })?;

        // Try to acquire an exclusive lock
        #[cfg(unix)]
        {
            use std::os::unix::io::AsRawFd;
            let fd = lock_file.as_raw_fd();
            let result = unsafe {
                libc::flock(fd, libc::LOCK_EX | libc::LOCK_NB)
            };
            if result != 0 {
                return Err(Error::LockError(
                    "Database is already locked by another process".to_string(),
                ));
            }
        }

        // Write process info to lock file
        #[allow(unused_mut)]
        let mut lock_file = lock_file;
        writeln!(lock_file, "rustdb lock").ok();

        Ok(lock_file)
    }

    /// Replay WAL to recover memtable state.
    fn replay_wal(
        wal_path: &Path,
        file_number: u64,
        memtable: &MemTable,
        mut max_sequence: u64,
    ) -> Result<u64> {
        let mut reader = WalReader::new(wal_path, file_number)?;
        reader.set_checksum_errors_fatal(false); // Be lenient during recovery

        while let Some(record) = reader.read_record()? {
            // Decode the write batch
            let batch = WriteBatch::decode(&record)?;

            // Apply each entry to the memtable
            for entry in batch.entries() {
                max_sequence += 1;

                if let Some(ref value) = entry.value {
                    let internal_key = InternalKey::for_value(entry.key.clone(), max_sequence);
                    memtable.put(&internal_key, value);
                } else {
                    let internal_key = InternalKey::for_deletion(entry.key.clone(), max_sequence);
                    memtable.delete(&internal_key);
                }
            }
        }

        Ok(max_sequence)
    }

    /// Put a key-value pair.
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let mut batch = WriteBatch::new();
        batch.put(Bytes::copy_from_slice(key), Bytes::copy_from_slice(value));
        self.write(batch)
    }

    /// Delete a key.
    pub fn delete(&self, key: &[u8]) -> Result<()> {
        let mut batch = WriteBatch::new();
        batch.delete(Bytes::copy_from_slice(key));
        self.write(batch)
    }

    /// Write a batch of operations atomically.
    pub fn write(&self, batch: WriteBatch) -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }

        // Check for background errors
        if let Some(ref err) = *self.bg_error.read() {
            return Err(err.clone());
        }

        // Serialize writes
        let _write_guard = self.write_mutex.lock();

        // Check if shutting down
        if self.shutting_down.load(Ordering::Relaxed) {
            return Err(Error::internal("Database is shutting down"));
        }

        // Make room for the write (may trigger flush)
        self.make_room_for_write()?;

        // Allocate sequence numbers
        let count = batch.len() as u64;
        let first_sequence = self.sequence.fetch_add(count, Ordering::SeqCst) + 1;

        // Write to WAL first (for durability)
        {
            let encoded = batch.encode();
            let mut wal_guard = self.wal.lock();
            if let Some(ref mut wal) = *wal_guard {
                wal.add_record(&encoded)?;
            }
        }

        // Write to memtable
        let memtable = self.memtable.read();
        let mut seq = first_sequence;

        for entry in batch.entries() {
            if let Some(ref value) = entry.value {
                let internal_key = InternalKey::for_value(entry.key.clone(), seq);
                memtable.put(&internal_key, value);
            } else {
                let internal_key = InternalKey::for_deletion(entry.key.clone(), seq);
                memtable.delete(&internal_key);
            }
            seq += 1;
        }

        // Update version set's last sequence
        self.versions.set_last_sequence(first_sequence + count - 1);

        Ok(())
    }

    /// Make room for a write by potentially flushing the memtable.
    fn make_room_for_write(&self) -> Result<()> {
        loop {
            // Check for background errors
            if let Some(ref err) = *self.bg_error.read() {
                return Err(err.clone());
            }

            // Check memtable size
            let mem_size = self.memtable.read().approximate_memory_usage();

            if mem_size < self.options.max_memtable_size {
                // Enough room
                return Ok(());
            }

            // Need to flush - convert current memtable to immutable
            self.switch_memtable()?;

            // Trigger flush of immutable memtables
            self.maybe_schedule_flush()?;

            // If we have too many immutable memtables, wait
            let imm_count = self.imm_memtables.read().len();
            if imm_count < self.options.max_write_buffers - 1 {
                return Ok(());
            }

            // For now, we'll do a synchronous flush since we don't have
            // background threads yet
            self.flush_imm_memtables()?;
        }
    }

    /// Switch to a new memtable.
    fn switch_memtable(&self) -> Result<()> {
        // Create new WAL
        let new_wal_number = self.versions.new_file_number();
        let wal_path = log_file_path(&self.db_path, new_wal_number);
        let new_wal = WalWriter::new(&wal_path, new_wal_number, self.options.sync_mode)?;

        // Create new memtable
        let memtable_id = self.versions.new_file_number();
        let new_memtable = Arc::new(MemTable::new(memtable_id));

        // Swap memtable and WAL
        let old_memtable = {
            let mut mem_guard = self.memtable.write();
            let old = Arc::clone(&*mem_guard);
            *mem_guard = new_memtable;
            old
        };

        let old_wal = {
            let mut wal_guard = self.wal.lock();
            self.wal_number.store(new_wal_number, Ordering::SeqCst);
            wal_guard.replace(new_wal)
        };

        // Close old WAL
        if let Some(wal) = old_wal {
            wal.close()?;
        }

        // Add old memtable to immutable list
        {
            let imm = ImmutableMemTable::from_arc(old_memtable);
            self.imm_memtables.write().push_back(imm);
        }

        Ok(())
    }

    /// Maybe schedule a flush (for now, this is synchronous).
    fn maybe_schedule_flush(&self) -> Result<()> {
        // In a full implementation, this would trigger background flush
        // For now, we'll check if flush is needed
        Ok(())
    }

    /// Flush immutable memtables to SSTables.
    fn flush_imm_memtables(&self) -> Result<()> {
        while let Some(imm) = self.imm_memtables.write().pop_front() {
            self.flush_memtable(&imm)?;
        }
        Ok(())
    }

    /// Flush a single memtable to an SSTable.
    fn flush_memtable(&self, imm: &ImmutableMemTable) -> Result<()> {
        // Skip empty memtables
        if imm.inner().is_empty() {
            return Ok(());
        }

        // Allocate file number
        let file_number = self.versions.new_file_number();
        let table_path = table_file_path(&self.db_path, file_number);

        // Build SSTable
        let compression = match self.options.compression {
            crate::options::Compression::None => CompressionType::None,
            crate::options::Compression::Lz4 => CompressionType::Lz4,
            crate::options::Compression::Snappy => CompressionType::Snappy,
        };

        let mut writer = SSTableWriter::new(
            &table_path,
            file_number,
            compression,
            self.options.bloom_filter_bits_per_key,
        )?;

        // Collect and sort entries by encoded key (byte order for SSTable)
        let mut entries: Vec<_> = imm.iter().collect();
        entries.sort_by(|(a, _), (b, _)| a.encode().cmp(&b.encode()));

        let mut smallest_key: Option<InternalKey> = None;
        let mut largest_key: Option<InternalKey> = None;

        for (internal_key, value) in entries {
            // Track key range
            if smallest_key.is_none() {
                smallest_key = Some(internal_key.clone());
            }
            largest_key = Some(internal_key.clone());

            // Write entry
            let encoded_key = internal_key.encode();
            writer.add(&encoded_key, &value)?;
        }

        // Finish writing
        let info = writer.finish()?;

        // Create version edit
        if let (Some(smallest), Some(largest)) = (smallest_key, largest_key) {
            let file_meta = FileMetadata::new(file_number, info.file_size, smallest, largest);

            let mut edit = VersionEdit::new();
            edit.add_file(0, file_meta); // Add to L0
            edit.set_log_number(self.wal_number.load(Ordering::SeqCst));

            // Apply edit
            self.versions.log_and_apply(&mut edit)?;
        }

        Ok(())
    }

    /// Get a value by key.
    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.get_at_sequence(key, self.sequence.load(Ordering::SeqCst))
    }

    /// Get a value at a specific sequence number (for snapshots).
    pub fn get_at_sequence(&self, key: &[u8], sequence: u64) -> Result<Option<Bytes>> {
        // Check for background errors
        if let Some(ref err) = *self.bg_error.read() {
            return Err(err.clone());
        }

        // First, check active memtable
        {
            let memtable = self.memtable.read();
            match memtable.get(key, sequence) {
                LookupResult::Found(value) => return Ok(Some(value)),
                LookupResult::Deleted => return Ok(None),
                LookupResult::NotFound => {}
            }
        }

        // Check immutable memtables (newest first)
        {
            let imm_tables = self.imm_memtables.read();
            for imm in imm_tables.iter().rev() {
                match imm.get(key, sequence) {
                    LookupResult::Found(value) => return Ok(Some(value)),
                    LookupResult::Deleted => return Ok(None),
                    LookupResult::NotFound => {}
                }
            }
        }

        // Check SSTables in version
        self.get_from_sstables(key, sequence)
    }

    /// Search for a key in SSTables.
    fn get_from_sstables(&self, key: &[u8], sequence: u64) -> Result<Option<Bytes>> {
        let version = self.versions.current();

        // Check Level 0 (may have overlapping files, check all - newest first)
        // L0 files can overlap, so we need to check all of them and keep the best match

        for file in version.files(0) {
            if !file.may_contain_key(key) {
                continue;
            }

            let table_path = table_file_path(&self.db_path, file.file_number());
            let mut reader = SSTableReader::open(&table_path, file.file_number())?;

            if let Some(value) = reader.get_user_key(key, sequence)? {
                // Found a value - since L0 files can overlap, we take the first match
                // (files are ordered newest first)
                return Ok(Some(value));
            }
        }

        // Check higher levels (non-overlapping, can use binary search)
        for level in 1..7 {
            let files = version.get_files_for_key(level, key);

            for file in files {
                let table_path = table_file_path(&self.db_path, file.file_number());
                let mut reader = SSTableReader::open(&table_path, file.file_number())?;

                if let Some(value) = reader.get_user_key(key, sequence)? {
                    return Ok(Some(value));
                }
            }
        }

        Ok(None)
    }

    /// Flush the active memtable to disk.
    pub fn flush(&self) -> Result<()> {
        let _write_guard = self.write_mutex.lock();

        // Switch memtable
        self.switch_memtable()?;

        // Flush all immutable memtables
        self.flush_imm_memtables()?;

        Ok(())
    }

    /// Force compaction of all levels.
    pub fn compact(&self) -> Result<()> {
        // Flush memtable first
        self.flush()?;

        // Compaction will be implemented in Phase 7
        Ok(())
    }

    /// Get database statistics.
    pub fn stats(&self) -> DatabaseStats {
        let version = self.versions.current();
        let mut level_stats = Vec::new();

        for level in 0..7 {
            let num_files = version.num_files(level);
            let size = version.level_size(level);
            if num_files > 0 || level == 0 {
                level_stats.push(LevelStats {
                    level,
                    num_files,
                    size_bytes: size,
                });
            }
        }

        DatabaseStats {
            memtable_size: self.memtable.read().approximate_memory_usage(),
            imm_memtable_count: self.imm_memtables.read().len(),
            level_stats,
            sequence: self.sequence.load(Ordering::SeqCst),
        }
    }

    /// Close the database gracefully.
    pub fn close(&self) -> Result<()> {
        // Mark as shutting down
        self.shutting_down.store(true, Ordering::SeqCst);

        // Acquire write lock to ensure no concurrent writes
        let _write_guard = self.write_mutex.lock();

        // Flush memtable
        if !self.memtable.read().is_empty() {
            // We need to be careful here since switch_memtable creates a new memtable
            // Just flush what we have
            let memtable = self.memtable.read();
            if !memtable.is_empty() {
                let imm = ImmutableMemTable::new(MemTable::new(memtable.id()));
                // Copy entries (simplified - in production you'd want a better approach)
                for (key, value) in memtable.iter() {
                    imm.inner().put(&key, &value);
                }
                if !imm.inner().is_empty() {
                    self.flush_memtable(&imm)?;
                }
            }
        }

        // Flush immutable memtables
        self.flush_imm_memtables()?;

        // Close WAL
        let wal = self.wal.lock().take();
        if let Some(wal) = wal {
            wal.close()?;
        }

        Ok(())
    }

    /// Get the database path.
    pub fn path(&self) -> &Path {
        &self.db_path
    }

    /// Get the current sequence number.
    pub fn sequence(&self) -> u64 {
        self.sequence.load(Ordering::SeqCst)
    }
}

impl Drop for Database {
    fn drop(&mut self) {
        // Try to close gracefully, ignore errors
        let _ = self.close();
    }
}

/// Database statistics.
#[derive(Debug, Clone)]
pub struct DatabaseStats {
    /// Active memtable size.
    pub memtable_size: usize,
    /// Number of immutable memtables.
    pub imm_memtable_count: usize,
    /// Per-level statistics.
    pub level_stats: Vec<LevelStats>,
    /// Current sequence number.
    pub sequence: u64,
}

/// Statistics for a single level.
#[derive(Debug, Clone)]
pub struct LevelStats {
    /// Level number.
    pub level: usize,
    /// Number of files at this level.
    pub num_files: usize,
    /// Total size in bytes.
    pub size_bytes: u64,
}

impl std::fmt::Display for DatabaseStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Database Statistics:")?;
        writeln!(
            f,
            "  Memtable: {} bytes",
            self.memtable_size
        )?;
        writeln!(
            f,
            "  Immutable memtables: {}",
            self.imm_memtable_count
        )?;
        writeln!(f, "  Sequence: {}", self.sequence)?;
        writeln!(f, "  Levels:")?;
        for level in &self.level_stats {
            writeln!(
                f,
                "    L{}: {} files, {:.2} MB",
                level.level,
                level.num_files,
                level.size_bytes as f64 / (1024.0 * 1024.0)
            )?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_database_open_new() {
        let dir = tempdir().unwrap();
        let db = Database::open(dir.path()).unwrap();

        assert_eq!(db.path(), dir.path());
        drop(db);
    }

    #[test]
    fn test_database_put_get() {
        let dir = tempdir().unwrap();
        let db = Database::open(dir.path()).unwrap();

        db.put(b"hello", b"world").unwrap();

        let value = db.get(b"hello").unwrap();
        assert_eq!(value, Some(Bytes::from("world")));

        let missing = db.get(b"missing").unwrap();
        assert_eq!(missing, None);
    }

    #[test]
    fn test_database_delete() {
        let dir = tempdir().unwrap();
        let db = Database::open(dir.path()).unwrap();

        db.put(b"key", b"value").unwrap();
        assert!(db.get(b"key").unwrap().is_some());

        db.delete(b"key").unwrap();
        assert!(db.get(b"key").unwrap().is_none());
    }

    #[test]
    fn test_database_write_batch() {
        let dir = tempdir().unwrap();
        let db = Database::open(dir.path()).unwrap();

        let mut batch = WriteBatch::new();
        batch.put(&b"key1"[..], &b"value1"[..]);
        batch.put(&b"key2"[..], &b"value2"[..]);
        batch.put(&b"key3"[..], &b"value3"[..]);
        db.write(batch).unwrap();

        assert_eq!(db.get(b"key1").unwrap(), Some(Bytes::from("value1")));
        assert_eq!(db.get(b"key2").unwrap(), Some(Bytes::from("value2")));
        assert_eq!(db.get(b"key3").unwrap(), Some(Bytes::from("value3")));
    }

    #[test]
    fn test_database_persistence() {
        let dir = tempdir().unwrap();

        // Write data
        {
            let db = Database::open(dir.path()).unwrap();
            db.put(b"persistent_key", b"persistent_value").unwrap();
            db.flush().unwrap();
        }

        // Reopen and read
        {
            let db = Database::open(dir.path()).unwrap();
            let value = db.get(b"persistent_key").unwrap();
            assert_eq!(value, Some(Bytes::from("persistent_value")));
        }
    }

    #[test]
    fn test_database_wal_recovery() {
        let dir = tempdir().unwrap();

        // Write data without explicit flush (WAL only)
        {
            let db = Database::open(dir.path()).unwrap();
            db.put(b"wal_key", b"wal_value").unwrap();
            // Don't call flush - data is only in WAL
        }

        // Reopen - should recover from WAL
        {
            let db = Database::open(dir.path()).unwrap();
            let value = db.get(b"wal_key").unwrap();
            assert_eq!(value, Some(Bytes::from("wal_value")));
        }
    }

    #[test]
    fn test_database_multiple_writes() {
        let dir = tempdir().unwrap();
        let db = Database::open(dir.path()).unwrap();

        // Write many keys
        for i in 0..100 {
            let key = format!("key{:04}", i);
            let value = format!("value{}", i);
            db.put(key.as_bytes(), value.as_bytes()).unwrap();
        }

        // Verify all keys
        for i in 0..100 {
            let key = format!("key{:04}", i);
            let expected = format!("value{}", i);
            let value = db.get(key.as_bytes()).unwrap();
            assert_eq!(value, Some(Bytes::from(expected)));
        }
    }

    #[test]
    fn test_database_overwrite() {
        let dir = tempdir().unwrap();
        let db = Database::open(dir.path()).unwrap();

        db.put(b"key", b"value1").unwrap();
        assert_eq!(db.get(b"key").unwrap(), Some(Bytes::from("value1")));

        db.put(b"key", b"value2").unwrap();
        assert_eq!(db.get(b"key").unwrap(), Some(Bytes::from("value2")));

        db.put(b"key", b"value3").unwrap();
        assert_eq!(db.get(b"key").unwrap(), Some(Bytes::from("value3")));
    }

    #[test]
    fn test_database_stats() {
        let dir = tempdir().unwrap();
        let db = Database::open(dir.path()).unwrap();

        db.put(b"key", b"value").unwrap();

        let stats = db.stats();
        assert!(stats.memtable_size > 0);
        assert!(stats.sequence > 0);
    }

    #[test]
    fn test_database_flush() {
        let dir = tempdir().unwrap();
        let db = Database::open(dir.path()).unwrap();

        // Write some data
        for i in 0..10 {
            let key = format!("key{}", i);
            let value = format!("value{}", i);
            db.put(key.as_bytes(), value.as_bytes()).unwrap();
        }

        // Flush to SSTable
        db.flush().unwrap();

        // Stats should show L0 file
        let stats = db.stats();
        assert!(
            stats.level_stats.iter().any(|l| l.level == 0 && l.num_files > 0),
            "Expected at least one L0 file after flush"
        );

        // Data should still be accessible
        for i in 0..10 {
            let key = format!("key{}", i);
            let expected = format!("value{}", i);
            let value = db.get(key.as_bytes()).unwrap();
            assert_eq!(value, Some(Bytes::from(expected)));
        }
    }

    #[test]
    fn test_database_error_if_exists() {
        let dir = tempdir().unwrap();

        // Create database
        {
            let _db = Database::open(dir.path()).unwrap();
        }

        // Try to open with error_if_exists
        let mut opts = Options::default();
        opts.error_if_exists = true;

        let result = Database::open_with_options(dir.path(), opts);
        assert!(result.is_err());
    }

    #[test]
    fn test_database_not_found() {
        let dir = tempdir().unwrap();
        let non_existent = dir.path().join("not_exists");

        let mut opts = Options::default();
        opts.create_if_missing = false;

        let result = Database::open_with_options(&non_existent, opts);
        assert!(result.is_err());
    }
}
