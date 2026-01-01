//! VersionSet - Manages current Version with atomic updates.
//!
//! The VersionSet is responsible for:
//! - Tracking the current Version
//! - Applying VersionEdits atomically
//! - Managing file number allocation
//! - Tracking sequence numbers
//! - Managing the CURRENT file and manifest

use std::collections::HashSet;
use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use arc_swap::ArcSwap;
use parking_lot::RwLock;

use crate::options::{Options, SyncMode, MAX_LEVELS};
use crate::{Error, Result};

use super::{
    manifest_file_path, parse_manifest_filename, FileMetadata, Manifest, ManifestReader, Version,
    VersionEdit,
};

/// Manages the set of versions and file metadata.
///
/// The VersionSet tracks:
/// - Current Version (immutable snapshot of active files)
/// - Next file number for allocation
/// - Last sequence number
/// - Active manifest file
pub struct VersionSet {
    /// Database directory path.
    db_path: PathBuf,
    /// Current version (atomically swappable).
    current: ArcSwap<Version>,
    /// Next file number to allocate.
    next_file_number: AtomicU64,
    /// Last used sequence number.
    last_sequence: AtomicU64,
    /// Current log file number.
    log_number: AtomicU64,
    /// Previous log file number (for recovery).
    prev_log_number: AtomicU64,
    /// Current manifest writer.
    manifest: RwLock<Option<Manifest>>,
    /// Manifest file number.
    manifest_number: AtomicU64,
    /// Comparator name.
    comparator_name: String,
    /// Database options.
    options: Arc<Options>,
    /// Compaction pointers by level.
    compact_pointers: RwLock<[Option<Vec<u8>>; MAX_LEVELS]>,
}

impl VersionSet {
    /// Create a new VersionSet.
    pub fn new(db_path: &Path, options: Arc<Options>) -> Self {
        Self {
            db_path: db_path.to_path_buf(),
            current: ArcSwap::from_pointee(Version::new()),
            next_file_number: AtomicU64::new(2), // 1 is reserved for manifest
            last_sequence: AtomicU64::new(0),
            log_number: AtomicU64::new(0),
            prev_log_number: AtomicU64::new(0),
            manifest: RwLock::new(None),
            manifest_number: AtomicU64::new(1),
            comparator_name: "leveldb.BytewiseComparator".to_string(),
            options,
            compact_pointers: RwLock::new(Default::default()),
        }
    }

    /// Get the database path.
    pub fn db_path(&self) -> &Path {
        &self.db_path
    }

    /// Get the current version.
    pub fn current(&self) -> Arc<Version> {
        self.current.load().clone()
    }

    /// Get next file number and increment.
    pub fn new_file_number(&self) -> u64 {
        self.next_file_number.fetch_add(1, Ordering::SeqCst)
    }

    /// Get current next file number without incrementing.
    pub fn next_file_number(&self) -> u64 {
        self.next_file_number.load(Ordering::SeqCst)
    }

    /// Set next file number (used during recovery).
    fn set_next_file_number(&self, num: u64) {
        self.next_file_number.store(num, Ordering::SeqCst);
    }

    /// Get the last sequence number.
    pub fn last_sequence(&self) -> u64 {
        self.last_sequence.load(Ordering::SeqCst)
    }

    /// Set the last sequence number.
    pub fn set_last_sequence(&self, seq: u64) {
        self.last_sequence.store(seq, Ordering::SeqCst);
    }

    /// Get the current log number.
    pub fn log_number(&self) -> u64 {
        self.log_number.load(Ordering::SeqCst)
    }

    /// Set the log number.
    pub fn set_log_number(&self, num: u64) {
        self.log_number.store(num, Ordering::SeqCst);
    }

    /// Get the previous log number.
    pub fn prev_log_number(&self) -> u64 {
        self.prev_log_number.load(Ordering::SeqCst)
    }

    /// Set the previous log number.
    fn set_prev_log_number(&self, num: u64) {
        self.prev_log_number.store(num, Ordering::SeqCst);
    }

    /// Get the manifest number.
    pub fn manifest_number(&self) -> u64 {
        self.manifest_number.load(Ordering::SeqCst)
    }

    /// Get the comparator name.
    pub fn comparator_name(&self) -> &str {
        &self.comparator_name
    }

    /// Recover the VersionSet from disk.
    ///
    /// This reads the CURRENT file to find the active manifest,
    /// then replays all VersionEdits to reconstruct the current Version.
    pub fn recover(&self) -> Result<bool> {
        // Read CURRENT file to find manifest
        let current_path = self.db_path.join("CURRENT");

        if !current_path.exists() {
            // Fresh database, nothing to recover
            return Ok(false);
        }

        let manifest_name = read_current_file(&current_path)?;
        let manifest_number = parse_manifest_filename(&manifest_name)
            .ok_or_else(|| Error::corruption("invalid manifest name in CURRENT"))?;

        // Read and replay all edits from manifest
        let manifest_path = manifest_file_path(&self.db_path, manifest_number);
        let mut reader = ManifestReader::new(&manifest_path, manifest_number)?;

        let mut log_number = 0u64;
        let mut prev_log_number = 0u64;
        let mut next_file_number = 0u64;
        let mut last_sequence = 0u64;
        let mut has_log_number = false;
        let mut has_next_file = false;
        let mut has_last_sequence = false;

        // Build the version from edits
        let mut builder = VersionBuilder::new();

        while let Some(edit) = reader.read_edit()? {
            // Apply edit to builder
            builder.apply(&edit);

            // Track metadata
            if let Some(num) = edit.log_number {
                log_number = num;
                has_log_number = true;
            }
            if let Some(num) = edit.prev_log_number {
                prev_log_number = num;
            }
            if let Some(num) = edit.next_file_number {
                next_file_number = num;
                has_next_file = true;
            }
            if let Some(seq) = edit.last_sequence {
                last_sequence = seq;
                has_last_sequence = true;
            }

            // Update compact pointers
            for (level, key) in edit.compact_pointers {
                let mut pointers = self.compact_pointers.write();
                pointers[level] = Some(key.user_key().to_vec());
            }
        }

        if !has_next_file {
            return Err(Error::corruption("no next_file_number in manifest"));
        }
        if !has_log_number {
            return Err(Error::corruption("no log_number in manifest"));
        }
        if !has_last_sequence {
            return Err(Error::corruption("no last_sequence in manifest"));
        }

        // Mark file number as used
        self.mark_file_number_used(log_number);
        self.mark_file_number_used(prev_log_number);

        // Apply recovered state
        self.set_next_file_number(next_file_number);
        self.set_last_sequence(last_sequence);
        self.set_log_number(log_number);
        self.set_prev_log_number(prev_log_number);
        self.manifest_number.store(manifest_number, Ordering::SeqCst);

        // Build and install the new version
        let new_version = builder.build(&self.options);
        self.current.store(Arc::new(new_version));

        // Reopen manifest for appending
        let manifest =
            Manifest::open(&self.db_path, manifest_number, self.options.sync_mode)?;
        *self.manifest.write() = Some(manifest);

        Ok(true)
    }

    /// Create a new manifest file and write initial state.
    pub fn create_new_manifest(&self, first_edit: &VersionEdit) -> Result<()> {
        let manifest_number = self.new_file_number();
        let mut manifest =
            Manifest::create(&self.db_path, manifest_number, self.options.sync_mode)?;

        // Write the edit
        manifest.log_edit(first_edit)?;
        manifest.sync()?;

        // Update CURRENT file
        self.set_current_file(manifest_number)?;

        // Store manifest
        self.manifest_number.store(manifest_number, Ordering::SeqCst);
        *self.manifest.write() = Some(manifest);

        Ok(())
    }

    /// Log a VersionEdit and apply it to create a new Version.
    pub fn log_and_apply(&self, edit: &mut VersionEdit) -> Result<()> {
        // Fill in edit metadata if not set
        if edit.log_number.is_none() {
            edit.set_log_number(self.log_number());
        }
        if edit.prev_log_number.is_none() {
            edit.set_prev_log_number(self.prev_log_number());
        }
        if edit.next_file_number.is_none() {
            edit.set_next_file_number(self.next_file_number());
        }
        if edit.last_sequence.is_none() {
            edit.set_last_sequence(self.last_sequence());
        }

        // Get current version
        let current = self.current();

        // Build new version from edit
        let mut builder = VersionBuilder::from_version(&current);
        builder.apply(edit);
        let new_version = builder.build(&self.options);

        // Log to manifest
        {
            let mut manifest_guard = self.manifest.write();
            let manifest = manifest_guard
                .as_mut()
                .ok_or_else(|| Error::internal("no active manifest"))?;
            manifest.log_edit(edit)?;
            manifest.sync()?;
        }

        // Install new version
        self.current.store(Arc::new(new_version));

        // Update metadata from edit
        if let Some(num) = edit.log_number {
            self.set_log_number(num);
        }
        if let Some(num) = edit.prev_log_number {
            self.set_prev_log_number(num);
        }

        // Update compact pointers
        for (level, key) in &edit.compact_pointers {
            let mut pointers = self.compact_pointers.write();
            pointers[*level] = Some(key.user_key().to_vec());
        }

        Ok(())
    }

    /// Mark a file number as used.
    pub fn mark_file_number_used(&self, num: u64) {
        if num >= self.next_file_number() {
            self.set_next_file_number(num + 1);
        }
    }

    /// Set the CURRENT file to point to the given manifest.
    fn set_current_file(&self, manifest_number: u64) -> Result<()> {
        let manifest_name = format!("MANIFEST-{:06}", manifest_number);
        let current_path = self.db_path.join("CURRENT");
        let temp_path = self.db_path.join("CURRENT.tmp");

        // Write to temp file first
        {
            let mut file = File::create(&temp_path)?;
            writeln!(file, "{}", manifest_name)?;
            file.sync_all()?;
        }

        // Atomic rename
        fs::rename(&temp_path, &current_path)?;

        Ok(())
    }

    /// Get the number of files at a level.
    pub fn num_files_at_level(&self, level: usize) -> usize {
        self.current().num_files(level)
    }

    /// Get total file count across all levels.
    pub fn total_files(&self) -> usize {
        self.current().total_files()
    }

    /// Get the compaction score of the current version.
    pub fn compaction_score(&self) -> f64 {
        self.current().compaction_score()
    }

    /// Check if compaction is needed.
    pub fn needs_compaction(&self) -> bool {
        self.current().needs_compaction()
    }

    /// Get files that overlap with a key range at a given level.
    pub fn get_overlapping_files(
        &self,
        level: usize,
        smallest: &[u8],
        largest: &[u8],
    ) -> Vec<Arc<FileMetadata>> {
        self.current().get_overlapping_files(level, smallest, largest)
    }

    /// Get compact pointer for a level.
    pub fn compact_pointer(&self, level: usize) -> Option<Vec<u8>> {
        self.compact_pointers.read()[level].clone()
    }

    /// Set compact pointer for a level.
    pub fn set_compact_pointer(&self, level: usize, key: Vec<u8>) {
        self.compact_pointers.write()[level] = Some(key);
    }

    /// Get a summary of the version set.
    pub fn summary(&self) -> String {
        let current = self.current();
        let mut summary = String::new();
        for level in 0..MAX_LEVELS {
            let num_files = current.num_files(level);
            if num_files > 0 {
                let size = current.level_size(level);
                summary.push_str(&format!(
                    "L{}: {} files ({:.2} MB)\n",
                    level,
                    num_files,
                    size as f64 / (1024.0 * 1024.0)
                ));
            }
        }
        summary
    }
}

/// Read the CURRENT file to get the active manifest name.
fn read_current_file(path: &Path) -> Result<String> {
    let mut file = File::open(path)?;
    let mut content = String::new();
    file.read_to_string(&mut content)?;
    Ok(content.trim().to_string())
}

/// Builder for creating a new Version from edits.
struct VersionBuilder {
    /// Files at each level.
    files: [Vec<Arc<FileMetadata>>; MAX_LEVELS],
    /// Deleted files (level, file_number).
    deleted_files: HashSet<(usize, u64)>,
}

impl VersionBuilder {
    /// Create a new empty builder.
    fn new() -> Self {
        Self {
            files: Default::default(),
            deleted_files: HashSet::new(),
        }
    }

    /// Create a builder from an existing version.
    fn from_version(version: &Version) -> Self {
        let mut files: [Vec<Arc<FileMetadata>>; MAX_LEVELS] = Default::default();
        for level in 0..MAX_LEVELS {
            files[level] = version.files(level).to_vec();
        }
        Self {
            files,
            deleted_files: HashSet::new(),
        }
    }

    /// Apply a VersionEdit to this builder.
    fn apply(&mut self, edit: &VersionEdit) {
        // Process deleted files
        for &(level, file_number) in &edit.deleted_files {
            self.deleted_files.insert((level, file_number));
        }

        // Process new files
        for (level, file) in &edit.new_files {
            // Remove from deleted set if present
            self.deleted_files.remove(&(*level, file.file_number()));
            // Add to files
            self.files[*level].push(Arc::new(file.clone()));
        }
    }

    /// Build the final Version.
    fn build(mut self, options: &Options) -> Version {
        // Remove deleted files
        for level in 0..MAX_LEVELS {
            self.files[level].retain(|f| !self.deleted_files.contains(&(level, f.file_number())));

            // Sort files
            if level == 0 {
                // L0: Sort by file number (newest first)
                self.files[level].sort_by(|a, b| b.file_number().cmp(&a.file_number()));
            } else {
                // Higher levels: Sort by smallest key
                self.files[level].sort_by(|a, b| {
                    a.smallest().user_key().cmp(b.smallest().user_key())
                });
            }
        }

        // Create version
        let mut version = Version::with_files(self.files);

        // Calculate compaction score
        let (score, level) = compute_compaction_score(options, &version);
        version.set_compaction_info(score, level);

        version
    }

}

/// Compute the compaction score for a version.
fn compute_compaction_score(options: &Options, version: &Version) -> (f64, usize) {
    let mut best_score = 0.0;
    let mut best_level = 0;

    // L0 compaction triggered by file count
    let l0_score = version.num_files(0) as f64 / options.l0_compaction_trigger as f64;
    if l0_score > best_score {
        best_score = l0_score;
        best_level = 0;
    }

    // Other levels: compaction triggered by size
    for level in 1..MAX_LEVELS - 1 {
        let level_size = version.level_size(level);
        let max_size = options.max_bytes_for_level(level) as u64;
        if max_size > 0 {
            let score = level_size as f64 / max_size as f64;
            if score > best_score {
                best_score = score;
                best_level = level;
            }
        }
    }

    (best_score, best_level)
}

/// Write CURRENT file for a fresh database.
pub fn write_current_file(db_path: &Path, manifest_number: u64) -> Result<()> {
    let manifest_name = format!("MANIFEST-{:06}", manifest_number);
    let current_path = db_path.join("CURRENT");
    let temp_path = db_path.join("CURRENT.tmp");

    // Write to temp file first
    {
        let mut file = File::create(&temp_path)?;
        writeln!(file, "{}", manifest_name)?;
        file.sync_all()?;
    }

    // Atomic rename
    fs::rename(&temp_path, &current_path)?;

    Ok(())
}

/// Read the current manifest name from CURRENT file.
pub fn read_current_manifest(db_path: &Path) -> Result<Option<String>> {
    let current_path = db_path.join("CURRENT");
    if !current_path.exists() {
        return Ok(None);
    }
    let name = read_current_file(&current_path)?;
    Ok(Some(name))
}

/// Check if a database exists at the given path.
pub fn database_exists(db_path: &Path) -> bool {
    db_path.join("CURRENT").exists()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{InternalKey, ValueType};
    use bytes::Bytes;
    use tempfile::tempdir;

    fn make_key(user_key: &[u8], seq: u64) -> InternalKey {
        InternalKey::new(Bytes::copy_from_slice(user_key), seq, ValueType::Value)
    }

    #[test]
    fn test_version_set_new() {
        let dir = tempdir().unwrap();
        let options = Arc::new(Options::default());
        let vs = VersionSet::new(dir.path(), options);

        assert_eq!(vs.next_file_number(), 2);
        assert_eq!(vs.last_sequence(), 0);
        assert_eq!(vs.log_number(), 0);
        assert_eq!(vs.total_files(), 0);
    }

    #[test]
    fn test_version_set_file_numbers() {
        let dir = tempdir().unwrap();
        let options = Arc::new(Options::default());
        let vs = VersionSet::new(dir.path(), options);

        let n1 = vs.new_file_number();
        let n2 = vs.new_file_number();
        let n3 = vs.new_file_number();

        assert_eq!(n1, 2);
        assert_eq!(n2, 3);
        assert_eq!(n3, 4);
        assert_eq!(vs.next_file_number(), 5);
    }

    #[test]
    fn test_version_set_sequences() {
        let dir = tempdir().unwrap();
        let options = Arc::new(Options::default());
        let vs = VersionSet::new(dir.path(), options);

        vs.set_last_sequence(100);
        assert_eq!(vs.last_sequence(), 100);

        vs.set_last_sequence(200);
        assert_eq!(vs.last_sequence(), 200);
    }

    #[test]
    fn test_version_builder_empty() {
        let options = Options::default();
        let builder = VersionBuilder::new();
        let version = builder.build(&options);

        assert_eq!(version.total_files(), 0);
    }

    #[test]
    fn test_version_builder_add_files() {
        let options = Options::default();
        let mut builder = VersionBuilder::new();

        let mut edit = VersionEdit::new();
        edit.add_file_info(0, 1, 1024, make_key(b"a", 1), make_key(b"z", 100));
        edit.add_file_info(0, 2, 2048, make_key(b"b", 1), make_key(b"y", 100));
        edit.add_file_info(1, 3, 4096, make_key(b"c", 1), make_key(b"x", 100));

        builder.apply(&edit);
        let version = builder.build(&options);

        assert_eq!(version.num_files(0), 2);
        assert_eq!(version.num_files(1), 1);
        assert_eq!(version.total_files(), 3);
    }

    #[test]
    fn test_version_builder_delete_files() {
        let options = Options::default();
        let mut builder = VersionBuilder::new();

        // Add files
        let mut edit1 = VersionEdit::new();
        edit1.add_file_info(0, 1, 1024, make_key(b"a", 1), make_key(b"z", 100));
        edit1.add_file_info(0, 2, 2048, make_key(b"b", 1), make_key(b"y", 100));
        builder.apply(&edit1);

        // Delete a file
        let mut edit2 = VersionEdit::new();
        edit2.delete_file(0, 1);
        builder.apply(&edit2);

        let version = builder.build(&options);
        assert_eq!(version.num_files(0), 1);
        assert_eq!(version.files(0)[0].file_number(), 2);
    }

    #[test]
    fn test_version_builder_from_version() {
        let options = Options::default();

        // Create initial version
        let mut builder1 = VersionBuilder::new();
        let mut edit = VersionEdit::new();
        edit.add_file_info(0, 1, 1024, make_key(b"a", 1), make_key(b"z", 100));
        builder1.apply(&edit);
        let version1 = builder1.build(&options);

        // Create builder from version and add more files
        let mut builder2 = VersionBuilder::from_version(&version1);
        let mut edit2 = VersionEdit::new();
        edit2.add_file_info(0, 2, 2048, make_key(b"b", 1), make_key(b"y", 100));
        builder2.apply(&edit2);
        let version2 = builder2.build(&options);

        assert_eq!(version1.num_files(0), 1);
        assert_eq!(version2.num_files(0), 2);
    }

    #[test]
    fn test_write_and_read_current() {
        let dir = tempdir().unwrap();

        write_current_file(dir.path(), 1).unwrap();
        let name = read_current_manifest(dir.path()).unwrap();
        assert_eq!(name, Some("MANIFEST-000001".to_string()));

        write_current_file(dir.path(), 42).unwrap();
        let name = read_current_manifest(dir.path()).unwrap();
        assert_eq!(name, Some("MANIFEST-000042".to_string()));
    }

    #[test]
    fn test_database_exists() {
        let dir = tempdir().unwrap();

        assert!(!database_exists(dir.path()));

        write_current_file(dir.path(), 1).unwrap();
        assert!(database_exists(dir.path()));
    }

    #[test]
    fn test_version_set_create_manifest() {
        let dir = tempdir().unwrap();
        let options = Arc::new(Options::default());
        let vs = VersionSet::new(dir.path(), options);

        let mut edit = VersionEdit::new();
        edit.set_comparator("bytewise");
        edit.set_log_number(1);
        edit.set_next_file_number(10);
        edit.set_last_sequence(0);

        vs.create_new_manifest(&edit).unwrap();

        // Verify CURRENT file was created
        assert!(database_exists(dir.path()));

        // Verify manifest file was created
        let manifest_path = manifest_file_path(dir.path(), vs.manifest_number());
        assert!(manifest_path.exists());
    }

    #[test]
    fn test_version_set_log_and_apply() {
        let dir = tempdir().unwrap();
        let options = Arc::new(Options::default());
        let vs = VersionSet::new(dir.path(), options);

        // Create initial manifest
        let mut init_edit = VersionEdit::new();
        init_edit.set_comparator("bytewise");
        init_edit.set_log_number(1);
        init_edit.set_next_file_number(10);
        init_edit.set_last_sequence(0);
        vs.create_new_manifest(&init_edit).unwrap();

        // Apply an edit that adds a file
        let mut edit = VersionEdit::new();
        edit.add_file_info(0, 10, 1024, make_key(b"a", 1), make_key(b"z", 100));
        vs.log_and_apply(&mut edit).unwrap();

        assert_eq!(vs.num_files_at_level(0), 1);
        assert_eq!(vs.total_files(), 1);
    }

    #[test]
    fn test_version_set_recover() {
        let dir = tempdir().unwrap();
        let options = Arc::new(Options::default());

        // Create and populate version set
        {
            let vs = VersionSet::new(dir.path(), Arc::clone(&options));

            let mut init_edit = VersionEdit::new();
            init_edit.set_comparator("bytewise");
            init_edit.set_log_number(1);
            init_edit.set_next_file_number(10);
            init_edit.set_last_sequence(100);
            vs.create_new_manifest(&init_edit).unwrap();

            let mut edit = VersionEdit::new();
            edit.add_file_info(0, 10, 1024, make_key(b"a", 1), make_key(b"z", 50));
            edit.add_file_info(0, 11, 2048, make_key(b"b", 51), make_key(b"y", 100));
            vs.log_and_apply(&mut edit).unwrap();

            vs.set_last_sequence(200);
            let mut edit2 = VersionEdit::new();
            edit2.add_file_info(1, 12, 4096, make_key(b"c", 101), make_key(b"x", 200));
            vs.log_and_apply(&mut edit2).unwrap();
        }

        // Recover version set
        let vs2 = VersionSet::new(dir.path(), options);
        let recovered = vs2.recover().unwrap();

        assert!(recovered);
        assert_eq!(vs2.num_files_at_level(0), 2);
        assert_eq!(vs2.num_files_at_level(1), 1);
        assert_eq!(vs2.total_files(), 3);
        assert!(vs2.last_sequence() >= 200);
    }

    #[test]
    fn test_compaction_score() {
        let dir = tempdir().unwrap();
        let mut opts = Options::default();
        opts.l0_compaction_trigger = 4;
        let options = Arc::new(opts);
        let vs = VersionSet::new(dir.path(), options);

        // Create initial manifest
        let mut init_edit = VersionEdit::new();
        init_edit.set_comparator("bytewise");
        init_edit.set_log_number(1);
        init_edit.set_next_file_number(10);
        init_edit.set_last_sequence(0);
        vs.create_new_manifest(&init_edit).unwrap();

        // Add files to L0 until compaction is needed
        for i in 0..5 {
            let mut edit = VersionEdit::new();
            edit.add_file_info(
                0,
                10 + i,
                1024,
                make_key(format!("a{}", i).as_bytes(), 1),
                make_key(format!("z{}", i).as_bytes(), 100),
            );
            vs.log_and_apply(&mut edit).unwrap();
        }

        assert!(vs.needs_compaction());
        assert!(vs.compaction_score() > 1.0);
    }

    #[test]
    fn test_version_set_summary() {
        let dir = tempdir().unwrap();
        let options = Arc::new(Options::default());
        let vs = VersionSet::new(dir.path(), options);

        let mut init_edit = VersionEdit::new();
        init_edit.set_comparator("bytewise");
        init_edit.set_log_number(1);
        init_edit.set_next_file_number(10);
        init_edit.set_last_sequence(0);
        vs.create_new_manifest(&init_edit).unwrap();

        let mut edit = VersionEdit::new();
        edit.add_file_info(0, 10, 1024 * 1024, make_key(b"a", 1), make_key(b"z", 100));
        vs.log_and_apply(&mut edit).unwrap();

        let summary = vs.summary();
        assert!(summary.contains("L0: 1 files"));
    }
}
