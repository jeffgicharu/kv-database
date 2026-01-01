//! Version - immutable snapshot of active SSTable files.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use crate::options::MAX_LEVELS;

use super::FileMetadata;

/// An immutable snapshot of all SSTable files at a point in time.
///
/// Versions are reference-counted and can be held by readers to ensure
/// the files they're reading don't get deleted during compaction.
#[derive(Debug)]
pub struct Version {
    /// Files at each level (0 to MAX_LEVELS-1).
    /// Level 0 files may overlap; higher levels are sorted and non-overlapping.
    files: [Vec<Arc<FileMetadata>>; MAX_LEVELS],

    /// Reference count for readers.
    refs: AtomicUsize,

    /// Compaction score for this version.
    /// > 1.0 means compaction is needed.
    compaction_score: f64,

    /// Level that should be compacted next.
    compaction_level: usize,

    /// Total file size at each level.
    level_sizes: [u64; MAX_LEVELS],
}

impl Version {
    /// Create an empty version.
    pub fn new() -> Self {
        Self {
            files: Default::default(),
            refs: AtomicUsize::new(0),
            compaction_score: 0.0,
            compaction_level: 0,
            level_sizes: [0; MAX_LEVELS],
        }
    }

    /// Create a version with the given files.
    pub fn with_files(files: [Vec<Arc<FileMetadata>>; MAX_LEVELS]) -> Self {
        let mut level_sizes = [0u64; MAX_LEVELS];
        for (level, level_files) in files.iter().enumerate() {
            level_sizes[level] = level_files.iter().map(|f| f.file_size()).sum();
        }

        Self {
            files,
            refs: AtomicUsize::new(0),
            compaction_score: 0.0,
            compaction_level: 0,
            level_sizes,
        }
    }

    /// Get files at a specific level.
    pub fn files(&self, level: usize) -> &[Arc<FileMetadata>] {
        &self.files[level]
    }

    /// Get number of files at a level.
    pub fn num_files(&self, level: usize) -> usize {
        self.files[level].len()
    }

    /// Get total number of files across all levels.
    pub fn total_files(&self) -> usize {
        self.files.iter().map(|f| f.len()).sum()
    }

    /// Get total size at a level.
    pub fn level_size(&self, level: usize) -> u64 {
        self.level_sizes[level]
    }

    /// Get the compaction score.
    pub fn compaction_score(&self) -> f64 {
        self.compaction_score
    }

    /// Get the level that should be compacted next.
    pub fn compaction_level(&self) -> usize {
        self.compaction_level
    }

    /// Check if compaction is needed.
    pub fn needs_compaction(&self) -> bool {
        self.compaction_score > 1.0
    }

    /// Set compaction info.
    pub fn set_compaction_info(&mut self, score: f64, level: usize) {
        self.compaction_score = score;
        self.compaction_level = level;
    }

    /// Find files that overlap with a key range at a given level.
    pub fn get_overlapping_files(
        &self,
        level: usize,
        smallest: &[u8],
        largest: &[u8],
    ) -> Vec<Arc<FileMetadata>> {
        let mut result = Vec::new();

        if level == 0 {
            // Level 0 files may overlap, check all
            for file in &self.files[0] {
                if file.overlaps(smallest, largest) {
                    result.push(Arc::clone(file));
                }
            }
        } else {
            // Higher levels are sorted, use binary search
            let files = &self.files[level];
            if files.is_empty() {
                return result;
            }

            // Find first file that might overlap
            let start_idx = self.find_file(level, smallest);

            for file in &files[start_idx..] {
                if file.smallest().user_key() > largest {
                    break;
                }
                if file.overlaps(smallest, largest) {
                    result.push(Arc::clone(file));
                }
            }
        }

        result
    }

    /// Find files that might contain a specific key.
    pub fn get_files_for_key(&self, level: usize, user_key: &[u8]) -> Vec<Arc<FileMetadata>> {
        self.get_overlapping_files(level, user_key, user_key)
    }

    /// Find the index of the first file that might contain the key.
    ///
    /// For level > 0, uses binary search.
    fn find_file(&self, level: usize, user_key: &[u8]) -> usize {
        if level == 0 {
            return 0;
        }

        let files = &self.files[level];
        files.partition_point(|f| f.largest().user_key() < user_key)
    }

    /// Increment reference count.
    pub fn add_ref(&self) {
        self.refs.fetch_add(1, Ordering::Relaxed);
    }

    /// Decrement reference count.
    pub fn release_ref(&self) {
        self.refs.fetch_sub(1, Ordering::Relaxed);
    }

    /// Get current reference count.
    pub fn refs(&self) -> usize {
        self.refs.load(Ordering::Relaxed)
    }

    /// Get an iterator over all files at all levels.
    pub fn all_files(&self) -> impl Iterator<Item = (usize, &Arc<FileMetadata>)> {
        self.files
            .iter()
            .enumerate()
            .flat_map(|(level, files)| files.iter().map(move |f| (level, f)))
    }

    /// Calculate the overlap between Level 0 and Level 1 for a key range.
    pub fn level0_level1_overlap(&self, smallest: &[u8], largest: &[u8]) -> u64 {
        let l1_files = self.get_overlapping_files(1, smallest, largest);
        l1_files.iter().map(|f| f.file_size()).sum()
    }

    /// Pick files for level-0 compaction.
    ///
    /// Returns all Level-0 files that overlap with the given file,
    /// plus the expanded key range.
    pub fn pick_level0_files(
        &self,
        file: &FileMetadata,
    ) -> (Vec<Arc<FileMetadata>>, Option<(Vec<u8>, Vec<u8>)>) {
        let mut smallest = file.smallest().user_key().to_vec();
        let mut largest = file.largest().user_key().to_vec();
        let mut result = Vec::new();

        // Keep expanding until we've captured all overlapping L0 files
        loop {
            let overlapping = self.get_overlapping_files(0, &smallest, &largest);

            if overlapping.len() == result.len() {
                break;
            }

            result = overlapping;

            // Expand range
            for f in &result {
                if f.smallest().user_key() < smallest.as_slice() {
                    smallest = f.smallest().user_key().to_vec();
                }
                if f.largest().user_key() > largest.as_slice() {
                    largest = f.largest().user_key().to_vec();
                }
            }
        }

        if result.is_empty() {
            (result, None)
        } else {
            (result, Some((smallest, largest)))
        }
    }
}

impl Default for Version {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for Version {
    fn clone(&self) -> Self {
        Self {
            files: self.files.clone(),
            refs: AtomicUsize::new(0),
            compaction_score: self.compaction_score,
            compaction_level: self.compaction_level,
            level_sizes: self.level_sizes,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{InternalKey, ValueType};
    use bytes::Bytes;

    fn make_key(user_key: &[u8], seq: u64) -> InternalKey {
        InternalKey::new(Bytes::copy_from_slice(user_key), seq, ValueType::Value)
    }

    fn make_file(num: u64, smallest: &[u8], largest: &[u8]) -> Arc<FileMetadata> {
        Arc::new(FileMetadata::new(
            num,
            1024,
            make_key(smallest, 1),
            make_key(largest, 1),
        ))
    }

    #[test]
    fn test_version_empty() {
        let version = Version::new();
        assert_eq!(version.total_files(), 0);
        assert_eq!(version.num_files(0), 0);
        assert!(!version.needs_compaction());
    }

    #[test]
    fn test_version_with_files() {
        let mut files: [Vec<Arc<FileMetadata>>; MAX_LEVELS] = Default::default();

        files[0].push(make_file(1, b"a", b"c"));
        files[0].push(make_file(2, b"b", b"d")); // Overlapping in L0
        files[1].push(make_file(3, b"a", b"m"));
        files[1].push(make_file(4, b"n", b"z"));

        let version = Version::with_files(files);

        assert_eq!(version.num_files(0), 2);
        assert_eq!(version.num_files(1), 2);
        assert_eq!(version.total_files(), 4);
    }

    #[test]
    fn test_get_overlapping_files_level0() {
        let mut files: [Vec<Arc<FileMetadata>>; MAX_LEVELS] = Default::default();

        files[0].push(make_file(1, b"a", b"c"));
        files[0].push(make_file(2, b"b", b"d"));
        files[0].push(make_file(3, b"x", b"z"));

        let version = Version::with_files(files);

        // Query that overlaps with files 1 and 2
        let overlapping = version.get_overlapping_files(0, b"b", b"c");
        assert_eq!(overlapping.len(), 2);

        // Query that only overlaps with file 3
        let overlapping = version.get_overlapping_files(0, b"y", b"z");
        assert_eq!(overlapping.len(), 1);
        assert_eq!(overlapping[0].file_number(), 3);

        // Query with no overlap
        let overlapping = version.get_overlapping_files(0, b"e", b"w");
        assert_eq!(overlapping.len(), 0);
    }

    #[test]
    fn test_get_overlapping_files_higher_level() {
        let mut files: [Vec<Arc<FileMetadata>>; MAX_LEVELS] = Default::default();

        // Non-overlapping files in level 1
        files[1].push(make_file(1, b"a", b"c"));
        files[1].push(make_file(2, b"d", b"f"));
        files[1].push(make_file(3, b"g", b"i"));
        files[1].push(make_file(4, b"j", b"l"));

        let version = Version::with_files(files);

        // Query that overlaps with files 2 and 3
        let overlapping = version.get_overlapping_files(1, b"e", b"h");
        assert_eq!(overlapping.len(), 2);

        // Query at boundaries
        let overlapping = version.get_overlapping_files(1, b"a", b"a");
        assert_eq!(overlapping.len(), 1);
        assert_eq!(overlapping[0].file_number(), 1);
    }

    #[test]
    fn test_version_refs() {
        let version = Version::new();
        assert_eq!(version.refs(), 0);

        version.add_ref();
        assert_eq!(version.refs(), 1);

        version.add_ref();
        assert_eq!(version.refs(), 2);

        version.release_ref();
        assert_eq!(version.refs(), 1);
    }

    #[test]
    fn test_compaction_score() {
        let mut version = Version::new();

        assert!(!version.needs_compaction());

        version.set_compaction_info(1.5, 1);
        assert!(version.needs_compaction());
        assert_eq!(version.compaction_level(), 1);
        assert_eq!(version.compaction_score(), 1.5);
    }

    #[test]
    fn test_all_files_iterator() {
        let mut files: [Vec<Arc<FileMetadata>>; MAX_LEVELS] = Default::default();

        files[0].push(make_file(1, b"a", b"b"));
        files[1].push(make_file(2, b"c", b"d"));
        files[2].push(make_file(3, b"e", b"f"));

        let version = Version::with_files(files);

        let all: Vec<_> = version.all_files().collect();
        assert_eq!(all.len(), 3);
        assert_eq!(all[0].0, 0); // Level 0
        assert_eq!(all[1].0, 1); // Level 1
        assert_eq!(all[2].0, 2); // Level 2
    }
}
