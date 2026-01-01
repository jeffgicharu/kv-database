//! Compaction picker - decides when and what to compact.
//!
//! The picker analyzes the current version state and determines:
//! - Whether compaction is needed
//! - Which level to compact
//! - Which files to include in the compaction

use std::sync::Arc;

use bytes::Bytes;

use crate::options::{Options, MAX_LEVELS};
use crate::types::InternalKey;
use crate::version::{FileMetadata, Version};

use super::CompactionInput;

/// Type of compaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompactionType {
    /// L0 compaction triggered by file count.
    Level0,
    /// Level compaction triggered by size.
    LevelN(usize),
    /// Manual compaction requested by user.
    Manual,
    /// Trivial move (no merge needed, just move file to next level).
    TrivialMove,
}

/// A compaction to be executed.
#[derive(Debug, Clone)]
pub struct Compaction {
    /// Type of compaction.
    pub compaction_type: CompactionType,
    /// Input for the compaction.
    pub input: CompactionInput,
    /// Smallest user key in the compaction.
    pub smallest_user_key: Option<Bytes>,
    /// Largest user key in the compaction.
    pub largest_user_key: Option<Bytes>,
    /// Edit to apply after compaction (files to add/remove).
    pub grandparent_overlap_bytes: u64,
}

impl Compaction {
    /// Create a new compaction.
    pub fn new(compaction_type: CompactionType, input: CompactionInput) -> Self {
        // Compute key range
        let (smallest, largest) = Self::compute_key_range(&input);

        Self {
            compaction_type,
            input,
            smallest_user_key: smallest,
            largest_user_key: largest,
            grandparent_overlap_bytes: 0,
        }
    }

    /// Compute the key range covered by all input files.
    fn compute_key_range(input: &CompactionInput) -> (Option<Bytes>, Option<Bytes>) {
        let all_files = input.all_files();
        if all_files.is_empty() {
            return (None, None);
        }

        let mut smallest: Option<&[u8]> = None;
        let mut largest: Option<&[u8]> = None;

        for file in &all_files {
            let file_smallest = file.smallest().user_key();
            let file_largest = file.largest().user_key();

            smallest = match smallest {
                None => Some(file_smallest),
                Some(s) if file_smallest < s => Some(file_smallest),
                s => s,
            };

            largest = match largest {
                None => Some(file_largest),
                Some(l) if file_largest > l => Some(file_largest),
                l => l,
            };
        }

        (
            smallest.map(|s| Bytes::copy_from_slice(s)),
            largest.map(|l| Bytes::copy_from_slice(l)),
        )
    }

    /// Check if this is a trivial move.
    ///
    /// A trivial move is when we can just move a file to the next level
    /// without actually merging it.
    pub fn is_trivial_move(&self) -> bool {
        matches!(self.compaction_type, CompactionType::TrivialMove)
    }

    /// Get the source level.
    pub fn level(&self) -> usize {
        self.input.level
    }

    /// Get the target level.
    pub fn output_level(&self) -> usize {
        self.input.output_level()
    }

    /// Get the number of input files.
    pub fn num_input_files(&self) -> usize {
        self.input.level_files.len() + self.input.level_plus_one_files.len()
    }

    /// Set grandparent overlap.
    pub fn set_grandparent_overlap(&mut self, bytes: u64) {
        self.grandparent_overlap_bytes = bytes;
    }
}

/// Compaction picker that decides what to compact.
pub struct CompactionPicker {
    /// Configuration options.
    options: Arc<Options>,
}

impl CompactionPicker {
    /// Create a new compaction picker.
    pub fn new(options: Arc<Options>) -> Self {
        Self { options }
    }

    /// Pick a compaction to run, if any.
    ///
    /// Returns None if no compaction is needed.
    pub fn pick_compaction(&self, version: &Version) -> Option<Compaction> {
        // Check if compaction is needed
        if !version.needs_compaction() {
            return None;
        }

        let level = version.compaction_level();

        if level == 0 {
            self.pick_l0_compaction(version)
        } else {
            self.pick_level_compaction(version, level)
        }
    }

    /// Pick an L0 compaction.
    fn pick_l0_compaction(&self, version: &Version) -> Option<Compaction> {
        let l0_files = version.files(0);
        if l0_files.is_empty() {
            return None;
        }

        // For L0 compaction, we need to include all overlapping L0 files
        // Start with all L0 files (since they can all overlap)
        let mut level_files: Vec<Arc<FileMetadata>> = l0_files
            .iter()
            .map(|f| f.clone())
            .collect();

        // Sort by newest first (highest file number = newest)
        level_files.sort_by(|a, b| b.file_number().cmp(&a.file_number()));

        // Find the key range covered by L0 files
        let (smallest, largest) = self.get_key_range(&level_files);

        if smallest.is_none() {
            return None;
        }

        // Find overlapping files in L1
        let level_plus_one_files = self.get_overlapping_files(
            version,
            1,
            smallest.as_ref().unwrap(),
            largest.as_ref().unwrap(),
        );

        // Check if this can be a trivial move
        if level_files.len() == 1 && level_plus_one_files.is_empty() {
            let input = CompactionInput::new(0, level_files, level_plus_one_files);
            return Some(Compaction::new(CompactionType::TrivialMove, input));
        }

        let input = CompactionInput::new(0, level_files, level_plus_one_files);
        Some(Compaction::new(CompactionType::Level0, input))
    }

    /// Pick a level N (N > 0) compaction.
    fn pick_level_compaction(&self, version: &Version, level: usize) -> Option<Compaction> {
        let files = version.files(level);
        if files.is_empty() {
            return None;
        }

        // Pick the file with the largest key range (or use compaction pointer)
        // For simplicity, we pick the first file that exceeds the level size
        // A more sophisticated approach would track a compaction pointer
        let picked_file = files.first()?;

        let level_files = vec![picked_file.clone()];

        // Find overlapping files in the next level
        let smallest = picked_file.smallest().user_key();
        let largest = picked_file.largest().user_key();
        let level_plus_one_files = self.get_overlapping_files(version, level + 1, smallest, largest);

        // Check for trivial move
        if level_plus_one_files.is_empty() {
            // Check grandparent overlap (level + 2)
            let grandparent_overlap = if level + 2 < MAX_LEVELS {
                self.get_overlapping_size(version, level + 2, smallest, largest)
            } else {
                0
            };

            // If grandparent overlap is small, this can be a trivial move
            if grandparent_overlap < self.options.target_file_size_base as u64 * 10 {
                let input = CompactionInput::new(level, level_files, level_plus_one_files);
                return Some(Compaction::new(CompactionType::TrivialMove, input));
            }
        }

        let input = CompactionInput::new(level, level_files, level_plus_one_files);
        Some(Compaction::new(CompactionType::LevelN(level), input))
    }

    /// Pick a manual compaction for a specific key range.
    pub fn pick_manual_compaction(
        &self,
        version: &Version,
        level: usize,
        begin: Option<&[u8]>,
        end: Option<&[u8]>,
    ) -> Option<Compaction> {
        let files = version.files(level);
        if files.is_empty() {
            return None;
        }

        // Find files that overlap with the range
        let level_files: Vec<Arc<FileMetadata>> = files
            .iter()
            .filter(|f| {
                let file_smallest = f.smallest().user_key();
                let file_largest = f.largest().user_key();

                let overlaps_begin = begin.map_or(true, |b| file_largest >= b);
                let overlaps_end = end.map_or(true, |e| file_smallest <= e);

                overlaps_begin && overlaps_end
            })
            .map(|f| f.clone())
            .collect();

        if level_files.is_empty() {
            return None;
        }

        // Get the full range of selected files
        let (smallest, largest) = self.get_key_range(&level_files);

        if smallest.is_none() {
            return None;
        }

        // Find overlapping files in the next level
        let level_plus_one_files = if level + 1 < MAX_LEVELS {
            self.get_overlapping_files(
                version,
                level + 1,
                smallest.as_ref().unwrap(),
                largest.as_ref().unwrap(),
            )
        } else {
            vec![]
        };

        let input = CompactionInput::new(level, level_files, level_plus_one_files);
        Some(Compaction::new(CompactionType::Manual, input))
    }

    /// Get the key range covered by a set of files.
    fn get_key_range(&self, files: &[Arc<FileMetadata>]) -> (Option<Bytes>, Option<Bytes>) {
        if files.is_empty() {
            return (None, None);
        }

        let mut smallest: Option<Bytes> = None;
        let mut largest: Option<Bytes> = None;

        for file in files {
            let file_smallest = file.smallest().user_key();
            let file_largest = file.largest().user_key();

            smallest = match &smallest {
                None => Some(Bytes::copy_from_slice(file_smallest)),
                Some(s) if file_smallest < s.as_ref() => {
                    Some(Bytes::copy_from_slice(file_smallest))
                }
                s => s.clone(),
            };

            largest = match &largest {
                None => Some(Bytes::copy_from_slice(file_largest)),
                Some(l) if file_largest > l.as_ref() => Some(Bytes::copy_from_slice(file_largest)),
                l => l.clone(),
            };
        }

        (smallest, largest)
    }

    /// Get files in a level that overlap with a key range.
    fn get_overlapping_files(
        &self,
        version: &Version,
        level: usize,
        smallest: &[u8],
        largest: &[u8],
    ) -> Vec<Arc<FileMetadata>> {
        version
            .files(level)
            .iter()
            .filter(|f| {
                let file_smallest = f.smallest().user_key();
                let file_largest = f.largest().user_key();

                // Check if ranges overlap
                file_smallest <= largest && file_largest >= smallest
            })
            .map(|f| f.clone())
            .collect()
    }

    /// Get the total size of overlapping files in a level.
    fn get_overlapping_size(
        &self,
        version: &Version,
        level: usize,
        smallest: &[u8],
        largest: &[u8],
    ) -> u64 {
        self.get_overlapping_files(version, level, smallest, largest)
            .iter()
            .map(|f| f.file_size())
            .sum()
    }

    /// Calculate the compaction score for L0.
    pub fn l0_compaction_score(&self, l0_file_count: usize) -> f64 {
        l0_file_count as f64 / self.options.l0_compaction_trigger as f64
    }

    /// Calculate the compaction score for a level.
    pub fn level_compaction_score(&self, level: usize, level_size: u64) -> f64 {
        let max_size = self.options.max_bytes_for_level(level) as u64;
        if max_size == 0 {
            0.0
        } else {
            level_size as f64 / max_size as f64
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::options::MAX_LEVELS;

    fn create_test_file(
        file_number: u64,
        size: u64,
        smallest: &str,
        largest: &str,
    ) -> Arc<FileMetadata> {
        Arc::new(FileMetadata::new(
            file_number,
            size,
            InternalKey::for_value(Bytes::copy_from_slice(smallest.as_bytes()), 1),
            InternalKey::for_value(Bytes::copy_from_slice(largest.as_bytes()), 1),
        ))
    }

    fn create_test_version_with_l0_files(count: usize) -> Version {
        let mut files: [Vec<Arc<FileMetadata>>; MAX_LEVELS] = Default::default();

        for i in 0..count {
            let file = create_test_file(
                i as u64,
                1000,
                &format!("key_{:04}", i * 10),
                &format!("key_{:04}", i * 10 + 9),
            );
            files[0].push(file);
        }

        let mut version = Version::with_files(files);
        // Set compaction info
        if count >= 4 {
            version.set_compaction_info(count as f64 / 4.0, 0);
        }
        version
    }

    #[test]
    fn test_picker_no_compaction_needed() {
        let options = Arc::new(Options::default());
        let picker = CompactionPicker::new(options);

        let version = Version::new();
        assert!(picker.pick_compaction(&version).is_none());
    }

    #[test]
    fn test_picker_l0_compaction() {
        let options = Arc::new(Options::default());
        let picker = CompactionPicker::new(options);

        // Create version with enough L0 files to trigger compaction
        let version = create_test_version_with_l0_files(5);

        let compaction = picker.pick_compaction(&version);
        assert!(compaction.is_some());

        let c = compaction.unwrap();
        assert_eq!(c.level(), 0);
        assert_eq!(c.output_level(), 1);
        assert!(matches!(
            c.compaction_type,
            CompactionType::Level0 | CompactionType::TrivialMove
        ));
    }

    #[test]
    fn test_picker_trivial_move() {
        let options = Arc::new(Options::default());
        let picker = CompactionPicker::new(options);

        // Single L0 file with no overlapping L1 files = trivial move
        let mut files: [Vec<Arc<FileMetadata>>; MAX_LEVELS] = Default::default();
        files[0].push(create_test_file(1, 1000, "aaa", "zzz"));

        let mut version = Version::with_files(files);
        version.set_compaction_info(1.1, 0);

        let compaction = picker.pick_compaction(&version);
        assert!(compaction.is_some());

        let c = compaction.unwrap();
        assert!(c.is_trivial_move());
    }

    #[test]
    fn test_picker_manual_compaction() {
        let options = Arc::new(Options::default());
        let picker = CompactionPicker::new(options);

        let version = create_test_version_with_l0_files(3);

        // Manual compaction of specific range
        let compaction = picker.pick_manual_compaction(
            &version,
            0,
            Some(b"key_0010"),
            Some(b"key_0020"),
        );

        assert!(compaction.is_some());
        let c = compaction.unwrap();
        assert!(matches!(c.compaction_type, CompactionType::Manual));
    }

    #[test]
    fn test_compaction_key_range() {
        let file1 = create_test_file(1, 1000, "aaa", "mmm");
        let file2 = create_test_file(2, 1000, "nnn", "zzz");

        let input = CompactionInput::new(0, vec![file1, file2], vec![]);
        let compaction = Compaction::new(CompactionType::Level0, input);

        assert_eq!(
            compaction.smallest_user_key.as_ref().map(|b| b.as_ref()),
            Some(b"aaa".as_slice())
        );
        assert_eq!(
            compaction.largest_user_key.as_ref().map(|b| b.as_ref()),
            Some(b"zzz".as_slice())
        );
    }

    #[test]
    fn test_l0_compaction_score() {
        let options = Arc::new(Options::default());
        let picker = CompactionPicker::new(options);

        assert!(picker.l0_compaction_score(2) < 1.0);
        assert!(picker.l0_compaction_score(4) >= 1.0);
        assert!(picker.l0_compaction_score(8) > 1.0);
    }
}
