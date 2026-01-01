//! Compaction - Background merging of SSTables.
//!
//! The compaction module is responsible for:
//! - Picking files to compact based on size/count thresholds
//! - Merging multiple sorted iterators into one
//! - Writing compacted data to new SSTable(s)
//! - Managing background compaction threads
//!
//! # LSM-Tree Compaction Strategy
//!
//! - **Level 0**: Contains files flushed from MemTable. Files may overlap.
//!   Compaction triggered when file count exceeds threshold.
//! - **Levels 1-6**: Files within a level don't overlap. Each level has a
//!   size target (exponentially increasing). Compaction triggered when
//!   level size exceeds target.
//!
//! # Compaction Process
//!
//! 1. Pick files to compact (from level N to level N+1)
//! 2. Create merge iterator over all input files
//! 3. For each unique key, keep newest version, drop older versions
//! 4. Write output to new SSTable file(s)
//! 5. Update manifest atomically
//! 6. Delete old input files

pub mod background;
pub mod compactor;
pub mod merge_iterator;
pub mod picker;

pub use background::{BackgroundCompaction, CompactionRequest, CompactionState};
pub use compactor::{Compactor, CompactionStats};
pub use merge_iterator::MergeIterator;
pub use picker::{Compaction, CompactionPicker, CompactionType};

use std::sync::Arc;

use crate::options::Options;
use crate::version::FileMetadata;

/// Input for a compaction operation.
#[derive(Debug, Clone)]
pub struct CompactionInput {
    /// Level being compacted from.
    pub level: usize,
    /// Files from the source level.
    pub level_files: Vec<Arc<FileMetadata>>,
    /// Files from the target level (level + 1) that overlap.
    pub level_plus_one_files: Vec<Arc<FileMetadata>>,
}

impl CompactionInput {
    /// Create a new compaction input.
    pub fn new(
        level: usize,
        level_files: Vec<Arc<FileMetadata>>,
        level_plus_one_files: Vec<Arc<FileMetadata>>,
    ) -> Self {
        Self {
            level,
            level_files,
            level_plus_one_files,
        }
    }

    /// Get all input files.
    pub fn all_files(&self) -> Vec<Arc<FileMetadata>> {
        let mut files = self.level_files.clone();
        files.extend(self.level_plus_one_files.iter().cloned());
        files
    }

    /// Get the target level for output files.
    pub fn output_level(&self) -> usize {
        self.level + 1
    }

    /// Get total input size in bytes.
    pub fn total_input_size(&self) -> u64 {
        self.level_files.iter().map(|f| f.file_size()).sum::<u64>()
            + self.level_plus_one_files.iter().map(|f| f.file_size()).sum::<u64>()
    }

    /// Check if this is an L0 compaction.
    pub fn is_l0_compaction(&self) -> bool {
        self.level == 0
    }
}

/// Output from a compaction operation.
#[derive(Debug, Clone)]
pub struct CompactionOutput {
    /// Level where output files were written.
    pub level: usize,
    /// Files that were created.
    pub output_files: Vec<Arc<FileMetadata>>,
    /// Files that should be deleted (input files).
    pub files_to_delete: Vec<Arc<FileMetadata>>,
}

impl CompactionOutput {
    /// Create a new compaction output.
    pub fn new(
        level: usize,
        output_files: Vec<Arc<FileMetadata>>,
        files_to_delete: Vec<Arc<FileMetadata>>,
    ) -> Self {
        Self {
            level,
            output_files,
            files_to_delete,
        }
    }

    /// Get total output size in bytes.
    pub fn total_output_size(&self) -> u64 {
        self.output_files.iter().map(|f| f.file_size()).sum()
    }
}

/// Compaction filter decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FilterDecision {
    /// Keep the key-value pair.
    Keep,
    /// Remove the key-value pair.
    Remove,
}

/// Trait for custom compaction filters.
///
/// Compaction filters allow users to modify or remove key-value pairs
/// during compaction.
pub trait CompactionFilter: Send + Sync {
    /// Filter a key-value pair during compaction.
    ///
    /// # Arguments
    /// * `level` - The compaction level
    /// * `key` - The key being compacted
    /// * `value` - The value being compacted
    /// * `sequence` - The sequence number
    ///
    /// # Returns
    /// Decision on whether to keep or remove the entry.
    fn filter(
        &self,
        level: usize,
        key: &[u8],
        value: &[u8],
        sequence: u64,
    ) -> FilterDecision;

    /// Name of the filter (for logging).
    fn name(&self) -> &str {
        "CompactionFilter"
    }
}

/// A no-op compaction filter that keeps all entries.
pub struct NoopCompactionFilter;

impl CompactionFilter for NoopCompactionFilter {
    fn filter(
        &self,
        _level: usize,
        _key: &[u8],
        _value: &[u8],
        _sequence: u64,
    ) -> FilterDecision {
        FilterDecision::Keep
    }

    fn name(&self) -> &str {
        "NoopCompactionFilter"
    }
}

/// TTL-based compaction filter.
///
/// Removes entries older than a specified time-to-live.
pub struct TtlCompactionFilter {
    /// TTL in sequence number units.
    /// Entries with sequence < current_sequence - ttl are removed.
    pub ttl_sequences: u64,
    /// Current sequence number.
    pub current_sequence: u64,
}

impl TtlCompactionFilter {
    /// Create a new TTL filter.
    pub fn new(ttl_sequences: u64, current_sequence: u64) -> Self {
        Self {
            ttl_sequences,
            current_sequence,
        }
    }
}

impl CompactionFilter for TtlCompactionFilter {
    fn filter(
        &self,
        _level: usize,
        _key: &[u8],
        _value: &[u8],
        sequence: u64,
    ) -> FilterDecision {
        if self.current_sequence > self.ttl_sequences
            && sequence < self.current_sequence - self.ttl_sequences
        {
            FilterDecision::Remove
        } else {
            FilterDecision::Keep
        }
    }

    fn name(&self) -> &str {
        "TtlCompactionFilter"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use crate::types::InternalKey;

    #[test]
    fn test_compaction_input() {
        let file1 = Arc::new(FileMetadata::new(
            1,
            1000,
            InternalKey::for_value(Bytes::from("a"), 1),
            InternalKey::for_value(Bytes::from("m"), 1),
        ));
        let file2 = Arc::new(FileMetadata::new(
            2,
            2000,
            InternalKey::for_value(Bytes::from("n"), 2),
            InternalKey::for_value(Bytes::from("z"), 2),
        ));

        let input = CompactionInput::new(0, vec![file1.clone()], vec![file2.clone()]);

        assert_eq!(input.level, 0);
        assert_eq!(input.output_level(), 1);
        assert_eq!(input.total_input_size(), 3000);
        assert!(input.is_l0_compaction());
        assert_eq!(input.all_files().len(), 2);
    }

    #[test]
    fn test_compaction_output() {
        let file = Arc::new(FileMetadata::new(
            3,
            5000,
            InternalKey::for_value(Bytes::from("a"), 1),
            InternalKey::for_value(Bytes::from("z"), 10),
        ));

        let output = CompactionOutput::new(1, vec![file], vec![]);

        assert_eq!(output.level, 1);
        assert_eq!(output.total_output_size(), 5000);
    }

    #[test]
    fn test_noop_filter() {
        let filter = NoopCompactionFilter;
        assert_eq!(
            filter.filter(0, b"key", b"value", 1),
            FilterDecision::Keep
        );
    }

    #[test]
    fn test_ttl_filter() {
        let filter = TtlCompactionFilter::new(100, 200);

        // Sequence 50 is older than TTL (200 - 100 = 100), should be removed
        assert_eq!(
            filter.filter(0, b"key", b"value", 50),
            FilterDecision::Remove
        );

        // Sequence 150 is within TTL, should be kept
        assert_eq!(
            filter.filter(0, b"key", b"value", 150),
            FilterDecision::Keep
        );
    }
}
