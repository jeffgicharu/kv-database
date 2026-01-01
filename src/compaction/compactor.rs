//! Compactor - executes compaction operations.
//!
//! The compactor takes a compaction input and:
//! 1. Creates iterators over all input files
//! 2. Merges entries using MergeIterator
//! 3. Drops older versions and tombstones
//! 4. Writes output to new SSTable(s)
//! 5. Returns the compaction result

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;

use crate::options::Options;
use crate::sstable::{CompressionType, SSTableEntryIterator, SSTableReader, SSTableWriter};
use crate::types::InternalKey;
use crate::version::FileMetadata;
use crate::{Error, Result};

use super::merge_iterator::{DedupMergeIterator, MergeEntry, MergeSource, VecMergeSource};
use super::{Compaction, CompactionFilter, CompactionInput, CompactionOutput, FilterDecision, NoopCompactionFilter};

/// Statistics from a compaction operation.
#[derive(Debug, Clone, Default)]
pub struct CompactionStats {
    /// Number of input files.
    pub num_input_files: usize,
    /// Number of output files.
    pub num_output_files: usize,
    /// Total bytes read.
    pub bytes_read: u64,
    /// Total bytes written.
    pub bytes_written: u64,
    /// Number of entries read.
    pub entries_read: u64,
    /// Number of entries written.
    pub entries_written: u64,
    /// Number of entries dropped (older versions).
    pub entries_dropped: u64,
    /// Number of deletions dropped.
    pub deletions_dropped: u64,
    /// Time taken in milliseconds.
    pub elapsed_ms: u64,
}

impl CompactionStats {
    /// Get the space amplification (bytes_written / bytes_read).
    pub fn space_amplification(&self) -> f64 {
        if self.bytes_read == 0 {
            0.0
        } else {
            self.bytes_written as f64 / self.bytes_read as f64
        }
    }

    /// Get the write amplification.
    pub fn write_amplification(&self) -> f64 {
        if self.entries_read == 0 {
            0.0
        } else {
            self.entries_written as f64 / self.entries_read as f64
        }
    }
}

/// Compactor that executes compaction operations.
pub struct Compactor {
    /// Database path.
    db_path: PathBuf,
    /// Configuration options.
    options: Arc<Options>,
    /// Compaction filter.
    filter: Box<dyn CompactionFilter>,
    /// Oldest sequence that must be preserved.
    /// Entries older than this with newer versions can be dropped.
    oldest_snapshot_sequence: u64,
}

impl Compactor {
    /// Create a new compactor.
    pub fn new(db_path: &Path, options: Arc<Options>) -> Self {
        Self {
            db_path: db_path.to_path_buf(),
            options,
            filter: Box::new(NoopCompactionFilter),
            oldest_snapshot_sequence: 0,
        }
    }

    /// Set the compaction filter.
    pub fn set_filter(&mut self, filter: Box<dyn CompactionFilter>) {
        self.filter = filter;
    }

    /// Set the oldest snapshot sequence.
    ///
    /// Entries with sequence numbers >= this value must be preserved
    /// even if there are newer versions.
    pub fn set_oldest_snapshot(&mut self, sequence: u64) {
        self.oldest_snapshot_sequence = sequence;
    }

    /// Execute a compaction.
    pub fn compact(
        &self,
        compaction: &Compaction,
        new_file_number: impl FnMut() -> u64,
    ) -> Result<(CompactionOutput, CompactionStats)> {
        let start = Instant::now();
        let mut stats = CompactionStats::default();

        // Handle trivial move
        if compaction.is_trivial_move() {
            return self.trivial_move(compaction, &mut stats, start);
        }

        // Collect all entries from input files
        let mut all_entries = Vec::new();
        let input = &compaction.input;

        stats.num_input_files = input.all_files().len();

        // Read L0/source level files
        for file in &input.level_files {
            let entries = self.read_sstable_entries(file)?;
            stats.bytes_read += file.file_size();
            stats.entries_read += entries.len() as u64;
            all_entries.push(entries);
        }

        // Read L1/target level files
        for file in &input.level_plus_one_files {
            let entries = self.read_sstable_entries(file)?;
            stats.bytes_read += file.file_size();
            stats.entries_read += entries.len() as u64;
            all_entries.push(entries);
        }

        // Create merge iterator sources
        let sources: Vec<VecMergeSource> = all_entries
            .into_iter()
            .map(VecMergeSource::new)
            .collect();

        // Create deduplicating merge iterator
        let mut merge_iter = DedupMergeIterator::new(sources);
        merge_iter.seek_to_first()?;

        // Write output files
        let (output_files, write_stats) = self.write_output_files(
            compaction.output_level(),
            &mut merge_iter,
            new_file_number,
        )?;

        stats.num_output_files = output_files.len();
        stats.bytes_written = write_stats.bytes_written;
        stats.entries_written = write_stats.entries_written;
        stats.entries_dropped = write_stats.entries_dropped;
        stats.deletions_dropped = write_stats.deletions_dropped;
        stats.elapsed_ms = start.elapsed().as_millis() as u64;

        // Build output
        let output = CompactionOutput::new(
            compaction.output_level(),
            output_files,
            input.all_files(),
        );

        Ok((output, stats))
    }

    /// Perform a trivial move (just move file metadata to next level).
    fn trivial_move(
        &self,
        compaction: &Compaction,
        stats: &mut CompactionStats,
        start: Instant,
    ) -> Result<(CompactionOutput, CompactionStats)> {
        let file = compaction.input.level_files.first()
            .ok_or_else(|| Error::internal("Trivial move with no files"))?;

        stats.num_input_files = 1;
        stats.num_output_files = 1;
        stats.bytes_read = file.file_size();
        stats.bytes_written = file.file_size();
        stats.elapsed_ms = start.elapsed().as_millis() as u64;

        // The output is the same file, just at a different level
        let output = CompactionOutput::new(
            compaction.output_level(),
            vec![file.clone()],
            compaction.input.level_files.clone(),
        );

        Ok((output, stats.clone()))
    }

    /// Read all entries from an SSTable.
    fn read_sstable_entries(&self, file: &Arc<FileMetadata>) -> Result<Vec<(Bytes, Bytes)>> {
        let path = self.db_path.join(format!("{:06}.ldb", file.file_number()));
        let mut reader = SSTableReader::open(&path, file.file_number())?;
        let iter = SSTableEntryIterator::new(&mut reader)?;
        Ok(iter.collect())
    }

    /// Write output files from the merge iterator.
    fn write_output_files(
        &self,
        level: usize,
        iter: &mut DedupMergeIterator<VecMergeSource>,
        mut new_file_number: impl FnMut() -> u64,
    ) -> Result<(Vec<Arc<FileMetadata>>, WriteStats)> {
        let mut output_files = Vec::new();
        let mut stats = WriteStats::default();

        let target_file_size = self.options.target_file_size_for_level(level) as u64;
        let compression = match self.options.compression {
            crate::options::Compression::None => CompressionType::None,
            crate::options::Compression::Lz4 => CompressionType::Lz4,
            crate::options::Compression::Snappy => CompressionType::Snappy,
        };

        let mut current_writer: Option<OutputFileWriter> = None;
        let mut last_user_key: Option<Bytes> = None;

        while iter.valid() {
            let entry = match iter.current() {
                Some(e) => e.clone(),
                None => break,
            };

            let user_key = Bytes::copy_from_slice(entry.user_key());
            let sequence = entry.sequence();
            let is_deletion = entry.is_deletion();

            // Check if we should drop this entry
            let should_drop = self.should_drop_entry(
                &entry,
                &last_user_key,
                level,
            );

            if should_drop {
                if is_deletion {
                    stats.deletions_dropped += 1;
                } else {
                    stats.entries_dropped += 1;
                }
                iter.next()?;
                continue;
            }

            // Check if we need to start a new output file
            let need_new_file = match &current_writer {
                None => true,
                Some(writer) => writer.estimated_size >= target_file_size,
            };

            if need_new_file {
                // Finish current file if exists
                if let Some(writer) = current_writer.take() {
                    let file_meta = writer.finish()?;
                    stats.bytes_written += file_meta.file_size();
                    output_files.push(Arc::new(file_meta));
                }

                // Start new file
                let file_number = new_file_number();
                let path = self.db_path.join(format!("{:06}.ldb", file_number));
                current_writer = Some(OutputFileWriter::new(
                    &path,
                    file_number,
                    compression,
                    self.options.bloom_filter_bits_per_key,
                )?);
            }

            // Write entry
            if let Some(ref mut writer) = current_writer {
                writer.add(&entry.key, &entry.value)?;
                stats.entries_written += 1;
            }

            last_user_key = Some(user_key);
            iter.next()?;
        }

        // Finish last file
        if let Some(writer) = current_writer {
            if writer.entry_count > 0 {
                let file_meta = writer.finish()?;
                stats.bytes_written += file_meta.file_size();
                output_files.push(Arc::new(file_meta));
            }
        }

        Ok((output_files, stats))
    }

    /// Check if an entry should be dropped during compaction.
    fn should_drop_entry(
        &self,
        entry: &MergeEntry,
        last_user_key: &Option<Bytes>,
        level: usize,
    ) -> bool {
        let user_key = entry.user_key();
        let sequence = entry.sequence();
        let is_deletion = entry.is_deletion();

        // Check compaction filter first
        if !is_deletion {
            let decision = self.filter.filter(
                level,
                user_key,
                &entry.value,
                sequence,
            );
            if decision == FilterDecision::Remove {
                return true;
            }
        }

        // Check if this is an older version (same user key as previous entry)
        if let Some(ref last) = last_user_key {
            if user_key == last.as_ref() {
                // This is an older version
                // Only drop if there are no active snapshots that need it
                if sequence < self.oldest_snapshot_sequence {
                    return true;
                }
            }
        }

        // Check if we can drop a deletion marker
        if is_deletion {
            // Can only drop deletion at the bottom level
            // For simplicity, we keep all deletions for now
            // A more sophisticated implementation would track whether
            // there are any files in lower levels that might contain this key
            return false;
        }

        false
    }
}

/// Statistics from writing output files.
#[derive(Debug, Default)]
struct WriteStats {
    bytes_written: u64,
    entries_written: u64,
    entries_dropped: u64,
    deletions_dropped: u64,
}

/// Helper for writing an output SSTable file.
struct OutputFileWriter {
    writer: SSTableWriter,
    file_number: u64,
    smallest_key: Option<InternalKey>,
    largest_key: Option<InternalKey>,
    estimated_size: u64,
    entry_count: u64,
}

impl OutputFileWriter {
    fn new(
        path: &Path,
        file_number: u64,
        compression: CompressionType,
        bloom_bits: usize,
    ) -> Result<Self> {
        let writer = SSTableWriter::new(path, file_number, compression, bloom_bits)?;
        Ok(Self {
            writer,
            file_number,
            smallest_key: None,
            largest_key: None,
            estimated_size: 0,
            entry_count: 0,
        })
    }

    fn add(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        // Track key range
        let internal_key = InternalKey::decode(key)
            .ok_or_else(|| Error::corruption("Invalid internal key in compaction"))?;

        if self.smallest_key.is_none() {
            self.smallest_key = Some(internal_key.clone());
        }
        self.largest_key = Some(internal_key);

        // Write to SSTable
        self.writer.add(key, value)?;

        // Update estimates
        self.estimated_size += (key.len() + value.len() + 16) as u64;
        self.entry_count += 1;

        Ok(())
    }

    fn finish(self) -> Result<FileMetadata> {
        let info = self.writer.finish()?;

        let smallest = self.smallest_key
            .ok_or_else(|| Error::internal("No entries written to output file"))?;
        let largest = self.largest_key
            .ok_or_else(|| Error::internal("No entries written to output file"))?;

        Ok(FileMetadata::new(
            self.file_number,
            info.file_size,
            smallest,
            largest,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn create_test_options() -> Options {
        let mut opts = Options::default();
        opts.target_file_size_base = 4096; // Small for testing
        opts
    }

    #[test]
    fn test_compaction_stats() {
        let stats = CompactionStats {
            bytes_read: 1000,
            bytes_written: 800,
            entries_read: 100,
            entries_written: 80,
            ..Default::default()
        };

        assert!(stats.space_amplification() < 1.0);
        assert!(stats.write_amplification() < 1.0);
    }

    #[test]
    fn test_compaction_stats_empty() {
        let stats = CompactionStats::default();
        assert_eq!(stats.space_amplification(), 0.0);
        assert_eq!(stats.write_amplification(), 0.0);
    }
}
