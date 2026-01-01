//! Background compaction - manages background compaction threads.
//!
//! This module provides:
//! - Background compaction scheduling
//! - Thread management for compaction workers
//! - Integration with the version set

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

use parking_lot::{Condvar, Mutex, RwLock};

use crate::options::Options;
use crate::version::VersionSet;
use crate::{Error, Result};

use super::compactor::Compactor;
use super::picker::CompactionPicker;
use super::{CompactionOutput, CompactionStats};

/// Background compaction state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompactionState {
    /// Idle, no compaction running.
    Idle,
    /// Compaction is scheduled but not yet started.
    Scheduled,
    /// Compaction is running.
    Running,
    /// Compaction completed successfully.
    Completed,
    /// Compaction failed.
    Failed,
}

/// Request for a compaction operation.
#[derive(Debug, Clone)]
pub struct CompactionRequest {
    /// Level to compact from (None for auto-pick).
    pub level: Option<usize>,
    /// Whether this is a manual compaction.
    pub manual: bool,
    /// Optional key range for manual compaction.
    pub begin: Option<Vec<u8>>,
    pub end: Option<Vec<u8>>,
}

impl CompactionRequest {
    /// Create an automatic compaction request.
    pub fn auto() -> Self {
        Self {
            level: None,
            manual: false,
            begin: None,
            end: None,
        }
    }

    /// Create a manual compaction request for a specific level.
    pub fn manual(level: usize) -> Self {
        Self {
            level: Some(level),
            manual: true,
            begin: None,
            end: None,
        }
    }

    /// Create a manual compaction request with a key range.
    pub fn manual_range(level: usize, begin: Option<Vec<u8>>, end: Option<Vec<u8>>) -> Self {
        Self {
            level: Some(level),
            manual: true,
            begin,
            end,
        }
    }
}

/// Result of a compaction operation.
#[derive(Debug)]
pub struct CompactionResult {
    /// Output from the compaction.
    pub output: CompactionOutput,
    /// Statistics from the compaction.
    pub stats: CompactionStats,
}

/// Background compaction scheduler and executor.
pub struct BackgroundCompaction {
    /// Database path.
    db_path: PathBuf,
    /// Database options.
    options: Arc<Options>,
    /// Version set.
    versions: Arc<VersionSet>,
    /// Current compaction state.
    state: RwLock<CompactionState>,
    /// Pending compaction request.
    pending_request: Mutex<Option<CompactionRequest>>,
    /// Condition variable for signaling.
    cond: Condvar,
    /// Whether background compaction is enabled.
    enabled: AtomicBool,
    /// Whether the background thread should shut down.
    shutdown: AtomicBool,
    /// Background thread handle.
    thread_handle: Mutex<Option<JoinHandle<()>>>,
    /// Error from the last compaction (if any).
    last_error: RwLock<Option<Error>>,
    /// Total bytes compacted.
    total_bytes_compacted: AtomicU64,
    /// Total compaction count.
    total_compactions: AtomicU64,
    /// Oldest snapshot sequence (for MVCC).
    oldest_snapshot: AtomicU64,
}

impl BackgroundCompaction {
    /// Create a new background compaction manager.
    pub fn new(
        db_path: &Path,
        options: Arc<Options>,
        versions: Arc<VersionSet>,
    ) -> Arc<Self> {
        Arc::new(Self {
            db_path: db_path.to_path_buf(),
            options,
            versions,
            state: RwLock::new(CompactionState::Idle),
            pending_request: Mutex::new(None),
            cond: Condvar::new(),
            enabled: AtomicBool::new(true),
            shutdown: AtomicBool::new(false),
            thread_handle: Mutex::new(None),
            last_error: RwLock::new(None),
            total_bytes_compacted: AtomicU64::new(0),
            total_compactions: AtomicU64::new(0),
            oldest_snapshot: AtomicU64::new(0),
        })
    }

    /// Start the background compaction thread.
    pub fn start(self: &Arc<Self>) {
        let this = Arc::clone(self);
        let handle = thread::Builder::new()
            .name("rustdb-compaction".to_string())
            .spawn(move || {
                this.background_loop();
            })
            .expect("Failed to spawn compaction thread");

        *self.thread_handle.lock() = Some(handle);
    }

    /// Stop the background compaction thread.
    pub fn stop(&self) {
        self.shutdown.store(true, Ordering::SeqCst);

        // Wake up the background thread
        {
            let mut pending = self.pending_request.lock();
            *pending = None;
            self.cond.notify_all();
        }

        // Wait for thread to finish
        if let Some(handle) = self.thread_handle.lock().take() {
            let _ = handle.join();
        }
    }

    /// Enable or disable background compaction.
    pub fn set_enabled(&self, enabled: bool) {
        self.enabled.store(enabled, Ordering::SeqCst);
    }

    /// Check if background compaction is enabled.
    pub fn is_enabled(&self) -> bool {
        self.enabled.load(Ordering::SeqCst)
    }

    /// Set the oldest snapshot sequence.
    pub fn set_oldest_snapshot(&self, sequence: u64) {
        self.oldest_snapshot.store(sequence, Ordering::SeqCst);
    }

    /// Schedule a compaction if needed.
    pub fn maybe_schedule_compaction(&self) {
        if !self.enabled.load(Ordering::SeqCst) {
            return;
        }

        if self.shutdown.load(Ordering::SeqCst) {
            return;
        }

        // Check if compaction is needed
        let version = self.versions.current();
        if !version.needs_compaction() {
            return;
        }

        self.schedule(CompactionRequest::auto());
    }

    /// Schedule a compaction request.
    pub fn schedule(&self, request: CompactionRequest) {
        let mut pending = self.pending_request.lock();

        // Don't override manual requests with auto requests
        if let Some(ref existing) = *pending {
            if existing.manual && !request.manual {
                return;
            }
        }

        *pending = Some(request);
        *self.state.write() = CompactionState::Scheduled;
        self.cond.notify_one();
    }

    /// Run a manual compaction synchronously.
    pub fn compact_level(&self, level: usize) -> Result<Option<CompactionStats>> {
        self.run_compaction_sync(CompactionRequest::manual(level))
    }

    /// Run a manual compaction with a key range synchronously.
    pub fn compact_range(
        &self,
        level: usize,
        begin: Option<&[u8]>,
        end: Option<&[u8]>,
    ) -> Result<Option<CompactionStats>> {
        let request = CompactionRequest::manual_range(
            level,
            begin.map(|b| b.to_vec()),
            end.map(|e| e.to_vec()),
        );
        self.run_compaction_sync(request)
    }

    /// Run a compaction synchronously.
    fn run_compaction_sync(&self, request: CompactionRequest) -> Result<Option<CompactionStats>> {
        let picker = CompactionPicker::new(Arc::clone(&self.options));
        let version = self.versions.current();

        // Pick compaction based on request
        let compaction = if request.manual {
            let level = request.level.unwrap_or(0);
            picker.pick_manual_compaction(
                &version,
                level,
                request.begin.as_deref(),
                request.end.as_deref(),
            )
        } else {
            picker.pick_compaction(&version)
        };

        let compaction = match compaction {
            Some(c) => c,
            None => return Ok(None),
        };

        // Run the compaction
        let mut compactor = Compactor::new(&self.db_path, Arc::clone(&self.options));
        compactor.set_oldest_snapshot(self.oldest_snapshot.load(Ordering::SeqCst));

        let versions = Arc::clone(&self.versions);
        let (output, stats) = compactor.compact(&compaction, || versions.new_file_number())?;

        // Apply the compaction result to the version set
        self.apply_compaction_result(&compaction, &output)?;

        // Update statistics
        self.total_bytes_compacted.fetch_add(stats.bytes_written, Ordering::Relaxed);
        self.total_compactions.fetch_add(1, Ordering::Relaxed);

        Ok(Some(stats))
    }

    /// Background compaction loop.
    fn background_loop(&self) {
        while !self.shutdown.load(Ordering::SeqCst) {
            // Wait for a compaction request
            let request = {
                let mut pending = self.pending_request.lock();

                while pending.is_none() && !self.shutdown.load(Ordering::SeqCst) {
                    // Wait with timeout to periodically check for compaction needs
                    self.cond.wait_for(&mut pending, Duration::from_millis(500));

                    // Check if we should auto-schedule
                    if pending.is_none() && self.enabled.load(Ordering::SeqCst) {
                        let version = self.versions.current();
                        if version.needs_compaction() {
                            *pending = Some(CompactionRequest::auto());
                        }
                    }
                }

                if self.shutdown.load(Ordering::SeqCst) {
                    break;
                }

                pending.take()
            };

            if let Some(request) = request {
                *self.state.write() = CompactionState::Running;

                match self.run_compaction_sync(request) {
                    Ok(_) => {
                        *self.state.write() = CompactionState::Completed;
                        *self.last_error.write() = None;
                    }
                    Err(e) => {
                        *self.state.write() = CompactionState::Failed;
                        *self.last_error.write() = Some(e);
                    }
                }

                *self.state.write() = CompactionState::Idle;
            }
        }
    }

    /// Apply compaction result to the version set.
    fn apply_compaction_result(
        &self,
        compaction: &super::Compaction,
        output: &CompactionOutput,
    ) -> Result<()> {
        use crate::version::VersionEdit;

        let mut edit = VersionEdit::new();

        // Remove old files
        for file in &compaction.input.level_files {
            edit.delete_file(compaction.level(), file.file_number());
        }
        for file in &compaction.input.level_plus_one_files {
            edit.delete_file(compaction.output_level(), file.file_number());
        }

        // Add new files
        for file in &output.output_files {
            // Clone the file metadata for the edit (unwrap from Arc)
            edit.add_file(output.level, (*file.as_ref()).clone());
        }

        // Apply the edit
        self.versions.log_and_apply(&mut edit)?;

        Ok(())
    }

    /// Get the current compaction state.
    pub fn state(&self) -> CompactionState {
        *self.state.read()
    }

    /// Get the last compaction error (if any).
    pub fn last_error(&self) -> Option<Error> {
        self.last_error.read().clone()
    }

    /// Get total bytes compacted.
    pub fn total_bytes_compacted(&self) -> u64 {
        self.total_bytes_compacted.load(Ordering::Relaxed)
    }

    /// Get total compaction count.
    pub fn total_compactions(&self) -> u64 {
        self.total_compactions.load(Ordering::Relaxed)
    }

    /// Wait for any pending compaction to complete.
    pub fn wait_for_compaction(&self, timeout: Duration) -> bool {
        let mut pending = self.pending_request.lock();
        let start = std::time::Instant::now();

        while *self.state.read() != CompactionState::Idle {
            let remaining = timeout.saturating_sub(start.elapsed());
            if remaining.is_zero() {
                return false;
            }
            self.cond.wait_for(&mut pending, remaining);
        }

        true
    }
}

impl Drop for BackgroundCompaction {
    fn drop(&mut self) {
        self.stop();
    }
}

/// Compaction statistics collector.
#[derive(Debug, Default)]
pub struct CompactionStatsCollector {
    /// Stats per level.
    level_stats: [LevelCompactionStats; 7],
    /// Total compactions.
    total_compactions: u64,
    /// Total bytes read.
    total_bytes_read: u64,
    /// Total bytes written.
    total_bytes_written: u64,
}

/// Per-level compaction statistics.
#[derive(Debug, Default, Clone, Copy)]
pub struct LevelCompactionStats {
    /// Number of compactions at this level.
    pub compaction_count: u64,
    /// Bytes read from this level.
    pub bytes_read: u64,
    /// Bytes written to this level.
    pub bytes_written: u64,
    /// Time spent compacting (milliseconds).
    pub time_ms: u64,
}

impl CompactionStatsCollector {
    /// Create a new stats collector.
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a compaction.
    pub fn record(&mut self, level: usize, stats: &CompactionStats) {
        if level < 7 {
            self.level_stats[level].compaction_count += 1;
            self.level_stats[level].bytes_read += stats.bytes_read;
            self.level_stats[level].bytes_written += stats.bytes_written;
            self.level_stats[level].time_ms += stats.elapsed_ms;
        }

        self.total_compactions += 1;
        self.total_bytes_read += stats.bytes_read;
        self.total_bytes_written += stats.bytes_written;
    }

    /// Get stats for a level.
    pub fn level_stats(&self, level: usize) -> Option<&LevelCompactionStats> {
        self.level_stats.get(level)
    }

    /// Get total write amplification.
    pub fn write_amplification(&self) -> f64 {
        if self.total_bytes_read == 0 {
            0.0
        } else {
            self.total_bytes_written as f64 / self.total_bytes_read as f64
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compaction_request_auto() {
        let request = CompactionRequest::auto();
        assert!(!request.manual);
        assert!(request.level.is_none());
    }

    #[test]
    fn test_compaction_request_manual() {
        let request = CompactionRequest::manual(2);
        assert!(request.manual);
        assert_eq!(request.level, Some(2));
    }

    #[test]
    fn test_compaction_request_manual_range() {
        let request = CompactionRequest::manual_range(
            1,
            Some(b"begin".to_vec()),
            Some(b"end".to_vec()),
        );
        assert!(request.manual);
        assert_eq!(request.level, Some(1));
        assert_eq!(request.begin, Some(b"begin".to_vec()));
        assert_eq!(request.end, Some(b"end".to_vec()));
    }

    #[test]
    fn test_compaction_state() {
        assert_eq!(CompactionState::Idle, CompactionState::Idle);
        assert_ne!(CompactionState::Idle, CompactionState::Running);
    }

    #[test]
    fn test_stats_collector() {
        let mut collector = CompactionStatsCollector::new();

        let stats = CompactionStats {
            bytes_read: 1000,
            bytes_written: 800,
            elapsed_ms: 100,
            ..Default::default()
        };

        collector.record(0, &stats);

        assert_eq!(collector.total_compactions, 1);
        assert_eq!(collector.total_bytes_read, 1000);
        assert_eq!(collector.total_bytes_written, 800);
        assert!(collector.write_amplification() < 1.0);
    }
}
