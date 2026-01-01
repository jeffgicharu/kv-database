//! Snapshot - point-in-time view of the database.
//!
//! A snapshot captures a sequence number at creation time. All reads
//! through the snapshot see a consistent view as of that sequence.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// A snapshot of the database at a specific sequence number.
///
/// Snapshots provide consistent reads - all reads see the database
/// state as of when the snapshot was created, regardless of concurrent
/// writes.
///
/// Snapshots are reference-counted and tracked by the TransactionManager
/// to ensure garbage collection doesn't remove data that snapshots need.
#[derive(Debug)]
pub struct Snapshot {
    /// The sequence number this snapshot sees.
    sequence: u64,
    /// Reference to the snapshot counter for tracking.
    /// When dropped, decrements the counter.
    _tracker: Option<SnapshotTracker>,
}

impl Snapshot {
    /// Create a new snapshot at the given sequence.
    pub fn new(sequence: u64) -> Self {
        Self {
            sequence,
            _tracker: None,
        }
    }

    /// Create a tracked snapshot.
    ///
    /// The tracker ensures the snapshot is counted for garbage collection.
    pub fn new_tracked(sequence: u64, tracker: SnapshotTracker) -> Self {
        Self {
            sequence,
            _tracker: Some(tracker),
        }
    }

    /// Get the sequence number for this snapshot.
    pub fn sequence(&self) -> u64 {
        self.sequence
    }
}

impl Clone for Snapshot {
    fn clone(&self) -> Self {
        // Clone creates a new untracked snapshot at the same sequence
        Self {
            sequence: self.sequence,
            _tracker: self._tracker.clone(),
        }
    }
}

/// Tracks active snapshots for garbage collection.
///
/// When all trackers for a sequence are dropped, that sequence
/// can be garbage collected.
#[derive(Debug, Clone)]
pub struct SnapshotTracker {
    /// The tracked sequence number.
    sequence: u64,
    /// Counter for active snapshots at this sequence.
    counter: Arc<AtomicU64>,
}

impl SnapshotTracker {
    /// Create a new tracker.
    pub fn new(sequence: u64, counter: Arc<AtomicU64>) -> Self {
        counter.fetch_add(1, Ordering::SeqCst);
        Self { sequence, counter }
    }

    /// Get the tracked sequence.
    pub fn sequence(&self) -> u64 {
        self.sequence
    }
}

impl Drop for SnapshotTracker {
    fn drop(&mut self) {
        self.counter.fetch_sub(1, Ordering::SeqCst);
    }
}

/// Manages the oldest active snapshot sequence.
///
/// This is used by compaction to know which old versions can be
/// garbage collected.
#[derive(Debug, Default)]
pub struct SnapshotList {
    /// Active snapshot count.
    active_count: AtomicU64,
    /// Oldest snapshot sequence (0 if no snapshots).
    oldest_sequence: AtomicU64,
}

impl SnapshotList {
    /// Create a new empty snapshot list.
    pub fn new() -> Self {
        Self::default()
    }

    /// Get the number of active snapshots.
    pub fn active_count(&self) -> u64 {
        self.active_count.load(Ordering::SeqCst)
    }

    /// Get the oldest active snapshot sequence.
    ///
    /// Returns 0 if no snapshots are active.
    pub fn oldest_sequence(&self) -> u64 {
        self.oldest_sequence.load(Ordering::SeqCst)
    }

    /// Record that a snapshot was created.
    pub fn snapshot_created(&self, sequence: u64) {
        self.active_count.fetch_add(1, Ordering::SeqCst);

        // Update oldest if this is older (or first)
        let mut current = self.oldest_sequence.load(Ordering::SeqCst);
        loop {
            if current != 0 && current <= sequence {
                break;
            }
            match self.oldest_sequence.compare_exchange_weak(
                current,
                sequence,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => break,
                Err(actual) => current = actual,
            }
        }
    }

    /// Record that a snapshot was released.
    ///
    /// Note: This doesn't automatically update oldest_sequence.
    /// Call update_oldest() after releasing snapshots.
    pub fn snapshot_released(&self) {
        self.active_count.fetch_sub(1, Ordering::SeqCst);
    }

    /// Reset oldest sequence (typically after recalculating).
    pub fn set_oldest_sequence(&self, sequence: u64) {
        self.oldest_sequence.store(sequence, Ordering::SeqCst);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_snapshot_sequence() {
        let snapshot = Snapshot::new(12345);
        assert_eq!(snapshot.sequence(), 12345);
    }

    #[test]
    fn test_snapshot_clone() {
        let snapshot = Snapshot::new(100);
        let cloned = snapshot.clone();
        assert_eq!(cloned.sequence(), 100);
    }

    #[test]
    fn test_snapshot_tracker() {
        let counter = Arc::new(AtomicU64::new(0));

        {
            let _tracker1 = SnapshotTracker::new(100, Arc::clone(&counter));
            assert_eq!(counter.load(Ordering::SeqCst), 1);

            let _tracker2 = SnapshotTracker::new(200, Arc::clone(&counter));
            assert_eq!(counter.load(Ordering::SeqCst), 2);
        }

        // After drop, counter should be back to 0
        assert_eq!(counter.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn test_snapshot_list() {
        let list = SnapshotList::new();
        assert_eq!(list.active_count(), 0);
        assert_eq!(list.oldest_sequence(), 0);

        list.snapshot_created(100);
        assert_eq!(list.active_count(), 1);
        assert_eq!(list.oldest_sequence(), 100);

        list.snapshot_created(200);
        assert_eq!(list.active_count(), 2);
        assert_eq!(list.oldest_sequence(), 100); // Still 100

        list.snapshot_created(50);
        assert_eq!(list.active_count(), 3);
        assert_eq!(list.oldest_sequence(), 50); // Now 50
    }
}
