//! TransactionManager - manages active transactions.
//!
//! The transaction manager:
//! - Assigns transaction IDs
//! - Tracks active transactions
//! - Provides oldest snapshot sequence for garbage collection
//! - Creates snapshots with proper tracking

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};

use parking_lot::RwLock;

use crate::db::Database;
use crate::{Error, Result};

use super::snapshot::{Snapshot, SnapshotList, SnapshotTracker};
use super::transaction::Transaction;
use super::TransactionId;

/// Manages transactions and snapshots.
///
/// The transaction manager is responsible for:
/// - Allocating transaction IDs
/// - Tracking active transactions
/// - Creating and tracking snapshots
/// - Providing information for garbage collection
pub struct TransactionManager {
    /// Next transaction ID.
    next_txn_id: AtomicU64,
    /// Active transaction count.
    active_count: AtomicU64,
    /// Weak reference to database (to avoid circular reference).
    db: Weak<Database>,
    /// Snapshot tracking.
    snapshots: SnapshotList,
    /// Per-transaction snapshot sequences.
    txn_sequences: RwLock<HashMap<TransactionId, u64>>,
    /// Counter for snapshot tracking.
    snapshot_counter: Arc<AtomicU64>,
    /// Whether to track reads by default.
    default_track_reads: bool,
}

impl TransactionManager {
    /// Create a new transaction manager.
    pub fn new(db: Arc<Database>) -> Self {
        Self {
            next_txn_id: AtomicU64::new(1),
            active_count: AtomicU64::new(0),
            db: Arc::downgrade(&db),
            snapshots: SnapshotList::new(),
            txn_sequences: RwLock::new(HashMap::new()),
            snapshot_counter: Arc::new(AtomicU64::new(0)),
            default_track_reads: true,
        }
    }

    /// Get a strong reference to the database.
    fn get_db(&self) -> Result<Arc<Database>> {
        self.db.upgrade()
            .ok_or_else(|| Error::internal("Database has been dropped"))
    }

    /// Set whether transactions should track reads by default.
    ///
    /// If true, transactions will track all reads for conflict detection.
    /// This has some overhead but enables stricter isolation.
    /// If false, only write-write conflicts are detected.
    pub fn set_track_reads(&mut self, track: bool) {
        self.default_track_reads = track;
    }

    /// Begin a new transaction.
    ///
    /// The transaction starts with a snapshot of the current database
    /// state and will see a consistent view regardless of concurrent writes.
    pub fn begin(&self) -> Result<Transaction> {
        self.begin_with_options(self.default_track_reads)
    }

    /// Begin a transaction with specific options.
    pub fn begin_with_options(&self, track_reads: bool) -> Result<Transaction> {
        let db = self.get_db()?;
        let txn_id = self.next_txn_id.fetch_add(1, Ordering::SeqCst);
        let sequence = db.sequence();

        // Create tracked snapshot
        let tracker = SnapshotTracker::new(sequence, Arc::clone(&self.snapshot_counter));
        let snapshot = Snapshot::new_tracked(sequence, tracker);

        // Record in snapshot list
        self.snapshots.snapshot_created(sequence);

        // Track the transaction
        self.active_count.fetch_add(1, Ordering::SeqCst);
        self.txn_sequences.write().insert(txn_id, sequence);

        Ok(Transaction::new(txn_id, snapshot, db, track_reads))
    }

    /// Create a snapshot without a transaction.
    ///
    /// Snapshots are lighter weight than transactions - they only
    /// provide consistent reads, not writes.
    pub fn snapshot(&self) -> Result<Snapshot> {
        let db = self.get_db()?;
        let sequence = db.sequence();
        let tracker = SnapshotTracker::new(sequence, Arc::clone(&self.snapshot_counter));
        self.snapshots.snapshot_created(sequence);
        Ok(Snapshot::new_tracked(sequence, tracker))
    }

    /// Get the number of active transactions.
    pub fn active_count(&self) -> u64 {
        self.active_count.load(Ordering::SeqCst)
    }

    /// Get the number of active snapshots (including those from transactions).
    pub fn snapshot_count(&self) -> u64 {
        self.snapshot_counter.load(Ordering::SeqCst)
    }

    /// Get the oldest snapshot sequence.
    ///
    /// This is used by compaction to determine which old versions
    /// must be preserved. Returns 0 if no snapshots are active.
    pub fn oldest_snapshot_sequence(&self) -> u64 {
        self.snapshots.oldest_sequence()
    }

    /// Notify that a transaction has ended.
    ///
    /// This is called automatically when a transaction commits or rolls back.
    pub(crate) fn transaction_ended(&self, txn_id: TransactionId) {
        self.active_count.fetch_sub(1, Ordering::SeqCst);
        self.txn_sequences.write().remove(&txn_id);
        self.snapshots.snapshot_released();

        // Recalculate oldest sequence
        self.update_oldest_sequence();
    }

    /// Update the oldest sequence after a snapshot is released.
    fn update_oldest_sequence(&self) {
        let sequences = self.txn_sequences.read();
        if sequences.is_empty() {
            self.snapshots.set_oldest_sequence(0);
        } else {
            let oldest = *sequences.values().min().unwrap_or(&0);
            self.snapshots.set_oldest_sequence(oldest);
        }
    }

    /// Get the current sequence from the database.
    pub fn current_sequence(&self) -> Result<u64> {
        Ok(self.get_db()?.sequence())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn create_test_db() -> Arc<Database> {
        let dir = tempdir().unwrap();
        Database::open(dir.path()).unwrap()
    }

    #[test]
    fn test_transaction_manager_new() {
        let db = create_test_db();
        let mgr = TransactionManager::new(Arc::clone(&db));

        assert_eq!(mgr.active_count(), 0);
        assert_eq!(mgr.snapshot_count(), 0);
    }

    #[test]
    fn test_begin_transaction() {
        let db = create_test_db();
        let mgr = TransactionManager::new(Arc::clone(&db));

        let txn = mgr.begin().unwrap();
        assert!(txn.is_active());
        assert_eq!(txn.id(), 1);
        assert_eq!(mgr.active_count(), 1);
        assert_eq!(mgr.snapshot_count(), 1);
    }

    #[test]
    fn test_multiple_transactions() {
        let db = create_test_db();
        let mgr = TransactionManager::new(Arc::clone(&db));

        let txn1 = mgr.begin().unwrap();
        let txn2 = mgr.begin().unwrap();

        assert_eq!(txn1.id(), 1);
        assert_eq!(txn2.id(), 2);
        assert_eq!(mgr.active_count(), 2);
    }

    #[test]
    fn test_snapshot_creation() {
        let db = create_test_db();
        let mgr = TransactionManager::new(Arc::clone(&db));

        let snap = mgr.snapshot().unwrap();
        assert!(snap.sequence() >= 0);
        assert_eq!(mgr.snapshot_count(), 1);
    }

    #[test]
    fn test_oldest_snapshot_sequence() {
        let db = create_test_db();

        // Write some data to advance sequence
        db.put(b"key1", b"value1").unwrap();
        db.put(b"key2", b"value2").unwrap();

        let mgr = TransactionManager::new(Arc::clone(&db));

        // No snapshots initially
        assert_eq!(mgr.oldest_snapshot_sequence(), 0);

        let _txn = mgr.begin().unwrap();
        let seq = mgr.oldest_snapshot_sequence();
        assert!(seq > 0);
    }
}
