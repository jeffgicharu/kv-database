//! Transaction - ACID transaction implementation.
//!
//! Transactions provide:
//! - Snapshot isolation (reads see consistent point-in-time view)
//! - Optimistic concurrency control (conflicts detected at commit)
//! - Atomic commits (all-or-nothing writes)

use std::sync::Arc;

use bytes::Bytes;
use parking_lot::Mutex;

use crate::db::Database;
use crate::types::WriteBatch;
use crate::{Error, Result};

use super::snapshot::Snapshot;
use super::{ReadSet, TransactionId, WriteOp, WriteSet};

/// Transaction state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionState {
    /// Transaction is active and can read/write.
    Active,
    /// Transaction is being committed.
    Committing,
    /// Transaction was committed successfully.
    Committed,
    /// Transaction was rolled back.
    RolledBack,
    /// Transaction was aborted due to conflict.
    Aborted,
}

/// A database transaction.
///
/// Transactions provide snapshot isolation - all reads see a consistent
/// view as of when the transaction started. Writes are buffered locally
/// until commit, when they are validated and applied atomically.
///
/// # Example
///
/// ```ignore
/// let txn = db.begin_transaction()?;
///
/// // Reads see snapshot from transaction start
/// let value = txn.get(b"key")?;
///
/// // Writes are buffered
/// txn.put(b"key", b"new_value")?;
///
/// // Commit applies all writes atomically
/// txn.commit()?;
/// ```
pub struct Transaction {
    /// Transaction ID.
    id: TransactionId,
    /// Snapshot for consistent reads.
    snapshot: Snapshot,
    /// Database reference.
    db: Arc<Database>,
    /// Write set (buffered writes).
    write_set: Mutex<WriteSet>,
    /// Read set (tracked reads for conflict detection).
    read_set: Mutex<ReadSet>,
    /// Current state.
    state: Mutex<TransactionState>,
    /// Whether to track reads for conflict detection.
    track_reads: bool,
}

impl Transaction {
    /// Create a new transaction.
    pub(crate) fn new(
        id: TransactionId,
        snapshot: Snapshot,
        db: Arc<Database>,
        track_reads: bool,
    ) -> Self {
        Self {
            id,
            snapshot,
            db,
            write_set: Mutex::new(WriteSet::new()),
            read_set: Mutex::new(ReadSet::new()),
            state: Mutex::new(TransactionState::Active),
            track_reads,
        }
    }

    /// Get the transaction ID.
    pub fn id(&self) -> TransactionId {
        self.id
    }

    /// Get the snapshot sequence number.
    pub fn snapshot_sequence(&self) -> u64 {
        self.snapshot.sequence()
    }

    /// Get the current state.
    pub fn state(&self) -> TransactionState {
        *self.state.lock()
    }

    /// Check if the transaction is still active.
    pub fn is_active(&self) -> bool {
        *self.state.lock() == TransactionState::Active
    }

    /// Get a value by key.
    ///
    /// First checks the transaction's write buffer, then reads from
    /// the database at the snapshot sequence.
    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.check_active()?;

        // Check write set first
        {
            let write_set = self.write_set.lock();
            if let Some(op) = write_set.get(key) {
                return match op {
                    WriteOp::Put { value, .. } => Ok(Some(value.clone())),
                    WriteOp::Delete { .. } => Ok(None),
                };
            }
        }

        // Read from database at snapshot sequence
        let result = self.db.get_at_sequence(key, self.snapshot.sequence())?;

        // Track read for conflict detection
        if self.track_reads {
            let mut read_set = self.read_set.lock();
            read_set.add(Bytes::copy_from_slice(key), self.snapshot.sequence());
        }

        Ok(result)
    }

    /// Put a key-value pair.
    ///
    /// The write is buffered until commit.
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.check_active()?;

        let mut write_set = self.write_set.lock();
        write_set.put(
            Bytes::copy_from_slice(key),
            Bytes::copy_from_slice(value),
        );

        Ok(())
    }

    /// Delete a key.
    ///
    /// The delete is buffered until commit.
    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.check_active()?;

        let mut write_set = self.write_set.lock();
        write_set.delete(Bytes::copy_from_slice(key));

        Ok(())
    }

    /// Commit the transaction.
    ///
    /// Validates no conflicts occurred and applies all buffered writes
    /// atomically.
    pub fn commit(self) -> Result<()> {
        // Set state to committing
        {
            let mut state = self.state.lock();
            if *state != TransactionState::Active {
                return Err(Error::TransactionAborted(
                    "Transaction is not active".to_string()
                ));
            }
            *state = TransactionState::Committing;
        }

        // Check for conflicts
        if self.track_reads {
            self.check_conflicts()?;
        }

        // Get the write set
        let write_set = self.write_set.lock();

        if write_set.is_empty() {
            // No writes, just mark as committed
            *self.state.lock() = TransactionState::Committed;
            return Ok(());
        }

        // Build a write batch from the buffered writes
        let mut batch = WriteBatch::new();
        for op in write_set.operations() {
            match op {
                WriteOp::Put { key, value } => {
                    batch.put(key.clone(), value.clone());
                }
                WriteOp::Delete { key } => {
                    batch.delete(key.clone());
                }
            }
        }

        // Apply the batch
        match self.db.write(batch) {
            Ok(()) => {
                *self.state.lock() = TransactionState::Committed;
                Ok(())
            }
            Err(e) => {
                *self.state.lock() = TransactionState::Aborted;
                Err(e)
            }
        }
    }

    /// Rollback the transaction.
    ///
    /// Discards all buffered writes without applying them.
    pub fn rollback(self) -> Result<()> {
        let mut state = self.state.lock();
        if *state != TransactionState::Active {
            return Err(Error::TransactionAborted(
                "Transaction is not active".to_string()
            ));
        }
        *state = TransactionState::RolledBack;
        Ok(())
    }

    /// Check if the transaction is still active.
    fn check_active(&self) -> Result<()> {
        let state = self.state.lock();
        if *state != TransactionState::Active {
            return Err(Error::TransactionAborted(format!(
                "Transaction is {:?}",
                *state
            )));
        }
        Ok(())
    }

    /// Check for conflicts with concurrent transactions.
    ///
    /// A conflict occurs if any key in the read set was modified
    /// after our snapshot sequence.
    fn check_conflicts(&self) -> Result<()> {
        let read_set = self.read_set.lock();

        for entry in read_set.entries() {
            // Check if this key has been modified since our snapshot
            let current_seq = self.db.sequence();
            if current_seq > self.snapshot.sequence() {
                // Someone wrote after us - check if they modified our keys
                // Get the current value and compare sequence
                if let Ok(Some(_)) = self.db.get_at_sequence(&entry.key, current_seq) {
                    // Key exists at current sequence
                    // Check if it was modified after our snapshot
                    if self.was_key_modified_after(&entry.key, self.snapshot.sequence())? {
                        *self.state.lock() = TransactionState::Aborted;
                        return Err(Error::TransactionConflict(format!(
                            "Key {:?} was modified by another transaction",
                            String::from_utf8_lossy(&entry.key)
                        )));
                    }
                }
            }
        }

        Ok(())
    }

    /// Check if a key was modified after a given sequence.
    fn was_key_modified_after(&self, key: &[u8], sequence: u64) -> Result<bool> {
        // Get value at current sequence
        let current = self.db.get(key)?;
        // Get value at snapshot sequence
        let at_snapshot = self.db.get_at_sequence(key, sequence)?;

        // If they differ, the key was modified
        Ok(current != at_snapshot)
    }

    /// Get the number of pending writes.
    pub fn write_count(&self) -> usize {
        self.write_set.lock().len()
    }

    /// Get the number of tracked reads.
    pub fn read_count(&self) -> usize {
        self.read_set.lock().len()
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {
        // If transaction is still active, it's being dropped without
        // commit or rollback - treat as implicit rollback
        let mut state = self.state.lock();
        if *state == TransactionState::Active {
            *state = TransactionState::RolledBack;
        }
        drop(state);

        // Notify the database that this transaction has ended
        self.db.transaction_ended(self.id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_transaction_state() {
        assert_eq!(TransactionState::Active, TransactionState::Active);
        assert_ne!(TransactionState::Active, TransactionState::Committed);
    }
}
