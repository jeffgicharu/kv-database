//! Transaction module - MVCC transactions with snapshot isolation.
//!
//! This module provides:
//! - Snapshot isolation for consistent reads
//! - Optimistic concurrency control
//! - Write batching within transactions
//! - Conflict detection at commit time
//!
//! # Transaction Lifecycle
//!
//! 1. Begin transaction (captures snapshot sequence)
//! 2. Read operations see consistent snapshot
//! 3. Write operations buffer locally
//! 4. Commit validates no conflicts and applies writes
//! 5. Rollback discards buffered writes
//!
//! # Conflict Detection
//!
//! Uses optimistic concurrency control:
//! - Track which keys are read during transaction
//! - At commit, check if any read keys were modified since snapshot
//! - If conflicts exist, abort transaction

mod manager;
mod snapshot;
mod transaction;

pub use manager::TransactionManager;
pub use snapshot::Snapshot;
pub use transaction::{Transaction, TransactionState};

use std::collections::HashSet;

use bytes::Bytes;

/// Transaction ID type.
pub type TransactionId = u64;

/// Write operation in a transaction.
#[derive(Debug, Clone)]
pub enum WriteOp {
    /// Put a value.
    Put { key: Bytes, value: Bytes },
    /// Delete a key.
    Delete { key: Bytes },
}

impl WriteOp {
    /// Get the key for this operation.
    pub fn key(&self) -> &Bytes {
        match self {
            WriteOp::Put { key, .. } => key,
            WriteOp::Delete { key } => key,
        }
    }

    /// Check if this is a delete operation.
    pub fn is_delete(&self) -> bool {
        matches!(self, WriteOp::Delete { .. })
    }
}

/// Read set entry - tracks which keys were read.
#[derive(Debug, Clone)]
pub struct ReadEntry {
    /// The key that was read.
    pub key: Bytes,
    /// The sequence number at which it was read.
    pub sequence: u64,
}

/// Write set - all pending writes in a transaction.
#[derive(Debug, Default)]
pub struct WriteSet {
    /// Ordered list of write operations.
    operations: Vec<WriteOp>,
    /// Set of keys being written (for quick lookup).
    keys: HashSet<Vec<u8>>,
}

impl WriteSet {
    /// Create a new empty write set.
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a put operation.
    pub fn put(&mut self, key: Bytes, value: Bytes) {
        self.keys.insert(key.to_vec());
        self.operations.push(WriteOp::Put { key, value });
    }

    /// Add a delete operation.
    pub fn delete(&mut self, key: Bytes) {
        self.keys.insert(key.to_vec());
        self.operations.push(WriteOp::Delete { key });
    }

    /// Check if a key is in the write set.
    pub fn contains(&self, key: &[u8]) -> bool {
        self.keys.contains(key)
    }

    /// Get the latest write for a key (if any).
    pub fn get(&self, key: &[u8]) -> Option<&WriteOp> {
        // Search in reverse to get the latest write
        self.operations
            .iter()
            .rev()
            .find(|op| op.key().as_ref() == key)
    }

    /// Get all operations.
    pub fn operations(&self) -> &[WriteOp] {
        &self.operations
    }

    /// Check if the write set is empty.
    pub fn is_empty(&self) -> bool {
        self.operations.is_empty()
    }

    /// Get the number of operations.
    pub fn len(&self) -> usize {
        self.operations.len()
    }

    /// Clear the write set.
    pub fn clear(&mut self) {
        self.operations.clear();
        self.keys.clear();
    }
}

/// Read set - tracks all keys read during a transaction.
#[derive(Debug, Default)]
pub struct ReadSet {
    /// Keys that were read.
    entries: Vec<ReadEntry>,
    /// Set of keys for quick lookup.
    keys: HashSet<Vec<u8>>,
}

impl ReadSet {
    /// Create a new empty read set.
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a read.
    pub fn add(&mut self, key: Bytes, sequence: u64) {
        if !self.keys.contains(key.as_ref()) {
            self.keys.insert(key.to_vec());
            self.entries.push(ReadEntry { key, sequence });
        }
    }

    /// Check if a key was read.
    pub fn contains(&self, key: &[u8]) -> bool {
        self.keys.contains(key)
    }

    /// Get all read entries.
    pub fn entries(&self) -> &[ReadEntry] {
        &self.entries
    }

    /// Check if the read set is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Get the number of entries.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Clear the read set.
    pub fn clear(&mut self) {
        self.entries.clear();
        self.keys.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_write_set() {
        let mut ws = WriteSet::new();
        assert!(ws.is_empty());

        ws.put(Bytes::from("key1"), Bytes::from("value1"));
        assert!(!ws.is_empty());
        assert_eq!(ws.len(), 1);
        assert!(ws.contains(b"key1"));
        assert!(!ws.contains(b"key2"));

        ws.delete(Bytes::from("key2"));
        assert_eq!(ws.len(), 2);
        assert!(ws.contains(b"key2"));

        // Check get returns latest
        let op = ws.get(b"key1").unwrap();
        assert!(!op.is_delete());

        let op = ws.get(b"key2").unwrap();
        assert!(op.is_delete());
    }

    #[test]
    fn test_write_set_overwrite() {
        let mut ws = WriteSet::new();
        ws.put(Bytes::from("key"), Bytes::from("v1"));
        ws.put(Bytes::from("key"), Bytes::from("v2"));
        ws.delete(Bytes::from("key"));

        // Should get the latest operation (delete)
        let op = ws.get(b"key").unwrap();
        assert!(op.is_delete());
    }

    #[test]
    fn test_read_set() {
        let mut rs = ReadSet::new();
        assert!(rs.is_empty());

        rs.add(Bytes::from("key1"), 100);
        assert!(!rs.is_empty());
        assert_eq!(rs.len(), 1);
        assert!(rs.contains(b"key1"));

        // Adding same key again doesn't duplicate
        rs.add(Bytes::from("key1"), 101);
        assert_eq!(rs.len(), 1);

        rs.add(Bytes::from("key2"), 102);
        assert_eq!(rs.len(), 2);
    }
}
