//! Database iterators for range scans.
//!
//! The iterator module provides efficient iteration over database contents:
//! - DBIterator: Main iterator merging all data sources
//! - Snapshot-consistent views
//! - Seek operations for efficient range queries
//! - Tombstone handling (deleted keys are skipped)

mod db_iterator;

pub use db_iterator::{DBIterator, DBIteratorBuilder, IteratorDirection};
