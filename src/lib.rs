//! # rustdb
//!
//! An embedded LSM-tree key-value database with ACID transactions.
//!
//! ## Features
//!
//! - **LSM-Tree Storage**: Optimized for write-heavy workloads
//! - **ACID Transactions**: Full ACID compliance with MVCC
//! - **Snapshot Isolation**: Each transaction sees consistent point-in-time view
//! - **Concurrent Access**: Multiple readers and writers
//! - **Compression**: Optional LZ4/Snappy compression
//! - **Durability**: Write-ahead logging with configurable sync
//!
//! ## Quick Start
//!
//! ```rust,ignore
//! use rustdb::{Database, Options};
//!
//! // Open database
//! let db = Database::open("./my_data")?;
//!
//! // Basic operations
//! db.put(b"hello", b"world")?;
//! let value = db.get(b"hello")?;
//! db.delete(b"hello")?;
//!
//! // Transactions
//! let txn = db.begin_transaction()?;
//! txn.put(b"key1", b"value1")?;
//! txn.put(b"key2", b"value2")?;
//! txn.commit()?;
//! ```

// Public modules
pub mod error;
pub mod options;
pub mod types;

// Database module
mod db;

// Internal modules
mod cache;
mod compaction;
mod iterator;
mod memtable;
mod metrics;
mod sstable;
mod transaction;
mod util;
mod version;
mod wal;

// Re-export main types for convenience
pub use error::{Error, Result};
pub use options::{Compression, Options, SyncMode};
pub use types::{InternalKey, ValueType, WriteBatch};

// Database
pub use db::{Database, DatabaseStats, LevelStats};

// Compaction (re-export useful types)
pub use compaction::CompactionStats;

// Iterators
pub use iterator::{DBIterator, DBIteratorBuilder, IteratorDirection};

// Cache
pub use cache::{BlockCache, CacheStats, LruCache, TableCache};

// Metrics
pub use metrics::{Counter, DbMetrics, Gauge, Histogram, MetricsSummary};

// Transactions
pub use transaction::{Transaction, TransactionState};
