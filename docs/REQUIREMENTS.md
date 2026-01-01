# Key-Value Database Engine Requirements

## 1. Problem Statement

### 1.1 The Core Problem

Applications need to store data that:
1. **Survives restarts** - Data persists across program restarts and crashes
2. **Exceeds RAM** - Can store more data than available memory
3. **Supports concurrent access** - Multiple threads can read/write simultaneously
4. **Provides consistency** - Transactions see coherent snapshots of data

A simple HashMap fails all four requirements. A key-value database solves them.

### 1.2 Why This Matters

Key-value databases are foundational infrastructure:
- **RocksDB** powers Facebook's MySQL (MyRocks), TiKV, CockroachDB
- **LevelDB** powers Chrome's IndexedDB, Bitcoin Core
- **SQLite** is in every smartphone, browser, and embedded device

Understanding storage engines demonstrates deep systems knowledge.

### 1.3 Target Use Cases

| Use Case | Example | Why KV Database Fits |
|----------|---------|---------------------|
| Application state | Game save data, user preferences | Structured, survives restarts |
| Caching layer | Persistent cache, session store | Fast reads, durability optional |
| Time-series data | Metrics, logs, events | Sequential writes, range queries |
| Document storage | JSON documents, configurations | Arbitrary value sizes |
| Index backing store | Search indexes, materialized views | Ordered iteration |

---

## 2. Requirements Overview

### 2.1 What We're Building

**rustdb** - An embedded LSM-tree key-value database with ACID transactions.

### 2.2 Architecture Choice: LSM-Tree

**Why LSM-Tree over B-Tree:**

| Aspect | LSM-Tree | B-Tree |
|--------|----------|--------|
| Write performance | Excellent (sequential I/O) | Good (random I/O) |
| Read performance | Good (may check multiple levels) | Excellent (single tree traversal) |
| Space amplification | Higher (multiple copies during compaction) | Lower (in-place updates) |
| Write amplification | Lower | Higher |
| Implementation | More complex | Simpler |
| Industry adoption | RocksDB, LevelDB, Cassandra | SQLite, PostgreSQL |

LSM-trees are the industry standard for write-heavy and balanced workloads.

### 2.3 Key Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Storage engine | LSM-Tree | Industry standard, write-optimized |
| ACID compliance | Full with MVCC | Production-grade, demonstrates depth |
| Transaction isolation | Snapshot Isolation | Industry standard, prevents most anomalies |
| Concurrency | Multi-reader, multi-writer | Maximum concurrency |
| Key/value types | Raw bytes | Maximum flexibility |
| Target scale | 10-100GB | Real-world useful, manageable |
| Compaction | Leveled | Good read amplification |
| API style | Embedded library | No network overhead |
| Durability | Configurable fsync | Performance flexibility |
| Compression | Optional (LZ4/Snappy) | Reduces disk usage |

---

## 3. Functional Requirements

### 3.1 Core Operations

#### 3.1.1 Point Operations

```rust
// Basic CRUD operations
fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;
fn put(&self, key: &[u8], value: &[u8]) -> Result<()>;
fn delete(&self, key: &[u8]) -> Result<()>;

// Check existence without fetching value
fn contains(&self, key: &[u8]) -> Result<bool>;
```

#### 3.1.2 Range Operations

```rust
// Iterate over key range (inclusive start, exclusive end)
fn range(&self, start: &[u8], end: &[u8]) -> Result<Iterator>;

// Iterate with prefix
fn prefix(&self, prefix: &[u8]) -> Result<Iterator>;

// Iterate all keys
fn iter(&self) -> Result<Iterator>;

// Reverse iteration
fn range_rev(&self, start: &[u8], end: &[u8]) -> Result<Iterator>;
```

#### 3.1.3 Batch Operations

```rust
// Atomic batch write
let mut batch = WriteBatch::new();
batch.put(b"key1", b"value1");
batch.put(b"key2", b"value2");
batch.delete(b"key3");
db.write(batch)?;
```

### 3.2 Transaction Operations

#### 3.2.1 Transaction API

```rust
// Start a transaction
let txn = db.begin_transaction()?;

// Read within transaction (sees consistent snapshot)
let value = txn.get(b"key")?;

// Write within transaction (buffered until commit)
txn.put(b"key", b"new_value")?;

// Commit or rollback
txn.commit()?;  // Makes writes visible
// OR
txn.rollback()?;  // Discards writes
```

#### 3.2.2 Transaction Guarantees

| Property | Guarantee |
|----------|-----------|
| Atomicity | All writes in transaction succeed or none do |
| Consistency | Database moves from one valid state to another |
| Isolation | Snapshot Isolation - each txn sees consistent snapshot |
| Durability | Committed data survives crashes (configurable) |

#### 3.2.3 Snapshot Isolation Behavior

```
Time →
        T1 (read-only)              T2 (read-write)
        ─────────────────           ─────────────────
t=0     begin
t=1     read(A) = 100              begin
t=2                                 write(A, 200)
t=3     read(A) = 100              commit
t=4     read(A) = 100              ← T1 still sees old value!
t=5     end
```

Each transaction sees a consistent snapshot from its start time.

### 3.3 Database Lifecycle

```rust
// Open database (creates if not exists)
let db = Database::open("/path/to/db")?;

// Open with custom options
let db = Database::open_with_options("/path/to/db", Options {
    create_if_missing: true,
    compression: Compression::Lz4,
    max_memtable_size: 64 * 1024 * 1024,  // 64MB
    ..Default::default()
})?;

// Close gracefully
db.close()?;

// Or rely on Drop
drop(db);
```

### 3.4 Maintenance Operations

```rust
// Force compaction of all levels
db.compact()?;

// Get database statistics
let stats = db.stats();
println!("Disk usage: {} bytes", stats.disk_usage);
println!("Key count: {}", stats.key_count);

// Flush memtable to disk
db.flush()?;
```

---

## 4. Non-Functional Requirements

### 4.1 Performance Targets

| Metric | Target | Notes |
|--------|--------|-------|
| Point read latency | < 100μs (RAM), < 1ms (disk) | With bloom filters |
| Point write latency | < 10μs (to memtable) | WAL fsync configurable |
| Sequential scan | > 100 MB/s | Limited by disk |
| Concurrent readers | 64+ threads | Lock-free reads |
| Concurrent writers | 16+ threads | With transaction ordering |

### 4.2 Durability Guarantees

| Sync Mode | Guarantee | Performance |
|-----------|-----------|-------------|
| `SyncMode::Always` | Every write persisted | Slowest |
| `SyncMode::Batch` | Sync every N ms or N bytes | Balanced |
| `SyncMode::None` | OS decides when to sync | Fastest, may lose recent writes |

### 4.3 Resource Limits

| Resource | Default | Configurable |
|----------|---------|--------------|
| MemTable size | 64 MB | Yes |
| Block cache size | 256 MB | Yes |
| Max open files | 1000 | Yes |
| Max L0 files before stall | 12 | Yes |
| Write buffer count | 2 | Yes |

### 4.4 Reliability

- **Crash recovery**: Database recovers to consistent state after any crash
- **Corruption detection**: CRC checksums on all data
- **Graceful degradation**: Compaction can lag without blocking writes (until limits)
- **Atomic operations**: All visible operations are atomic

---

## 5. Technical Requirements

### 5.1 LSM-Tree Structure

```
                    WRITES
                       │
                       ▼
              ┌─────────────────┐
              │   Write Buffer  │  Batches writes for WAL
              └────────┬────────┘
                       │
                       ▼
              ┌─────────────────┐
              │       WAL       │  Durability (sequential append)
              └────────┬────────┘
                       │
                       ▼
              ┌─────────────────┐
              │    MemTable     │  In-memory sorted structure
              │   (Active)      │  Lock-free skip list
              └────────┬────────┘
                       │ When full
                       ▼
              ┌─────────────────┐
              │    MemTable     │  Immutable, being flushed
              │  (Immutable)    │
              └────────┬────────┘
                       │ Flush to disk
                       ▼
     Level 0: ┌───┐ ┌───┐ ┌───┐     Unsorted SSTables
              └───┘ └───┘ └───┘     (may overlap)
                       │
                       │ Compact
                       ▼
     Level 1: ┌─────────────────┐   Sorted, non-overlapping
              └─────────────────┘   10x size of L0
                       │
                       ▼
     Level 2: ┌─────────────────────────────────┐
              └─────────────────────────────────┘   10x size of L1
                       │
                       ▼
              ... more levels ...
```

### 5.2 MVCC Implementation

Each key-value pair stored with version:
```
┌──────────┬──────────┬───────────┬──────────┐
│ Key      │ Version  │ ValueType │ Value    │
│ (bytes)  │ (u64)    │ (u8)      │ (bytes)  │
└──────────┴──────────┴───────────┴──────────┘

ValueType:
  0 = Value (normal data)
  1 = Deletion (tombstone)
```

Versions are monotonically increasing sequence numbers:
- Each write gets the next sequence number
- Transactions read at their start sequence number
- Old versions garbage collected when no longer needed

### 5.3 WAL Format

```
┌────────────────────────────────────────────────┐
│ Record 1                                        │
├──────────┬──────────┬──────────┬───────────────┤
│ Length   │ Type     │ CRC32    │ Payload       │
│ (4 bytes)│ (1 byte) │ (4 bytes)│ (variable)    │
└──────────┴──────────┴──────────┴───────────────┘
│ Record 2                                        │
├──────────┬──────────┬──────────┬───────────────┤
│ ...      │ ...      │ ...      │ ...           │
└──────────┴──────────┴──────────┴───────────────┘

Record Types:
  1 = Full record (complete in this block)
  2 = First fragment
  3 = Middle fragment
  4 = Last fragment
```

### 5.4 SSTable Format

```
┌─────────────────────────────────────────────────────┐
│                    Data Blocks                       │
│  ┌─────────────────────────────────────────────┐    │
│  │ Block 0: sorted key-value pairs              │    │
│  │ [key1, value1] [key2, value2] ...           │    │
│  └─────────────────────────────────────────────┘    │
│  ┌─────────────────────────────────────────────┐    │
│  │ Block 1: sorted key-value pairs              │    │
│  └─────────────────────────────────────────────┘    │
│  ...                                                 │
├─────────────────────────────────────────────────────┤
│                   Meta Blocks                        │
│  ┌─────────────────────────────────────────────┐    │
│  │ Bloom Filter                                  │    │
│  └─────────────────────────────────────────────┘    │
│  ┌─────────────────────────────────────────────┐    │
│  │ Statistics                                    │    │
│  └─────────────────────────────────────────────┘    │
├─────────────────────────────────────────────────────┤
│                   Index Block                        │
│  [last_key_block_0, offset_0]                       │
│  [last_key_block_1, offset_1]                       │
│  ...                                                 │
├─────────────────────────────────────────────────────┤
│                     Footer                           │
│  [metaindex_offset] [index_offset] [magic_number]   │
└─────────────────────────────────────────────────────┘
```

### 5.5 Manifest (Version Control)

The manifest tracks which files are currently active:

```
MANIFEST-000001
├── VersionEdit { new_files: [(L0, file001.sst)] }
├── VersionEdit { new_files: [(L0, file002.sst)] }
├── VersionEdit {
│     deleted_files: [(L0, file001.sst), (L0, file002.sst)],
│     new_files: [(L1, file003.sst)]
│   }
└── ...
```

Recovery: replay manifest to reconstruct current file set.

---

## 6. Configuration Options

```rust
pub struct Options {
    // Storage
    pub create_if_missing: bool,        // Create DB if not exists
    pub error_if_exists: bool,          // Error if DB already exists
    pub paranoid_checks: bool,          // Extra verification

    // MemTable
    pub max_memtable_size: usize,       // Size before flush (default: 64MB)
    pub max_write_buffers: usize,       // Number of memtables (default: 2)

    // SSTable
    pub block_size: usize,              // Data block size (default: 4KB)
    pub compression: Compression,        // None, Lz4, Snappy
    pub bloom_filter_bits: usize,       // Bits per key (default: 10)

    // Compaction
    pub level_zero_file_limit: usize,   // Trigger compaction (default: 4)
    pub level_zero_slowdown: usize,     // Slow writes (default: 8)
    pub level_zero_stop: usize,         // Stop writes (default: 12)
    pub max_bytes_for_level_base: usize,// L1 size (default: 256MB)
    pub max_bytes_for_level_multiplier: usize, // Each level 10x (default: 10)

    // Cache
    pub block_cache_size: usize,        // Block cache (default: 256MB)

    // Durability
    pub sync_mode: SyncMode,            // WAL sync behavior

    // Concurrency
    pub max_background_compactions: usize, // Parallel compactions (default: 4)
    pub max_background_flushes: usize,     // Parallel flushes (default: 2)
}
```

---

## 7. Observability

### 7.1 Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `rustdb_reads_total` | Counter | Total read operations |
| `rustdb_writes_total` | Counter | Total write operations |
| `rustdb_read_latency_seconds` | Histogram | Read operation latency |
| `rustdb_write_latency_seconds` | Histogram | Write operation latency |
| `rustdb_bytes_read` | Counter | Bytes read from disk |
| `rustdb_bytes_written` | Counter | Bytes written to disk |
| `rustdb_memtable_size_bytes` | Gauge | Current memtable size |
| `rustdb_sstable_count` | Gauge | SSTables per level |
| `rustdb_compaction_duration_seconds` | Histogram | Compaction duration |
| `rustdb_wal_size_bytes` | Gauge | WAL file size |
| `rustdb_block_cache_hits` | Counter | Block cache hits |
| `rustdb_block_cache_misses` | Counter | Block cache misses |
| `rustdb_bloom_filter_hits` | Counter | Negative lookups avoided |
| `rustdb_transactions_active` | Gauge | Active transactions |
| `rustdb_transactions_committed` | Counter | Committed transactions |
| `rustdb_transactions_aborted` | Counter | Aborted transactions |

### 7.2 Logging

Structured JSON logging with levels:
- `ERROR`: Unrecoverable failures
- `WARN`: Recoverable issues (compaction lag, etc.)
- `INFO`: Major events (flush, compaction complete)
- `DEBUG`: Detailed operations
- `TRACE`: Very verbose (each read/write)

---

## 8. Error Handling

### 8.1 Error Categories

```rust
pub enum Error {
    // I/O errors
    Io(std::io::Error),

    // Corruption detected
    Corruption(String),

    // Transaction conflicts
    TransactionConflict,

    // Database is closed
    DatabaseClosed,

    // Configuration errors
    InvalidConfiguration(String),

    // Resource limits
    TooManyOpenFiles,
    WriteBufferFull,

    // Key/value size limits
    KeyTooLarge,
    ValueTooLarge,
}
```

### 8.2 Recovery Behavior

| Failure | Recovery |
|---------|----------|
| Crash during write | Replay WAL, discard incomplete records |
| Crash during flush | Complete flush or discard partial SSTable |
| Crash during compaction | Use old files, delete partial new files |
| Corrupted WAL record | Truncate at corruption, log warning |
| Corrupted SSTable | Mark file as bad, trigger repair |
| Missing file | Report corruption, attempt recovery |

---

## 9. Testing Requirements

### 9.1 Unit Tests

- MemTable operations (put, get, delete, iterate)
- SkipList correctness
- WAL write and recovery
- SSTable read and write
- Bloom filter accuracy
- Block compression/decompression
- MVCC version management
- Transaction isolation
- Iterator merging

### 9.2 Integration Tests

- Multi-threaded concurrent access
- Crash recovery scenarios
- Compaction correctness
- Large dataset handling
- Transaction conflict detection
- Iterator consistency during writes

### 9.3 Stress Tests

- High write throughput
- Many concurrent transactions
- Large keys/values
- Many small keys/values
- Random read/write patterns
- Sequential scan performance

### 9.4 Benchmarks

Compare against:
- RocksDB (industry standard)
- sled (Rust native)
- LMDB (B-tree based)

Benchmark scenarios:
- Sequential writes
- Random writes
- Sequential reads
- Random reads
- Mixed read/write
- Range scans

---

## 10. Out of Scope (v1.0)

The following are explicitly NOT included:
- Network/server mode
- Replication
- Sharding
- SQL query language
- Secondary indexes
- Column families
- TTL (time-to-live)
- Merge operators
- Custom comparators

These can be added in future versions.

---

## 11. Success Criteria

The project is complete when:

1. **Core Operations**: get/put/delete work correctly
2. **Transactions**: ACID properties hold under concurrent access
3. **Persistence**: Data survives process restart
4. **Recovery**: Database recovers from simulated crashes
5. **Performance**: Meets latency targets in benchmarks
6. **Concurrency**: Multi-reader/writer works without corruption
7. **Documentation**: API docs, architecture docs, README
8. **Testing**: Comprehensive test suite passes
9. **Benchmarks**: Comparison with RocksDB/sled exists
