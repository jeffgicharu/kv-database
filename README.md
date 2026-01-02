# rustdb

An embedded LSM-tree key-value database engine with ACID transactions, written in Rust.

## Features

- **LSM-Tree Storage** - Write-optimized storage with MemTable, immutable MemTables, and multi-level SSTables
- **ACID Transactions** - Full ACID compliance with MVCC and snapshot isolation
- **Write-Ahead Logging** - Durable writes with configurable sync modes
- **Background Compaction** - Automatic level-based compaction with garbage collection
- **Compression** - Optional LZ4 or Snappy compression for data blocks
- **Caching** - LRU block cache and table cache for read performance
- **Range Queries** - Efficient iteration with prefix and range scans
- **Metrics** - Prometheus-compatible metrics for monitoring

## Quick Start

```rust
use rustdb::Database;

fn main() -> rustdb::Result<()> {
    // Open database
    let db = Database::open("./my_data")?;

    // Basic operations
    db.put(b"hello", b"world")?;

    if let Some(value) = db.get(b"hello")? {
        println!("Got: {}", String::from_utf8_lossy(&value));
    }

    db.delete(b"hello")?;

    Ok(())
}
```

## Transactions

```rust
use rustdb::Database;
use std::sync::Arc;

fn main() -> rustdb::Result<()> {
    let db = Arc::new(Database::open("./my_data")?);

    let txn = db.begin_transaction()?;

    txn.put(b"key1", b"value1")?;
    txn.put(b"key2", b"value2")?;

    // Reads within transaction see uncommitted writes
    let val = txn.get(b"key1")?;

    txn.commit()?; // Or txn.rollback()?

    Ok(())
}
```

## Range Queries

```rust
use rustdb::Database;
use std::sync::Arc;

fn main() -> rustdb::Result<()> {
    let db = Arc::new(Database::open("./my_data")?);

    // Iterate all keys
    for (key, value) in db.iter()? {
        println!("{:?} = {:?}", key, value);
    }

    // Prefix iteration
    for (key, value) in db.prefix_iter(b"user:")? {
        println!("{:?} = {:?}", key, value);
    }

    // Range iteration
    for (key, value) in db.range(b"a", b"z")? {
        println!("{:?} = {:?}", key, value);
    }

    Ok(())
}
```

## Configuration

```rust
use rustdb::{Database, Options, Compression, SyncMode};
use std::time::Duration;

fn main() -> rustdb::Result<()> {
    let mut options = Options::default();

    // Storage
    options.max_memtable_size = 64 * 1024 * 1024;  // 64MB memtable
    options.compression = Compression::Lz4;         // LZ4 compression

    // Durability
    options.sync_mode = SyncMode::Interval {
        interval: Duration::from_millis(100),
    };

    // Cache
    options.block_cache_size = 256 * 1024 * 1024;  // 256MB block cache

    let db = Database::open_with_options("./my_data", options)?;

    Ok(())
}
```

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                      Database                            │
├─────────────────────────────────────────────────────────┤
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────┐  │
│  │  MemTable   │  │  Immutable  │  │  Transaction    │  │
│  │ (Skip List) │  │  MemTables  │  │  Manager        │  │
│  └─────────────┘  └─────────────┘  └─────────────────┘  │
├─────────────────────────────────────────────────────────┤
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────┐  │
│  │    WAL      │  │  Version    │  │   Compaction    │  │
│  │  (Durability)│  │    Set      │  │   (Background)  │  │
│  └─────────────┘  └─────────────┘  └─────────────────┘  │
├─────────────────────────────────────────────────────────┤
│  ┌─────────────────────────────────────────────────────┐│
│  │                    SSTable Files                     ││
│  │  Level 0: [SST] [SST] [SST]                         ││
│  │  Level 1: [SST] [SST]                               ││
│  │  Level 2: [SST]                                     ││
│  └─────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────┘
```

### Components

| Component | Description |
|-----------|-------------|
| **MemTable** | In-memory skip list for recent writes |
| **WAL** | Write-ahead log for crash recovery |
| **SSTable** | Sorted string table files with bloom filters |
| **VersionSet** | Tracks active files and supports atomic updates |
| **Compaction** | Merges SSTables to reclaim space and reduce read amplification |
| **BlockCache** | LRU cache for decompressed data blocks |
| **TableCache** | LRU cache for open SSTable file handles |

## Benchmarks

Run benchmarks with:

```bash
cargo bench
```

Benchmarks include:
- Sequential and random write throughput
- Sequential and random read throughput
- Range scan performance
- Write batch performance
- Compression comparison (None vs LZ4 vs Snappy)
- Transaction commit overhead

## Examples

```bash
cargo run --example basic           # Simple get/put/delete
cargo run --example transactions    # ACID transactions
cargo run --example iterators       # Range queries
cargo run --example concurrent      # Multi-threaded access
cargo run --example custom_config   # Configuration options
```

## Testing

```bash
# Run all tests
cargo test

# Run specific test suites
cargo test --test integration_tests
cargo test --test stress_tests
```

The test suite includes:
- **246 unit tests** - Core functionality
- **12 integration tests** - End-to-end workflows
- **8 stress tests** - Concurrency and durability

## Project Structure

```
src/
├── lib.rs              # Public API exports
├── db.rs               # Database implementation
├── memtable/           # In-memory storage (skip list)
├── wal/                # Write-ahead logging
├── sstable/            # SSTable format (blocks, bloom filters, compression)
├── version/            # Version management and manifest
├── compaction/         # Background compaction
├── transaction/        # MVCC transactions
├── iterator/           # Range queries and iteration
├── cache/              # LRU caching (block + table)
├── metrics/            # Prometheus-compatible metrics
├── options.rs          # Configuration
├── error.rs            # Error types
├── types.rs            # Core types (InternalKey, WriteBatch)
└── util/               # Utilities (encoding, checksums)

tests/
├── integration_tests.rs  # End-to-end tests
└── stress_tests.rs       # Concurrency tests

benches/
└── benchmark.rs          # Performance benchmarks

examples/
├── basic.rs              # Simple usage
├── transactions.rs       # Transaction example
├── iterators.rs          # Range query example
├── concurrent.rs         # Multi-threaded example
└── custom_config.rs      # Configuration example
```

## Inspiration

This project draws inspiration from:
- [LevelDB](https://github.com/google/leveldb) - Google's LSM-tree implementation
- [RocksDB](https://github.com/facebook/rocksdb) - Facebook's LevelDB fork
- [redb](https://github.com/cberner/redb) - Pure Rust embedded database
- [agatedb](https://github.com/tikv/agatedb) - TiKV's experimental storage engine

## License

MIT
