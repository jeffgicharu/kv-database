// Example API Usage - What We're Building
// This file shows how the database will be used

use rustdb::{Database, Options, SyncMode, Compression, WriteBatch, Error};

fn main() -> Result<(), Error> {
    // ===========================================
    // Basic Usage
    // ===========================================

    // Open database with default options
    let db = Database::open("./my_data")?;

    // Simple put/get/delete
    db.put(b"hello", b"world")?;

    let value = db.get(b"hello")?;
    assert_eq!(value, Some(b"world".to_vec()));

    db.delete(b"hello")?;
    assert_eq!(db.get(b"hello")?, None);

    // ===========================================
    // Custom Options
    // ===========================================

    let db = Database::open_with_options("./my_data", Options {
        create_if_missing: true,
        compression: Compression::Lz4,
        max_memtable_size: 64 * 1024 * 1024,  // 64 MB
        sync_mode: SyncMode::Batch { interval_ms: 100 },
        bloom_filter_bits: 10,
        ..Default::default()
    })?;

    // ===========================================
    // Batch Writes (Atomic)
    // ===========================================

    let mut batch = WriteBatch::new();
    batch.put(b"user:1:name", b"Alice");
    batch.put(b"user:1:email", b"alice@example.com");
    batch.put(b"user:1:created", b"2024-01-15");
    batch.delete(b"user:1:temp_token");

    // All writes are atomic - either all succeed or none
    db.write(batch)?;

    // ===========================================
    // Range Queries
    // ===========================================

    // Store some data
    db.put(b"user:1", b"Alice")?;
    db.put(b"user:2", b"Bob")?;
    db.put(b"user:3", b"Charlie")?;
    db.put(b"user:4", b"Diana")?;
    db.put(b"order:1", b"Order 1")?;
    db.put(b"order:2", b"Order 2")?;

    // Iterate all keys with prefix "user:"
    for entry in db.prefix(b"user:")? {
        let (key, value) = entry?;
        println!("{:?} = {:?}",
            String::from_utf8_lossy(&key),
            String::from_utf8_lossy(&value));
    }
    // Output:
    // "user:1" = "Alice"
    // "user:2" = "Bob"
    // "user:3" = "Charlie"
    // "user:4" = "Diana"

    // Range query: user:2 to user:4 (exclusive end)
    for entry in db.range(b"user:2", b"user:4")? {
        let (key, value) = entry?;
        println!("{:?}", String::from_utf8_lossy(&key));
    }
    // Output:
    // "user:2"
    // "user:3"

    // Iterate all keys in reverse
    for entry in db.iter_rev()? {
        let (key, _value) = entry?;
        println!("{:?}", String::from_utf8_lossy(&key));
    }
    // Output:
    // "user:4"
    // "user:3"
    // "user:2"
    // "user:1"
    // "order:2"
    // "order:1"

    // ===========================================
    // Transactions (ACID)
    // ===========================================

    // Transfer $100 from account A to account B
    let txn = db.begin_transaction()?;

    // Read current balances
    let balance_a: i64 = txn.get(b"account:A")?
        .map(|v| i64::from_be_bytes(v.try_into().unwrap()))
        .unwrap_or(0);

    let balance_b: i64 = txn.get(b"account:B")?
        .map(|v| i64::from_be_bytes(v.try_into().unwrap()))
        .unwrap_or(0);

    // Update balances
    txn.put(b"account:A", &(balance_a - 100).to_be_bytes())?;
    txn.put(b"account:B", &(balance_b + 100).to_be_bytes())?;

    // Commit transaction - all writes become visible atomically
    txn.commit()?;

    // ===========================================
    // Snapshot Isolation Demo
    // ===========================================

    // Initial: key = "old_value"
    db.put(b"key", b"old_value")?;

    // Transaction 1 starts - sees snapshot at this point
    let txn1 = db.begin_transaction()?;

    // Transaction 2 modifies the key and commits
    let txn2 = db.begin_transaction()?;
    txn2.put(b"key", b"new_value")?;
    txn2.commit()?;

    // Transaction 1 still sees old value (snapshot isolation!)
    assert_eq!(txn1.get(b"key")?, Some(b"old_value".to_vec()));

    // After txn1 ends, new readers see new value
    assert_eq!(db.get(b"key")?, Some(b"new_value".to_vec()));

    // ===========================================
    // Transaction Conflict Handling
    // ===========================================

    // Two transactions writing to the same key
    let txn_a = db.begin_transaction()?;
    let txn_b = db.begin_transaction()?;

    txn_a.put(b"contested_key", b"value_from_A")?;
    txn_b.put(b"contested_key", b"value_from_B")?;

    // First commit wins
    txn_a.commit()?;

    // Second commit detects conflict
    match txn_b.commit() {
        Err(Error::TransactionConflict) => {
            println!("Transaction B aborted due to conflict");
            // Application can retry with new transaction
        }
        Ok(()) => unreachable!(),
        Err(e) => return Err(e),
    }

    // ===========================================
    // Read-Only Transactions (Efficient Snapshots)
    // ===========================================

    // Read-only transaction is cheaper - no write tracking
    let snapshot = db.snapshot();

    // Can read at this point-in-time even as writes continue
    let value = snapshot.get(b"key")?;

    // Writes to db don't affect snapshot
    db.put(b"key", b"newer_value")?;
    assert_eq!(snapshot.get(b"key")?, value);  // Still sees old value

    // ===========================================
    // Maintenance Operations
    // ===========================================

    // Force flush memtable to disk
    db.flush()?;

    // Trigger manual compaction
    db.compact()?;

    // Get statistics
    let stats = db.stats();
    println!("Database statistics:");
    println!("  Disk usage: {} bytes", stats.disk_size);
    println!("  Keys: {}", stats.num_keys);
    println!("  SSTables: L0={}, L1={}, L2={}",
        stats.sst_count[0], stats.sst_count[1], stats.sst_count[2]);
    println!("  MemTable size: {} bytes", stats.memtable_size);
    println!("  Block cache hit rate: {:.1}%", stats.cache_hit_rate * 100.0);

    // ===========================================
    // Graceful Shutdown
    // ===========================================

    // Close flushes data and releases resources
    db.close()?;

    // Or let Drop handle it
    // drop(db);

    Ok(())
}

// ===========================================
// Multi-threaded Access
// ===========================================

use std::thread;
use std::sync::Arc;

fn concurrent_example() -> Result<(), Error> {
    let db = Arc::new(Database::open("./concurrent_data")?);

    // Spawn multiple reader threads
    let mut handles = vec![];

    for i in 0..4 {
        let db = Arc::clone(&db);
        handles.push(thread::spawn(move || {
            for j in 0..1000 {
                let key = format!("key:{}", j);
                let _ = db.get(key.as_bytes());
            }
            println!("Reader {} finished", i);
        }));
    }

    // Spawn writer threads
    for i in 0..2 {
        let db = Arc::clone(&db);
        handles.push(thread::spawn(move || {
            for j in 0..1000 {
                let key = format!("writer{}:key:{}", i, j);
                let value = format!("value_{}", j);
                db.put(key.as_bytes(), value.as_bytes()).unwrap();
            }
            println!("Writer {} finished", i);
        }));
    }

    for handle in handles {
        handle.join().unwrap();
    }

    Ok(())
}

// ===========================================
// Error Handling
// ===========================================

fn error_handling_example() -> Result<(), Error> {
    let db = Database::open("./data")?;

    // Handle specific errors
    match db.put(b"key", b"value") {
        Ok(()) => println!("Write successful"),
        Err(Error::WriteBufferFull) => {
            println!("Write buffer full, waiting for flush...");
            db.flush()?;
            db.put(b"key", b"value")?;
        }
        Err(Error::Io(e)) => {
            eprintln!("I/O error: {}", e);
            return Err(Error::Io(e));
        }
        Err(e) => return Err(e),
    }

    Ok(())
}
