//! Integration tests for complete database workflows.

use bytes::Bytes;
use rustdb::{Compression, Database, Options, SyncMode, WriteBatch};
use std::sync::Arc;
use std::thread;
use tempfile::TempDir;

/// Test complete CRUD workflow.
#[test]
fn integration_crud_workflow() {
    let dir = TempDir::new().unwrap();
    let db = Database::open(dir.path()).unwrap();

    // Create
    db.put(b"user:1", b"Alice").unwrap();
    db.put(b"user:2", b"Bob").unwrap();
    db.put(b"user:3", b"Charlie").unwrap();

    // Read
    assert_eq!(db.get(b"user:1").unwrap(), Some(Bytes::from("Alice")));
    assert_eq!(db.get(b"user:2").unwrap(), Some(Bytes::from("Bob")));
    assert_eq!(db.get(b"user:3").unwrap(), Some(Bytes::from("Charlie")));

    // Update
    db.put(b"user:2", b"Bobby").unwrap();
    assert_eq!(db.get(b"user:2").unwrap(), Some(Bytes::from("Bobby")));

    // Delete
    db.delete(b"user:3").unwrap();
    assert_eq!(db.get(b"user:3").unwrap(), None);

    // Verify remaining
    assert_eq!(db.get(b"user:1").unwrap(), Some(Bytes::from("Alice")));
    assert_eq!(db.get(b"user:2").unwrap(), Some(Bytes::from("Bobby")));
}

/// Test write batch atomicity.
#[test]
fn integration_write_batch_atomicity() {
    let dir = TempDir::new().unwrap();
    let db = Database::open(dir.path()).unwrap();

    // Write batch with multiple operations
    let mut batch = WriteBatch::new();
    batch.put(&b"account:1"[..], &b"1000"[..]);
    batch.put(&b"account:2"[..], &b"500"[..]);
    batch.put(&b"account:3"[..], &b"250"[..]);
    db.write(batch).unwrap();

    // All should be visible
    assert_eq!(db.get(b"account:1").unwrap(), Some(Bytes::from("1000")));
    assert_eq!(db.get(b"account:2").unwrap(), Some(Bytes::from("500")));
    assert_eq!(db.get(b"account:3").unwrap(), Some(Bytes::from("250")));
}

/// Test transaction isolation.
#[test]
fn integration_transaction_isolation() {
    let dir = TempDir::new().unwrap();
    let db = Arc::new(Database::open(dir.path()).unwrap());

    // Initial value
    db.put(b"counter", b"0").unwrap();

    // Start transaction 1 - reads initial value
    let txn1 = db.begin_transaction().unwrap();
    let v1 = txn1.get(b"counter").unwrap();
    assert_eq!(v1, Some(Bytes::from("0")));

    // Transaction 2 updates and commits
    let txn2 = db.begin_transaction().unwrap();
    txn2.put(b"counter", b"100").unwrap();
    txn2.commit().unwrap();

    // Transaction 1 still sees old value (snapshot isolation)
    let v1_again = txn1.get(b"counter").unwrap();
    assert_eq!(v1_again, Some(Bytes::from("0")));

    // After txn1 ends, new reads see updated value
    drop(txn1);
    assert_eq!(db.get(b"counter").unwrap(), Some(Bytes::from("100")));
}

/// Test persistence across restarts.
#[test]
fn integration_persistence() {
    let dir = TempDir::new().unwrap();

    // Write data
    {
        let db = Database::open(dir.path()).unwrap();
        for i in 0..100 {
            let key = format!("persist_key_{}", i);
            let value = format!("persist_value_{}", i);
            db.put(key.as_bytes(), value.as_bytes()).unwrap();
        }
        db.close().unwrap();
    }

    // Reopen and verify
    {
        let db = Database::open(dir.path()).unwrap();
        for i in 0..100 {
            let key = format!("persist_key_{}", i);
            let expected = format!("persist_value_{}", i);
            let value = db.get(key.as_bytes()).unwrap();
            assert_eq!(value, Some(Bytes::from(expected)));
        }
    }
}

/// Test WAL recovery after crash simulation.
#[test]
fn integration_wal_recovery() {
    let dir = TempDir::new().unwrap();

    // Write without explicit flush (data in WAL)
    {
        let db = Database::open(dir.path()).unwrap();
        db.put(b"wal_key_1", b"wal_value_1").unwrap();
        db.put(b"wal_key_2", b"wal_value_2").unwrap();
        // Drop without close - simulates crash
    }

    // Reopen - should recover from WAL
    {
        let db = Database::open(dir.path()).unwrap();
        assert_eq!(db.get(b"wal_key_1").unwrap(), Some(Bytes::from("wal_value_1")));
        assert_eq!(db.get(b"wal_key_2").unwrap(), Some(Bytes::from("wal_value_2")));
    }
}

/// Test compaction workflow.
#[test]
fn integration_compaction() {
    let dir = TempDir::new().unwrap();
    let mut options = Options::default();
    options.max_memtable_size = 32 * 1024; // Small to trigger flushes
    let db = Database::open_with_options(dir.path(), options).unwrap();

    // Write enough data to create multiple SSTables
    for i in 0..200 {
        let key = format!("compact_key_{:05}", i);
        let value = vec![b'v'; 100];
        db.put(key.as_bytes(), &value).unwrap();
    }

    // Force flush
    db.flush().unwrap();

    // Verify all data still accessible
    for i in 0..200 {
        let key = format!("compact_key_{:05}", i);
        let value = db.get(key.as_bytes()).unwrap();
        assert!(value.is_some(), "Missing key after flush: {}", key);
        assert_eq!(value.unwrap().len(), 100);
    }
}

/// Test iterator workflow.
#[test]
fn integration_iterator() {
    let dir = TempDir::new().unwrap();
    let db = Database::open(dir.path()).unwrap();

    // Insert data with prefix pattern
    let prefixes = ["users:", "orders:", "products:"];
    for prefix in &prefixes {
        for i in 0..10 {
            let key = format!("{}{:03}", prefix, i);
            let value = format!("value_{}", i);
            db.put(key.as_bytes(), value.as_bytes()).unwrap();
        }
    }

    // Iterate all keys
    let iter = db.iter().unwrap();
    let all_keys: Vec<_> = iter.map(|(k, _)| k).collect();
    assert_eq!(all_keys.len(), 30);

    // Prefix iteration
    let user_iter = db.prefix_iter(b"users:").unwrap();
    let user_keys: Vec<_> = user_iter.map(|(k, _)| k).collect();
    assert_eq!(user_keys.len(), 10);
    assert!(user_keys.iter().all(|k| k.starts_with(b"users:")));

    // Range iteration
    let range_iter = db.range(b"orders:000", b"orders:005").unwrap();
    let range_keys: Vec<_> = range_iter.map(|(k, _)| k).collect();
    assert_eq!(range_keys.len(), 5);
}

/// Test with different compression options.
#[test]
fn integration_compression_options() {
    // Test with LZ4
    {
        let dir = TempDir::new().unwrap();
        let mut options = Options::default();
        options.compression = Compression::Lz4;
        let db = Database::open_with_options(dir.path(), options).unwrap();

        db.put(b"lz4_key", b"lz4_value").unwrap();
        db.flush().unwrap();
        assert_eq!(db.get(b"lz4_key").unwrap(), Some(Bytes::from("lz4_value")));
    }

    // Test with Snappy
    {
        let dir = TempDir::new().unwrap();
        let mut options = Options::default();
        options.compression = Compression::Snappy;
        let db = Database::open_with_options(dir.path(), options).unwrap();

        db.put(b"snappy_key", b"snappy_value").unwrap();
        db.flush().unwrap();
        assert_eq!(db.get(b"snappy_key").unwrap(), Some(Bytes::from("snappy_value")));
    }

    // Test with no compression
    {
        let dir = TempDir::new().unwrap();
        let mut options = Options::default();
        options.compression = Compression::None;
        let db = Database::open_with_options(dir.path(), options).unwrap();

        db.put(b"none_key", b"none_value").unwrap();
        db.flush().unwrap();
        assert_eq!(db.get(b"none_key").unwrap(), Some(Bytes::from("none_value")));
    }
}

/// Test concurrent transactions with no conflicts.
#[test]
fn integration_concurrent_transactions_no_conflict() {
    let dir = TempDir::new().unwrap();
    let db = Arc::new(Database::open(dir.path()).unwrap());

    let handles: Vec<_> = (0..4)
        .map(|t| {
            let db = Arc::clone(&db);
            thread::spawn(move || {
                let txn = db.begin_transaction().unwrap();

                // Each thread writes to different keys
                for i in 0..10 {
                    let key = format!("thread{}_key{}", t, i);
                    let value = format!("thread{}_value{}", t, i);
                    txn.put(key.as_bytes(), value.as_bytes()).unwrap();
                }

                txn.commit().unwrap();
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    // Verify all data
    for t in 0..4 {
        for i in 0..10 {
            let key = format!("thread{}_key{}", t, i);
            let expected = format!("thread{}_value{}", t, i);
            assert_eq!(db.get(key.as_bytes()).unwrap(), Some(Bytes::from(expected)));
        }
    }
}

/// Test database statistics.
#[test]
fn integration_statistics() {
    let dir = TempDir::new().unwrap();
    let db = Database::open(dir.path()).unwrap();

    // Perform operations
    for i in 0..100 {
        let key = format!("stats_key_{}", i);
        db.put(key.as_bytes(), b"value").unwrap();
    }

    for i in 0..50 {
        let key = format!("stats_key_{}", i);
        let _ = db.get(key.as_bytes());
    }

    db.flush().unwrap();

    // Get stats
    let stats = db.stats();
    assert!(stats.memtable_size >= 0);
    assert!(stats.sequence > 0);
}

/// Test sync modes.
#[test]
fn integration_sync_modes() {
    // SyncMode::Always
    {
        let dir = TempDir::new().unwrap();
        let mut options = Options::default();
        options.sync_mode = SyncMode::Always;
        let db = Database::open_with_options(dir.path(), options).unwrap();
        db.put(b"sync_always", b"value").unwrap();
        assert_eq!(db.get(b"sync_always").unwrap(), Some(Bytes::from("value")));
    }

    // SyncMode::None (faster but less durable)
    {
        let dir = TempDir::new().unwrap();
        let mut options = Options::default();
        options.sync_mode = SyncMode::None;
        let db = Database::open_with_options(dir.path(), options).unwrap();
        db.put(b"sync_none", b"value").unwrap();
        assert_eq!(db.get(b"sync_none").unwrap(), Some(Bytes::from("value")));
    }
}

/// Test edge cases with empty and binary data.
#[test]
fn integration_edge_cases() {
    let dir = TempDir::new().unwrap();
    let db = Database::open(dir.path()).unwrap();

    // Empty value
    db.put(b"empty_value", b"").unwrap();
    assert_eq!(db.get(b"empty_value").unwrap(), Some(Bytes::new()));

    // Binary data
    let binary_key = vec![0x00, 0x01, 0x02, 0xFF, 0xFE, 0xFD];
    let binary_value: Vec<u8> = vec![0xFF; 256];
    db.put(&binary_key, &binary_value).unwrap();
    assert_eq!(db.get(&binary_key).unwrap(), Some(Bytes::from(binary_value)));

    // Long key
    let long_key = "k".repeat(1000);
    db.put(long_key.as_bytes(), b"long_key_value").unwrap();
    assert_eq!(db.get(long_key.as_bytes()).unwrap(), Some(Bytes::from("long_key_value")));
}
