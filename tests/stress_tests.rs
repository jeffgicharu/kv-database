//! Stress tests for concurrency and durability.

use bytes::Bytes;
use rustdb::{Compression, Database, Options};
use std::sync::Arc;
use std::thread;
use tempfile::TempDir;

/// Test concurrent writers with many keys.
#[test]
fn stress_concurrent_writers() {
    let dir = TempDir::new().unwrap();
    let db = Arc::new(Database::open(dir.path()).unwrap());

    let num_threads = 8;
    let keys_per_thread = 1000;

    let handles: Vec<_> = (0..num_threads)
        .map(|t| {
            let db = Arc::clone(&db);
            thread::spawn(move || {
                for i in 0..keys_per_thread {
                    let key = format!("thread{:02}_key{:05}", t, i);
                    let value = format!("value_{}", i);
                    db.put(key.as_bytes(), value.as_bytes()).unwrap();
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    // Verify all keys exist
    for t in 0..num_threads {
        for i in 0..keys_per_thread {
            let key = format!("thread{:02}_key{:05}", t, i);
            let value = db.get(key.as_bytes()).unwrap();
            assert!(value.is_some(), "Missing key: {}", key);
        }
    }
}

/// Test concurrent readers and writers.
#[test]
fn stress_concurrent_read_write() {
    let dir = TempDir::new().unwrap();
    let db = Arc::new(Database::open(dir.path()).unwrap());

    // Pre-populate some data
    for i in 0..1000 {
        let key = format!("key{:05}", i);
        let value = format!("value_{}", i);
        db.put(key.as_bytes(), value.as_bytes()).unwrap();
    }

    let num_readers = 4;
    let num_writers = 4;
    let ops_per_thread = 500;

    let mut handles = vec![];

    // Spawn readers
    for _ in 0..num_readers {
        let db = Arc::clone(&db);
        handles.push(thread::spawn(move || {
            for i in 0..ops_per_thread {
                let key = format!("key{:05}", i % 1000);
                let _ = db.get(key.as_bytes());
            }
        }));
    }

    // Spawn writers
    for t in 0..num_writers {
        let db = Arc::clone(&db);
        handles.push(thread::spawn(move || {
            for i in 0..ops_per_thread {
                let key = format!("new_key_t{}_i{}", t, i);
                let value = format!("new_value_{}", i);
                db.put(key.as_bytes(), value.as_bytes()).unwrap();
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }
}

/// Test rapid open/close cycles.
#[test]
fn stress_open_close_cycles() {
    let dir = TempDir::new().unwrap();

    for cycle in 0..10 {
        let db = Database::open(dir.path()).unwrap();

        // Write some data
        for i in 0..100 {
            let key = format!("cycle{}_key{}", cycle, i);
            let value = format!("value_{}", i);
            db.put(key.as_bytes(), value.as_bytes()).unwrap();
        }

        // Read previous cycle's data
        if cycle > 0 {
            for i in 0..100 {
                let key = format!("cycle{}_key{}", cycle - 1, i);
                let value = db.get(key.as_bytes()).unwrap();
                assert!(value.is_some(), "Missing key from cycle {}", cycle - 1);
            }
        }

        db.close().unwrap();
    }

    // Final verification
    let db = Database::open(dir.path()).unwrap();
    for cycle in 0..10 {
        for i in 0..100 {
            let key = format!("cycle{}_key{}", cycle, i);
            let value = db.get(key.as_bytes()).unwrap();
            assert!(value.is_some(), "Missing key: {}", key);
        }
    }
}

/// Test large values.
#[test]
fn stress_large_values() {
    let dir = TempDir::new().unwrap();
    let db = Database::open(dir.path()).unwrap();

    let sizes = [1024, 4096, 16384, 65536, 262144]; // 1KB to 256KB

    for (i, &size) in sizes.iter().enumerate() {
        let key = format!("large_key_{}", i);
        let value = vec![b'x'; size];
        db.put(key.as_bytes(), &value).unwrap();
    }

    // Verify
    for (i, &size) in sizes.iter().enumerate() {
        let key = format!("large_key_{}", i);
        let value = db.get(key.as_bytes()).unwrap().unwrap();
        assert_eq!(value.len(), size);
        assert!(value.iter().all(|&b| b == b'x'));
    }
}

/// Test many small transactions.
#[test]
fn stress_many_transactions() {
    let dir = TempDir::new().unwrap();
    let db = Arc::new(Database::open(dir.path()).unwrap());

    let num_threads = 4;
    let txns_per_thread = 100;
    let ops_per_txn = 10;

    let handles: Vec<_> = (0..num_threads)
        .map(|t| {
            let db = Arc::clone(&db);
            thread::spawn(move || {
                for txn_id in 0..txns_per_thread {
                    let txn = db.begin_transaction().unwrap();

                    for op in 0..ops_per_txn {
                        let key = format!("t{}_txn{}_op{}", t, txn_id, op);
                        let value = format!("value_{}", op);
                        txn.put(key.as_bytes(), value.as_bytes()).unwrap();
                    }

                    txn.commit().unwrap();
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    // Verify all committed data
    for t in 0..num_threads {
        for txn_id in 0..txns_per_thread {
            for op in 0..ops_per_txn {
                let key = format!("t{}_txn{}_op{}", t, txn_id, op);
                let value = db.get(key.as_bytes()).unwrap();
                assert!(value.is_some(), "Missing key: {}", key);
            }
        }
    }
}

/// Test overwrite stress - same keys updated many times.
#[test]
fn stress_overwrites() {
    let dir = TempDir::new().unwrap();
    let db = Arc::new(Database::open(dir.path()).unwrap());

    let num_keys = 100;
    let updates_per_key = 100;

    let handles: Vec<_> = (0..4)
        .map(|t| {
            let db = Arc::clone(&db);
            thread::spawn(move || {
                for update in 0..updates_per_key {
                    for key_id in 0..num_keys {
                        let key = format!("shared_key_{}", key_id);
                        let value = format!("thread{}_update{}", t, update);
                        db.put(key.as_bytes(), value.as_bytes()).unwrap();
                    }
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    // Verify all keys exist (value will be from one of the threads)
    for key_id in 0..num_keys {
        let key = format!("shared_key_{}", key_id);
        let value = db.get(key.as_bytes()).unwrap();
        assert!(value.is_some(), "Missing key: {}", key);
    }
}

/// Test delete stress.
#[test]
fn stress_deletes() {
    let dir = TempDir::new().unwrap();
    let db = Database::open(dir.path()).unwrap();

    // Insert keys
    for i in 0..1000 {
        let key = format!("del_key_{}", i);
        db.put(key.as_bytes(), b"value").unwrap();
    }

    // Delete odd keys
    for i in (1..1000).step_by(2) {
        let key = format!("del_key_{}", i);
        db.delete(key.as_bytes()).unwrap();
    }

    // Verify
    for i in 0..1000 {
        let key = format!("del_key_{}", i);
        let value = db.get(key.as_bytes()).unwrap();
        if i % 2 == 0 {
            assert!(value.is_some(), "Even key {} should exist", i);
        } else {
            assert!(value.is_none(), "Odd key {} should be deleted", i);
        }
    }
}

/// Test with compression enabled.
#[test]
fn stress_with_compression() {
    let dir = TempDir::new().unwrap();
    let mut options = Options::default();
    options.compression = Compression::Lz4;
    let db = Database::open_with_options(dir.path(), options).unwrap();

    // Write compressible data (repeated patterns)
    for i in 0..500 {
        let key = format!("comp_key_{}", i);
        let value = "abcdefghij".repeat(100); // Highly compressible
        db.put(key.as_bytes(), value.as_bytes()).unwrap();
    }

    // Force flush to create SSTable with compression
    db.flush().unwrap();

    // Verify data
    for i in 0..500 {
        let key = format!("comp_key_{}", i);
        let value = db.get(key.as_bytes()).unwrap().unwrap();
        let expected = "abcdefghij".repeat(100);
        assert_eq!(value, Bytes::from(expected));
    }
}
