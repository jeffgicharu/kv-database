//! Concurrent access example for rustdb.
//!
//! Run with: cargo run --example concurrent

use rustdb::Database;
use std::sync::Arc;
use std::thread;
use std::time::Instant;
use tempfile::TempDir;

fn main() -> rustdb::Result<()> {
    let dir = TempDir::new().expect("failed to create temp dir");
    let db = Arc::new(Database::open(dir.path())?);

    println!("=== Concurrent Access Example ===\n");

    let num_writers = 4;
    let num_readers = 4;
    let ops_per_writer = 1000;
    let ops_per_reader = 2000;

    // Pre-populate some data for readers
    println!("Pre-populating 1000 keys...");
    for i in 0..1000 {
        let key = format!("preload:{:05}", i);
        let value = format!("value_{}", i);
        db.put(key.as_bytes(), value.as_bytes())?;
    }
    println!("Done.\n");

    let start = Instant::now();

    // Spawn writers
    let mut handles = vec![];

    for writer_id in 0..num_writers {
        let db = Arc::clone(&db);
        handles.push(thread::spawn(move || {
            for i in 0..ops_per_writer {
                let key = format!("writer{}:{:05}", writer_id, i);
                let value = format!("data_from_writer_{}", writer_id);
                db.put(key.as_bytes(), value.as_bytes())
                    .expect("write failed");
            }
            println!("Writer {} completed {} writes", writer_id, ops_per_writer);
        }));
    }

    // Spawn readers
    for reader_id in 0..num_readers {
        let db = Arc::clone(&db);
        handles.push(thread::spawn(move || {
            let mut found = 0;
            for i in 0..ops_per_reader {
                let key = format!("preload:{:05}", i % 1000);
                if db.get(key.as_bytes()).expect("read failed").is_some() {
                    found += 1;
                }
            }
            println!(
                "Reader {} completed {} reads ({} found)",
                reader_id, ops_per_reader, found
            );
        }));
    }

    // Wait for all threads
    for handle in handles {
        handle.join().expect("thread panicked");
    }

    let elapsed = start.elapsed();
    let total_ops = (num_writers * ops_per_writer) + (num_readers * ops_per_reader);

    println!("\n--- Summary ---");
    println!("Writers: {}", num_writers);
    println!("Readers: {}", num_readers);
    println!("Total operations: {}", total_ops);
    println!("Time: {:?}", elapsed);
    println!(
        "Throughput: {:.0} ops/sec",
        total_ops as f64 / elapsed.as_secs_f64()
    );

    // Verify data integrity
    println!("\n--- Verification ---");
    let mut verified = 0;
    for writer_id in 0..num_writers {
        for i in 0..ops_per_writer {
            let key = format!("writer{}:{:05}", writer_id, i);
            if db.get(key.as_bytes())?.is_some() {
                verified += 1;
            }
        }
    }
    println!(
        "Verified {} of {} written keys",
        verified,
        num_writers * ops_per_writer
    );

    println!("\n=== Concurrent Access Example Complete ===");
    Ok(())
}
