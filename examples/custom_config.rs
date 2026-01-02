//! Custom configuration example for rustdb.
//!
//! Run with: cargo run --example custom_config

use rustdb::{Compression, Database, Options, SyncMode};
use std::time::Duration;
use tempfile::TempDir;

fn main() -> rustdb::Result<()> {
    println!("=== Custom Configuration Example ===\n");

    // Example 1: High-performance configuration (less durable)
    println!("--- High-Performance Config ---");
    {
        let dir = TempDir::new().expect("failed to create temp dir");

        let mut options = Options::default();
        // Larger memtable for fewer flushes
        options.max_memtable_size = 128 * 1024 * 1024; // 128MB
        // No sync on every write (data may be lost on crash)
        options.sync_mode = SyncMode::None;
        // Fast compression
        options.compression = Compression::Lz4;
        // Larger block cache
        options.block_cache_size = 512 * 1024 * 1024; // 512MB

        let db = Database::open_with_options(dir.path(), options)?;

        // Benchmark writes
        let start = std::time::Instant::now();
        for i in 0..10000 {
            let key = format!("perf:{:08}", i);
            db.put(key.as_bytes(), b"performance test value")?;
        }
        println!("  Wrote 10,000 keys in {:?}", start.elapsed());

        db.close()?;
    }

    // Example 2: Maximum durability configuration
    println!("\n--- Maximum Durability Config ---");
    {
        let dir = TempDir::new().expect("failed to create temp dir");

        let mut options = Options::default();
        // Sync on every write
        options.sync_mode = SyncMode::Always;
        // Enable paranoid checks
        options.paranoid_checks = true;

        let db = Database::open_with_options(dir.path(), options)?;

        // These writes are guaranteed durable
        db.put(b"critical:data", b"important value")?;
        println!("  Critical data stored with fsync on every write");

        db.close()?;
    }

    // Example 3: Memory-efficient configuration
    println!("\n--- Memory-Efficient Config ---");
    {
        let dir = TempDir::new().expect("failed to create temp dir");

        let mut options = Options::default();
        // Smaller memtable
        options.max_memtable_size = 16 * 1024 * 1024; // 16MB
        // Smaller block cache
        options.block_cache_size = 64 * 1024 * 1024; // 64MB
        // Aggressive compression
        options.compression = Compression::Snappy;

        let db = Database::open_with_options(dir.path(), options)?;

        for i in 0..1000 {
            let key = format!("mem:{:05}", i);
            let value = "x".repeat(1000);
            db.put(key.as_bytes(), value.as_bytes())?;
        }
        println!("  Stored 1,000 keys with memory-efficient settings");

        db.close()?;
    }

    // Example 4: Balanced configuration with interval sync
    println!("\n--- Balanced Config (Recommended) ---");
    {
        let dir = TempDir::new().expect("failed to create temp dir");

        let mut options = Options::default();
        // Sync every 100ms (balance between durability and performance)
        options.sync_mode = SyncMode::Interval {
            interval: Duration::from_millis(100),
        };
        // Good compression ratio
        options.compression = Compression::Lz4;
        // Reasonable cache size
        options.block_cache_size = 256 * 1024 * 1024; // 256MB

        let db = Database::open_with_options(dir.path(), options)?;

        for i in 0..5000 {
            let key = format!("balanced:{:05}", i);
            db.put(key.as_bytes(), b"balanced configuration value")?;
        }
        println!("  Stored 5,000 keys with balanced settings");

        db.close()?;
    }

    println!("\n=== Custom Configuration Example Complete ===");
    Ok(())
}
