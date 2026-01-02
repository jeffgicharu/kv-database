//! Benchmarks for rustdb performance.

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use rustdb::{Compression, Database, Options, WriteBatch};
use tempfile::TempDir;

/// Benchmark sequential writes.
fn bench_sequential_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("sequential_write");

    for size in [100, 1000, 10000].iter() {
        group.throughput(Throughput::Elements(*size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            b.iter_with_setup(
                || {
                    let dir = TempDir::new().unwrap();
                    let db = Database::open(dir.path()).unwrap();
                    (dir, db)
                },
                |(_dir, db)| {
                    for i in 0..size {
                        let key = format!("key{:08}", i);
                        let value = format!("value{:08}", i);
                        db.put(key.as_bytes(), value.as_bytes()).unwrap();
                    }
                    black_box(())
                },
            );
        });
    }

    group.finish();
}

/// Benchmark random writes.
fn bench_random_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("random_write");

    // Pre-generate random keys
    let keys: Vec<String> = (0..10000)
        .map(|i| format!("rkey{:08}", (i * 7919) % 100000)) // Pseudo-random distribution
        .collect();

    group.throughput(Throughput::Elements(10000));
    group.bench_function("10000_keys", |b| {
        b.iter_with_setup(
            || {
                let dir = TempDir::new().unwrap();
                let db = Database::open(dir.path()).unwrap();
                (dir, db, keys.clone())
            },
            |(_dir, db, keys)| {
                for key in keys {
                    db.put(key.as_bytes(), b"value").unwrap();
                }
                black_box(())
            },
        );
    });

    group.finish();
}

/// Benchmark sequential reads.
fn bench_sequential_read(c: &mut Criterion) {
    let mut group = c.benchmark_group("sequential_read");

    for size in [100, 1000, 10000].iter() {
        group.throughput(Throughput::Elements(*size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            b.iter_with_setup(
                || {
                    let dir = TempDir::new().unwrap();
                    let db = Database::open(dir.path()).unwrap();

                    // Pre-populate
                    for i in 0..size {
                        let key = format!("key{:08}", i);
                        let value = format!("value{:08}", i);
                        db.put(key.as_bytes(), value.as_bytes()).unwrap();
                    }
                    db.flush().unwrap();

                    (dir, db, size)
                },
                |(_dir, db, size)| {
                    for i in 0..size {
                        let key = format!("key{:08}", i);
                        let _ = black_box(db.get(key.as_bytes()).unwrap());
                    }
                },
            );
        });
    }

    group.finish();
}

/// Benchmark random reads.
fn bench_random_read(c: &mut Criterion) {
    let mut group = c.benchmark_group("random_read");

    let indices: Vec<usize> = (0..10000)
        .map(|i| (i * 7919) % 10000)
        .collect();

    group.throughput(Throughput::Elements(10000));
    group.bench_function("10000_keys", |b| {
        b.iter_with_setup(
            || {
                let dir = TempDir::new().unwrap();
                let db = Database::open(dir.path()).unwrap();

                for i in 0..10000 {
                    let key = format!("key{:08}", i);
                    db.put(key.as_bytes(), b"value").unwrap();
                }
                db.flush().unwrap();

                (dir, db, indices.clone())
            },
            |(_dir, db, indices)| {
                for i in indices {
                    let key = format!("key{:08}", i);
                    let _ = black_box(db.get(key.as_bytes()).unwrap());
                }
            },
        );
    });

    group.finish();
}

/// Benchmark write batch operations.
fn bench_write_batch(c: &mut Criterion) {
    let mut group = c.benchmark_group("write_batch");

    for batch_size in [10, 100, 1000].iter() {
        group.throughput(Throughput::Elements(*batch_size as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(batch_size),
            batch_size,
            |b, &batch_size| {
                b.iter_with_setup(
                    || {
                        let dir = TempDir::new().unwrap();
                        let db = Database::open(dir.path()).unwrap();
                        (dir, db)
                    },
                    |(_dir, db)| {
                        let mut batch = WriteBatch::new();
                        for i in 0..batch_size {
                            let key = format!("bkey{:08}", i);
                            let value = format!("bvalue{:08}", i);
                            batch.put(key.as_bytes(), value.as_bytes());
                        }
                        db.write(batch).unwrap();
                        black_box(())
                    },
                );
            },
        );
    }

    group.finish();
}

/// Benchmark range scans.
fn bench_range_scan(c: &mut Criterion) {
    let mut group = c.benchmark_group("range_scan");

    group.throughput(Throughput::Elements(1000));
    group.bench_function("1000_keys", |b| {
        b.iter_with_setup(
            || {
                let dir = TempDir::new().unwrap();
                let db = Database::open(dir.path()).unwrap();

                for i in 0..10000 {
                    let key = format!("scan{:08}", i);
                    db.put(key.as_bytes(), b"value").unwrap();
                }
                db.flush().unwrap();

                (dir, db)
            },
            |(_dir, db)| {
                let iter = db
                    .range(b"scan00001000".to_vec()..b"scan00002000".to_vec())
                    .unwrap();
                let count = iter.count();
                black_box(count)
            },
        );
    });

    group.finish();
}

/// Benchmark with different compression settings.
fn bench_compression(c: &mut Criterion) {
    let mut group = c.benchmark_group("compression");

    let compressions = [
        ("none", Compression::None),
        ("lz4", Compression::Lz4),
        ("snappy", Compression::Snappy),
    ];

    // Compressible data
    let value = "abcdefghij".repeat(100);

    for (name, compression) in compressions {
        group.bench_function(format!("{}_write_1000", name), |b| {
            b.iter_with_setup(
                || {
                    let dir = TempDir::new().unwrap();
                    let options = Options::default().compression(compression);
                    let db = Database::open_with_options(dir.path(), options).unwrap();
                    (dir, db, value.clone())
                },
                |(_dir, db, value)| {
                    for i in 0..1000 {
                        let key = format!("ckey{:08}", i);
                        db.put(key.as_bytes(), value.as_bytes()).unwrap();
                    }
                    db.flush().unwrap();
                    black_box(())
                },
            );
        });
    }

    group.finish();
}

/// Benchmark transaction operations.
fn bench_transactions(c: &mut Criterion) {
    let mut group = c.benchmark_group("transactions");

    group.bench_function("commit_10_ops", |b| {
        b.iter_with_setup(
            || {
                let dir = TempDir::new().unwrap();
                let db = Database::open(dir.path()).unwrap();
                (dir, db)
            },
            |(_dir, db)| {
                let txn = db.begin_transaction().unwrap();
                for i in 0..10 {
                    let key = format!("txn_key{}", i);
                    txn.put(key.as_bytes(), b"value").unwrap();
                }
                txn.commit().unwrap();
                black_box(())
            },
        );
    });

    group.finish();
}

/// Benchmark flush operations.
fn bench_flush(c: &mut Criterion) {
    let mut group = c.benchmark_group("flush");

    for count in [100, 1000, 5000].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(count), count, |b, &count| {
            b.iter_with_setup(
                || {
                    let dir = TempDir::new().unwrap();
                    let db = Database::open(dir.path()).unwrap();

                    for i in 0..count {
                        let key = format!("flush_key{:08}", i);
                        db.put(key.as_bytes(), b"value").unwrap();
                    }

                    (dir, db)
                },
                |(_dir, db)| {
                    db.flush().unwrap();
                    black_box(())
                },
            );
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_sequential_write,
    bench_random_write,
    bench_sequential_read,
    bench_random_read,
    bench_write_batch,
    bench_range_scan,
    bench_compression,
    bench_transactions,
    bench_flush,
);

criterion_main!(benches);
