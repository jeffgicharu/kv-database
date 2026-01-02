//! Database-specific metrics.
//!
//! Comprehensive metrics for monitoring database operations,
//! internal state, and performance.

use std::fmt;
use std::sync::Arc;

use super::collector::{Counter, Gauge, Histogram};

/// Database operation metrics.
#[derive(Debug, Default)]
pub struct OperationMetrics {
    // Read operations
    /// Total read operations.
    pub reads: Counter,
    /// Read latency histogram (microseconds).
    pub read_latency: Histogram,
    /// Bytes read.
    pub bytes_read: Counter,

    // Write operations
    /// Total write operations.
    pub writes: Counter,
    /// Write latency histogram (microseconds).
    pub write_latency: Histogram,
    /// Bytes written.
    pub bytes_written: Counter,

    // Delete operations
    /// Total delete operations.
    pub deletes: Counter,

    // Batch operations
    /// Total batch writes.
    pub batch_writes: Counter,
    /// Batch size histogram (number of operations).
    pub batch_size: Histogram,

    // Transaction operations
    /// Transactions started.
    pub txn_begin: Counter,
    /// Transactions committed.
    pub txn_commit: Counter,
    /// Transactions rolled back.
    pub txn_rollback: Counter,
    /// Transaction conflicts.
    pub txn_conflicts: Counter,

    // Iterator operations
    /// Iterator seeks.
    pub iter_seeks: Counter,
    /// Iterator nexts.
    pub iter_nexts: Counter,
}

impl OperationMetrics {
    /// Create new operation metrics.
    pub fn new() -> Self {
        Self::default()
    }
}

impl Clone for OperationMetrics {
    fn clone(&self) -> Self {
        Self {
            reads: self.reads.clone(),
            read_latency: self.read_latency.clone(),
            bytes_read: self.bytes_read.clone(),
            writes: self.writes.clone(),
            write_latency: self.write_latency.clone(),
            bytes_written: self.bytes_written.clone(),
            deletes: self.deletes.clone(),
            batch_writes: self.batch_writes.clone(),
            batch_size: self.batch_size.clone(),
            txn_begin: self.txn_begin.clone(),
            txn_commit: self.txn_commit.clone(),
            txn_rollback: self.txn_rollback.clone(),
            txn_conflicts: self.txn_conflicts.clone(),
            iter_seeks: self.iter_seeks.clone(),
            iter_nexts: self.iter_nexts.clone(),
        }
    }
}

/// Internal database state metrics.
#[derive(Debug, Default)]
pub struct InternalMetrics {
    // MemTable metrics
    /// Current memtable size in bytes.
    pub memtable_size: Gauge,
    /// Number of immutable memtables.
    pub imm_memtable_count: Gauge,

    // SSTable metrics
    /// Number of SSTables per level (7 levels).
    pub sstable_count: [Gauge; 7],
    /// Size of SSTables per level in bytes.
    pub sstable_size: [Gauge; 7],

    // Compaction metrics
    /// Compactions in progress.
    pub compactions_running: Gauge,
    /// Total compactions completed.
    pub compactions_total: Counter,
    /// Compaction bytes read.
    pub compaction_bytes_read: Counter,
    /// Compaction bytes written.
    pub compaction_bytes_written: Counter,

    // Write stall metrics
    /// Current write stall status (0 = no stall, 1 = stalled).
    pub write_stall: Gauge,
    /// Total write stall count.
    pub write_stalls_total: Counter,

    // WAL metrics
    /// Current WAL size in bytes.
    pub wal_size: Gauge,
    /// Total WAL syncs.
    pub wal_syncs: Counter,

    // Cache metrics
    /// Block cache hits.
    pub block_cache_hits: Counter,
    /// Block cache misses.
    pub block_cache_misses: Counter,
    /// Table cache hits.
    pub table_cache_hits: Counter,
    /// Table cache misses.
    pub table_cache_misses: Counter,
}

impl InternalMetrics {
    /// Create new internal metrics.
    pub fn new() -> Self {
        Self::default()
    }

    /// Get block cache hit rate.
    pub fn block_cache_hit_rate(&self) -> f64 {
        let hits = self.block_cache_hits.get();
        let misses = self.block_cache_misses.get();
        let total = hits + misses;
        if total == 0 {
            0.0
        } else {
            hits as f64 / total as f64
        }
    }

    /// Get table cache hit rate.
    pub fn table_cache_hit_rate(&self) -> f64 {
        let hits = self.table_cache_hits.get();
        let misses = self.table_cache_misses.get();
        let total = hits + misses;
        if total == 0 {
            0.0
        } else {
            hits as f64 / total as f64
        }
    }
}

impl Clone for InternalMetrics {
    fn clone(&self) -> Self {
        Self {
            memtable_size: self.memtable_size.clone(),
            imm_memtable_count: self.imm_memtable_count.clone(),
            sstable_count: [
                self.sstable_count[0].clone(),
                self.sstable_count[1].clone(),
                self.sstable_count[2].clone(),
                self.sstable_count[3].clone(),
                self.sstable_count[4].clone(),
                self.sstable_count[5].clone(),
                self.sstable_count[6].clone(),
            ],
            sstable_size: [
                self.sstable_size[0].clone(),
                self.sstable_size[1].clone(),
                self.sstable_size[2].clone(),
                self.sstable_size[3].clone(),
                self.sstable_size[4].clone(),
                self.sstable_size[5].clone(),
                self.sstable_size[6].clone(),
            ],
            compactions_running: self.compactions_running.clone(),
            compactions_total: self.compactions_total.clone(),
            compaction_bytes_read: self.compaction_bytes_read.clone(),
            compaction_bytes_written: self.compaction_bytes_written.clone(),
            write_stall: self.write_stall.clone(),
            write_stalls_total: self.write_stalls_total.clone(),
            wal_size: self.wal_size.clone(),
            wal_syncs: self.wal_syncs.clone(),
            block_cache_hits: self.block_cache_hits.clone(),
            block_cache_misses: self.block_cache_misses.clone(),
            table_cache_hits: self.table_cache_hits.clone(),
            table_cache_misses: self.table_cache_misses.clone(),
        }
    }
}

/// All database metrics combined.
#[derive(Clone)]
pub struct DbMetrics {
    /// Operation metrics.
    pub ops: Arc<OperationMetrics>,
    /// Internal metrics.
    pub internal: Arc<InternalMetrics>,
}

impl Default for DbMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl DbMetrics {
    /// Create new database metrics.
    pub fn new() -> Self {
        Self {
            ops: Arc::new(OperationMetrics::new()),
            internal: Arc::new(InternalMetrics::new()),
        }
    }

    /// Format as Prometheus metrics.
    pub fn to_prometheus(&self) -> String {
        let mut output = String::new();

        // Operation metrics
        output.push_str(&format!("# HELP rustdb_reads_total Total read operations\n"));
        output.push_str(&format!("# TYPE rustdb_reads_total counter\n"));
        output.push_str(&format!("rustdb_reads_total {}\n\n", self.ops.reads.get()));

        output.push_str(&format!("# HELP rustdb_writes_total Total write operations\n"));
        output.push_str(&format!("# TYPE rustdb_writes_total counter\n"));
        output.push_str(&format!("rustdb_writes_total {}\n\n", self.ops.writes.get()));

        output.push_str(&format!("# HELP rustdb_deletes_total Total delete operations\n"));
        output.push_str(&format!("# TYPE rustdb_deletes_total counter\n"));
        output.push_str(&format!("rustdb_deletes_total {}\n\n", self.ops.deletes.get()));

        output.push_str(&format!("# HELP rustdb_bytes_read_total Total bytes read\n"));
        output.push_str(&format!("# TYPE rustdb_bytes_read_total counter\n"));
        output.push_str(&format!("rustdb_bytes_read_total {}\n\n", self.ops.bytes_read.get()));

        output.push_str(&format!("# HELP rustdb_bytes_written_total Total bytes written\n"));
        output.push_str(&format!("# TYPE rustdb_bytes_written_total counter\n"));
        output.push_str(&format!("rustdb_bytes_written_total {}\n\n", self.ops.bytes_written.get()));

        // Transaction metrics
        output.push_str(&format!("# HELP rustdb_txn_begin_total Transactions started\n"));
        output.push_str(&format!("# TYPE rustdb_txn_begin_total counter\n"));
        output.push_str(&format!("rustdb_txn_begin_total {}\n\n", self.ops.txn_begin.get()));

        output.push_str(&format!("# HELP rustdb_txn_commit_total Transactions committed\n"));
        output.push_str(&format!("# TYPE rustdb_txn_commit_total counter\n"));
        output.push_str(&format!("rustdb_txn_commit_total {}\n\n", self.ops.txn_commit.get()));

        output.push_str(&format!("# HELP rustdb_txn_conflicts_total Transaction conflicts\n"));
        output.push_str(&format!("# TYPE rustdb_txn_conflicts_total counter\n"));
        output.push_str(&format!("rustdb_txn_conflicts_total {}\n\n", self.ops.txn_conflicts.get()));

        // Internal metrics
        output.push_str(&format!("# HELP rustdb_memtable_size_bytes Current memtable size\n"));
        output.push_str(&format!("# TYPE rustdb_memtable_size_bytes gauge\n"));
        output.push_str(&format!("rustdb_memtable_size_bytes {}\n\n", self.internal.memtable_size.get()));

        output.push_str(&format!("# HELP rustdb_imm_memtable_count Number of immutable memtables\n"));
        output.push_str(&format!("# TYPE rustdb_imm_memtable_count gauge\n"));
        output.push_str(&format!("rustdb_imm_memtable_count {}\n\n", self.internal.imm_memtable_count.get()));

        // SSTable counts per level
        output.push_str(&format!("# HELP rustdb_sstable_count Number of SSTables per level\n"));
        output.push_str(&format!("# TYPE rustdb_sstable_count gauge\n"));
        for (level, gauge) in self.internal.sstable_count.iter().enumerate() {
            output.push_str(&format!("rustdb_sstable_count{{level=\"{}\"}} {}\n", level, gauge.get()));
        }
        output.push('\n');

        // Compaction metrics
        output.push_str(&format!("# HELP rustdb_compactions_total Total compactions completed\n"));
        output.push_str(&format!("# TYPE rustdb_compactions_total counter\n"));
        output.push_str(&format!("rustdb_compactions_total {}\n\n", self.internal.compactions_total.get()));

        // Cache metrics
        output.push_str(&format!("# HELP rustdb_block_cache_hit_rate Block cache hit rate\n"));
        output.push_str(&format!("# TYPE rustdb_block_cache_hit_rate gauge\n"));
        output.push_str(&format!("rustdb_block_cache_hit_rate {:.4}\n\n", self.internal.block_cache_hit_rate()));

        output.push_str(&format!("# HELP rustdb_table_cache_hit_rate Table cache hit rate\n"));
        output.push_str(&format!("# TYPE rustdb_table_cache_hit_rate gauge\n"));
        output.push_str(&format!("rustdb_table_cache_hit_rate {:.4}\n\n", self.internal.table_cache_hit_rate()));

        // Latency histograms
        output.push_str(&format!("# HELP rustdb_read_latency_seconds Read operation latency\n"));
        output.push_str(&format!("# TYPE rustdb_read_latency_seconds histogram\n"));
        output.push_str(&self.ops.read_latency.to_prometheus("rustdb_read_latency_seconds"));
        output.push('\n');

        output.push_str(&format!("# HELP rustdb_write_latency_seconds Write operation latency\n"));
        output.push_str(&format!("# TYPE rustdb_write_latency_seconds histogram\n"));
        output.push_str(&self.ops.write_latency.to_prometheus("rustdb_write_latency_seconds"));

        output
    }

    /// Get a human-readable summary.
    pub fn summary(&self) -> MetricsSummary {
        MetricsSummary {
            reads: self.ops.reads.get(),
            writes: self.ops.writes.get(),
            deletes: self.ops.deletes.get(),
            bytes_read: self.ops.bytes_read.get(),
            bytes_written: self.ops.bytes_written.get(),
            txn_commits: self.ops.txn_commit.get(),
            txn_conflicts: self.ops.txn_conflicts.get(),
            read_latency_mean_us: self.ops.read_latency.mean(),
            write_latency_mean_us: self.ops.write_latency.mean(),
            memtable_size: self.internal.memtable_size.get() as u64,
            imm_memtable_count: self.internal.imm_memtable_count.get() as u64,
            compactions_total: self.internal.compactions_total.get(),
            block_cache_hit_rate: self.internal.block_cache_hit_rate(),
            table_cache_hit_rate: self.internal.table_cache_hit_rate(),
        }
    }
}

/// Human-readable metrics summary.
#[derive(Debug, Clone)]
pub struct MetricsSummary {
    pub reads: u64,
    pub writes: u64,
    pub deletes: u64,
    pub bytes_read: u64,
    pub bytes_written: u64,
    pub txn_commits: u64,
    pub txn_conflicts: u64,
    pub read_latency_mean_us: f64,
    pub write_latency_mean_us: f64,
    pub memtable_size: u64,
    pub imm_memtable_count: u64,
    pub compactions_total: u64,
    pub block_cache_hit_rate: f64,
    pub table_cache_hit_rate: f64,
}

impl fmt::Display for MetricsSummary {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "=== Database Metrics Summary ===")?;
        writeln!(f)?;
        writeln!(f, "Operations:")?;
        writeln!(f, "  Reads:   {} ({} bytes)", self.reads, format_bytes(self.bytes_read))?;
        writeln!(f, "  Writes:  {} ({} bytes)", self.writes, format_bytes(self.bytes_written))?;
        writeln!(f, "  Deletes: {}", self.deletes)?;
        writeln!(f)?;
        writeln!(f, "Latency (mean):")?;
        writeln!(f, "  Read:  {:.2} µs", self.read_latency_mean_us)?;
        writeln!(f, "  Write: {:.2} µs", self.write_latency_mean_us)?;
        writeln!(f)?;
        writeln!(f, "Transactions:")?;
        writeln!(f, "  Commits:   {}", self.txn_commits)?;
        writeln!(f, "  Conflicts: {}", self.txn_conflicts)?;
        writeln!(f)?;
        writeln!(f, "Storage:")?;
        writeln!(f, "  MemTable size:    {}", format_bytes(self.memtable_size))?;
        writeln!(f, "  Imm MemTables:    {}", self.imm_memtable_count)?;
        writeln!(f, "  Compactions:      {}", self.compactions_total)?;
        writeln!(f)?;
        writeln!(f, "Cache hit rates:")?;
        writeln!(f, "  Block cache: {:.1}%", self.block_cache_hit_rate * 100.0)?;
        writeln!(f, "  Table cache: {:.1}%", self.table_cache_hit_rate * 100.0)?;
        Ok(())
    }
}

fn format_bytes(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = KB * 1024;
    const GB: u64 = MB * 1024;

    if bytes >= GB {
        format!("{:.2} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.2} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.2} KB", bytes as f64 / KB as f64)
    } else {
        format!("{} B", bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_operation_metrics() {
        let metrics = OperationMetrics::new();

        metrics.reads.inc();
        metrics.reads.inc();
        metrics.writes.inc();
        metrics.bytes_read.add(1024);

        assert_eq!(metrics.reads.get(), 2);
        assert_eq!(metrics.writes.get(), 1);
        assert_eq!(metrics.bytes_read.get(), 1024);
    }

    #[test]
    fn test_internal_metrics() {
        let metrics = InternalMetrics::new();

        metrics.memtable_size.set(1024 * 1024);
        metrics.sstable_count[0].set(5);
        metrics.block_cache_hits.add(90);
        metrics.block_cache_misses.add(10);

        assert_eq!(metrics.memtable_size.get(), 1024 * 1024);
        assert_eq!(metrics.sstable_count[0].get(), 5);
        assert!((metrics.block_cache_hit_rate() - 0.9).abs() < 0.001);
    }

    #[test]
    fn test_db_metrics_prometheus() {
        let metrics = DbMetrics::new();

        metrics.ops.reads.add(100);
        metrics.ops.writes.add(50);
        metrics.internal.memtable_size.set(1024);

        let prometheus = metrics.to_prometheus();
        assert!(prometheus.contains("rustdb_reads_total 100"));
        assert!(prometheus.contains("rustdb_writes_total 50"));
        assert!(prometheus.contains("rustdb_memtable_size_bytes 1024"));
    }

    #[test]
    fn test_metrics_summary() {
        let metrics = DbMetrics::new();

        metrics.ops.reads.add(1000);
        metrics.ops.writes.add(500);
        metrics.ops.bytes_read.add(1024 * 1024);

        let summary = metrics.summary();
        assert_eq!(summary.reads, 1000);
        assert_eq!(summary.writes, 500);
        assert_eq!(summary.bytes_read, 1024 * 1024);

        // Test Display formatting
        let display = format!("{}", summary);
        assert!(display.contains("Reads:   1000"));
        assert!(display.contains("1.00 MB"));
    }
}
