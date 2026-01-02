//! Metrics and observability for the database.
//!
//! This module provides comprehensive metrics for monitoring:
//!
//! - **Operation metrics**: Read/write counts, latencies, bytes transferred
//! - **Transaction metrics**: Commits, rollbacks, conflicts
//! - **Internal metrics**: MemTable size, SSTable counts, cache hit rates
//! - **Prometheus export**: Compatible metric format for monitoring

mod collector;
mod db_metrics;

pub use collector::{Counter, Gauge, Histogram, Timer};
pub use db_metrics::{DbMetrics, InternalMetrics, MetricsSummary, OperationMetrics};
