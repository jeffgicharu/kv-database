//! Metrics collector with Prometheus-compatible types.
//!
//! Provides Counter, Gauge, and Histogram metric types with
//! atomic operations for thread-safe updates.

use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::time::Instant;

/// A monotonically increasing counter.
///
/// Counters are typically used for counting events like requests,
/// errors, or bytes processed.
#[derive(Debug, Default)]
pub struct Counter {
    value: AtomicU64,
}

impl Counter {
    /// Create a new counter initialized to 0.
    pub fn new() -> Self {
        Self::default()
    }

    /// Increment the counter by 1.
    pub fn inc(&self) {
        self.add(1);
    }

    /// Add a value to the counter.
    pub fn add(&self, v: u64) {
        self.value.fetch_add(v, Ordering::Relaxed);
    }

    /// Get the current value.
    pub fn get(&self) -> u64 {
        self.value.load(Ordering::Relaxed)
    }

    /// Reset to 0.
    pub fn reset(&self) {
        self.value.store(0, Ordering::Relaxed);
    }
}

impl Clone for Counter {
    fn clone(&self) -> Self {
        Self {
            value: AtomicU64::new(self.get()),
        }
    }
}

/// A gauge that can go up or down.
///
/// Gauges are typically used for values that can increase or decrease,
/// like current memory usage or active connections.
#[derive(Debug, Default)]
pub struct Gauge {
    value: AtomicI64,
}

impl Gauge {
    /// Create a new gauge initialized to 0.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the gauge to a specific value.
    pub fn set(&self, v: i64) {
        self.value.store(v, Ordering::Relaxed);
    }

    /// Increment the gauge by 1.
    pub fn inc(&self) {
        self.add(1);
    }

    /// Decrement the gauge by 1.
    pub fn dec(&self) {
        self.sub(1);
    }

    /// Add a value to the gauge.
    pub fn add(&self, v: i64) {
        self.value.fetch_add(v, Ordering::Relaxed);
    }

    /// Subtract a value from the gauge.
    pub fn sub(&self, v: i64) {
        self.value.fetch_sub(v, Ordering::Relaxed);
    }

    /// Get the current value.
    pub fn get(&self) -> i64 {
        self.value.load(Ordering::Relaxed)
    }
}

impl Clone for Gauge {
    fn clone(&self) -> Self {
        Self {
            value: AtomicI64::new(self.get()),
        }
    }
}

/// A histogram for tracking value distributions.
///
/// Tracks count, sum, min, max, and approximate percentiles
/// using a simple bucketing approach.
#[derive(Debug)]
pub struct Histogram {
    /// Count of observations.
    count: AtomicU64,
    /// Sum of all observations.
    sum: AtomicU64,
    /// Minimum observed value (in microseconds).
    min: AtomicU64,
    /// Maximum observed value (in microseconds).
    max: AtomicU64,
    /// Bucket counts for latency ranges (in microseconds).
    /// Buckets: <1us, <10us, <100us, <1ms, <10ms, <100ms, <1s, <10s, >=10s
    buckets: [AtomicU64; 9],
}

impl Default for Histogram {
    fn default() -> Self {
        Self::new()
    }
}

impl Histogram {
    /// Bucket thresholds in microseconds.
    const BUCKET_THRESHOLDS: [u64; 8] = [
        1,       // <1us
        10,      // <10us
        100,     // <100us
        1_000,   // <1ms
        10_000,  // <10ms
        100_000, // <100ms
        1_000_000,  // <1s
        10_000_000, // <10s
    ];

    /// Create a new histogram.
    pub fn new() -> Self {
        Self {
            count: AtomicU64::new(0),
            sum: AtomicU64::new(0),
            min: AtomicU64::new(u64::MAX),
            max: AtomicU64::new(0),
            buckets: Default::default(),
        }
    }

    /// Record a duration observation.
    pub fn observe_duration(&self, start: Instant) {
        let micros = start.elapsed().as_micros() as u64;
        self.observe(micros);
    }

    /// Record a value observation (in microseconds for latencies).
    pub fn observe(&self, value: u64) {
        self.count.fetch_add(1, Ordering::Relaxed);
        self.sum.fetch_add(value, Ordering::Relaxed);

        // Update min
        let mut current_min = self.min.load(Ordering::Relaxed);
        while value < current_min {
            match self.min.compare_exchange_weak(
                current_min,
                value,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(v) => current_min = v,
            }
        }

        // Update max
        let mut current_max = self.max.load(Ordering::Relaxed);
        while value > current_max {
            match self.max.compare_exchange_weak(
                current_max,
                value,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(v) => current_max = v,
            }
        }

        // Update bucket
        let bucket_idx = Self::BUCKET_THRESHOLDS
            .iter()
            .position(|&threshold| value < threshold)
            .unwrap_or(8);
        self.buckets[bucket_idx].fetch_add(1, Ordering::Relaxed);
    }

    /// Get the count of observations.
    pub fn count(&self) -> u64 {
        self.count.load(Ordering::Relaxed)
    }

    /// Get the sum of all observations.
    pub fn sum(&self) -> u64 {
        self.sum.load(Ordering::Relaxed)
    }

    /// Get the minimum observed value.
    pub fn min(&self) -> Option<u64> {
        let v = self.min.load(Ordering::Relaxed);
        if v == u64::MAX {
            None
        } else {
            Some(v)
        }
    }

    /// Get the maximum observed value.
    pub fn max(&self) -> Option<u64> {
        let v = self.max.load(Ordering::Relaxed);
        if v == 0 && self.count() == 0 {
            None
        } else {
            Some(v)
        }
    }

    /// Get the mean value.
    pub fn mean(&self) -> f64 {
        let count = self.count();
        if count == 0 {
            0.0
        } else {
            self.sum() as f64 / count as f64
        }
    }

    /// Get bucket counts.
    pub fn bucket_counts(&self) -> [u64; 9] {
        let mut counts = [0u64; 9];
        for (i, bucket) in self.buckets.iter().enumerate() {
            counts[i] = bucket.load(Ordering::Relaxed);
        }
        counts
    }

    /// Reset all values.
    pub fn reset(&self) {
        self.count.store(0, Ordering::Relaxed);
        self.sum.store(0, Ordering::Relaxed);
        self.min.store(u64::MAX, Ordering::Relaxed);
        self.max.store(0, Ordering::Relaxed);
        for bucket in &self.buckets {
            bucket.store(0, Ordering::Relaxed);
        }
    }

    /// Format as Prometheus histogram metrics.
    pub fn to_prometheus(&self, name: &str) -> String {
        let mut output = String::new();
        
        let buckets = self.bucket_counts();
        let mut cumulative = 0u64;
        
        for (i, &threshold) in Self::BUCKET_THRESHOLDS.iter().enumerate() {
            cumulative += buckets[i];
            let le = if threshold >= 1_000_000 {
                format!("{}", threshold as f64 / 1_000_000.0)
            } else if threshold >= 1_000 {
                format!("{:.3}", threshold as f64 / 1_000_000.0)
            } else {
                format!("{:.6}", threshold as f64 / 1_000_000.0)
            };
            output.push_str(&format!("{}_bucket{{le=\"{}\"}} {}\n", name, le, cumulative));
        }
        
        cumulative += buckets[8];
        output.push_str(&format!("{}_bucket{{le=\"+Inf\"}} {}\n", name, cumulative));
        output.push_str(&format!("{}_sum {}\n", name, self.sum()));
        output.push_str(&format!("{}_count {}\n", name, self.count()));
        
        output
    }
}

impl Clone for Histogram {
    fn clone(&self) -> Self {
        let mut buckets: [AtomicU64; 9] = Default::default();
        for (i, b) in self.buckets.iter().enumerate() {
            buckets[i] = AtomicU64::new(b.load(Ordering::Relaxed));
        }
        
        Self {
            count: AtomicU64::new(self.count()),
            sum: AtomicU64::new(self.sum()),
            min: AtomicU64::new(self.min.load(Ordering::Relaxed)),
            max: AtomicU64::new(self.max.load(Ordering::Relaxed)),
            buckets,
        }
    }
}

/// Timer for measuring operation duration.
///
/// When dropped, records the elapsed time to the histogram.
pub struct Timer<'a> {
    histogram: &'a Histogram,
    start: Instant,
}

impl<'a> Timer<'a> {
    /// Create a new timer for the given histogram.
    pub fn new(histogram: &'a Histogram) -> Self {
        Self {
            histogram,
            start: Instant::now(),
        }
    }

    /// Stop the timer and record the duration.
    pub fn stop(self) {
        // Drop will record the duration
    }
}

impl<'a> Drop for Timer<'a> {
    fn drop(&mut self) {
        self.histogram.observe_duration(self.start);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_counter_basic() {
        let counter = Counter::new();
        assert_eq!(counter.get(), 0);

        counter.inc();
        assert_eq!(counter.get(), 1);

        counter.add(5);
        assert_eq!(counter.get(), 6);

        counter.reset();
        assert_eq!(counter.get(), 0);
    }

    #[test]
    fn test_gauge_basic() {
        let gauge = Gauge::new();
        assert_eq!(gauge.get(), 0);

        gauge.set(10);
        assert_eq!(gauge.get(), 10);

        gauge.inc();
        assert_eq!(gauge.get(), 11);

        gauge.dec();
        assert_eq!(gauge.get(), 10);

        gauge.add(5);
        assert_eq!(gauge.get(), 15);

        gauge.sub(20);
        assert_eq!(gauge.get(), -5);
    }

    #[test]
    fn test_histogram_basic() {
        let hist = Histogram::new();

        hist.observe(100);
        hist.observe(200);
        hist.observe(300);

        assert_eq!(hist.count(), 3);
        assert_eq!(hist.sum(), 600);
        assert_eq!(hist.min(), Some(100));
        assert_eq!(hist.max(), Some(300));
        assert!((hist.mean() - 200.0).abs() < 0.001);
    }

    #[test]
    fn test_histogram_buckets() {
        let hist = Histogram::new();

        // <1us bucket
        hist.observe(0);
        
        // <10us bucket
        hist.observe(5);
        
        // <100us bucket
        hist.observe(50);
        
        // <1ms bucket
        hist.observe(500);

        let buckets = hist.bucket_counts();
        assert_eq!(buckets[0], 1); // <1us
        assert_eq!(buckets[1], 1); // <10us
        assert_eq!(buckets[2], 1); // <100us
        assert_eq!(buckets[3], 1); // <1ms
    }

    #[test]
    fn test_timer() {
        let hist = Histogram::new();

        {
            let _timer = Timer::new(&hist);
            thread::sleep(Duration::from_micros(100));
        }

        assert_eq!(hist.count(), 1);
        assert!(hist.sum() >= 100); // At least 100 microseconds
    }

    #[test]
    fn test_counter_thread_safety() {
        let counter = Counter::new();
        let counter_ref = &counter;

        std::thread::scope(|s| {
            for _ in 0..10 {
                s.spawn(|| {
                    for _ in 0..1000 {
                        counter_ref.inc();
                    }
                });
            }
        });

        assert_eq!(counter.get(), 10_000);
    }

    #[test]
    fn test_histogram_prometheus_format() {
        let hist = Histogram::new();
        hist.observe(500); // 500us = 0.0005s

        let output = hist.to_prometheus("request_duration_seconds");
        assert!(output.contains("request_duration_seconds_bucket"));
        assert!(output.contains("request_duration_seconds_sum"));
        assert!(output.contains("request_duration_seconds_count"));
    }
}
