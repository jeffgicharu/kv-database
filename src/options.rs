//! Configuration options for rustdb.

use std::time::Duration;

/// Maximum number of levels in the LSM tree.
pub const MAX_LEVELS: usize = 7;

/// Default block size (4KB).
pub const DEFAULT_BLOCK_SIZE: usize = 4 * 1024;

/// Default memtable size (64MB).
pub const DEFAULT_MEMTABLE_SIZE: usize = 64 * 1024 * 1024;

/// Default block cache size (256MB).
pub const DEFAULT_BLOCK_CACHE_SIZE: usize = 256 * 1024 * 1024;

/// Default bloom filter bits per key.
pub const DEFAULT_BLOOM_BITS_PER_KEY: usize = 10;

/// Default L0 file limit before compaction.
pub const DEFAULT_L0_COMPACTION_TRIGGER: usize = 4;

/// Default L0 file limit before slowing writes.
pub const DEFAULT_L0_SLOWDOWN_TRIGGER: usize = 8;

/// Default L0 file limit before stopping writes.
pub const DEFAULT_L0_STOP_TRIGGER: usize = 12;

/// Default L1 size (256MB).
pub const DEFAULT_L1_SIZE: usize = 256 * 1024 * 1024;

/// Default level size multiplier.
pub const DEFAULT_LEVEL_MULTIPLIER: usize = 10;

/// Maximum key size (8KB).
pub const MAX_KEY_SIZE: usize = 8 * 1024;

/// Maximum value size (1GB).
pub const MAX_VALUE_SIZE: usize = 1024 * 1024 * 1024;

/// Compression algorithm.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Compression {
    /// No compression.
    #[default]
    None,
    /// LZ4 compression (fast).
    Lz4,
    /// Snappy compression (very fast).
    Snappy,
}

impl Compression {
    /// Check if compression is enabled.
    pub fn is_enabled(&self) -> bool {
        !matches!(self, Compression::None)
    }
}

/// WAL sync mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncMode {
    /// Sync on every write (safest, slowest).
    Always,
    /// Sync at intervals (balanced).
    Interval {
        /// Interval between syncs.
        interval: Duration,
    },
    /// Sync after N bytes written.
    Bytes {
        /// Number of bytes before sync.
        bytes: usize,
    },
    /// Let OS decide when to sync (fastest, may lose recent writes on crash).
    None,
}

impl Default for SyncMode {
    fn default() -> Self {
        SyncMode::Interval {
            interval: Duration::from_millis(100),
        }
    }
}

/// Database configuration options.
#[derive(Debug, Clone)]
pub struct Options {
    // === Storage ===
    /// Create database if it doesn't exist.
    pub create_if_missing: bool,

    /// Return error if database already exists.
    pub error_if_exists: bool,

    /// Enable extra verification (paranoid checks).
    pub paranoid_checks: bool,

    // === MemTable ===
    /// Maximum size of a single memtable before flush.
    pub max_memtable_size: usize,

    /// Maximum number of memtables (including immutable ones).
    pub max_write_buffers: usize,

    // === SSTable ===
    /// Target size for data blocks.
    pub block_size: usize,

    /// Compression algorithm for data blocks.
    pub compression: Compression,

    /// Bloom filter bits per key (0 to disable).
    pub bloom_filter_bits_per_key: usize,

    /// Use block-based bloom filter.
    pub block_based_bloom_filter: bool,

    // === Compaction ===
    /// Number of L0 files to trigger compaction.
    pub l0_compaction_trigger: usize,

    /// Number of L0 files to slow down writes.
    pub l0_slowdown_trigger: usize,

    /// Number of L0 files to stop writes.
    pub l0_stop_trigger: usize,

    /// Target size for level 1.
    pub max_bytes_for_level_base: usize,

    /// Size multiplier for each level.
    pub max_bytes_for_level_multiplier: usize,

    /// Target file size for levels > 0.
    pub target_file_size_base: usize,

    /// File size multiplier for each level.
    pub target_file_size_multiplier: usize,

    /// Maximum number of background compaction threads.
    pub max_background_compactions: usize,

    /// Maximum number of background flush threads.
    pub max_background_flushes: usize,

    // === Cache ===
    /// Block cache size in bytes.
    pub block_cache_size: usize,

    /// Enable table cache.
    pub enable_table_cache: bool,

    /// Maximum number of open files in table cache.
    pub max_open_files: usize,

    // === Durability ===
    /// WAL sync mode.
    pub sync_mode: SyncMode,

    /// Maximum WAL file size before rotation.
    pub max_wal_size: usize,

    /// Recycle WAL files instead of deleting.
    pub recycle_wal_files: bool,

    // === Transactions ===
    /// Maximum number of concurrent transactions.
    pub max_transactions: usize,

    /// Transaction timeout.
    pub transaction_timeout: Option<Duration>,

    // === Limits ===
    /// Maximum key size.
    pub max_key_size: usize,

    /// Maximum value size.
    pub max_value_size: usize,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            // Storage
            create_if_missing: true,
            error_if_exists: false,
            paranoid_checks: false,

            // MemTable
            max_memtable_size: DEFAULT_MEMTABLE_SIZE,
            max_write_buffers: 3,

            // SSTable
            block_size: DEFAULT_BLOCK_SIZE,
            compression: Compression::default(),
            bloom_filter_bits_per_key: DEFAULT_BLOOM_BITS_PER_KEY,
            block_based_bloom_filter: true,

            // Compaction
            l0_compaction_trigger: DEFAULT_L0_COMPACTION_TRIGGER,
            l0_slowdown_trigger: DEFAULT_L0_SLOWDOWN_TRIGGER,
            l0_stop_trigger: DEFAULT_L0_STOP_TRIGGER,
            max_bytes_for_level_base: DEFAULT_L1_SIZE,
            max_bytes_for_level_multiplier: DEFAULT_LEVEL_MULTIPLIER,
            target_file_size_base: 64 * 1024 * 1024, // 64MB
            target_file_size_multiplier: 1,
            max_background_compactions: 4,
            max_background_flushes: 2,

            // Cache
            block_cache_size: DEFAULT_BLOCK_CACHE_SIZE,
            enable_table_cache: true,
            max_open_files: 1000,

            // Durability
            sync_mode: SyncMode::default(),
            max_wal_size: 128 * 1024 * 1024, // 128MB
            recycle_wal_files: true,

            // Transactions
            max_transactions: 1000,
            transaction_timeout: Some(Duration::from_secs(60)),

            // Limits
            max_key_size: MAX_KEY_SIZE,
            max_value_size: MAX_VALUE_SIZE,
        }
    }
}

impl Options {
    /// Create new options with defaults.
    pub fn new() -> Self {
        Self::default()
    }

    /// Validate the options.
    pub fn validate(&self) -> crate::Result<()> {
        if self.max_memtable_size < 1024 {
            return Err(crate::Error::InvalidConfiguration(
                "max_memtable_size must be at least 1KB".into(),
            ));
        }

        if self.block_size < 256 {
            return Err(crate::Error::InvalidConfiguration(
                "block_size must be at least 256 bytes".into(),
            ));
        }

        if self.l0_compaction_trigger > self.l0_slowdown_trigger {
            return Err(crate::Error::InvalidConfiguration(
                "l0_compaction_trigger must be <= l0_slowdown_trigger".into(),
            ));
        }

        if self.l0_slowdown_trigger > self.l0_stop_trigger {
            return Err(crate::Error::InvalidConfiguration(
                "l0_slowdown_trigger must be <= l0_stop_trigger".into(),
            ));
        }

        if self.max_write_buffers < 2 {
            return Err(crate::Error::InvalidConfiguration(
                "max_write_buffers must be at least 2".into(),
            ));
        }

        if self.max_key_size > MAX_KEY_SIZE {
            return Err(crate::Error::InvalidConfiguration(format!(
                "max_key_size cannot exceed {}",
                MAX_KEY_SIZE
            )));
        }

        Ok(())
    }

    /// Calculate the maximum size for a given level.
    pub fn max_bytes_for_level(&self, level: usize) -> usize {
        if level == 0 {
            // L0 is special - compaction triggered by file count, not size
            self.max_memtable_size * self.l0_compaction_trigger
        } else {
            let mut size = self.max_bytes_for_level_base;
            for _ in 1..level {
                size *= self.max_bytes_for_level_multiplier;
            }
            size
        }
    }

    /// Calculate the target file size for a given level.
    pub fn target_file_size_for_level(&self, level: usize) -> usize {
        let mut size = self.target_file_size_base;
        for _ in 0..level {
            size *= self.target_file_size_multiplier;
        }
        size
    }
}

/// Builder for Options.
#[derive(Debug, Clone, Default)]
pub struct OptionsBuilder {
    options: Options,
}

impl OptionsBuilder {
    /// Create a new builder with default options.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set create_if_missing.
    pub fn create_if_missing(mut self, value: bool) -> Self {
        self.options.create_if_missing = value;
        self
    }

    /// Set error_if_exists.
    pub fn error_if_exists(mut self, value: bool) -> Self {
        self.options.error_if_exists = value;
        self
    }

    /// Set max_memtable_size.
    pub fn max_memtable_size(mut self, size: usize) -> Self {
        self.options.max_memtable_size = size;
        self
    }

    /// Set compression.
    pub fn compression(mut self, compression: Compression) -> Self {
        self.options.compression = compression;
        self
    }

    /// Set bloom filter bits per key.
    pub fn bloom_filter_bits(mut self, bits: usize) -> Self {
        self.options.bloom_filter_bits_per_key = bits;
        self
    }

    /// Set sync mode.
    pub fn sync_mode(mut self, mode: SyncMode) -> Self {
        self.options.sync_mode = mode;
        self
    }

    /// Set block cache size.
    pub fn block_cache_size(mut self, size: usize) -> Self {
        self.options.block_cache_size = size;
        self
    }

    /// Build the options.
    pub fn build(self) -> crate::Result<Options> {
        self.options.validate()?;
        Ok(self.options)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_options() {
        let opts = Options::default();
        assert!(opts.create_if_missing);
        assert!(!opts.error_if_exists);
        assert_eq!(opts.max_memtable_size, DEFAULT_MEMTABLE_SIZE);
    }

    #[test]
    fn test_options_validation() {
        let mut opts = Options::default();
        assert!(opts.validate().is_ok());

        opts.max_memtable_size = 100; // Too small
        assert!(opts.validate().is_err());
    }

    #[test]
    fn test_level_size_calculation() {
        let opts = Options::default();

        // L1 should be 256MB
        assert_eq!(opts.max_bytes_for_level(1), DEFAULT_L1_SIZE);

        // L2 should be 10x L1 = 2.56GB
        assert_eq!(
            opts.max_bytes_for_level(2),
            DEFAULT_L1_SIZE * DEFAULT_LEVEL_MULTIPLIER
        );
    }

    #[test]
    fn test_options_builder() {
        let opts = OptionsBuilder::new()
            .create_if_missing(false)
            .compression(Compression::Lz4)
            .max_memtable_size(32 * 1024 * 1024)
            .build()
            .unwrap();

        assert!(!opts.create_if_missing);
        assert_eq!(opts.compression, Compression::Lz4);
        assert_eq!(opts.max_memtable_size, 32 * 1024 * 1024);
    }
}
