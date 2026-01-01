//! Error types for rustdb.

use std::io;
use thiserror::Error;

/// Result type alias for rustdb operations.
pub type Result<T> = std::result::Result<T, Error>;

/// Error types for database operations.
#[derive(Error, Debug, Clone)]
pub enum Error {
    /// I/O error from file operations.
    #[error("I/O error: {0}")]
    Io(String),

    /// Data corruption detected.
    #[error("Corruption detected: {0}")]
    Corruption(String),

    /// Resource not found.
    #[error("Not found: {0}")]
    NotFound(String),

    /// Resource already exists.
    #[error("Already exists: {0}")]
    AlreadyExists(String),

    /// Lock error.
    #[error("Lock error: {0}")]
    LockError(String),

    /// CRC checksum mismatch.
    #[error("CRC mismatch: expected {expected:#x}, got {actual:#x}")]
    CrcMismatch { expected: u32, actual: u32 },

    /// Transaction conflict during commit.
    #[error("Transaction conflict: key was modified by another transaction")]
    TransactionConflict,

    /// Transaction has already been committed or aborted.
    #[error("Transaction is no longer active")]
    TransactionNotActive,

    /// Database is closed.
    #[error("Database is closed")]
    DatabaseClosed,

    /// Database already exists when trying to create.
    #[error("Database already exists at: {0}")]
    DatabaseExists(String),

    /// Database not found when trying to open.
    #[error("Database not found at: {0}")]
    DatabaseNotFound(String),

    /// Database is locked by another process.
    #[error("Database is locked by another process")]
    DatabaseLocked,

    /// Invalid configuration.
    #[error("Invalid configuration: {0}")]
    InvalidConfiguration(String),

    /// Key is too large.
    #[error("Key too large: {size} bytes (max: {max})")]
    KeyTooLarge { size: usize, max: usize },

    /// Value is too large.
    #[error("Value too large: {size} bytes (max: {max})")]
    ValueTooLarge { size: usize, max: usize },

    /// Empty key is not allowed.
    #[error("Empty key is not allowed")]
    EmptyKey,

    /// Write buffer is full (backpressure).
    #[error("Write buffer full, waiting for flush")]
    WriteBufferFull,

    /// Too many open files.
    #[error("Too many open files")]
    TooManyOpenFiles,

    /// Too many active transactions.
    #[error("Too many active transactions")]
    TooManyTransactions,

    /// Invalid file format or magic number.
    #[error("Invalid file format: {0}")]
    InvalidFormat(String),

    /// Incomplete record in WAL.
    #[error("Incomplete WAL record")]
    IncompleteRecord,

    /// Manifest error.
    #[error("Manifest error: {0}")]
    ManifestError(String),

    /// Compaction error.
    #[error("Compaction error: {0}")]
    CompactionError(String),

    /// Iterator error.
    #[error("Iterator error: {0}")]
    IteratorError(String),

    /// Internal error (should not happen).
    #[error("Internal error: {0}")]
    Internal(String),
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Self {
        Error::Io(err.to_string())
    }
}

impl Error {
    /// Create a corruption error with the given message.
    pub fn corruption<S: Into<String>>(msg: S) -> Self {
        Error::Corruption(msg.into())
    }

    /// Create an invalid format error.
    pub fn invalid_format<S: Into<String>>(msg: S) -> Self {
        Error::InvalidFormat(msg.into())
    }

    /// Create a manifest error.
    pub fn manifest<S: Into<String>>(msg: S) -> Self {
        Error::ManifestError(msg.into())
    }

    /// Create an internal error.
    pub fn internal<S: Into<String>>(msg: S) -> Self {
        Error::Internal(msg.into())
    }

    /// Check if this error is recoverable.
    pub fn is_recoverable(&self) -> bool {
        matches!(
            self,
            Error::WriteBufferFull
                | Error::TooManyOpenFiles
                | Error::TooManyTransactions
                | Error::TransactionConflict
        )
    }

    /// Check if this error indicates corruption.
    pub fn is_corruption(&self) -> bool {
        matches!(
            self,
            Error::Corruption(_) | Error::CrcMismatch { .. } | Error::InvalidFormat(_)
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let err = Error::corruption("bad data");
        assert_eq!(format!("{}", err), "Corruption detected: bad data");

        let err = Error::KeyTooLarge {
            size: 1000,
            max: 100,
        };
        assert_eq!(
            format!("{}", err),
            "Key too large: 1000 bytes (max: 100)"
        );
    }

    #[test]
    fn test_error_is_recoverable() {
        assert!(Error::WriteBufferFull.is_recoverable());
        assert!(Error::TransactionConflict.is_recoverable());
        assert!(!Error::Corruption("bad".into()).is_recoverable());
    }

    #[test]
    fn test_error_is_corruption() {
        assert!(Error::corruption("bad").is_corruption());
        assert!(Error::CrcMismatch {
            expected: 1,
            actual: 2
        }
        .is_corruption());
        assert!(!Error::WriteBufferFull.is_corruption());
    }
}
