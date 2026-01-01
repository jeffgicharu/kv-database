//! WAL writer implementation.

use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::Path;

use crate::options::SyncMode;
use crate::util::crc::crc32;
use crate::Result;

use super::{RecordType, BLOCK_SIZE, HEADER_SIZE};

/// WAL writer for durable logging.
///
/// Writes records to a log file using a block-based format with CRC checksums.
pub struct WalWriter {
    /// Buffered writer for the WAL file.
    writer: BufWriter<File>,
    /// Current position within the current block.
    block_offset: usize,
    /// Sync mode for durability.
    sync_mode: SyncMode,
    /// Bytes written since last sync.
    bytes_since_sync: usize,
    /// File number for this WAL.
    file_number: u64,
}

impl WalWriter {
    /// Create a new WAL writer.
    pub fn new(path: &Path, file_number: u64, sync_mode: SyncMode) -> Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path)?;

        Ok(Self {
            writer: BufWriter::with_capacity(BLOCK_SIZE * 4, file),
            block_offset: 0,
            sync_mode,
            bytes_since_sync: 0,
            file_number,
        })
    }

    /// Open an existing WAL for appending.
    pub fn open_for_append(path: &Path, file_number: u64, sync_mode: SyncMode) -> Result<Self> {
        let file = OpenOptions::new().create(true).append(true).open(path)?;

        // Calculate current block offset from file size
        let file_size = file.metadata()?.len() as usize;
        let block_offset = file_size % BLOCK_SIZE;

        Ok(Self {
            writer: BufWriter::with_capacity(BLOCK_SIZE * 4, file),
            block_offset,
            sync_mode,
            bytes_since_sync: 0,
            file_number,
        })
    }

    /// Get the file number.
    pub fn file_number(&self) -> u64 {
        self.file_number
    }

    /// Add a record to the WAL.
    ///
    /// The record may be split across multiple fragments if it doesn't
    /// fit in the current block.
    pub fn add_record(&mut self, data: &[u8]) -> Result<()> {
        let mut left = data.len();
        let mut ptr = 0;
        let mut begin = true;

        while left > 0 {
            let leftover = BLOCK_SIZE - self.block_offset;

            // Switch to a new block if we can't fit a header
            if leftover < HEADER_SIZE {
                // Fill rest of block with zeros
                if leftover > 0 {
                    self.writer.write_all(&vec![0u8; leftover])?;
                    self.bytes_since_sync += leftover;
                }
                self.block_offset = 0;
            }

            let avail = BLOCK_SIZE - self.block_offset - HEADER_SIZE;
            let fragment_length = std::cmp::min(left, avail);
            let end = left == fragment_length;

            let record_type = if begin && end {
                RecordType::Full
            } else if begin {
                RecordType::First
            } else if end {
                RecordType::Last
            } else {
                RecordType::Middle
            };

            self.emit_physical_record(record_type, &data[ptr..ptr + fragment_length])?;

            ptr += fragment_length;
            left -= fragment_length;
            begin = false;
        }

        // Handle sync based on mode
        self.maybe_sync()?;

        Ok(())
    }

    /// Write a physical record (header + data).
    fn emit_physical_record(&mut self, record_type: RecordType, data: &[u8]) -> Result<()> {
        debug_assert!(data.len() <= 0xFFFF); // Length fits in 2 bytes
        debug_assert!(self.block_offset + HEADER_SIZE + data.len() <= BLOCK_SIZE);

        // Build header
        let mut header = [0u8; HEADER_SIZE];

        // Calculate CRC over type + data
        let mut crc_data = Vec::with_capacity(1 + data.len());
        crc_data.push(record_type.to_byte());
        crc_data.extend_from_slice(data);
        let crc = crc32(&crc_data);

        // Header format: CRC (4) + Length (2) + Type (1)
        header[0..4].copy_from_slice(&crc.to_le_bytes());
        header[4..6].copy_from_slice(&(data.len() as u16).to_le_bytes());
        header[6] = record_type.to_byte();

        // Write header and data
        self.writer.write_all(&header)?;
        self.writer.write_all(data)?;

        let record_size = HEADER_SIZE + data.len();
        self.block_offset += record_size;
        self.bytes_since_sync += record_size;

        Ok(())
    }

    /// Sync if required by sync mode.
    fn maybe_sync(&mut self) -> Result<()> {
        match self.sync_mode {
            SyncMode::Always => {
                self.sync()?;
            }
            SyncMode::Bytes { bytes } => {
                if self.bytes_since_sync >= bytes {
                    self.sync()?;
                }
            }
            SyncMode::Interval { .. } => {
                // Interval-based sync is handled externally
            }
            SyncMode::None => {
                // No sync
            }
        }
        Ok(())
    }

    /// Force a sync to disk.
    pub fn sync(&mut self) -> Result<()> {
        self.writer.flush()?;
        self.writer.get_ref().sync_data()?;
        self.bytes_since_sync = 0;
        Ok(())
    }

    /// Flush buffered data (but don't sync to disk).
    pub fn flush(&mut self) -> Result<()> {
        self.writer.flush()?;
        Ok(())
    }

    /// Close the writer.
    pub fn close(mut self) -> Result<()> {
        self.flush()?;
        self.sync()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_writer_create() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.wal");

        let writer = WalWriter::new(&path, 1, SyncMode::None).unwrap();
        assert_eq!(writer.file_number(), 1);
    }

    #[test]
    fn test_writer_add_record() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.wal");

        let mut writer = WalWriter::new(&path, 1, SyncMode::None).unwrap();
        writer.add_record(b"hello world").unwrap();
        writer.close().unwrap();

        // Verify file was created with data
        let metadata = std::fs::metadata(&path).unwrap();
        assert!(metadata.len() > 0);
    }

    #[test]
    fn test_writer_multiple_records() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.wal");

        let mut writer = WalWriter::new(&path, 1, SyncMode::None).unwrap();

        for i in 0..100 {
            let data = format!("record {}", i);
            writer.add_record(data.as_bytes()).unwrap();
        }

        writer.close().unwrap();
    }

    #[test]
    fn test_writer_large_record() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.wal");

        let mut writer = WalWriter::new(&path, 1, SyncMode::None).unwrap();

        // Create a record larger than one block
        let large_data = vec![b'x'; BLOCK_SIZE * 2];
        writer.add_record(&large_data).unwrap();
        writer.close().unwrap();

        // File should be at least 2 blocks
        let metadata = std::fs::metadata(&path).unwrap();
        assert!(metadata.len() >= (BLOCK_SIZE * 2) as u64);
    }

    #[test]
    fn test_writer_sync_always() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.wal");

        let mut writer = WalWriter::new(&path, 1, SyncMode::Always).unwrap();
        writer.add_record(b"synced record").unwrap();
        writer.close().unwrap();
    }
}
