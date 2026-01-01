//! WAL reader implementation.

use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::path::Path;

use bytes::Bytes;

use crate::util::crc::crc32;
use crate::{Error, Result};

use super::{RecordType, BLOCK_SIZE, HEADER_SIZE};

/// WAL reader for recovery.
///
/// Reads records from a WAL file, handling fragmented records and
/// verifying CRC checksums.
pub struct WalReader {
    /// Buffered reader for the WAL file.
    reader: BufReader<File>,
    /// Current block buffer.
    buffer: Vec<u8>,
    /// Current position within the buffer.
    buffer_offset: usize,
    /// Valid bytes in the buffer.
    buffer_size: usize,
    /// Whether we've reached EOF.
    eof: bool,
    /// Whether to report corruption or skip.
    checksum_errors_are_fatal: bool,
    /// File number for this WAL.
    file_number: u64,
}

impl WalReader {
    /// Create a new WAL reader.
    pub fn new(path: &Path, file_number: u64) -> Result<Self> {
        let file = File::open(path)?;

        Ok(Self {
            reader: BufReader::with_capacity(BLOCK_SIZE * 4, file),
            buffer: vec![0u8; BLOCK_SIZE],
            buffer_offset: 0,
            buffer_size: 0,
            eof: false,
            checksum_errors_are_fatal: true,
            file_number,
        })
    }

    /// Set whether checksum errors should be fatal.
    pub fn set_checksum_errors_fatal(&mut self, fatal: bool) {
        self.checksum_errors_are_fatal = fatal;
    }

    /// Get the file number.
    pub fn file_number(&self) -> u64 {
        self.file_number
    }

    /// Read the next record.
    ///
    /// Returns None when there are no more records.
    pub fn read_record(&mut self) -> Result<Option<Bytes>> {
        let mut scratch = Vec::new();
        let mut in_fragmented_record = false;

        loop {
            match self.read_physical_record()? {
                Some((record_type, data)) => {
                    match record_type {
                        RecordType::Full => {
                            if in_fragmented_record {
                                // Previous fragments were incomplete
                                scratch.clear();
                            }
                            return Ok(Some(Bytes::copy_from_slice(&data)));
                        }
                        RecordType::First => {
                            if in_fragmented_record {
                                // Previous fragments were incomplete
                                scratch.clear();
                            }
                            scratch.extend_from_slice(&data);
                            in_fragmented_record = true;
                        }
                        RecordType::Middle => {
                            if !in_fragmented_record {
                                // Unexpected middle fragment
                                if self.checksum_errors_are_fatal {
                                    return Err(Error::corruption(
                                        "unexpected middle record fragment",
                                    ));
                                }
                                continue;
                            }
                            scratch.extend_from_slice(&data);
                        }
                        RecordType::Last => {
                            if !in_fragmented_record {
                                // Unexpected last fragment
                                if self.checksum_errors_are_fatal {
                                    return Err(Error::corruption("unexpected last record fragment"));
                                }
                                continue;
                            }
                            scratch.extend_from_slice(&data);
                            return Ok(Some(Bytes::from(scratch)));
                        }
                        RecordType::Zero => {
                            // Skip padding
                            continue;
                        }
                    }
                }
                None => {
                    if in_fragmented_record {
                        // Incomplete record at end of file
                        scratch.clear();
                    }
                    return Ok(None);
                }
            }
        }
    }

    /// Read a physical record from the current position.
    fn read_physical_record(&mut self) -> Result<Option<(RecordType, Vec<u8>)>> {
        loop {
            // Check if we need to read a new block
            if self.buffer_offset + HEADER_SIZE > self.buffer_size {
                if !self.read_block()? {
                    return Ok(None);
                }
                continue;
            }

            // Read header
            let header = &self.buffer[self.buffer_offset..self.buffer_offset + HEADER_SIZE];

            let crc_expected = u32::from_le_bytes([header[0], header[1], header[2], header[3]]);
            let length = u16::from_le_bytes([header[4], header[5]]) as usize;
            let record_type_byte = header[6];

            let record_type = match RecordType::from_byte(record_type_byte) {
                Some(rt) => rt,
                None => {
                    if self.checksum_errors_are_fatal {
                        return Err(Error::corruption("invalid record type"));
                    }
                    // Skip this block
                    self.buffer_offset = self.buffer_size;
                    continue;
                }
            };

            // Check if record fits in buffer
            if self.buffer_offset + HEADER_SIZE + length > self.buffer_size {
                if self.eof {
                    // Truncated record at end of file
                    return Ok(None);
                }
                if self.checksum_errors_are_fatal {
                    return Err(Error::corruption("record extends beyond block"));
                }
                self.buffer_offset = self.buffer_size;
                continue;
            }

            // Read data
            let data_start = self.buffer_offset + HEADER_SIZE;
            let data_end = data_start + length;
            let data = &self.buffer[data_start..data_end];

            // Verify CRC
            let mut crc_data = Vec::with_capacity(1 + length);
            crc_data.push(record_type_byte);
            crc_data.extend_from_slice(data);
            let crc_actual = crc32(&crc_data);

            if crc_expected != crc_actual {
                if self.checksum_errors_are_fatal {
                    return Err(Error::corruption("record checksum mismatch"));
                }
                // Skip to next block
                self.buffer_offset = self.buffer_size;
                continue;
            }

            self.buffer_offset = data_end;

            return Ok(Some((record_type, data.to_vec())));
        }
    }

    /// Read the next block into the buffer.
    fn read_block(&mut self) -> Result<bool> {
        if self.eof {
            return Ok(false);
        }

        self.buffer_offset = 0;
        let bytes_read = self.reader.read(&mut self.buffer)?;

        if bytes_read == 0 {
            self.eof = true;
            self.buffer_size = 0;
            return Ok(false);
        }

        self.buffer_size = bytes_read;

        // If we read less than a full block, we're at EOF
        if bytes_read < BLOCK_SIZE {
            self.eof = true;
        }

        Ok(true)
    }

    /// Seek to beginning of file.
    pub fn seek_to_start(&mut self) -> Result<()> {
        self.reader.seek(SeekFrom::Start(0))?;
        self.buffer_offset = 0;
        self.buffer_size = 0;
        self.eof = false;
        Ok(())
    }
}

/// Iterator over WAL records.
pub struct WalIterator<'a> {
    reader: &'a mut WalReader,
}

impl<'a> WalIterator<'a> {
    /// Create a new iterator.
    pub fn new(reader: &'a mut WalReader) -> Self {
        Self { reader }
    }
}

impl<'a> Iterator for WalIterator<'a> {
    type Item = Result<Bytes>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.reader.read_record() {
            Ok(Some(record)) => Some(Ok(record)),
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::WalWriter;
    use super::*;
    use crate::options::SyncMode;
    use tempfile::tempdir;

    #[test]
    fn test_reader_empty_file() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.wal");

        // Create empty WAL
        let writer = WalWriter::new(&path, 1, SyncMode::None).unwrap();
        writer.close().unwrap();

        let mut reader = WalReader::new(&path, 1).unwrap();
        assert!(reader.read_record().unwrap().is_none());
    }

    #[test]
    fn test_reader_single_record() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.wal");

        // Write a record
        let mut writer = WalWriter::new(&path, 1, SyncMode::None).unwrap();
        writer.add_record(b"hello world").unwrap();
        writer.close().unwrap();

        // Read it back
        let mut reader = WalReader::new(&path, 1).unwrap();
        let record = reader.read_record().unwrap().unwrap();
        assert_eq!(&record[..], b"hello world");

        // No more records
        assert!(reader.read_record().unwrap().is_none());
    }

    #[test]
    fn test_reader_multiple_records() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.wal");

        // Write records
        let mut writer = WalWriter::new(&path, 1, SyncMode::None).unwrap();
        for i in 0..10 {
            let data = format!("record {}", i);
            writer.add_record(data.as_bytes()).unwrap();
        }
        writer.close().unwrap();

        // Read them back
        let mut reader = WalReader::new(&path, 1).unwrap();
        for i in 0..10 {
            let record = reader.read_record().unwrap().unwrap();
            let expected = format!("record {}", i);
            assert_eq!(&record[..], expected.as_bytes());
        }

        assert!(reader.read_record().unwrap().is_none());
    }

    #[test]
    fn test_reader_large_record() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.wal");

        // Write a large record (spans multiple blocks)
        let large_data: Vec<u8> = (0..BLOCK_SIZE * 2).map(|i| (i % 256) as u8).collect();

        let mut writer = WalWriter::new(&path, 1, SyncMode::None).unwrap();
        writer.add_record(&large_data).unwrap();
        writer.close().unwrap();

        // Read it back
        let mut reader = WalReader::new(&path, 1).unwrap();
        let record = reader.read_record().unwrap().unwrap();
        assert_eq!(&record[..], &large_data[..]);
    }

    #[test]
    fn test_reader_mixed_sizes() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.wal");

        let records: Vec<Vec<u8>> = vec![
            b"small".to_vec(),
            vec![b'x'; 1000],
            b"tiny".to_vec(),
            vec![b'y'; BLOCK_SIZE + 100], // Spans blocks
            b"end".to_vec(),
        ];

        // Write records
        let mut writer = WalWriter::new(&path, 1, SyncMode::None).unwrap();
        for record in &records {
            writer.add_record(record).unwrap();
        }
        writer.close().unwrap();

        // Read them back
        let mut reader = WalReader::new(&path, 1).unwrap();
        for expected in &records {
            let record = reader.read_record().unwrap().unwrap();
            assert_eq!(&record[..], &expected[..]);
        }

        assert!(reader.read_record().unwrap().is_none());
    }

    #[test]
    fn test_reader_seek_to_start() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.wal");

        // Write records
        let mut writer = WalWriter::new(&path, 1, SyncMode::None).unwrap();
        writer.add_record(b"first").unwrap();
        writer.add_record(b"second").unwrap();
        writer.close().unwrap();

        // Read all
        let mut reader = WalReader::new(&path, 1).unwrap();
        reader.read_record().unwrap();
        reader.read_record().unwrap();
        assert!(reader.read_record().unwrap().is_none());

        // Seek and read again
        reader.seek_to_start().unwrap();
        let record = reader.read_record().unwrap().unwrap();
        assert_eq!(&record[..], b"first");
    }
}
