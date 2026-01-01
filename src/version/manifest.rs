//! Manifest - Append-only log of VersionEdits.
//!
//! The Manifest persists all changes to the database's file structure.
//! On startup, the manifest is replayed to reconstruct the current Version.
//!
//! The manifest uses the same block-based format as WAL for durability.

use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crate::options::SyncMode;
use crate::util::crc::crc32;
use crate::version::VersionEdit;
use crate::{Error, Result};

/// Block size for manifest (32KB, same as WAL).
const BLOCK_SIZE: usize = 32 * 1024;

/// Header size: CRC (4) + Length (2) + Type (1) = 7 bytes.
const HEADER_SIZE: usize = 7;

/// Record types for manifest entries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
enum RecordType {
    /// Zero is reserved for pre-allocated files.
    Zero = 0,
    /// Complete record in a single fragment.
    Full = 1,
    /// First fragment of a record.
    First = 2,
    /// Middle fragment(s) of a record.
    Middle = 3,
    /// Last fragment of a record.
    Last = 4,
}

impl RecordType {
    fn from_byte(b: u8) -> Option<Self> {
        match b {
            0 => Some(RecordType::Zero),
            1 => Some(RecordType::Full),
            2 => Some(RecordType::First),
            3 => Some(RecordType::Middle),
            4 => Some(RecordType::Last),
            _ => None,
        }
    }

    fn to_byte(self) -> u8 {
        self as u8
    }
}

/// Manifest file writer.
///
/// Appends VersionEdits to the manifest file using a block-based format
/// with CRC checksums for durability.
pub struct ManifestWriter {
    /// Buffered writer for the manifest file.
    writer: BufWriter<File>,
    /// Current position within the current block.
    block_offset: usize,
    /// Sync mode for durability.
    sync_mode: SyncMode,
    /// Bytes written since last sync.
    bytes_since_sync: usize,
    /// File number for this manifest.
    file_number: u64,
    /// Path to the manifest file.
    path: PathBuf,
}

impl ManifestWriter {
    /// Create a new manifest writer.
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
            path: path.to_path_buf(),
        })
    }

    /// Open an existing manifest for appending.
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
            path: path.to_path_buf(),
        })
    }

    /// Get the file number.
    pub fn file_number(&self) -> u64 {
        self.file_number
    }

    /// Get the manifest file path.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Add a VersionEdit to the manifest.
    pub fn add_edit(&mut self, edit: &VersionEdit) -> Result<()> {
        let data = edit.encode();
        self.add_record(&data)
    }

    /// Add a raw record to the manifest.
    fn add_record(&mut self, data: &[u8]) -> Result<()> {
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
        debug_assert!(data.len() <= 0xFFFF);
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

/// Manifest file reader.
///
/// Reads and replays VersionEdits from a manifest file.
pub struct ManifestReader {
    /// Buffered reader for the manifest file.
    reader: BufReader<File>,
    /// Current block buffer.
    buffer: Vec<u8>,
    /// Current position within the buffer.
    buffer_offset: usize,
    /// Valid bytes in the buffer.
    buffer_size: usize,
    /// Whether we've reached EOF.
    eof: bool,
    /// Whether checksum errors are fatal.
    checksum_errors_are_fatal: bool,
    /// File number for this manifest.
    file_number: u64,
}

impl ManifestReader {
    /// Create a new manifest reader.
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

    /// Read the next VersionEdit.
    ///
    /// Returns None when there are no more edits.
    pub fn read_edit(&mut self) -> Result<Option<VersionEdit>> {
        match self.read_record()? {
            Some(data) => {
                let edit = VersionEdit::decode(&data)?;
                Ok(Some(edit))
            }
            None => Ok(None),
        }
    }

    /// Read all VersionEdits from the manifest.
    pub fn read_all_edits(&mut self) -> Result<Vec<VersionEdit>> {
        let mut edits = Vec::new();
        while let Some(edit) = self.read_edit()? {
            edits.push(edit);
        }
        Ok(edits)
    }

    /// Read the next raw record.
    fn read_record(&mut self) -> Result<Option<Vec<u8>>> {
        let mut scratch = Vec::new();
        let mut in_fragmented_record = false;

        loop {
            match self.read_physical_record()? {
                Some((record_type, data)) => match record_type {
                    RecordType::Full => {
                        if in_fragmented_record {
                            scratch.clear();
                        }
                        return Ok(Some(data));
                    }
                    RecordType::First => {
                        if in_fragmented_record {
                            scratch.clear();
                        }
                        scratch.extend_from_slice(&data);
                        in_fragmented_record = true;
                    }
                    RecordType::Middle => {
                        if !in_fragmented_record {
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
                            if self.checksum_errors_are_fatal {
                                return Err(Error::corruption("unexpected last record fragment"));
                            }
                            continue;
                        }
                        scratch.extend_from_slice(&data);
                        return Ok(Some(scratch));
                    }
                    RecordType::Zero => {
                        continue;
                    }
                },
                None => {
                    if in_fragmented_record {
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
                    self.buffer_offset = self.buffer_size;
                    continue;
                }
            };

            // Check if record fits in buffer
            if self.buffer_offset + HEADER_SIZE + length > self.buffer_size {
                if self.eof {
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

/// Manifest file manager.
///
/// Handles manifest file creation, recovery, and switching.
pub struct Manifest {
    /// Current manifest writer.
    writer: ManifestWriter,
    /// Database directory.
    db_path: PathBuf,
    /// Current manifest file number.
    manifest_number: u64,
}

impl Manifest {
    /// Create a new manifest.
    pub fn create(db_path: &Path, manifest_number: u64, sync_mode: SyncMode) -> Result<Self> {
        let manifest_path = manifest_file_path(db_path, manifest_number);
        let writer = ManifestWriter::new(&manifest_path, manifest_number, sync_mode)?;

        Ok(Self {
            writer,
            db_path: db_path.to_path_buf(),
            manifest_number,
        })
    }

    /// Open an existing manifest for appending.
    pub fn open(db_path: &Path, manifest_number: u64, sync_mode: SyncMode) -> Result<Self> {
        let manifest_path = manifest_file_path(db_path, manifest_number);
        let writer = ManifestWriter::open_for_append(&manifest_path, manifest_number, sync_mode)?;

        Ok(Self {
            writer,
            db_path: db_path.to_path_buf(),
            manifest_number,
        })
    }

    /// Get the current manifest number.
    pub fn manifest_number(&self) -> u64 {
        self.manifest_number
    }

    /// Add a VersionEdit to the manifest.
    pub fn log_edit(&mut self, edit: &VersionEdit) -> Result<()> {
        self.writer.add_edit(edit)
    }

    /// Sync the manifest to disk.
    pub fn sync(&mut self) -> Result<()> {
        self.writer.sync()
    }

    /// Recover by reading all edits from a manifest file.
    pub fn recover(db_path: &Path, manifest_number: u64) -> Result<Vec<VersionEdit>> {
        let manifest_path = manifest_file_path(db_path, manifest_number);
        let mut reader = ManifestReader::new(&manifest_path, manifest_number)?;
        reader.read_all_edits()
    }

    /// Close the manifest.
    pub fn close(self) -> Result<()> {
        self.writer.close()
    }
}

/// Generate manifest file path.
pub fn manifest_file_path(db_path: &Path, manifest_number: u64) -> PathBuf {
    db_path.join(format!("MANIFEST-{:06}", manifest_number))
}

/// Parse manifest file name to extract the manifest number.
pub fn parse_manifest_filename(filename: &str) -> Option<u64> {
    if filename.starts_with("MANIFEST-") {
        filename[9..].parse().ok()
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{InternalKey, ValueType};
    use bytes::Bytes;
    use tempfile::tempdir;

    fn make_key(user_key: &[u8], seq: u64) -> InternalKey {
        InternalKey::new(Bytes::copy_from_slice(user_key), seq, ValueType::Value)
    }

    #[test]
    fn test_manifest_writer_reader_empty() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("MANIFEST-000001");

        // Create empty manifest
        let writer = ManifestWriter::new(&path, 1, SyncMode::None).unwrap();
        writer.close().unwrap();

        // Read empty manifest
        let mut reader = ManifestReader::new(&path, 1).unwrap();
        assert!(reader.read_edit().unwrap().is_none());
    }

    #[test]
    fn test_manifest_writer_reader_single_edit() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("MANIFEST-000001");

        // Write edit
        let mut writer = ManifestWriter::new(&path, 1, SyncMode::None).unwrap();

        let mut edit = VersionEdit::new();
        edit.set_comparator("bytewise");
        edit.set_log_number(10);
        edit.set_next_file_number(100);

        writer.add_edit(&edit).unwrap();
        writer.close().unwrap();

        // Read edit
        let mut reader = ManifestReader::new(&path, 1).unwrap();
        let recovered = reader.read_edit().unwrap().unwrap();

        assert_eq!(recovered.comparator.as_deref(), Some("bytewise"));
        assert_eq!(recovered.log_number, Some(10));
        assert_eq!(recovered.next_file_number, Some(100));

        assert!(reader.read_edit().unwrap().is_none());
    }

    #[test]
    fn test_manifest_writer_reader_multiple_edits() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("MANIFEST-000001");

        // Write multiple edits
        let mut writer = ManifestWriter::new(&path, 1, SyncMode::None).unwrap();

        for i in 0..10 {
            let mut edit = VersionEdit::new();
            edit.set_last_sequence(i * 100);
            edit.add_file_info(
                (i % 7) as usize,
                i,
                1024 * (i + 1),
                make_key(b"start", i),
                make_key(b"end", i + 100),
            );
            writer.add_edit(&edit).unwrap();
        }

        writer.close().unwrap();

        // Read all edits
        let mut reader = ManifestReader::new(&path, 1).unwrap();
        let edits = reader.read_all_edits().unwrap();

        assert_eq!(edits.len(), 10);
        for (i, edit) in edits.iter().enumerate() {
            assert_eq!(edit.last_sequence, Some((i as u64) * 100));
            assert_eq!(edit.new_files.len(), 1);
            assert_eq!(edit.new_files[0].0, i % 7);
        }
    }

    #[test]
    fn test_manifest_with_deleted_files() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("MANIFEST-000001");

        // Write edit with deleted files
        let mut writer = ManifestWriter::new(&path, 1, SyncMode::None).unwrap();

        let mut edit = VersionEdit::new();
        edit.delete_file(0, 1);
        edit.delete_file(0, 2);
        edit.delete_file(1, 5);
        edit.add_file_info(1, 10, 2048, make_key(b"a", 1), make_key(b"z", 100));

        writer.add_edit(&edit).unwrap();
        writer.close().unwrap();

        // Read edit
        let mut reader = ManifestReader::new(&path, 1).unwrap();
        let recovered = reader.read_edit().unwrap().unwrap();

        assert_eq!(recovered.deleted_files.len(), 3);
        assert!(recovered.deleted_files.contains(&(0, 1)));
        assert!(recovered.deleted_files.contains(&(0, 2)));
        assert!(recovered.deleted_files.contains(&(1, 5)));
        assert_eq!(recovered.new_files.len(), 1);
    }

    #[test]
    fn test_manifest_with_compact_pointers() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("MANIFEST-000001");

        // Write edit with compact pointers
        let mut writer = ManifestWriter::new(&path, 1, SyncMode::None).unwrap();

        let mut edit = VersionEdit::new();
        edit.set_compact_pointer(1, make_key(b"compact_at", 50));
        edit.set_compact_pointer(2, make_key(b"another_pointer", 100));

        writer.add_edit(&edit).unwrap();
        writer.close().unwrap();

        // Read edit
        let mut reader = ManifestReader::new(&path, 1).unwrap();
        let recovered = reader.read_edit().unwrap().unwrap();

        assert_eq!(recovered.compact_pointers.len(), 2);
        assert!(recovered.compact_pointers.contains_key(&1));
        assert!(recovered.compact_pointers.contains_key(&2));
    }

    #[test]
    fn test_manifest_manager() {
        let dir = tempdir().unwrap();

        // Create manifest
        let mut manifest = Manifest::create(dir.path(), 1, SyncMode::None).unwrap();

        let mut edit = VersionEdit::new();
        edit.set_log_number(5);
        edit.set_next_file_number(50);

        manifest.log_edit(&edit).unwrap();
        manifest.sync().unwrap();
        manifest.close().unwrap();

        // Recover
        let edits = Manifest::recover(dir.path(), 1).unwrap();
        assert_eq!(edits.len(), 1);
        assert_eq!(edits[0].log_number, Some(5));
    }

    #[test]
    fn test_manifest_file_path() {
        let db_path = Path::new("/data/mydb");
        let path = manifest_file_path(db_path, 1);
        assert_eq!(path.to_str().unwrap(), "/data/mydb/MANIFEST-000001");

        let path = manifest_file_path(db_path, 123);
        assert_eq!(path.to_str().unwrap(), "/data/mydb/MANIFEST-000123");
    }

    #[test]
    fn test_parse_manifest_filename() {
        assert_eq!(parse_manifest_filename("MANIFEST-000001"), Some(1));
        assert_eq!(parse_manifest_filename("MANIFEST-000123"), Some(123));
        assert_eq!(parse_manifest_filename("MANIFEST-999999"), Some(999999));
        assert_eq!(parse_manifest_filename("MANIFEST-"), None);
        assert_eq!(parse_manifest_filename("manifest-000001"), None);
        assert_eq!(parse_manifest_filename("000001.sst"), None);
    }

    #[test]
    fn test_manifest_append() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("MANIFEST-000001");

        // Write first edit
        {
            let mut writer = ManifestWriter::new(&path, 1, SyncMode::None).unwrap();
            let mut edit = VersionEdit::new();
            edit.set_log_number(1);
            writer.add_edit(&edit).unwrap();
            writer.close().unwrap();
        }

        // Append second edit
        {
            let mut writer =
                ManifestWriter::open_for_append(&path, 1, SyncMode::None).unwrap();
            let mut edit = VersionEdit::new();
            edit.set_log_number(2);
            writer.add_edit(&edit).unwrap();
            writer.close().unwrap();
        }

        // Read both edits
        let mut reader = ManifestReader::new(&path, 1).unwrap();
        let edits = reader.read_all_edits().unwrap();

        assert_eq!(edits.len(), 2);
        assert_eq!(edits[0].log_number, Some(1));
        assert_eq!(edits[1].log_number, Some(2));
    }
}
