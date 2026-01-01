//! Version Management for tracking active SSTable files.
//!
//! The version system provides:
//! - **FileMetadata**: Information about each SSTable file
//! - **Version**: Immutable snapshot of all files organized by level
//! - **VersionEdit**: Delta/changes to apply to a Version
//! - **Manifest**: Append-only log of VersionEdits for recovery
//! - **VersionSet**: Manages the current Version with atomic updates
//!
//! # LSM-Tree File Organization
//!
//! ```text
//! Level 0:  [SST-1] [SST-2] [SST-3]  (overlapping keys, newest first)
//! Level 1:  [SST-4][SST-5][SST-6]    (non-overlapping, sorted)
//! Level 2:  [SST-7][SST-8][SST-9][SST-10]  (non-overlapping, sorted)
//! ...
//! ```
//!
//! # Recovery
//!
//! On startup:
//! 1. Read CURRENT file to find active manifest
//! 2. Replay all VersionEdits from manifest
//! 3. Reconstruct the current Version

mod file_metadata;
mod manifest;
mod version;
mod version_edit;
mod version_set;

pub use file_metadata::FileMetadata;
pub use manifest::{manifest_file_path, parse_manifest_filename, Manifest, ManifestReader, ManifestWriter};
pub use version::Version;
pub use version_edit::VersionEdit;
pub use version_set::{database_exists, read_current_manifest, write_current_file, VersionSet};

use crate::options::MAX_LEVELS;

/// Tag values for encoding VersionEdit fields.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum EditTag {
    /// Comparator name.
    Comparator = 1,
    /// Log number.
    LogNumber = 2,
    /// Next file number.
    NextFileNumber = 3,
    /// Last sequence number.
    LastSequence = 4,
    /// Compaction pointer for a level.
    CompactPointer = 5,
    /// Deleted file (level, file_number).
    DeletedFile = 6,
    /// New file (level, file_number, size, smallest, largest).
    NewFile = 7,
    /// Previous log number (deprecated but kept for compatibility).
    PrevLogNumber = 9,
}

impl EditTag {
    /// Create from byte.
    pub fn from_byte(b: u8) -> Option<Self> {
        match b {
            1 => Some(EditTag::Comparator),
            2 => Some(EditTag::LogNumber),
            3 => Some(EditTag::NextFileNumber),
            4 => Some(EditTag::LastSequence),
            5 => Some(EditTag::CompactPointer),
            6 => Some(EditTag::DeletedFile),
            7 => Some(EditTag::NewFile),
            9 => Some(EditTag::PrevLogNumber),
            _ => None,
        }
    }

    /// Convert to byte.
    pub fn to_byte(self) -> u8 {
        self as u8
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_edit_tag_roundtrip() {
        for tag in [
            EditTag::Comparator,
            EditTag::LogNumber,
            EditTag::NextFileNumber,
            EditTag::LastSequence,
            EditTag::CompactPointer,
            EditTag::DeletedFile,
            EditTag::NewFile,
            EditTag::PrevLogNumber,
        ] {
            assert_eq!(EditTag::from_byte(tag.to_byte()), Some(tag));
        }
    }

    #[test]
    fn test_max_levels() {
        assert_eq!(MAX_LEVELS, 7);
    }
}
