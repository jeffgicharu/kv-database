//! Database file naming conventions.

use std::path::{Path, PathBuf};

/// File types in the database directory.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileType {
    /// Write-ahead log file.
    Log,
    /// Lock file to prevent concurrent access.
    Lock,
    /// SSTable data file.
    Table,
    /// Manifest file (version history).
    Manifest,
    /// Current file (points to current manifest).
    Current,
    /// Temporary file.
    Temp,
    /// Info log file.
    InfoLog,
}

/// Generate the lock file path.
pub fn lock_file_path(db_path: &Path) -> PathBuf {
    db_path.join("LOCK")
}

/// Generate the current file path.
pub fn current_file_path(db_path: &Path) -> PathBuf {
    db_path.join("CURRENT")
}

/// Generate a manifest file path.
pub fn manifest_file_path(db_path: &Path, number: u64) -> PathBuf {
    db_path.join(format!("MANIFEST-{:06}", number))
}

/// Generate a log (WAL) file path.
pub fn log_file_path(db_path: &Path, number: u64) -> PathBuf {
    db_path.join(format!("{:06}.log", number))
}

/// Generate an SSTable file path.
pub fn table_file_path(db_path: &Path, number: u64) -> PathBuf {
    db_path.join(format!("{:06}.sst", number))
}

/// Generate a temporary file path.
pub fn temp_file_path(db_path: &Path, number: u64) -> PathBuf {
    db_path.join(format!("{:06}.tmp", number))
}

/// Generate the info log file path.
pub fn info_log_path(db_path: &Path) -> PathBuf {
    db_path.join("LOG")
}

/// Generate the old info log file path.
pub fn old_info_log_path(db_path: &Path) -> PathBuf {
    db_path.join("LOG.old")
}

/// Parse a file name and return its type and number.
///
/// Returns `None` if the file name doesn't match any known pattern.
pub fn parse_file_name(name: &str) -> Option<(FileType, u64)> {
    // Check for special files first
    if name == "CURRENT" {
        return Some((FileType::Current, 0));
    }
    if name == "LOCK" {
        return Some((FileType::Lock, 0));
    }
    if name == "LOG" || name == "LOG.old" {
        return Some((FileType::InfoLog, 0));
    }

    // Check for manifest files: MANIFEST-NNNNNN
    if let Some(suffix) = name.strip_prefix("MANIFEST-") {
        if let Ok(number) = suffix.parse::<u64>() {
            return Some((FileType::Manifest, number));
        }
    }

    // Check for numbered files: NNNNNN.ext
    if let Some(dot_pos) = name.rfind('.') {
        let (num_str, ext) = name.split_at(dot_pos);
        let ext = &ext[1..]; // Skip the dot

        if let Ok(number) = num_str.parse::<u64>() {
            let file_type = match ext {
                "log" => FileType::Log,
                "sst" => FileType::Table,
                "tmp" => FileType::Temp,
                _ => return None,
            };
            return Some((file_type, number));
        }
    }

    None
}

/// Set the current manifest file.
///
/// This atomically updates the CURRENT file to point to the new manifest.
pub fn set_current_file(db_path: &Path, manifest_number: u64) -> std::io::Result<()> {
    let manifest_name = format!("MANIFEST-{:06}", manifest_number);
    let current_path = current_file_path(db_path);
    let temp_path = temp_file_path(db_path, manifest_number);

    // Write to temp file first
    std::fs::write(&temp_path, format!("{}\n", manifest_name))?;

    // Sync the temp file
    let file = std::fs::File::open(&temp_path)?;
    file.sync_all()?;
    drop(file);

    // Atomically rename
    std::fs::rename(&temp_path, &current_path)?;

    Ok(())
}

/// Read the current manifest file name.
pub fn read_current_file(db_path: &Path) -> std::io::Result<String> {
    let content = std::fs::read_to_string(current_file_path(db_path))?;
    Ok(content.trim().to_string())
}

/// Get the manifest number from the CURRENT file.
pub fn get_current_manifest_number(db_path: &Path) -> std::io::Result<u64> {
    let name = read_current_file(db_path)?;

    // Parse MANIFEST-NNNNNN
    if let Some(suffix) = name.strip_prefix("MANIFEST-") {
        if let Ok(number) = suffix.parse::<u64>() {
            return Ok(number);
        }
    }

    Err(std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        format!("Invalid manifest name in CURRENT: {}", name),
    ))
}

/// List all files of a given type in the database directory.
pub fn list_files_of_type(db_path: &Path, file_type: FileType) -> std::io::Result<Vec<u64>> {
    let mut numbers = Vec::new();

    for entry in std::fs::read_dir(db_path)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();

        if let Some((ft, number)) = parse_file_name(&name) {
            if ft == file_type {
                numbers.push(number);
            }
        }
    }

    numbers.sort();
    Ok(numbers)
}

/// Find the maximum file number in the database directory.
pub fn find_max_file_number(db_path: &Path) -> std::io::Result<u64> {
    let mut max_number = 0u64;

    for entry in std::fs::read_dir(db_path)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();

        if let Some((_, number)) = parse_file_name(&name) {
            max_number = max_number.max(number);
        }
    }

    Ok(max_number)
}

/// Check if a file exists.
pub fn file_exists(path: &Path) -> bool {
    path.exists()
}

/// Get the file size.
pub fn file_size(path: &Path) -> std::io::Result<u64> {
    Ok(std::fs::metadata(path)?.len())
}

/// Delete a file, ignoring "not found" errors.
pub fn delete_file(path: &Path) -> std::io::Result<()> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e),
    }
}

/// Create directory if it doesn't exist.
pub fn create_dir_if_missing(path: &Path) -> std::io::Result<()> {
    match std::fs::create_dir_all(path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => Ok(()),
        Err(e) => Err(e),
    }
}

/// Sync a directory to ensure file operations are durable.
pub fn sync_dir(path: &Path) -> std::io::Result<()> {
    let dir = std::fs::File::open(path)?;
    dir.sync_all()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_file_paths() {
        let db_path = Path::new("/data/db");

        assert_eq!(lock_file_path(db_path), Path::new("/data/db/LOCK"));
        assert_eq!(current_file_path(db_path), Path::new("/data/db/CURRENT"));
        assert_eq!(
            manifest_file_path(db_path, 5),
            Path::new("/data/db/MANIFEST-000005")
        );
        assert_eq!(log_file_path(db_path, 123), Path::new("/data/db/000123.log"));
        assert_eq!(
            table_file_path(db_path, 456),
            Path::new("/data/db/000456.sst")
        );
        assert_eq!(temp_file_path(db_path, 789), Path::new("/data/db/000789.tmp"));
    }

    #[test]
    fn test_parse_file_name() {
        assert_eq!(parse_file_name("CURRENT"), Some((FileType::Current, 0)));
        assert_eq!(parse_file_name("LOCK"), Some((FileType::Lock, 0)));
        assert_eq!(
            parse_file_name("MANIFEST-000005"),
            Some((FileType::Manifest, 5))
        );
        assert_eq!(parse_file_name("000123.log"), Some((FileType::Log, 123)));
        assert_eq!(parse_file_name("000456.sst"), Some((FileType::Table, 456)));
        assert_eq!(parse_file_name("000789.tmp"), Some((FileType::Temp, 789)));
        assert_eq!(parse_file_name("LOG"), Some((FileType::InfoLog, 0)));

        assert_eq!(parse_file_name("random.txt"), None);
        assert_eq!(parse_file_name("abc.log"), None);
    }

    #[test]
    fn test_set_and_read_current() {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path();

        set_current_file(db_path, 42).unwrap();

        let name = read_current_file(db_path).unwrap();
        assert_eq!(name, "MANIFEST-000042");

        let number = get_current_manifest_number(db_path).unwrap();
        assert_eq!(number, 42);
    }

    #[test]
    fn test_list_files_of_type() {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path();

        // Create some test files
        std::fs::write(log_file_path(db_path, 1), "").unwrap();
        std::fs::write(log_file_path(db_path, 3), "").unwrap();
        std::fs::write(log_file_path(db_path, 5), "").unwrap();
        std::fs::write(table_file_path(db_path, 2), "").unwrap();
        std::fs::write(table_file_path(db_path, 4), "").unwrap();

        let logs = list_files_of_type(db_path, FileType::Log).unwrap();
        assert_eq!(logs, vec![1, 3, 5]);

        let tables = list_files_of_type(db_path, FileType::Table).unwrap();
        assert_eq!(tables, vec![2, 4]);
    }

    #[test]
    fn test_find_max_file_number() {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path();

        // Empty directory
        assert_eq!(find_max_file_number(db_path).unwrap(), 0);

        // Create some files
        std::fs::write(log_file_path(db_path, 10), "").unwrap();
        std::fs::write(table_file_path(db_path, 20), "").unwrap();
        std::fs::write(manifest_file_path(db_path, 5), "").unwrap();

        assert_eq!(find_max_file_number(db_path).unwrap(), 20);
    }

    #[test]
    fn test_delete_file() {
        let temp_dir = tempdir().unwrap();
        let path = temp_dir.path().join("test.txt");

        // Delete non-existent file should succeed
        assert!(delete_file(&path).is_ok());

        // Create and delete
        std::fs::write(&path, "test").unwrap();
        assert!(path.exists());
        delete_file(&path).unwrap();
        assert!(!path.exists());
    }
}
