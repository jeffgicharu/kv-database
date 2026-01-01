# Technical Architecture

## 1. System Overview

### High-Level Architecture

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                                  rustdb                                       │
│                                                                               │
│  ┌─────────────────────────────────────────────────────────────────────────┐ │
│  │                           Public API Layer                               │ │
│  │  Database::open() | get() | put() | delete() | begin_transaction()      │ │
│  └─────────────────────────────────────────────────────────────────────────┘ │
│                                      │                                        │
│  ┌───────────────────────────────────┼───────────────────────────────────┐   │
│  │                          Transaction Layer                             │   │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐   │   │
│  │  │ Transaction │  │  Snapshot   │  │   MVCC      │  │  Conflict   │   │   │
│  │  │  Manager    │  │  Manager    │  │  Version    │  │  Detector   │   │   │
│  │  └─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘   │   │
│  └───────────────────────────────────┼───────────────────────────────────┘   │
│                                      │                                        │
│  ┌───────────────────────────────────┼───────────────────────────────────┐   │
│  │                           Storage Engine                               │   │
│  │                                                                        │   │
│  │   ┌─────────────────────────────────────────────────────────────┐     │   │
│  │   │                     Write Path                               │     │   │
│  │   │  ┌─────────┐    ┌─────────┐    ┌─────────────────┐          │     │   │
│  │   │  │  WAL    │───▶│MemTable│───▶│ Flush to SSTable │          │     │   │
│  │   │  │ Writer  │    │ (Active)│    │                  │          │     │   │
│  │   │  └─────────┘    └─────────┘    └─────────────────┘          │     │   │
│  │   └─────────────────────────────────────────────────────────────┘     │   │
│  │                                                                        │   │
│  │   ┌─────────────────────────────────────────────────────────────┐     │   │
│  │   │                     Read Path                                │     │   │
│  │   │  ┌─────────┐    ┌─────────────┐    ┌─────────────┐          │     │   │
│  │   │  │MemTable│───▶│ Immutable   │───▶│  SSTable    │          │     │   │
│  │   │  │         │    │ MemTables   │    │  Levels     │          │     │   │
│  │   │  └─────────┘    └─────────────┘    └─────────────┘          │     │   │
│  │   │       │                                   │                  │     │   │
│  │   │       └──────────▶ Block Cache ◀──────────┘                  │     │   │
│  │   └─────────────────────────────────────────────────────────────┘     │   │
│  │                                                                        │   │
│  │   ┌─────────────────────────────────────────────────────────────┐     │   │
│  │   │                   Background Tasks                           │     │   │
│  │   │  ┌───────────────┐    ┌───────────────┐    ┌─────────────┐  │     │   │
│  │   │  │  Compaction   │    │    Flush      │    │   GC        │  │     │   │
│  │   │  │  Scheduler    │    │   Scheduler   │    │  (Versions) │  │     │   │
│  │   │  └───────────────┘    └───────────────┘    └─────────────┘  │     │   │
│  │   └─────────────────────────────────────────────────────────────┘     │   │
│  │                                                                        │   │
│  └────────────────────────────────────────────────────────────────────────┘  │
│                                                                               │
│  ┌─────────────────────────────────────────────────────────────────────────┐ │
│  │                          File Management                                 │ │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐    │ │
│  │  │  Manifest   │  │   WAL       │  │  SSTable    │  │   LOCK      │    │ │
│  │  │  (versions) │  │  (durability)│ │  (data)     │  │  (single)   │    │ │
│  │  └─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘    │ │
│  └─────────────────────────────────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────────────────────────────┘
```

---

## 2. Component Breakdown

### 2.1 Core Components

| Component | Responsibility | Key Data Structures |
|-----------|---------------|---------------------|
| **Database** | Entry point, coordinates all components | Arc to shared state |
| **TransactionManager** | Track active transactions, assign sequence numbers | DashMap<TxnId, TxnState> |
| **MemTable** | In-memory sorted storage | Lock-free SkipList |
| **WAL** | Durability via append-only log | File + Buffer |
| **SSTable** | On-disk sorted storage | File + Index + BloomFilter |
| **Manifest** | Track current file versions | Append-only log |
| **Compactor** | Background level merging | Priority queue of tasks |
| **BlockCache** | Cache frequently accessed blocks | LRU cache |
| **VersionSet** | Current view of all levels | Arc<Version> |

### 2.2 Component Diagram

```
                    ┌─────────────────────────────────┐
                    │           Database              │
                    │  - options: Options             │
                    │  - state: Arc<DatabaseState>    │
                    └───────────────┬─────────────────┘
                                    │
            ┌───────────────────────┼───────────────────────┐
            │                       │                       │
            ▼                       ▼                       ▼
┌───────────────────┐   ┌───────────────────┐   ┌───────────────────┐
│ TransactionManager│   │   StorageEngine   │   │   Compactor       │
│                   │   │                   │   │                   │
│ - next_txn_id     │   │ - memtable        │   │ - version_set     │
│ - active_txns     │   │ - imm_memtables   │   │ - task_queue      │
│ - sequence_number │   │ - wal             │   │ - workers         │
└───────────────────┘   │ - version_set     │   └───────────────────┘
                        │ - block_cache     │
                        └─────────┬─────────┘
                                  │
        ┌─────────────────────────┼─────────────────────────┐
        │                         │                         │
        ▼                         ▼                         ▼
┌───────────────┐       ┌───────────────┐       ┌───────────────┐
│   MemTable    │       │   VersionSet  │       │  BlockCache   │
│               │       │               │       │               │
│ - skiplist    │       │ - current     │       │ - cache       │
│ - size        │       │ - manifest    │       │ - capacity    │
│ - min_seq     │       │ - next_file   │       │ - hits/misses │
│ - max_seq     │       │               │       │               │
└───────────────┘       └───────┬───────┘       └───────────────┘
                                │
                                ▼
                        ┌───────────────┐
                        │    Version    │
                        │               │
                        │ - levels[]    │
                        │   - SSTable[] │
                        └───────────────┘
```

---

## 3. Data Structures

### 3.1 Key-Value Entry (Internal)

```rust
/// Internal representation of a key-value pair with version information
pub struct InternalKey {
    /// User key (raw bytes)
    pub user_key: Vec<u8>,
    /// Sequence number (version)
    pub sequence: u64,
    /// Value type (Put or Delete)
    pub value_type: ValueType,
}

pub enum ValueType {
    Put = 1,
    Delete = 2,
}

/// Encoded format (for storage)
/// ┌────────────────┬──────────────┬───────────┐
/// │   user_key     │  sequence    │ value_type│
/// │   (variable)   │  (7 bytes)   │ (1 byte)  │
/// └────────────────┴──────────────┴───────────┘
///
/// Sequence and value_type packed into 8 bytes:
/// bits 0-7:   value_type
/// bits 8-63:  sequence number (56 bits = ~72 quadrillion versions)
```

### 3.2 MemTable

```rust
/// In-memory sorted structure using skip list
pub struct MemTable {
    /// Lock-free skip list (key -> value)
    /// Keys are InternalKey encoded
    skiplist: SkipList<Bytes, Bytes>,

    /// Approximate size in bytes
    size: AtomicUsize,

    /// Sequence number range
    min_sequence: AtomicU64,
    max_sequence: AtomicU64,

    /// Reference count for readers
    refs: AtomicUsize,
}

impl MemTable {
    /// Insert key-value pair
    /// Returns size delta
    pub fn put(&self, key: &InternalKey, value: &[u8]) -> usize;

    /// Get value for key at or before given sequence
    pub fn get(&self, key: &[u8], sequence: u64) -> Option<LookupResult>;

    /// Create iterator over all entries
    pub fn iter(&self) -> MemTableIterator;
}

pub enum LookupResult {
    Found(Vec<u8>),
    Deleted,
    NotFound,
}
```

### 3.3 SkipList

```rust
/// Lock-free skip list for concurrent access
pub struct SkipList<K, V> {
    head: AtomicPtr<Node<K, V>>,
    max_height: AtomicUsize,
    arena: Arena,  // Memory allocator
}

struct Node<K, V> {
    key: K,
    value: V,
    height: usize,
    next: [AtomicPtr<Node<K, V>>; MAX_HEIGHT],  // MAX_HEIGHT = 12
}

/// Probability for each level = 1/4
/// Expected height = log4(n)
/// MAX_HEIGHT = 12 supports ~16 million entries efficiently
```

### 3.4 SSTable Format

```rust
/// On-disk sorted table structure
pub struct SSTable {
    /// File handle
    file: File,

    /// Metadata
    file_number: u64,
    file_size: u64,

    /// Key range
    smallest_key: Vec<u8>,
    largest_key: Vec<u8>,

    /// Index for block lookup
    index: BlockIndex,

    /// Bloom filter for fast negative lookups
    bloom_filter: BloomFilter,

    /// Cached footer
    footer: Footer,
}

/// SSTable File Format:
/// ┌─────────────────────────────────────────────────────┐
/// │                    Data Blocks                       │
/// │  ┌─────────────────────────────────────────────┐    │
/// │  │ Block 0                                      │    │
/// │  │ ┌──────────┬───────────────────────────────┐│    │
/// │  │ │ Entry 0  │ [shared][non_shared][value_len]││   │
/// │  │ │          │ [key_delta][value]            ││    │
/// │  │ ├──────────┼───────────────────────────────┤│    │
/// │  │ │ Entry 1  │ ...                           ││    │
/// │  │ └──────────┴───────────────────────────────┘│    │
/// │  │ [restart_points...] [num_restarts]          │    │
/// │  │ [compression_type] [crc32]                  │    │
/// │  └─────────────────────────────────────────────┘    │
/// │  ┌─────────────────────────────────────────────┐    │
/// │  │ Block 1 ...                                  │    │
/// │  └─────────────────────────────────────────────┘    │
/// ├─────────────────────────────────────────────────────┤
/// │                   Filter Block                       │
/// │  [bloom_filter_data]                                │
/// ├─────────────────────────────────────────────────────┤
/// │                   Stats Block                        │
/// │  [entry_count] [raw_key_size] [raw_value_size] ...  │
/// ├─────────────────────────────────────────────────────┤
/// │                   Index Block                        │
/// │  [last_key_block_0] [offset_0] [size_0]             │
/// │  [last_key_block_1] [offset_1] [size_1]             │
/// │  ...                                                 │
/// ├─────────────────────────────────────────────────────┤
/// │                     Footer                           │
/// │  [metaindex_handle] [index_handle] [magic_number]   │
/// │  (48 bytes fixed)                                    │
/// └─────────────────────────────────────────────────────┘
```

### 3.5 WAL Format

```rust
/// Write-ahead log for durability
pub struct WAL {
    file: BufWriter<File>,
    file_number: u64,
    sync_mode: SyncMode,
    block_offset: usize,  // Position within 32KB block
}

/// WAL uses fixed 32KB blocks
const BLOCK_SIZE: usize = 32 * 1024;

/// Record format within block:
/// ┌──────────┬──────────┬──────────┬───────────────┐
/// │ CRC32    │ Length   │ Type     │ Payload       │
/// │ (4 bytes)│ (2 bytes)│ (1 byte) │ (variable)    │
/// └──────────┴──────────┴──────────┴───────────────┘
///
/// Record types:
///   FULL   = 1  // Complete record in this fragment
///   FIRST  = 2  // First fragment of record
///   MIDDLE = 3  // Middle fragment
///   LAST   = 4  // Last fragment

/// Batch format (payload):
/// ┌──────────┬──────────┬───────────────────────────┐
/// │ Sequence │ Count    │ Records...                │
/// │ (8 bytes)│ (4 bytes)│                           │
/// └──────────┴──────────┴───────────────────────────┘
///
/// Each record:
/// ┌──────────┬──────────┬────────┬──────────┬───────┐
/// │ Type     │ Key Len  │ Key    │ Value Len│ Value │
/// │ (1 byte) │ (varint) │        │ (varint) │       │
/// └──────────┴──────────┴────────┴──────────┴───────┘
```

### 3.6 Manifest Format

```rust
/// Manifest tracks current version of the database
pub struct Manifest {
    file: BufWriter<File>,
    file_number: u64,
}

/// Version edit (append to manifest)
pub struct VersionEdit {
    /// Log number (WAL file)
    pub log_number: Option<u64>,

    /// Next file number to allocate
    pub next_file_number: Option<u64>,

    /// Last sequence number used
    pub last_sequence: Option<u64>,

    /// Compaction pointers (where to resume)
    pub compact_pointers: Vec<(usize, Vec<u8>)>,

    /// Files deleted
    pub deleted_files: Vec<(usize, u64)>,  // (level, file_number)

    /// Files added
    pub new_files: Vec<(usize, FileMetadata)>,
}

pub struct FileMetadata {
    pub file_number: u64,
    pub file_size: u64,
    pub smallest_key: Vec<u8>,
    pub largest_key: Vec<u8>,
}
```

### 3.7 Transaction State

```rust
/// Active transaction state
pub struct Transaction {
    /// Unique transaction ID
    id: u64,

    /// Snapshot sequence number (reads see this version)
    snapshot_sequence: u64,

    /// Buffered writes (not yet committed)
    write_buffer: WriteBatch,

    /// Keys read during transaction (for conflict detection)
    read_set: HashSet<Vec<u8>>,

    /// Keys written during transaction
    write_set: HashSet<Vec<u8>>,

    /// Reference to database
    db: Arc<DatabaseState>,

    /// Transaction status
    status: TransactionStatus,
}

pub enum TransactionStatus {
    Active,
    Committed,
    Aborted,
}

/// Write batch for atomic multi-key writes
pub struct WriteBatch {
    entries: Vec<BatchEntry>,
    size: usize,
}

pub struct BatchEntry {
    key: Vec<u8>,
    value: Option<Vec<u8>>,  // None = delete
}
```

### 3.8 Version and VersionSet

```rust
/// Immutable snapshot of database state
pub struct Version {
    /// Files at each level
    files: [Vec<Arc<FileMetadata>>; MAX_LEVELS],

    /// Reference count
    refs: AtomicUsize,

    /// Version set this belongs to
    vset: Weak<VersionSet>,
}

/// Manages versions and file lifecycle
pub struct VersionSet {
    /// Database directory
    db_path: PathBuf,

    /// Current version
    current: ArcSwap<Version>,

    /// Manifest file
    manifest: Mutex<Manifest>,

    /// Next file number to allocate
    next_file_number: AtomicU64,

    /// Last sequence number
    last_sequence: AtomicU64,

    /// Log file number
    log_number: AtomicU64,

    /// Previous log number (during recovery)
    prev_log_number: AtomicU64,
}

const MAX_LEVELS: usize = 7;

/// Level size targets (leveled compaction)
/// L0: special (trigger based on file count)
/// L1: 256 MB
/// L2: 2.56 GB
/// L3: 25.6 GB
/// L4: 256 GB
/// ...
```

---

## 4. Key Flows

### 4.1 Write Path

```
User: db.put(key, value)
         │
         ▼
┌─────────────────────────────────┐
│ 1. Acquire sequence number      │  sequence = next_sequence.fetch_add(1)
└─────────────────┬───────────────┘
                  │
                  ▼
┌─────────────────────────────────┐
│ 2. Build WriteBatch             │  Encode key + sequence + value_type
└─────────────────┬───────────────┘
                  │
                  ▼
┌─────────────────────────────────┐
│ 3. Write to WAL                 │  Append to log file
│    (durability)                 │  fsync based on sync_mode
└─────────────────┬───────────────┘
                  │
                  ▼
┌─────────────────────────────────┐
│ 4. Write to MemTable            │  Insert into skip list
│    (visibility)                 │  Update size counter
└─────────────────┬───────────────┘
                  │
                  ▼
┌─────────────────────────────────┐
│ 5. Check MemTable size          │  If size > threshold:
│                                 │    - Make immutable
│                                 │    - Create new active memtable
│                                 │    - Schedule flush
└─────────────────────────────────┘
```

### 4.2 Read Path

```
User: db.get(key)
         │
         ▼
┌─────────────────────────────────┐
│ 1. Get current sequence         │  read_sequence = last_sequence.load()
└─────────────────┬───────────────┘
                  │
                  ▼
┌─────────────────────────────────┐
│ 2. Search Active MemTable       │  Look for key at read_sequence
│                                 │  If found → return
└─────────────────┬───────────────┘
                  │ Not found
                  ▼
┌─────────────────────────────────┐
│ 3. Search Immutable MemTables   │  Check each (newest first)
│                                 │  If found → return
└─────────────────┬───────────────┘
                  │ Not found
                  ▼
┌─────────────────────────────────┐
│ 4. Search Level 0               │  Check all L0 files (may overlap)
│                                 │  Use bloom filter first
│                                 │  If found → return
└─────────────────┬───────────────┘
                  │ Not found
                  ▼
┌─────────────────────────────────┐
│ 5. Search Levels 1-N            │  Binary search to find file
│                                 │  Files don't overlap within level
│                                 │  Check bloom filter
│                                 │  If found → return
└─────────────────┬───────────────┘
                  │ Not found in any level
                  ▼
            Return None
```

### 4.3 Flush Path (MemTable → SSTable)

```
Trigger: MemTable size > threshold
         │
         ▼
┌─────────────────────────────────┐
│ 1. Make MemTable Immutable      │  Swap pointer atomically
│                                 │  Create new active MemTable
└─────────────────┬───────────────┘
                  │
                  ▼
┌─────────────────────────────────┐
│ 2. Iterate MemTable (sorted)    │  Skip list iteration
└─────────────────┬───────────────┘
                  │
                  ▼
┌─────────────────────────────────┐
│ 3. Build SSTable                │  Write data blocks
│                                 │  Build bloom filter
│                                 │  Write index block
│                                 │  Write footer
│                                 │  fsync file
└─────────────────┬───────────────┘
                  │
                  ▼
┌─────────────────────────────────┐
│ 4. Update Manifest              │  Append VersionEdit
│                                 │  Add new file to L0
│                                 │  fsync manifest
└─────────────────┬───────────────┘
                  │
                  ▼
┌─────────────────────────────────┐
│ 5. Install new Version          │  Atomic swap via ArcSwap
└─────────────────┬───────────────┘
                  │
                  ▼
┌─────────────────────────────────┐
│ 6. Delete old WAL               │  Safe after flush complete
└─────────────────────────────────┘
```

### 4.4 Compaction Path

```
Trigger: L0 files > threshold OR level size > target
         │
         ▼
┌─────────────────────────────────┐
│ 1. Pick Compaction              │  L0→L1: Pick overlapping files
│                                 │  Ln→Ln+1: Round-robin key range
└─────────────────┬───────────────┘
                  │
                  ▼
┌─────────────────────────────────┐
│ 2. Create Merge Iterator        │  Priority queue of iterators
│                                 │  Ordered by key then sequence
└─────────────────┬───────────────┘
                  │
                  ▼
┌─────────────────────────────────┐
│ 3. Iterate and Merge            │  For each unique user key:
│                                 │    - Keep newest version
│                                 │    - Drop older versions if:
│                                 │      - No active txn needs it
│                                 │      - Newer version exists
│                                 │    - Drop tombstones if:
│                                 │      - No older version in lower levels
└─────────────────┬───────────────┘
                  │
                  ▼
┌─────────────────────────────────┐
│ 4. Write New SSTable(s)         │  Split at ~64MB boundaries
│                                 │  Respect level key boundaries
└─────────────────┬───────────────┘
                  │
                  ▼
┌─────────────────────────────────┐
│ 5. Update Manifest              │  VersionEdit:
│                                 │    - deleted_files (old)
│                                 │    - new_files (compaction output)
└─────────────────┬───────────────┘
                  │
                  ▼
┌─────────────────────────────────┐
│ 6. Install New Version          │  Atomic swap
└─────────────────┬───────────────┘
                  │
                  ▼
┌─────────────────────────────────┐
│ 7. Delete Old Files             │  After no readers reference them
└─────────────────────────────────┘
```

### 4.5 Transaction Flow (Optimistic)

```
User: txn = db.begin_transaction()
         │
         ▼
┌─────────────────────────────────┐
│ 1. Assign Transaction ID        │  txn_id = next_txn_id.fetch_add(1)
└─────────────────┬───────────────┘
                  │
                  ▼
┌─────────────────────────────────┐
│ 2. Capture Snapshot             │  snapshot_seq = last_sequence.load()
└─────────────────┬───────────────┘
                  │
                  ▼
User: txn.get(key)
         │
         ▼
┌─────────────────────────────────┐
│ 3. Read at Snapshot             │  Use snapshot_seq for reads
│                                 │  Add key to read_set
└─────────────────────────────────┘
         │
         ▼
User: txn.put(key, value)
         │
         ▼
┌─────────────────────────────────┐
│ 4. Buffer Write                 │  Add to write_buffer
│                                 │  Add key to write_set
│                                 │  (not yet visible)
└─────────────────────────────────┘
         │
         ▼
User: txn.commit()
         │
         ▼
┌─────────────────────────────────┐
│ 5. Check for Conflicts          │  For each key in write_set:
│                                 │    Check if written by another txn
│                                 │    since our snapshot_seq
│                                 │  If conflict → abort
└─────────────────┬───────────────┘
                  │ No conflict
                  ▼
┌─────────────────────────────────┐
│ 6. Assign Commit Sequence       │  commit_seq = next_sequence.fetch_add(n)
└─────────────────┬───────────────┘
                  │
                  ▼
┌─────────────────────────────────┐
│ 7. Write to WAL + MemTable      │  All writes with commit_seq
│                                 │  Now visible to new readers
└─────────────────┬───────────────┘
                  │
                  ▼
┌─────────────────────────────────┐
│ 8. Mark Transaction Complete    │  Remove from active transactions
└─────────────────────────────────┘
```

### 4.6 Recovery Flow

```
Database startup
         │
         ▼
┌─────────────────────────────────┐
│ 1. Acquire Lock File            │  Prevent concurrent access
│                                 │  LOCK file in db directory
└─────────────────┬───────────────┘
                  │
                  ▼
┌─────────────────────────────────┐
│ 2. Read CURRENT file            │  Points to active manifest
│                                 │  e.g., "MANIFEST-000007"
└─────────────────┬───────────────┘
                  │
                  ▼
┌─────────────────────────────────┐
│ 3. Replay Manifest              │  Read all VersionEdits
│                                 │  Reconstruct current Version
│                                 │  Know which files exist
└─────────────────┬───────────────┘
                  │
                  ▼
┌─────────────────────────────────┐
│ 4. Replay WAL                   │  For each record in WAL:
│                                 │    Skip if sequence ≤ last_sequence
│                                 │    Apply to MemTable
│                                 │  Truncate at first bad CRC
└─────────────────┬───────────────┘
                  │
                  ▼
┌─────────────────────────────────┐
│ 5. Delete Orphaned Files        │  Files not in manifest
│                                 │  (from crashed compaction)
└─────────────────┬───────────────┘
                  │
                  ▼
┌─────────────────────────────────┐
│ 6. Open for Business            │  Create new WAL
│                                 │  Ready for reads and writes
└─────────────────────────────────┘
```

---

## 5. Module Organization

```
rustdb/
├── Cargo.toml
├── src/
│   ├── lib.rs                    # Public API exports
│   ├── db.rs                     # Database struct, main entry point
│   ├── options.rs                # Configuration options
│   ├── error.rs                  # Error types
│   │
│   ├── memtable/
│   │   ├── mod.rs                # MemTable implementation
│   │   ├── skiplist.rs           # Lock-free skip list
│   │   └── arena.rs              # Memory arena allocator
│   │
│   ├── wal/
│   │   ├── mod.rs                # WAL writer and reader
│   │   ├── reader.rs             # Recovery reading
│   │   └── writer.rs             # Append writing
│   │
│   ├── sstable/
│   │   ├── mod.rs                # SSTable module
│   │   ├── reader.rs             # Read SSTable files
│   │   ├── writer.rs             # Write SSTable files
│   │   ├── block.rs              # Data block format
│   │   ├── block_builder.rs      # Build data blocks
│   │   ├── filter.rs             # Bloom filter
│   │   └── iterator.rs           # SSTable iterator
│   │
│   ├── version/
│   │   ├── mod.rs                # Version management
│   │   ├── version.rs            # Immutable version snapshot
│   │   ├── version_set.rs        # Manage versions
│   │   ├── version_edit.rs       # Version changes
│   │   └── manifest.rs           # Manifest file I/O
│   │
│   ├── compaction/
│   │   ├── mod.rs                # Compaction module
│   │   ├── compactor.rs          # Background compaction
│   │   ├── picker.rs             # Choose files to compact
│   │   └── merge_iterator.rs     # Merge sorted iterators
│   │
│   ├── transaction/
│   │   ├── mod.rs                # Transaction module
│   │   ├── transaction.rs        # Transaction struct
│   │   ├── manager.rs            # TransactionManager
│   │   ├── mvcc.rs               # Version visibility
│   │   └── conflict.rs           # Conflict detection
│   │
│   ├── cache/
│   │   ├── mod.rs                # Cache module
│   │   ├── block_cache.rs        # LRU block cache
│   │   └── table_cache.rs        # Cache open SSTable handles
│   │
│   ├── iterator/
│   │   ├── mod.rs                # Iterator traits and impls
│   │   ├── db_iterator.rs        # Database-level iterator
│   │   ├── merge_iterator.rs     # Merge multiple iterators
│   │   └── two_level_iterator.rs # Index + data iteration
│   │
│   ├── util/
│   │   ├── mod.rs                # Utilities
│   │   ├── coding.rs             # Varint, fixed encoding
│   │   ├── crc.rs                # CRC32 checksum
│   │   ├── comparator.rs         # Key comparison
│   │   └── filename.rs           # File naming conventions
│   │
│   └── metrics/
│       ├── mod.rs                # Metrics module
│       └── collector.rs          # Prometheus metrics
│
├── benches/
│   └── benchmark.rs              # Performance benchmarks
│
└── tests/
    ├── basic_test.rs             # Basic CRUD tests
    ├── transaction_test.rs       # ACID tests
    ├── recovery_test.rs          # Crash recovery tests
    ├── compaction_test.rs        # Compaction tests
    └── concurrent_test.rs        # Multi-threaded tests
```

---

## 6. Concurrency Model

### 6.1 Threading Model

```
┌───────────────────────────────────────────────────────────────┐
│                        User Threads                            │
│  ┌───────┐  ┌───────┐  ┌───────┐  ┌───────┐  ┌───────┐       │
│  │ Read  │  │ Read  │  │ Write │  │ Write │  │  Txn  │  ...  │
│  └───┬───┘  └───┬───┘  └───┬───┘  └───┬───┘  └───┬───┘       │
└──────┼──────────┼──────────┼──────────┼──────────┼────────────┘
       │          │          │          │          │
       ▼          ▼          ▼          ▼          ▼
┌───────────────────────────────────────────────────────────────┐
│                     Shared State (Arc)                         │
│                                                                │
│  ┌────────────────┐  ┌────────────────┐  ┌────────────────┐   │
│  │   MemTable     │  │  VersionSet    │  │  BlockCache    │   │
│  │  (lock-free)   │  │ (ArcSwap)      │  │ (sharded LRU)  │   │
│  └────────────────┘  └────────────────┘  └────────────────┘   │
│                                                                │
│  ┌────────────────┐  ┌────────────────┐  ┌────────────────┐   │
│  │    WAL         │  │   Manifest     │  │   Metrics      │   │
│  │  (Mutex)       │  │   (Mutex)      │  │  (atomics)     │   │
│  └────────────────┘  └────────────────┘  └────────────────┘   │
└───────────────────────────────────────────────────────────────┘
       │
       │ Spawned background tasks
       ▼
┌───────────────────────────────────────────────────────────────┐
│                    Background Threads                          │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐         │
│  │   Flusher    │  │  Compactor   │  │  Compactor   │  ...    │
│  │   (1 thread) │  │  (thread 1)  │  │  (thread N)  │         │
│  └──────────────┘  └──────────────┘  └──────────────┘         │
└───────────────────────────────────────────────────────────────┘
```

### 6.2 Synchronization Strategy

| Component | Synchronization | Rationale |
|-----------|-----------------|-----------|
| MemTable | Lock-free SkipList | High read/write concurrency |
| WAL | Mutex | Single writer at a time |
| Manifest | Mutex | Single modifier at a time |
| VersionSet.current | ArcSwap | Lock-free version swaps |
| BlockCache | Sharded LRU | Reduce contention |
| Sequence number | AtomicU64 | Lock-free increment |
| File numbers | AtomicU64 | Lock-free allocation |
| Active transactions | DashMap | Concurrent map |
| Metrics | AtomicU64 | Lock-free counters |

### 6.3 Critical Sections

```rust
// Write path critical section (minimal)
fn write_batch(&self, batch: &WriteBatch) -> Result<()> {
    // 1. Allocate sequence numbers (atomic, no lock)
    let sequence = self.sequence.fetch_add(batch.count(), Ordering::SeqCst);

    // 2. Write to WAL (requires lock, but just file append)
    {
        let mut wal = self.wal.lock();
        wal.write(sequence, batch)?;
    } // Lock released

    // 3. Write to MemTable (lock-free)
    for (i, entry) in batch.entries.iter().enumerate() {
        self.memtable.put(entry.key, sequence + i, entry.value);
    }

    Ok(())
}
```

---

## 7. File Layout

### 7.1 Directory Structure

```
/path/to/database/
├── LOCK                    # Lock file (prevents concurrent access)
├── CURRENT                 # Points to current manifest
├── MANIFEST-000001         # Version history (may have multiple)
├── MANIFEST-000005         # Current manifest
├── 000003.log              # Active WAL
├── 000010.sst              # SSTable files
├── 000011.sst
├── 000012.sst
├── 000015.sst
└── 000016.sst
```

### 7.2 File Naming Convention

| Pattern | Description |
|---------|-------------|
| `LOCK` | Single-process lock |
| `CURRENT` | Text file: name of current manifest |
| `MANIFEST-NNNNNN` | Manifest file (version history) |
| `NNNNNN.log` | Write-ahead log |
| `NNNNNN.sst` | SSTable (sorted string table) |
| `NNNNNN.tmp` | Temporary file (being written) |

All file numbers are 6-digit zero-padded.

---

## 8. Dependencies

```toml
[package]
name = "rustdb"
version = "0.1.0"
edition = "2021"

[dependencies]
# Concurrency
crossbeam-skiplist = "0.1"     # Lock-free skip list
parking_lot = "0.12"           # Fast mutexes
arc-swap = "1"                 # Atomic Arc swapping
dashmap = "5"                  # Concurrent HashMap

# Serialization
bytes = "1"                    # Byte buffer utilities
integer-encoding = "4"         # Varint encoding

# Compression
lz4 = "1"                      # LZ4 compression
snap = "1"                     # Snappy compression

# Hashing
crc32fast = "1"                # CRC32 checksums
xxhash-rust = "0.8"            # Fast hashing for bloom filter

# Async (optional, for background tasks)
tokio = { version = "1", features = ["rt-multi-thread", "sync", "fs"] }

# Logging
tracing = "0.1"
tracing-subscriber = "0.3"

# Metrics
prometheus-client = "0.22"

# Error handling
thiserror = "1"

[dev-dependencies]
tempfile = "3"                 # Temp directories for tests
rand = "0.8"                   # Random data generation
criterion = "0.5"              # Benchmarking

[profile.release]
lto = true
codegen-units = 1
panic = "abort"
```

---

## 9. Performance Considerations

### 9.1 Memory Optimization

| Area | Technique |
|------|-----------|
| MemTable | Arena allocator to reduce fragmentation |
| Block Cache | Sharded LRU to reduce contention |
| Bloom Filter | ~10 bits per key for 1% false positive |
| Iterator | Lazy loading, don't load all blocks |

### 9.2 I/O Optimization

| Area | Technique |
|------|-----------|
| WAL | Buffered writes, configurable fsync |
| SSTable writes | Sequential, no seeking |
| SSTable reads | Binary search index, bloom filter |
| Compaction | Background, doesn't block reads/writes |

### 9.3 Concurrency Optimization

| Area | Technique |
|------|-----------|
| Reads | Lock-free from MemTable and VersionSet |
| Writes | Only WAL requires lock (brief) |
| Background | Separate threads for flush/compaction |
| Version | ArcSwap for instant atomic swaps |

---

## 10. Error Handling

### 10.1 Error Types

```rust
#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Corruption detected: {0}")]
    Corruption(String),

    #[error("Transaction conflict: key was modified by another transaction")]
    TransactionConflict,

    #[error("Database is closed")]
    DatabaseClosed,

    #[error("Invalid configuration: {0}")]
    InvalidConfiguration(String),

    #[error("Key too large: {0} bytes (max: {1})")]
    KeyTooLarge(usize, usize),

    #[error("Value too large: {0} bytes (max: {1})")]
    ValueTooLarge(usize, usize),

    #[error("Write buffer full")]
    WriteBufferFull,

    #[error("Too many open files")]
    TooManyOpenFiles,
}
```

### 10.2 Recovery Strategy

| Failure | Recovery Action |
|---------|-----------------|
| Crash during write | Replay WAL, truncate incomplete record |
| Crash during flush | Delete incomplete SSTable, re-flush |
| Crash during compaction | Keep old files, delete partial outputs |
| Corrupted WAL record | Truncate at corruption point |
| Corrupted SSTable | Mark as bad, report error |
| Missing file | Report corruption, fail open |
| Bad CRC | Report corruption |

---

## 11. Testing Strategy

### 11.1 Unit Tests

- SkipList: insert, lookup, iteration, concurrent access
- WAL: write, read, recovery, truncation
- SSTable: build, read, bloom filter accuracy
- Block: compression, encoding, checksums
- Manifest: write, replay, version reconstruction
- MVCC: visibility, version ordering

### 11.2 Integration Tests

- Basic CRUD operations
- Transaction commit and rollback
- Conflict detection
- Crash recovery simulation
- Compaction correctness
- Iterator consistency

### 11.3 Stress Tests

- High write throughput
- Many concurrent transactions
- Large dataset (gigabytes)
- Random read/write patterns
- Repeated crash/recovery cycles

---

## 12. Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `rustdb_reads_total` | Counter | Total get operations |
| `rustdb_writes_total` | Counter | Total put/delete operations |
| `rustdb_read_latency_seconds` | Histogram | Read latency distribution |
| `rustdb_write_latency_seconds` | Histogram | Write latency distribution |
| `rustdb_bytes_written` | Counter | Bytes written to WAL |
| `rustdb_bytes_compacted` | Counter | Bytes processed by compaction |
| `rustdb_memtable_size_bytes` | Gauge | Current memtable size |
| `rustdb_level_sst_count` | Gauge | SSTables per level |
| `rustdb_level_bytes` | Gauge | Bytes per level |
| `rustdb_block_cache_hits` | Counter | Cache hits |
| `rustdb_block_cache_misses` | Counter | Cache misses |
| `rustdb_bloom_filter_useful` | Counter | Negative lookups avoided |
| `rustdb_compaction_seconds` | Histogram | Compaction duration |
| `rustdb_flush_seconds` | Histogram | Flush duration |
| `rustdb_active_transactions` | Gauge | Active transaction count |
| `rustdb_stalled_writes` | Counter | Writes stalled for compaction |
