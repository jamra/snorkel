//! Persistence module for fast restart from shared memory
//!
//! Provides snapshot-based persistence using memory-mapped files.

pub mod mmap;
pub mod snapshot;

pub use mmap::MmapBackend;
pub use snapshot::SnapshotManager;

use std::path::Path;

/// Trait for persistence backends
pub trait PersistenceBackend: Send + Sync {
    /// Write data to persistence
    fn write(&self, key: &str, data: &[u8]) -> Result<(), PersistenceError>;

    /// Read data from persistence
    fn read(&self, key: &str) -> Result<Option<Vec<u8>>, PersistenceError>;

    /// Delete data from persistence
    fn delete(&self, key: &str) -> Result<(), PersistenceError>;

    /// List all keys
    fn list_keys(&self) -> Result<Vec<String>, PersistenceError>;

    /// Sync to disk
    fn sync(&self) -> Result<(), PersistenceError>;
}

/// Persistence configuration
#[derive(Debug, Clone)]
pub struct PersistenceConfig {
    /// Base directory for persistence files
    pub data_dir: std::path::PathBuf,
    /// Whether to enable memory mapping
    pub enable_mmap: bool,
    /// Snapshot interval in seconds
    pub snapshot_interval_secs: u64,
    /// Maximum snapshot size in bytes
    pub max_snapshot_size: usize,
}

impl Default for PersistenceConfig {
    fn default() -> Self {
        Self {
            data_dir: std::path::PathBuf::from("./snorkel_data"),
            enable_mmap: true,
            snapshot_interval_secs: 300, // 5 minutes
            max_snapshot_size: 1024 * 1024 * 1024, // 1GB
        }
    }
}

impl PersistenceConfig {
    pub fn new<P: AsRef<Path>>(data_dir: P) -> Self {
        Self {
            data_dir: data_dir.as_ref().to_path_buf(),
            ..Default::default()
        }
    }

    pub fn with_snapshot_interval(mut self, secs: u64) -> Self {
        self.snapshot_interval_secs = secs;
        self
    }

    pub fn with_mmap(mut self, enabled: bool) -> Self {
        self.enable_mmap = enabled;
        self
    }
}

#[derive(Debug, thiserror::Error)]
pub enum PersistenceError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("Deserialization error: {0}")]
    Deserialization(String),

    #[error("Snapshot not found: {0}")]
    SnapshotNotFound(String),

    #[error("Corrupted data: {0}")]
    Corrupted(String),

    #[error("Out of space")]
    OutOfSpace,
}
