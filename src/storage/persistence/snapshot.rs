//! Snapshot manager for creating and restoring table snapshots

use super::{PersistenceBackend, PersistenceConfig, PersistenceError};
use super::mmap::MmapBackend;
use crate::data::{Table, TableConfig, Value};
use crate::storage::StorageEngine;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

/// Snapshot metadata
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SnapshotMetadata {
    /// Snapshot ID
    pub id: String,
    /// Creation timestamp (epoch ms)
    pub created_at: i64,
    /// Tables included in snapshot
    pub tables: Vec<String>,
    /// Total size in bytes
    pub size_bytes: usize,
    /// Schema version for compatibility
    pub schema_version: u32,
}

/// Serialized table data
#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct TableSnapshot {
    config: TableConfigSnapshot,
    rows: Vec<HashMap<String, Value>>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct TableConfigSnapshot {
    name: String,
    shard_duration_ms: i64,
    ttl_ms: i64,
    max_memory_bytes: usize,
    subsample_threshold_ms: i64,
    subsample_ratio: f64,
    #[serde(default = "default_sample_rate")]
    default_sample_rate: f64,
}

fn default_sample_rate() -> f64 {
    1.0
}

impl From<&TableConfig> for TableConfigSnapshot {
    fn from(config: &TableConfig) -> Self {
        Self {
            name: config.name.clone(),
            shard_duration_ms: config.shard_duration_ms,
            ttl_ms: config.ttl_ms,
            max_memory_bytes: config.max_memory_bytes,
            subsample_threshold_ms: config.subsample_threshold_ms,
            subsample_ratio: config.subsample_ratio,
            default_sample_rate: config.default_sample_rate,
        }
    }
}

impl From<TableConfigSnapshot> for TableConfig {
    fn from(snapshot: TableConfigSnapshot) -> Self {
        Self {
            name: snapshot.name,
            shard_duration_ms: snapshot.shard_duration_ms,
            ttl_ms: snapshot.ttl_ms,
            max_memory_bytes: snapshot.max_memory_bytes,
            subsample_threshold_ms: snapshot.subsample_threshold_ms,
            subsample_ratio: snapshot.subsample_ratio,
            default_sample_rate: snapshot.default_sample_rate,
        }
    }
}

/// Manages snapshots for fast restart
pub struct SnapshotManager {
    backend: MmapBackend,
    #[allow(dead_code)]
    config: PersistenceConfig,
    latest_snapshot: RwLock<Option<SnapshotMetadata>>,
}

impl SnapshotManager {
    /// Create a new snapshot manager
    pub fn new(config: PersistenceConfig) -> Result<Self, PersistenceError> {
        let backend = MmapBackend::new(config.clone())?;

        let mut manager = Self {
            backend,
            config,
            latest_snapshot: RwLock::new(None),
        };

        // Load latest snapshot metadata if exists
        manager.load_latest_metadata()?;

        Ok(manager)
    }

    /// Load the latest snapshot metadata
    fn load_latest_metadata(&mut self) -> Result<(), PersistenceError> {
        if let Some(data) = self.backend.read("_latest")? {
            let metadata: SnapshotMetadata = serde_json::from_slice(&data)
                .map_err(|e| PersistenceError::Deserialization(e.to_string()))?;
            *self.latest_snapshot.write() = Some(metadata);
        }
        Ok(())
    }

    /// Create a snapshot of the storage engine
    pub fn create_snapshot(&self, engine: &StorageEngine) -> Result<SnapshotMetadata, PersistenceError> {
        let snapshot_id = format!("snapshot_{}", current_time_ms());
        let tables = engine.list_tables();
        let mut total_size = 0;

        // Snapshot each table
        for table_name in &tables {
            if let Some(table) = engine.get_table(table_name) {
                let snapshot = self.snapshot_table(&table)?;
                let data = serde_json::to_vec(&snapshot)
                    .map_err(|e| PersistenceError::Serialization(e.to_string()))?;

                total_size += data.len();
                self.backend.write(&format!("{}_{}", snapshot_id, table_name), &data)?;
            }
        }

        // Create metadata
        let metadata = SnapshotMetadata {
            id: snapshot_id.clone(),
            created_at: current_time_ms(),
            tables,
            size_bytes: total_size,
            schema_version: 1,
        };

        // Save metadata
        let metadata_bytes = serde_json::to_vec(&metadata)
            .map_err(|e| PersistenceError::Serialization(e.to_string()))?;
        self.backend.write(&format!("{}_meta", snapshot_id), &metadata_bytes)?;

        // Update latest pointer
        self.backend.write("_latest", &metadata_bytes)?;
        *self.latest_snapshot.write() = Some(metadata.clone());

        self.backend.sync()?;

        Ok(metadata)
    }

    /// Snapshot a single table
    fn snapshot_table(&self, table: &Table) -> Result<TableSnapshot, PersistenceError> {
        let config = TableConfigSnapshot::from(&table.config);

        // Collect all rows from all shards
        let mut rows = Vec::new();
        for shard in table.get_shards() {
            for idx in 0..shard.row_count() {
                if let Some(row) = shard.get_row(idx) {
                    rows.push(row);
                }
            }
        }

        Ok(TableSnapshot { config, rows })
    }

    /// Restore from the latest snapshot
    pub fn restore_latest(&self, engine: &StorageEngine) -> Result<Option<SnapshotMetadata>, PersistenceError> {
        let metadata = {
            let latest = self.latest_snapshot.read();
            match &*latest {
                Some(m) => m.clone(),
                None => return Ok(None),
            }
        };

        self.restore_snapshot(engine, &metadata.id)?;
        Ok(Some(metadata))
    }

    /// Restore a specific snapshot
    pub fn restore_snapshot(&self, engine: &StorageEngine, snapshot_id: &str) -> Result<(), PersistenceError> {
        // Load metadata
        let metadata_data = self.backend.read(&format!("{}_meta", snapshot_id))?
            .ok_or_else(|| PersistenceError::SnapshotNotFound(snapshot_id.to_string()))?;

        let metadata: SnapshotMetadata = serde_json::from_slice(&metadata_data)
            .map_err(|e| PersistenceError::Deserialization(e.to_string()))?;

        // Restore each table
        for table_name in &metadata.tables {
            let table_data = self.backend.read(&format!("{}_{}", snapshot_id, table_name))?;

            if let Some(data) = table_data {
                let snapshot: TableSnapshot = serde_json::from_slice(&data)
                    .map_err(|e| PersistenceError::Deserialization(e.to_string()))?;

                // Create table with config
                let config: TableConfig = snapshot.config.into();
                let _ = engine.create_table(config);

                // Insert rows
                if let Err(e) = engine.insert_batch(table_name, snapshot.rows) {
                    tracing::warn!("Failed to restore table {}: {}", table_name, e);
                }
            }
        }

        Ok(())
    }

    /// Get the latest snapshot metadata
    pub fn latest_snapshot(&self) -> Option<SnapshotMetadata> {
        self.latest_snapshot.read().clone()
    }

    /// List all available snapshots
    pub fn list_snapshots(&self) -> Result<Vec<SnapshotMetadata>, PersistenceError> {
        let keys = self.backend.list_keys()?;
        let mut snapshots = Vec::new();

        for key in keys {
            if key.ends_with("_meta") && key != "_latest" {
                if let Some(data) = self.backend.read(&key)? {
                    if let Ok(metadata) = serde_json::from_slice::<SnapshotMetadata>(&data) {
                        snapshots.push(metadata);
                    }
                }
            }
        }

        // Sort by creation time, newest first
        snapshots.sort_by(|a, b| b.created_at.cmp(&a.created_at));

        Ok(snapshots)
    }

    /// Delete old snapshots, keeping only the N most recent
    pub fn cleanup_old_snapshots(&self, keep_count: usize) -> Result<usize, PersistenceError> {
        let snapshots = self.list_snapshots()?;

        if snapshots.len() <= keep_count {
            return Ok(0);
        }

        let mut deleted = 0;
        for snapshot in snapshots.iter().skip(keep_count) {
            // Delete table snapshots
            for table in &snapshot.tables {
                self.backend.delete(&format!("{}_{}", snapshot.id, table))?;
            }
            // Delete metadata
            self.backend.delete(&format!("{}_meta", snapshot.id))?;
            deleted += 1;
        }

        Ok(deleted)
    }
}

fn current_time_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn make_row(timestamp: i64, event: &str, value: i64) -> HashMap<String, Value> {
        let mut row = HashMap::new();
        row.insert("timestamp".to_string(), Value::Timestamp(timestamp));
        row.insert("event".to_string(), Value::String(event.to_string()));
        row.insert("value".to_string(), Value::Int64(value));
        row
    }

    #[test]
    fn test_snapshot_create_restore() {
        let temp_dir = TempDir::new().unwrap();
        let config = PersistenceConfig::new(temp_dir.path());
        let manager = SnapshotManager::new(config).unwrap();

        // Create engine and insert data
        let engine = StorageEngine::new();
        engine.insert("events", make_row(1000, "click", 42)).unwrap();
        engine.insert("events", make_row(2000, "view", 10)).unwrap();

        // Create snapshot
        let metadata = manager.create_snapshot(&engine).unwrap();
        assert_eq!(metadata.tables.len(), 1);
        assert!(metadata.tables.contains(&"events".to_string()));

        // Create new engine and restore
        let engine2 = StorageEngine::new();
        manager.restore_snapshot(&engine2, &metadata.id).unwrap();

        // Verify data was restored
        let stats = engine2.table_stats("events").unwrap();
        assert_eq!(stats.row_count, 2);
    }

    #[test]
    fn test_restore_latest() {
        let temp_dir = TempDir::new().unwrap();
        let config = PersistenceConfig::new(temp_dir.path());
        let manager = SnapshotManager::new(config).unwrap();

        // No snapshot yet
        let engine = StorageEngine::new();
        assert!(manager.restore_latest(&engine).unwrap().is_none());

        // Create snapshot
        engine.insert("test", make_row(1000, "a", 1)).unwrap();
        manager.create_snapshot(&engine).unwrap();

        // Restore latest
        let engine2 = StorageEngine::new();
        let restored = manager.restore_latest(&engine2).unwrap();
        assert!(restored.is_some());
    }
}
