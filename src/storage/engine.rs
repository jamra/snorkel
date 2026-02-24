use crate::data::{DataType, Table, TableConfig, TableError, TableStats, Value};
use dashmap::DashMap;
use std::collections::HashMap;
use std::sync::Arc;

use super::memory::{MemoryStats, MemoryTracker};

/// Main storage engine managing all tables
pub struct StorageEngine {
    /// Tables indexed by name
    tables: DashMap<String, Arc<Table>>,
    /// Global memory tracker
    memory: Arc<MemoryTracker>,
    /// Default table configuration
    #[allow(dead_code)]
    default_config: TableConfig,
}

impl StorageEngine {
    pub fn new() -> Self {
        Self {
            tables: DashMap::new(),
            memory: Arc::new(MemoryTracker::default()),
            default_config: TableConfig::default(),
        }
    }

    pub fn with_memory_limit(max_bytes: usize) -> Self {
        Self {
            tables: DashMap::new(),
            memory: Arc::new(MemoryTracker::new(max_bytes)),
            default_config: TableConfig::default(),
        }
    }

    /// Create a new table with configuration
    pub fn create_table(&self, config: TableConfig) -> Result<Arc<Table>, StorageError> {
        let name = config.name.clone();

        if self.tables.contains_key(&name) {
            return Err(StorageError::TableExists(name));
        }

        let table = Arc::new(Table::new(config));
        self.tables.insert(name.clone(), Arc::clone(&table));

        Ok(table)
    }

    /// Get or create a table (auto-creates with default config if not exists)
    pub fn get_or_create_table(&self, name: &str) -> Arc<Table> {
        if let Some(table) = self.tables.get(name) {
            return Arc::clone(&table);
        }

        // Create with default config
        let config = TableConfig::new(name);
        let table = Arc::new(Table::new(config));
        self.tables.insert(name.to_string(), Arc::clone(&table));
        table
    }

    /// Get an existing table
    pub fn get_table(&self, name: &str) -> Option<Arc<Table>> {
        self.tables.get(name).map(|t| Arc::clone(&t))
    }

    /// Drop a table
    pub fn drop_table(&self, name: &str) -> Result<(), StorageError> {
        if self.tables.remove(name).is_none() {
            return Err(StorageError::TableNotFound(name.to_string()));
        }
        Ok(())
    }

    /// List all table names
    pub fn list_tables(&self) -> Vec<String> {
        self.tables.iter().map(|e| e.key().clone()).collect()
    }

    /// Insert a row into a table (creates table if not exists)
    pub fn insert(
        &self,
        table_name: &str,
        row: HashMap<String, Value>,
    ) -> Result<(), StorageError> {
        let table = self.get_or_create_table(table_name);
        table.insert_row(row).map_err(StorageError::TableError)?;

        // Update memory tracking
        // Note: This is an approximation; we periodically sync actual usage
        self.memory.allocate(self.estimate_row_size(&table));

        Ok(())
    }

    /// Insert multiple rows
    pub fn insert_batch(
        &self,
        table_name: &str,
        rows: Vec<HashMap<String, Value>>,
    ) -> Result<usize, StorageError> {
        let table = self.get_or_create_table(table_name);
        let mut inserted = 0;

        for row in rows {
            match table.insert_row(row) {
                Ok(()) => inserted += 1,
                Err(e) => {
                    tracing::warn!("Failed to insert row: {}", e);
                }
            }
        }

        // Batch memory tracking update
        let row_size = self.estimate_row_size(&table);
        self.memory.allocate(inserted * row_size);

        Ok(inserted)
    }

    /// Get table statistics
    pub fn table_stats(&self, name: &str) -> Option<TableStats> {
        self.tables.get(name).map(|t| t.stats())
    }

    /// Get all table statistics
    pub fn all_table_stats(&self) -> Vec<TableStats> {
        self.tables.iter().map(|e| e.value().stats()).collect()
    }

    /// Get table schema
    pub fn table_schema(&self, name: &str) -> Option<HashMap<String, DataType>> {
        self.tables.get(name).map(|t| t.get_schema())
    }

    /// Get memory statistics
    pub fn memory_stats(&self) -> MemoryStats {
        MemoryStats::from(self.memory.as_ref())
    }

    /// Recalculate actual memory usage from tables
    pub fn sync_memory(&self) {
        let actual: usize = self.tables.iter().map(|e| e.value().memory_usage()).sum();
        self.memory.reset();
        self.memory.allocate(actual);
    }

    /// Check if under memory pressure
    pub fn is_under_memory_pressure(&self) -> bool {
        self.memory.is_under_pressure()
    }

    /// Get memory tracker for background workers
    pub fn memory_tracker(&self) -> Arc<MemoryTracker> {
        Arc::clone(&self.memory)
    }

    /// Estimate size of a row in bytes (rough approximation)
    fn estimate_row_size(&self, table: &Table) -> usize {
        let schema = table.get_schema();
        let mut size = 0;

        for (_name, dtype) in schema {
            size += match dtype {
                DataType::Null => 1,
                DataType::Bool => 2,
                DataType::Int64 => 9,
                DataType::Float64 => 9,
                DataType::String => 24, // Average string size estimate
                DataType::Timestamp => 9,
            };
        }

        size.max(16) // Minimum row overhead
    }

    /// Expire old data from all tables based on TTL
    pub fn expire_old_data(&self, current_time: i64) -> usize {
        let mut total_expired = 0;

        for entry in self.tables.iter() {
            let table = entry.value();
            let cutoff = current_time - table.config.ttl_ms;
            total_expired += table.expire_old_shards(cutoff);
        }

        // Sync memory tracking after expiration
        self.sync_memory();

        total_expired
    }
}

impl Default for StorageEngine {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("Table '{0}' already exists")]
    TableExists(String),

    #[error("Table '{0}' not found")]
    TableNotFound(String),

    #[error("Table error: {0}")]
    TableError(#[from] TableError),

    #[error("Memory limit exceeded")]
    MemoryLimitExceeded,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_row(timestamp: i64, event: &str, value: i64) -> HashMap<String, Value> {
        let mut row = HashMap::new();
        row.insert("timestamp".to_string(), Value::Timestamp(timestamp));
        row.insert("event".to_string(), Value::String(event.to_string()));
        row.insert("value".to_string(), Value::Int64(value));
        row
    }

    #[test]
    fn test_create_table() {
        let engine = StorageEngine::new();

        let config = TableConfig::new("events");
        engine.create_table(config).unwrap();

        assert!(engine.get_table("events").is_some());
        assert!(engine.get_table("nonexistent").is_none());
    }

    #[test]
    fn test_create_duplicate_table() {
        let engine = StorageEngine::new();

        let config1 = TableConfig::new("events");
        engine.create_table(config1).unwrap();

        let config2 = TableConfig::new("events");
        assert!(matches!(
            engine.create_table(config2),
            Err(StorageError::TableExists(_))
        ));
    }

    #[test]
    fn test_insert_auto_creates_table() {
        let engine = StorageEngine::new();

        engine.insert("auto_table", make_row(1000, "test", 42)).unwrap();

        assert!(engine.get_table("auto_table").is_some());
        assert_eq!(engine.table_stats("auto_table").unwrap().row_count, 1);
    }

    #[test]
    fn test_insert_batch() {
        let engine = StorageEngine::new();

        let rows = vec![
            make_row(1000, "a", 1),
            make_row(2000, "b", 2),
            make_row(3000, "c", 3),
        ];

        let inserted = engine.insert_batch("batch_table", rows).unwrap();
        assert_eq!(inserted, 3);
        assert_eq!(engine.table_stats("batch_table").unwrap().row_count, 3);
    }

    #[test]
    fn test_list_tables() {
        let engine = StorageEngine::new();

        engine.create_table(TableConfig::new("table1")).unwrap();
        engine.create_table(TableConfig::new("table2")).unwrap();

        let tables = engine.list_tables();
        assert_eq!(tables.len(), 2);
        assert!(tables.contains(&"table1".to_string()));
        assert!(tables.contains(&"table2".to_string()));
    }

    #[test]
    fn test_drop_table() {
        let engine = StorageEngine::new();

        engine.create_table(TableConfig::new("to_drop")).unwrap();
        assert!(engine.get_table("to_drop").is_some());

        engine.drop_table("to_drop").unwrap();
        assert!(engine.get_table("to_drop").is_none());

        assert!(matches!(
            engine.drop_table("nonexistent"),
            Err(StorageError::TableNotFound(_))
        ));
    }

    #[test]
    fn test_table_schema() {
        let engine = StorageEngine::new();

        engine.insert("schema_test", make_row(1000, "test", 42)).unwrap();

        let schema = engine.table_schema("schema_test").unwrap();
        assert_eq!(schema.get("timestamp"), Some(&DataType::Timestamp));
        assert_eq!(schema.get("event"), Some(&DataType::String));
        assert_eq!(schema.get("value"), Some(&DataType::Int64));
    }

    #[test]
    fn test_expire_old_data() {
        let engine = StorageEngine::new();

        // Create table with short TTL (1000ms)
        let config = TableConfig::new("expiring").with_ttl(1000).with_shard_duration(100);
        engine.create_table(config).unwrap();

        // Insert some data
        let table = engine.get_table("expiring").unwrap();
        for i in 0..10 {
            let row = make_row(i * 100, "event", i);
            table.insert_row(row).unwrap();
        }

        // All data should exist
        assert!(table.shard_count() > 0);

        // Expire data older than timestamp 2000 (cutoff = 2000 - 1000 = 1000)
        engine.expire_old_data(2000);

        // Some shards should be removed
        let remaining = table.shard_count();
        assert!(remaining < 10);
    }
}
