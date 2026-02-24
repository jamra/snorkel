use super::shard::{calculate_shard_bounds, Shard, ShardError};
use super::value::{DataType, Value};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;

/// Configuration for a table
#[derive(Debug, Clone)]
pub struct TableConfig {
    /// Table name
    pub name: String,
    /// Duration of each shard in milliseconds (default: 1 hour)
    pub shard_duration_ms: i64,
    /// TTL for data in milliseconds (default: 24 hours)
    pub ttl_ms: i64,
    /// Maximum memory for this table in bytes (default: 1GB)
    pub max_memory_bytes: usize,
    /// Threshold for subsampling (data older than this gets subsampled)
    pub subsample_threshold_ms: i64,
    /// Subsample ratio (e.g., 0.01 = keep 1% of rows)
    pub subsample_ratio: f64,
}

impl Default for TableConfig {
    fn default() -> Self {
        Self {
            name: String::new(),
            shard_duration_ms: 3600 * 1000,        // 1 hour
            ttl_ms: 24 * 3600 * 1000,              // 24 hours
            max_memory_bytes: 1024 * 1024 * 1024,  // 1 GB
            subsample_threshold_ms: 6 * 3600 * 1000, // 6 hours
            subsample_ratio: 0.01,                 // 1%
        }
    }
}

impl TableConfig {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            ..Default::default()
        }
    }

    pub fn with_ttl(mut self, ttl_ms: i64) -> Self {
        self.ttl_ms = ttl_ms;
        self
    }

    pub fn with_shard_duration(mut self, duration_ms: i64) -> Self {
        self.shard_duration_ms = duration_ms;
        self
    }

    pub fn with_max_memory(mut self, max_bytes: usize) -> Self {
        self.max_memory_bytes = max_bytes;
        self
    }
}

/// A table containing time-series data across multiple shards
#[derive(Debug)]
pub struct Table {
    /// Table configuration
    pub config: TableConfig,
    /// Shards ordered by start time
    shards: RwLock<Vec<Arc<Shard>>>,
    /// Merged schema across all shards
    schema: RwLock<HashMap<String, DataType>>,
}

impl Table {
    pub fn new(config: TableConfig) -> Self {
        Self {
            config,
            shards: RwLock::new(Vec::new()),
            schema: RwLock::new(HashMap::new()),
        }
    }

    /// Insert a row into the appropriate shard
    pub fn insert_row(&self, row: HashMap<String, Value>) -> Result<(), TableError> {
        let timestamp = row
            .get("timestamp")
            .and_then(|v| v.as_i64())
            .ok_or(TableError::MissingTimestamp)?;

        // Update table schema
        {
            let mut schema = self.schema.write();
            for (name, value) in &row {
                let value_type = DataType::from_value(value);
                schema
                    .entry(name.clone())
                    .and_modify(|t| *t = t.merge(&value_type))
                    .or_insert(value_type);
            }
        }

        // Find or create the appropriate shard
        let shard = self.get_or_create_shard(timestamp);

        // Insert into shard
        shard
            .insert_row(&row)
            .map_err(|e| TableError::ShardError(e))
    }

    /// Get or create a shard for the given timestamp
    fn get_or_create_shard(&self, timestamp: i64) -> Arc<Shard> {
        let (start, end) = calculate_shard_bounds(timestamp, self.config.shard_duration_ms);

        // First, try to find existing shard with read lock
        {
            let shards = self.shards.read();
            for shard in shards.iter() {
                if shard.start_time == start {
                    return Arc::clone(shard);
                }
            }
        }

        // Need to create new shard with write lock
        let mut shards = self.shards.write();

        // Double-check in case another thread created it
        for shard in shards.iter() {
            if shard.start_time == start {
                return Arc::clone(shard);
            }
        }

        // Create new shard
        let shard = Arc::new(Shard::new(start, end));
        shards.push(Arc::clone(&shard));

        // Keep shards sorted by start time
        shards.sort_by_key(|s| s.start_time);

        shard
    }

    /// Get table schema
    pub fn get_schema(&self) -> HashMap<String, DataType> {
        self.schema.read().clone()
    }

    /// Get all shards (for querying)
    pub fn get_shards(&self) -> Vec<Arc<Shard>> {
        self.shards.read().clone()
    }

    /// Get shards that overlap with a time range
    pub fn get_shards_in_range(&self, start_time: i64, end_time: i64) -> Vec<Arc<Shard>> {
        self.shards
            .read()
            .iter()
            .filter(|s| s.start_time < end_time && s.end_time > start_time)
            .cloned()
            .collect()
    }

    /// Total row count across all shards
    pub fn row_count(&self) -> usize {
        self.shards.read().iter().map(|s| s.row_count()).sum()
    }

    /// Number of shards
    pub fn shard_count(&self) -> usize {
        self.shards.read().len()
    }

    /// Total memory usage
    pub fn memory_usage(&self) -> usize {
        self.shards.read().iter().map(|s| s.memory_usage()).sum()
    }

    /// Remove shards older than the cutoff time
    pub fn expire_old_shards(&self, cutoff_time: i64) -> usize {
        let mut shards = self.shards.write();
        let before = shards.len();
        shards.retain(|s| s.end_time > cutoff_time);
        before - shards.len()
    }

    /// Get shards that should be subsampled
    pub fn get_shards_for_subsampling(&self, threshold_time: i64) -> Vec<Arc<Shard>> {
        self.shards
            .read()
            .iter()
            .filter(|s| s.end_time <= threshold_time && !s.is_sealed())
            .cloned()
            .collect()
    }

    /// Get the name of this table
    pub fn name(&self) -> &str {
        &self.config.name
    }
}

#[derive(Debug, thiserror::Error)]
pub enum TableError {
    #[error("Row missing required 'timestamp' field")]
    MissingTimestamp,

    #[error("Shard error: {0}")]
    ShardError(#[from] ShardError),
}

/// Statistics about a table
#[derive(Debug, Clone, serde::Serialize)]
pub struct TableStats {
    pub name: String,
    pub row_count: usize,
    pub shard_count: usize,
    pub memory_bytes: usize,
    pub oldest_data_time: Option<i64>,
    pub newest_data_time: Option<i64>,
}

impl Table {
    pub fn stats(&self) -> TableStats {
        let shards = self.shards.read();
        let oldest = shards.first().map(|s| s.start_time);
        let newest = shards.last().map(|s| s.end_time);

        TableStats {
            name: self.config.name.clone(),
            row_count: shards.iter().map(|s| s.row_count()).sum(),
            shard_count: shards.len(),
            memory_bytes: shards.iter().map(|s| s.memory_usage()).sum(),
            oldest_data_time: oldest,
            newest_data_time: newest,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_row(timestamp: i64, event: &str) -> HashMap<String, Value> {
        let mut row = HashMap::new();
        row.insert("timestamp".to_string(), Value::Timestamp(timestamp));
        row.insert("event".to_string(), Value::String(event.to_string()));
        row
    }

    #[test]
    fn test_table_insert() {
        let config = TableConfig::new("test").with_shard_duration(1000);
        let table = Table::new(config);

        table.insert_row(make_row(100, "click")).unwrap();
        table.insert_row(make_row(200, "view")).unwrap();
        table.insert_row(make_row(1100, "click")).unwrap(); // Different shard

        assert_eq!(table.row_count(), 3);
        assert_eq!(table.shard_count(), 2); // Two shards
    }

    #[test]
    fn test_table_schema() {
        let config = TableConfig::new("test");
        let table = Table::new(config);

        let mut row = HashMap::new();
        row.insert("timestamp".to_string(), Value::Timestamp(1000));
        row.insert("count".to_string(), Value::Int64(42));
        row.insert("rate".to_string(), Value::Float64(3.14));

        table.insert_row(row).unwrap();

        let schema = table.get_schema();
        assert_eq!(schema.get("timestamp"), Some(&DataType::Timestamp));
        assert_eq!(schema.get("count"), Some(&DataType::Int64));
        assert_eq!(schema.get("rate"), Some(&DataType::Float64));
    }

    #[test]
    fn test_get_shards_in_range() {
        let config = TableConfig::new("test").with_shard_duration(1000);
        let table = Table::new(config);

        table.insert_row(make_row(100, "a")).unwrap(); // Shard [0, 1000)
        table.insert_row(make_row(1100, "b")).unwrap(); // Shard [1000, 2000)
        table.insert_row(make_row(2100, "c")).unwrap(); // Shard [2000, 3000)

        let shards = table.get_shards_in_range(500, 1500);
        assert_eq!(shards.len(), 2);
    }

    #[test]
    fn test_expire_old_shards() {
        let config = TableConfig::new("test").with_shard_duration(1000);
        let table = Table::new(config);

        table.insert_row(make_row(100, "old")).unwrap();
        table.insert_row(make_row(1100, "mid")).unwrap();
        table.insert_row(make_row(2100, "new")).unwrap();

        assert_eq!(table.shard_count(), 3);

        let expired = table.expire_old_shards(2000);
        assert_eq!(expired, 2);
        assert_eq!(table.shard_count(), 1);
    }
}
