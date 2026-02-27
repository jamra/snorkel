use super::column::Column;
use super::value::{DataType, Value};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Statistics about compression for a shard
#[derive(Debug, Clone, serde::Serialize)]
pub struct ShardCompressionStats {
    pub compressed_columns: usize,
    pub uncompressed_columns: usize,
    pub original_size: usize,
    pub compressed_size: usize,
    pub compression_ratio: f64,
}

/// Time-bounded partition of data.
/// Each shard covers a specific time range (e.g., 1 hour).
#[derive(Debug)]
pub struct Shard {
    /// Start timestamp (inclusive) - epoch milliseconds
    pub start_time: i64,
    /// End timestamp (exclusive) - epoch milliseconds
    pub end_time: i64,
    /// Columns indexed by column name
    columns: RwLock<HashMap<String, Column>>,
    /// Number of rows in this shard
    row_count: AtomicUsize,
    /// Schema: column name -> data type
    schema: RwLock<HashMap<String, DataType>>,
    /// Whether this shard is sealed (no more writes)
    sealed: RwLock<bool>,
}

impl Shard {
    /// Create a new shard for the given time range
    pub fn new(start_time: i64, end_time: i64) -> Self {
        Self {
            start_time,
            end_time,
            columns: RwLock::new(HashMap::new()),
            row_count: AtomicUsize::new(0),
            schema: RwLock::new(HashMap::new()),
            sealed: RwLock::new(false),
        }
    }

    /// Check if a timestamp falls within this shard's time range
    pub fn contains_time(&self, timestamp: i64) -> bool {
        timestamp >= self.start_time && timestamp < self.end_time
    }

    /// Insert a row into the shard
    pub fn insert_row(&self, row: &HashMap<String, Value>) -> Result<(), ShardError> {
        if *self.sealed.read() {
            return Err(ShardError::ShardSealed);
        }

        // Get or infer timestamp
        let timestamp = row
            .get("timestamp")
            .and_then(|v| v.as_i64())
            .ok_or(ShardError::MissingTimestamp)?;

        if !self.contains_time(timestamp) {
            return Err(ShardError::TimestampOutOfRange {
                timestamp,
                start: self.start_time,
                end: self.end_time,
            });
        }

        let mut columns = self.columns.write();
        let mut schema = self.schema.write();
        let current_row_count = self.row_count.load(Ordering::SeqCst);

        // First, ensure all existing columns have a value (possibly null)
        for (name, col) in columns.iter_mut() {
            if !row.contains_key(name) {
                col.push(&Value::Null);
            }
        }

        // Then add values for all columns in the row
        for (name, value) in row {
            // Update schema
            let value_type = DataType::from_value(value);
            schema
                .entry(name.clone())
                .and_modify(|t| *t = t.merge(&value_type))
                .or_insert(value_type);

            // Get or create column
            if let Some(col) = columns.get_mut(name) {
                col.push(value);
            } else {
                // New column - need to backfill with nulls
                let mut col = Column::new(value_type);
                for _ in 0..current_row_count {
                    col.push(&Value::Null);
                }
                col.push(value);
                columns.insert(name.clone(), col);
            }
        }

        self.row_count.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    /// Get the number of rows
    pub fn row_count(&self) -> usize {
        self.row_count.load(Ordering::SeqCst)
    }

    /// Get column by name
    pub fn get_column(&self, name: &str) -> Option<Column> {
        self.columns.read().get(name).cloned()
    }

    /// Get all column names
    pub fn column_names(&self) -> Vec<String> {
        self.columns.read().keys().cloned().collect()
    }

    /// Get schema
    pub fn get_schema(&self) -> HashMap<String, DataType> {
        self.schema.read().clone()
    }

    /// Get value at specific row and column
    pub fn get_value(&self, row_idx: usize, column: &str) -> Option<Value> {
        self.columns
            .read()
            .get(column)
            .map(|col| col.get(row_idx))
    }

    /// Access columns with a single lock acquisition for batch operations.
    /// This is more efficient than calling get_value multiple times.
    pub fn with_columns<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&HashMap<String, Column>) -> R,
    {
        let columns = self.columns.read();
        f(&columns)
    }

    /// Get timestamp for a row
    pub fn get_timestamp(&self, row_idx: usize) -> Option<i64> {
        self.columns
            .read()
            .get("timestamp")
            .and_then(|col| col.get(row_idx).as_i64())
    }

    /// Seal the shard (no more writes allowed)
    /// This also compresses columns to reduce memory usage.
    pub fn seal(&self) {
        let mut sealed = self.sealed.write();
        if *sealed {
            return; // Already sealed
        }
        *sealed = true;

        // Compress columns for memory efficiency
        self.compress_columns();
    }

    /// Compress all columns in the shard
    fn compress_columns(&self) {
        let mut columns = self.columns.write();
        let mut compressed_columns = std::collections::HashMap::new();

        for (name, column) in columns.iter() {
            // Only compress if we have enough data to make it worthwhile
            if column.len() >= 100 && !column.is_compressed() {
                compressed_columns.insert(name.clone(), column.compress());
            }
        }

        // Replace with compressed versions
        for (name, compressed) in compressed_columns {
            columns.insert(name, compressed);
        }
    }

    /// Get compression statistics for this shard
    pub fn compression_stats(&self) -> ShardCompressionStats {
        let columns = self.columns.read();
        let mut compressed_count = 0;
        let mut uncompressed_count = 0;
        let mut total_original_size = 0;
        let mut total_compressed_size = 0;

        for column in columns.values() {
            if let Column::Compressed { data, .. } = column {
                compressed_count += 1;
                total_original_size += data.original_size;
                total_compressed_size += data.data.len();
            } else {
                uncompressed_count += 1;
            }
        }

        ShardCompressionStats {
            compressed_columns: compressed_count,
            uncompressed_columns: uncompressed_count,
            original_size: total_original_size,
            compressed_size: total_compressed_size,
            compression_ratio: if total_compressed_size > 0 {
                total_original_size as f64 / total_compressed_size as f64
            } else {
                1.0
            },
        }
    }

    /// Check if shard is sealed
    pub fn is_sealed(&self) -> bool {
        *self.sealed.read()
    }

    /// Estimate memory usage in bytes
    pub fn memory_usage(&self) -> usize {
        self.columns
            .read()
            .values()
            .map(|c| c.memory_usage())
            .sum()
    }

    /// Iterator over rows (returns row indices)
    pub fn row_indices(&self) -> impl Iterator<Item = usize> {
        0..self.row_count.load(Ordering::SeqCst)
    }

    /// Get a row as a HashMap
    pub fn get_row(&self, idx: usize) -> Option<HashMap<String, Value>> {
        if idx >= self.row_count() {
            return None;
        }

        let columns = self.columns.read();
        let mut row = HashMap::new();
        for (name, col) in columns.iter() {
            row.insert(name.clone(), col.get(idx));
        }
        Some(row)
    }

    /// Filter rows by a predicate on a column
    pub fn filter_rows<F>(&self, column: &str, predicate: F) -> Vec<usize>
    where
        F: Fn(&Value) -> bool,
    {
        let columns = self.columns.read();
        let Some(col) = columns.get(column) else {
            return vec![];
        };

        col.iter()
            .enumerate()
            .filter(|(_, v)| predicate(v))
            .map(|(i, _)| i)
            .collect()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ShardError {
    #[error("Shard is sealed and cannot accept writes")]
    ShardSealed,

    #[error("Row missing required 'timestamp' field")]
    MissingTimestamp,

    #[error("Timestamp {timestamp} out of range [{start}, {end})")]
    TimestampOutOfRange {
        timestamp: i64,
        start: i64,
        end: i64,
    },
}

/// Calculate shard boundaries for a given timestamp and shard duration
pub fn calculate_shard_bounds(timestamp: i64, shard_duration_ms: i64) -> (i64, i64) {
    let start = (timestamp / shard_duration_ms) * shard_duration_ms;
    let end = start + shard_duration_ms;
    (start, end)
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
    fn test_shard_insert_and_read() {
        let shard = Shard::new(0, 3600000); // 1 hour shard

        let row = make_row(1000, "click", 42);
        shard.insert_row(&row).unwrap();

        assert_eq!(shard.row_count(), 1);
        assert_eq!(
            shard.get_value(0, "event"),
            Some(Value::String("click".into()))
        );
        assert_eq!(shard.get_value(0, "value"), Some(Value::Int64(42)));
    }

    #[test]
    fn test_shard_timestamp_range() {
        let shard = Shard::new(0, 1000);

        // Valid timestamp
        let row = make_row(500, "ok", 1);
        assert!(shard.insert_row(&row).is_ok());

        // Invalid timestamp
        let row = make_row(1500, "bad", 2);
        assert!(matches!(
            shard.insert_row(&row),
            Err(ShardError::TimestampOutOfRange { .. })
        ));
    }

    #[test]
    fn test_shard_schema_inference() {
        let shard = Shard::new(0, 3600000);

        let row = make_row(1000, "test", 42);
        shard.insert_row(&row).unwrap();

        let schema = shard.get_schema();
        assert_eq!(schema.get("timestamp"), Some(&DataType::Timestamp));
        assert_eq!(schema.get("event"), Some(&DataType::String));
        assert_eq!(schema.get("value"), Some(&DataType::Int64));
    }

    #[test]
    fn test_shard_sealing() {
        let shard = Shard::new(0, 3600000);

        let row = make_row(1000, "before", 1);
        shard.insert_row(&row).unwrap();

        shard.seal();

        let row = make_row(2000, "after", 2);
        assert!(matches!(shard.insert_row(&row), Err(ShardError::ShardSealed)));
    }

    #[test]
    fn test_calculate_shard_bounds() {
        // 1 hour shards (3600000 ms)
        let (start, end) = calculate_shard_bounds(3700000, 3600000);
        assert_eq!(start, 3600000);
        assert_eq!(end, 7200000);

        let (start, end) = calculate_shard_bounds(0, 3600000);
        assert_eq!(start, 0);
        assert_eq!(end, 3600000);
    }

    #[test]
    fn test_filter_rows() {
        let shard = Shard::new(0, 3600000);

        shard.insert_row(&make_row(100, "click", 10)).unwrap();
        shard.insert_row(&make_row(200, "view", 20)).unwrap();
        shard.insert_row(&make_row(300, "click", 30)).unwrap();

        let clicks = shard.filter_rows("event", |v| v == &Value::String("click".into()));
        assert_eq!(clicks, vec![0, 2]);
    }
}
