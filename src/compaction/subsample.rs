use crate::data::Shard;
use crate::storage::StorageEngine;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::time;

/// Subsampling worker that compacts old data
pub struct SubsampleWorker {
    engine: Arc<StorageEngine>,
    interval: Duration,
    running: Arc<AtomicBool>,
}

impl SubsampleWorker {
    pub fn new(engine: Arc<StorageEngine>, interval: Duration) -> Self {
        Self {
            engine,
            interval,
            running: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Start the background worker
    pub fn start(self: Arc<Self>) -> tokio::task::JoinHandle<()> {
        self.running.store(true, Ordering::SeqCst);

        tokio::spawn(async move {
            tracing::info!("Subsample worker started with interval {:?}", self.interval);

            let mut interval = time::interval(self.interval);

            while self.running.load(Ordering::SeqCst) {
                interval.tick().await;

                let now = chrono::Utc::now().timestamp_millis();

                for table_name in self.engine.list_tables() {
                    if let Some(table) = self.engine.get_table(&table_name) {
                        let threshold = now - table.config.subsample_threshold_ms;
                        let shards = table.get_shards_for_subsampling(threshold);

                        for shard in shards {
                            let ratio = table.config.subsample_ratio;
                            if let Err(e) = subsample_shard(&shard, ratio) {
                                tracing::warn!(
                                    "Failed to subsample shard [{}, {}): {}",
                                    shard.start_time,
                                    shard.end_time,
                                    e
                                );
                            } else {
                                // Seal the shard after subsampling
                                shard.seal();
                            }
                        }
                    }
                }
            }

            tracing::info!("Subsample worker stopped");
        })
    }

    /// Stop the worker
    pub fn stop(&self) {
        self.running.store(false, Ordering::SeqCst);
    }

    /// Check if worker is running
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }
}

/// Subsample a shard by keeping only a fraction of rows
///
/// Note: In a production system, we would actually create a new compacted shard.
/// For simplicity, this implementation just marks the shard as subsampled
/// and the actual data reduction would happen through the seal mechanism.
pub fn subsample_shard(shard: &Shard, ratio: f64) -> Result<SubsampleStats, SubsampleError> {
    if shard.is_sealed() {
        return Err(SubsampleError::AlreadySealed);
    }

    let original_count = shard.row_count();
    let target_count = ((original_count as f64) * ratio).ceil() as usize;

    // In a real implementation, we would:
    // 1. Create a new shard with aggregated data
    // 2. Randomly sample raw rows
    // 3. Replace the original shard
    //
    // For MVP, we just compute statistics about what would be kept

    let stats = SubsampleStats {
        original_rows: original_count,
        sampled_rows: target_count,
        ratio,
        time_range: (shard.start_time, shard.end_time),
    };

    tracing::debug!(
        "Would subsample shard [{}, {}): {} -> {} rows",
        shard.start_time,
        shard.end_time,
        original_count,
        target_count
    );

    Ok(stats)
}

/// Statistics from a subsampling operation
#[derive(Debug, Clone)]
pub struct SubsampleStats {
    pub original_rows: usize,
    pub sampled_rows: usize,
    pub ratio: f64,
    pub time_range: (i64, i64),
}

/// Compute aggregated statistics for a shard
pub fn compute_shard_aggregates(shard: &Shard) -> HashMap<String, AggregateStats> {
    let mut stats = HashMap::new();
    let schema = shard.get_schema();

    for (col_name, _dtype) in schema {
        if col_name == "timestamp" {
            continue;
        }

        let mut sum = 0.0;
        let mut count = 0i64;
        let mut min: Option<f64> = None;
        let mut max: Option<f64> = None;

        if let Some(col) = shard.get_column(&col_name) {
            for value in col.iter() {
                if let Some(v) = value.as_f64() {
                    sum += v;
                    count += 1;
                    min = Some(min.map(|m| m.min(v)).unwrap_or(v));
                    max = Some(max.map(|m| m.max(v)).unwrap_or(v));
                }
            }
        }

        if count > 0 {
            stats.insert(
                col_name,
                AggregateStats {
                    count,
                    sum,
                    avg: sum / count as f64,
                    min,
                    max,
                },
            );
        }
    }

    stats
}

/// Aggregate statistics for a column
#[derive(Debug, Clone, serde::Serialize)]
pub struct AggregateStats {
    pub count: i64,
    pub sum: f64,
    pub avg: f64,
    pub min: Option<f64>,
    pub max: Option<f64>,
}

#[derive(Debug, thiserror::Error)]
pub enum SubsampleError {
    #[error("Shard is already sealed")]
    AlreadySealed,

    #[error("Subsample error: {0}")]
    General(String),
}

/// Select random sample indices using reservoir sampling
pub fn reservoir_sample(n: usize, k: usize) -> Vec<usize> {
    if k >= n {
        return (0..n).collect();
    }

    use std::collections::HashSet;

    let mut selected = HashSet::new();
    let mut rng_state = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64;

    // Simple LCG random number generator
    let mut random = || {
        rng_state = rng_state.wrapping_mul(6364136223846793005).wrapping_add(1);
        rng_state
    };

    while selected.len() < k {
        let idx = (random() as usize) % n;
        selected.insert(idx);
    }

    let mut result: Vec<usize> = selected.into_iter().collect();
    result.sort();
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::{Shard, Value};

    fn make_shard_with_data(n: usize) -> Shard {
        let shard = Shard::new(0, 1000000);

        for i in 0..n {
            let mut row = HashMap::new();
            row.insert("timestamp".to_string(), Value::Timestamp(i as i64 * 100));
            row.insert("value".to_string(), Value::Int64(i as i64));
            row.insert("latency".to_string(), Value::Float64(i as f64 * 1.5));
            shard.insert_row(&row).unwrap();
        }

        shard
    }

    #[test]
    fn test_subsample_shard() {
        let shard = make_shard_with_data(100);
        let stats = subsample_shard(&shard, 0.1).unwrap();

        assert_eq!(stats.original_rows, 100);
        assert_eq!(stats.sampled_rows, 10);
        assert!((stats.ratio - 0.1).abs() < 0.001);
    }

    #[test]
    fn test_compute_shard_aggregates() {
        let shard = make_shard_with_data(100);
        let stats = compute_shard_aggregates(&shard);

        let value_stats = stats.get("value").unwrap();
        assert_eq!(value_stats.count, 100);
        assert_eq!(value_stats.min, Some(0.0));
        assert_eq!(value_stats.max, Some(99.0));
        // Sum of 0..99 = 99 * 100 / 2 = 4950
        assert!((value_stats.sum - 4950.0).abs() < 0.01);
    }

    #[test]
    fn test_reservoir_sample() {
        let indices = reservoir_sample(100, 10);
        assert_eq!(indices.len(), 10);

        // All indices should be in range
        for idx in &indices {
            assert!(*idx < 100);
        }

        // Should be sorted
        for i in 1..indices.len() {
            assert!(indices[i] > indices[i - 1]);
        }
    }

    #[test]
    fn test_sealed_shard_cannot_subsample() {
        let shard = make_shard_with_data(10);
        shard.seal();

        let result = subsample_shard(&shard, 0.1);
        assert!(matches!(result, Err(SubsampleError::AlreadySealed)));
    }
}
