use crate::storage::StorageEngine;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::time;

/// TTL expiration worker that periodically removes old data
pub struct TtlWorker {
    engine: Arc<StorageEngine>,
    interval: Duration,
    running: Arc<AtomicBool>,
}

impl TtlWorker {
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
            tracing::info!("TTL worker started with interval {:?}", self.interval);

            let mut interval = time::interval(self.interval);

            while self.running.load(Ordering::SeqCst) {
                interval.tick().await;

                let now = chrono::Utc::now().timestamp_millis();
                let expired = self.engine.expire_old_data(now);

                if expired > 0 {
                    tracing::info!("TTL worker expired {} shards", expired);
                }
            }

            tracing::info!("TTL worker stopped");
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

/// Run TTL expiration once (for manual/testing use)
pub fn run_ttl_expiration(engine: &StorageEngine) -> usize {
    let now = chrono::Utc::now().timestamp_millis();
    engine.expire_old_data(now)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::{TableConfig, Value};
    use std::collections::HashMap;

    fn make_row(timestamp: i64) -> HashMap<String, Value> {
        let mut row = HashMap::new();
        row.insert("timestamp".to_string(), Value::Timestamp(timestamp));
        row.insert("event".to_string(), Value::String("test".to_string()));
        row
    }

    #[test]
    fn test_run_ttl_expiration() {
        let engine = StorageEngine::new();

        // Create table with very short TTL (1 second)
        let config = TableConfig::new("test")
            .with_ttl(1000)
            .with_shard_duration(100);
        engine.create_table(config).unwrap();

        let table = engine.get_table("test").unwrap();

        // Insert data at different timestamps
        // Use a base time that's definitely in the past
        let now = chrono::Utc::now().timestamp_millis();
        let base = now - 10000; // 10 seconds ago

        for i in 0..10 {
            table.insert_row(make_row(base + i * 100)).unwrap();
        }

        assert!(table.shard_count() > 0);
        let initial_count = table.shard_count();

        // Run expiration with current time
        let expired = run_ttl_expiration(&engine);

        // All shards should be expired (they're all > 1 second old)
        assert!(expired > 0);
        assert!(table.shard_count() < initial_count);
    }
}
