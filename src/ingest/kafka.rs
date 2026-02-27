//! Kafka consumer for durable ingest
//!
//! Consumes messages from Kafka topics and inserts into storage.
//! On restart, resumes from the last committed offset for recovery.
//!
//! ## Message Format
//!
//! Messages should be JSON with the following structure:
//! ```json
//! {
//!     "table": "events",
//!     "rows": [
//!         {"timestamp": 1234567890000, "event": "click", "user_id": 123},
//!         {"timestamp": 1234567891000, "event": "view", "user_id": 456}
//!     ]
//! }
//! ```
//!
//! Or single-row format (table name from topic):
//! ```json
//! {"timestamp": 1234567890000, "event": "click", "user_id": 123}
//! ```
//!
//! ## Configuration
//!
//! Environment variables:
//! - `KAFKA_BROKERS`: Comma-separated list of brokers (default: localhost:9092)
//! - `KAFKA_TOPICS`: Comma-separated list of topics to consume
//! - `KAFKA_GROUP_ID`: Consumer group ID (default: snorkel)
//! - `KAFKA_AUTO_OFFSET_RESET`: Where to start if no offset (earliest/latest, default: earliest)

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer, CommitMode};
use rdkafka::message::Message;
use rdkafka::error::KafkaError;
use tokio::sync::mpsc;

use crate::data::value::flatten_json;
use crate::data::Value;
use crate::storage::StorageEngine;

/// Kafka consumer configuration
#[derive(Debug, Clone)]
pub struct KafkaConfig {
    /// Kafka broker addresses
    pub brokers: String,
    /// Topics to consume from
    pub topics: Vec<String>,
    /// Consumer group ID
    pub group_id: String,
    /// Auto offset reset (earliest or latest)
    pub auto_offset_reset: String,
    /// Enable auto commit (default: false for at-least-once)
    pub enable_auto_commit: bool,
    /// Session timeout in ms
    pub session_timeout_ms: u32,
    /// Max poll interval in ms
    pub max_poll_interval_ms: u32,
}

impl Default for KafkaConfig {
    fn default() -> Self {
        Self {
            brokers: "localhost:9092".to_string(),
            topics: vec![],
            group_id: "snorkel".to_string(),
            auto_offset_reset: "earliest".to_string(),
            enable_auto_commit: false,
            session_timeout_ms: 30000,
            max_poll_interval_ms: 300000,
        }
    }
}

impl KafkaConfig {
    /// Create config from environment variables
    pub fn from_env() -> Option<Self> {
        let topics = std::env::var("KAFKA_TOPICS").ok()?;
        if topics.is_empty() {
            return None;
        }

        Some(Self {
            brokers: std::env::var("KAFKA_BROKERS")
                .unwrap_or_else(|_| "localhost:9092".to_string()),
            topics: topics.split(',').map(|s| s.trim().to_string()).collect(),
            group_id: std::env::var("KAFKA_GROUP_ID")
                .unwrap_or_else(|_| "snorkel".to_string()),
            auto_offset_reset: std::env::var("KAFKA_AUTO_OFFSET_RESET")
                .unwrap_or_else(|_| "earliest".to_string()),
            enable_auto_commit: false,
            session_timeout_ms: 30000,
            max_poll_interval_ms: 300000,
        })
    }
}

/// Statistics from Kafka consumer
#[derive(Debug, Default)]
pub struct KafkaStats {
    pub messages_received: u64,
    pub messages_processed: u64,
    pub rows_inserted: u64,
    pub errors: u64,
    pub last_offset: HashMap<String, i64>,
}

/// Kafka consumer for ingesting data into Snorkel
pub struct KafkaConsumer {
    config: KafkaConfig,
    engine: Arc<StorageEngine>,
    consumer: StreamConsumer,
    shutdown_tx: Option<mpsc::Sender<()>>,
    stats: Arc<parking_lot::RwLock<KafkaStats>>,
}

impl KafkaConsumer {
    /// Create a new Kafka consumer
    pub fn new(config: KafkaConfig, engine: Arc<StorageEngine>) -> Result<Self, KafkaError> {
        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", &config.brokers)
            .set("group.id", &config.group_id)
            .set("auto.offset.reset", &config.auto_offset_reset)
            .set("enable.auto.commit", config.enable_auto_commit.to_string())
            .set("session.timeout.ms", config.session_timeout_ms.to_string())
            .set("max.poll.interval.ms", config.max_poll_interval_ms.to_string())
            // Optimize for throughput
            .set("fetch.min.bytes", "1024")
            .set("fetch.max.wait.ms", "100")
            .create()?;

        Ok(Self {
            config,
            engine,
            consumer,
            shutdown_tx: None,
            stats: Arc::new(parking_lot::RwLock::new(KafkaStats::default())),
        })
    }

    /// Subscribe to configured topics
    pub fn subscribe(&self) -> Result<(), KafkaError> {
        let topics: Vec<&str> = self.config.topics.iter().map(|s| s.as_str()).collect();
        self.consumer.subscribe(&topics)?;
        tracing::info!("Subscribed to Kafka topics: {:?}", self.config.topics);
        Ok(())
    }

    /// Get current statistics
    pub fn stats(&self) -> KafkaStats {
        let stats = self.stats.read();
        KafkaStats {
            messages_received: stats.messages_received,
            messages_processed: stats.messages_processed,
            rows_inserted: stats.rows_inserted,
            errors: stats.errors,
            last_offset: stats.last_offset.clone(),
        }
    }

    /// Start consuming in background
    pub fn start(mut self) -> tokio::task::JoinHandle<()> {
        let (shutdown_tx, mut shutdown_rx) = mpsc::channel::<()>(1);
        self.shutdown_tx = Some(shutdown_tx);

        let consumer = self.consumer;
        let engine = self.engine;
        let stats = self.stats;

        tokio::spawn(async move {
            tracing::info!("Kafka consumer started");

            loop {
                tokio::select! {
                    _ = shutdown_rx.recv() => {
                        tracing::info!("Kafka consumer shutting down");
                        break;
                    }
                    result = consumer.recv() => {
                        match result {
                            Ok(message) => {
                                let topic = message.topic().to_string();
                                let partition = message.partition();
                                let offset = message.offset();

                                {
                                    let mut s = stats.write();
                                    s.messages_received += 1;
                                }

                                // Process message
                                if let Some(payload) = message.payload() {
                                    match Self::process_message(&engine, &topic, payload) {
                                        Ok(rows_inserted) => {
                                            // Commit offset after successful processing
                                            if let Err(e) = consumer.commit_message(&message, CommitMode::Async) {
                                                tracing::error!(
                                                    topic = %topic,
                                                    partition = partition,
                                                    offset = offset,
                                                    error = %e,
                                                    "Failed to commit offset"
                                                );
                                            }

                                            let mut s = stats.write();
                                            s.messages_processed += 1;
                                            s.rows_inserted += rows_inserted as u64;
                                            s.last_offset.insert(
                                                format!("{}:{}", topic, partition),
                                                offset
                                            );
                                        }
                                        Err(e) => {
                                            tracing::error!(
                                                topic = %topic,
                                                partition = partition,
                                                offset = offset,
                                                error = %e,
                                                "Failed to process message"
                                            );
                                            let mut s = stats.write();
                                            s.errors += 1;
                                        }
                                    }
                                }
                            }
                            Err(e) => {
                                tracing::error!(error = %e, "Kafka receive error");
                                // Back off on error
                                tokio::time::sleep(Duration::from_millis(100)).await;
                            }
                        }
                    }
                }
            }
        })
    }

    /// Process a single message
    fn process_message(
        engine: &StorageEngine,
        topic: &str,
        payload: &[u8],
    ) -> Result<usize, ProcessError> {
        // Parse JSON
        let value: serde_json::Value = serde_json::from_slice(payload)
            .map_err(|e| ProcessError::Parse(e.to_string()))?;

        // Check message format
        if let Some(obj) = value.as_object() {
            if obj.contains_key("table") && obj.contains_key("rows") {
                // Batch format: {"table": "name", "rows": [...]}
                return Self::process_batch_message(engine, obj);
            }
        }

        // Single row format: use topic name as table
        Self::process_single_message(engine, topic, &value)
    }

    /// Process batch format message
    fn process_batch_message(
        engine: &StorageEngine,
        obj: &serde_json::Map<String, serde_json::Value>,
    ) -> Result<usize, ProcessError> {
        let table = obj.get("table")
            .and_then(|v| v.as_str())
            .ok_or_else(|| ProcessError::Parse("missing 'table' field".to_string()))?;

        let rows = obj.get("rows")
            .and_then(|v| v.as_array())
            .ok_or_else(|| ProcessError::Parse("missing 'rows' array".to_string()))?;

        let flattened: Vec<HashMap<String, Value>> = rows
            .iter()
            .filter_map(|row| row.as_object().map(flatten_json))
            .collect();

        let count = flattened.len();
        engine.insert_batch(table, flattened)
            .map_err(|e| ProcessError::Insert(e.to_string()))?;

        Ok(count)
    }

    /// Process single row message (topic = table name)
    fn process_single_message(
        engine: &StorageEngine,
        topic: &str,
        value: &serde_json::Value,
    ) -> Result<usize, ProcessError> {
        let obj = value.as_object()
            .ok_or_else(|| ProcessError::Parse("expected JSON object".to_string()))?;

        let row = flatten_json(obj);
        engine.insert(topic, row)
            .map_err(|e| ProcessError::Insert(e.to_string()))?;

        Ok(1)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ProcessError {
    #[error("Parse error: {0}")]
    Parse(String),

    #[error("Insert error: {0}")]
    Insert(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_kafka_config_default() {
        let config = KafkaConfig::default();
        assert_eq!(config.brokers, "localhost:9092");
        assert_eq!(config.group_id, "snorkel");
        assert!(!config.enable_auto_commit);
    }

    #[test]
    fn test_process_batch_message() {
        let engine = Arc::new(StorageEngine::new());
        let payload = r#"{
            "table": "events",
            "rows": [
                {"timestamp": 1234567890000, "event": "click"},
                {"timestamp": 1234567891000, "event": "view"}
            ]
        }"#;

        let value: serde_json::Value = serde_json::from_str(payload).unwrap();
        let obj = value.as_object().unwrap();
        let count = KafkaConsumer::process_batch_message(&engine, obj).unwrap();

        assert_eq!(count, 2);
    }

    #[test]
    fn test_process_single_message() {
        let engine = Arc::new(StorageEngine::new());
        let payload = r#"{"timestamp": 1234567890000, "event": "click", "user_id": 123}"#;

        let value: serde_json::Value = serde_json::from_str(payload).unwrap();
        let count = KafkaConsumer::process_single_message(&engine, "events", &value).unwrap();

        assert_eq!(count, 1);
    }
}
