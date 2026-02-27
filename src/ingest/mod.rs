//! Ingest sources for Snorkel
//!
//! Supports multiple ingest methods:
//! - HTTP API (default)
//! - Kafka consumer (optional, enable with `kafka` feature)

#[cfg(feature = "kafka")]
pub mod kafka;

#[cfg(feature = "kafka")]
pub use kafka::{KafkaConsumer, KafkaConfig};
