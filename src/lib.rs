//! Snorkel: In-Memory Time-Series Analytics Database
//!
//! A Rust implementation of a Scuba-like bounded in-memory time-series database
//! with SQL-like queries (no joins), automatic data expiration, and subsampling
//! for older data.
//!
//! # Features
//!
//! - **Columnar Storage**: Efficient memory layout for analytics queries
//! - **String Dictionary Encoding**: Compact storage for repeated strings
//! - **Time-Based Sharding**: Data partitioned by time for efficient pruning
//! - **SQL-Like Queries**: SELECT, WHERE, GROUP BY, ORDER BY, LIMIT
//! - **Aggregations**: COUNT, SUM, AVG, MIN, MAX, PERCENTILE
//! - **TIME_BUCKET**: Group data by time intervals
//! - **TTL Expiration**: Automatic removal of old data
//! - **Subsampling**: Compact old data while preserving statistics
//!
//! # Example
//!
//! ```no_run
//! use snorkel::storage::StorageEngine;
//! use snorkel::data::Value;
//! use snorkel::query::run_query;
//! use std::collections::HashMap;
//!
//! let engine = StorageEngine::new();
//!
//! // Insert data
//! let mut row = HashMap::new();
//! row.insert("timestamp".to_string(), Value::Timestamp(1000));
//! row.insert("event".to_string(), Value::String("click".to_string()));
//! row.insert("latency_ms".to_string(), Value::Int64(45));
//! engine.insert("events", row).unwrap();
//!
//! // Query data
//! let result = run_query(&engine, "SELECT event, COUNT(*) FROM events GROUP BY event").unwrap();
//! println!("Results: {:?}", result);
//! ```

pub mod alerts;
pub mod api;
pub mod cluster;
pub mod compaction;
pub mod data;
pub mod ingest;
pub mod query;
pub mod storage;

// Re-export commonly used types
pub use data::{DataType, Table, TableConfig, Value};
pub use query::{run_query, QueryError, QueryResult};
pub use storage::{StorageEngine, StorageError};

// Integrate allocation counter globally
// mod alloc_counter;
