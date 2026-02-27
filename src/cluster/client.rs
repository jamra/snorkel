use serde::{Deserialize, Serialize};
use std::time::Duration;

use crate::data::Value;
use crate::query::QueryResult;

/// Client for communicating with peer nodes
#[derive(Debug, Clone)]
pub struct ClusterClient {
    http_client: reqwest::Client,
    #[allow(dead_code)]
    timeout: Duration,
}

/// Request to execute a query on a remote node
#[derive(Debug, Serialize, Deserialize)]
pub struct RemoteQueryRequest {
    pub sql: String,
    /// If true, return partial aggregates instead of final results
    pub partial: bool,
}

/// Response from a remote node
#[derive(Debug, Serialize, Deserialize)]
pub struct RemoteQueryResponse {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Value>>,
    pub rows_scanned: usize,
    pub shards_scanned: usize,
    /// For partial aggregates: the accumulator states
    pub partial_states: Option<Vec<PartialAggregateState>>,
}

/// Partial aggregate state for distributed merging
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartialAggregateState {
    pub group_key: Vec<Value>,
    pub aggregates: Vec<PartialAggregate>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PartialAggregate {
    Count(i64),
    Sum { sum: f64, has_value: bool },
    Avg { sum: f64, count: i64 },
    Min(Option<Value>),
    Max(Option<Value>),
    // For percentile, we'd need to pass the sample values
}

impl ClusterClient {
    pub fn new() -> Self {
        Self {
            http_client: reqwest::Client::builder()
                .timeout(Duration::from_secs(30))
                .build()
                .expect("Failed to create HTTP client"),
            timeout: Duration::from_secs(30),
        }
    }

    pub fn with_timeout(timeout: Duration) -> Self {
        Self {
            http_client: reqwest::Client::builder()
                .timeout(timeout)
                .build()
                .expect("Failed to create HTTP client"),
            timeout,
        }
    }

    /// Execute a query on a remote node
    pub async fn query(&self, addr: &str, sql: &str) -> Result<QueryResult, ClusterError> {
        let url = format!("http://{}/query", addr);
        let request = serde_json::json!({ "sql": sql });

        let response = self
            .http_client
            .post(&url)
            .json(&request)
            .send()
            .await
            .map_err(|e| ClusterError::Network(e.to_string()))?;

        if !response.status().is_success() {
            let error_text = response.text().await.unwrap_or_default();
            return Err(ClusterError::RemoteError(error_text));
        }

        let result: QueryResponse = response
            .json()
            .await
            .map_err(|e| ClusterError::Deserialization(e.to_string()))?;

        Ok(QueryResult {
            columns: result.columns,
            rows: result.rows,
            rows_scanned: result.rows_scanned,
            shards_scanned: result.shards_scanned,
            execution_time_ms: result.execution_time_ms,
            availability: None, // Will be populated by coordinator
        })
    }

    /// Execute a query on multiple nodes in parallel
    pub async fn query_all(
        &self,
        addrs: &[String],
        sql: &str,
    ) -> Vec<Result<QueryResult, ClusterError>> {
        let futures: Vec<_> = addrs
            .iter()
            .map(|addr| self.query(addr, sql))
            .collect();

        futures::future::join_all(futures).await
    }

    /// Check if a node is healthy
    pub async fn health_check(&self, addr: &str) -> Result<bool, ClusterError> {
        let url = format!("http://{}/health", addr);

        let response = self
            .http_client
            .get(&url)
            .send()
            .await
            .map_err(|e| ClusterError::Network(e.to_string()))?;

        Ok(response.status().is_success())
    }
}

impl Default for ClusterClient {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Deserialize)]
struct QueryResponse {
    columns: Vec<String>,
    rows: Vec<Vec<Value>>,
    rows_scanned: usize,
    shards_scanned: usize,
    execution_time_ms: u64,
}

#[derive(Debug, thiserror::Error)]
pub enum ClusterError {
    #[error("Network error: {0}")]
    Network(String),

    #[error("Remote error: {0}")]
    RemoteError(String),

    #[error("Deserialization error: {0}")]
    Deserialization(String),

    #[error("No healthy nodes available")]
    NoHealthyNodes,
}
