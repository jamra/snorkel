use std::collections::HashMap;
use std::sync::Arc;

use crate::data::Value;
use crate::query::{QueryResult, run_query};
use crate::storage::StorageEngine;

use super::client::{ClusterClient, ClusterError};
use super::config::ClusterConfig;

/// Distributed query coordinator
pub struct Coordinator {
    config: ClusterConfig,
    client: ClusterClient,
    local_engine: Arc<StorageEngine>,
}

impl Coordinator {
    pub fn new(config: ClusterConfig, local_engine: Arc<StorageEngine>) -> Self {
        Self {
            config,
            client: ClusterClient::new(),
            local_engine,
        }
    }

    /// Execute a query across the cluster
    pub async fn execute_query(&self, sql: &str) -> Result<QueryResult, CoordinatorError> {
        let start = std::time::Instant::now();

        if !self.config.is_distributed() {
            // Single node mode - just execute locally
            return run_query(&self.local_engine, sql)
                .map_err(|e| CoordinatorError::Query(e.to_string()));
        }

        // Distributed mode - fan out to all nodes (including self)
        let peer_addrs = self.config.peer_addrs();

        // Execute on peers in parallel
        let peer_futures = self.client.query_all(&peer_addrs, sql);

        // Execute locally
        let local_result = run_query(&self.local_engine, sql)
            .map_err(|e| CoordinatorError::Query(e.to_string()))?;

        // Wait for peer results
        let peer_results = peer_futures.await;

        // Collect all successful results
        let mut all_results = vec![local_result];
        for result in peer_results {
            match result {
                Ok(r) => all_results.push(r),
                Err(e) => {
                    tracing::warn!("Peer query failed: {}", e);
                    // Continue with other results - partial failure is OK
                }
            }
        }

        if all_results.is_empty() {
            return Err(CoordinatorError::NoResults);
        }

        // Merge results
        let merged = self.merge_results(all_results, sql)?;

        let execution_time_ms = start.elapsed().as_millis() as u64;

        Ok(QueryResult {
            execution_time_ms,
            ..merged
        })
    }

    /// Merge results from multiple nodes
    fn merge_results(
        &self,
        results: Vec<QueryResult>,
        _sql: &str,
    ) -> Result<QueryResult, CoordinatorError> {
        if results.is_empty() {
            return Err(CoordinatorError::NoResults);
        }

        if results.len() == 1 {
            return Ok(results.into_iter().next().unwrap());
        }

        let first = &results[0];
        let columns = first.columns.clone();

        // Check if this is an aggregation query (has aggregation columns)
        let is_aggregation = columns.iter().any(|c| {
            c.starts_with("count_")
                || c.starts_with("sum_")
                || c.starts_with("avg_")
                || c.starts_with("min_")
                || c.starts_with("max_")
        });

        let (rows, rows_scanned, shards_scanned) = if is_aggregation {
            self.merge_aggregation_results(&results, &columns)
        } else {
            self.merge_scan_results(&results)
        };

        Ok(QueryResult {
            columns,
            rows,
            rows_scanned,
            shards_scanned,
            execution_time_ms: 0, // Will be set by caller
        })
    }

    /// Merge aggregation results by re-aggregating
    fn merge_aggregation_results(
        &self,
        results: &[QueryResult],
        columns: &[String],
    ) -> (Vec<Vec<Value>>, usize, usize) {
        // Find group key columns (non-aggregate columns)
        let group_key_indices: Vec<usize> = columns
            .iter()
            .enumerate()
            .filter(|(_, c)| {
                !c.starts_with("count_")
                    && !c.starts_with("sum_")
                    && !c.starts_with("avg_")
                    && !c.starts_with("min_")
                    && !c.starts_with("max_")
            })
            .map(|(i, _)| i)
            .collect();

        // Find aggregate column indices and their types
        let agg_columns: Vec<(usize, AggType)> = columns
            .iter()
            .enumerate()
            .filter_map(|(i, c)| {
                if c.starts_with("count_") {
                    Some((i, AggType::Count))
                } else if c.starts_with("sum_") {
                    Some((i, AggType::Sum))
                } else if c.starts_with("avg_") {
                    Some((i, AggType::Avg))
                } else if c.starts_with("min_") {
                    Some((i, AggType::Min))
                } else if c.starts_with("max_") {
                    Some((i, AggType::Max))
                } else {
                    None
                }
            })
            .collect();

        // Group by key and merge aggregates
        // Key: group values, Value: (count, sum, min, max) for each agg column
        let mut groups: HashMap<Vec<Value>, Vec<AggState>> = HashMap::new();

        let mut total_rows_scanned = 0;
        let mut total_shards_scanned = 0;

        for result in results {
            total_rows_scanned += result.rows_scanned;
            total_shards_scanned += result.shards_scanned;

            for row in &result.rows {
                let group_key: Vec<Value> = group_key_indices
                    .iter()
                    .map(|&i| row.get(i).cloned().unwrap_or(Value::Null))
                    .collect();

                let states = groups.entry(group_key).or_insert_with(|| {
                    agg_columns
                        .iter()
                        .map(|(_, agg_type)| AggState::new(agg_type.clone()))
                        .collect()
                });

                // Update each aggregate state
                for (state_idx, (col_idx, _)) in agg_columns.iter().enumerate() {
                    if let Some(value) = row.get(*col_idx) {
                        states[state_idx].merge_value(value);
                    }
                }
            }
        }

        // Build final result rows
        let mut rows: Vec<Vec<Value>> = groups
            .into_iter()
            .map(|(group_key, states)| {
                let mut row = Vec::with_capacity(columns.len());

                let mut group_idx = 0;
                let mut state_idx = 0;

                for (col_idx, _col_name) in columns.iter().enumerate() {
                    if group_key_indices.contains(&col_idx) {
                        row.push(group_key[group_idx].clone());
                        group_idx += 1;
                    } else {
                        row.push(states[state_idx].result());
                        state_idx += 1;
                    }
                }

                row
            })
            .collect();

        // Sort by group key for consistent ordering
        rows.sort_by(|a, b| {
            for i in &group_key_indices {
                if let (Some(av), Some(bv)) = (a.get(*i), b.get(*i)) {
                    match av.cmp(bv) {
                        std::cmp::Ordering::Equal => continue,
                        other => return other,
                    }
                }
            }
            std::cmp::Ordering::Equal
        });

        (rows, total_rows_scanned, total_shards_scanned)
    }

    /// Merge non-aggregation (scan) results by concatenating
    fn merge_scan_results(&self, results: &[QueryResult]) -> (Vec<Vec<Value>>, usize, usize) {
        let mut all_rows = Vec::new();
        let mut total_rows_scanned = 0;
        let mut total_shards_scanned = 0;

        for result in results {
            all_rows.extend(result.rows.clone());
            total_rows_scanned += result.rows_scanned;
            total_shards_scanned += result.shards_scanned;
        }

        (all_rows, total_rows_scanned, total_shards_scanned)
    }
}

#[derive(Clone)]
enum AggType {
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

struct AggState {
    agg_type: AggType,
    count: i64,
    sum: f64,
    min: Option<Value>,
    max: Option<Value>,
}

impl AggState {
    fn new(agg_type: AggType) -> Self {
        Self {
            agg_type,
            count: 0,
            sum: 0.0,
            min: None,
            max: None,
        }
    }

    fn merge_value(&mut self, value: &Value) {
        match &self.agg_type {
            AggType::Count => {
                if let Some(c) = value.as_i64() {
                    self.count += c;
                }
            }
            AggType::Sum => {
                if let Some(s) = value.as_f64() {
                    self.sum += s;
                    self.count += 1;
                }
            }
            AggType::Avg => {
                // For AVG, we receive the partial average
                // We need to track count separately to properly merge
                // For now, we'll use a weighted approach
                if let Some(avg) = value.as_f64() {
                    // Approximate: treat each partial avg as one sample
                    self.sum += avg;
                    self.count += 1;
                }
            }
            AggType::Min => {
                if !value.is_null() {
                    match &self.min {
                        None => self.min = Some(value.clone()),
                        Some(current) if value < current => self.min = Some(value.clone()),
                        _ => {}
                    }
                }
            }
            AggType::Max => {
                if !value.is_null() {
                    match &self.max {
                        None => self.max = Some(value.clone()),
                        Some(current) if value > current => self.max = Some(value.clone()),
                        _ => {}
                    }
                }
            }
        }
    }

    fn result(&self) -> Value {
        match &self.agg_type {
            AggType::Count => Value::Int64(self.count),
            AggType::Sum => Value::Float64(self.sum),
            AggType::Avg => {
                if self.count > 0 {
                    Value::Float64(self.sum / self.count as f64)
                } else {
                    Value::Null
                }
            }
            AggType::Min => self.min.clone().unwrap_or(Value::Null),
            AggType::Max => self.max.clone().unwrap_or(Value::Null),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum CoordinatorError {
    #[error("Query error: {0}")]
    Query(String),

    #[error("Cluster error: {0}")]
    Cluster(#[from] ClusterError),

    #[error("No results from any node")]
    NoResults,
}
