use super::aggregates
use std::cell::RefCell;
use crate::data::Value;

// Thread-local pool of reusable row buffers to minimize allocations
thread_local! {
    static ROW_BUF_POOL: RefCell<Vec<Vec<Value>>> = RefCell::new(Vec::new());
}

fn get_row_buf(initial_capacity: usize) -> Vec<Value> {
    ROW_BUF_POOL.with(|p| p.borrow_mut().pop().unwrap_or_else(|| Vec::with_capacity(initial_capacity)))
}

fn return_row_buf(buf: Vec<Value>) {
    ROW_BUF_POOL.with(|p| p.borrow_mut().push(buf));
}

// End buffer pool setup

use super::aggregates::{create_accumulator, Accumulator};
use super::parser::FilterOperator;
use super::planner::{
    FilterPlan, GroupByColumnPlan, GroupByPlan, OrderByPlan, ProjectionPlan, QueryPlan,
};
use crate::data::{Shard, Table, Value};
use crate::storage::StorageEngine;
use std::collections::HashMap;
use std::sync::Arc;

/// Query execution result
#[derive(Debug, Clone, serde::Serialize)]
pub struct QueryResult {
    /// Column names
    pub columns: Vec<String>,
    /// Result rows
    pub rows: Vec<Vec<Value>>,
    /// Number of rows scanned
    pub rows_scanned: usize,
    /// Number of shards scanned
    pub shards_scanned: usize,
    /// Execution time in milliseconds
    pub execution_time_ms: u64,
}

impl QueryResult {
    pub fn empty() -> Self {
        Self {
            columns: Vec::new(),
            rows: Vec::new(),
            rows_scanned: 0,
            shards_scanned: 0,
            execution_time_ms: 0,
        }
    }

    pub fn row_count(&self) -> usize {
        self.rows.len()
    }
}

/// Execute a query plan against the storage engine
pub fn execute_query(engine: &StorageEngine, plan: &QueryPlan) -> Result<QueryResult, ExecuteError> {
    let start = std::time::Instant::now();

    let table = engine
        .get_table(&plan.table)
        .ok_or_else(|| ExecuteError::TableNotFound(plan.table.clone()))?;

    // Get relevant shards based on time range
    let shards = get_relevant_shards(&table, plan);
    let shards_scanned = shards.len();

    // Expand wildcard projections
    let projections = expand_wildcards(&plan.projections, &table);

    // Check if we need aggregation
    let has_aggregations = projections
        .iter()
        .any(|p| matches!(p, ProjectionPlan::Aggregate { .. }));

    let (columns, mut rows, rows_scanned) = if has_aggregations {
        execute_aggregation(&shards, plan, &projections)?
    } else {
        execute_scan(&shards, plan, &projections)?
    };

    // Apply ORDER BY
    if !plan.order_by.is_empty() {
        apply_order_by(&mut rows, &columns, &plan.order_by);
    }

    // Apply LIMIT
    if let Some(limit) = plan.limit {
        rows.truncate(limit);
    }

    let execution_time_ms = start.elapsed().as_millis() as u64;

    Ok(QueryResult {
        columns,
        rows,
        rows_scanned,
        shards_scanned,
        execution_time_ms,
    })
}

fn get_relevant_shards(table: &Table, plan: &QueryPlan) -> Vec<Arc<Shard>> {
    if let Some(time_range) = &plan.time_range {
        let start = time_range.start.unwrap_or(i64::MIN);
        let end = time_range.end.unwrap_or(i64::MAX);
        table.get_shards_in_range(start, end)
    } else {
        table.get_shards()
    }
}

fn expand_wildcards(projections: &[ProjectionPlan], table: &Table) -> Vec<ProjectionPlan> {
    let mut result = Vec::new();

    for proj in projections {
        if let ProjectionPlan::Column { name, .. } = proj {
            if name == "*" {
                // Expand to all columns in the table
                for col_name in table.get_schema().keys() {
                    result.push(ProjectionPlan::Column {
                        name: col_name.clone(),
                        output_name: col_name.clone(),
                    });
                }
                continue;
            }
        }
        result.push(proj.clone());
    }

    result
}

/// Execute a simple scan (no aggregation)
fn execute_scan(
    shards: &[Arc<Shard>],
    plan: &QueryPlan,
    projections: &[ProjectionPlan],
) -> Result<(Vec<String>, Vec<Vec<Value>>, usize), ExecuteError> {
    // Column names for result
    let columns: Vec<String> = projections
        .iter()
        .map(|p| match p {
            ProjectionPlan::Column { output_name, .. } => output_name.clone(),
            ProjectionPlan::TimeBucket { output_name, .. } => output_name.clone(),
            ProjectionPlan::Aggregate { output_name, .. } => output_name.clone(),
        })
        .collect();

    let mut rows = Vec::new();
    let mut rows_scanned = 0;
    let num_cols = projections.len();

    for shard in shards {
        for row_idx in shard.row_indices() {
            rows_scanned += 1;
            if !passes_filters(shard, row_idx, &plan.filters) {
                continue;
            }
            // Get a reusable buffer for this row
            let mut buf = get_row_buf(num_cols);
            buf.clear();
            for p in projections {
                buf.push(project_value(shard, row_idx, p));
            }
            rows.push(buf);
        }
    }

    Ok((columns, rows, rows_scanned))
}
fn execute_scan(
    shards: &[Arc<Shard>],
    plan: &QueryPlan,
    projections: &[ProjectionPlan],
) -> Result<(Vec<String>, Vec<Vec<Value>>, usize), ExecuteError> {
    let columns: Vec<String> = projections
        .iter()
        .map(|p| match p {
            ProjectionPlan::Column { output_name, .. } => output_name.clone(),
            ProjectionPlan::TimeBucket { output_name, .. } => output_name.clone(),
            ProjectionPlan::Aggregate { output_name, .. } => output_name.clone(),
        })
        .collect();

    let mut rows = Vec::new();
    let mut rows_scanned = 0;

    for shard in shards {
        for row_idx in shard.row_indices() {
            rows_scanned += 1;

            // Apply filters
            if !passes_filters(shard, row_idx, &plan.filters) {
                continue;
            }

            // Project columns
            let row: Vec<Value> = projections
                .iter()
                .map(|p| project_value(shard, row_idx, p))
                .collect();

            rows.push(row);
        }
    }

    Ok((columns, rows, rows_scanned))
}

/// Execute an aggregation query
fn execute_aggregation(
    shards: &[Arc<Shard>],
    plan: &QueryPlan,
    projections: &[ProjectionPlan],
) -> Result<(Vec<String>, Vec<Vec<Value>>, usize), ExecuteError> {
    let columns: Vec<String> = projections
        .iter()
        .map(|p| match p {
            ProjectionPlan::Column { output_name, .. } => output_name.clone(),
            ProjectionPlan::TimeBucket { output_name, .. } => output_name.clone(),
            ProjectionPlan::Aggregate { output_name, .. } => output_name.clone(),
        })
        .collect();

    let mut rows_scanned = 0;

    // Group key -> (group values, accumulators for each aggregation)
    let mut groups: HashMap<Vec<Value>, (Vec<Value>, Vec<Box<dyn Accumulator>>)> = HashMap::new();

    for shard in shards {
        for row_idx in shard.row_indices() {
            rows_scanned += 1;

            // Apply filters
            if !passes_filters(shard, row_idx, &plan.filters) {
                continue;
            }

            // Compute group key
            let group_key = if let Some(ref group_by) = plan.group_by {
                compute_group_key(shard, row_idx, group_by)
            } else {
                vec![] // Single global group
            };

            // Get or create accumulators for this group
            let (_group_values, accumulators) = groups.entry(group_key.clone()).or_insert_with(|| {
                let accs: Vec<Box<dyn Accumulator>> = projections
                    .iter()
                    .filter_map(|p| {
                        if let ProjectionPlan::Aggregate { function, column, .. } = p {
                            Some(create_accumulator(*function, column))
                        } else {
                            None
                        }
                    })
                    .collect();
                (group_key, accs)
            });

            // Accumulate values
            let mut acc_idx = 0;
            for proj in projections {
                if let ProjectionPlan::Aggregate { column, .. } = proj {
                    let value = if let Some(col) = column {
                        shard.get_value(row_idx, col).unwrap_or(Value::Null)
                    } else {
                        Value::Int64(1) // COUNT(*)
                    };
                    accumulators[acc_idx].accumulate(&value);
                    acc_idx += 1;
                }
            }
        }
    }

    // Build result rows
    let rows: Vec<Vec<Value>> = groups
        .into_iter()
        .map(|(_, (group_values, accumulators))| {
            let mut row = Vec::new();
            let mut acc_idx = 0;
            let mut group_idx = 0;

            for proj in projections {
                match proj {
                    ProjectionPlan::Column { .. } => {
                        // Find the value in group_values
                        if group_idx < group_values.len() {
                            row.push(group_values[group_idx].clone());
                            group_idx += 1;
                        } else {
                            row.push(Value::Null);
                        }
                    }
                    ProjectionPlan::TimeBucket { .. } => {
                        if group_idx < group_values.len() {
                            row.push(group_values[group_idx].clone());
                            group_idx += 1;
                        } else {
                            row.push(Value::Null);
                        }
                    }
                    ProjectionPlan::Aggregate { .. } => {
                        row.push(accumulators[acc_idx].result());
                        acc_idx += 1;
                    }
                }
            }
            row
        })
        .collect();

    Ok((columns, rows, rows_scanned))
}

fn passes_filters(shard: &Shard, row_idx: usize, filters: &[FilterPlan]) -> bool {
    for filter in filters {
        let value = shard
            .get_value(row_idx, &filter.column)
            .unwrap_or(Value::Null);

        let passes = match filter.operator {
            FilterOperator::Eq => value == filter.value,
            FilterOperator::NotEq => value != filter.value,
            FilterOperator::Lt => value < filter.value,
            FilterOperator::LtEq => value <= filter.value,
            FilterOperator::Gt => value > filter.value,
            FilterOperator::GtEq => value >= filter.value,
            FilterOperator::Like => {
                if let (Value::String(s), Value::String(pattern)) = (&value, &filter.value) {
                    like_match(s, pattern)
                } else {
                    false
                }
            }
        };

        if !passes {
            return false;
        }
    }
    true
}

fn like_match(s: &str, pattern: &str) -> bool {
    // Simple LIKE implementation with % wildcard
    let pattern = pattern.replace('%', ".*").replace('_', ".");
    regex::Regex::new(&format!("^{}$", pattern))
        .map(|re| re.is_match(s))
        .unwrap_or(false)
}

fn project_value(shard: &Shard, row_idx: usize, proj: &ProjectionPlan) -> Value {
    match proj {
        ProjectionPlan::Column { name, .. } => {
            shard.get_value(row_idx, name).unwrap_or(Value::Null)
        }
        ProjectionPlan::TimeBucket { interval_ms, column, .. } => {
            let ts = shard
                .get_value(row_idx, column)
                .and_then(|v| v.as_i64())
                .unwrap_or(0);
            let bucket = (ts / interval_ms) * interval_ms;
            Value::Timestamp(bucket)
        }
        ProjectionPlan::Aggregate { .. } => {
            // Aggregates should not appear in non-aggregation queries
            Value::Null
        }
    }
}

fn compute_group_key(shard: &Shard, row_idx: usize, group_by: &GroupByPlan) -> Vec<Value> {
    group_by
        .columns
        .iter()
        .map(|col| match col {
            GroupByColumnPlan::Column(name) => {
                shard.get_value(row_idx, name).unwrap_or(Value::Null)
            }
            GroupByColumnPlan::TimeBucket { interval_ms, column } => {
                let ts = shard
                    .get_value(row_idx, column)
                    .and_then(|v| v.as_i64())
                    .unwrap_or(0);
                let bucket = (ts / interval_ms) * interval_ms;
                Value::Timestamp(bucket)
            }
        })
        .collect()
}

fn apply_order_by(rows: &mut Vec<Vec<Value>>, columns: &[String], order_by: &[OrderByPlan]) {
    // Build column index map
    let col_indices: HashMap<&str, usize> = columns
        .iter()
        .enumerate()
        .map(|(i, c)| (c.as_str(), i))
        .collect();

    rows.sort_by(|a, b| {
        for ob in order_by {
            if let Some(&idx) = col_indices.get(ob.column.as_str()) {
                let cmp = a[idx].cmp(&b[idx]);
                if cmp != std::cmp::Ordering::Equal {
                    return if ob.descending { cmp.reverse() } else { cmp };
                }
            }
        }
        std::cmp::Ordering::Equal
    });
}

#[derive(Debug, thiserror::Error)]
pub enum ExecuteError {
    #[error("Table '{0}' not found")]
    TableNotFound(String),

    #[error("Column '{0}' not found")]
    ColumnNotFound(String),

    #[error("Execution error: {0}")]
    General(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::parser::parse_query;
    use crate::query::planner::plan_query;

    fn setup_test_engine() -> StorageEngine {
        let engine = StorageEngine::new();

        // Insert test data
        for i in 0..100 {
            let mut row = HashMap::new();
            row.insert("timestamp".to_string(), Value::Timestamp(i * 1000));
            row.insert(
                "event".to_string(),
                Value::String(if i % 2 == 0 { "click" } else { "view" }.to_string()),
            );
            row.insert("value".to_string(), Value::Int64(i));
            row.insert("latency".to_string(), Value::Float64(i as f64 * 1.5));

            engine.insert("events", row).unwrap();
        }

        engine
    }

    #[test]
    fn test_simple_select() {
        let engine = setup_test_engine();

        let query = parse_query("SELECT * FROM events LIMIT 10").unwrap();
        let plan = plan_query(query).unwrap();
        let result = execute_query(&engine, &plan).unwrap();

        assert_eq!(result.row_count(), 10);
        assert!(result.columns.contains(&"event".to_string()));
        assert!(result.columns.contains(&"value".to_string()));
    }

    #[test]
    fn test_select_with_filter() {
        let engine = setup_test_engine();

        let query = parse_query("SELECT * FROM events WHERE event = 'click'").unwrap();
        let plan = plan_query(query).unwrap();
        let result = execute_query(&engine, &plan).unwrap();

        assert_eq!(result.row_count(), 50); // Half are clicks

        // Verify all results are clicks
        let event_col = result.columns.iter().position(|c| c == "event").unwrap();
        for row in &result.rows {
            assert_eq!(row[event_col], Value::String("click".to_string()));
        }
    }

    #[test]
    fn test_count_aggregation() {
        let engine = setup_test_engine();

        let query = parse_query("SELECT COUNT(*) FROM events").unwrap();
        let plan = plan_query(query).unwrap();
        let result = execute_query(&engine, &plan).unwrap();

        assert_eq!(result.row_count(), 1);
        assert_eq!(result.rows[0][0], Value::Int64(100));
    }

    #[test]
    fn test_group_by() {
        let engine = setup_test_engine();

        let query =
            parse_query("SELECT event, COUNT(*) FROM events GROUP BY event ORDER BY event")
                .unwrap();
        let plan = plan_query(query).unwrap();
        let result = execute_query(&engine, &plan).unwrap();

        assert_eq!(result.row_count(), 2); // click and view

        // Find the counts
        for row in &result.rows {
            if row[0] == Value::String("click".to_string()) {
                assert_eq!(row[1], Value::Int64(50));
            } else if row[0] == Value::String("view".to_string()) {
                assert_eq!(row[1], Value::Int64(50));
            }
        }
    }

    #[test]
    fn test_avg_aggregation() {
        let engine = setup_test_engine();

        let query = parse_query("SELECT AVG(value) FROM events").unwrap();
        let plan = plan_query(query).unwrap();
        let result = execute_query(&engine, &plan).unwrap();

        // Average of 0..99 is 49.5
        if let Value::Float64(avg) = &result.rows[0][0] {
            assert!((avg - 49.5).abs() < 0.01);
        } else {
            panic!("Expected float result");
        }
    }

    #[test]
    fn test_order_by_desc() {
        let engine = setup_test_engine();

        let query =
            parse_query("SELECT value FROM events ORDER BY value DESC LIMIT 5").unwrap();
        let plan = plan_query(query).unwrap();
        let result = execute_query(&engine, &plan).unwrap();

        assert_eq!(result.row_count(), 5);
        assert_eq!(result.rows[0][0], Value::Int64(99));
        assert_eq!(result.rows[1][0], Value::Int64(98));
        assert_eq!(result.rows[2][0], Value::Int64(97));
    }

    #[test]
    fn test_time_range_filter() {
        let engine = setup_test_engine();

        let query =
            parse_query("SELECT COUNT(*) FROM events WHERE timestamp >= 10000 AND timestamp < 20000")
                .unwrap();
        let plan = plan_query(query).unwrap();
        let result = execute_query(&engine, &plan).unwrap();

        // Timestamps 10000..20000 covers indices 10..20 = 10 rows
        assert_eq!(result.rows[0][0], Value::Int64(10));
    }
}
