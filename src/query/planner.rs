use super::parser::{
    AggregateFunction, FilterOperator, GroupByColumn, ParsedQuery, Projection,
};
use crate::data::Value;

/// Query execution plan
#[derive(Debug)]
pub struct QueryPlan {
    /// Table name to query
    pub table: String,
    /// Time range filter (optimization)
    pub time_range: Option<TimeRange>,
    /// Column filters
    pub filters: Vec<FilterPlan>,
    /// Columns to read from storage
    pub required_columns: Vec<String>,
    /// Projection plan
    pub projections: Vec<ProjectionPlan>,
    /// Group by plan
    pub group_by: Option<GroupByPlan>,
    /// Order by plan
    pub order_by: Vec<OrderByPlan>,
    /// Result limit
    pub limit: Option<usize>,
}

#[derive(Debug, Clone)]
pub struct TimeRange {
    pub start: Option<i64>,
    pub end: Option<i64>,
}

#[derive(Debug, Clone)]
pub struct FilterPlan {
    pub column: String,
    pub operator: FilterOperator,
    pub value: Value,
}

#[derive(Debug, Clone)]
pub enum ProjectionPlan {
    /// Pass through a column value
    Column { name: String, output_name: String },
    /// Compute an aggregation
    Aggregate {
        function: AggregateFunction,
        column: Option<String>,
        output_name: String,
    },
    /// Compute time bucket
    TimeBucket {
        interval_ms: i64,
        column: String,
        output_name: String,
    },
}

#[derive(Debug, Clone)]
pub struct GroupByPlan {
    pub columns: Vec<GroupByColumnPlan>,
}

#[derive(Debug, Clone)]
pub enum GroupByColumnPlan {
    Column(String),
    TimeBucket { interval_ms: i64, column: String },
}

#[derive(Debug, Clone)]
pub struct OrderByPlan {
    pub column: String,
    pub descending: bool,
}

/// Create an execution plan from a parsed query
pub fn plan_query(query: ParsedQuery) -> Result<QueryPlan, PlanError> {
    let mut required_columns = Vec::new();
    let mut projections = Vec::new();
    let mut time_range = TimeRange {
        start: None,
        end: None,
    };

    // Always need timestamp for time-based operations
    required_columns.push("timestamp".to_string());

    // Extract time range from filters
    let mut filters = Vec::new();
    for filter in &query.filters {
        if filter.column == "timestamp" {
            update_time_range(&mut time_range, &filter.operator, &filter.value);
        }
        filters.push(FilterPlan {
            column: filter.column.clone(),
            operator: filter.operator,
            value: filter.value.clone(),
        });
        if !required_columns.contains(&filter.column) {
            required_columns.push(filter.column.clone());
        }
    }

    // Plan projections
    for (idx, proj) in query.projections.iter().enumerate() {
        match proj {
            Projection::Wildcard => {
                // Will be expanded during execution when we know the schema
                projections.push(ProjectionPlan::Column {
                    name: "*".to_string(),
                    output_name: "*".to_string(),
                });
            }
            Projection::Column(name) => {
                if !required_columns.contains(name) {
                    required_columns.push(name.clone());
                }
                projections.push(ProjectionPlan::Column {
                    name: name.clone(),
                    output_name: name.clone(),
                });
            }
            Projection::Aggregation {
                function,
                column,
                alias,
            } => {
                if let Some(col) = column {
                    if !required_columns.contains(col) {
                        required_columns.push(col.clone());
                    }
                }
                let output_name = alias.clone().unwrap_or_else(|| {
                    format!(
                        "{}_{}",
                        format!("{:?}", function).to_lowercase(),
                        column.as_deref().unwrap_or("*")
                    )
                });
                projections.push(ProjectionPlan::Aggregate {
                    function: *function,
                    column: column.clone(),
                    output_name,
                });
            }
            Projection::TimeBucket {
                interval_ms,
                column,
                alias,
            } => {
                if !required_columns.contains(column) {
                    required_columns.push(column.clone());
                }
                let output_name = alias
                    .clone()
                    .unwrap_or_else(|| format!("time_bucket_{}", idx));
                projections.push(ProjectionPlan::TimeBucket {
                    interval_ms: *interval_ms,
                    column: column.clone(),
                    output_name,
                });
            }
        }
    }

    // Plan group by
    let group_by = if query.group_by.is_empty() {
        None
    } else {
        let columns = query
            .group_by
            .iter()
            .map(|gb| match gb {
                GroupByColumn::Column(name) => {
                    if !required_columns.contains(name) {
                        required_columns.push(name.clone());
                    }
                    GroupByColumnPlan::Column(name.clone())
                }
                GroupByColumn::TimeBucket { interval_ms, column } => {
                    if !required_columns.contains(column) {
                        required_columns.push(column.clone());
                    }
                    GroupByColumnPlan::TimeBucket {
                        interval_ms: *interval_ms,
                        column: column.clone(),
                    }
                }
            })
            .collect();
        Some(GroupByPlan { columns })
    };

    // Plan order by
    let order_by = query
        .order_by
        .iter()
        .map(|ob| OrderByPlan {
            column: ob.column.clone(),
            descending: ob.descending,
        })
        .collect();

    // Determine time range for shard pruning
    let time_range_opt = if time_range.start.is_some() || time_range.end.is_some() {
        Some(time_range)
    } else {
        None
    };

    Ok(QueryPlan {
        table: query.table,
        time_range: time_range_opt,
        filters,
        required_columns,
        projections,
        group_by,
        order_by,
        limit: query.limit,
    })
}

fn update_time_range(range: &mut TimeRange, op: &FilterOperator, value: &Value) {
    let Some(ts) = value.as_i64() else {
        return;
    };

    match op {
        FilterOperator::Gt => {
            range.start = Some(range.start.map(|s| s.max(ts + 1)).unwrap_or(ts + 1));
        }
        FilterOperator::GtEq => {
            range.start = Some(range.start.map(|s| s.max(ts)).unwrap_or(ts));
        }
        FilterOperator::Lt => {
            range.end = Some(range.end.map(|e| e.min(ts)).unwrap_or(ts));
        }
        FilterOperator::LtEq => {
            range.end = Some(range.end.map(|e| e.min(ts + 1)).unwrap_or(ts + 1));
        }
        FilterOperator::Eq => {
            range.start = Some(ts);
            range.end = Some(ts + 1);
        }
        _ => {}
    }
}

/// Check if projections contain aggregations
pub fn has_aggregations(plan: &QueryPlan) -> bool {
    plan.projections
        .iter()
        .any(|p| matches!(p, ProjectionPlan::Aggregate { .. }))
}

/// Get output column names from the plan
pub fn get_output_columns(plan: &QueryPlan) -> Vec<String> {
    plan.projections
        .iter()
        .map(|p| match p {
            ProjectionPlan::Column { output_name, .. } => output_name.clone(),
            ProjectionPlan::Aggregate { output_name, .. } => output_name.clone(),
            ProjectionPlan::TimeBucket { output_name, .. } => output_name.clone(),
        })
        .collect()
}

#[derive(Debug, thiserror::Error)]
pub enum PlanError {
    #[error("Planning error: {0}")]
    General(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::parser::parse_query;

    #[test]
    fn test_simple_plan() {
        let query = parse_query("SELECT * FROM events").unwrap();
        let plan = plan_query(query).unwrap();

        assert_eq!(plan.table, "events");
        assert!(plan.time_range.is_none());
        assert!(plan.group_by.is_none());
    }

    #[test]
    fn test_plan_with_time_filter() {
        let query = parse_query("SELECT * FROM events WHERE timestamp > 1000 AND timestamp < 2000")
            .unwrap();
        let plan = plan_query(query).unwrap();

        let time_range = plan.time_range.unwrap();
        assert_eq!(time_range.start, Some(1001));
        assert_eq!(time_range.end, Some(2000));
    }

    #[test]
    fn test_plan_aggregation() {
        let query = parse_query("SELECT event, COUNT(*), AVG(latency) FROM events GROUP BY event")
            .unwrap();
        let plan = plan_query(query).unwrap();

        assert_eq!(plan.projections.len(), 3);
        assert!(plan.group_by.is_some());

        let group_by = plan.group_by.unwrap();
        assert_eq!(group_by.columns.len(), 1);
    }

    #[test]
    fn test_required_columns() {
        let query = parse_query(
            "SELECT event, SUM(value) FROM events WHERE user_id = 123 GROUP BY event",
        )
        .unwrap();
        let plan = plan_query(query).unwrap();

        assert!(plan.required_columns.contains(&"timestamp".to_string()));
        assert!(plan.required_columns.contains(&"event".to_string()));
        assert!(plan.required_columns.contains(&"value".to_string()));
        assert!(plan.required_columns.contains(&"user_id".to_string()));
    }
}
