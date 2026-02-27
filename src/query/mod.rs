pub mod aggregates;
pub mod cache;
pub mod executor;
pub mod parser;
pub mod planner;

pub use cache::{QueryCache, CacheStats};
pub use executor::{execute_query, ExecuteError, QueryResult, AvailabilityMetrics};
pub use parser::{parse_query, ParseError, ParsedQuery};
pub use planner::{plan_query, PlanError, QueryPlan};

/// Convenience function to parse, plan, and execute a query
pub fn run_query(
    engine: &crate::storage::StorageEngine,
    sql: &str,
) -> Result<QueryResult, QueryError> {
    let parsed = parse_query(sql)?;
    let plan = plan_query(parsed)?;
    let result = execute_query(engine, &plan)?;
    Ok(result)
}

#[derive(Debug, thiserror::Error)]
pub enum QueryError {
    #[error("Parse error: {0}")]
    Parse(#[from] ParseError),

    #[error("Plan error: {0}")]
    Plan(#[from] PlanError),

    #[error("Execute error: {0}")]
    Execute(#[from] ExecuteError),
}
