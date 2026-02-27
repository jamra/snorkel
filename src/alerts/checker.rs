//! Background alert checker

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use tokio::sync::mpsc;
use tokio::time::interval;

use super::config::{Alert, AlertCondition, AlertState, AlertValue};
use super::notifier::Notifier;
use crate::query::{QueryResult, run_query};
use crate::storage::StorageEngine;

/// Background alert checker
pub struct AlertChecker {
    /// Registered alerts
    alerts: Arc<RwLock<HashMap<String, Alert>>>,
    /// Storage engine for running queries
    engine: Arc<StorageEngine>,
    /// Notifier for sending notifications
    notifier: Notifier,
    /// Shutdown signal sender
    shutdown_tx: Option<mpsc::Sender<()>>,
}

impl AlertChecker {
    /// Create a new alert checker
    pub fn new(engine: Arc<StorageEngine>) -> Self {
        Self {
            alerts: Arc::new(RwLock::new(HashMap::new())),
            engine,
            notifier: Notifier::new(),
            shutdown_tx: None,
        }
    }

    /// Register an alert
    pub fn register(&self, alert: Alert) {
        let mut alerts = self.alerts.write();
        alerts.insert(alert.id.clone(), alert);
    }

    /// Unregister an alert
    pub fn unregister(&self, id: &str) -> Option<Alert> {
        let mut alerts = self.alerts.write();
        alerts.remove(id)
    }

    /// Get an alert by ID
    pub fn get(&self, id: &str) -> Option<Alert> {
        let alerts = self.alerts.read();
        alerts.get(id).cloned()
    }

    /// List all alerts
    pub fn list(&self) -> Vec<Alert> {
        let alerts = self.alerts.read();
        alerts.values().cloned().collect()
    }

    /// Update an alert
    pub fn update(&self, alert: Alert) -> Option<Alert> {
        let mut alerts = self.alerts.write();
        alerts.insert(alert.id.clone(), alert)
    }

    /// Enable/disable an alert
    pub fn set_enabled(&self, id: &str, enabled: bool) -> bool {
        let mut alerts = self.alerts.write();
        if let Some(alert) = alerts.get_mut(id) {
            alert.enabled = enabled;
            true
        } else {
            false
        }
    }

    /// Start the background checker
    pub fn start(&mut self, check_interval: Duration) -> tokio::task::JoinHandle<()> {
        let (shutdown_tx, mut shutdown_rx) = mpsc::channel::<()>(1);
        self.shutdown_tx = Some(shutdown_tx);

        let alerts = Arc::clone(&self.alerts);
        let engine = Arc::clone(&self.engine);
        let notifier = Notifier::new();

        tokio::spawn(async move {
            let mut ticker = interval(check_interval);
            let mut last_checks: HashMap<String, Instant> = HashMap::new();

            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        Self::check_alerts(&alerts, &engine, &notifier, &mut last_checks).await;
                    }
                    _ = shutdown_rx.recv() => {
                        tracing::info!("Alert checker shutting down");
                        break;
                    }
                }
            }
        })
    }

    /// Stop the background checker
    pub async fn stop(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(()).await;
        }
    }

    /// Check all alerts that are due
    async fn check_alerts(
        alerts: &Arc<RwLock<HashMap<String, Alert>>>,
        engine: &Arc<StorageEngine>,
        notifier: &Notifier,
        last_checks: &mut HashMap<String, Instant>,
    ) {
        // Get alerts that need checking
        let alerts_to_check: Vec<Alert> = {
            let alerts = alerts.read();
            alerts
                .values()
                .filter(|a| {
                    if !a.enabled {
                        return false;
                    }
                    last_checks
                        .get(&a.id)
                        .map(|t| t.elapsed() >= a.interval)
                        .unwrap_or(true)
                })
                .cloned()
                .collect()
        };

        for alert in alerts_to_check {
            let alert_id = alert.id.clone();
            last_checks.insert(alert_id.clone(), Instant::now());

            match Self::check_alert(&alert, engine).await {
                Ok((triggered, message)) => {
                    // Update state
                    {
                        let mut alerts_map = alerts.write();
                        if let Some(a) = alerts_map.get_mut(&alert_id) {
                            let now = chrono::Utc::now().timestamp_millis();
                            a.state.last_checked = Some(now);
                            a.state.last_error = None;

                            if triggered {
                                a.state.firing = true;
                                a.state.last_triggered = Some(now);
                                a.state.consecutive_fires += 1;
                            } else {
                                a.state.firing = false;
                                a.state.consecutive_fires = 0;
                            }
                        }
                    }

                    // Send notification if triggered
                    if triggered {
                        let alert_for_notify = {
                            let alerts_map = alerts.read();
                            alerts_map.get(&alert_id).cloned()
                        };

                        if let Some(a) = alert_for_notify {
                            if let Err(e) = notifier.notify(&a, &message).await {
                                tracing::error!(
                                    alert_id = %alert_id,
                                    error = %e,
                                    "Failed to send notification"
                                );
                            }
                        }
                    }
                }
                Err(e) => {
                    let mut alerts_map = alerts.write();
                    if let Some(a) = alerts_map.get_mut(&alert_id) {
                        a.state.last_checked = Some(chrono::Utc::now().timestamp_millis());
                        a.state.last_error = Some(e.to_string());
                    }
                    tracing::error!(
                        alert_id = %alert_id,
                        error = %e,
                        "Alert check failed"
                    );
                }
            }
        }
    }

    /// Check a single alert
    async fn check_alert(
        alert: &Alert,
        engine: &Arc<StorageEngine>,
    ) -> Result<(bool, String), CheckError> {
        // Run the query
        let result = run_query(engine, &alert.query)
            .map_err(|e| CheckError::Query(e.to_string()))?;

        // Evaluate the condition
        let (triggered, message) = Self::evaluate_condition(&alert.condition, &result)?;

        Ok((triggered, message))
    }

    /// Evaluate an alert condition against a query result
    fn evaluate_condition(
        condition: &AlertCondition,
        result: &QueryResult,
    ) -> Result<(bool, String), CheckError> {
        match condition {
            AlertCondition::NoResults => {
                let triggered = result.rows.is_empty();
                Ok((
                    triggered,
                    if triggered {
                        "Query returned no results".to_string()
                    } else {
                        format!("Query returned {} rows", result.rows.len())
                    },
                ))
            }
            AlertCondition::HasResults => {
                let triggered = !result.rows.is_empty();
                Ok((
                    triggered,
                    if triggered {
                        format!("Query returned {} rows", result.rows.len())
                    } else {
                        "Query returned no results".to_string()
                    },
                ))
            }
            AlertCondition::RowCountGreaterThan { threshold } => {
                let count = result.rows.len();
                let triggered = count > *threshold;
                Ok((
                    triggered,
                    format!(
                        "Row count {} {} threshold {}",
                        count,
                        if triggered { ">" } else { "<=" },
                        threshold
                    ),
                ))
            }
            AlertCondition::GreaterThan { column, threshold } => {
                let value = Self::get_column_value(column, result)?;
                let triggered = value > *threshold;
                Ok((
                    triggered,
                    format!(
                        "{} = {} {} threshold {}",
                        column,
                        value,
                        if triggered { ">" } else { "<=" },
                        threshold
                    ),
                ))
            }
            AlertCondition::LessThan { column, threshold } => {
                let value = Self::get_column_value(column, result)?;
                let triggered = value < *threshold;
                Ok((
                    triggered,
                    format!(
                        "{} = {} {} threshold {}",
                        column,
                        value,
                        if triggered { "<" } else { ">=" },
                        threshold
                    ),
                ))
            }
            AlertCondition::GreaterOrEqual { column, threshold } => {
                let value = Self::get_column_value(column, result)?;
                let triggered = value >= *threshold;
                Ok((
                    triggered,
                    format!(
                        "{} = {} {} threshold {}",
                        column,
                        value,
                        if triggered { ">=" } else { "<" },
                        threshold
                    ),
                ))
            }
            AlertCondition::LessOrEqual { column, threshold } => {
                let value = Self::get_column_value(column, result)?;
                let triggered = value <= *threshold;
                Ok((
                    triggered,
                    format!(
                        "{} = {} {} threshold {}",
                        column,
                        value,
                        if triggered { "<=" } else { ">" },
                        threshold
                    ),
                ))
            }
            AlertCondition::Equals { column, value: target } => {
                let actual = Self::get_column_alert_value(column, result)?;
                let triggered = actual == *target;
                Ok((
                    triggered,
                    format!(
                        "{} = {:?} {} {:?}",
                        column,
                        actual,
                        if triggered { "==" } else { "!=" },
                        target
                    ),
                ))
            }
            AlertCondition::NotEquals { column, value: target } => {
                let actual = Self::get_column_alert_value(column, result)?;
                let triggered = actual != *target;
                Ok((
                    triggered,
                    format!(
                        "{} = {:?} {} {:?}",
                        column,
                        actual,
                        if triggered { "!=" } else { "==" },
                        target
                    ),
                ))
            }
        }
    }

    /// Get a numeric value from a column in the first row
    fn get_column_value(column: &str, result: &QueryResult) -> Result<f64, CheckError> {
        let col_idx = result
            .columns
            .iter()
            .position(|c| c == column)
            .ok_or_else(|| CheckError::ColumnNotFound(column.to_string()))?;

        let row = result
            .rows
            .first()
            .ok_or(CheckError::NoRows)?;

        let value = row
            .get(col_idx)
            .ok_or_else(|| CheckError::ColumnNotFound(column.to_string()))?;

        value
            .as_f64()
            .ok_or_else(|| CheckError::InvalidValue(format!("{:?} is not numeric", value)))
    }

    /// Get a value from a column as AlertValue
    fn get_column_alert_value(column: &str, result: &QueryResult) -> Result<AlertValue, CheckError> {
        let col_idx = result
            .columns
            .iter()
            .position(|c| c == column)
            .ok_or_else(|| CheckError::ColumnNotFound(column.to_string()))?;

        let row = result
            .rows
            .first()
            .ok_or(CheckError::NoRows)?;

        let value = row
            .get(col_idx)
            .ok_or_else(|| CheckError::ColumnNotFound(column.to_string()))?;

        Ok(match value {
            crate::data::Value::Int64(i) => AlertValue::Int(*i),
            crate::data::Value::Float64(f) => AlertValue::Float(*f),
            crate::data::Value::String(s) => AlertValue::String(s.clone()),
            crate::data::Value::Bool(b) => AlertValue::Bool(*b),
            crate::data::Value::Null => AlertValue::Null,
            crate::data::Value::Timestamp(_) => {
                AlertValue::Int(value.as_i64().unwrap_or(0))
            }
        })
    }
}

/// Alert check errors
#[derive(Debug, thiserror::Error)]
pub enum CheckError {
    #[error("Query error: {0}")]
    Query(String),

    #[error("Column not found: {0}")]
    ColumnNotFound(String),

    #[error("No rows in result")]
    NoRows,

    #[error("Invalid value: {0}")]
    InvalidValue(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::Value;

    fn make_result(columns: Vec<&str>, rows: Vec<Vec<Value>>) -> QueryResult {
        let rows_scanned = rows.len();
        QueryResult {
            columns: columns.into_iter().map(String::from).collect(),
            rows,
            rows_scanned,
            shards_scanned: 1,
            execution_time_ms: 1,
            availability: None,
        }
    }

    #[test]
    fn test_evaluate_no_results() {
        let cond = AlertCondition::NoResults;

        let result = make_result(vec!["col"], vec![]);
        let (triggered, _) = AlertChecker::evaluate_condition(&cond, &result).unwrap();
        assert!(triggered);

        let result = make_result(vec!["col"], vec![vec![Value::Int64(1)]]);
        let (triggered, _) = AlertChecker::evaluate_condition(&cond, &result).unwrap();
        assert!(!triggered);
    }

    #[test]
    fn test_evaluate_has_results() {
        let cond = AlertCondition::HasResults;

        let result = make_result(vec!["col"], vec![vec![Value::Int64(1)]]);
        let (triggered, _) = AlertChecker::evaluate_condition(&cond, &result).unwrap();
        assert!(triggered);

        let result = make_result(vec!["col"], vec![]);
        let (triggered, _) = AlertChecker::evaluate_condition(&cond, &result).unwrap();
        assert!(!triggered);
    }

    #[test]
    fn test_evaluate_greater_than() {
        let cond = AlertCondition::GreaterThan {
            column: "count".to_string(),
            threshold: 100.0,
        };

        let result = make_result(vec!["count"], vec![vec![Value::Int64(150)]]);
        let (triggered, _) = AlertChecker::evaluate_condition(&cond, &result).unwrap();
        assert!(triggered);

        let result = make_result(vec!["count"], vec![vec![Value::Int64(50)]]);
        let (triggered, _) = AlertChecker::evaluate_condition(&cond, &result).unwrap();
        assert!(!triggered);
    }

    #[test]
    fn test_evaluate_row_count() {
        let cond = AlertCondition::RowCountGreaterThan { threshold: 2 };

        let result = make_result(
            vec!["col"],
            vec![
                vec![Value::Int64(1)],
                vec![Value::Int64(2)],
                vec![Value::Int64(3)],
            ],
        );
        let (triggered, _) = AlertChecker::evaluate_condition(&cond, &result).unwrap();
        assert!(triggered);

        let result = make_result(vec!["col"], vec![vec![Value::Int64(1)]]);
        let (triggered, _) = AlertChecker::evaluate_condition(&cond, &result).unwrap();
        assert!(!triggered);
    }
}
