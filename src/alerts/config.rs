//! Alert configuration types

use std::time::Duration;
use serde::{Deserialize, Serialize};

/// Alert definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Alert {
    /// Unique alert ID
    pub id: String,
    /// Human-readable name
    pub name: String,
    /// SQL query to execute
    pub query: String,
    /// Condition to check against query result
    pub condition: AlertCondition,
    /// Check interval
    #[serde(with = "duration_serde")]
    pub interval: Duration,
    /// Notification targets
    pub targets: Vec<NotifyTarget>,
    /// Whether alert is enabled
    pub enabled: bool,
    /// Current state
    #[serde(default)]
    pub state: AlertState,
}

impl Alert {
    /// Create a new alert
    pub fn new(
        id: impl Into<String>,
        name: impl Into<String>,
        query: impl Into<String>,
        condition: AlertCondition,
    ) -> Self {
        Self {
            id: id.into(),
            name: name.into(),
            query: query.into(),
            condition,
            interval: Duration::from_secs(60),
            targets: vec![NotifyTarget::Log],
            enabled: true,
            state: AlertState::default(),
        }
    }

    /// Set check interval
    pub fn with_interval(mut self, interval: Duration) -> Self {
        self.interval = interval;
        self
    }

    /// Add notification target
    pub fn with_target(mut self, target: NotifyTarget) -> Self {
        self.targets.push(target);
        self
    }

    /// Set enabled state
    pub fn with_enabled(mut self, enabled: bool) -> Self {
        self.enabled = enabled;
        self
    }
}

/// Alert condition to evaluate
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AlertCondition {
    /// Trigger when value > threshold
    GreaterThan { column: String, threshold: f64 },
    /// Trigger when value < threshold
    LessThan { column: String, threshold: f64 },
    /// Trigger when value == target
    Equals { column: String, value: AlertValue },
    /// Trigger when value != target
    NotEquals { column: String, value: AlertValue },
    /// Trigger when value >= threshold
    GreaterOrEqual { column: String, threshold: f64 },
    /// Trigger when value <= threshold
    LessOrEqual { column: String, threshold: f64 },
    /// Trigger when row count > threshold
    RowCountGreaterThan { threshold: usize },
    /// Trigger when row count == 0
    NoResults,
    /// Trigger when any results exist
    HasResults,
}

impl AlertCondition {
    /// Get the column name this condition checks (if any)
    pub fn column(&self) -> Option<&str> {
        match self {
            AlertCondition::GreaterThan { column, .. }
            | AlertCondition::LessThan { column, .. }
            | AlertCondition::Equals { column, .. }
            | AlertCondition::NotEquals { column, .. }
            | AlertCondition::GreaterOrEqual { column, .. }
            | AlertCondition::LessOrEqual { column, .. } => Some(column),
            AlertCondition::RowCountGreaterThan { .. }
            | AlertCondition::NoResults
            | AlertCondition::HasResults => None,
        }
    }
}

/// Value for equality comparisons
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum AlertValue {
    Int(i64),
    Float(f64),
    String(String),
    Bool(bool),
    Null,
}

impl AlertValue {
    /// Convert to f64 for numeric comparisons
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            AlertValue::Int(i) => Some(*i as f64),
            AlertValue::Float(f) => Some(*f),
            _ => None,
        }
    }
}

/// Current state of an alert
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AlertState {
    /// Whether alert is currently firing
    pub firing: bool,
    /// Last check timestamp (unix millis)
    pub last_checked: Option<i64>,
    /// Last triggered timestamp (unix millis)
    pub last_triggered: Option<i64>,
    /// Number of consecutive fires
    pub consecutive_fires: u32,
    /// Last error message
    pub last_error: Option<String>,
}

/// Notification target
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum NotifyTarget {
    /// Log to tracing
    Log,
    /// HTTP webhook
    Webhook {
        url: String,
        #[serde(default)]
        headers: std::collections::HashMap<String, String>,
    },
    /// Email (placeholder for future implementation)
    Email { to: Vec<String> },
}

/// Duration serialization helper
mod duration_serde {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use std::time::Duration;

    #[derive(Serialize, Deserialize)]
    struct DurationHelper {
        secs: u64,
        #[serde(default)]
        nanos: u32,
    }

    pub fn serialize<S>(duration: &Duration, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        DurationHelper {
            secs: duration.as_secs(),
            nanos: duration.subsec_nanos(),
        }
        .serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Duration, D::Error>
    where
        D: Deserializer<'de>,
    {
        let helper = DurationHelper::deserialize(deserializer)?;
        Ok(Duration::new(helper.secs, helper.nanos))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_alert_builder() {
        let alert = Alert::new(
            "test-alert",
            "Test Alert",
            "SELECT COUNT(*) FROM events",
            AlertCondition::GreaterThan {
                column: "count".to_string(),
                threshold: 100.0,
            },
        )
        .with_interval(Duration::from_secs(30))
        .with_target(NotifyTarget::Log);

        assert_eq!(alert.id, "test-alert");
        assert_eq!(alert.interval.as_secs(), 30);
        assert!(alert.enabled);
    }

    #[test]
    fn test_alert_condition_column() {
        let cond = AlertCondition::GreaterThan {
            column: "count".to_string(),
            threshold: 10.0,
        };
        assert_eq!(cond.column(), Some("count"));

        let cond = AlertCondition::NoResults;
        assert_eq!(cond.column(), None);
    }
}
