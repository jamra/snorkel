//! Alerts service for threshold-based alerting
//!
//! Provides background monitoring of query results with configurable
//! thresholds and notification targets.

pub mod checker;
pub mod config;
pub mod notifier;

pub use checker::AlertChecker;
pub use config::{Alert, AlertCondition, AlertState, NotifyTarget};
pub use notifier::{Notifier, NotifierError};
