//! Notification handlers for alerts

use std::collections::HashMap;

use super::config::{Alert, NotifyTarget};

/// Notifier for sending alert notifications
pub struct Notifier {
    client: reqwest::Client,
}

impl Notifier {
    /// Create a new notifier
    pub fn new() -> Self {
        Self {
            client: reqwest::Client::new(),
        }
    }

    /// Send notification to all targets for an alert
    pub async fn notify(&self, alert: &Alert, message: &str) -> Result<(), NotifierError> {
        let mut errors = Vec::new();

        for target in &alert.targets {
            if let Err(e) = self.notify_target(alert, target, message).await {
                errors.push(e);
            }
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(NotifierError::Multiple(errors))
        }
    }

    /// Send notification to a single target
    async fn notify_target(
        &self,
        alert: &Alert,
        target: &NotifyTarget,
        message: &str,
    ) -> Result<(), NotifierError> {
        match target {
            NotifyTarget::Log => {
                tracing::warn!(
                    alert_id = %alert.id,
                    alert_name = %alert.name,
                    "Alert triggered: {}",
                    message
                );
                Ok(())
            }
            NotifyTarget::Webhook { url, headers } => {
                self.send_webhook(alert, url, headers, message).await
            }
            NotifyTarget::Email { to } => {
                // Email not implemented yet, just log
                tracing::info!(
                    alert_id = %alert.id,
                    recipients = ?to,
                    "Email notification (not implemented): {}",
                    message
                );
                Ok(())
            }
        }
    }

    /// Send webhook notification
    async fn send_webhook(
        &self,
        alert: &Alert,
        url: &str,
        headers: &HashMap<String, String>,
        message: &str,
    ) -> Result<(), NotifierError> {
        let payload = serde_json::json!({
            "alert_id": alert.id,
            "alert_name": alert.name,
            "message": message,
            "query": alert.query,
            "timestamp": chrono::Utc::now().to_rfc3339(),
            "state": {
                "firing": alert.state.firing,
                "consecutive_fires": alert.state.consecutive_fires,
            }
        });

        let mut request = self.client.post(url).json(&payload);

        for (key, value) in headers {
            request = request.header(key, value);
        }

        let response = request.send().await.map_err(|e| {
            NotifierError::Webhook(format!("Failed to send webhook: {}", e))
        })?;

        if !response.status().is_success() {
            return Err(NotifierError::Webhook(format!(
                "Webhook returned status {}",
                response.status()
            )));
        }

        tracing::debug!(
            alert_id = %alert.id,
            url = %url,
            "Webhook notification sent"
        );

        Ok(())
    }
}

impl Default for Notifier {
    fn default() -> Self {
        Self::new()
    }
}

/// Notifier errors
#[derive(Debug, thiserror::Error)]
pub enum NotifierError {
    #[error("Webhook error: {0}")]
    Webhook(String),

    #[error("Email error: {0}")]
    Email(String),

    #[error("Multiple notification failures: {0:?}")]
    Multiple(Vec<NotifierError>),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::alerts::config::AlertCondition;
    use std::time::Duration;

    #[tokio::test]
    async fn test_log_notification() {
        let notifier = Notifier::new();
        let alert = Alert::new(
            "test",
            "Test",
            "SELECT 1",
            AlertCondition::HasResults,
        )
        .with_target(NotifyTarget::Log);

        // Log notification should always succeed
        let result = notifier
            .notify_target(&alert, &NotifyTarget::Log, "test message")
            .await;
        assert!(result.is_ok());
    }
}
