//! OTel span data model

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Span kind from OTel spec
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SpanKind {
    Unspecified,
    Internal,
    Server,
    Client,
    Producer,
    Consumer,
}

impl SpanKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            SpanKind::Unspecified => "UNSPECIFIED",
            SpanKind::Internal => "INTERNAL",
            SpanKind::Server => "SERVER",
            SpanKind::Client => "CLIENT",
            SpanKind::Producer => "PRODUCER",
            SpanKind::Consumer => "CONSUMER",
        }
    }

    pub fn from_i32(v: i32) -> Self {
        match v {
            1 => SpanKind::Internal,
            2 => SpanKind::Server,
            3 => SpanKind::Client,
            4 => SpanKind::Producer,
            5 => SpanKind::Consumer,
            _ => SpanKind::Unspecified,
        }
    }
}

/// Span status from OTel spec
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SpanStatus {
    Unset,
    Ok,
    Error,
}

impl SpanStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            SpanStatus::Unset => "UNSET",
            SpanStatus::Ok => "OK",
            SpanStatus::Error => "ERROR",
        }
    }

    pub fn from_i32(v: i32) -> Self {
        match v {
            1 => SpanStatus::Ok,
            2 => SpanStatus::Error,
            _ => SpanStatus::Unset,
        }
    }
}

/// A flattened OTel span ready for storage
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OtelSpan {
    /// 32 hex character trace ID
    pub trace_id: String,
    /// 16 hex character span ID
    pub span_id: String,
    /// Parent span ID (empty string if root)
    pub parent_span_id: String,
    /// Trace state (W3C format)
    pub trace_state: String,
    /// Service name from resource attributes
    pub service_name: String,
    /// Operation name
    pub span_name: String,
    /// Span kind
    pub span_kind: SpanKind,
    /// Start time in milliseconds since epoch
    pub start_time: i64,
    /// End time in milliseconds since epoch
    pub end_time: i64,
    /// Duration in milliseconds
    pub duration_ms: i64,
    /// Status code
    pub status_code: SpanStatus,
    /// Status message (usually for errors)
    pub status_message: String,
    /// Flattened attributes (resource + span attributes)
    pub attributes: HashMap<String, AttributeValue>,
    /// Number of events in this span
    pub events_count: i64,
    /// Number of links in this span
    pub links_count: i64,
}

/// Attribute value types
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum AttributeValue {
    String(String),
    Int(i64),
    Float(f64),
    Bool(bool),
}

impl OtelSpan {
    /// Convert to a row for storage
    pub fn to_row(&self) -> HashMap<String, crate::data::Value> {
        use crate::data::Value;

        let mut row = HashMap::new();

        // Core span fields
        row.insert("timestamp".to_string(), Value::Timestamp(self.start_time));
        row.insert("trace_id".to_string(), Value::String(self.trace_id.clone()));
        row.insert("span_id".to_string(), Value::String(self.span_id.clone()));
        row.insert(
            "parent_span_id".to_string(),
            Value::String(self.parent_span_id.clone()),
        );
        row.insert(
            "service_name".to_string(),
            Value::String(self.service_name.clone()),
        );
        row.insert(
            "span_name".to_string(),
            Value::String(self.span_name.clone()),
        );
        row.insert(
            "span_kind".to_string(),
            Value::String(self.span_kind.as_str().to_string()),
        );
        row.insert("start_time".to_string(), Value::Timestamp(self.start_time));
        row.insert("end_time".to_string(), Value::Timestamp(self.end_time));
        row.insert("duration_ms".to_string(), Value::Int64(self.duration_ms));
        row.insert(
            "status_code".to_string(),
            Value::String(self.status_code.as_str().to_string()),
        );
        row.insert(
            "status_message".to_string(),
            Value::String(self.status_message.clone()),
        );
        row.insert("events_count".to_string(), Value::Int64(self.events_count));
        row.insert("links_count".to_string(), Value::Int64(self.links_count));

        // Flatten attributes with prefix
        for (key, value) in &self.attributes {
            let col_name = format!("attr.{}", key);
            let val = match value {
                AttributeValue::String(s) => Value::String(s.clone()),
                AttributeValue::Int(i) => Value::Int64(*i),
                AttributeValue::Float(f) => Value::Float64(*f),
                AttributeValue::Bool(b) => Value::Bool(*b),
            };
            row.insert(col_name, val);
        }

        row
    }
}

/// OTLP JSON format structures (for HTTP/JSON ingest)
pub mod otlp_json {
    use super::*;

    #[derive(Debug, Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub struct ExportTraceServiceRequest {
        pub resource_spans: Vec<ResourceSpans>,
    }

    #[derive(Debug, Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub struct ResourceSpans {
        pub resource: Option<Resource>,
        pub scope_spans: Vec<ScopeSpans>,
    }

    #[derive(Debug, Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub struct Resource {
        pub attributes: Option<Vec<KeyValue>>,
    }

    #[derive(Debug, Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub struct ScopeSpans {
        pub scope: Option<InstrumentationScope>,
        pub spans: Vec<Span>,
    }

    #[derive(Debug, Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub struct InstrumentationScope {
        pub name: Option<String>,
        pub version: Option<String>,
    }

    #[derive(Debug, Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub struct Span {
        pub trace_id: String,
        pub span_id: String,
        pub parent_span_id: Option<String>,
        pub trace_state: Option<String>,
        pub name: String,
        pub kind: Option<i32>,
        pub start_time_unix_nano: String,
        pub end_time_unix_nano: String,
        pub attributes: Option<Vec<KeyValue>>,
        pub events: Option<Vec<Event>>,
        pub links: Option<Vec<Link>>,
        pub status: Option<Status>,
    }

    #[derive(Debug, Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub struct KeyValue {
        pub key: String,
        pub value: AnyValue,
    }

    #[derive(Debug, Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub struct AnyValue {
        pub string_value: Option<String>,
        pub int_value: Option<String>, // OTLP sends as string
        pub double_value: Option<f64>,
        pub bool_value: Option<bool>,
    }

    #[derive(Debug, Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub struct Event {
        pub name: String,
        pub time_unix_nano: Option<String>,
        pub attributes: Option<Vec<KeyValue>>,
    }

    #[derive(Debug, Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub struct Link {
        pub trace_id: String,
        pub span_id: String,
        pub attributes: Option<Vec<KeyValue>>,
    }

    #[derive(Debug, Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub struct Status {
        pub code: Option<i32>,
        pub message: Option<String>,
    }

    impl AnyValue {
        pub fn to_attribute_value(&self) -> Option<AttributeValue> {
            if let Some(s) = &self.string_value {
                return Some(AttributeValue::String(s.clone()));
            }
            if let Some(i) = &self.int_value {
                if let Ok(parsed) = i.parse::<i64>() {
                    return Some(AttributeValue::Int(parsed));
                }
            }
            if let Some(d) = self.double_value {
                return Some(AttributeValue::Float(d));
            }
            if let Some(b) = self.bool_value {
                return Some(AttributeValue::Bool(b));
            }
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_span_kind_from_i32() {
        assert_eq!(SpanKind::from_i32(0), SpanKind::Unspecified);
        assert_eq!(SpanKind::from_i32(1), SpanKind::Internal);
        assert_eq!(SpanKind::from_i32(2), SpanKind::Server);
        assert_eq!(SpanKind::from_i32(3), SpanKind::Client);
    }

    #[test]
    fn test_span_to_row() {
        let span = OtelSpan {
            trace_id: "abc123".to_string(),
            span_id: "def456".to_string(),
            parent_span_id: "".to_string(),
            trace_state: "".to_string(),
            service_name: "test-service".to_string(),
            span_name: "GET /api".to_string(),
            span_kind: SpanKind::Server,
            start_time: 1000,
            end_time: 1050,
            duration_ms: 50,
            status_code: SpanStatus::Ok,
            status_message: "".to_string(),
            attributes: HashMap::new(),
            events_count: 0,
            links_count: 0,
        };

        let row = span.to_row();
        assert_eq!(
            row.get("service_name"),
            Some(&crate::data::Value::String("test-service".to_string()))
        );
        assert_eq!(
            row.get("duration_ms"),
            Some(&crate::data::Value::Int64(50))
        );
    }
}
