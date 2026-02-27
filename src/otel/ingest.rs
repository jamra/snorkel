//! OTLP ingest handler

use axum::{
    extract::State,
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use std::collections::HashMap;
use std::sync::Arc;

use super::model::{
    otlp_json::ExportTraceServiceRequest, AttributeValue, OtelSpan, SpanKind, SpanStatus,
};
use crate::api::handlers::AppState;

/// Table name for OTel traces
pub const OTEL_TRACES_TABLE: &str = "otel_traces";

/// Handle OTLP/HTTP trace export (JSON format)
///
/// Endpoint: POST /v1/traces
pub async fn handle_otlp_traces(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<ExportTraceServiceRequest>,
) -> impl IntoResponse {
    let mut spans_inserted = 0;
    let mut errors = Vec::new();

    for resource_spans in payload.resource_spans {
        // Extract service name from resource attributes
        let service_name = resource_spans
            .resource
            .as_ref()
            .and_then(|r| r.attributes.as_ref())
            .and_then(|attrs| {
                attrs.iter().find(|kv| kv.key == "service.name").and_then(
                    |kv| kv.value.string_value.clone(),
                )
            })
            .unwrap_or_else(|| "unknown".to_string());

        // Collect resource attributes
        let mut resource_attrs: HashMap<String, AttributeValue> = HashMap::new();
        if let Some(resource) = &resource_spans.resource {
            if let Some(attrs) = &resource.attributes {
                for kv in attrs {
                    if let Some(val) = kv.value.to_attribute_value() {
                        resource_attrs.insert(kv.key.clone(), val);
                    }
                }
            }
        }

        for scope_spans in resource_spans.scope_spans {
            // Add scope info to attributes
            let scope_name = scope_spans
                .scope
                .as_ref()
                .and_then(|s| s.name.clone())
                .unwrap_or_default();
            let scope_version = scope_spans
                .scope
                .as_ref()
                .and_then(|s| s.version.clone())
                .unwrap_or_default();

            for span in scope_spans.spans {
                // Parse timestamps (nanoseconds -> milliseconds)
                let start_time_ns: i64 = span.start_time_unix_nano.parse().unwrap_or(0);
                let end_time_ns: i64 = span.end_time_unix_nano.parse().unwrap_or(0);
                let start_time = start_time_ns / 1_000_000;
                let end_time = end_time_ns / 1_000_000;
                let duration_ms = end_time - start_time;

                // Collect span attributes
                let mut attributes = resource_attrs.clone();
                if !scope_name.is_empty() {
                    attributes.insert(
                        "otel.scope.name".to_string(),
                        AttributeValue::String(scope_name.clone()),
                    );
                }
                if !scope_version.is_empty() {
                    attributes.insert(
                        "otel.scope.version".to_string(),
                        AttributeValue::String(scope_version.clone()),
                    );
                }
                if let Some(span_attrs) = &span.attributes {
                    for kv in span_attrs {
                        if let Some(val) = kv.value.to_attribute_value() {
                            attributes.insert(kv.key.clone(), val);
                        }
                    }
                }

                let otel_span = OtelSpan {
                    trace_id: span.trace_id,
                    span_id: span.span_id,
                    parent_span_id: span.parent_span_id.unwrap_or_default(),
                    trace_state: span.trace_state.unwrap_or_default(),
                    service_name: service_name.clone(),
                    span_name: span.name,
                    span_kind: SpanKind::from_i32(span.kind.unwrap_or(0)),
                    start_time,
                    end_time,
                    duration_ms,
                    status_code: span
                        .status
                        .as_ref()
                        .map(|s| SpanStatus::from_i32(s.code.unwrap_or(0)))
                        .unwrap_or(SpanStatus::Unset),
                    status_message: span
                        .status
                        .as_ref()
                        .and_then(|s| s.message.clone())
                        .unwrap_or_default(),
                    attributes,
                    events_count: span.events.as_ref().map(|e| e.len() as i64).unwrap_or(0),
                    links_count: span.links.as_ref().map(|l| l.len() as i64).unwrap_or(0),
                };

                // Insert into storage
                let row = otel_span.to_row();
                if let Err(e) = state.engine.insert(OTEL_TRACES_TABLE, row) {
                    errors.push(format!("Failed to insert span: {}", e));
                } else {
                    spans_inserted += 1;
                }
            }
        }
    }

    if errors.is_empty() {
        (
            StatusCode::OK,
            Json(serde_json::json!({
                "partialSuccess": null
            })),
        )
    } else {
        tracing::warn!(
            "OTLP ingest had {} errors out of {} spans",
            errors.len(),
            spans_inserted + errors.len()
        );
        (
            StatusCode::OK, // OTLP spec says return 200 even on partial failure
            Json(serde_json::json!({
                "partialSuccess": {
                    "rejectedSpans": errors.len(),
                    "errorMessage": errors.first().unwrap_or(&"".to_string())
                }
            })),
        )
    }
}

/// Response for trace list queries
#[derive(serde::Serialize)]
pub struct TraceListResponse {
    pub traces: Vec<TraceSummary>,
}

/// Summary of a trace for list view
#[derive(serde::Serialize)]
pub struct TraceSummary {
    pub trace_id: String,
    pub root_service: String,
    pub root_span: String,
    pub span_count: i64,
    pub duration_ms: i64,
    pub start_time: i64,
    pub has_error: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_otlp_json() {
        let json = r#"{
            "resourceSpans": [{
                "resource": {
                    "attributes": [{
                        "key": "service.name",
                        "value": {"stringValue": "test-service"}
                    }]
                },
                "scopeSpans": [{
                    "scope": {"name": "my-lib", "version": "1.0"},
                    "spans": [{
                        "traceId": "5b8aa5a2d2c872e8321cf37308d69df2",
                        "spanId": "051581bf3cb55c13",
                        "name": "GET /api/users",
                        "kind": 2,
                        "startTimeUnixNano": "1544712660000000000",
                        "endTimeUnixNano": "1544712661000000000",
                        "status": {"code": 1}
                    }]
                }]
            }]
        }"#;

        let request: ExportTraceServiceRequest = serde_json::from_str(json).unwrap();
        assert_eq!(request.resource_spans.len(), 1);

        let rs = &request.resource_spans[0];
        let service_name = rs
            .resource
            .as_ref()
            .and_then(|r| r.attributes.as_ref())
            .and_then(|attrs| {
                attrs
                    .iter()
                    .find(|kv| kv.key == "service.name")
                    .and_then(|kv| kv.value.string_value.clone())
            });
        assert_eq!(service_name, Some("test-service".to_string()));
    }
}
