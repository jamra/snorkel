//! OpenTelemetry support for Snorkel
//!
//! Provides OTLP ingest endpoint and trace viewing capabilities.
//!
//! ## Ingest
//!
//! Send traces via OTLP/HTTP:
//! ```bash
//! # Configure your app's OTLP exporter
//! OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:8080
//! OTEL_EXPORTER_OTLP_PROTOCOL=http/json
//! ```
//!
//! Or configure an OTel Collector to forward:
//! ```yaml
//! exporters:
//!   otlphttp:
//!     endpoint: http://snorkel:8080
//! ```
//!
//! ## Querying
//!
//! Traces are stored in the `otel_traces` table:
//! ```sql
//! SELECT trace_id, service_name, span_name, duration_ms
//! FROM otel_traces
//! WHERE service_name = 'my-service'
//!   AND duration_ms > 100
//! ORDER BY start_time DESC
//! LIMIT 100
//! ```

mod ingest;
mod model;

pub use ingest::handle_otlp_traces;
pub use model::{OtelSpan, SpanKind, SpanStatus};
