use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

use crate::cluster::{ClusterConfig, Coordinator};
use crate::data::{value::flatten_json, TableConfig, Value};
use crate::query::{run_query, QueryResult};
use crate::storage::StorageEngine;

/// Application state shared across handlers
pub struct AppState {
    pub engine: Arc<StorageEngine>,
    pub coordinator: Option<Arc<Coordinator>>,
    pub cluster_config: ClusterConfig,
}

// ============================================================================
// Health Check
// ============================================================================

#[derive(Serialize)]
pub struct HealthResponse {
    pub status: &'static str,
    pub version: &'static str,
}

pub async fn health_check() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "healthy",
        version: env!("CARGO_PKG_VERSION"),
    })
}

// ============================================================================
// Ingest
// ============================================================================

#[derive(Deserialize)]
pub struct IngestRequest {
    pub table: String,
    pub rows: Vec<serde_json::Map<String, serde_json::Value>>,
}

#[derive(Serialize)]
pub struct IngestResponse {
    pub inserted: usize,
    pub errors: usize,
}

pub async fn ingest(
    State(state): State<Arc<AppState>>,
    Json(request): Json<IngestRequest>,
) -> Result<Json<IngestResponse>, ApiError> {
    // Flatten nested JSON objects/arrays into dot-notation columns
    let rows: Vec<HashMap<String, Value>> = request
        .rows
        .iter()
        .map(|row| flatten_json(row))
        .collect();

    let total = rows.len();
    let inserted = state
        .engine
        .insert_batch(&request.table, rows)
        .map_err(|e| ApiError::Internal(e.to_string()))?;

    Ok(Json(IngestResponse {
        inserted,
        errors: total - inserted,
    }))
}

// ============================================================================
// Query
// ============================================================================

#[derive(Deserialize)]
pub struct QueryRequest {
    pub sql: String,
}

#[derive(Serialize)]
pub struct QueryResponse {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<serde_json::Value>>,
    pub row_count: usize,
    pub rows_scanned: usize,
    pub shards_scanned: usize,
    pub execution_time_ms: u64,
}

impl From<QueryResult> for QueryResponse {
    fn from(result: QueryResult) -> Self {
        let rows: Vec<Vec<serde_json::Value>> = result
            .rows
            .into_iter()
            .map(|row| row.into_iter().map(value_to_json).collect())
            .collect();

        Self {
            columns: result.columns,
            row_count: rows.len(),
            rows,
            rows_scanned: result.rows_scanned,
            shards_scanned: result.shards_scanned,
            execution_time_ms: result.execution_time_ms,
        }
    }
}

fn value_to_json(value: Value) -> serde_json::Value {
    match value {
        Value::Null => serde_json::Value::Null,
        Value::Bool(b) => serde_json::Value::Bool(b),
        Value::Int64(i) => serde_json::json!(i),
        Value::Float64(f) => serde_json::json!(f),
        Value::String(s) => serde_json::Value::String(s),
        Value::Timestamp(t) => serde_json::json!(t),
    }
}

pub async fn query(
    State(state): State<Arc<AppState>>,
    Json(request): Json<QueryRequest>,
) -> Result<Json<QueryResponse>, ApiError> {
    let result = if let Some(ref coordinator) = state.coordinator {
        // Distributed query
        coordinator
            .execute_query(&request.sql)
            .await
            .map_err(|e| ApiError::Query(e.to_string()))?
    } else {
        // Local query
        run_query(&state.engine, &request.sql).map_err(|e| ApiError::Query(e.to_string()))?
    };

    Ok(Json(result.into()))
}

// ============================================================================
// Table Management
// ============================================================================

#[derive(Serialize)]
pub struct TablesResponse {
    pub tables: Vec<TableInfo>,
}

#[derive(Serialize)]
pub struct TableInfo {
    pub name: String,
    pub row_count: usize,
    pub shard_count: usize,
    pub memory_bytes: usize,
}

pub async fn list_tables(State(state): State<Arc<AppState>>) -> Json<TablesResponse> {
    let tables = state
        .engine
        .all_table_stats()
        .into_iter()
        .map(|stats| TableInfo {
            name: stats.name,
            row_count: stats.row_count,
            shard_count: stats.shard_count,
            memory_bytes: stats.memory_bytes,
        })
        .collect();

    Json(TablesResponse { tables })
}

#[derive(Serialize)]
pub struct SchemaResponse {
    pub table: String,
    pub columns: Vec<ColumnInfo>,
}

#[derive(Serialize)]
pub struct ColumnInfo {
    pub name: String,
    #[serde(rename = "type")]
    pub data_type: String,
}

pub async fn table_schema(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> Result<Json<SchemaResponse>, ApiError> {
    let schema = state
        .engine
        .table_schema(&name)
        .ok_or_else(|| ApiError::NotFound(format!("Table '{}' not found", name)))?;

    let columns = schema
        .into_iter()
        .map(|(name, dtype)| ColumnInfo {
            name,
            data_type: dtype.to_string(),
        })
        .collect();

    Ok(Json(SchemaResponse {
        table: name,
        columns,
    }))
}

#[derive(Deserialize)]
pub struct CreateTableRequest {
    pub name: String,
    #[serde(default)]
    pub ttl_ms: Option<i64>,
    #[serde(default)]
    pub shard_duration_ms: Option<i64>,
}

#[derive(Serialize)]
pub struct CreateTableResponse {
    pub name: String,
    pub created: bool,
}

pub async fn create_table(
    State(state): State<Arc<AppState>>,
    Json(request): Json<CreateTableRequest>,
) -> Result<Json<CreateTableResponse>, ApiError> {
    let mut config = TableConfig::new(&request.name);

    if let Some(ttl) = request.ttl_ms {
        config = config.with_ttl(ttl);
    }

    if let Some(duration) = request.shard_duration_ms {
        config = config.with_shard_duration(duration);
    }

    state
        .engine
        .create_table(config)
        .map_err(|e| ApiError::BadRequest(e.to_string()))?;

    Ok(Json(CreateTableResponse {
        name: request.name,
        created: true,
    }))
}

pub async fn drop_table(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    state
        .engine
        .drop_table(&name)
        .map_err(|e| ApiError::NotFound(e.to_string()))?;

    Ok(Json(serde_json::json!({ "dropped": name })))
}

// ============================================================================
// Stats
// ============================================================================

#[derive(Serialize)]
pub struct StatsResponse {
    pub tables: usize,
    pub total_rows: usize,
    pub total_shards: usize,
    pub memory: MemoryInfo,
}

#[derive(Serialize)]
pub struct MemoryInfo {
    pub current_bytes: usize,
    pub peak_bytes: usize,
    pub max_bytes: usize,
    pub usage_percent: f64,
}

pub async fn stats(State(state): State<Arc<AppState>>) -> Json<StatsResponse> {
    let table_stats = state.engine.all_table_stats();
    let memory = state.engine.memory_stats();

    let total_rows: usize = table_stats.iter().map(|t| t.row_count).sum();
    let total_shards: usize = table_stats.iter().map(|t| t.shard_count).sum();

    Json(StatsResponse {
        tables: table_stats.len(),
        total_rows,
        total_shards,
        memory: MemoryInfo {
            current_bytes: memory.current_bytes,
            peak_bytes: memory.peak_bytes,
            max_bytes: memory.max_bytes,
            usage_percent: memory.usage_ratio * 100.0,
        },
    })
}

// ============================================================================
// Error Handling
// ============================================================================

#[derive(Debug)]
pub enum ApiError {
    BadRequest(String),
    NotFound(String),
    Query(String),
    Internal(String),
}

impl IntoResponse for ApiError {
    fn into_response(self) -> axum::response::Response {
        let (status, message) = match self {
            ApiError::BadRequest(msg) => (StatusCode::BAD_REQUEST, msg),
            ApiError::NotFound(msg) => (StatusCode::NOT_FOUND, msg),
            ApiError::Query(msg) => (StatusCode::BAD_REQUEST, msg),
            ApiError::Internal(msg) => (StatusCode::INTERNAL_SERVER_ERROR, msg),
        };

        let body = serde_json::json!({
            "error": message
        });

        (status, Json(body)).into_response()
    }
}
