use axum::{
    http::{header, StatusCode},
    response::{Html, IntoResponse, Response},
    routing::{delete, get, post},
    Router,
};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tower_http::cors::{Any, CorsLayer};
use tower_http::trace::TraceLayer;

use super::handlers::{
    cache_stats, create_alert, create_table, delete_alert, drop_table, get_alert, health_check,
    ingest, invalidate_cache, list_alerts, list_tables, query, set_alert_enabled, stats,
    table_schema, update_alert, AppState,
};
use crate::alerts::AlertChecker;
use crate::cluster::{ClusterConfig, Coordinator};
use crate::compaction::{SubsampleWorker, TtlWorker};
#[cfg(feature = "kafka")]
use crate::ingest::{KafkaConfig, KafkaConsumer};
use crate::otel::handle_otlp_traces;
use crate::query::QueryCache;
use crate::storage::persistence::{PersistenceConfig, SnapshotManager};
use crate::storage::StorageEngine;

// Embed UI files at compile time
const INDEX_HTML: &str = include_str!("../ui/index.html");
const APP_JS: &str = include_str!("../ui/app.js");
const QUERY_BUILDER_JS: &str = include_str!("../ui/query-builder.js");
const CHART_JS: &str = include_str!("../ui/chart.js");
const QUERY_FORMS_JS: &str = include_str!("../ui/query-forms.js");
const TRACES_JS: &str = include_str!("../ui/traces.js");

/// Server configuration
#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub host: String,
    pub port: u16,
    pub max_memory_bytes: usize,
    pub ttl_check_interval_secs: u64,
    pub subsample_check_interval_secs: u64,
    pub cluster_config: ClusterConfig,
    /// Data directory for persistence (None = no persistence)
    pub data_dir: Option<std::path::PathBuf>,
    /// Snapshot interval in seconds (default: 300 = 5 minutes)
    pub snapshot_interval_secs: u64,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            host: "0.0.0.0".to_string(),
            port: 8080,
            max_memory_bytes: 1024 * 1024 * 1024, // 1GB
            ttl_check_interval_secs: 60,
            subsample_check_interval_secs: 300,
            cluster_config: ClusterConfig::default(),
            data_dir: None,
            snapshot_interval_secs: 300,
        }
    }
}

// UI file handlers
async fn serve_index() -> Html<&'static str> {
    Html(INDEX_HTML)
}

async fn serve_app_js() -> Response {
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/javascript")],
        APP_JS,
    )
        .into_response()
}

async fn serve_query_builder_js() -> Response {
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/javascript")],
        QUERY_BUILDER_JS,
    )
        .into_response()
}

async fn serve_chart_js() -> Response {
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/javascript")],
        CHART_JS,
    )
        .into_response()
}

async fn serve_query_forms_js() -> Response {
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/javascript")],
        QUERY_FORMS_JS,
    )
        .into_response()
}

async fn serve_traces_js() -> Response {
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/javascript")],
        TRACES_JS,
    )
        .into_response()
}

/// Build the application router
pub fn build_router(state: Arc<AppState>) -> Router {
    Router::new()
        // UI routes
        .route("/", get(serve_index))
        .route("/ui/app.js", get(serve_app_js))
        .route("/ui/query-builder.js", get(serve_query_builder_js))
        .route("/ui/chart.js", get(serve_chart_js))
        .route("/ui/query-forms.js", get(serve_query_forms_js))
        .route("/ui/traces.js", get(serve_traces_js))
        // Health check
        .route("/health", get(health_check))
        // Data operations
        .route("/ingest", post(ingest))
        .route("/query", post(query))
        // Table management
        .route("/tables", get(list_tables))
        .route("/tables", post(create_table))
        .route("/tables/:name", delete(drop_table))
        .route("/tables/:name/schema", get(table_schema))
        // Stats
        .route("/stats", get(stats))
        // Alerts
        .route("/alerts", get(list_alerts))
        .route("/alerts", post(create_alert))
        .route("/alerts/:id", get(get_alert))
        .route("/alerts/:id", axum::routing::put(update_alert))
        .route("/alerts/:id", delete(delete_alert))
        .route("/alerts/:id/enable", post(set_alert_enabled))
        // Cache
        .route("/cache/stats", get(cache_stats))
        .route("/cache/invalidate", post(invalidate_cache))
        // OpenTelemetry
        .route("/v1/traces", post(handle_otlp_traces))
        // Middleware
        .layer(TraceLayer::new_for_http())
        .layer(
            CorsLayer::new()
                .allow_origin(Any)
                .allow_methods(Any)
                .allow_headers(Any),
        )
        .with_state(state)
}

/// Run the HTTP server
pub async fn run_server(config: ServerConfig) -> Result<(), Box<dyn std::error::Error>> {
    // Initialize storage engine
    let engine = Arc::new(StorageEngine::with_memory_limit(config.max_memory_bytes));

    // Initialize persistence and restore from snapshot if configured
    let snapshot_manager = if let Some(ref data_dir) = config.data_dir {
        let persistence_config = PersistenceConfig::new(data_dir)
            .with_snapshot_interval(config.snapshot_interval_secs);

        match SnapshotManager::new(persistence_config) {
            Ok(manager) => {
                // Try to restore from latest snapshot
                match manager.restore_latest(&engine) {
                    Ok(Some(metadata)) => {
                        tracing::info!(
                            "Restored from snapshot: {} ({} tables, {} bytes)",
                            metadata.id,
                            metadata.tables.len(),
                            metadata.size_bytes
                        );
                    }
                    Ok(None) => {
                        tracing::info!("No snapshot found, starting fresh");
                    }
                    Err(e) => {
                        tracing::warn!("Failed to restore snapshot: {}, starting fresh", e);
                    }
                }
                Some(Arc::new(manager))
            }
            Err(e) => {
                tracing::error!("Failed to initialize persistence: {}", e);
                None
            }
        }
    } else {
        tracing::info!("Persistence disabled (set SNORKEL_DATA_DIR to enable)");
        None
    };

    // Initialize coordinator if clustering is enabled
    let coordinator = if config.cluster_config.is_distributed() {
        tracing::info!(
            "Cluster mode enabled: node_id={}, peers={}",
            config.cluster_config.node_id,
            config.cluster_config.peers.len()
        );
        Some(Arc::new(Coordinator::new(
            config.cluster_config.clone(),
            Arc::clone(&engine),
        )))
    } else {
        tracing::info!("Running in single-node mode");
        None
    };

    // Initialize query cache
    let query_cache = Arc::new(QueryCache::new());

    // Initialize alert checker
    let alert_checker = Arc::new(AlertChecker::new(Arc::clone(&engine)));

    // Initialize app state
    let state = Arc::new(AppState {
        engine: Arc::clone(&engine),
        coordinator,
        cluster_config: config.cluster_config.clone(),
        query_cache,
        alert_checker,
    });

    // Start Kafka consumer if configured
    #[cfg(feature = "kafka")]
    let _kafka_handle = {
        if let Some(kafka_config) = KafkaConfig::from_env() {
            tracing::info!(
                "Kafka ingest enabled: brokers={}, topics={:?}, group={}",
                kafka_config.brokers,
                kafka_config.topics,
                kafka_config.group_id
            );
            match KafkaConsumer::new(kafka_config, Arc::clone(&engine)) {
                Ok(consumer) => {
                    if let Err(e) = consumer.subscribe() {
                        tracing::error!("Failed to subscribe to Kafka topics: {}", e);
                        None
                    } else {
                        Some(consumer.start())
                    }
                }
                Err(e) => {
                    tracing::error!("Failed to create Kafka consumer: {}", e);
                    None
                }
            }
        } else {
            tracing::debug!("Kafka ingest not configured (set KAFKA_TOPICS to enable)");
            None
        }
    };

    // Start background workers
    let ttl_worker = Arc::new(TtlWorker::new(
        Arc::clone(&engine),
        std::time::Duration::from_secs(config.ttl_check_interval_secs),
    ));
    let ttl_handle = Arc::clone(&ttl_worker).start();

    let subsample_worker = Arc::new(SubsampleWorker::new(
        Arc::clone(&engine),
        std::time::Duration::from_secs(config.subsample_check_interval_secs),
    ));
    let subsample_handle = Arc::clone(&subsample_worker).start();

    // Start snapshot worker if persistence is enabled
    let _snapshot_handle = if let Some(ref manager) = snapshot_manager {
        let engine_clone = Arc::clone(&engine);
        let manager_clone = Arc::clone(manager);
        let interval = std::time::Duration::from_secs(config.snapshot_interval_secs);

        Some(tokio::spawn(async move {
            let mut interval_timer = tokio::time::interval(interval);
            loop {
                interval_timer.tick().await;
                match manager_clone.create_snapshot(&engine_clone) {
                    Ok(metadata) => {
                        tracing::info!(
                            "Created snapshot: {} ({} tables, {} bytes)",
                            metadata.id,
                            metadata.tables.len(),
                            metadata.size_bytes
                        );
                    }
                    Err(e) => {
                        tracing::error!("Failed to create snapshot: {}", e);
                    }
                }
            }
        }))
    } else {
        None
    };

    // Build router
    let app = build_router(state);

    // Start server
    let addr: SocketAddr = format!("{}:{}", config.host, config.port).parse()?;
    tracing::info!("Starting Snorkel server on {}", addr);

    let listener = TcpListener::bind(addr).await?;
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal(ttl_worker, subsample_worker))
        .await?;

    // Wait for workers to stop
    ttl_handle.abort();
    subsample_handle.abort();

    tracing::info!("Snorkel server stopped");
    Ok(())
}

async fn shutdown_signal(
    ttl_worker: Arc<TtlWorker>,
    subsample_worker: Arc<SubsampleWorker>,
) {
    tokio::signal::ctrl_c()
        .await
        .expect("Failed to install CTRL+C signal handler");

    tracing::info!("Shutdown signal received, stopping workers...");
    ttl_worker.stop();
    subsample_worker.stop();
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::util::ServiceExt;

    fn create_test_app() -> Router {
        let engine = Arc::new(StorageEngine::new());
        let state = Arc::new(AppState {
            engine: Arc::clone(&engine),
            coordinator: None,
            cluster_config: ClusterConfig::default(),
            query_cache: Arc::new(QueryCache::new()),
            alert_checker: Arc::new(AlertChecker::new(engine)),
        });
        build_router(state)
    }

    #[tokio::test]
    async fn test_health_check() {
        let app = create_test_app();

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_ingest_and_query() {
        let engine = Arc::new(StorageEngine::new());
        let state = Arc::new(AppState {
            engine: Arc::clone(&engine),
            coordinator: None,
            cluster_config: ClusterConfig::default(),
            query_cache: Arc::new(QueryCache::new()),
            alert_checker: Arc::new(AlertChecker::new(Arc::clone(&engine))),
        });
        let app = build_router(state);

        // Ingest some data
        let ingest_body = serde_json::json!({
            "table": "events",
            "rows": [
                {"timestamp": 1000, "event": "click", "value": 42},
                {"timestamp": 2000, "event": "view", "value": 10}
            ]
        });

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/ingest")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_string(&ingest_body).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        // Query the data
        let query_body = serde_json::json!({
            "sql": "SELECT COUNT(*) FROM events"
        });

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/query")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_string(&query_body).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_list_tables() {
        let engine = Arc::new(StorageEngine::new());

        // Create a table
        engine
            .create_table(crate::data::TableConfig::new("test_table"))
            .unwrap();

        let state = Arc::new(AppState {
            engine: Arc::clone(&engine),
            coordinator: None,
            cluster_config: ClusterConfig::default(),
            query_cache: Arc::new(QueryCache::new()),
            alert_checker: Arc::new(AlertChecker::new(engine)),
        });
        let app = build_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/tables")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_table_not_found() {
        let app = create_test_app();

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/tables/nonexistent/schema")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }
}
