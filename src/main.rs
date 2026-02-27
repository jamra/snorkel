//! Snorkel Server
//!
//! Run with: cargo run
//!
//! Environment variables:
//! - SNORKEL_HOST: Bind address (default: 0.0.0.0)
//! - SNORKEL_PORT: Port number (default: 8080)
//! - SNORKEL_MAX_MEMORY_MB: Maximum memory in MB (default: 1024)
//! - RUST_LOG: Log level (default: info)
//!
//! Cluster configuration (symmetric mode - any node can coordinate):
//! - SNORKEL_NODE_ID: Unique identifier for this node (default: node-1)
//! - SNORKEL_ADVERTISE_ADDR: Address this node advertises to peers (default: 127.0.0.1:PORT)
//! - SNORKEL_PEERS: Comma-separated list of peer addresses (e.g., "10.0.0.2:8080,10.0.0.3:8080")
//!
//! In cluster mode, put a load balancer in front to distribute queries across all nodes.

use snorkel::api::{run_server, ServerConfig};
use snorkel::cluster::{ClusterConfig, PeerNode};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "snorkel=info,tower_http=info".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    // Parse basic configuration from environment
    let host = std::env::var("SNORKEL_HOST").unwrap_or_else(|_| "0.0.0.0".to_string());
    let port: u16 = std::env::var("SNORKEL_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(8080);
    let max_memory_bytes = std::env::var("SNORKEL_MAX_MEMORY_MB")
        .ok()
        .and_then(|m| m.parse::<usize>().ok())
        .map(|mb| mb * 1024 * 1024)
        .unwrap_or(1024 * 1024 * 1024);

    // Parse cluster configuration (symmetric mode - any node can coordinate)
    let node_id = std::env::var("SNORKEL_NODE_ID").unwrap_or_else(|_| "node-1".to_string());
    let advertise_addr = std::env::var("SNORKEL_ADVERTISE_ADDR")
        .unwrap_or_else(|_| format!("127.0.0.1:{}", port));

    // Parse peer list: "10.0.0.2:8080,10.0.0.3:8080" format
    let peers: Vec<PeerNode> = std::env::var("SNORKEL_PEERS")
        .ok()
        .map(|peers_str| {
            peers_str
                .split(',')
                .filter(|s| !s.trim().is_empty())
                .enumerate()
                .map(|(i, addr)| PeerNode {
                    id: format!("peer-{}", i + 1),
                    addr: addr.trim().to_string(),
                })
                .collect()
        })
        .unwrap_or_default();

    let cluster_config = ClusterConfig {
        node_id,
        advertise_addr,
        peers,
        is_coordinator: true, // Ignored - all nodes can coordinate in symmetric mode
    };

    let config = ServerConfig {
        host,
        port,
        max_memory_bytes,
        ttl_check_interval_secs: 60,
        subsample_check_interval_secs: 300,
        cluster_config,
    };

    tracing::info!("Snorkel configuration:");
    tracing::info!("  Host: {}:{}", config.host, config.port);
    tracing::info!(
        "  Max memory: {} MB",
        config.max_memory_bytes / (1024 * 1024)
    );
    tracing::info!(
        "  TTL check interval: {} seconds",
        config.ttl_check_interval_secs
    );
    tracing::info!(
        "  Subsample check interval: {} seconds",
        config.subsample_check_interval_secs
    );

    // Cluster info
    if config.cluster_config.is_distributed() {
        tracing::info!("  Cluster mode: SYMMETRIC (any node can coordinate)");
        tracing::info!("  Node ID: {}", config.cluster_config.node_id);
        tracing::info!("  Advertise address: {}", config.cluster_config.advertise_addr);
        tracing::info!("  Peers: {}", config.cluster_config.peers.len());
        for peer in &config.cluster_config.peers {
            tracing::info!("    - {} @ {}", peer.id, peer.addr);
        }
        tracing::info!("  Note: Put a load balancer in front to distribute queries");
    } else {
        tracing::info!("  Cluster mode: DISABLED (single node)");
    }

    println!(
        r#"
   _____                      _        _
  / ____|                    | |      | |
 | (___   _ __    ___   _ __ | | _____| |
  \___ \ | '_ \  / _ \ | '__|| |/ / _ \ |
  ____) || | | || (_) || |   |   <  __/ |
 |_____/ |_| |_| \___/ |_|   |_|\_\___|_|

 In-Memory Time-Series Analytics Database
 Version: {}
"#,
        env!("CARGO_PKG_VERSION")
    );

    run_server(config).await
}
