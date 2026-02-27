//! Ingest load balancer for routing requests to least-loaded nodes
//!
//! Prevents hot-spots by routing ingests based on memory pressure.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use parking_lot::RwLock;

use super::client::ClusterClient;

/// Statistics for a node
#[derive(Debug, Clone, Default)]
pub struct NodeStats {
    /// Memory usage in bytes
    pub memory_bytes: usize,
    /// Memory limit in bytes
    pub memory_limit: usize,
    /// Number of active ingests
    pub active_ingests: usize,
    /// Last update time
    pub last_updated: Option<Instant>,
}

impl NodeStats {
    /// Calculate memory pressure as a percentage (0.0 - 1.0)
    pub fn memory_pressure(&self) -> f64 {
        if self.memory_limit == 0 {
            return 0.0;
        }
        self.memory_bytes as f64 / self.memory_limit as f64
    }

    /// Calculate load score (lower is better)
    pub fn load_score(&self) -> f64 {
        self.memory_pressure() + (self.active_ingests as f64 * 0.1)
    }
}

/// Load balancer for distributing ingest requests
pub struct IngestLoadBalancer {
    /// Node statistics
    node_stats: RwLock<HashMap<String, NodeStats>>,
    /// Round-robin counter for fallback
    next_node: AtomicUsize,
    /// Node addresses
    nodes: Vec<String>,
    /// HTTP client for stats fetching
    client: ClusterClient,
    /// Stats refresh interval
    refresh_interval: Duration,
}

impl IngestLoadBalancer {
    /// Create a new load balancer
    pub fn new(nodes: Vec<String>) -> Self {
        Self {
            node_stats: RwLock::new(HashMap::new()),
            next_node: AtomicUsize::new(0),
            nodes,
            client: ClusterClient::new(),
            refresh_interval: Duration::from_secs(5),
        }
    }

    /// Create with custom refresh interval
    pub fn with_refresh_interval(nodes: Vec<String>, interval: Duration) -> Self {
        Self {
            node_stats: RwLock::new(HashMap::new()),
            next_node: AtomicUsize::new(0),
            nodes,
            client: ClusterClient::new(),
            refresh_interval: interval,
        }
    }

    /// Select the best node for an ingest request
    pub fn select_node(&self) -> Option<String> {
        if self.nodes.is_empty() {
            return None;
        }

        let stats = self.node_stats.read();

        // Find node with lowest load score
        let best_node = self
            .nodes
            .iter()
            .map(|addr| {
                let score = stats
                    .get(addr)
                    .map(|s| s.load_score())
                    .unwrap_or(0.5); // Default to moderate load
                (addr.clone(), score)
            })
            .min_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal))
            .map(|(addr, _)| addr);

        // Fallback to round-robin if no stats available
        best_node.or_else(|| {
            let idx = self.next_node.fetch_add(1, Ordering::Relaxed) % self.nodes.len();
            self.nodes.get(idx).cloned()
        })
    }

    /// Select node based on memory pressure threshold
    pub fn select_node_below_pressure(&self, threshold: f64) -> Option<String> {
        if self.nodes.is_empty() {
            return None;
        }

        let stats = self.node_stats.read();

        // Find nodes below threshold
        let eligible: Vec<_> = self
            .nodes
            .iter()
            .filter(|addr| {
                stats
                    .get(*addr)
                    .map(|s| s.memory_pressure() < threshold)
                    .unwrap_or(true)
            })
            .collect();

        if eligible.is_empty() {
            // All nodes above threshold, return least loaded
            return self.select_node();
        }

        // Return least loaded eligible node
        eligible
            .into_iter()
            .map(|addr| {
                let score = stats.get(addr).map(|s| s.load_score()).unwrap_or(0.5);
                (addr.clone(), score)
            })
            .min_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal))
            .map(|(addr, _)| addr)
    }

    /// Update stats for a node
    pub fn update_stats(&self, addr: &str, stats: NodeStats) {
        let mut node_stats = self.node_stats.write();
        node_stats.insert(addr.to_string(), stats);
    }

    /// Update stats from node response
    pub fn update_from_response(&self, addr: &str, memory_bytes: usize, memory_limit: usize) {
        self.update_stats(
            addr,
            NodeStats {
                memory_bytes,
                memory_limit,
                active_ingests: 0,
                last_updated: Some(Instant::now()),
            },
        );
    }

    /// Mark that an ingest is starting on a node
    pub fn ingest_start(&self, addr: &str) {
        let mut stats = self.node_stats.write();
        if let Some(node_stats) = stats.get_mut(addr) {
            node_stats.active_ingests += 1;
        }
    }

    /// Mark that an ingest is complete on a node
    pub fn ingest_complete(&self, addr: &str) {
        let mut stats = self.node_stats.write();
        if let Some(node_stats) = stats.get_mut(addr) {
            node_stats.active_ingests = node_stats.active_ingests.saturating_sub(1);
        }
    }

    /// Get all node stats
    pub fn all_stats(&self) -> HashMap<String, NodeStats> {
        self.node_stats.read().clone()
    }

    /// Check if stats need refresh
    pub fn needs_refresh(&self, addr: &str) -> bool {
        let stats = self.node_stats.read();
        stats
            .get(addr)
            .and_then(|s| s.last_updated)
            .map(|t| t.elapsed() > self.refresh_interval)
            .unwrap_or(true)
    }

    /// Refresh stats for all nodes
    pub async fn refresh_all_stats(&self) {
        for addr in &self.nodes {
            if self.needs_refresh(addr) {
                if let Ok(stats) = self.fetch_node_stats(addr).await {
                    self.update_stats(addr, stats);
                }
            }
        }
    }

    /// Fetch stats from a node
    async fn fetch_node_stats(&self, addr: &str) -> Result<NodeStats, ()> {
        // In a real implementation, this would call the /stats endpoint
        // For now, return default stats
        Ok(NodeStats {
            memory_bytes: 0,
            memory_limit: 1024 * 1024 * 1024, // 1GB default
            active_ingests: 0,
            last_updated: Some(Instant::now()),
        })
    }
}

impl Default for IngestLoadBalancer {
    fn default() -> Self {
        Self::new(vec![])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_select_node_round_robin() {
        let lb = IngestLoadBalancer::new(vec![
            "node1:8080".to_string(),
            "node2:8080".to_string(),
            "node3:8080".to_string(),
        ]);

        // Without stats, should round-robin
        let nodes: Vec<_> = (0..6).map(|_| lb.select_node().unwrap()).collect();

        // Should cycle through nodes
        assert_eq!(nodes[0], nodes[3]);
        assert_eq!(nodes[1], nodes[4]);
        assert_eq!(nodes[2], nodes[5]);
    }

    #[test]
    fn test_select_least_loaded() {
        let lb = IngestLoadBalancer::new(vec![
            "node1:8080".to_string(),
            "node2:8080".to_string(),
            "node3:8080".to_string(),
        ]);

        // Set up different load levels
        lb.update_stats(
            "node1:8080",
            NodeStats {
                memory_bytes: 800_000_000,
                memory_limit: 1_000_000_000,
                active_ingests: 0,
                last_updated: Some(Instant::now()),
            },
        );
        lb.update_stats(
            "node2:8080",
            NodeStats {
                memory_bytes: 200_000_000,
                memory_limit: 1_000_000_000,
                active_ingests: 0,
                last_updated: Some(Instant::now()),
            },
        );
        lb.update_stats(
            "node3:8080",
            NodeStats {
                memory_bytes: 500_000_000,
                memory_limit: 1_000_000_000,
                active_ingests: 0,
                last_updated: Some(Instant::now()),
            },
        );

        // Should select node2 (lowest load)
        let selected = lb.select_node().unwrap();
        assert_eq!(selected, "node2:8080");
    }

    #[test]
    fn test_select_below_pressure() {
        let lb = IngestLoadBalancer::new(vec![
            "node1:8080".to_string(),
            "node2:8080".to_string(),
        ]);

        lb.update_stats(
            "node1:8080",
            NodeStats {
                memory_bytes: 900_000_000,
                memory_limit: 1_000_000_000, // 90% full
                active_ingests: 0,
                last_updated: Some(Instant::now()),
            },
        );
        lb.update_stats(
            "node2:8080",
            NodeStats {
                memory_bytes: 500_000_000,
                memory_limit: 1_000_000_000, // 50% full
                active_ingests: 0,
                last_updated: Some(Instant::now()),
            },
        );

        // With 80% threshold, should only select node2
        let selected = lb.select_node_below_pressure(0.8).unwrap();
        assert_eq!(selected, "node2:8080");
    }

    #[test]
    fn test_memory_pressure() {
        let stats = NodeStats {
            memory_bytes: 500_000_000,
            memory_limit: 1_000_000_000,
            active_ingests: 0,
            last_updated: None,
        };

        assert!((stats.memory_pressure() - 0.5).abs() < 0.001);
    }
}
