use serde::{Deserialize, Serialize};

/// Cluster configuration
///
/// In symmetric mode (default), any node can coordinate queries.
/// Whichever node receives a query becomes the coordinator for that request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterConfig {
    /// This node's ID
    pub node_id: String,
    /// This node's advertised address
    pub advertise_addr: String,
    /// List of peer nodes (excluding self)
    pub peers: Vec<PeerNode>,
    /// Deprecated: ignored in symmetric mode. Kept for backwards compatibility.
    #[serde(default = "default_true")]
    pub is_coordinator: bool,
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerNode {
    pub id: String,
    pub addr: String,
}

impl ClusterConfig {
    /// Create a single-node (non-distributed) config
    pub fn single_node() -> Self {
        Self {
            node_id: "node-1".to_string(),
            advertise_addr: "127.0.0.1:8080".to_string(),
            peers: vec![],
            is_coordinator: true, // Ignored in symmetric mode
        }
    }

    /// Create a cluster config from environment variables
    ///
    /// ## Environment Variables
    ///
    /// - `SNORKEL_NODE_ID` - Unique identifier for this node (default: "node-1")
    /// - `SNORKEL_ADVERTISE_ADDR` - Address this node advertises to peers (default: "127.0.0.1:8080")
    /// - `SNORKEL_PEERS` - Comma-separated list of peer nodes in format "id:host:port"
    ///
    /// ## Example
    ///
    /// ```bash
    /// # Node 1
    /// SNORKEL_NODE_ID=node-1 \
    /// SNORKEL_ADVERTISE_ADDR=10.0.0.1:8080 \
    /// SNORKEL_PEERS=node-2:10.0.0.2:8080,node-3:10.0.0.3:8080
    ///
    /// # Node 2
    /// SNORKEL_NODE_ID=node-2 \
    /// SNORKEL_ADVERTISE_ADDR=10.0.0.2:8080 \
    /// SNORKEL_PEERS=node-1:10.0.0.1:8080,node-3:10.0.0.3:8080
    /// ```
    ///
    /// ## Symmetric Coordination
    ///
    /// All nodes are equal - any node can coordinate queries. Put a load balancer
    /// in front to distribute requests across nodes.
    pub fn from_env() -> Self {
        let node_id = std::env::var("SNORKEL_NODE_ID").unwrap_or_else(|_| "node-1".to_string());
        let advertise_addr =
            std::env::var("SNORKEL_ADVERTISE_ADDR").unwrap_or_else(|_| "127.0.0.1:8080".to_string());

        // is_coordinator is ignored in symmetric mode but kept for backwards compatibility
        let is_coordinator = std::env::var("SNORKEL_IS_COORDINATOR")
            .map(|v| v == "true" || v == "1")
            .unwrap_or(true);

        let peers = std::env::var("SNORKEL_PEERS")
            .map(|s| {
                s.split(',')
                    .filter_map(|peer| {
                        let parts: Vec<&str> = peer.split(':').collect();
                        if parts.len() >= 2 {
                            Some(PeerNode {
                                id: parts[0].to_string(),
                                addr: parts[1..].join(":"),
                            })
                        } else {
                            None
                        }
                    })
                    .collect()
            })
            .unwrap_or_default();

        Self {
            node_id,
            advertise_addr,
            peers,
            is_coordinator,
        }
    }

    /// Check if this node can coordinate queries (always true in symmetric mode)
    pub fn can_coordinate(&self) -> bool {
        true // All nodes can coordinate in symmetric mode
    }

    /// Check if this is a distributed cluster
    pub fn is_distributed(&self) -> bool {
        !self.peers.is_empty()
    }

    /// Get all node addresses (including self)
    pub fn all_addrs(&self) -> Vec<String> {
        let mut addrs = vec![self.advertise_addr.clone()];
        addrs.extend(self.peers.iter().map(|p| p.addr.clone()));
        addrs
    }

    /// Get peer addresses only (excluding self)
    pub fn peer_addrs(&self) -> Vec<String> {
        self.peers.iter().map(|p| p.addr.clone()).collect()
    }
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self::single_node()
    }
}
