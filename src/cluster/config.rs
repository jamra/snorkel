use serde::{Deserialize, Serialize};

/// Cluster configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterConfig {
    /// This node's ID
    pub node_id: String,
    /// This node's advertised address
    pub advertise_addr: String,
    /// List of peer nodes (excluding self)
    pub peers: Vec<PeerNode>,
    /// Whether this node acts as coordinator
    pub is_coordinator: bool,
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
            is_coordinator: true,
        }
    }

    /// Create a cluster config from environment variables
    /// SNORKEL_NODE_ID=node-1
    /// SNORKEL_ADVERTISE_ADDR=127.0.0.1:8080
    /// SNORKEL_PEERS=node-2:127.0.0.1:8081,node-3:127.0.0.1:8082
    /// SNORKEL_IS_COORDINATOR=true
    pub fn from_env() -> Self {
        let node_id = std::env::var("SNORKEL_NODE_ID").unwrap_or_else(|_| "node-1".to_string());
        let advertise_addr =
            std::env::var("SNORKEL_ADVERTISE_ADDR").unwrap_or_else(|_| "127.0.0.1:8080".to_string());
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
