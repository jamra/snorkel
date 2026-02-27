//! Cluster topology management for hierarchical aggregation
//!
//! Supports multi-tier aggregation to scale to 100+ nodes without coordinator bottleneck.

use super::config::PeerNode;
use std::collections::HashMap;

/// Node tier in the aggregation hierarchy
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum NodeTier {
    /// Leaf nodes that store data
    Leaf,
    /// Intermediate aggregators
    Aggregator,
    /// Top-level coordinator
    Coordinator,
}

/// Topology node configuration
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct TopologyNode {
    /// Node ID
    pub id: String,
    /// Node address
    pub addr: String,
    /// Node tier
    pub tier: NodeTier,
    /// Parent aggregator (None for coordinator)
    pub parent: Option<String>,
    /// Child nodes (for aggregators and coordinator)
    pub children: Vec<String>,
}

/// Cluster topology for hierarchical aggregation
#[derive(Debug, Clone)]
pub struct ClusterTopology {
    /// This node's ID
    pub local_node_id: String,
    /// This node's tier
    pub local_tier: NodeTier,
    /// All nodes in the cluster
    nodes: HashMap<String, TopologyNode>,
    /// Parent aggregator for this node
    parent: Option<TopologyNode>,
    /// Children of this node
    children: Vec<TopologyNode>,
}

impl ClusterTopology {
    /// Create a new topology from configuration
    pub fn new(
        local_node_id: String,
        local_tier: NodeTier,
        parent: Option<TopologyNode>,
        children: Vec<TopologyNode>,
    ) -> Self {
        let mut nodes = HashMap::new();

        // Add self
        nodes.insert(
            local_node_id.clone(),
            TopologyNode {
                id: local_node_id.clone(),
                addr: String::new(), // Will be set later
                tier: local_tier,
                parent: parent.as_ref().map(|p| p.id.clone()),
                children: children.iter().map(|c| c.id.clone()).collect(),
            },
        );

        // Add parent
        if let Some(ref p) = parent {
            nodes.insert(p.id.clone(), p.clone());
        }

        // Add children
        for child in &children {
            nodes.insert(child.id.clone(), child.clone());
        }

        Self {
            local_node_id,
            local_tier,
            nodes,
            parent,
            children,
        }
    }

    /// Create a single-node topology
    pub fn single_node(node_id: &str) -> Self {
        Self::new(node_id.to_string(), NodeTier::Coordinator, None, vec![])
    }

    /// Create a flat topology (all nodes at same level)
    pub fn flat(local_id: &str, peers: &[PeerNode], is_coordinator: bool) -> Self {
        let tier = if is_coordinator {
            NodeTier::Coordinator
        } else {
            NodeTier::Leaf
        };

        let children: Vec<TopologyNode> = if is_coordinator {
            peers
                .iter()
                .map(|p| TopologyNode {
                    id: p.id.clone(),
                    addr: p.addr.clone(),
                    tier: NodeTier::Leaf,
                    parent: Some(local_id.to_string()),
                    children: vec![],
                })
                .collect()
        } else {
            vec![]
        };

        Self::new(local_id.to_string(), tier, None, children)
    }

    /// Get this node's tier
    pub fn tier(&self) -> NodeTier {
        self.local_tier
    }

    /// Get parent node (for leaf/aggregator nodes)
    pub fn parent(&self) -> Option<&TopologyNode> {
        self.parent.as_ref()
    }

    /// Get child nodes (for aggregator/coordinator nodes)
    pub fn children(&self) -> &[TopologyNode] {
        &self.children
    }

    /// Get all child addresses
    pub fn child_addrs(&self) -> Vec<String> {
        self.children.iter().map(|c| c.addr.clone()).collect()
    }

    /// Check if this node is a coordinator
    pub fn is_coordinator(&self) -> bool {
        self.local_tier == NodeTier::Coordinator
    }

    /// Check if this node is a leaf
    pub fn is_leaf(&self) -> bool {
        self.local_tier == NodeTier::Leaf
    }

    /// Get node by ID
    pub fn get_node(&self, id: &str) -> Option<&TopologyNode> {
        self.nodes.get(id)
    }

    /// Get number of nodes in topology
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }
}

impl Default for ClusterTopology {
    fn default() -> Self {
        Self::single_node("node-1")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_single_node_topology() {
        let topology = ClusterTopology::single_node("node-1");

        assert!(topology.is_coordinator());
        assert!(!topology.is_leaf());
        assert!(topology.parent().is_none());
        assert!(topology.children().is_empty());
    }

    #[test]
    fn test_flat_topology_coordinator() {
        let peers = vec![
            PeerNode {
                id: "node-2".to_string(),
                addr: "127.0.0.1:8081".to_string(),
            },
            PeerNode {
                id: "node-3".to_string(),
                addr: "127.0.0.1:8082".to_string(),
            },
        ];

        let topology = ClusterTopology::flat("node-1", &peers, true);

        assert!(topology.is_coordinator());
        assert_eq!(topology.children().len(), 2);
        assert_eq!(topology.child_addrs().len(), 2);
    }

    #[test]
    fn test_flat_topology_leaf() {
        let peers = vec![
            PeerNode {
                id: "node-1".to_string(),
                addr: "127.0.0.1:8080".to_string(),
            },
        ];

        let topology = ClusterTopology::flat("node-2", &peers, false);

        assert!(topology.is_leaf());
        assert!(topology.children().is_empty());
    }
}
