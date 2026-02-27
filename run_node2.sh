#!/bin/bash
# Node 2 - Symmetric cluster node
# All nodes can coordinate queries

export SNORKEL_PORT=9001
export SNORKEL_NODE_ID=node-2
export SNORKEL_ADVERTISE_ADDR=127.0.0.1:9001
export SNORKEL_PEERS="127.0.0.1:9000,127.0.0.1:9002"
export SNORKEL_MAX_MEMORY_MB=512
export RUST_LOG=snorkel=info

echo "Starting Snorkel Node 2 on port 9001..."
cargo run
