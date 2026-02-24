#!/bin/bash
# Node 2 - Worker node

export SNORKEL_PORT=9001
export SNORKEL_NODE_ID=node-2
export SNORKEL_ADVERTISE_ADDR=127.0.0.1:9001
export SNORKEL_PEERS=""
export SNORKEL_IS_COORDINATOR=false
export SNORKEL_MAX_MEMORY_MB=512
export RUST_LOG=snorkel=info

echo "Starting Snorkel Node 2 on port 9001..."
cargo run
