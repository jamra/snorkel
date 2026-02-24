#!/bin/bash
# Node 3 - Worker node

export SNORKEL_PORT=9002
export SNORKEL_NODE_ID=node-3
export SNORKEL_ADVERTISE_ADDR=127.0.0.1:9002
export SNORKEL_PEERS=""
export SNORKEL_IS_COORDINATOR=false
export SNORKEL_MAX_MEMORY_MB=512
export RUST_LOG=snorkel=info

echo "Starting Snorkel Node 3 on port 9002..."
cargo run
