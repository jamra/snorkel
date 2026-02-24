#!/bin/bash
# Stop all Snorkel nodes

echo "Stopping all Snorkel nodes..."
pkill -f "target/debug/snorkel" || pkill -f "target/release/snorkel" || true
echo "Done."
