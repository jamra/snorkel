#!/bin/bash
# Stop all Snorkel nodes

echo "Stopping all Snorkel nodes..."

# Kill any snorkel processes
pkill -f "target/debug/snorkel" 2>/dev/null
pkill -f "target/release/snorkel" 2>/dev/null
pkill -f "cargo run" 2>/dev/null

# Kill anything on the cluster ports
for port in 9000 9001 9002; do
    pid=$(lsof -ti :$port 2>/dev/null)
    if [ -n "$pid" ]; then
        echo "Killing process on port $port (PID: $pid)"
        kill -9 $pid 2>/dev/null
    fi
done

echo "Done."
