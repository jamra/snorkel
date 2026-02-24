#!/bin/bash
# Start all 3 nodes in the background

echo "Starting Snorkel 3-node cluster..."

# Start worker nodes first
./run_node2.sh &
NODE2_PID=$!
echo "Node 2 started (PID: $NODE2_PID)"

./run_node3.sh &
NODE3_PID=$!
echo "Node 3 started (PID: $NODE3_PID)"

# Wait for workers to be ready
sleep 2

# Start coordinator (in foreground so Ctrl+C stops everything)
echo "Starting coordinator..."
./run_node1.sh &
NODE1_PID=$!
echo "Node 1 (Coordinator) started (PID: $NODE1_PID)"

echo ""
echo "Cluster is running!"
echo "  - Coordinator: http://localhost:9000"
echo "  - Node 2:      http://localhost:9001"
echo "  - Node 3:      http://localhost:9002"
echo ""
echo "Press Ctrl+C to stop all nodes..."

# Wait for Ctrl+C
trap "echo 'Stopping cluster...'; kill $NODE1_PID $NODE2_PID $NODE3_PID 2>/dev/null; exit 0" SIGINT SIGTERM

wait
