#!/bin/bash
# Start a 3-node Snorkel cluster in symmetric mode
# Any node can handle queries - put a load balancer in front for production

echo "Starting Snorkel 3-node cluster (symmetric mode)..."

# Clean up any existing processes first
./stop_cluster.sh 2>/dev/null

# Small delay to ensure ports are released
sleep 1

# Start all nodes (order doesn't matter in symmetric mode)
./run_node1.sh &
NODE1_PID=$!
echo "Node 1 started (PID: $NODE1_PID)"

./run_node2.sh &
NODE2_PID=$!
echo "Node 2 started (PID: $NODE2_PID)"

./run_node3.sh &
NODE3_PID=$!
echo "Node 3 started (PID: $NODE3_PID)"

# Wait for nodes to be ready
sleep 2

echo ""
echo "Cluster is running (symmetric mode - any node can coordinate)!"
echo "  - Node 1: http://localhost:9000"
echo "  - Node 2: http://localhost:9001"
echo "  - Node 3: http://localhost:9002"
echo ""
echo "You can query any node directly, e.g.:"
echo "  curl -X POST http://localhost:9000/query -d '{\"sql\":\"SELECT COUNT(*) FROM events\"}'"
echo "  curl -X POST http://localhost:9001/query -d '{\"sql\":\"SELECT COUNT(*) FROM events\"}'"
echo ""
echo "For production, put a load balancer in front to distribute queries."
echo ""
echo "Press Ctrl+C to stop all nodes..."

# Wait for Ctrl+C
trap "echo 'Stopping cluster...'; kill $NODE1_PID $NODE2_PID $NODE3_PID 2>/dev/null; ./stop_cluster.sh; exit 0" SIGINT SIGTERM

wait
