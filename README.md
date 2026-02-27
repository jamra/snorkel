# Snorkel

A fast, in-memory time-series analytics database written in Rust. Inspired by Facebook's Scuba, Snorkel is designed for real-time analytics on event data with SQL-like queries, automatic data expiration, and horizontal scaling.

<img width="1512" height="830" alt="Screenshot 2026-02-25 at 10 22 17 PM" src="https://github.com/user-attachments/assets/da9e62bb-6387-4ee2-8916-7fceb599a29e" />

## Features

- **Columnar Storage** - Cache-efficient memory layout optimized for analytics queries
- **String Dictionary Encoding** - Compact storage for repeated string values
- **Time-Based Sharding** - Data partitioned by time for efficient range queries and TTL expiration
- **SQL-Like Queries** - Familiar syntax with SELECT, WHERE, GROUP BY, ORDER BY, LIMIT
- **Rich Aggregations** - COUNT, SUM, AVG, MIN, MAX, PERCENTILE
- **JSON Auto-Flattening** - Nested JSON objects automatically flattened to dot-notation columns
- **Distributed Queries** - Fan-out queries across multiple nodes with result merging
- **Query Caching** - TTL-based result cache with automatic invalidation
- **Bloom Filters** - Fast shard pruning for equality filters
- **SIMD Aggregations** - Vectorized single-pass statistics computation
- **Web UI** - Visual query builder with interactive charts
- **No Dependencies** - Pure Rust with no external database requirements

## Quick Start

```bash
# Clone and build
git clone https://github.com/jamra/snorkel.git
cd snorkel
cargo build --release

# Run the server (default port 8080)
cargo run --release

# Or with custom settings
SNORKEL_PORT=9000 SNORKEL_MAX_MEMORY_MB=2048 cargo run --release
```

## Usage

### Ingest Data

```bash
curl -X POST http://localhost:9000/ingest \
  -H "Content-Type: application/json" \
  -d '{
    "table": "events",
    "rows": [
      {"timestamp": 1708700000000, "event": "click", "user_id": 123, "latency_ms": 45},
      {"timestamp": 1708700001000, "event": "view", "user_id": 456, "latency_ms": 120}
    ]
  }'
```

### Query Data

```bash
# Count events by type
curl -X POST http://localhost:9000/query \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT event, COUNT(*), AVG(latency_ms) FROM events GROUP BY event"}'

# Filter and aggregate
curl -X POST http://localhost:9000/query \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT event, COUNT(*) FROM events WHERE latency_ms > 100 GROUP BY event"}'
```

### JSON Flattening

Nested JSON is automatically flattened on ingest:

```bash
# Ingest nested data
curl -X POST http://localhost:9000/ingest \
  -H "Content-Type: application/json" \
  -d '{
    "table": "orders",
    "rows": [
      {
        "timestamp": 1708700000000,
        "customer": {"name": "Alice", "tier": "premium"},
        "order": {"total": 99.99, "items": 3}
      }
    ]
  }'

# Query with dot notation
curl -X POST http://localhost:9000/query \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT customer.tier, SUM(order.total) FROM orders GROUP BY customer.tier"}'
```

## Web UI

Open `http://localhost:9000/` in your browser for the visual query builder:

- Select tables and metrics from dropdowns
- Build filters with point-and-click
- Visualize results as bar, line, or area charts
- View sample data alongside aggregations

## API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/` | GET | Web UI |
| `/health` | GET | Health check |
| `/ingest` | POST | Insert rows |
| `/query` | POST | Execute SQL query |
| `/tables` | GET | List all tables |
| `/tables` | POST | Create table with config |
| `/tables/:name/schema` | GET | Get table schema |
| `/tables/:name` | DELETE | Drop table |
| `/stats` | GET | Server statistics |

## Configuration

Environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `SNORKEL_HOST` | `0.0.0.0` | Bind address |
| `SNORKEL_PORT` | `8080` | Server port |
| `SNORKEL_MAX_MEMORY_MB` | `1024` | Maximum memory usage |

### Cluster Mode

Run multiple nodes for horizontal scaling:

```bash
# Node 1 (Coordinator)
SNORKEL_PORT=9000 \
SNORKEL_NODE_ID=node-1 \
SNORKEL_PEERS="127.0.0.1:9001,127.0.0.1:9002" \
SNORKEL_IS_COORDINATOR=true \
cargo run

# Node 2
SNORKEL_PORT=9001 SNORKEL_NODE_ID=node-2 cargo run

# Node 3
SNORKEL_PORT=9002 SNORKEL_NODE_ID=node-3 cargo run
```

Or use the provided scripts:

```bash
./run_cluster.sh    # Start 3-node cluster
./stop_cluster.sh   # Stop all nodes
./seed_data.sh      # Load sample data
```

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                      HTTP API                           │
│              (Axum + Tower middleware)                  │
├─────────────────────────────────────────────────────────┤
│                    Query Engine                         │
│         (SQL Parser → Planner → Executor)               │
├─────────────────────────────────────────────────────────┤
│                   Storage Engine                        │
│    ┌─────────────┐  ┌─────────────┐  ┌─────────────┐   │
│    │   Table 1   │  │   Table 2   │  │   Table N   │   │
│    │  [Shards]   │  │  [Shards]   │  │  [Shards]   │   │
│    └─────────────┘  └─────────────┘  └─────────────┘   │
├─────────────────────────────────────────────────────────┤
│                 Background Workers                      │
│        (TTL Expiration + Subsampling)                   │
└─────────────────────────────────────────────────────────┘
```

## SQL Support

### Supported Syntax

```sql
SELECT column1, column2, AGG(column3)
FROM table_name
WHERE condition1 AND condition2
GROUP BY column1, column2
ORDER BY column1 [ASC|DESC]
LIMIT n
```

### Aggregation Functions

- `COUNT(*)` / `COUNT(column)`
- `SUM(column)`
- `AVG(column)`
- `MIN(column)`
- `MAX(column)`
- `PERCENTILE(column, 0.95)`

### Filter Operators

- Comparison: `=`, `!=`, `>`, `<`, `>=`, `<=`
- Pattern: `LIKE`
- Logical: `AND`, `OR`

## Performance

Snorkel is optimized for high-throughput ingestion and sub-millisecond queries.

### Benchmark Results (11M rows)

| Metric | Result |
|--------|--------|
| **Ingest throughput** | 300-390K rows/sec |
| **Query latency (avg)** | 85-100µs |
| **Cache speedup** | 287x |
| **Bloom filter speedup** | 9.5x |
| **Memory efficiency** | 75 bytes/row |

### Query Latencies

| Query Type | Average | Min |
|------------|---------|-----|
| COUNT(*) | 95µs | 53µs |
| GROUP BY | 85µs | 53µs |
| AVG aggregation | 89µs | 57µs |
| Multiple aggregates | 85µs | 59µs |
| Top N with ORDER BY | 79µs | 56µs |

### Optimizations

- **Bloom filters** - Prune shards that don't contain searched values (9.5x speedup for selective queries)
- **Predicate pushdown** - Build row masks once per shard instead of per-row filter evaluation
- **SIMD-friendly aggregations** - Single-pass statistics computation with auto-vectorization
- **Query result caching** - TTL-based cache with 287x speedup for repeated queries
- **Parallel shard processing** - Rayon-based parallel execution across CPU cores

### Running Benchmarks

```bash
# Start server
cargo run --release &

# Run benchmark (choose scale)
cargo run --release --bin benchmark small   # 10K rows/table
cargo run --release --bin benchmark medium  # 100K rows/table
cargo run --release --bin benchmark large   # 1M rows/table
cargo run --release --bin benchmark xlarge  # 10M rows/table
```

## Development

```bash
# Run tests
cargo test

# Run with logging
RUST_LOG=snorkel=debug cargo run

# Build release
cargo build --release
```

## License

MIT
