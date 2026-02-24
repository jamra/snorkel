#!/bin/bash
# Seed Snorkel with sample data

HOST="${SNORKEL_HOST:-localhost}"
PORT="${SNORKEL_PORT:-9000}"
BASE_URL="http://${HOST}:${PORT}"

echo "Seeding data to ${BASE_URL}..."

NOW=$(date +%s)000

# Web events - 50 rows
echo "Creating web_events table with 50 rows..."
curl -s -X POST "${BASE_URL}/ingest" -H "Content-Type: application/json" -d '{
  "table": "web_events",
  "rows": [
    {"timestamp": '$NOW', "event": "click", "page": "/home", "user_id": 101, "latency_ms": 45, "country": "US"},
    {"timestamp": '$((NOW-1000))', "event": "view", "page": "/products", "user_id": 102, "latency_ms": 120, "country": "UK"},
    {"timestamp": '$((NOW-2000))', "event": "click", "page": "/checkout", "user_id": 103, "latency_ms": 89, "country": "US"},
    {"timestamp": '$((NOW-3000))', "event": "purchase", "page": "/checkout", "user_id": 104, "latency_ms": 200, "country": "CA"},
    {"timestamp": '$((NOW-4000))', "event": "click", "page": "/home", "user_id": 105, "latency_ms": 55, "country": "US"},
    {"timestamp": '$((NOW-5000))', "event": "view", "page": "/about", "user_id": 106, "latency_ms": 80, "country": "DE"},
    {"timestamp": '$((NOW-6000))', "event": "click", "page": "/products", "user_id": 107, "latency_ms": 65, "country": "FR"},
    {"timestamp": '$((NOW-7000))', "event": "view", "page": "/home", "user_id": 108, "latency_ms": 95, "country": "US"},
    {"timestamp": '$((NOW-8000))', "event": "purchase", "page": "/checkout", "user_id": 109, "latency_ms": 180, "country": "UK"},
    {"timestamp": '$((NOW-9000))', "event": "click", "page": "/products", "user_id": 110, "latency_ms": 70, "country": "US"},
    {"timestamp": '$((NOW-10000))', "event": "view", "page": "/home", "user_id": 111, "latency_ms": 110, "country": "JP"},
    {"timestamp": '$((NOW-11000))', "event": "click", "page": "/about", "user_id": 112, "latency_ms": 42, "country": "US"},
    {"timestamp": '$((NOW-12000))', "event": "view", "page": "/products", "user_id": 113, "latency_ms": 88, "country": "AU"},
    {"timestamp": '$((NOW-13000))', "event": "click", "page": "/home", "user_id": 114, "latency_ms": 52, "country": "US"},
    {"timestamp": '$((NOW-14000))', "event": "purchase", "page": "/checkout", "user_id": 115, "latency_ms": 220, "country": "CA"},
    {"timestamp": '$((NOW-15000))', "event": "view", "page": "/about", "user_id": 116, "latency_ms": 75, "country": "US"},
    {"timestamp": '$((NOW-16000))', "event": "click", "page": "/products", "user_id": 117, "latency_ms": 62, "country": "DE"},
    {"timestamp": '$((NOW-17000))', "event": "view", "page": "/home", "user_id": 118, "latency_ms": 98, "country": "US"},
    {"timestamp": '$((NOW-18000))', "event": "click", "page": "/checkout", "user_id": 119, "latency_ms": 78, "country": "UK"},
    {"timestamp": '$((NOW-19000))', "event": "purchase", "page": "/checkout", "user_id": 120, "latency_ms": 195, "country": "US"},
    {"timestamp": '$((NOW-60000))', "event": "click", "page": "/home", "user_id": 201, "latency_ms": 48, "country": "US"},
    {"timestamp": '$((NOW-120000))', "event": "view", "page": "/products", "user_id": 202, "latency_ms": 115, "country": "FR"},
    {"timestamp": '$((NOW-180000))', "event": "click", "page": "/about", "user_id": 203, "latency_ms": 58, "country": "US"},
    {"timestamp": '$((NOW-240000))', "event": "view", "page": "/home", "user_id": 204, "latency_ms": 92, "country": "JP"},
    {"timestamp": '$((NOW-300000))', "event": "purchase", "page": "/checkout", "user_id": 205, "latency_ms": 210, "country": "US"},
    {"timestamp": '$((NOW-360000))', "event": "click", "page": "/products", "user_id": 206, "latency_ms": 67, "country": "UK"},
    {"timestamp": '$((NOW-420000))', "event": "view", "page": "/about", "user_id": 207, "latency_ms": 85, "country": "US"},
    {"timestamp": '$((NOW-480000))', "event": "click", "page": "/home", "user_id": 208, "latency_ms": 51, "country": "CA"},
    {"timestamp": '$((NOW-540000))', "event": "view", "page": "/products", "user_id": 209, "latency_ms": 105, "country": "US"},
    {"timestamp": '$((NOW-600000))', "event": "purchase", "page": "/checkout", "user_id": 210, "latency_ms": 188, "country": "DE"},
    {"timestamp": '$((NOW-3600000))', "event": "click", "page": "/home", "user_id": 301, "latency_ms": 44, "country": "US"},
    {"timestamp": '$((NOW-3660000))', "event": "view", "page": "/products", "user_id": 302, "latency_ms": 125, "country": "AU"},
    {"timestamp": '$((NOW-3720000))', "event": "click", "page": "/checkout", "user_id": 303, "latency_ms": 82, "country": "US"},
    {"timestamp": '$((NOW-3780000))', "event": "purchase", "page": "/checkout", "user_id": 304, "latency_ms": 205, "country": "UK"},
    {"timestamp": '$((NOW-3840000))', "event": "click", "page": "/home", "user_id": 305, "latency_ms": 59, "country": "US"},
    {"timestamp": '$((NOW-3900000))', "event": "view", "page": "/about", "user_id": 306, "latency_ms": 78, "country": "FR"},
    {"timestamp": '$((NOW-3960000))', "event": "click", "page": "/products", "user_id": 307, "latency_ms": 63, "country": "US"},
    {"timestamp": '$((NOW-4020000))', "event": "view", "page": "/home", "user_id": 308, "latency_ms": 99, "country": "JP"},
    {"timestamp": '$((NOW-4080000))', "event": "purchase", "page": "/checkout", "user_id": 309, "latency_ms": 175, "country": "US"},
    {"timestamp": '$((NOW-4140000))', "event": "click", "page": "/products", "user_id": 310, "latency_ms": 72, "country": "CA"},
    {"timestamp": '$((NOW-7200000))', "event": "view", "page": "/home", "user_id": 401, "latency_ms": 112, "country": "US"},
    {"timestamp": '$((NOW-7260000))', "event": "click", "page": "/about", "user_id": 402, "latency_ms": 46, "country": "DE"},
    {"timestamp": '$((NOW-7320000))', "event": "view", "page": "/products", "user_id": 403, "latency_ms": 91, "country": "US"},
    {"timestamp": '$((NOW-7380000))', "event": "click", "page": "/home", "user_id": 404, "latency_ms": 54, "country": "UK"},
    {"timestamp": '$((NOW-7440000))', "event": "purchase", "page": "/checkout", "user_id": 405, "latency_ms": 215, "country": "US"},
    {"timestamp": '$((NOW-7500000))', "event": "view", "page": "/about", "user_id": 406, "latency_ms": 79, "country": "AU"},
    {"timestamp": '$((NOW-7560000))', "event": "click", "page": "/products", "user_id": 407, "latency_ms": 66, "country": "US"},
    {"timestamp": '$((NOW-7620000))', "event": "view", "page": "/home", "user_id": 408, "latency_ms": 101, "country": "FR"},
    {"timestamp": '$((NOW-7680000))', "event": "click", "page": "/checkout", "user_id": 409, "latency_ms": 76, "country": "US"},
    {"timestamp": '$((NOW-7740000))', "event": "purchase", "page": "/checkout", "user_id": 410, "latency_ms": 192, "country": "JP"}
  ]
}'
echo ""

# Metrics - 30 rows
echo "Creating metrics table with 30 rows..."
curl -s -X POST "${BASE_URL}/ingest" -H "Content-Type: application/json" -d '{
  "table": "metrics",
  "rows": [
    {"timestamp": '$NOW', "host": "web-1", "cpu_percent": 45.2, "memory_mb": 2048, "requests_per_sec": 150},
    {"timestamp": '$((NOW-60000))', "host": "web-1", "cpu_percent": 52.1, "memory_mb": 2100, "requests_per_sec": 175},
    {"timestamp": '$((NOW-120000))', "host": "web-1", "cpu_percent": 48.7, "memory_mb": 2080, "requests_per_sec": 160},
    {"timestamp": '$((NOW-180000))', "host": "web-1", "cpu_percent": 61.3, "memory_mb": 2200, "requests_per_sec": 210},
    {"timestamp": '$((NOW-240000))', "host": "web-1", "cpu_percent": 55.9, "memory_mb": 2150, "requests_per_sec": 185},
    {"timestamp": '$NOW', "host": "web-2", "cpu_percent": 38.5, "memory_mb": 1800, "requests_per_sec": 120},
    {"timestamp": '$((NOW-60000))', "host": "web-2", "cpu_percent": 42.3, "memory_mb": 1850, "requests_per_sec": 135},
    {"timestamp": '$((NOW-120000))', "host": "web-2", "cpu_percent": 39.8, "memory_mb": 1820, "requests_per_sec": 128},
    {"timestamp": '$((NOW-180000))', "host": "web-2", "cpu_percent": 51.2, "memory_mb": 1950, "requests_per_sec": 165},
    {"timestamp": '$((NOW-240000))', "host": "web-2", "cpu_percent": 44.6, "memory_mb": 1880, "requests_per_sec": 142},
    {"timestamp": '$NOW', "host": "db-1", "cpu_percent": 25.1, "memory_mb": 8192, "requests_per_sec": 50},
    {"timestamp": '$((NOW-60000))', "host": "db-1", "cpu_percent": 28.4, "memory_mb": 8250, "requests_per_sec": 58},
    {"timestamp": '$((NOW-120000))', "host": "db-1", "cpu_percent": 22.9, "memory_mb": 8100, "requests_per_sec": 45},
    {"timestamp": '$((NOW-180000))', "host": "db-1", "cpu_percent": 35.7, "memory_mb": 8400, "requests_per_sec": 72},
    {"timestamp": '$((NOW-240000))', "host": "db-1", "cpu_percent": 30.2, "memory_mb": 8300, "requests_per_sec": 62},
    {"timestamp": '$((NOW-3600000))', "host": "web-1", "cpu_percent": 42.8, "memory_mb": 2000, "requests_per_sec": 145},
    {"timestamp": '$((NOW-3660000))', "host": "web-1", "cpu_percent": 49.5, "memory_mb": 2050, "requests_per_sec": 168},
    {"timestamp": '$((NOW-3720000))', "host": "web-1", "cpu_percent": 55.2, "memory_mb": 2120, "requests_per_sec": 190},
    {"timestamp": '$((NOW-3780000))', "host": "web-1", "cpu_percent": 47.1, "memory_mb": 2030, "requests_per_sec": 155},
    {"timestamp": '$((NOW-3840000))', "host": "web-1", "cpu_percent": 58.9, "memory_mb": 2180, "requests_per_sec": 202},
    {"timestamp": '$((NOW-3600000))', "host": "web-2", "cpu_percent": 35.2, "memory_mb": 1750, "requests_per_sec": 112},
    {"timestamp": '$((NOW-3660000))', "host": "web-2", "cpu_percent": 40.8, "memory_mb": 1820, "requests_per_sec": 130},
    {"timestamp": '$((NOW-3720000))', "host": "web-2", "cpu_percent": 45.5, "memory_mb": 1900, "requests_per_sec": 148},
    {"timestamp": '$((NOW-3780000))', "host": "web-2", "cpu_percent": 37.9, "memory_mb": 1780, "requests_per_sec": 118},
    {"timestamp": '$((NOW-3840000))', "host": "web-2", "cpu_percent": 48.2, "memory_mb": 1920, "requests_per_sec": 158},
    {"timestamp": '$((NOW-3600000))', "host": "db-1", "cpu_percent": 22.5, "memory_mb": 8050, "requests_per_sec": 42},
    {"timestamp": '$((NOW-3660000))', "host": "db-1", "cpu_percent": 26.8, "memory_mb": 8150, "requests_per_sec": 52},
    {"timestamp": '$((NOW-3720000))', "host": "db-1", "cpu_percent": 31.2, "memory_mb": 8280, "requests_per_sec": 65},
    {"timestamp": '$((NOW-3780000))', "host": "db-1", "cpu_percent": 24.1, "memory_mb": 8080, "requests_per_sec": 48},
    {"timestamp": '$((NOW-3840000))', "host": "db-1", "cpu_percent": 33.5, "memory_mb": 8350, "requests_per_sec": 70}
  ]
}'
echo ""

# Error logs - 20 rows
echo "Creating error_logs table with 20 rows..."
curl -s -X POST "${BASE_URL}/ingest" -H "Content-Type: application/json" -d '{
  "table": "error_logs",
  "rows": [
    {"timestamp": '$NOW', "level": "error", "service": "api", "message": "Connection timeout", "status_code": 504},
    {"timestamp": '$((NOW-5000))', "level": "warn", "service": "api", "message": "Slow query detected", "status_code": 200},
    {"timestamp": '$((NOW-10000))', "level": "error", "service": "auth", "message": "Invalid token", "status_code": 401},
    {"timestamp": '$((NOW-15000))', "level": "error", "service": "api", "message": "Rate limit exceeded", "status_code": 429},
    {"timestamp": '$((NOW-20000))', "level": "warn", "service": "db", "message": "High memory usage", "status_code": 200},
    {"timestamp": '$((NOW-60000))', "level": "error", "service": "api", "message": "Internal server error", "status_code": 500},
    {"timestamp": '$((NOW-120000))', "level": "warn", "service": "cache", "message": "Cache miss rate high", "status_code": 200},
    {"timestamp": '$((NOW-180000))', "level": "error", "service": "auth", "message": "Session expired", "status_code": 401},
    {"timestamp": '$((NOW-240000))', "level": "error", "service": "api", "message": "Bad request", "status_code": 400},
    {"timestamp": '$((NOW-300000))', "level": "warn", "service": "db", "message": "Connection pool exhausted", "status_code": 200},
    {"timestamp": '$((NOW-3600000))', "level": "error", "service": "api", "message": "Service unavailable", "status_code": 503},
    {"timestamp": '$((NOW-3660000))', "level": "warn", "service": "api", "message": "Deprecated endpoint used", "status_code": 200},
    {"timestamp": '$((NOW-3720000))', "level": "error", "service": "auth", "message": "Permission denied", "status_code": 403},
    {"timestamp": '$((NOW-3780000))', "level": "error", "service": "api", "message": "Resource not found", "status_code": 404},
    {"timestamp": '$((NOW-3840000))', "level": "warn", "service": "cache", "message": "Eviction rate high", "status_code": 200},
    {"timestamp": '$((NOW-7200000))', "level": "error", "service": "db", "message": "Deadlock detected", "status_code": 500},
    {"timestamp": '$((NOW-7260000))', "level": "warn", "service": "api", "message": "Response time degraded", "status_code": 200},
    {"timestamp": '$((NOW-7320000))', "level": "error", "service": "auth", "message": "Invalid credentials", "status_code": 401},
    {"timestamp": '$((NOW-7380000))', "level": "error", "service": "api", "message": "Gateway timeout", "status_code": 504},
    {"timestamp": '$((NOW-7440000))', "level": "warn", "service": "db", "message": "Replication lag", "status_code": 200}
  ]
}'
echo ""

echo "Done! Seeded 100 rows across 3 tables."
echo ""
echo "Tables:"
curl -s "${BASE_URL}/tables" | python3 -c "import sys,json; d=json.load(sys.stdin); [print(f\"  - {t['name']}: {t['row_count']} rows\") for t in d['tables']]" 2>/dev/null || curl -s "${BASE_URL}/tables"
