#!/bin/bash
# Stress test Snorkel with large amounts of data

set -e

HOST="${SNORKEL_HOST:-localhost}"
PORT="${SNORKEL_PORT:-9001}"
BASE_URL="http://${HOST}:${PORT}"

# Configuration
ROWS_PER_BATCH="${1:-1000}"
NUM_BATCHES="${2:-10}"
PARALLEL="${3:-4}"

TOTAL_ROWS=$((ROWS_PER_BATCH * NUM_BATCHES))

echo "Stress test configuration:"
echo "  Target: ${BASE_URL}"
echo "  Rows per batch: ${ROWS_PER_BATCH}"
echo "  Number of batches: ${NUM_BATCHES}"
echo "  Parallel requests: ${PARALLEL}"
echo "  Total rows: ${TOTAL_ROWS}"
echo ""

# Arrays for random data generation
EVENTS=("click" "view" "purchase" "scroll" "hover" "submit" "load" "error")
PAGES=("/home" "/products" "/checkout" "/about" "/contact" "/pricing" "/docs" "/api" "/dashboard" "/settings")
COUNTRIES=("US" "UK" "CA" "DE" "FR" "JP" "AU" "BR" "IN" "MX" "ES" "IT" "NL" "SE" "NO")
HOSTS=("web-1" "web-2" "web-3" "db-1" "db-2" "cache-1" "api-1" "api-2")
LEVELS=("error" "warn" "info" "debug")
SERVICES=("api" "auth" "db" "cache" "worker" "scheduler" "gateway" "storage")
MESSAGES=("Connection timeout" "Slow query" "Invalid token" "Rate limit" "High memory" "Server error" "Cache miss" "Session expired" "Bad request" "Pool exhausted")
STATUS_CODES=(200 400 401 403 404 429 500 502 503 504)

# Function to generate a batch of web_events
generate_web_events() {
    local batch_num=$1
    local rows_per_batch=$2
    local now=$(date +%s)000
    local offset=$((batch_num * rows_per_batch * 1000))

    local rows=""
    for ((i=0; i<rows_per_batch; i++)); do
        local ts=$((now - offset - i * 100 - RANDOM % 100))
        local event="${EVENTS[$((RANDOM % ${#EVENTS[@]}))]}"
        local page="${PAGES[$((RANDOM % ${#PAGES[@]}))]}"
        local user_id=$((1000 + batch_num * rows_per_batch + i))
        local latency=$((20 + RANDOM % 480))
        local country="${COUNTRIES[$((RANDOM % ${#COUNTRIES[@]}))]}"

        if [ -n "$rows" ]; then
            rows+=","
        fi
        rows+="{\"timestamp\":${ts},\"event\":\"${event}\",\"page\":\"${page}\",\"user_id\":${user_id},\"latency_ms\":${latency},\"country\":\"${country}\"}"
    done

    echo "{\"table\":\"web_events\",\"rows\":[${rows}]}"
}

# Function to generate a batch of metrics
generate_metrics() {
    local batch_num=$1
    local rows_per_batch=$2
    local now=$(date +%s)000
    local offset=$((batch_num * rows_per_batch * 1000))

    local rows=""
    for ((i=0; i<rows_per_batch; i++)); do
        local ts=$((now - offset - i * 1000 - RANDOM % 1000))
        local host="${HOSTS[$((RANDOM % ${#HOSTS[@]}))]}"
        local cpu=$(echo "scale=1; $((RANDOM % 800)) / 10" | bc)
        local memory=$((512 + RANDOM % 15872))
        local rps=$((10 + RANDOM % 990))

        if [ -n "$rows" ]; then
            rows+=","
        fi
        rows+="{\"timestamp\":${ts},\"host\":\"${host}\",\"cpu_percent\":${cpu},\"memory_mb\":${memory},\"requests_per_sec\":${rps}}"
    done

    echo "{\"table\":\"metrics\",\"rows\":[${rows}]}"
}

# Function to generate a batch of error_logs
generate_error_logs() {
    local batch_num=$1
    local rows_per_batch=$2
    local now=$(date +%s)000
    local offset=$((batch_num * rows_per_batch * 1000))

    local rows=""
    for ((i=0; i<rows_per_batch; i++)); do
        local ts=$((now - offset - i * 500 - RANDOM % 500))
        local level="${LEVELS[$((RANDOM % ${#LEVELS[@]}))]}"
        local service="${SERVICES[$((RANDOM % ${#SERVICES[@]}))]}"
        local message="${MESSAGES[$((RANDOM % ${#MESSAGES[@]}))]}"
        local status="${STATUS_CODES[$((RANDOM % ${#STATUS_CODES[@]}))]}"

        if [ -n "$rows" ]; then
            rows+=","
        fi
        rows+="{\"timestamp\":${ts},\"level\":\"${level}\",\"service\":\"${service}\",\"message\":\"${message}\",\"status_code\":${status}}"
    done

    echo "{\"table\":\"error_logs\",\"rows\":[${rows}]}"
}

# Function to send data
send_batch() {
    local data="$1"
    curl -s -X POST "${BASE_URL}/ingest" -H "Content-Type: application/json" -d "$data" > /dev/null
}

# Main loop
echo "Starting data generation..."
start_time=$(date +%s)

for ((batch=0; batch<NUM_BATCHES; batch++)); do
    echo -ne "\rBatch $((batch+1))/${NUM_BATCHES}..."

    # Generate and send batches for each table (can be parallelized)
    web_data=$(generate_web_events $batch $ROWS_PER_BATCH)
    metrics_data=$(generate_metrics $batch $ROWS_PER_BATCH)
    errors_data=$(generate_error_logs $batch $ROWS_PER_BATCH)

    # Send in parallel
    send_batch "$web_data" &
    send_batch "$metrics_data" &
    send_batch "$errors_data" &

    # Wait for parallel jobs, limiting concurrency
    if (( (batch + 1) % PARALLEL == 0 )); then
        wait
    fi
done

wait
end_time=$(date +%s)
duration=$((end_time - start_time))

echo -e "\rDone!                              "
echo ""
echo "Inserted $((TOTAL_ROWS * 3)) total rows across 3 tables in ${duration}s"
echo ""
echo "Tables:"
curl -s "${BASE_URL}/tables" | python3 -c "import sys,json; d=json.load(sys.stdin); [print(f\"  - {t['name']}: {t['row_count']} rows, {t['memory_bytes']/(1024*1024):.2f} MB\") for t in d['tables']]" 2>/dev/null || curl -s "${BASE_URL}/tables"
