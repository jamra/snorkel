#!/bin/bash
# Seed Snorkel with sample data (100k records)

HOST="${SNORKEL_HOST:-localhost}"
PORT="${SNORKEL_PORT:-9000}"
BASE_URL="http://${HOST}:${PORT}"

EVENTS=("click" "view" "purchase" "scroll" "hover" "submit" "load" "error")
PAGES=("/home" "/products" "/checkout" "/about" "/contact" "/pricing" "/docs" "/api")
COUNTRIES=("US" "UK" "CA" "DE" "FR" "JP" "AU" "BR" "IN" "MX")
HOSTS=("web-1" "web-2" "web-3" "db-1" "db-2" "cache-1" "api-1" "api-2")
LEVELS=("error" "warn" "info" "debug")
SERVICES=("api" "auth" "db" "cache" "worker" "scheduler" "gateway" "storage")
MESSAGES=("Connection timeout" "Slow query" "Invalid token" "Rate limit" "High memory" "Server error" "Cache miss" "Session expired")
STATUS_CODES=(200 400 401 403 404 429 500 502 503 504)

echo "Seeding data to ${BASE_URL}..."
echo "Target: 100,000 records across 3 tables"
echo ""

NOW=$(date +%s)000
BATCH_SIZE=1000

# Generate web_events - 50,000 rows
echo "Creating web_events table with 50,000 rows..."
for batch in $(seq 0 49); do
    rows="["
    for i in $(seq 0 $((BATCH_SIZE - 1))); do
        idx=$((batch * BATCH_SIZE + i))
        ts=$((NOW - idx * 100 - RANDOM % 100))
        event=${EVENTS[$((RANDOM % ${#EVENTS[@]}))]}
        page=${PAGES[$((RANDOM % ${#PAGES[@]}))]}
        country=${COUNTRIES[$((RANDOM % ${#COUNTRIES[@]}))]}
        latency=$((20 + RANDOM % 480))
        user_id=$((1000 + idx))

        if [ $i -gt 0 ]; then rows+=","; fi
        rows+="{\"timestamp\":$ts,\"event\":\"$event\",\"page\":\"$page\",\"user_id\":$user_id,\"latency_ms\":$latency,\"country\":\"$country\"}"
    done
    rows+="]"

    curl -s -X POST "${BASE_URL}/ingest" -H "Content-Type: application/json" \
        -d "{\"table\":\"web_events\",\"rows\":$rows}" > /dev/null

    printf "\r  Progress: %d%%" $(( (batch + 1) * 2 ))
done
echo ""

# Generate metrics - 30,000 rows
echo "Creating metrics table with 30,000 rows..."
for batch in $(seq 0 29); do
    rows="["
    for i in $(seq 0 $((BATCH_SIZE - 1))); do
        idx=$((batch * BATCH_SIZE + i))
        ts=$((NOW - idx * 1000 - RANDOM % 1000))
        host=${HOSTS[$((RANDOM % ${#HOSTS[@]}))]}
        cpu="$((RANDOM % 80)).$((RANDOM % 10))"
        memory=$((512 + RANDOM % 15872))
        rps=$((10 + RANDOM % 990))

        if [ $i -gt 0 ]; then rows+=","; fi
        rows+="{\"timestamp\":$ts,\"host\":\"$host\",\"cpu_percent\":$cpu,\"memory_mb\":$memory,\"requests_per_sec\":$rps}"
    done
    rows+="]"

    curl -s -X POST "${BASE_URL}/ingest" -H "Content-Type: application/json" \
        -d "{\"table\":\"metrics\",\"rows\":$rows}" > /dev/null

    printf "\r  Progress: %d%%" $(( (batch + 1) * 100 / 30 ))
done
echo ""

# Generate error_logs - 20,000 rows
echo "Creating error_logs table with 20,000 rows..."
for batch in $(seq 0 19); do
    rows="["
    for i in $(seq 0 $((BATCH_SIZE - 1))); do
        idx=$((batch * BATCH_SIZE + i))
        ts=$((NOW - idx * 500 - RANDOM % 500))
        level=${LEVELS[$((RANDOM % ${#LEVELS[@]}))]}
        service=${SERVICES[$((RANDOM % ${#SERVICES[@]}))]}
        message=${MESSAGES[$((RANDOM % ${#MESSAGES[@]}))]}
        status=${STATUS_CODES[$((RANDOM % ${#STATUS_CODES[@]}))]}

        if [ $i -gt 0 ]; then rows+=","; fi
        rows+="{\"timestamp\":$ts,\"level\":\"$level\",\"service\":\"$service\",\"message\":\"$message\",\"status_code\":$status}"
    done
    rows+="]"

    curl -s -X POST "${BASE_URL}/ingest" -H "Content-Type: application/json" \
        -d "{\"table\":\"error_logs\",\"rows\":$rows}" > /dev/null

    printf "\r  Progress: %d%%" $(( (batch + 1) * 100 / 20 ))
done
echo ""

echo ""
echo "Done! Seeded 100,000 rows across 3 tables."
echo ""
echo "Tables:"
curl -s "${BASE_URL}/tables" | python3 -c "import sys,json; d=json.load(sys.stdin); [print(f\"  - {t['name']}: {t['row_count']} rows, {t['memory_bytes']/1024/1024:.2f} MB\") for t in d['tables']]" 2>/dev/null || curl -s "${BASE_URL}/tables"
