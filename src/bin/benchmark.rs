use reqwest::Client;
use serde_json::{json, Value};
use std::time::{Duration, Instant};

const EVENTS: &[&str] = &["click", "view", "purchase", "scroll", "hover", "submit", "load", "error"];
const PAGES: &[&str] = &["/home", "/products", "/checkout", "/about", "/contact", "/pricing", "/docs", "/api", "/dashboard", "/settings"];
const COUNTRIES: &[&str] = &["US", "UK", "CA", "DE", "FR", "JP", "AU", "BR", "IN", "MX", "ES", "IT", "NL", "SE", "NO"];
const HOSTS: &[&str] = &["web-1", "web-2", "web-3", "db-1", "db-2", "cache-1", "api-1", "api-2"];
const LEVELS: &[&str] = &["error", "warn", "info", "debug"];
const SERVICES: &[&str] = &["api", "auth", "db", "cache", "worker", "scheduler", "gateway", "storage"];
const MESSAGES: &[&str] = &["Connection timeout", "Slow query", "Invalid token", "Rate limit", "High memory", "Server error", "Cache miss", "Session expired", "Bad request", "Pool exhausted"];
const STATUS_CODES: &[i64] = &[200, 400, 401, 403, 404, 429, 500, 502, 503, 504];

// Predefined scales for quick selection
const SCALE_SMALL: (usize, usize) = (1_000, 10);       // 10K rows per table
const SCALE_MEDIUM: (usize, usize) = (10_000, 10);     // 100K rows per table
const SCALE_LARGE: (usize, usize) = (10_000, 100);     // 1M rows per table
const SCALE_XLARGE: (usize, usize) = (10_000, 1_000);  // 10M rows per table

fn fast_random(seed: &mut u64) -> u64 {
    *seed ^= *seed << 13;
    *seed ^= *seed >> 7;
    *seed ^= *seed << 17;
    *seed
}

fn generate_web_events(count: usize, batch_num: usize, seed: &mut u64) -> Value {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;

    let rows: Vec<Value> = (0..count)
        .map(|i| {
            let ts = now - (batch_num * count * 100 + i * 100) as i64 - (fast_random(seed) % 100) as i64;
            json!({
                "timestamp": ts,
                "event": EVENTS[fast_random(seed) as usize % EVENTS.len()],
                "page": PAGES[fast_random(seed) as usize % PAGES.len()],
                "user_id": 1000 + batch_num * count + i,
                "latency_ms": 20 + (fast_random(seed) % 480) as i64,
                "country": COUNTRIES[fast_random(seed) as usize % COUNTRIES.len()]
            })
        })
        .collect();

    json!({ "table": "web_events", "rows": rows })
}

fn generate_metrics(count: usize, batch_num: usize, seed: &mut u64) -> Value {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;

    let rows: Vec<Value> = (0..count)
        .map(|i| {
            let ts = now - (batch_num * count * 1000 + i * 1000) as i64 - (fast_random(seed) % 1000) as i64;
            json!({
                "timestamp": ts,
                "host": HOSTS[fast_random(seed) as usize % HOSTS.len()],
                "cpu_percent": (fast_random(seed) % 800) as f64 / 10.0,
                "memory_mb": 512 + (fast_random(seed) % 15872) as i64,
                "requests_per_sec": 10 + (fast_random(seed) % 990) as i64
            })
        })
        .collect();

    json!({ "table": "metrics", "rows": rows })
}

fn generate_error_logs(count: usize, batch_num: usize, seed: &mut u64) -> Value {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;

    let rows: Vec<Value> = (0..count)
        .map(|i| {
            let ts = now - (batch_num * count * 500 + i * 500) as i64 - (fast_random(seed) % 500) as i64;
            json!({
                "timestamp": ts,
                "level": LEVELS[fast_random(seed) as usize % LEVELS.len()],
                "service": SERVICES[fast_random(seed) as usize % SERVICES.len()],
                "message": MESSAGES[fast_random(seed) as usize % MESSAGES.len()],
                "status_code": STATUS_CODES[fast_random(seed) as usize % STATUS_CODES.len()]
            })
        })
        .collect();

    json!({ "table": "error_logs", "rows": rows })
}

struct BenchmarkStats {
    total_rows: usize,
    total_duration: Duration,
    batch_latencies: Vec<Duration>,
}

impl BenchmarkStats {
    fn rows_per_sec(&self) -> f64 {
        self.total_rows as f64 / self.total_duration.as_secs_f64()
    }

    fn avg_latency(&self) -> Duration {
        let sum: Duration = self.batch_latencies.iter().sum();
        sum / self.batch_latencies.len() as u32
    }

    fn min_latency(&self) -> Duration {
        *self.batch_latencies.iter().min().unwrap()
    }

    fn max_latency(&self) -> Duration {
        *self.batch_latencies.iter().max().unwrap()
    }

    fn p50_latency(&self) -> Duration {
        let mut sorted = self.batch_latencies.clone();
        sorted.sort();
        sorted[sorted.len() / 2]
    }

    fn p99_latency(&self) -> Duration {
        let mut sorted = self.batch_latencies.clone();
        sorted.sort();
        sorted[sorted.len() * 99 / 100]
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();

    // Support scale shortcuts: small, medium, large, xlarge
    let (rows_per_batch, num_batches) = match args.get(1).map(|s| s.as_str()) {
        Some("small") => SCALE_SMALL,
        Some("medium") => SCALE_MEDIUM,
        Some("large") => SCALE_LARGE,
        Some("xlarge") => SCALE_XLARGE,
        _ => {
            let rows = args.get(1).and_then(|s| s.parse().ok()).unwrap_or(1000);
            let batches = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(10);
            (rows, batches)
        }
    };

    let host = std::env::var("SNORKEL_HOST").unwrap_or_else(|_| "localhost".to_string());
    let port = std::env::var("SNORKEL_PORT").unwrap_or_else(|_| "8080".to_string());
    let base_url = format!("http://{}:{}", host, port);

    let total_rows = rows_per_batch * num_batches;
    let scale_name = match total_rows {
        0..=50_000 => "small",
        50_001..=500_000 => "medium",
        500_001..=5_000_000 => "large",
        _ => "xlarge",
    };

    println!("Snorkel Benchmark");
    println!("=================");
    println!("Target:          {}", base_url);
    println!("Scale:           {} ({} rows/table)", scale_name, total_rows);
    println!("Rows per batch:  {}", rows_per_batch);
    println!("Batches:         {}", num_batches);
    println!();
    println!("Usage: benchmark [small|medium|large|xlarge] or [rows_per_batch] [num_batches]");
    println!();

    let client = Client::new();
    let mut seed: u64 = 12345;

    // Benchmark each table type
    for table in ["web_events", "metrics", "error_logs"] {
        print!("Benchmarking {}... ", table);
        std::io::Write::flush(&mut std::io::stdout())?;

        let mut batch_latencies = Vec::with_capacity(num_batches);
        let start = Instant::now();

        for batch_num in 0..num_batches {
            let data = match table {
                "web_events" => generate_web_events(rows_per_batch, batch_num, &mut seed),
                "metrics" => generate_metrics(rows_per_batch, batch_num, &mut seed),
                "error_logs" => generate_error_logs(rows_per_batch, batch_num, &mut seed),
                _ => unreachable!(),
            };

            let batch_start = Instant::now();
            let response = client
                .post(format!("{}/ingest", base_url))
                .json(&data)
                .send()
                .await?;

            if !response.status().is_success() {
                eprintln!("Error: {}", response.text().await?);
                return Ok(());
            }
            batch_latencies.push(batch_start.elapsed());
        }

        let stats = BenchmarkStats {
            total_rows: rows_per_batch * num_batches,
            total_duration: start.elapsed(),
            batch_latencies,
        };

        println!("done");
        println!("  Rows/sec:    {:.0}", stats.rows_per_sec());
        println!("  Total time:  {:?}", stats.total_duration);
        println!("  Latency:     avg={:?} min={:?} max={:?}",
            stats.avg_latency(), stats.min_latency(), stats.max_latency());
        println!("  Percentiles: p50={:?} p99={:?}",
            stats.p50_latency(), stats.p99_latency());
        println!();
    }

    // Print table stats
    println!("Table Statistics:");
    let resp: Value = client
        .get(format!("{}/tables", base_url))
        .send()
        .await?
        .json()
        .await?;

    if let Some(tables) = resp["tables"].as_array() {
        for t in tables {
            println!(
                "  {}: {} rows, {:.2} MB",
                t["name"].as_str().unwrap_or("?"),
                t["row_count"],
                t["memory_bytes"].as_u64().unwrap_or(0) as f64 / 1024.0 / 1024.0
            );
        }
    }

    // Query benchmarks
    println!();
    println!("Query Benchmarks");
    println!("================");

    let queries = vec![
        ("COUNT(*)", "SELECT COUNT(*) FROM web_events"),
        ("COUNT with filter", "SELECT COUNT(*) FROM web_events WHERE event = 'click'"),
        ("GROUP BY", "SELECT event, COUNT(*) FROM web_events GROUP BY event"),
        ("GROUP BY + ORDER", "SELECT event, COUNT(*) as cnt FROM web_events GROUP BY event ORDER BY cnt DESC"),
        ("AVG aggregation", "SELECT AVG(latency_ms) FROM web_events"),
        ("Multiple aggs", "SELECT COUNT(*), AVG(latency_ms), MIN(latency_ms), MAX(latency_ms) FROM web_events"),
        ("GROUP BY country", "SELECT country, COUNT(*), AVG(latency_ms) FROM web_events GROUP BY country"),
        ("Top pages", "SELECT page, COUNT(*) as hits FROM web_events GROUP BY page ORDER BY hits DESC LIMIT 5"),
        ("Metrics by host", "SELECT host, AVG(cpu_percent), AVG(memory_mb) FROM metrics GROUP BY host"),
        ("Error counts", "SELECT level, COUNT(*) FROM error_logs GROUP BY level"),
    ];

    let num_iterations = 10;

    for (name, sql) in &queries {
        let mut latencies = Vec::with_capacity(num_iterations);

        // Warm up (1 query, not timed)
        let _ = client
            .post(format!("{}/query", base_url))
            .json(&json!({ "sql": sql }))
            .send()
            .await?;

        // Timed iterations
        for _ in 0..num_iterations {
            let start = Instant::now();
            let response = client
                .post(format!("{}/query", base_url))
                .json(&json!({ "sql": sql }))
                .send()
                .await?;

            if !response.status().is_success() {
                eprintln!("Query failed: {} - {}", name, response.text().await?);
                break;
            }
            latencies.push(start.elapsed());
        }

        if latencies.is_empty() {
            continue;
        }

        let avg: Duration = latencies.iter().sum::<Duration>() / latencies.len() as u32;
        let min = *latencies.iter().min().unwrap();
        let max = *latencies.iter().max().unwrap();
        latencies.sort();
        let p50 = latencies[latencies.len() / 2];
        let p99 = latencies[latencies.len() * 99 / 100];

        println!("{:20} avg={:>8.2?} min={:>8.2?} max={:>8.2?} p50={:>8.2?} p99={:>8.2?}",
            name, avg, min, max, p50, p99);
    }

    // Cache benchmark
    println!();
    println!("Cache Performance");
    println!("=================");

    // Clear cache first
    let _ = client
        .post(format!("{}/cache/invalidate", base_url))
        .send()
        .await?;

    let cache_query = "SELECT event, COUNT(*) FROM web_events GROUP BY event";

    // Cold query (cache miss)
    let start = Instant::now();
    let _ = client
        .post(format!("{}/query", base_url))
        .json(&json!({ "sql": cache_query }))
        .send()
        .await?;
    let cold_time = start.elapsed();

    // Warm queries (cache hits)
    let mut warm_times = Vec::with_capacity(10);
    for _ in 0..10 {
        let start = Instant::now();
        let _ = client
            .post(format!("{}/query", base_url))
            .json(&json!({ "sql": cache_query }))
            .send()
            .await?;
        warm_times.push(start.elapsed());
    }

    let warm_avg: Duration = warm_times.iter().sum::<Duration>() / warm_times.len() as u32;
    let speedup = cold_time.as_secs_f64() / warm_avg.as_secs_f64();

    println!("Cold (cache miss):   {:?}", cold_time);
    println!("Warm (cache hit):    {:?} avg", warm_avg);
    println!("Cache speedup:       {:.1}x", speedup);

    // Print cache stats
    let cache_stats: Value = client
        .get(format!("{}/cache/stats", base_url))
        .send()
        .await?
        .json()
        .await?;

    if let Some(cache) = cache_stats.get("cache") {
        println!("Cache stats:         hits={}, misses={}, size={}",
            cache["hits"], cache["misses"], cache["size"]);
    }

    // Bloom Filter Effectiveness
    println!();
    println!("Bloom Filter Effectiveness");
    println!("==========================");
    println!("Testing shard pruning with rare vs common value queries...");

    // Query for a common value (exists in many shards)
    let common_query = "SELECT COUNT(*) FROM web_events WHERE event = 'click'";
    let mut common_times = Vec::with_capacity(10);
    for _ in 0..10 {
        let start = Instant::now();
        let _ = client
            .post(format!("{}/query", base_url))
            .json(&json!({ "sql": common_query }))
            .send()
            .await?;
        common_times.push(start.elapsed());
    }
    let common_avg: Duration = common_times.iter().sum::<Duration>() / common_times.len() as u32;

    // Query for a rare value (exists in few/no shards - bloom filter should prune)
    let rare_query = "SELECT COUNT(*) FROM web_events WHERE event = 'rare_event_xyz'";
    let mut rare_times = Vec::with_capacity(10);
    for _ in 0..10 {
        let start = Instant::now();
        let _ = client
            .post(format!("{}/query", base_url))
            .json(&json!({ "sql": rare_query }))
            .send()
            .await?;
        rare_times.push(start.elapsed());
    }
    let rare_avg: Duration = rare_times.iter().sum::<Duration>() / rare_times.len() as u32;

    println!("Common value query:  {:?} avg (scans all matching shards)", common_avg);
    println!("Rare value query:    {:?} avg (bloom filter prunes shards)", rare_avg);
    if common_avg > rare_avg {
        let improvement = common_avg.as_secs_f64() / rare_avg.as_secs_f64();
        println!("Bloom filter speedup: {:.1}x for non-existent values", improvement);
    }

    // Predicate Pushdown Effectiveness
    println!();
    println!("Predicate Pushdown Effectiveness");
    println!("================================");

    // Compare scan with no filter vs scan with selective filter
    let no_filter = "SELECT COUNT(*) FROM web_events";
    let selective_filter = "SELECT COUNT(*) FROM web_events WHERE country = 'US' AND event = 'purchase'";

    let mut no_filter_times = Vec::with_capacity(10);
    for _ in 0..10 {
        let start = Instant::now();
        let _ = client
            .post(format!("{}/query", base_url))
            .json(&json!({ "sql": no_filter }))
            .send()
            .await?;
        no_filter_times.push(start.elapsed());
    }
    let no_filter_avg: Duration = no_filter_times.iter().sum::<Duration>() / no_filter_times.len() as u32;

    let mut selective_times = Vec::with_capacity(10);
    for _ in 0..10 {
        let start = Instant::now();
        let _ = client
            .post(format!("{}/query", base_url))
            .json(&json!({ "sql": selective_filter }))
            .send()
            .await?;
        selective_times.push(start.elapsed());
    }
    let selective_avg: Duration = selective_times.iter().sum::<Duration>() / selective_times.len() as u32;

    println!("Full scan (no filter):     {:?} avg", no_filter_avg);
    println!("Selective filter:          {:?} avg", selective_avg);
    println!("Note: Predicate pushdown builds row masks once vs per-row evaluation");

    // Aggregation Performance Comparison
    println!();
    println!("Aggregation Performance");
    println!("=======================");

    let agg_queries = vec![
        ("COUNT(*)", "SELECT COUNT(*) FROM web_events"),
        ("SUM", "SELECT SUM(latency_ms) FROM web_events"),
        ("AVG", "SELECT AVG(latency_ms) FROM web_events"),
        ("MIN/MAX", "SELECT MIN(latency_ms), MAX(latency_ms) FROM web_events"),
        ("Multi-agg (SIMD)", "SELECT COUNT(*), SUM(latency_ms), AVG(latency_ms), MIN(latency_ms), MAX(latency_ms) FROM web_events"),
        ("GROUP BY (hash)", "SELECT event, COUNT(*) FROM web_events GROUP BY event"),
        ("GROUP BY + aggs", "SELECT country, AVG(latency_ms), SUM(latency_ms) FROM web_events GROUP BY country"),
    ];

    for (name, sql) in &agg_queries {
        let mut times = Vec::with_capacity(10);
        for _ in 0..10 {
            let start = Instant::now();
            let _ = client
                .post(format!("{}/query", base_url))
                .json(&json!({ "sql": sql }))
                .send()
                .await?;
            times.push(start.elapsed());
        }
        let avg: Duration = times.iter().sum::<Duration>() / times.len() as u32;
        let min = *times.iter().min().unwrap();
        println!("{:20} avg={:>8.2?} min={:>8.2?}", name, avg, min);
    }

    // Final Summary
    println!();
    println!("Summary");
    println!("=======");

    // Re-fetch table stats
    let resp: Value = client
        .get(format!("{}/tables", base_url))
        .send()
        .await?
        .json()
        .await?;

    let mut total_rows_in_db = 0u64;
    let mut total_memory = 0u64;
    if let Some(tables) = resp["tables"].as_array() {
        for t in tables {
            total_rows_in_db += t["row_count"].as_u64().unwrap_or(0);
            total_memory += t["memory_bytes"].as_u64().unwrap_or(0);
        }
    }

    let bytes_per_row = if total_rows_in_db > 0 {
        total_memory as f64 / total_rows_in_db as f64
    } else {
        0.0
    };

    println!("Total rows:          {}", total_rows_in_db);
    println!("Total memory:        {:.2} MB", total_memory as f64 / 1024.0 / 1024.0);
    println!("Bytes per row:       {:.1}", bytes_per_row);
    println!();
    println!("Optimizations enabled:");
    println!("  - Bloom filters for shard pruning (equality filters)");
    println!("  - Predicate pushdown with row masks");
    println!("  - SIMD-friendly aggregations (single-pass stats)");
    println!("  - Query result caching (TTL-based)");
    println!("  - Parallel shard processing (rayon)");

    Ok(())
}
