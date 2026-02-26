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

    let rows_per_batch: usize = args.get(1).and_then(|s| s.parse().ok()).unwrap_or(1000);
    let num_batches: usize = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(10);
    let host = std::env::var("SNORKEL_HOST").unwrap_or_else(|_| "localhost".to_string());
    let port = std::env::var("SNORKEL_PORT").unwrap_or_else(|_| "9001".to_string());
    let base_url = format!("http://{}:{}", host, port);

    println!("Snorkel Benchmark");
    println!("=================");
    println!("Target:          {}", base_url);
    println!("Rows per batch:  {}", rows_per_batch);
    println!("Batches:         {}", num_batches);
    println!("Total rows:      {} (per table)", rows_per_batch * num_batches);
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

    Ok(())
}
