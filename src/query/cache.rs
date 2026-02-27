//! Query result caching for sub-millisecond repeated query responses
//!
//! Uses moka for thread-safe concurrent caching with TTL-based expiration.

use moka::sync::Cache;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use super::executor::QueryResult;

/// Cache key for query results
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct CacheKey {
    /// SQL query (normalized)
    sql: String,
    /// Table name (for targeted invalidation)
    table: Option<String>,
}

impl CacheKey {
    pub fn new(sql: &str) -> Self {
        Self {
            sql: normalize_sql(sql),
            table: extract_table_name(sql),
        }
    }

    pub fn table(&self) -> Option<&str> {
        self.table.as_deref()
    }
}

/// Query cache with TTL and invalidation support
pub struct QueryCache {
    cache: Cache<CacheKey, QueryResult>,
    /// Cache hit count
    hits: AtomicU64,
    /// Cache miss count
    misses: AtomicU64,
    /// TTL for cache entries
    ttl: Duration,
}

impl QueryCache {
    /// Create a new cache with default settings
    pub fn new() -> Self {
        Self::with_config(1000, Duration::from_secs(60))
    }

    /// Create a cache with custom configuration
    pub fn with_config(max_entries: u64, ttl: Duration) -> Self {
        Self {
            cache: Cache::builder()
                .max_capacity(max_entries)
                .time_to_live(ttl)
                .build(),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            ttl,
        }
    }

    /// Get a cached result
    pub fn get(&self, sql: &str) -> Option<QueryResult> {
        let key = CacheKey::new(sql);
        if let Some(result) = self.cache.get(&key) {
            self.hits.fetch_add(1, Ordering::Relaxed);
            Some(result)
        } else {
            self.misses.fetch_add(1, Ordering::Relaxed);
            None
        }
    }

    /// Store a result in the cache
    pub fn put(&self, sql: &str, result: QueryResult) {
        let key = CacheKey::new(sql);
        self.cache.insert(key, result);
    }

    /// Invalidate cache entries for a specific table
    pub fn invalidate_table(&self, table: &str) {
        // Collect keys that match the table, then invalidate each one
        let table_lower = table.to_lowercase();
        let keys_to_invalidate: Vec<CacheKey> = self
            .cache
            .iter()
            .filter(|(key, _)| {
                key.table()
                    .map(|t| t.to_lowercase() == table_lower)
                    .unwrap_or(false)
            })
            .map(|(key, _)| (*key).clone())
            .collect();

        for key in keys_to_invalidate {
            self.cache.invalidate(&key);
        }
    }

    /// Invalidate all cache entries
    pub fn invalidate_all(&self) {
        self.cache.invalidate_all();
    }

    /// Get cache statistics
    pub fn stats(&self) -> CacheStats {
        let hits = self.hits.load(Ordering::Relaxed);
        let misses = self.misses.load(Ordering::Relaxed);
        let total = hits + misses;

        CacheStats {
            hits,
            misses,
            hit_rate: if total > 0 {
                hits as f64 / total as f64
            } else {
                0.0
            },
            entry_count: self.cache.entry_count(),
            ttl_secs: self.ttl.as_secs(),
        }
    }

    /// Get TTL
    pub fn ttl(&self) -> Duration {
        self.ttl
    }
}

impl Default for QueryCache {
    fn default() -> Self {
        Self::new()
    }
}

/// Cache statistics
#[derive(Debug, Clone, serde::Serialize)]
pub struct CacheStats {
    /// Number of cache hits
    pub hits: u64,
    /// Number of cache misses
    pub misses: u64,
    /// Hit rate (0.0 - 1.0)
    pub hit_rate: f64,
    /// Number of entries in cache
    pub entry_count: u64,
    /// TTL in seconds
    pub ttl_secs: u64,
}

/// Normalize SQL for consistent cache keys
fn normalize_sql(sql: &str) -> String {
    sql.trim()
        .to_lowercase()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

/// Extract table name from SQL for targeted invalidation
fn extract_table_name(sql: &str) -> Option<String> {
    let lower = sql.to_lowercase();

    // Handle SELECT ... FROM table
    if let Some(from_idx) = lower.find(" from ") {
        let after_from = &lower[from_idx + 6..];
        let table = after_from
            .split_whitespace()
            .next()?
            .trim_end_matches(|c: char| !c.is_alphanumeric() && c != '_');
        return Some(table.to_string());
    }

    // Handle INSERT INTO table
    if let Some(into_idx) = lower.find(" into ") {
        let after_into = &lower[into_idx + 6..];
        let table = after_into
            .split_whitespace()
            .next()?
            .trim_end_matches(|c: char| !c.is_alphanumeric() && c != '_');
        return Some(table.to_string());
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::Value;

    fn make_result(rows: usize) -> QueryResult {
        QueryResult {
            columns: vec!["col1".to_string()],
            rows: (0..rows).map(|i| vec![Value::Int64(i as i64)]).collect(),
            rows_scanned: rows,
            shards_scanned: 1,
            execution_time_ms: 10,
            availability: None,
        }
    }

    #[test]
    fn test_cache_put_get() {
        let cache = QueryCache::new();

        let result = make_result(5);
        cache.put("SELECT * FROM events", result.clone());

        let cached = cache.get("SELECT * FROM events").unwrap();
        assert_eq!(cached.rows.len(), 5);
    }

    #[test]
    fn test_cache_normalization() {
        let cache = QueryCache::new();

        let result = make_result(3);
        cache.put("SELECT * FROM events", result);

        // Same query with different whitespace should hit cache
        let cached = cache.get("  SELECT   *   FROM   events  ");
        assert!(cached.is_some());

        // Same query with different case should hit cache
        let cached = cache.get("select * from EVENTS");
        assert!(cached.is_some());
    }

    #[test]
    fn test_cache_miss() {
        let cache = QueryCache::new();

        let result = cache.get("SELECT * FROM nonexistent");
        assert!(result.is_none());

        let stats = cache.stats();
        assert_eq!(stats.misses, 1);
    }

    #[test]
    fn test_cache_invalidation() {
        let cache = QueryCache::new();

        cache.put("SELECT * FROM events", make_result(1));
        cache.put("SELECT * FROM logs", make_result(2));

        // Invalidate events table
        cache.invalidate_table("events");

        assert!(cache.get("SELECT * FROM events").is_none());
        assert!(cache.get("SELECT * FROM logs").is_some());
    }

    #[test]
    fn test_cache_stats() {
        let cache = QueryCache::new();

        cache.put("SELECT * FROM events", make_result(1));

        // Hit
        let _ = cache.get("SELECT * FROM events");
        // Miss
        let _ = cache.get("SELECT * FROM other");

        let stats = cache.stats();
        assert_eq!(stats.hits, 1);
        assert_eq!(stats.misses, 1);
        assert!((stats.hit_rate - 0.5).abs() < 0.01);
    }

    #[test]
    fn test_extract_table_name() {
        assert_eq!(
            extract_table_name("SELECT * FROM events"),
            Some("events".to_string())
        );
        assert_eq!(
            extract_table_name("SELECT COUNT(*) FROM events WHERE x > 0"),
            Some("events".to_string())
        );
        assert_eq!(
            extract_table_name("INSERT INTO logs VALUES (1, 2)"),
            Some("logs".to_string())
        );
    }
}
