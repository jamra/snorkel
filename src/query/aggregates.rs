use crate::data::Value;
use std::any::Any;
use std::collections::HashMap;

/// Accumulator trait for aggregation functions
pub trait Accumulator: Send + Sync + AsAny {
    /// Add a value to the accumulator
    fn accumulate(&mut self, value: &Value);

    /// Get the final result
    fn result(&self) -> Value;

    /// Create a fresh copy of this accumulator
    fn clone_box(&self) -> Box<dyn Accumulator>;

    /// Merge another accumulator into this one (for parallel aggregation)
    fn merge(&mut self, other: &dyn Accumulator);
}

/// Helper trait to enable downcasting
pub trait AsAny {
    fn as_any(&self) -> &dyn Any;
}

impl<T: 'static> AsAny for T {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// COUNT(*) or COUNT(column)
#[derive(Debug, Clone)]
pub struct CountAccumulator {
    count: i64,
    count_nulls: bool, // COUNT(*) counts nulls, COUNT(col) doesn't
}

impl CountAccumulator {
    pub fn new(count_nulls: bool) -> Self {
        Self {
            count: 0,
            count_nulls,
        }
    }

    pub fn count_all() -> Self {
        Self::new(true)
    }

    pub fn count_column() -> Self {
        Self::new(false)
    }
}

impl Accumulator for CountAccumulator {
    fn accumulate(&mut self, value: &Value) {
        if self.count_nulls || !value.is_null() {
            self.count += 1;
        }
    }

    fn result(&self) -> Value {
        Value::Int64(self.count)
    }

    fn clone_box(&self) -> Box<dyn Accumulator> {
        Box::new(self.clone())
    }

    fn merge(&mut self, other: &dyn Accumulator) {
        if let Some(count_acc) = other.as_any().downcast_ref::<CountAccumulator>() {
            self.count += count_acc.count;
        }
    }
}

/// SUM(column)
#[derive(Debug, Clone)]
pub struct SumAccumulator {
    sum: f64,
    has_value: bool,
}

impl SumAccumulator {
    pub fn new() -> Self {
        Self {
            sum: 0.0,
            has_value: false,
        }
    }
}

impl Default for SumAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl Accumulator for SumAccumulator {
    fn accumulate(&mut self, value: &Value) {
        if let Some(v) = value.as_f64() {
            self.sum += v;
            self.has_value = true;
        }
    }

    fn result(&self) -> Value {
        if self.has_value {
            Value::Float64(self.sum)
        } else {
            Value::Null
        }
    }

    fn clone_box(&self) -> Box<dyn Accumulator> {
        Box::new(self.clone())
    }

    fn merge(&mut self, other: &dyn Accumulator) {
        if let Some(sum_acc) = other.as_any().downcast_ref::<SumAccumulator>() {
            if sum_acc.has_value {
                self.sum += sum_acc.sum;
                self.has_value = true;
            }
        }
    }
}

/// AVG(column)
#[derive(Debug, Clone)]
pub struct AvgAccumulator {
    sum: f64,
    count: i64,
}

impl AvgAccumulator {
    pub fn new() -> Self {
        Self { sum: 0.0, count: 0 }
    }
}

impl Default for AvgAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl Accumulator for AvgAccumulator {
    fn accumulate(&mut self, value: &Value) {
        if let Some(v) = value.as_f64() {
            self.sum += v;
            self.count += 1;
        }
    }

    fn result(&self) -> Value {
        if self.count > 0 {
            Value::Float64(self.sum / self.count as f64)
        } else {
            Value::Null
        }
    }

    fn clone_box(&self) -> Box<dyn Accumulator> {
        Box::new(self.clone())
    }

    fn merge(&mut self, other: &dyn Accumulator) {
        if let Some(avg_acc) = other.as_any().downcast_ref::<AvgAccumulator>() {
            self.sum += avg_acc.sum;
            self.count += avg_acc.count;
        }
    }
}

/// MIN(column)
#[derive(Debug, Clone)]
pub struct MinAccumulator {
    min: Option<Value>,
}

impl MinAccumulator {
    pub fn new() -> Self {
        Self { min: None }
    }
}

impl Default for MinAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl Accumulator for MinAccumulator {
    fn accumulate(&mut self, value: &Value) {
        if value.is_null() {
            return;
        }
        match &self.min {
            None => self.min = Some(value.clone()),
            Some(current) => {
                if value < current {
                    self.min = Some(value.clone());
                }
            }
        }
    }

    fn result(&self) -> Value {
        self.min.clone().unwrap_or(Value::Null)
    }

    fn clone_box(&self) -> Box<dyn Accumulator> {
        Box::new(self.clone())
    }

    fn merge(&mut self, other: &dyn Accumulator) {
        if let Some(min_acc) = other.as_any().downcast_ref::<MinAccumulator>() {
            if let Some(other_min) = &min_acc.min {
                self.accumulate(other_min);
            }
        }
    }
}

/// MAX(column)
#[derive(Debug, Clone)]
pub struct MaxAccumulator {
    max: Option<Value>,
}

impl MaxAccumulator {
    pub fn new() -> Self {
        Self { max: None }
    }
}

impl Default for MaxAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl Accumulator for MaxAccumulator {
    fn accumulate(&mut self, value: &Value) {
        if value.is_null() {
            return;
        }
        match &self.max {
            None => self.max = Some(value.clone()),
            Some(current) => {
                if value > current {
                    self.max = Some(value.clone());
                }
            }
        }
    }

    fn result(&self) -> Value {
        self.max.clone().unwrap_or(Value::Null)
    }

    fn clone_box(&self) -> Box<dyn Accumulator> {
        Box::new(self.clone())
    }

    fn merge(&mut self, other: &dyn Accumulator) {
        if let Some(max_acc) = other.as_any().downcast_ref::<MaxAccumulator>() {
            if let Some(other_max) = &max_acc.max {
                self.accumulate(other_max);
            }
        }
    }
}

/// PERCENTILE(column, p) - Approximate percentile using a reservoir sample
#[derive(Debug, Clone)]
pub struct PercentileAccumulator {
    percentile: u8,
    values: Vec<f64>,
    max_samples: usize,
}

impl PercentileAccumulator {
    pub fn new(percentile: u8) -> Self {
        Self {
            percentile,
            values: Vec::new(),
            max_samples: 10000, // Keep up to 10k samples
        }
    }
}

impl Accumulator for PercentileAccumulator {
    fn accumulate(&mut self, value: &Value) {
        if let Some(v) = value.as_f64() {
            if self.values.len() < self.max_samples {
                self.values.push(v);
            } else {
                // Reservoir sampling
                let idx = rand_index(self.values.len() + 1);
                if idx < self.max_samples {
                    self.values[idx] = v;
                }
            }
        }
    }

    fn result(&self) -> Value {
        if self.values.is_empty() {
            return Value::Null;
        }

        let mut sorted = self.values.clone();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

        let idx = ((self.percentile as f64 / 100.0) * (sorted.len() - 1) as f64).round() as usize;
        Value::Float64(sorted[idx.min(sorted.len() - 1)])
    }

    fn clone_box(&self) -> Box<dyn Accumulator> {
        Box::new(self.clone())
    }

    fn merge(&mut self, other: &dyn Accumulator) {
        if let Some(p_acc) = other.as_any().downcast_ref::<PercentileAccumulator>() {
            for &v in &p_acc.values {
                self.accumulate(&Value::Float64(v));
            }
        }
    }
}

/// Simple deterministic "random" for reservoir sampling
fn rand_index(n: usize) -> usize {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let mut hasher = DefaultHasher::new();
    n.hash(&mut hasher);
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos()
        .hash(&mut hasher);
    (hasher.finish() as usize) % n
}

/// HISTOGRAM(column, bucket_size) - Build a histogram
#[derive(Debug, Clone)]
pub struct HistogramAccumulator {
    bucket_size: f64,
    buckets: HashMap<i64, i64>, // bucket_key -> count
}

impl HistogramAccumulator {
    pub fn new(bucket_size: f64) -> Self {
        Self {
            bucket_size,
            buckets: HashMap::new(),
        }
    }
}

impl Accumulator for HistogramAccumulator {
    fn accumulate(&mut self, value: &Value) {
        if let Some(v) = value.as_f64() {
            let bucket_key = (v / self.bucket_size).floor() as i64;
            *self.buckets.entry(bucket_key).or_insert(0) += 1;
        }
    }

    fn result(&self) -> Value {
        // Return as JSON string representation
        let mut result: Vec<(i64, i64)> = self.buckets.iter().map(|(&k, &v)| (k, v)).collect();
        result.sort_by_key(|(k, _)| *k);

        let json = serde_json::to_string(&result).unwrap_or_else(|_| "[]".to_string());
        Value::String(json)
    }

    fn clone_box(&self) -> Box<dyn Accumulator> {
        Box::new(self.clone())
    }

    fn merge(&mut self, other: &dyn Accumulator) {
        if let Some(h_acc) = other.as_any().downcast_ref::<HistogramAccumulator>() {
            for (&key, &count) in &h_acc.buckets {
                *self.buckets.entry(key).or_insert(0) += count;
            }
        }
    }
}

/// Factory for creating accumulators
use super::parser::AggregateFunction;

pub fn create_accumulator(func: AggregateFunction, column: &Option<String>) -> Box<dyn Accumulator> {
    match func {
        AggregateFunction::Count => {
            if column.is_some() {
                Box::new(CountAccumulator::count_column())
            } else {
                Box::new(CountAccumulator::count_all())
            }
        }
        AggregateFunction::Sum => Box::new(SumAccumulator::new()),
        AggregateFunction::Avg => Box::new(AvgAccumulator::new()),
        AggregateFunction::Min => Box::new(MinAccumulator::new()),
        AggregateFunction::Max => Box::new(MaxAccumulator::new()),
        AggregateFunction::Percentile(p) => Box::new(PercentileAccumulator::new(p)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_count_accumulator() {
        let mut acc = CountAccumulator::count_all();
        acc.accumulate(&Value::Int64(1));
        acc.accumulate(&Value::Int64(2));
        acc.accumulate(&Value::Null);

        assert_eq!(acc.result(), Value::Int64(3));
    }

    #[test]
    fn test_count_column_ignores_nulls() {
        let mut acc = CountAccumulator::count_column();
        acc.accumulate(&Value::Int64(1));
        acc.accumulate(&Value::Null);
        acc.accumulate(&Value::Int64(2));

        assert_eq!(acc.result(), Value::Int64(2));
    }

    #[test]
    fn test_sum_accumulator() {
        let mut acc = SumAccumulator::new();
        acc.accumulate(&Value::Int64(10));
        acc.accumulate(&Value::Float64(5.5));
        acc.accumulate(&Value::Null);

        assert_eq!(acc.result(), Value::Float64(15.5));
    }

    #[test]
    fn test_avg_accumulator() {
        let mut acc = AvgAccumulator::new();
        acc.accumulate(&Value::Int64(10));
        acc.accumulate(&Value::Int64(20));
        acc.accumulate(&Value::Int64(30));

        assert_eq!(acc.result(), Value::Float64(20.0));
    }

    #[test]
    fn test_min_accumulator() {
        let mut acc = MinAccumulator::new();
        acc.accumulate(&Value::Int64(30));
        acc.accumulate(&Value::Int64(10));
        acc.accumulate(&Value::Int64(20));

        assert_eq!(acc.result(), Value::Int64(10));
    }

    #[test]
    fn test_max_accumulator() {
        let mut acc = MaxAccumulator::new();
        acc.accumulate(&Value::Int64(10));
        acc.accumulate(&Value::Int64(30));
        acc.accumulate(&Value::Int64(20));

        assert_eq!(acc.result(), Value::Int64(30));
    }

    #[test]
    fn test_percentile_accumulator() {
        let mut acc = PercentileAccumulator::new(50);
        for i in 1..=100 {
            acc.accumulate(&Value::Int64(i));
        }

        if let Value::Float64(p50) = acc.result() {
            assert!((p50 - 50.0).abs() < 2.0); // Allow some tolerance
        } else {
            panic!("Expected float result");
        }
    }

    #[test]
    fn test_empty_accumulator() {
        let acc = SumAccumulator::new();
        assert_eq!(acc.result(), Value::Null);

        let acc = AvgAccumulator::new();
        assert_eq!(acc.result(), Value::Null);

        let acc = MinAccumulator::new();
        assert_eq!(acc.result(), Value::Null);
    }

    #[test]
    fn test_merge_accumulators() {
        let mut acc1 = SumAccumulator::new();
        acc1.accumulate(&Value::Int64(10));
        acc1.accumulate(&Value::Int64(20));

        let mut acc2 = SumAccumulator::new();
        acc2.accumulate(&Value::Int64(30));
        acc2.accumulate(&Value::Int64(40));

        acc1.merge(&acc2);
        assert_eq!(acc1.result(), Value::Float64(100.0));
    }

    #[test]
    fn test_merge_avg_accumulators() {
        let mut acc1 = AvgAccumulator::new();
        acc1.accumulate(&Value::Int64(10));
        acc1.accumulate(&Value::Int64(20));

        let mut acc2 = AvgAccumulator::new();
        acc2.accumulate(&Value::Int64(30));
        acc2.accumulate(&Value::Int64(40));

        acc1.merge(&acc2);
        // Average of 10, 20, 30, 40 = 25
        assert_eq!(acc1.result(), Value::Float64(25.0));
    }
}
