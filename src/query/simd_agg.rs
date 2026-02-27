//! SIMD-friendly aggregation functions
//!
//! These functions are written to enable auto-vectorization by the compiler.
//! They operate on slices of primitive types for maximum performance.

/// Sum a slice of i64 values, skipping None values
#[inline]
pub fn sum_i64(values: &[Option<i64>]) -> i64 {
    values.iter().filter_map(|v| *v).sum()
}

/// Sum a slice of i64 values (no nulls)
#[inline]
pub fn sum_i64_dense(values: &[i64]) -> i64 {
    // Written to enable auto-vectorization
    values.iter().copied().sum()
}

/// Sum a slice of f64 values, skipping None values
#[inline]
pub fn sum_f64(values: &[Option<f64>]) -> f64 {
    values.iter().filter_map(|v| *v).sum()
}

/// Sum a slice of f64 values (no nulls)
#[inline]
pub fn sum_f64_dense(values: &[f64]) -> f64 {
    values.iter().copied().sum()
}

/// Count non-null values
#[inline]
pub fn count_non_null<T>(values: &[Option<T>]) -> usize {
    values.iter().filter(|v| v.is_some()).count()
}

/// Count all values (for COUNT(*))
#[inline]
pub fn count_all(len: usize) -> i64 {
    len as i64
}

/// Find minimum i64, skipping None values
#[inline]
pub fn min_i64(values: &[Option<i64>]) -> Option<i64> {
    values.iter().filter_map(|v| *v).min()
}

/// Find minimum i64 (no nulls)
#[inline]
pub fn min_i64_dense(values: &[i64]) -> Option<i64> {
    values.iter().copied().min()
}

/// Find minimum f64, skipping None values
#[inline]
pub fn min_f64(values: &[Option<f64>]) -> Option<f64> {
    values
        .iter()
        .filter_map(|v| *v)
        .min_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
}

/// Find minimum f64 (no nulls)
#[inline]
pub fn min_f64_dense(values: &[f64]) -> Option<f64> {
    values
        .iter()
        .copied()
        .min_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
}

/// Find maximum i64, skipping None values
#[inline]
pub fn max_i64(values: &[Option<i64>]) -> Option<i64> {
    values.iter().filter_map(|v| *v).max()
}

/// Find maximum i64 (no nulls)
#[inline]
pub fn max_i64_dense(values: &[i64]) -> Option<i64> {
    values.iter().copied().max()
}

/// Find maximum f64, skipping None values
#[inline]
pub fn max_f64(values: &[Option<f64>]) -> Option<f64> {
    values
        .iter()
        .filter_map(|v| *v)
        .max_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
}

/// Find maximum f64 (no nulls)
#[inline]
pub fn max_f64_dense(values: &[f64]) -> Option<f64> {
    values
        .iter()
        .copied()
        .max_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
}

/// Compute sum, count, min, max in a single pass (more cache-friendly)
#[derive(Debug, Clone, Default)]
pub struct AggregateStats {
    pub sum: f64,
    pub count: usize,
    pub min: Option<f64>,
    pub max: Option<f64>,
}

impl AggregateStats {
    /// Compute all basic aggregates in a single pass
    #[inline]
    pub fn compute_f64(values: &[Option<f64>]) -> Self {
        let mut sum = 0.0;
        let mut count = 0usize;
        let mut min: Option<f64> = None;
        let mut max: Option<f64> = None;

        for v in values.iter().filter_map(|v| *v) {
            sum += v;
            count += 1;
            min = Some(min.map_or(v, |m| m.min(v)));
            max = Some(max.map_or(v, |m| m.max(v)));
        }

        Self { sum, count, min, max }
    }

    /// Compute all basic aggregates for dense f64 slice
    #[inline]
    pub fn compute_f64_dense(values: &[f64]) -> Self {
        if values.is_empty() {
            return Self::default();
        }

        let mut sum = 0.0;
        let mut min = values[0];
        let mut max = values[0];

        for &v in values {
            sum += v;
            if v < min { min = v; }
            if v > max { max = v; }
        }

        Self {
            sum,
            count: values.len(),
            min: Some(min),
            max: Some(max),
        }
    }

    /// Compute all basic aggregates for i64 values
    #[inline]
    pub fn compute_i64(values: &[Option<i64>]) -> Self {
        let mut sum = 0i64;
        let mut count = 0usize;
        let mut min: Option<i64> = None;
        let mut max: Option<i64> = None;

        for v in values.iter().filter_map(|v| *v) {
            sum += v;
            count += 1;
            min = Some(min.map_or(v, |m| m.min(v)));
            max = Some(max.map_or(v, |m| m.max(v)));
        }

        Self {
            sum: sum as f64,
            count,
            min: min.map(|v| v as f64),
            max: max.map(|v| v as f64),
        }
    }

    /// Compute all basic aggregates for dense i64 slice
    #[inline]
    pub fn compute_i64_dense(values: &[i64]) -> Self {
        if values.is_empty() {
            return Self::default();
        }

        let mut sum = 0i64;
        let mut min = values[0];
        let mut max = values[0];

        for &v in values {
            sum += v;
            if v < min { min = v; }
            if v > max { max = v; }
        }

        Self {
            sum: sum as f64,
            count: values.len(),
            min: Some(min as f64),
            max: Some(max as f64),
        }
    }

    /// Merge two aggregate stats
    #[inline]
    pub fn merge(&mut self, other: &Self) {
        self.sum += other.sum;
        self.count += other.count;
        self.min = match (self.min, other.min) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, None) => a,
            (None, b) => b,
        };
        self.max = match (self.max, other.max) {
            (Some(a), Some(b)) => Some(a.max(b)),
            (a, None) => a,
            (None, b) => b,
        };
    }

    /// Get the average
    #[inline]
    pub fn avg(&self) -> Option<f64> {
        if self.count > 0 {
            Some(self.sum / self.count as f64)
        } else {
            None
        }
    }
}

/// Vectorized filtering - returns indices of matching elements
#[inline]
pub fn filter_eq_i64(values: &[i64], target: i64) -> Vec<usize> {
    values
        .iter()
        .enumerate()
        .filter(|(_, &v)| v == target)
        .map(|(i, _)| i)
        .collect()
}

/// Vectorized filtering for range queries
#[inline]
pub fn filter_range_i64(values: &[i64], min: i64, max: i64) -> Vec<usize> {
    values
        .iter()
        .enumerate()
        .filter(|(_, &v)| v >= min && v < max)
        .map(|(i, _)| i)
        .collect()
}

/// Apply a bitmask to select values
#[inline]
pub fn apply_mask_i64(values: &[i64], mask: &[bool]) -> Vec<i64> {
    values
        .iter()
        .zip(mask.iter())
        .filter(|(_, &m)| m)
        .map(|(&v, _)| v)
        .collect()
}

/// Apply a bitmask to select values (f64 version)
#[inline]
pub fn apply_mask_f64(values: &[f64], mask: &[bool]) -> Vec<f64> {
    values
        .iter()
        .zip(mask.iter())
        .filter(|(_, &m)| m)
        .map(|(&v, _)| v)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sum_i64() {
        let values: Vec<Option<i64>> = vec![Some(1), Some(2), None, Some(3), Some(4)];
        assert_eq!(sum_i64(&values), 10);
    }

    #[test]
    fn test_sum_i64_dense() {
        let values = vec![1i64, 2, 3, 4, 5];
        assert_eq!(sum_i64_dense(&values), 15);
    }

    #[test]
    fn test_min_max_i64() {
        let values: Vec<Option<i64>> = vec![Some(5), Some(2), None, Some(8), Some(1)];
        assert_eq!(min_i64(&values), Some(1));
        assert_eq!(max_i64(&values), Some(8));
    }

    #[test]
    fn test_aggregate_stats() {
        let values: Vec<Option<f64>> = vec![Some(1.0), Some(2.0), None, Some(3.0), Some(4.0)];
        let stats = AggregateStats::compute_f64(&values);

        assert_eq!(stats.count, 4);
        assert!((stats.sum - 10.0).abs() < 0.001);
        assert!((stats.min.unwrap() - 1.0).abs() < 0.001);
        assert!((stats.max.unwrap() - 4.0).abs() < 0.001);
        assert!((stats.avg().unwrap() - 2.5).abs() < 0.001);
    }

    #[test]
    fn test_aggregate_stats_dense() {
        let values = vec![1.0f64, 2.0, 3.0, 4.0, 5.0];
        let stats = AggregateStats::compute_f64_dense(&values);

        assert_eq!(stats.count, 5);
        assert!((stats.sum - 15.0).abs() < 0.001);
        assert!((stats.avg().unwrap() - 3.0).abs() < 0.001);
    }

    #[test]
    fn test_merge_stats() {
        let mut stats1 = AggregateStats {
            sum: 10.0,
            count: 4,
            min: Some(1.0),
            max: Some(4.0),
        };
        let stats2 = AggregateStats {
            sum: 15.0,
            count: 3,
            min: Some(0.5),
            max: Some(6.0),
        };

        stats1.merge(&stats2);

        assert_eq!(stats1.count, 7);
        assert!((stats1.sum - 25.0).abs() < 0.001);
        assert!((stats1.min.unwrap() - 0.5).abs() < 0.001);
        assert!((stats1.max.unwrap() - 6.0).abs() < 0.001);
    }

    #[test]
    fn test_filter_eq() {
        let values = vec![1i64, 2, 3, 2, 4, 2, 5];
        let indices = filter_eq_i64(&values, 2);
        assert_eq!(indices, vec![1, 3, 5]);
    }

    #[test]
    fn test_filter_range() {
        let values = vec![1i64, 5, 3, 8, 4, 2, 7];
        let indices = filter_range_i64(&values, 3, 6);
        assert_eq!(indices, vec![1, 2, 4]); // 5, 3, 4 are in [3, 6)
    }

    #[test]
    fn test_apply_mask() {
        let values = vec![1i64, 2, 3, 4, 5];
        let mask = vec![true, false, true, false, true];
        let result = apply_mask_i64(&values, &mask);
        assert_eq!(result, vec![1, 3, 5]);
    }
}
