//! Predicate pushdown optimization
//!
//! Builds row masks for filters before scanning full rows,
//! reducing the amount of data that needs to be processed.

use super::parser::FilterOperator;
use super::planner::FilterPlan;
use crate::data::column::Column;
use crate::data::Value;
use std::collections::HashMap;

/// A bitmask representing which rows pass a filter
#[derive(Clone)]
pub struct RowMask {
    /// Bit array where 1 = row passes, 0 = row fails
    bits: Vec<u64>,
    /// Total number of rows
    len: usize,
    /// Count of passing rows (cached for efficiency)
    count: usize,
}

impl RowMask {
    /// Create a mask where all rows pass
    pub fn all_true(len: usize) -> Self {
        let num_words = (len + 63) / 64;
        let mut bits = vec![u64::MAX; num_words];

        // Clear bits beyond len
        if len % 64 != 0 {
            let last_word_bits = len % 64;
            bits[num_words - 1] = (1u64 << last_word_bits) - 1;
        }

        Self {
            bits,
            len,
            count: len,
        }
    }

    /// Create a mask where no rows pass
    pub fn all_false(len: usize) -> Self {
        let num_words = (len + 63) / 64;
        Self {
            bits: vec![0u64; num_words],
            len,
            count: 0,
        }
    }

    /// Check if a specific row passes
    #[inline]
    pub fn get(&self, index: usize) -> bool {
        if index >= self.len {
            return false;
        }
        let word = index / 64;
        let bit = index % 64;
        (self.bits[word] & (1u64 << bit)) != 0
    }

    /// Set a specific row to pass
    #[inline]
    pub fn set(&mut self, index: usize) {
        if index >= self.len {
            return;
        }
        let word = index / 64;
        let bit = index % 64;
        if self.bits[word] & (1u64 << bit) == 0 {
            self.bits[word] |= 1u64 << bit;
            self.count += 1;
        }
    }

    /// Set a specific row to fail
    #[inline]
    pub fn clear(&mut self, index: usize) {
        if index >= self.len {
            return;
        }
        let word = index / 64;
        let bit = index % 64;
        if self.bits[word] & (1u64 << bit) != 0 {
            self.bits[word] &= !(1u64 << bit);
            self.count -= 1;
        }
    }

    /// AND this mask with another (intersection)
    pub fn and(&mut self, other: &RowMask) {
        for (a, b) in self.bits.iter_mut().zip(other.bits.iter()) {
            *a &= *b;
        }
        self.recount();
    }

    /// OR this mask with another (union)
    pub fn or(&mut self, other: &RowMask) {
        for (a, b) in self.bits.iter_mut().zip(other.bits.iter()) {
            *a |= *b;
        }
        self.recount();
    }

    /// NOT this mask (invert)
    pub fn not(&mut self) {
        for word in &mut self.bits {
            *word = !*word;
        }
        // Clear bits beyond len
        if self.len % 64 != 0 {
            let num_words = self.bits.len();
            let last_word_bits = self.len % 64;
            self.bits[num_words - 1] &= (1u64 << last_word_bits) - 1;
        }
        self.recount();
    }

    /// Get count of passing rows
    #[inline]
    pub fn count(&self) -> usize {
        self.count
    }

    /// Check if any rows pass
    #[inline]
    pub fn any(&self) -> bool {
        self.count > 0
    }

    /// Check if no rows pass
    #[inline]
    pub fn none(&self) -> bool {
        self.count == 0
    }

    /// Check if all rows pass
    #[inline]
    pub fn all(&self) -> bool {
        self.count == self.len
    }

    /// Get indices of passing rows
    pub fn indices(&self) -> Vec<usize> {
        let mut result = Vec::with_capacity(self.count);
        for (word_idx, &word) in self.bits.iter().enumerate() {
            if word == 0 {
                continue;
            }
            let base = word_idx * 64;
            let mut w = word;
            while w != 0 {
                let bit = w.trailing_zeros() as usize;
                let idx = base + bit;
                if idx < self.len {
                    result.push(idx);
                }
                w &= w - 1; // Clear lowest set bit
            }
        }
        result
    }

    /// Iterate over passing row indices
    pub fn iter(&self) -> RowMaskIter<'_> {
        RowMaskIter {
            mask: self,
            word_idx: 0,
            bit_idx: 0,
        }
    }

    /// Recount passing rows
    fn recount(&mut self) {
        self.count = self.bits.iter().map(|w| w.count_ones() as usize).sum();
        // Adjust for bits beyond len
        if self.len % 64 != 0 {
            let num_words = self.bits.len();
            let excess_bits = 64 - (self.len % 64);
            let excess_count = (self.bits[num_words - 1] >> (64 - excess_bits)).count_ones() as usize;
            self.count -= excess_count;
        }
    }

    /// Get the length (total number of rows)
    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    /// Check if mask is empty (no rows)
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
}

pub struct RowMaskIter<'a> {
    mask: &'a RowMask,
    word_idx: usize,
    bit_idx: usize,
}

impl<'a> Iterator for RowMaskIter<'a> {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        while self.word_idx < self.mask.bits.len() {
            let word = self.mask.bits[self.word_idx];
            while self.bit_idx < 64 {
                let idx = self.word_idx * 64 + self.bit_idx;
                self.bit_idx += 1;
                if idx >= self.mask.len {
                    return None;
                }
                if word & (1u64 << (self.bit_idx - 1)) != 0 {
                    return Some(idx);
                }
            }
            self.word_idx += 1;
            self.bit_idx = 0;
        }
        None
    }
}

/// Build a row mask for a single filter on a column
pub fn build_filter_mask(
    column: &Column,
    filter: &FilterPlan,
    row_count: usize,
) -> RowMask {
    let mut mask = RowMask::all_false(row_count);

    // Fast path for i64 columns with equality filters
    if matches!(filter.operator, FilterOperator::Eq) {
        if let Some(slice) = column.as_i64_slice() {
            if let Some(target) = filter.value.as_i64() {
                for (i, val) in slice.iter().enumerate() {
                    if val == &Some(target) {
                        mask.set(i);
                    }
                }
                return mask;
            }
        }
    }

    // General path
    for i in 0..row_count {
        let value = column.get(i);
        if evaluate_filter(&value, filter) {
            mask.set(i);
        }
    }

    mask
}

/// Evaluate a filter against a value
#[inline]
fn evaluate_filter(value: &Value, filter: &FilterPlan) -> bool {
    match filter.operator {
        FilterOperator::Eq => value == &filter.value,
        FilterOperator::NotEq => value != &filter.value,
        FilterOperator::Lt => value < &filter.value,
        FilterOperator::LtEq => value <= &filter.value,
        FilterOperator::Gt => value > &filter.value,
        FilterOperator::GtEq => value >= &filter.value,
        FilterOperator::Like => {
            if let (Value::String(s), Value::String(pattern)) = (value, &filter.value) {
                like_match(s, pattern)
            } else {
                false
            }
        }
    }
}

fn like_match(s: &str, pattern: &str) -> bool {
    let pattern = pattern.replace('%', ".*").replace('_', ".");
    regex::Regex::new(&format!("^{}$", pattern))
        .map(|re| re.is_match(s))
        .unwrap_or(false)
}

/// Build a combined mask for all filters (AND logic)
pub fn build_combined_mask(
    columns: &HashMap<String, Column>,
    filters: &[FilterPlan],
    row_count: usize,
) -> RowMask {
    if filters.is_empty() {
        return RowMask::all_true(row_count);
    }

    let mut result: Option<RowMask> = None;

    for filter in filters {
        if let Some(column) = columns.get(&filter.column) {
            let mask = build_filter_mask(column, filter, row_count);

            result = Some(match result {
                Some(mut r) => {
                    r.and(&mask);
                    r
                }
                None => mask,
            });
        } else {
            // Column not found - no rows match
            return RowMask::all_false(row_count);
        }
    }

    result.unwrap_or_else(|| RowMask::all_true(row_count))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_row_mask_all_true() {
        let mask = RowMask::all_true(100);
        assert_eq!(mask.count(), 100);
        assert!(mask.all());
        assert!(mask.get(0));
        assert!(mask.get(99));
    }

    #[test]
    fn test_row_mask_all_false() {
        let mask = RowMask::all_false(100);
        assert_eq!(mask.count(), 0);
        assert!(mask.none());
        assert!(!mask.get(0));
        assert!(!mask.get(99));
    }

    #[test]
    fn test_row_mask_set_clear() {
        let mut mask = RowMask::all_false(100);

        mask.set(5);
        mask.set(10);
        mask.set(95);

        assert_eq!(mask.count(), 3);
        assert!(mask.get(5));
        assert!(mask.get(10));
        assert!(mask.get(95));
        assert!(!mask.get(0));

        mask.clear(10);
        assert_eq!(mask.count(), 2);
        assert!(!mask.get(10));
    }

    #[test]
    fn test_row_mask_and() {
        let mut mask1 = RowMask::all_false(10);
        mask1.set(1);
        mask1.set(2);
        mask1.set(3);

        let mut mask2 = RowMask::all_false(10);
        mask2.set(2);
        mask2.set(3);
        mask2.set(4);

        mask1.and(&mask2);
        assert_eq!(mask1.count(), 2); // 2 and 3
        assert!(!mask1.get(1));
        assert!(mask1.get(2));
        assert!(mask1.get(3));
        assert!(!mask1.get(4));
    }

    #[test]
    fn test_row_mask_indices() {
        let mut mask = RowMask::all_false(100);
        mask.set(5);
        mask.set(10);
        mask.set(95);

        let indices = mask.indices();
        assert_eq!(indices, vec![5, 10, 95]);
    }

    #[test]
    fn test_row_mask_iter() {
        let mut mask = RowMask::all_false(100);
        mask.set(1);
        mask.set(5);
        mask.set(10);

        let indices: Vec<usize> = mask.iter().collect();
        assert_eq!(indices, vec![1, 5, 10]);
    }

    #[test]
    fn test_row_mask_large() {
        let mut mask = RowMask::all_true(10000);
        assert_eq!(mask.count(), 10000);

        for i in (0..10000).step_by(2) {
            mask.clear(i);
        }
        assert_eq!(mask.count(), 5000);

        let indices = mask.indices();
        assert_eq!(indices.len(), 5000);
        assert!(indices.iter().all(|&i| i % 2 == 1));
    }
}
