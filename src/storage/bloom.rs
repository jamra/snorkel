//! Bloom filter for fast membership testing
//!
//! Used to quickly skip shards that definitely don't contain
//! values matching a filter predicate.

use std::hash::{Hash, Hasher};

/// A space-efficient probabilistic data structure for membership testing.
/// False positives are possible, but false negatives are not.
#[derive(Clone)]
pub struct BloomFilter {
    /// Bit array
    bits: Vec<u64>,
    /// Number of hash functions
    num_hashes: u32,
    /// Number of bits
    num_bits: usize,
    /// Number of items inserted
    count: usize,
}

impl BloomFilter {
    /// Create a new bloom filter with target capacity and false positive rate.
    ///
    /// # Arguments
    /// * `expected_items` - Expected number of items to insert
    /// * `false_positive_rate` - Desired false positive rate (e.g., 0.01 for 1%)
    pub fn new(expected_items: usize, false_positive_rate: f64) -> Self {
        // Calculate optimal number of bits: m = -n*ln(p) / (ln(2)^2)
        let ln2_squared = std::f64::consts::LN_2 * std::f64::consts::LN_2;
        let num_bits = (-(expected_items as f64) * false_positive_rate.ln() / ln2_squared)
            .ceil() as usize;
        let num_bits = num_bits.max(64); // Minimum 64 bits

        // Calculate optimal number of hash functions: k = (m/n) * ln(2)
        let num_hashes = ((num_bits as f64 / expected_items as f64) * std::f64::consts::LN_2)
            .ceil() as u32;
        let num_hashes = num_hashes.clamp(1, 16); // Between 1 and 16 hashes

        // Round up to multiple of 64 for efficient storage
        let num_words = (num_bits + 63) / 64;
        let num_bits = num_words * 64;

        Self {
            bits: vec![0u64; num_words],
            num_hashes,
            num_bits,
            count: 0,
        }
    }

    /// Create a bloom filter with specific parameters
    pub fn with_params(num_bits: usize, num_hashes: u32) -> Self {
        let num_words = (num_bits + 63) / 64;
        let num_bits = num_words * 64;

        Self {
            bits: vec![0u64; num_words],
            num_hashes: num_hashes.clamp(1, 16),
            num_bits,
            count: 0,
        }
    }

    /// Insert a value into the bloom filter
    pub fn insert<T: Hash>(&mut self, value: &T) {
        let (h1, h2) = self.hash_pair(value);

        for i in 0..self.num_hashes {
            let idx = self.get_index(h1, h2, i);
            let word = idx / 64;
            let bit = idx % 64;
            self.bits[word] |= 1u64 << bit;
        }

        self.count += 1;
    }

    /// Check if a value might be in the set.
    /// Returns true if the value might be present, false if definitely not present.
    pub fn might_contain<T: Hash>(&self, value: &T) -> bool {
        let (h1, h2) = self.hash_pair(value);

        for i in 0..self.num_hashes {
            let idx = self.get_index(h1, h2, i);
            let word = idx / 64;
            let bit = idx % 64;
            if self.bits[word] & (1u64 << bit) == 0 {
                return false;
            }
        }

        true
    }

    /// Insert a string value
    pub fn insert_str(&mut self, value: &str) {
        self.insert(&value);
    }

    /// Check if a string might be present
    pub fn might_contain_str(&self, value: &str) -> bool {
        self.might_contain(&value)
    }

    /// Insert an i64 value
    pub fn insert_i64(&mut self, value: i64) {
        self.insert(&value);
    }

    /// Check if an i64 might be present
    pub fn might_contain_i64(&self, value: i64) -> bool {
        self.might_contain(&value)
    }

    /// Get the number of items inserted
    pub fn count(&self) -> usize {
        self.count
    }

    /// Get the estimated false positive rate based on current fill
    pub fn estimated_false_positive_rate(&self) -> f64 {
        let bits_set = self.bits.iter().map(|w| w.count_ones() as usize).sum::<usize>();
        let fill_ratio = bits_set as f64 / self.num_bits as f64;
        fill_ratio.powi(self.num_hashes as i32)
    }

    /// Get memory usage in bytes
    pub fn memory_bytes(&self) -> usize {
        self.bits.len() * 8
    }

    /// Merge another bloom filter into this one (union)
    pub fn merge(&mut self, other: &BloomFilter) {
        if self.num_bits == other.num_bits && self.num_hashes == other.num_hashes {
            for (a, b) in self.bits.iter_mut().zip(other.bits.iter()) {
                *a |= *b;
            }
            self.count += other.count;
        }
    }

    /// Clear the bloom filter
    pub fn clear(&mut self) {
        self.bits.fill(0);
        self.count = 0;
    }

    /// Check if the filter is empty
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    // Compute two independent hashes using FxHash
    fn hash_pair<T: Hash>(&self, value: &T) -> (u64, u64) {
        // First hash
        let mut hasher1 = fxhash::FxHasher64::default();
        value.hash(&mut hasher1);
        let h1 = hasher1.finish();

        // Second hash (use h1 as seed variation)
        let mut hasher2 = fxhash::FxHasher64::default();
        h1.hash(&mut hasher2);
        value.hash(&mut hasher2);
        let h2 = hasher2.finish();

        (h1, h2)
    }

    // Get bit index using double hashing: h(i) = h1 + i*h2
    fn get_index(&self, h1: u64, h2: u64, i: u32) -> usize {
        let hash = h1.wrapping_add((i as u64).wrapping_mul(h2));
        (hash as usize) % self.num_bits
    }
}

impl Default for BloomFilter {
    fn default() -> Self {
        // Default: 10000 items, 1% false positive rate
        Self::new(10000, 0.01)
    }
}

impl std::fmt::Debug for BloomFilter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BloomFilter")
            .field("num_bits", &self.num_bits)
            .field("num_hashes", &self.num_hashes)
            .field("count", &self.count)
            .field("memory_bytes", &self.memory_bytes())
            .field("estimated_fpr", &self.estimated_false_positive_rate())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_insert_lookup() {
        let mut bf = BloomFilter::new(100, 0.01);

        bf.insert_str("hello");
        bf.insert_str("world");

        assert!(bf.might_contain_str("hello"));
        assert!(bf.might_contain_str("world"));
        assert!(!bf.might_contain_str("foo")); // Might have false positive, but unlikely
    }

    #[test]
    fn test_i64_values() {
        let mut bf = BloomFilter::new(1000, 0.01);

        for i in 0..100 {
            bf.insert_i64(i * 2); // Insert even numbers
        }

        // All even numbers should be found
        for i in 0..100 {
            assert!(bf.might_contain_i64(i * 2));
        }

        // Most odd numbers should not be found (some false positives possible)
        let mut false_positives = 0;
        for i in 0..100 {
            if bf.might_contain_i64(i * 2 + 1) {
                false_positives += 1;
            }
        }
        // With 1% FPR, expect ~1 false positive out of 100
        assert!(false_positives < 10, "Too many false positives: {}", false_positives);
    }

    #[test]
    fn test_false_positive_rate() {
        let mut bf = BloomFilter::new(10000, 0.01);

        // Insert 10000 items
        for i in 0..10000 {
            bf.insert_i64(i);
        }

        // Check items not inserted
        let mut false_positives = 0;
        let test_count = 10000;
        for i in 10000..(10000 + test_count) {
            if bf.might_contain_i64(i) {
                false_positives += 1;
            }
        }

        let fpr = false_positives as f64 / test_count as f64;
        // Should be close to 1% (allow up to 3%)
        assert!(fpr < 0.03, "False positive rate too high: {:.2}%", fpr * 100.0);
    }

    #[test]
    fn test_merge() {
        let mut bf1 = BloomFilter::new(100, 0.01);
        let mut bf2 = BloomFilter::new(100, 0.01);

        bf1.insert_str("hello");
        bf2.insert_str("world");

        bf1.merge(&bf2);

        assert!(bf1.might_contain_str("hello"));
        assert!(bf1.might_contain_str("world"));
    }

    #[test]
    fn test_clear() {
        let mut bf = BloomFilter::new(100, 0.01);

        bf.insert_str("hello");
        assert!(bf.might_contain_str("hello"));

        bf.clear();
        assert!(!bf.might_contain_str("hello"));
        assert!(bf.is_empty());
    }

    #[test]
    fn test_memory_size() {
        let bf = BloomFilter::new(10000, 0.01);
        // ~12KB for 10000 items at 1% FPR
        assert!(bf.memory_bytes() < 20000);
        assert!(bf.memory_bytes() > 5000);
    }
}
