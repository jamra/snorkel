//! Compression algorithms for sealed shards
//!
//! Provides multiple compression strategies:
//! - Delta encoding for timestamps and sequential integers
//! - Bit-packing for booleans
//! - Run-length encoding for repetitive data
//! - LZ4 for general-purpose compression of sealed shards

pub mod bitpack;
pub mod delta;
pub mod lz4;
pub mod rle;

use crate::data::Value;

/// Compression algorithm identifier
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum CompressionType {
    /// No compression
    None,
    /// Delta encoding for integers/timestamps
    Delta,
    /// Bit-packed booleans
    BitPack,
    /// Run-length encoding
    Rle,
    /// LZ4 block compression
    Lz4,
}

/// Result of compression operation
#[derive(Debug, Clone)]
pub struct CompressionResult {
    /// Original size in bytes
    pub original_size: usize,
    /// Compressed size in bytes
    pub compressed_size: usize,
    /// Compression algorithm used
    pub algorithm: CompressionType,
}

impl CompressionResult {
    /// Calculate compression ratio (original / compressed)
    pub fn ratio(&self) -> f64 {
        if self.compressed_size == 0 {
            return 1.0;
        }
        self.original_size as f64 / self.compressed_size as f64
    }
}

/// Trait for compressing column data
pub trait Compressor: Send + Sync {
    /// Compress data and return compressed bytes
    fn compress(&self, data: &[u8]) -> Vec<u8>;

    /// Decompress data and return original bytes
    fn decompress(&self, data: &[u8]) -> Result<Vec<u8>, CompressionError>;

    /// Get compression type
    fn compression_type(&self) -> CompressionType;
}

/// Compressed column data
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CompressedData {
    /// Compression algorithm used
    pub algorithm: CompressionType,
    /// Number of elements
    pub len: usize,
    /// Compressed bytes
    pub data: Vec<u8>,
    /// Original uncompressed size (for statistics)
    pub original_size: usize,
}

impl CompressedData {
    pub fn new(algorithm: CompressionType, len: usize, data: Vec<u8>, original_size: usize) -> Self {
        Self {
            algorithm,
            len,
            data,
            original_size,
        }
    }

    /// Memory usage of the compressed data
    pub fn memory_usage(&self) -> usize {
        self.data.len() + std::mem::size_of::<Self>()
    }

    /// Compression ratio achieved
    pub fn compression_ratio(&self) -> f64 {
        if self.data.is_empty() {
            return 1.0;
        }
        self.original_size as f64 / self.data.len() as f64
    }
}

/// Select the best compression algorithm for a column based on data characteristics
pub fn select_compression(values: &[Value]) -> CompressionType {
    if values.is_empty() {
        return CompressionType::None;
    }

    // Check data type from first non-null value
    let first_non_null = values.iter().find(|v| !v.is_null());

    match first_non_null {
        Some(Value::Bool(_)) => CompressionType::BitPack,
        Some(Value::Int64(_)) | Some(Value::Timestamp(_)) => {
            // Check if delta encoding would be beneficial (sequential/sorted data)
            if is_delta_friendly(values) {
                CompressionType::Delta
            } else if has_high_repetition(values) {
                CompressionType::Rle
            } else {
                CompressionType::Lz4
            }
        }
        Some(Value::String(_)) => {
            if has_high_repetition(values) {
                CompressionType::Rle
            } else {
                CompressionType::Lz4
            }
        }
        _ => CompressionType::Lz4,
    }
}

/// Check if values are sequential or sorted (delta-friendly)
fn is_delta_friendly(values: &[Value]) -> bool {
    if values.len() < 2 {
        return false;
    }

    let ints: Vec<i64> = values
        .iter()
        .filter_map(|v| v.as_i64())
        .collect();

    if ints.len() < 2 {
        return false;
    }

    // Calculate average delta magnitude
    let mut delta_sum: i64 = 0;
    let mut max_val: i64 = 0;

    for window in ints.windows(2) {
        let delta = (window[1] - window[0]).abs();
        delta_sum = delta_sum.saturating_add(delta);
        max_val = max_val.max(window[1].abs());
    }

    if max_val == 0 {
        return true;
    }

    let avg_delta = delta_sum / (ints.len() - 1) as i64;
    // Delta encoding is beneficial if average delta is much smaller than values
    avg_delta < max_val / 4
}

/// Check if values have high repetition (RLE-friendly)
fn has_high_repetition(values: &[Value]) -> bool {
    if values.len() < 4 {
        return false;
    }

    let mut run_count = 0;
    let mut total_runs = 0;
    let mut prev: Option<&Value> = None;

    for value in values {
        if Some(value) == prev {
            run_count += 1;
        } else {
            if run_count > 2 {
                total_runs += 1;
            }
            run_count = 1;
            prev = Some(value);
        }
    }

    if run_count > 2 {
        total_runs += 1;
    }

    // High repetition if >20% of data is in runs of 3+
    total_runs * 3 > values.len() / 5
}

#[derive(Debug, thiserror::Error)]
pub enum CompressionError {
    #[error("Failed to compress: {0}")]
    CompressionFailed(String),

    #[error("Failed to decompress: {0}")]
    DecompressionFailed(String),

    #[error("Invalid compressed data")]
    InvalidData,

    #[error("Data too large to compress")]
    DataTooLarge,
}
