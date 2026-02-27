//! LZ4 compression wrapper for general-purpose compression
//!
//! Uses LZ4 block compression for sealed shards when other algorithms aren't optimal.

use super::{CompressionError, CompressionType, Compressor};

/// LZ4 compressor
#[derive(Debug, Clone, Default)]
pub struct Lz4Compressor;

impl Lz4Compressor {
    pub fn new() -> Self {
        Self
    }

    /// Compress data using LZ4
    pub fn compress_data(&self, data: &[u8]) -> Vec<u8> {
        if data.is_empty() {
            return vec![];
        }

        // Prepend original size for decompression
        let mut result = Vec::with_capacity(4 + lz4_flex::block::get_maximum_output_size(data.len()));
        result.extend_from_slice(&(data.len() as u32).to_le_bytes());

        let compressed = lz4_flex::compress_prepend_size(data);
        result.extend_from_slice(&compressed);

        result
    }

    /// Decompress LZ4 data
    pub fn decompress_data(&self, data: &[u8]) -> Result<Vec<u8>, CompressionError> {
        if data.is_empty() {
            return Ok(vec![]);
        }

        if data.len() < 4 {
            return Err(CompressionError::InvalidData);
        }

        let _original_size = u32::from_le_bytes(
            data[0..4].try_into().map_err(|_| CompressionError::InvalidData)?
        ) as usize;

        lz4_flex::decompress_size_prepended(&data[4..])
            .map_err(|e| CompressionError::DecompressionFailed(e.to_string()))
    }

    /// Compress a column's raw bytes
    pub fn compress_column(&self, data: &[u8]) -> Vec<u8> {
        self.compress_data(data)
    }

    /// Decompress column bytes
    pub fn decompress_column(&self, data: &[u8]) -> Result<Vec<u8>, CompressionError> {
        self.decompress_data(data)
    }
}

impl Compressor for Lz4Compressor {
    fn compress(&self, data: &[u8]) -> Vec<u8> {
        self.compress_data(data)
    }

    fn decompress(&self, data: &[u8]) -> Result<Vec<u8>, CompressionError> {
        self.decompress_data(data)
    }

    fn compression_type(&self) -> CompressionType {
        CompressionType::Lz4
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lz4_compress_decompress() {
        let compressor = Lz4Compressor::new();
        let data = b"Hello, World! This is some test data for LZ4 compression.";

        let compressed = compressor.compress_data(data);
        let decompressed = compressor.decompress_data(&compressed).unwrap();

        assert_eq!(data.as_slice(), decompressed.as_slice());
    }

    #[test]
    fn test_lz4_repetitive_data() {
        let compressor = Lz4Compressor::new();
        let data: Vec<u8> = vec![0x42; 10000];

        let compressed = compressor.compress_data(&data);
        let decompressed = compressor.decompress_data(&compressed).unwrap();

        assert_eq!(data, decompressed);
        // Highly repetitive data should compress very well
        assert!(compressed.len() < data.len() / 10);
    }

    #[test]
    fn test_lz4_empty() {
        let compressor = Lz4Compressor::new();

        let compressed = compressor.compress_data(&[]);
        let decompressed = compressor.decompress_data(&compressed).unwrap();

        assert!(decompressed.is_empty());
    }

    #[test]
    fn test_lz4_random_data() {
        let compressor = Lz4Compressor::new();
        // Pseudo-random data (not truly random but varies)
        let data: Vec<u8> = (0..1000u32).map(|i| ((i * 17 + 13) % 256) as u8).collect();

        let compressed = compressor.compress_data(&data);
        let decompressed = compressor.decompress_data(&compressed).unwrap();

        assert_eq!(data, decompressed);
    }

    #[test]
    fn test_lz4_trait_impl() {
        let compressor = Lz4Compressor::new();
        let data = b"Testing trait implementation";

        let compressed = compressor.compress(data);
        let decompressed = compressor.decompress(&compressed).unwrap();

        assert_eq!(data.as_slice(), decompressed.as_slice());
        assert_eq!(compressor.compression_type(), CompressionType::Lz4);
    }
}
