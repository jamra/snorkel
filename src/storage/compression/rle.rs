//! Run-length encoding for repetitive data
//!
//! Encodes runs of identical values as (value, count) pairs.
//! Effective for columns with many repeated values (e.g., status codes, categories).

use super::{CompressionError, CompressionType, Compressor};

/// Run-length encoder
#[derive(Debug, Clone, Default)]
pub struct RleCompressor;

/// A run of identical values
#[derive(Debug, Clone)]
pub struct Run<T> {
    pub value: T,
    pub count: u32,
}

impl RleCompressor {
    pub fn new() -> Self {
        Self
    }

    /// Encode i64 values using RLE
    pub fn encode_i64(&self, values: &[i64]) -> Vec<u8> {
        if values.is_empty() {
            return vec![];
        }

        let mut result = Vec::new();
        let mut run_value = values[0];
        let mut run_count: u32 = 1;

        for &val in values.iter().skip(1) {
            if val == run_value && run_count < u32::MAX {
                run_count += 1;
            } else {
                // Write run
                result.extend_from_slice(&run_value.to_le_bytes());
                result.extend_from_slice(&run_count.to_le_bytes());
                run_value = val;
                run_count = 1;
            }
        }

        // Write final run
        result.extend_from_slice(&run_value.to_le_bytes());
        result.extend_from_slice(&run_count.to_le_bytes());

        result
    }

    /// Decode RLE-encoded i64 values
    pub fn decode_i64(&self, data: &[u8]) -> Result<Vec<i64>, CompressionError> {
        if data.is_empty() {
            return Ok(vec![]);
        }

        if data.len() % 12 != 0 {
            return Err(CompressionError::InvalidData);
        }

        let mut result = Vec::new();

        for chunk in data.chunks_exact(12) {
            let value = i64::from_le_bytes(
                chunk[0..8].try_into().map_err(|_| CompressionError::InvalidData)?
            );
            let count = u32::from_le_bytes(
                chunk[8..12].try_into().map_err(|_| CompressionError::InvalidData)?
            );

            for _ in 0..count {
                result.push(value);
            }
        }

        Ok(result)
    }

    /// Encode string IDs (u32) using RLE - useful for dictionary-encoded strings
    pub fn encode_string_ids(&self, ids: &[Option<u32>]) -> Vec<u8> {
        if ids.is_empty() {
            return vec![];
        }

        let mut result = Vec::new();
        let mut run_value = ids[0];
        let mut run_count: u32 = 1;

        for &id in ids.iter().skip(1) {
            if id == run_value && run_count < u32::MAX {
                run_count += 1;
            } else {
                self.write_optional_u32_run(&mut result, run_value, run_count);
                run_value = id;
                run_count = 1;
            }
        }

        self.write_optional_u32_run(&mut result, run_value, run_count);

        result
    }

    /// Decode RLE-encoded string IDs
    pub fn decode_string_ids(&self, data: &[u8]) -> Result<Vec<Option<u32>>, CompressionError> {
        if data.is_empty() {
            return Ok(vec![]);
        }

        let mut result = Vec::new();
        let mut pos = 0;

        while pos < data.len() {
            if data.len() - pos < 5 {
                return Err(CompressionError::InvalidData);
            }

            let is_null = data[pos] == 1;
            pos += 1;

            let value_or_count = u32::from_le_bytes(
                data[pos..pos + 4].try_into().map_err(|_| CompressionError::InvalidData)?
            );
            pos += 4;

            if is_null {
                // value_or_count is the count of nulls
                for _ in 0..value_or_count {
                    result.push(None);
                }
            } else {
                // Read count separately
                if data.len() - pos < 4 {
                    return Err(CompressionError::InvalidData);
                }
                let count = u32::from_le_bytes(
                    data[pos..pos + 4].try_into().map_err(|_| CompressionError::InvalidData)?
                );
                pos += 4;

                for _ in 0..count {
                    result.push(Some(value_or_count));
                }
            }
        }

        Ok(result)
    }

    fn write_optional_u32_run(&self, out: &mut Vec<u8>, value: Option<u32>, count: u32) {
        match value {
            None => {
                out.push(1); // null marker
                out.extend_from_slice(&count.to_le_bytes());
            }
            Some(v) => {
                out.push(0); // not null
                out.extend_from_slice(&v.to_le_bytes());
                out.extend_from_slice(&count.to_le_bytes());
            }
        }
    }

    /// Encode bytes using RLE
    pub fn encode_bytes(&self, data: &[u8]) -> Vec<u8> {
        if data.is_empty() {
            return vec![];
        }

        let mut result = Vec::new();
        let mut run_value = data[0];
        let mut run_count: u16 = 1;

        for &byte in data.iter().skip(1) {
            if byte == run_value && run_count < u16::MAX {
                run_count += 1;
            } else {
                result.push(run_value);
                result.extend_from_slice(&run_count.to_le_bytes());
                run_value = byte;
                run_count = 1;
            }
        }

        result.push(run_value);
        result.extend_from_slice(&run_count.to_le_bytes());

        result
    }

    /// Decode RLE-encoded bytes
    pub fn decode_bytes(&self, data: &[u8]) -> Result<Vec<u8>, CompressionError> {
        if data.is_empty() {
            return Ok(vec![]);
        }

        if data.len() % 3 != 0 {
            return Err(CompressionError::InvalidData);
        }

        let mut result = Vec::new();

        for chunk in data.chunks_exact(3) {
            let value = chunk[0];
            let count = u16::from_le_bytes([chunk[1], chunk[2]]);

            for _ in 0..count {
                result.push(value);
            }
        }

        Ok(result)
    }
}

impl Compressor for RleCompressor {
    fn compress(&self, data: &[u8]) -> Vec<u8> {
        self.encode_bytes(data)
    }

    fn decompress(&self, data: &[u8]) -> Result<Vec<u8>, CompressionError> {
        self.decode_bytes(data)
    }

    fn compression_type(&self) -> CompressionType {
        CompressionType::Rle
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rle_i64() {
        let compressor = RleCompressor::new();
        let values = vec![1i64, 1, 1, 2, 2, 3, 3, 3, 3];

        let encoded = compressor.encode_i64(&values);
        let decoded = compressor.decode_i64(&encoded).unwrap();

        assert_eq!(values, decoded);
    }

    #[test]
    fn test_rle_high_repetition() {
        let compressor = RleCompressor::new();
        let values = vec![42i64; 1000];

        let encoded = compressor.encode_i64(&values);
        let decoded = compressor.decode_i64(&encoded).unwrap();

        assert_eq!(values, decoded);
        // 1000 identical values should compress to just 12 bytes (value + count)
        assert_eq!(encoded.len(), 12);
    }

    #[test]
    fn test_rle_string_ids() {
        let compressor = RleCompressor::new();
        let ids = vec![
            Some(1u32), Some(1), Some(1),
            None, None,
            Some(2), Some(2),
            None,
            Some(3),
        ];

        let encoded = compressor.encode_string_ids(&ids);
        let decoded = compressor.decode_string_ids(&encoded).unwrap();

        assert_eq!(ids, decoded);
    }

    #[test]
    fn test_rle_bytes() {
        let compressor = RleCompressor::new();
        let data = vec![0u8, 0, 0, 1, 1, 2, 2, 2, 2];

        let encoded = compressor.encode_bytes(&data);
        let decoded = compressor.decode_bytes(&encoded).unwrap();

        assert_eq!(data, decoded);
    }

    #[test]
    fn test_rle_no_repetition() {
        let compressor = RleCompressor::new();
        let values: Vec<i64> = (0..10).collect();

        let encoded = compressor.encode_i64(&values);
        let decoded = compressor.decode_i64(&encoded).unwrap();

        assert_eq!(values, decoded);
        // No compression benefit without repetition
        assert_eq!(encoded.len(), 10 * 12); // 10 runs of 1
    }
}
