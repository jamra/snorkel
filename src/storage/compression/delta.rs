//! Delta encoding for i64 values and timestamps
//!
//! Stores the first value followed by deltas (differences) between consecutive values.
//! Effective for sequential or sorted data where deltas are small.

use super::{CompressionError, CompressionType, Compressor};

/// Delta encoder for i64 values
#[derive(Debug, Clone, Default)]
pub struct DeltaCompressor;

impl DeltaCompressor {
    pub fn new() -> Self {
        Self
    }

    /// Encode a slice of i64 values using delta encoding
    pub fn encode_i64(&self, values: &[i64]) -> Vec<u8> {
        if values.is_empty() {
            return vec![];
        }

        let mut result = Vec::with_capacity(values.len() * 4); // Estimate

        // Store first value as full 8 bytes
        result.extend_from_slice(&values[0].to_le_bytes());

        // Store deltas using variable-length encoding
        let mut prev = values[0];
        for &val in values.iter().skip(1) {
            let delta = val.wrapping_sub(prev);
            self.encode_varint(&mut result, delta);
            prev = val;
        }

        result
    }

    /// Decode delta-encoded data back to i64 values
    pub fn decode_i64(&self, data: &[u8], len: usize) -> Result<Vec<i64>, CompressionError> {
        if data.is_empty() || len == 0 {
            return Ok(vec![]);
        }

        if data.len() < 8 {
            return Err(CompressionError::InvalidData);
        }

        let mut result = Vec::with_capacity(len);

        // Read first value
        let first = i64::from_le_bytes(data[0..8].try_into().map_err(|_| CompressionError::InvalidData)?);
        result.push(first);

        // Read deltas
        let mut pos = 8;
        let mut prev = first;

        while result.len() < len && pos < data.len() {
            let (delta, bytes_read) = self.decode_varint(&data[pos..])?;
            pos += bytes_read;
            prev = prev.wrapping_add(delta);
            result.push(prev);
        }

        Ok(result)
    }

    /// Encode a slice of optional i64 values (with null handling)
    pub fn encode_optional_i64(&self, values: &[Option<i64>]) -> Vec<u8> {
        if values.is_empty() {
            return vec![];
        }

        let mut result = Vec::with_capacity(values.len() * 5);

        // First, encode null bitmap
        let null_bitmap = self.encode_null_bitmap(values);
        result.extend_from_slice(&(null_bitmap.len() as u32).to_le_bytes());
        result.extend_from_slice(&null_bitmap);

        // Then encode non-null values with delta
        let non_null_values: Vec<i64> = values.iter().filter_map(|v| *v).collect();
        let delta_encoded = self.encode_i64(&non_null_values);
        result.extend_from_slice(&delta_encoded);

        result
    }

    /// Decode optional i64 values
    pub fn decode_optional_i64(&self, data: &[u8], len: usize) -> Result<Vec<Option<i64>>, CompressionError> {
        if data.is_empty() || len == 0 {
            return Ok(vec![]);
        }

        if data.len() < 4 {
            return Err(CompressionError::InvalidData);
        }

        // Read null bitmap length
        let bitmap_len = u32::from_le_bytes(
            data[0..4].try_into().map_err(|_| CompressionError::InvalidData)?
        ) as usize;

        if data.len() < 4 + bitmap_len {
            return Err(CompressionError::InvalidData);
        }

        // Decode null bitmap
        let null_bitmap = &data[4..4 + bitmap_len];
        let non_null_count = self.count_non_nulls(null_bitmap, len);

        // Decode values
        let values = self.decode_i64(&data[4 + bitmap_len..], non_null_count)?;

        // Reconstruct with nulls
        let mut result = Vec::with_capacity(len);
        let mut value_idx = 0;

        for i in 0..len {
            if self.is_null(null_bitmap, i) {
                result.push(None);
            } else {
                result.push(Some(values[value_idx]));
                value_idx += 1;
            }
        }

        Ok(result)
    }

    // Variable-length integer encoding (zigzag + varint)
    fn encode_varint(&self, out: &mut Vec<u8>, value: i64) {
        // Zigzag encode to handle negative numbers efficiently
        let zigzag = ((value << 1) ^ (value >> 63)) as u64;

        let mut v = zigzag;
        loop {
            if v < 0x80 {
                out.push(v as u8);
                break;
            } else {
                out.push((v as u8) | 0x80);
                v >>= 7;
            }
        }
    }

    fn decode_varint(&self, data: &[u8]) -> Result<(i64, usize), CompressionError> {
        let mut result: u64 = 0;
        let mut shift = 0;
        let mut pos = 0;

        for &byte in data.iter() {
            pos += 1;
            result |= ((byte & 0x7F) as u64) << shift;

            if byte & 0x80 == 0 {
                // Zigzag decode
                let decoded = ((result >> 1) as i64) ^ -((result & 1) as i64);
                return Ok((decoded, pos));
            }

            shift += 7;
            if shift >= 64 {
                return Err(CompressionError::InvalidData);
            }
        }

        Err(CompressionError::InvalidData)
    }

    fn encode_null_bitmap(&self, values: &[Option<i64>]) -> Vec<u8> {
        let byte_count = (values.len() + 7) / 8;
        let mut bitmap = vec![0u8; byte_count];

        for (i, val) in values.iter().enumerate() {
            if val.is_none() {
                bitmap[i / 8] |= 1 << (i % 8);
            }
        }

        bitmap
    }

    fn is_null(&self, bitmap: &[u8], index: usize) -> bool {
        if index / 8 >= bitmap.len() {
            return false;
        }
        (bitmap[index / 8] >> (index % 8)) & 1 == 1
    }

    fn count_non_nulls(&self, bitmap: &[u8], total: usize) -> usize {
        let mut count = 0;
        for i in 0..total {
            if !self.is_null(bitmap, i) {
                count += 1;
            }
        }
        count
    }
}

impl Compressor for DeltaCompressor {
    fn compress(&self, data: &[u8]) -> Vec<u8> {
        // For raw bytes, interpret as i64 slice
        if data.len() % 8 != 0 {
            return data.to_vec(); // Can't delta-encode
        }

        let values: Vec<i64> = data
            .chunks_exact(8)
            .map(|chunk| i64::from_le_bytes(chunk.try_into().unwrap()))
            .collect();

        self.encode_i64(&values)
    }

    fn decompress(&self, _data: &[u8]) -> Result<Vec<u8>, CompressionError> {
        // We need to know the expected length, but for trait impl we estimate
        // In practice, use decode_i64 directly with known length
        Err(CompressionError::DecompressionFailed(
            "Use decode_i64 with explicit length".to_string()
        ))
    }

    fn compression_type(&self) -> CompressionType {
        CompressionType::Delta
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_delta_encode_decode() {
        let compressor = DeltaCompressor::new();
        let values = vec![100i64, 101, 103, 106, 110, 115, 121];

        let encoded = compressor.encode_i64(&values);
        let decoded = compressor.decode_i64(&encoded, values.len()).unwrap();

        assert_eq!(values, decoded);
    }

    #[test]
    fn test_delta_timestamps() {
        let compressor = DeltaCompressor::new();
        // Simulated millisecond timestamps
        let values: Vec<i64> = (0..100).map(|i| 1700000000000 + i * 1000).collect();

        let encoded = compressor.encode_i64(&values);
        let decoded = compressor.decode_i64(&encoded, values.len()).unwrap();

        assert_eq!(values, decoded);
        // Delta encoding should be very compact for sequential timestamps
        assert!(encoded.len() < values.len() * 8);
    }

    #[test]
    fn test_delta_with_nulls() {
        let compressor = DeltaCompressor::new();
        let values = vec![Some(100i64), None, Some(102), Some(103), None, Some(105)];

        let encoded = compressor.encode_optional_i64(&values);
        let decoded = compressor.decode_optional_i64(&encoded, values.len()).unwrap();

        assert_eq!(values, decoded);
    }

    #[test]
    fn test_negative_deltas() {
        let compressor = DeltaCompressor::new();
        let values = vec![100i64, 90, 95, 80, 85];

        let encoded = compressor.encode_i64(&values);
        let decoded = compressor.decode_i64(&encoded, values.len()).unwrap();

        assert_eq!(values, decoded);
    }
}
