//! Bit-packed boolean compression
//!
//! Packs boolean values into bits, achieving 8x compression for dense boolean columns.
//! Handles nullable booleans with a separate null bitmap.

use super::{CompressionError, CompressionType, Compressor};

/// Bit-packed boolean compressor
#[derive(Debug, Clone, Default)]
pub struct BitPackCompressor;

impl BitPackCompressor {
    pub fn new() -> Self {
        Self
    }

    /// Pack boolean values into bytes (8 bools per byte)
    pub fn pack_bools(&self, values: &[bool]) -> Vec<u8> {
        let byte_count = (values.len() + 7) / 8;
        let mut packed = vec![0u8; byte_count];

        for (i, &val) in values.iter().enumerate() {
            if val {
                packed[i / 8] |= 1 << (i % 8);
            }
        }

        packed
    }

    /// Unpack bytes back to boolean values
    pub fn unpack_bools(&self, packed: &[u8], len: usize) -> Vec<bool> {
        let mut result = Vec::with_capacity(len);

        for i in 0..len {
            let byte_idx = i / 8;
            let bit_idx = i % 8;

            if byte_idx < packed.len() {
                result.push((packed[byte_idx] >> bit_idx) & 1 == 1);
            } else {
                result.push(false);
            }
        }

        result
    }

    /// Pack optional booleans (with null handling)
    pub fn pack_optional_bools(&self, values: &[Option<bool>]) -> Vec<u8> {
        if values.is_empty() {
            return vec![];
        }

        let byte_count = (values.len() + 7) / 8;

        // Null bitmap + value bitmap
        let mut result = Vec::with_capacity(byte_count * 2 + 4);

        // Store count
        result.extend_from_slice(&(values.len() as u32).to_le_bytes());

        // Null bitmap
        let mut null_bitmap = vec![0u8; byte_count];
        let mut value_bitmap = vec![0u8; byte_count];

        for (i, val) in values.iter().enumerate() {
            match val {
                None => {
                    null_bitmap[i / 8] |= 1 << (i % 8);
                }
                Some(true) => {
                    value_bitmap[i / 8] |= 1 << (i % 8);
                }
                Some(false) => {
                    // Already 0
                }
            }
        }

        result.extend_from_slice(&null_bitmap);
        result.extend_from_slice(&value_bitmap);

        result
    }

    /// Unpack optional booleans
    pub fn unpack_optional_bools(&self, data: &[u8]) -> Result<Vec<Option<bool>>, CompressionError> {
        if data.len() < 4 {
            return Ok(vec![]);
        }

        let len = u32::from_le_bytes(
            data[0..4].try_into().map_err(|_| CompressionError::InvalidData)?
        ) as usize;

        let byte_count = (len + 7) / 8;

        if data.len() < 4 + byte_count * 2 {
            return Err(CompressionError::InvalidData);
        }

        let null_bitmap = &data[4..4 + byte_count];
        let value_bitmap = &data[4 + byte_count..4 + byte_count * 2];

        let mut result = Vec::with_capacity(len);

        for i in 0..len {
            let is_null = (null_bitmap[i / 8] >> (i % 8)) & 1 == 1;
            if is_null {
                result.push(None);
            } else {
                let value = (value_bitmap[i / 8] >> (i % 8)) & 1 == 1;
                result.push(Some(value));
            }
        }

        Ok(result)
    }
}

impl Compressor for BitPackCompressor {
    fn compress(&self, data: &[u8]) -> Vec<u8> {
        // Interpret each byte as a boolean (0 = false, non-zero = true)
        let bools: Vec<bool> = data.iter().map(|&b| b != 0).collect();
        self.pack_bools(&bools)
    }

    fn decompress(&self, _data: &[u8]) -> Result<Vec<u8>, CompressionError> {
        // Need to know length - for trait impl, we can't determine it
        Err(CompressionError::DecompressionFailed(
            "Use unpack_bools with explicit length".to_string()
        ))
    }

    fn compression_type(&self) -> CompressionType {
        CompressionType::BitPack
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pack_unpack_bools() {
        let compressor = BitPackCompressor::new();
        let values = vec![true, false, true, true, false, false, true, false, true];

        let packed = compressor.pack_bools(&values);
        let unpacked = compressor.unpack_bools(&packed, values.len());

        assert_eq!(values, unpacked);
        // 9 bools should pack into 2 bytes
        assert_eq!(packed.len(), 2);
    }

    #[test]
    fn test_pack_unpack_optional_bools() {
        let compressor = BitPackCompressor::new();
        let values = vec![
            Some(true),
            None,
            Some(false),
            Some(true),
            None,
            Some(false),
        ];

        let packed = compressor.pack_optional_bools(&values);
        let unpacked = compressor.unpack_optional_bools(&packed).unwrap();

        assert_eq!(values, unpacked);
    }

    #[test]
    fn test_all_true() {
        let compressor = BitPackCompressor::new();
        let values = vec![true; 16];

        let packed = compressor.pack_bools(&values);
        let unpacked = compressor.unpack_bools(&packed, values.len());

        assert_eq!(values, unpacked);
        assert_eq!(packed, vec![0xFF, 0xFF]);
    }

    #[test]
    fn test_all_false() {
        let compressor = BitPackCompressor::new();
        let values = vec![false; 16];

        let packed = compressor.pack_bools(&values);
        let unpacked = compressor.unpack_bools(&packed, values.len());

        assert_eq!(values, unpacked);
        assert_eq!(packed, vec![0x00, 0x00]);
    }

    #[test]
    fn test_compression_ratio() {
        let compressor = BitPackCompressor::new();
        let values = vec![true; 1000];

        let packed = compressor.pack_bools(&values);

        // 1000 bools should compress to 125 bytes (8x compression)
        assert_eq!(packed.len(), 125);
    }
}
