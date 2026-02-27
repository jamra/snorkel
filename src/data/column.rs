use super::value::{DataType, Value};
use crate::storage::dictionary::StringDictionary;
use crate::storage::compression::{
    CompressionType, CompressedData,
    bitpack::BitPackCompressor,
    delta::DeltaCompressor,
    rle::RleCompressor,
    lz4::Lz4Compressor,
    select_compression,
};
use std::sync::Arc;

/// Columnar storage for efficient memory usage and cache locality
#[derive(Debug, Clone)]
pub enum Column {
    /// Null column (all values are null)
    Null(usize),
    /// Boolean column
    Bool(Vec<Option<bool>>),
    /// 64-bit integer column
    Int64(Vec<Option<i64>>),
    /// 64-bit floating point column
    Float64(Vec<Option<f64>>),
    /// String column with dictionary encoding
    String {
        /// Dictionary IDs (None = null)
        ids: Vec<Option<u32>>,
        /// Shared dictionary for string interning
        dictionary: Arc<StringDictionary>,
    },
    /// Timestamp column (epoch milliseconds)
    Timestamp(Vec<Option<i64>>),
    /// Compressed column (for sealed shards)
    Compressed {
        /// Original data type
        data_type: DataType,
        /// Compressed data
        data: CompressedData,
        /// Dictionary for string columns (if applicable)
        dictionary: Option<Arc<StringDictionary>>,
    },
}

impl Column {
    pub fn new(data_type: DataType) -> Self {
        match data_type {
            DataType::Null => Column::Null(0),
            DataType::Bool => Column::Bool(Vec::new()),
            DataType::Int64 => Column::Int64(Vec::new()),
            DataType::Float64 => Column::Float64(Vec::new()),
            DataType::String => Column::String {
                ids: Vec::new(),
                dictionary: Arc::new(StringDictionary::new()),
            },
            DataType::Timestamp => Column::Timestamp(Vec::new()),
        }
    }

    pub fn with_capacity(data_type: DataType, capacity: usize) -> Self {
        match data_type {
            DataType::Null => Column::Null(0),
            DataType::Bool => Column::Bool(Vec::with_capacity(capacity)),
            DataType::Int64 => Column::Int64(Vec::with_capacity(capacity)),
            DataType::Float64 => Column::Float64(Vec::with_capacity(capacity)),
            DataType::String => Column::String {
                ids: Vec::with_capacity(capacity),
                dictionary: Arc::new(StringDictionary::new()),
            },
            DataType::Timestamp => Column::Timestamp(Vec::with_capacity(capacity)),
        }
    }

    pub fn data_type(&self) -> DataType {
        match self {
            Column::Null(_) => DataType::Null,
            Column::Bool(_) => DataType::Bool,
            Column::Int64(_) => DataType::Int64,
            Column::Float64(_) => DataType::Float64,
            Column::String { .. } => DataType::String,
            Column::Timestamp(_) => DataType::Timestamp,
            Column::Compressed { data_type, .. } => *data_type,
        }
    }

    pub fn len(&self) -> usize {
        match self {
            Column::Null(n) => *n,
            Column::Bool(v) => v.len(),
            Column::Int64(v) => v.len(),
            Column::Float64(v) => v.len(),
            Column::String { ids, .. } => ids.len(),
            Column::Timestamp(v) => v.len(),
            Column::Compressed { data, .. } => data.len,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Check if this column is compressed
    pub fn is_compressed(&self) -> bool {
        matches!(self, Column::Compressed { .. })
    }

    /// Push a value to the column
    pub fn push(&mut self, value: &Value) {
        match (self, value) {
            (Column::Null(n), _) => *n += 1,
            (Column::Bool(v), Value::Bool(b)) => v.push(Some(*b)),
            (Column::Bool(v), Value::Null) => v.push(None),
            (Column::Int64(v), Value::Int64(i)) => v.push(Some(*i)),
            (Column::Int64(v), Value::Null) => v.push(None),
            (Column::Float64(v), Value::Float64(f)) => v.push(Some(*f)),
            (Column::Float64(v), Value::Int64(i)) => v.push(Some(*i as f64)),
            (Column::Float64(v), Value::Null) => v.push(None),
            (Column::String { ids, dictionary }, Value::String(s)) => {
                let id = dictionary.get_or_insert(s);
                ids.push(Some(id));
            }
            (Column::String { ids, .. }, Value::Null) => ids.push(None),
            (Column::Timestamp(v), Value::Timestamp(t)) => v.push(Some(*t)),
            (Column::Timestamp(v), Value::Int64(i)) => v.push(Some(*i)),
            (Column::Timestamp(v), Value::Null) => v.push(None),
            // Type mismatch: store as null
            (Column::Bool(v), _) => v.push(None),
            (Column::Int64(v), _) => v.push(None),
            (Column::Float64(v), _) => v.push(None),
            (Column::String { ids, .. }, _) => ids.push(None),
            (Column::Timestamp(v), _) => v.push(None),
            // Compressed columns are read-only
            (Column::Compressed { .. }, _) => {
                panic!("Cannot push to compressed column")
            }
        }
    }

    /// Get value at index
    pub fn get(&self, index: usize) -> Value {
        match self {
            Column::Null(_) => Value::Null,
            Column::Bool(v) => v
                .get(index)
                .and_then(|v| *v)
                .map(Value::Bool)
                .unwrap_or(Value::Null),
            Column::Int64(v) => v
                .get(index)
                .and_then(|v| *v)
                .map(Value::Int64)
                .unwrap_or(Value::Null),
            Column::Float64(v) => v
                .get(index)
                .and_then(|v| *v)
                .map(Value::Float64)
                .unwrap_or(Value::Null),
            Column::String { ids, dictionary } => ids
                .get(index)
                .and_then(|id| *id)
                .and_then(|id| dictionary.get_string(id))
                .map(Value::String)
                .unwrap_or(Value::Null),
            Column::Timestamp(v) => v
                .get(index)
                .and_then(|v| *v)
                .map(Value::Timestamp)
                .unwrap_or(Value::Null),
            Column::Compressed { data_type, data, dictionary } => {
                self.get_compressed(index, *data_type, data, dictionary.as_ref())
            }
        }
    }

    /// Get value from compressed column (internal helper)
    fn get_compressed(
        &self,
        index: usize,
        data_type: DataType,
        data: &CompressedData,
        dictionary: Option<&Arc<StringDictionary>>,
    ) -> Value {
        if index >= data.len {
            return Value::Null;
        }

        // Decompress based on algorithm and data type
        match (data.algorithm, data_type) {
            (CompressionType::BitPack, DataType::Bool) => {
                let compressor = BitPackCompressor::new();
                let values = compressor.unpack_optional_bools(&data.data).unwrap_or_default();
                values.get(index).copied().flatten().map(Value::Bool).unwrap_or(Value::Null)
            }
            (CompressionType::Delta, DataType::Int64) |
            (CompressionType::Delta, DataType::Timestamp) => {
                let compressor = DeltaCompressor::new();
                let values = compressor.decode_optional_i64(&data.data, data.len).unwrap_or_default();
                let val = values.get(index).copied().flatten();
                match data_type {
                    DataType::Timestamp => val.map(Value::Timestamp).unwrap_or(Value::Null),
                    _ => val.map(Value::Int64).unwrap_or(Value::Null),
                }
            }
            (CompressionType::Rle, DataType::String) => {
                let compressor = RleCompressor::new();
                let ids = compressor.decode_string_ids(&data.data).unwrap_or_default();
                ids.get(index)
                    .copied()
                    .flatten()
                    .and_then(|id| dictionary.and_then(|d| d.get_string(id)))
                    .map(Value::String)
                    .unwrap_or(Value::Null)
            }
            (CompressionType::Lz4, _) => {
                // LZ4 requires full decompression - cache this in real implementation
                let compressor = Lz4Compressor::new();
                if let Ok(decompressed) = compressor.decompress_data(&data.data) {
                    self.get_from_decompressed(&decompressed, index, data_type, dictionary)
                } else {
                    Value::Null
                }
            }
            _ => Value::Null,
        }
    }

    /// Get value from decompressed bytes
    fn get_from_decompressed(
        &self,
        bytes: &[u8],
        index: usize,
        data_type: DataType,
        dictionary: Option<&Arc<StringDictionary>>,
    ) -> Value {
        match data_type {
            DataType::Bool => {
                if index < bytes.len() {
                    match bytes[index] {
                        0 => Value::Null,
                        1 => Value::Bool(false),
                        2 => Value::Bool(true),
                        _ => Value::Null,
                    }
                } else {
                    Value::Null
                }
            }
            DataType::Int64 | DataType::Timestamp => {
                let offset = index * 9; // 1 byte null flag + 8 bytes value
                if offset + 9 <= bytes.len() {
                    let is_null = bytes[offset] == 1;
                    if is_null {
                        Value::Null
                    } else {
                        let val = i64::from_le_bytes(bytes[offset + 1..offset + 9].try_into().unwrap_or([0; 8]));
                        if data_type == DataType::Timestamp {
                            Value::Timestamp(val)
                        } else {
                            Value::Int64(val)
                        }
                    }
                } else {
                    Value::Null
                }
            }
            DataType::Float64 => {
                let offset = index * 9;
                if offset + 9 <= bytes.len() {
                    let is_null = bytes[offset] == 1;
                    if is_null {
                        Value::Null
                    } else {
                        let val = f64::from_le_bytes(bytes[offset + 1..offset + 9].try_into().unwrap_or([0; 8]));
                        Value::Float64(val)
                    }
                } else {
                    Value::Null
                }
            }
            DataType::String => {
                let offset = index * 5; // 1 byte null + 4 bytes id
                if offset + 5 <= bytes.len() {
                    let is_null = bytes[offset] == 1;
                    if is_null {
                        Value::Null
                    } else {
                        let id = u32::from_le_bytes(bytes[offset + 1..offset + 5].try_into().unwrap_or([0; 4]));
                        dictionary
                            .and_then(|d| d.get_string(id))
                            .map(Value::String)
                            .unwrap_or(Value::Null)
                    }
                } else {
                    Value::Null
                }
            }
            DataType::Null => Value::Null,
        }
    }

    /// Estimate memory usage in bytes
    pub fn memory_usage(&self) -> usize {
        match self {
            Column::Null(_) => std::mem::size_of::<usize>(),
            Column::Bool(v) => v.capacity() * std::mem::size_of::<Option<bool>>(),
            Column::Int64(v) => v.capacity() * std::mem::size_of::<Option<i64>>(),
            Column::Float64(v) => v.capacity() * std::mem::size_of::<Option<f64>>(),
            Column::String { ids, dictionary } => {
                ids.capacity() * std::mem::size_of::<Option<u32>>() + dictionary.memory_usage()
            }
            Column::Timestamp(v) => v.capacity() * std::mem::size_of::<Option<i64>>(),
            Column::Compressed { data, dictionary, .. } => {
                data.memory_usage() + dictionary.as_ref().map(|d| d.memory_usage()).unwrap_or(0)
            }
        }
    }

    /// Compress this column and return a new compressed column
    pub fn compress(&self) -> Column {
        let values: Vec<Value> = self.iter().collect();
        let compression_type = select_compression(&values);

        match (self, compression_type) {
            (Column::Bool(v), CompressionType::BitPack) => {
                let compressor = BitPackCompressor::new();
                let packed = compressor.pack_optional_bools(v);
                let original_size = v.len() * std::mem::size_of::<Option<bool>>();
                Column::Compressed {
                    data_type: DataType::Bool,
                    data: CompressedData::new(CompressionType::BitPack, v.len(), packed, original_size),
                    dictionary: None,
                }
            }
            (Column::Int64(v), CompressionType::Delta) => {
                let compressor = DeltaCompressor::new();
                let encoded = compressor.encode_optional_i64(v);
                let original_size = v.len() * std::mem::size_of::<Option<i64>>();
                Column::Compressed {
                    data_type: DataType::Int64,
                    data: CompressedData::new(CompressionType::Delta, v.len(), encoded, original_size),
                    dictionary: None,
                }
            }
            (Column::Timestamp(v), CompressionType::Delta) => {
                let compressor = DeltaCompressor::new();
                let encoded = compressor.encode_optional_i64(v);
                let original_size = v.len() * std::mem::size_of::<Option<i64>>();
                Column::Compressed {
                    data_type: DataType::Timestamp,
                    data: CompressedData::new(CompressionType::Delta, v.len(), encoded, original_size),
                    dictionary: None,
                }
            }
            (Column::String { ids, dictionary }, CompressionType::Rle) => {
                let compressor = RleCompressor::new();
                let encoded = compressor.encode_string_ids(ids);
                let original_size = ids.len() * std::mem::size_of::<Option<u32>>();
                Column::Compressed {
                    data_type: DataType::String,
                    data: CompressedData::new(CompressionType::Rle, ids.len(), encoded, original_size),
                    dictionary: Some(Arc::clone(dictionary)),
                }
            }
            // Default to LZ4 for other cases
            _ => {
                let bytes = self.to_bytes();
                let original_size = bytes.len();
                let compressor = Lz4Compressor::new();
                let compressed = compressor.compress_data(&bytes);
                Column::Compressed {
                    data_type: self.data_type(),
                    data: CompressedData::new(CompressionType::Lz4, self.len(), compressed, original_size),
                    dictionary: if let Column::String { dictionary, .. } = self {
                        Some(Arc::clone(dictionary))
                    } else {
                        None
                    },
                }
            }
        }
    }

    /// Serialize column to bytes (for LZ4 compression)
    fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        match self {
            Column::Null(n) => {
                bytes.extend_from_slice(&(*n as u64).to_le_bytes());
            }
            Column::Bool(v) => {
                for val in v {
                    match val {
                        None => bytes.push(0),
                        Some(false) => bytes.push(1),
                        Some(true) => bytes.push(2),
                    }
                }
            }
            Column::Int64(v) | Column::Timestamp(v) => {
                for val in v {
                    match val {
                        None => {
                            bytes.push(1); // null flag
                            bytes.extend_from_slice(&0i64.to_le_bytes());
                        }
                        Some(n) => {
                            bytes.push(0);
                            bytes.extend_from_slice(&n.to_le_bytes());
                        }
                    }
                }
            }
            Column::Float64(v) => {
                for val in v {
                    match val {
                        None => {
                            bytes.push(1);
                            bytes.extend_from_slice(&0f64.to_le_bytes());
                        }
                        Some(n) => {
                            bytes.push(0);
                            bytes.extend_from_slice(&n.to_le_bytes());
                        }
                    }
                }
            }
            Column::String { ids, .. } => {
                for id in ids {
                    match id {
                        None => {
                            bytes.push(1);
                            bytes.extend_from_slice(&0u32.to_le_bytes());
                        }
                        Some(n) => {
                            bytes.push(0);
                            bytes.extend_from_slice(&n.to_le_bytes());
                        }
                    }
                }
            }
            Column::Compressed { data, .. } => {
                bytes.extend_from_slice(&data.data);
            }
        }
        bytes
    }

    /// Create an iterator over column values
    pub fn iter(&self) -> ColumnIter<'_> {
        ColumnIter {
            column: self,
            index: 0,
        }
    }
}

pub struct ColumnIter<'a> {
    column: &'a Column,
    index: usize,
}

impl<'a> Iterator for ColumnIter<'a> {
    type Item = Value;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.column.len() {
            return None;
        }
        let value = self.column.get(self.index);
        self.index += 1;
        Some(value)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.column.len() - self.index;
        (remaining, Some(remaining))
    }
}

impl<'a> ExactSizeIterator for ColumnIter<'a> {}

/// Builder for constructing columns incrementally
pub struct ColumnBuilder {
    data_type: DataType,
    column: Column,
}

impl ColumnBuilder {
    pub fn new(data_type: DataType) -> Self {
        Self {
            data_type,
            column: Column::new(data_type),
        }
    }

    pub fn with_capacity(data_type: DataType, capacity: usize) -> Self {
        Self {
            data_type,
            column: Column::with_capacity(data_type, capacity),
        }
    }

    pub fn push(&mut self, value: &Value) {
        self.column.push(value);
    }

    pub fn push_null(&mut self) {
        self.column.push(&Value::Null);
    }

    pub fn len(&self) -> usize {
        self.column.len()
    }

    pub fn is_empty(&self) -> bool {
        self.column.is_empty()
    }

    pub fn data_type(&self) -> DataType {
        self.data_type
    }

    pub fn build(self) -> Column {
        self.column
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_int64_column() {
        let mut col = Column::new(DataType::Int64);
        col.push(&Value::Int64(1));
        col.push(&Value::Int64(2));
        col.push(&Value::Null);
        col.push(&Value::Int64(3));

        assert_eq!(col.len(), 4);
        assert_eq!(col.get(0), Value::Int64(1));
        assert_eq!(col.get(1), Value::Int64(2));
        assert_eq!(col.get(2), Value::Null);
        assert_eq!(col.get(3), Value::Int64(3));
    }

    #[test]
    fn test_string_column_with_dictionary() {
        let mut col = Column::new(DataType::String);
        col.push(&Value::String("hello".into()));
        col.push(&Value::String("world".into()));
        col.push(&Value::String("hello".into())); // Duplicate

        assert_eq!(col.len(), 3);
        assert_eq!(col.get(0), Value::String("hello".into()));
        assert_eq!(col.get(1), Value::String("world".into()));
        assert_eq!(col.get(2), Value::String("hello".into()));

        // Verify dictionary has only 2 unique strings
        if let Column::String { dictionary, .. } = &col {
            assert_eq!(dictionary.len(), 2);
        }
    }

    #[test]
    fn test_column_iterator() {
        let mut col = Column::new(DataType::Int64);
        col.push(&Value::Int64(1));
        col.push(&Value::Int64(2));
        col.push(&Value::Int64(3));

        let values: Vec<Value> = col.iter().collect();
        assert_eq!(values.len(), 3);
        assert_eq!(values[0], Value::Int64(1));
        assert_eq!(values[1], Value::Int64(2));
        assert_eq!(values[2], Value::Int64(3));
    }
}
