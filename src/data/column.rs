use super::value::{DataType, Value};
use crate::storage::dictionary::StringDictionary;
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
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
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
        }
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
