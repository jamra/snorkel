use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::hash::{Hash, Hasher};

/// Core value types supported by Snorkel
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Value {
    Null,
    Bool(bool),
    Int64(i64),
    Float64(f64),
    String(String),
    Timestamp(i64),
}

impl Value {
    pub fn type_name(&self) -> &'static str {
        match self {
            Value::Null => "null",
            Value::Bool(_) => "bool",
            Value::Int64(_) => "int64",
            Value::Float64(_) => "float64",
            Value::String(_) => "string",
            Value::Timestamp(_) => "timestamp",
        }
    }

    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Value::Int64(v) => Some(*v),
            Value::Timestamp(v) => Some(*v),
            Value::Float64(v) => Some(*v as i64),
            _ => None,
        }
    }

    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Value::Float64(v) => Some(*v),
            Value::Int64(v) => Some(*v as f64),
            Value::Timestamp(v) => Some(*v as f64),
            _ => None,
        }
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::String(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Value::Bool(b) => Some(*b),
            _ => None,
        }
    }

    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    /// Infer column type from a JSON value
    pub fn from_json(json: &serde_json::Value, column_name: &str) -> Self {
        match json {
            serde_json::Value::Null => Value::Null,
            serde_json::Value::Bool(b) => Value::Bool(*b),
            serde_json::Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    if column_name == "timestamp" {
                        Value::Timestamp(i)
                    } else {
                        Value::Int64(i)
                    }
                } else if let Some(f) = n.as_f64() {
                    Value::Float64(f)
                } else {
                    Value::Null
                }
            }
            serde_json::Value::String(s) => Value::String(s.clone()),
            serde_json::Value::Array(_) | serde_json::Value::Object(_) => Value::Null,
        }
    }
}

/// Flatten a JSON object into a HashMap with dot-notation keys
///
/// Example:
/// ```json
/// {"user": {"name": "alice", "age": 30}, "tags": ["a", "b"]}
/// ```
/// Becomes:
/// - "user.name" -> "alice"
/// - "user.age" -> 30
/// - "tags.0" -> "a"
/// - "tags.1" -> "b"
pub fn flatten_json(json: &serde_json::Map<String, serde_json::Value>) -> std::collections::HashMap<String, Value> {
    let mut result = std::collections::HashMap::new();
    flatten_json_recursive(json, "", &mut result);
    result
}

fn flatten_json_recursive(
    obj: &serde_json::Map<String, serde_json::Value>,
    prefix: &str,
    result: &mut std::collections::HashMap<String, Value>,
) {
    for (key, value) in obj {
        let full_key = if prefix.is_empty() {
            key.clone()
        } else {
            format!("{}.{}", prefix, key)
        };

        match value {
            serde_json::Value::Object(nested) => {
                flatten_json_recursive(nested, &full_key, result);
            }
            serde_json::Value::Array(arr) => {
                flatten_array(arr, &full_key, result);
            }
            _ => {
                result.insert(full_key.clone(), Value::from_json(value, &full_key));
            }
        }
    }
}

fn flatten_array(
    arr: &[serde_json::Value],
    prefix: &str,
    result: &mut std::collections::HashMap<String, Value>,
) {
    for (idx, value) in arr.iter().enumerate() {
        let full_key = format!("{}.{}", prefix, idx);

        match value {
            serde_json::Value::Object(nested) => {
                flatten_json_recursive(nested, &full_key, result);
            }
            serde_json::Value::Array(nested_arr) => {
                flatten_array(nested_arr, &full_key, result);
            }
            _ => {
                result.insert(full_key.clone(), Value::from_json(value, &full_key));
            }
        }
    }
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Value::Null, Value::Null) => true,
            (Value::Bool(a), Value::Bool(b)) => a == b,
            (Value::Int64(a), Value::Int64(b)) => a == b,
            (Value::Float64(a), Value::Float64(b)) => a.to_bits() == b.to_bits(),
            (Value::String(a), Value::String(b)) => a == b,
            (Value::Timestamp(a), Value::Timestamp(b)) => a == b,
            // Cross-type numeric comparisons
            (Value::Int64(a), Value::Float64(b)) => (*a as f64).to_bits() == b.to_bits(),
            (Value::Float64(a), Value::Int64(b)) => a.to_bits() == (*b as f64).to_bits(),
            (Value::Int64(a), Value::Timestamp(b)) => a == b,
            (Value::Timestamp(a), Value::Int64(b)) => a == b,
            _ => false,
        }
    }
}

impl Eq for Value {}

impl Hash for Value {
    fn hash<H: Hasher>(&self, state: &mut H) {
        std::mem::discriminant(self).hash(state);
        match self {
            Value::Null => {}
            Value::Bool(b) => b.hash(state),
            Value::Int64(i) => i.hash(state),
            Value::Float64(f) => f.to_bits().hash(state),
            Value::String(s) => s.hash(state),
            Value::Timestamp(t) => t.hash(state),
        }
    }
}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Value {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (Value::Null, Value::Null) => Ordering::Equal,
            (Value::Null, _) => Ordering::Less,
            (_, Value::Null) => Ordering::Greater,
            (Value::Bool(a), Value::Bool(b)) => a.cmp(b),
            (Value::Int64(a), Value::Int64(b)) => a.cmp(b),
            (Value::Float64(a), Value::Float64(b)) => a.partial_cmp(b).unwrap_or(Ordering::Equal),
            (Value::String(a), Value::String(b)) => a.cmp(b),
            (Value::Timestamp(a), Value::Timestamp(b)) => a.cmp(b),
            (Value::Int64(a), Value::Float64(b)) => {
                (*a as f64).partial_cmp(b).unwrap_or(Ordering::Equal)
            }
            (Value::Float64(a), Value::Int64(b)) => {
                a.partial_cmp(&(*b as f64)).unwrap_or(Ordering::Equal)
            }
            (Value::Int64(a), Value::Timestamp(b)) => a.cmp(b),
            (Value::Timestamp(a), Value::Int64(b)) => a.cmp(b),
            // Different types: order by type discriminant
            _ => self.type_order().cmp(&other.type_order()),
        }
    }
}

impl Value {
    /// Get a numeric order for type comparison
    fn type_order(&self) -> u8 {
        match self {
            Value::Null => 0,
            Value::Bool(_) => 1,
            Value::Int64(_) => 2,
            Value::Float64(_) => 3,
            Value::String(_) => 4,
            Value::Timestamp(_) => 5,
        }
    }
}

impl Default for Value {
    fn default() -> Self {
        Value::Null
    }
}

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Null => write!(f, "NULL"),
            Value::Bool(b) => write!(f, "{}", b),
            Value::Int64(i) => write!(f, "{}", i),
            Value::Float64(v) => write!(f, "{}", v),
            Value::String(s) => write!(f, "{}", s),
            Value::Timestamp(t) => write!(f, "{}", t),
        }
    }
}

/// Column data type for schema
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DataType {
    Null,
    Bool,
    Int64,
    Float64,
    String,
    Timestamp,
}

impl DataType {
    pub fn from_value(value: &Value) -> Self {
        match value {
            Value::Null => DataType::Null,
            Value::Bool(_) => DataType::Bool,
            Value::Int64(_) => DataType::Int64,
            Value::Float64(_) => DataType::Float64,
            Value::String(_) => DataType::String,
            Value::Timestamp(_) => DataType::Timestamp,
        }
    }

    /// Determine the best type when merging two types
    pub fn merge(&self, other: &DataType) -> DataType {
        if self == other {
            return *self;
        }
        match (self, other) {
            (DataType::Null, t) | (t, DataType::Null) => *t,
            (DataType::Int64, DataType::Float64) | (DataType::Float64, DataType::Int64) => {
                DataType::Float64
            }
            (DataType::Int64, DataType::Timestamp) | (DataType::Timestamp, DataType::Int64) => {
                DataType::Int64
            }
            // Default to string for incompatible types
            _ => DataType::String,
        }
    }
}

impl std::fmt::Display for DataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DataType::Null => write!(f, "NULL"),
            DataType::Bool => write!(f, "BOOL"),
            DataType::Int64 => write!(f, "INT64"),
            DataType::Float64 => write!(f, "FLOAT64"),
            DataType::String => write!(f, "STRING"),
            DataType::Timestamp => write!(f, "TIMESTAMP"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_value_type_inference() {
        let json = serde_json::json!(42);
        assert!(matches!(Value::from_json(&json, "x"), Value::Int64(42)));

        let json = serde_json::json!(3.14);
        assert!(matches!(Value::from_json(&json, "x"), Value::Float64(_)));

        let json = serde_json::json!("hello");
        assert!(matches!(Value::from_json(&json, "x"), Value::String(_)));

        let json = serde_json::json!(true);
        assert!(matches!(Value::from_json(&json, "x"), Value::Bool(true)));
    }

    #[test]
    fn test_timestamp_inference() {
        let json = serde_json::json!(1234567890);
        assert!(matches!(
            Value::from_json(&json, "timestamp"),
            Value::Timestamp(1234567890)
        ));
    }

    #[test]
    fn test_value_ordering() {
        assert!(Value::Int64(1) < Value::Int64(2));
        assert!(Value::String("a".into()) < Value::String("b".into()));
        assert!(Value::Null < Value::Int64(0));
    }
}
