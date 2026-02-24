use dashmap::DashMap;
use parking_lot::RwLock;
use std::sync::atomic::{AtomicU32, Ordering};

/// String dictionary for efficient string storage via interning.
/// Maps strings to u32 IDs for compact columnar storage.
#[derive(Debug)]
pub struct StringDictionary {
    /// String to ID mapping (for encoding)
    string_to_id: DashMap<String, u32>,
    /// ID to String mapping (for decoding)
    id_to_string: RwLock<Vec<String>>,
    /// Next available ID
    next_id: AtomicU32,
}

impl StringDictionary {
    pub fn new() -> Self {
        Self {
            string_to_id: DashMap::new(),
            id_to_string: RwLock::new(Vec::new()),
            next_id: AtomicU32::new(0),
        }
    }

    /// Get or insert a string, returning its ID
    pub fn get_or_insert(&self, s: &str) -> u32 {
        // Fast path: check if string already exists
        if let Some(id) = self.string_to_id.get(s) {
            return *id;
        }

        // Slow path: insert new string
        // Use entry API to handle race conditions
        *self
            .string_to_id
            .entry(s.to_string())
            .or_insert_with(|| {
                let id = self.next_id.fetch_add(1, Ordering::SeqCst);
                let mut strings = self.id_to_string.write();
                // Ensure vector is large enough
                if strings.len() <= id as usize {
                    strings.resize(id as usize + 1, String::new());
                }
                strings[id as usize] = s.to_string();
                id
            })
            .value()
    }

    /// Get string by ID
    #[allow(dead_code)]
    pub fn get(&self, _id: u32) -> Option<&str> {
        // This is a bit tricky - we need to return a reference
        // but we can't hold the lock. For now, we'll use a workaround.
        // In practice, we'd use an unsafe cell or different design.
        // For simplicity, we return owned String in the public API.
        None // Placeholder - actual impl below
    }

    /// Get string by ID (returns owned String)
    pub fn get_string(&self, id: u32) -> Option<String> {
        let strings = self.id_to_string.read();
        strings.get(id as usize).cloned()
    }

    /// Look up ID for a string (without inserting)
    pub fn lookup(&self, s: &str) -> Option<u32> {
        self.string_to_id.get(s).map(|r| *r)
    }

    /// Number of unique strings
    pub fn len(&self) -> usize {
        self.string_to_id.len()
    }

    pub fn is_empty(&self) -> bool {
        self.string_to_id.is_empty()
    }

    /// Estimate memory usage in bytes
    pub fn memory_usage(&self) -> usize {
        let strings = self.id_to_string.read();
        let string_bytes: usize = strings.iter().map(|s| s.capacity()).sum();
        let overhead = strings.capacity() * std::mem::size_of::<String>()
            + self.string_to_id.len() * (std::mem::size_of::<String>() + std::mem::size_of::<u32>());
        string_bytes + overhead
    }
}

impl Default for StringDictionary {
    fn default() -> Self {
        Self::new()
    }
}

// We need to update the get method signature since we can't return &str easily
// Let's provide a different interface
impl StringDictionary {
    /// Get string by ID with a closure (avoids lifetime issues)
    pub fn with_string<F, R>(&self, id: u32, f: F) -> Option<R>
    where
        F: FnOnce(&str) -> R,
    {
        let strings = self.id_to_string.read();
        strings.get(id as usize).map(|s| f(s))
    }
}

/// A simpler dictionary that owns all strings and allows &str returns
/// Used for read-heavy workloads after initial loading
#[derive(Debug, Clone)]
pub struct FrozenDictionary {
    strings: Vec<String>,
}

impl FrozenDictionary {
    pub fn from_dictionary(dict: &StringDictionary) -> Self {
        let strings = dict.id_to_string.read().clone();
        Self { strings }
    }

    pub fn get(&self, id: u32) -> Option<&str> {
        self.strings.get(id as usize).map(|s| s.as_str())
    }

    pub fn len(&self) -> usize {
        self.strings.len()
    }

    pub fn is_empty(&self) -> bool {
        self.strings.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_or_insert() {
        let dict = StringDictionary::new();

        let id1 = dict.get_or_insert("hello");
        let id2 = dict.get_or_insert("world");
        let id3 = dict.get_or_insert("hello"); // Duplicate

        assert_eq!(id1, id3); // Same string = same ID
        assert_ne!(id1, id2); // Different strings = different IDs
        assert_eq!(dict.len(), 2);
    }

    #[test]
    fn test_get_string() {
        let dict = StringDictionary::new();

        let id = dict.get_or_insert("test");
        assert_eq!(dict.get_string(id), Some("test".to_string()));
        assert_eq!(dict.get_string(999), None);
    }

    #[test]
    fn test_lookup() {
        let dict = StringDictionary::new();

        assert_eq!(dict.lookup("missing"), None);

        let id = dict.get_or_insert("exists");
        assert_eq!(dict.lookup("exists"), Some(id));
    }

    #[test]
    fn test_concurrent_access() {
        use std::sync::Arc;
        use std::thread;

        let dict = Arc::new(StringDictionary::new());
        let mut handles = vec![];

        for i in 0..10 {
            let dict = Arc::clone(&dict);
            handles.push(thread::spawn(move || {
                for j in 0..100 {
                    dict.get_or_insert(&format!("string_{}_{}", i, j));
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        // Should have 1000 unique strings
        assert_eq!(dict.len(), 1000);
    }
}
