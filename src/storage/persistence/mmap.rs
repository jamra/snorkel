//! Memory-mapped file backend for fast persistence

use super::{PersistenceBackend, PersistenceConfig, PersistenceError};
use memmap2::{MmapMut, MmapOptions};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::PathBuf;

/// Memory-mapped file backend
pub struct MmapBackend {
    config: PersistenceConfig,
    /// Cached memory maps
    maps: RwLock<HashMap<String, MmapEntry>>,
}

struct MmapEntry {
    #[allow(dead_code)]
    file: File,
    mmap: MmapMut,
    size: usize,
}

impl MmapBackend {
    /// Create a new mmap backend
    pub fn new(config: PersistenceConfig) -> Result<Self, PersistenceError> {
        std::fs::create_dir_all(&config.data_dir)?;

        Ok(Self {
            config,
            maps: RwLock::new(HashMap::new()),
        })
    }

    /// Get the file path for a key
    fn key_path(&self, key: &str) -> PathBuf {
        self.config.data_dir.join(format!("{}.mmap", key))
    }

    /// Ensure a memory map exists for the key with sufficient size
    fn ensure_mmap(&self, key: &str, size: usize) -> Result<(), PersistenceError> {
        let path = self.key_path(key);

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)?;

        // Set file size
        file.set_len(size as u64)?;

        // Create memory map
        let mmap = unsafe {
            MmapOptions::new()
                .len(size)
                .map_mut(&file)?
        };

        let mut maps = self.maps.write();
        maps.insert(key.to_string(), MmapEntry { file, mmap, size });

        Ok(())
    }
}

impl PersistenceBackend for MmapBackend {
    fn write(&self, key: &str, data: &[u8]) -> Result<(), PersistenceError> {
        let path = self.key_path(key);

        // For simplicity, write directly to file (can be optimized with mmap for large data)
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;

        file.write_all(data)?;
        file.sync_all()?;

        Ok(())
    }

    fn read(&self, key: &str) -> Result<Option<Vec<u8>>, PersistenceError> {
        let path = self.key_path(key);

        if !path.exists() {
            return Ok(None);
        }

        let data = std::fs::read(path)?;
        Ok(Some(data))
    }

    fn delete(&self, key: &str) -> Result<(), PersistenceError> {
        let path = self.key_path(key);

        // Remove from cache
        {
            let mut maps = self.maps.write();
            maps.remove(key);
        }

        if path.exists() {
            std::fs::remove_file(path)?;
        }

        Ok(())
    }

    fn list_keys(&self) -> Result<Vec<String>, PersistenceError> {
        let mut keys = Vec::new();

        for entry in std::fs::read_dir(&self.config.data_dir)? {
            let entry = entry?;
            let path = entry.path();

            if let Some(ext) = path.extension() {
                if ext == "mmap" {
                    if let Some(stem) = path.file_stem() {
                        if let Some(name) = stem.to_str() {
                            keys.push(name.to_string());
                        }
                    }
                }
            }
        }

        Ok(keys)
    }

    fn sync(&self) -> Result<(), PersistenceError> {
        let maps = self.maps.read();

        for entry in maps.values() {
            entry.mmap.flush()?;
        }

        Ok(())
    }
}

impl MmapBackend {
    /// Write data using memory mapping (for large data)
    pub fn write_mmap(&self, key: &str, data: &[u8]) -> Result<(), PersistenceError> {
        self.ensure_mmap(key, data.len())?;

        let maps = self.maps.read();
        if let Some(entry) = maps.get(key) {
            // Safety: we just created this with the right size
            let mmap_slice = &entry.mmap[..data.len()];
            unsafe {
                std::ptr::copy_nonoverlapping(
                    data.as_ptr(),
                    mmap_slice.as_ptr() as *mut u8,
                    data.len(),
                );
            }
        }

        Ok(())
    }

    /// Read data using memory mapping (returns owned data due to lock scope)
    pub fn read_mmap(&self, key: &str) -> Result<Option<Vec<u8>>, PersistenceError> {
        let maps = self.maps.read();

        if let Some(entry) = maps.get(key) {
            Ok(Some(entry.mmap[..entry.size].to_vec()))
        } else {
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_mmap_write_read() {
        let temp_dir = TempDir::new().unwrap();
        let config = PersistenceConfig::new(temp_dir.path());
        let backend = MmapBackend::new(config).unwrap();

        let data = b"Hello, World!";
        backend.write("test_key", data).unwrap();

        let read_data = backend.read("test_key").unwrap().unwrap();
        assert_eq!(data.as_slice(), read_data.as_slice());
    }

    #[test]
    fn test_mmap_list_keys() {
        let temp_dir = TempDir::new().unwrap();
        let config = PersistenceConfig::new(temp_dir.path());
        let backend = MmapBackend::new(config).unwrap();

        backend.write("key1", b"data1").unwrap();
        backend.write("key2", b"data2").unwrap();

        let keys = backend.list_keys().unwrap();
        assert_eq!(keys.len(), 2);
        assert!(keys.contains(&"key1".to_string()));
        assert!(keys.contains(&"key2".to_string()));
    }

    #[test]
    fn test_mmap_delete() {
        let temp_dir = TempDir::new().unwrap();
        let config = PersistenceConfig::new(temp_dir.path());
        let backend = MmapBackend::new(config).unwrap();

        backend.write("to_delete", b"data").unwrap();
        assert!(backend.read("to_delete").unwrap().is_some());

        backend.delete("to_delete").unwrap();
        assert!(backend.read("to_delete").unwrap().is_none());
    }
}
