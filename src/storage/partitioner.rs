//! Core partitioner for CPU-aware shard assignment
//!
//! Assigns shards to CPU cores for better cache locality and reduced lock contention.

use std::sync::atomic::{AtomicUsize, Ordering};

/// Partitioner that assigns shards to CPU cores
#[derive(Debug)]
pub struct CorePartitioner {
    /// Number of CPU cores
    num_cores: usize,
    /// Next core to assign (round-robin)
    next_core: AtomicUsize,
}

impl CorePartitioner {
    /// Create a new partitioner using all available cores
    pub fn new() -> Self {
        Self {
            num_cores: num_cpus::get(),
            next_core: AtomicUsize::new(0),
        }
    }

    /// Create a partitioner with a specific number of cores
    pub fn with_cores(num_cores: usize) -> Self {
        Self {
            num_cores: num_cores.max(1),
            next_core: AtomicUsize::new(0),
        }
    }

    /// Get the number of cores
    pub fn num_cores(&self) -> usize {
        self.num_cores
    }

    /// Get the core assignment for a shard based on timestamp
    pub fn partition_by_time(&self, timestamp: i64, shard_duration_ms: i64) -> usize {
        let shard_index = (timestamp / shard_duration_ms) as usize;
        shard_index % self.num_cores
    }

    /// Get the next core in round-robin fashion
    pub fn next_partition(&self) -> usize {
        let core = self.next_core.fetch_add(1, Ordering::Relaxed);
        core % self.num_cores
    }

    /// Get partition for a given key (hash-based)
    pub fn partition_by_key(&self, key: &str) -> usize {
        let hash = fxhash::hash64(key.as_bytes());
        (hash as usize) % self.num_cores
    }

    /// Get recommended thread pool size
    pub fn recommended_parallelism(&self) -> usize {
        self.num_cores
    }
}

impl Default for CorePartitioner {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for CorePartitioner {
    fn clone(&self) -> Self {
        Self {
            num_cores: self.num_cores,
            next_core: AtomicUsize::new(self.next_core.load(Ordering::Relaxed)),
        }
    }
}

/// Core-affinity aware parallel execution
pub struct AffinityExecutor {
    partitioner: CorePartitioner,
}

impl AffinityExecutor {
    pub fn new() -> Self {
        Self {
            partitioner: CorePartitioner::new(),
        }
    }

    pub fn with_partitioner(partitioner: CorePartitioner) -> Self {
        Self { partitioner }
    }

    /// Get the partitioner
    pub fn partitioner(&self) -> &CorePartitioner {
        &self.partitioner
    }

    /// Execute work items grouped by core assignment
    pub fn execute_grouped<T, F>(&self, items: Vec<T>, key_fn: F) -> Vec<(usize, Vec<T>)>
    where
        F: Fn(&T) -> usize,
    {
        let num_cores = self.partitioner.num_cores;
        let mut groups: Vec<Vec<T>> = (0..num_cores).map(|_| Vec::new()).collect();

        for item in items {
            let core = key_fn(&item) % num_cores;
            groups[core].push(item);
        }

        groups
            .into_iter()
            .enumerate()
            .filter(|(_, v)| !v.is_empty())
            .collect()
    }
}

impl Default for AffinityExecutor {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_partitioner_basic() {
        let partitioner = CorePartitioner::with_cores(4);

        assert_eq!(partitioner.num_cores(), 4);
    }

    #[test]
    fn test_partition_by_time() {
        let partitioner = CorePartitioner::with_cores(4);
        let shard_duration = 3600000; // 1 hour

        // Same hour should get same partition
        let p1 = partitioner.partition_by_time(1000, shard_duration);
        let p2 = partitioner.partition_by_time(2000, shard_duration);
        assert_eq!(p1, p2);

        // Different hours may get different partitions
        let p3 = partitioner.partition_by_time(3600000, shard_duration);
        let p4 = partitioner.partition_by_time(7200000, shard_duration);
        // p3 and p4 are 1 apart modulo 4
        assert!((p4 + 4 - p3) % 4 == 1 || (p3 + 4 - p4) % 4 == 1 || p3 == p4);
    }

    #[test]
    fn test_round_robin() {
        let partitioner = CorePartitioner::with_cores(4);

        let assignments: Vec<usize> = (0..8).map(|_| partitioner.next_partition()).collect();

        assert_eq!(assignments, vec![0, 1, 2, 3, 0, 1, 2, 3]);
    }

    #[test]
    fn test_partition_by_key() {
        let partitioner = CorePartitioner::with_cores(4);

        // Same key should always get same partition
        let p1 = partitioner.partition_by_key("events");
        let p2 = partitioner.partition_by_key("events");
        assert_eq!(p1, p2);

        // Result should be in valid range
        assert!(p1 < 4);
    }

    #[test]
    fn test_affinity_executor() {
        let executor = AffinityExecutor::new();

        let items = vec![0usize, 1, 2, 3, 4, 5, 6, 7];
        let groups = executor.execute_grouped(items, |&x| x);

        // Each item should be in its corresponding core group
        for (core, items) in groups {
            for item in items {
                assert_eq!(item % executor.partitioner().num_cores(), core);
            }
        }
    }
}
