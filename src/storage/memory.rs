use std::sync::atomic::{AtomicUsize, Ordering};

/// Tracks memory usage across the storage engine
#[derive(Debug)]
pub struct MemoryTracker {
    /// Current memory usage in bytes
    current_bytes: AtomicUsize,
    /// Peak memory usage in bytes
    peak_bytes: AtomicUsize,
    /// Maximum allowed memory in bytes
    max_bytes: AtomicUsize,
}

impl MemoryTracker {
    pub fn new(max_bytes: usize) -> Self {
        Self {
            current_bytes: AtomicUsize::new(0),
            peak_bytes: AtomicUsize::new(0),
            max_bytes: AtomicUsize::new(max_bytes),
        }
    }

    /// Try to allocate memory. Returns true if successful.
    pub fn try_allocate(&self, bytes: usize) -> bool {
        let max = self.max_bytes.load(Ordering::SeqCst);
        let current = self.current_bytes.load(Ordering::SeqCst);

        if current + bytes > max {
            return false;
        }

        // Use compare-exchange for thread-safety
        loop {
            let current = self.current_bytes.load(Ordering::SeqCst);
            if current + bytes > max {
                return false;
            }
            if self
                .current_bytes
                .compare_exchange(current, current + bytes, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                // Update peak if necessary
                let new_current = current + bytes;
                loop {
                    let peak = self.peak_bytes.load(Ordering::SeqCst);
                    if new_current <= peak {
                        break;
                    }
                    if self
                        .peak_bytes
                        .compare_exchange(peak, new_current, Ordering::SeqCst, Ordering::SeqCst)
                        .is_ok()
                    {
                        break;
                    }
                }
                return true;
            }
        }
    }

    /// Allocate memory without checking limits (for tracking existing allocations)
    pub fn allocate(&self, bytes: usize) {
        let new = self.current_bytes.fetch_add(bytes, Ordering::SeqCst) + bytes;

        // Update peak if necessary
        loop {
            let peak = self.peak_bytes.load(Ordering::SeqCst);
            if new <= peak {
                break;
            }
            if self
                .peak_bytes
                .compare_exchange(peak, new, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                break;
            }
        }
    }

    /// Free allocated memory
    pub fn free(&self, bytes: usize) {
        self.current_bytes.fetch_sub(bytes, Ordering::SeqCst);
    }

    /// Get current memory usage
    pub fn current(&self) -> usize {
        self.current_bytes.load(Ordering::SeqCst)
    }

    /// Get peak memory usage
    pub fn peak(&self) -> usize {
        self.peak_bytes.load(Ordering::SeqCst)
    }

    /// Get maximum allowed memory
    pub fn max(&self) -> usize {
        self.max_bytes.load(Ordering::SeqCst)
    }

    /// Set maximum allowed memory
    pub fn set_max(&self, max_bytes: usize) {
        self.max_bytes.store(max_bytes, Ordering::SeqCst);
    }

    /// Get usage as a fraction (0.0 to 1.0+)
    pub fn usage_ratio(&self) -> f64 {
        let max = self.max_bytes.load(Ordering::SeqCst);
        if max == 0 {
            return 0.0;
        }
        self.current_bytes.load(Ordering::SeqCst) as f64 / max as f64
    }

    /// Check if memory is under pressure (>80% usage)
    pub fn is_under_pressure(&self) -> bool {
        self.usage_ratio() > 0.8
    }

    /// Check if memory limit is exceeded
    pub fn is_exceeded(&self) -> bool {
        self.current_bytes.load(Ordering::SeqCst) > self.max_bytes.load(Ordering::SeqCst)
    }

    /// Reset tracking (for testing)
    pub fn reset(&self) {
        self.current_bytes.store(0, Ordering::SeqCst);
        self.peak_bytes.store(0, Ordering::SeqCst);
    }
}

impl Default for MemoryTracker {
    fn default() -> Self {
        // Default to 1GB
        Self::new(1024 * 1024 * 1024)
    }
}

/// Memory statistics
#[derive(Debug, Clone, serde::Serialize)]
pub struct MemoryStats {
    pub current_bytes: usize,
    pub peak_bytes: usize,
    pub max_bytes: usize,
    pub usage_ratio: f64,
}

impl From<&MemoryTracker> for MemoryStats {
    fn from(tracker: &MemoryTracker) -> Self {
        Self {
            current_bytes: tracker.current(),
            peak_bytes: tracker.peak(),
            max_bytes: tracker.max(),
            usage_ratio: tracker.usage_ratio(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_allocate_and_free() {
        let tracker = MemoryTracker::new(1000);

        tracker.allocate(100);
        assert_eq!(tracker.current(), 100);

        tracker.allocate(200);
        assert_eq!(tracker.current(), 300);

        tracker.free(100);
        assert_eq!(tracker.current(), 200);

        assert_eq!(tracker.peak(), 300);
    }

    #[test]
    fn test_try_allocate_limit() {
        let tracker = MemoryTracker::new(100);

        assert!(tracker.try_allocate(50));
        assert!(tracker.try_allocate(40));
        assert!(!tracker.try_allocate(20)); // Would exceed limit

        assert_eq!(tracker.current(), 90);
    }

    #[test]
    fn test_usage_ratio() {
        let tracker = MemoryTracker::new(100);
        tracker.allocate(50);

        assert!((tracker.usage_ratio() - 0.5).abs() < 0.001);
    }

    #[test]
    fn test_pressure_detection() {
        let tracker = MemoryTracker::new(100);

        tracker.allocate(70);
        assert!(!tracker.is_under_pressure());

        tracker.allocate(15);
        assert!(tracker.is_under_pressure());
    }
}
