//! Cache statistics with atomic counters.
//!
//! Port of the Python `CacheStats` dataclass. The Rust version exposes
//! atomic counters under a shared [`CacheStats`] handle so multiple
//! tasks can update them concurrently without locking.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Live cache statistics.
///
/// Cloning is cheap: a [`CacheStats`] is a thin `Arc` wrapping the
/// underlying atomic counters. All clones observe the same state.
#[derive(Debug, Clone, Default)]
pub struct CacheStats {
    inner: Arc<StatsInner>,
}

#[derive(Debug, Default)]
struct StatsInner {
    hits: AtomicU64,
    misses: AtomicU64,
    size: AtomicU64,
    evictions: AtomicU64,
}

impl CacheStats {
    /// Construct a zeroed statistics handle.
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a cache hit.
    pub fn record_hit(&self) {
        self.inner.hits.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a cache miss.
    pub fn record_miss(&self) {
        self.inner.misses.fetch_add(1, Ordering::Relaxed);
    }

    /// Record an eviction.
    pub fn record_eviction(&self) {
        self.inner.evictions.fetch_add(1, Ordering::Relaxed);
    }

    /// Replace the tracked entry count.
    pub fn set_size(&self, size: u64) {
        self.inner.size.store(size, Ordering::Relaxed);
    }

    /// Number of hits observed so far.
    pub fn hits(&self) -> u64 {
        self.inner.hits.load(Ordering::Relaxed)
    }

    /// Number of misses observed so far.
    pub fn misses(&self) -> u64 {
        self.inner.misses.load(Ordering::Relaxed)
    }

    /// Current tracked entry count.
    pub fn size(&self) -> u64 {
        self.inner.size.load(Ordering::Relaxed)
    }

    /// Number of evictions observed.
    pub fn evictions(&self) -> u64 {
        self.inner.evictions.load(Ordering::Relaxed)
    }

    /// Immutable snapshot of the counters at this moment.
    pub fn snapshot(&self) -> CacheStatsSnapshot {
        CacheStatsSnapshot {
            hits: self.hits(),
            misses: self.misses(),
            size: self.size(),
            evictions: self.evictions(),
        }
    }

    /// Reset every counter to zero.
    pub fn reset(&self) {
        self.inner.hits.store(0, Ordering::Relaxed);
        self.inner.misses.store(0, Ordering::Relaxed);
        self.inner.size.store(0, Ordering::Relaxed);
        self.inner.evictions.store(0, Ordering::Relaxed);
    }

    /// Compute the hit ratio on the current counts.
    pub fn hit_ratio(&self) -> f64 {
        let h = self.hits();
        let m = self.misses();
        let total = h + m;
        if total == 0 {
            0.0
        } else {
            #[allow(clippy::cast_precision_loss)]
            let ratio = h as f64 / total as f64;
            ratio
        }
    }
}

/// Immutable snapshot of the [`CacheStats`] counters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CacheStatsSnapshot {
    /// Hit count at the time the snapshot was taken.
    pub hits: u64,
    /// Miss count at the time of snapshot.
    pub misses: u64,
    /// Entry count at the time of snapshot.
    pub size: u64,
    /// Eviction count at the time of snapshot.
    pub evictions: u64,
}

impl CacheStatsSnapshot {
    /// Compute the hit ratio from this snapshot's counts.
    pub fn hit_ratio(&self) -> f64 {
        let total = self.hits + self.misses;
        if total == 0 {
            0.0
        } else {
            #[allow(clippy::cast_precision_loss)]
            let ratio = self.hits as f64 / total as f64;
            ratio
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counters_start_at_zero() {
        let s = CacheStats::new();
        assert_eq!(s.hits(), 0);
        assert_eq!(s.misses(), 0);
        assert!((s.hit_ratio() - 0.0).abs() < f64::EPSILON);
    }

    #[test]
    fn increments_and_ratio() {
        let s = CacheStats::new();
        s.record_hit();
        s.record_hit();
        s.record_miss();
        let snap = s.snapshot();
        assert_eq!(snap.hits, 2);
        assert_eq!(snap.misses, 1);
        let expected = 2.0 / 3.0;
        assert!((snap.hit_ratio() - expected).abs() < 1e-9);
    }

    #[test]
    fn clones_share_state() {
        let s = CacheStats::new();
        let s2 = s.clone();
        s.record_hit();
        s2.record_miss();
        assert_eq!(s.hits(), 1);
        assert_eq!(s.misses(), 1);
    }

    #[test]
    fn reset_clears_counters() {
        let s = CacheStats::new();
        s.record_hit();
        s.record_miss();
        s.record_eviction();
        s.set_size(5);
        s.reset();
        assert_eq!(s.hits(), 0);
        assert_eq!(s.misses(), 0);
        assert_eq!(s.size(), 0);
        assert_eq!(s.evictions(), 0);
    }
}
