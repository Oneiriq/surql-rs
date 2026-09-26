//! In-process LRU+TTL cache backend.
//!
//! Port of `surql/cache/backends.py::MemoryCache`. Uses a
//! `HashMap<String, Entry>` protected by a `tokio::sync::RwLock`
//! rather than an LRU crate. When the cache is full, an insert first
//! drops every expired entry and, only if that frees nothing, evicts the
//! least-recently-used live one; TTL is otherwise enforced lazily on
//! access. This keeps the dependency footprint minimal.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use serde_json::Value;
use tokio::sync::RwLock;

use crate::error::Result;

use super::backend::{compile_glob, CacheBackend};
use super::stats::CacheStats;

/// Internal record for a single cache entry.
#[derive(Debug)]
struct Entry {
    value: Value,
    expires_at: Option<Instant>,
    /// Tick of the last read or write; the smallest is the LRU entry.
    /// Atomic so a read can refresh it under the shared lock.
    last_used: AtomicU64,
}

impl Entry {
    fn is_expired(&self, now: Instant) -> bool {
        self.expires_at.is_some_and(|e| now >= e)
    }
}

/// In-memory cache backend with LRU eviction and TTL expiry.
///
/// Not cloneable by design; wrap in [`std::sync::Arc`] if you need
/// multiple owners.
#[derive(Debug)]
pub struct MemoryCache {
    max_size: usize,
    default_ttl: Duration,
    inner: RwLock<HashMap<String, Entry>>,
    /// Monotonic use counter behind [`Entry::last_used`].
    clock: AtomicU64,
    stats: CacheStats,
}

impl MemoryCache {
    /// Create a memory cache with `max_size` entries and a default TTL.
    pub fn new(max_size: usize, default_ttl: Duration) -> Self {
        Self::with_stats(max_size, default_ttl, CacheStats::new())
    }

    /// Like [`MemoryCache::new`], reporting size and evictions into an
    /// existing statistics handle (a manager's, typically).
    pub fn with_stats(max_size: usize, default_ttl: Duration, stats: CacheStats) -> Self {
        Self {
            max_size: max_size.max(1),
            default_ttl,
            inner: RwLock::new(HashMap::new()),
            clock: AtomicU64::new(0),
            stats,
        }
    }

    /// Current number of entries (includes any not-yet-expired rows).
    pub async fn size(&self) -> usize {
        self.inner.read().await.len()
    }

    /// Shared statistics handle.
    pub fn stats(&self) -> CacheStats {
        self.stats.clone()
    }

    fn tick(&self) -> u64 {
        self.clock.fetch_add(1, Ordering::Relaxed)
    }

    fn resolve_ttl(&self, ttl: Option<u64>) -> Option<Instant> {
        let dur = match ttl {
            Some(0) => return None,
            Some(secs) => Duration::from_secs(secs),
            None => self.default_ttl,
        };
        if dur.is_zero() {
            None
        } else {
            Instant::now().checked_add(dur)
        }
    }

    fn report_size(&self, entries: &HashMap<String, Entry>) {
        self.stats
            .set_size(u64::try_from(entries.len()).unwrap_or(u64::MAX));
    }
}

/// Make room for one more entry: drop every expired entry first, and only
/// when none was expired evict the least-recently-used live entry.
/// Returns whether a live entry was evicted.
fn make_room(entries: &mut HashMap<String, Entry>, now: Instant) -> bool {
    let before = entries.len();
    entries.retain(|_, e| !e.is_expired(now));
    if entries.len() < before {
        return false;
    }
    let lru = entries
        .iter()
        .min_by_key(|(_, e)| e.last_used.load(Ordering::Relaxed))
        .map(|(k, _)| k.clone());
    lru.is_some_and(|k| entries.remove(&k).is_some())
}

#[async_trait]
impl CacheBackend for MemoryCache {
    async fn get(&self, key: &str) -> Result<Option<Value>> {
        let now = Instant::now();
        // Fast path: upgrade to write only when expiry cleanup is required.
        {
            let guard = self.inner.read().await;
            match guard.get(key) {
                None => return Ok(None),
                Some(entry) if !entry.is_expired(now) => {
                    entry.last_used.store(self.tick(), Ordering::Relaxed);
                    return Ok(Some(entry.value.clone()));
                }
                Some(_) => {}
            }
        }
        let mut guard = self.inner.write().await;
        if guard.get(key).is_some_and(|entry| entry.is_expired(now)) {
            guard.remove(key);
            self.report_size(&guard);
            return Ok(None);
        }
        Ok(guard.get(key).map(|entry| {
            entry.last_used.store(self.tick(), Ordering::Relaxed);
            entry.value.clone()
        }))
    }

    async fn set(&self, key: &str, value: Value, ttl_secs: Option<u64>) -> Result<()> {
        let expires_at = self.resolve_ttl(ttl_secs);
        let mut guard = self.inner.write().await;
        if !guard.contains_key(key)
            && guard.len() >= self.max_size
            && make_room(&mut guard, Instant::now())
        {
            self.stats.record_eviction();
        }
        guard.insert(
            key.to_string(),
            Entry {
                value,
                expires_at,
                last_used: AtomicU64::new(self.tick()),
            },
        );
        self.report_size(&guard);
        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<()> {
        let mut guard = self.inner.write().await;
        guard.remove(key);
        self.report_size(&guard);
        Ok(())
    }

    async fn clear(&self, pattern: Option<&str>) -> Result<usize> {
        let matcher = pattern.map(compile_glob).transpose()?;
        let mut guard = self.inner.write().await;
        let before = guard.len();
        match matcher {
            None => guard.clear(),
            Some(re) => guard.retain(|k, _| !re.is_match(k)),
        }
        let count = before.saturating_sub(guard.len());
        self.report_size(&guard);
        Ok(count)
    }

    async fn exists(&self, key: &str) -> Result<bool> {
        let now = Instant::now();
        let guard = self.inner.read().await;
        Ok(guard.get(key).is_some_and(|entry| !entry.is_expired(now)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn cache() -> MemoryCache {
        MemoryCache::new(16, Duration::from_mins(1))
    }

    #[tokio::test]
    async fn set_and_get_roundtrip() {
        let c = cache();
        c.set("k", json!({"a": 1}), None).await.unwrap();
        let v = c.get("k").await.unwrap();
        assert_eq!(v, Some(json!({"a": 1})));
    }

    #[tokio::test]
    async fn missing_key_returns_none() {
        let c = cache();
        assert_eq!(c.get("nope").await.unwrap(), None);
    }

    #[tokio::test]
    async fn delete_removes_key() {
        let c = cache();
        c.set("k", json!(1), None).await.unwrap();
        c.delete("k").await.unwrap();
        assert_eq!(c.get("k").await.unwrap(), None);
    }

    #[tokio::test]
    async fn exists_reports_presence() {
        let c = cache();
        c.set("k", json!(1), None).await.unwrap();
        assert!(c.exists("k").await.unwrap());
        assert!(!c.exists("nope").await.unwrap());
    }

    #[tokio::test]
    async fn clear_all_and_by_pattern() {
        let c = cache();
        c.set("user:1", json!(1), None).await.unwrap();
        c.set("user:2", json!(2), None).await.unwrap();
        c.set("product:1", json!(3), None).await.unwrap();
        assert_eq!(c.clear(Some("user:*")).await.unwrap(), 2);
        assert!(c.exists("product:1").await.unwrap());
        assert_eq!(c.clear(None).await.unwrap(), 1);
    }

    #[tokio::test]
    async fn ttl_expiry_removes_entries() {
        let c = MemoryCache::new(4, Duration::from_mins(1));
        c.set("k", json!(1), Some(1)).await.unwrap();
        assert_eq!(c.get("k").await.unwrap(), Some(json!(1)));
        tokio::time::sleep(Duration::from_millis(1100)).await;
        assert_eq!(c.get("k").await.unwrap(), None);
        assert!(!c.exists("k").await.unwrap());
    }

    #[tokio::test]
    async fn eviction_on_capacity_overflow() {
        let c = MemoryCache::new(2, Duration::from_mins(1));
        c.set("a", json!(1), None).await.unwrap();
        c.set("b", json!(2), None).await.unwrap();
        c.set("c", json!(3), None).await.unwrap();
        assert_eq!(c.size().await, 2);
        // `a` is least recently used; it must be the evicted one.
        assert_eq!(c.get("a").await.unwrap(), None);
        assert!(c.exists("b").await.unwrap());
        assert!(c.exists("c").await.unwrap());
        assert_eq!(c.stats().evictions(), 1);
    }

    /// Regression: eviction took the oldest INSERT, so a hot entry read on
    /// every request was evicted ahead of a cold one.
    #[tokio::test]
    async fn eviction_spares_recently_read_entries() {
        let c = MemoryCache::new(2, Duration::from_mins(1));
        c.set("hot", json!(1), None).await.unwrap();
        c.set("cold", json!(2), None).await.unwrap();
        assert!(c.get("hot").await.unwrap().is_some());
        c.set("new", json!(3), None).await.unwrap();
        assert!(c.exists("hot").await.unwrap());
        assert!(!c.exists("cold").await.unwrap());
    }

    /// Regression: a full cache evicted a live entry even when an expired
    /// one was sitting there to be dropped instead.
    #[tokio::test]
    async fn expired_entries_make_room_before_live_ones() {
        let c = MemoryCache::new(2, Duration::from_mins(1));
        c.set("live", json!(1), None).await.unwrap();
        c.set("brief", json!(2), Some(1)).await.unwrap();
        tokio::time::sleep(Duration::from_millis(1100)).await;
        c.set("new", json!(3), None).await.unwrap();
        assert!(c.exists("live").await.unwrap());
        assert!(c.exists("new").await.unwrap());
        assert_eq!(c.stats().evictions(), 0);
        assert_eq!(c.stats().size(), 2);
    }
}
