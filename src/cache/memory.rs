//! In-process LRU+TTL cache backend.
//!
//! Port of `surql/cache/backends.py::MemoryCache`. Uses a
//! `HashMap<String, CacheEntry>` protected by a `tokio::sync::RwLock`
//! rather than an LRU crate. Eviction on capacity overflow drops the
//! oldest-inserted entry; TTL is enforced lazily on access. This keeps
//! the dependency footprint minimal and matches the Python port's
//! observable semantics closely enough for parity tests.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use serde_json::Value;
use tokio::sync::RwLock;

use crate::error::Result;

use super::backend::{compile_glob, CacheBackend};
use super::stats::CacheStats;

/// Internal record for a single cache entry.
#[derive(Debug, Clone)]
struct Entry {
    value: Value,
    expires_at: Option<Instant>,
    inserted_at: Instant,
}

impl Entry {
    fn is_expired(&self, now: Instant) -> bool {
        self.expires_at.is_some_and(|e| now >= e)
    }
}

/// In-memory cache backend with size-based eviction and TTL expiry.
///
/// Not cloneable by design; wrap in [`std::sync::Arc`] if you need
/// multiple owners.
#[derive(Debug)]
pub struct MemoryCache {
    max_size: usize,
    default_ttl: Duration,
    inner: RwLock<HashMap<String, Entry>>,
    stats: CacheStats,
}

impl MemoryCache {
    /// Create a memory cache with `max_size` entries and a default TTL.
    pub fn new(max_size: usize, default_ttl: Duration) -> Self {
        Self {
            max_size: max_size.max(1),
            default_ttl,
            inner: RwLock::new(HashMap::new()),
            stats: CacheStats::new(),
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
}

#[async_trait]
impl CacheBackend for MemoryCache {
    async fn get(&self, key: &str) -> Result<Option<Value>> {
        let now = Instant::now();
        // Fast path: upgrade to write only when expiry cleanup is required.
        {
            let guard = self.inner.read().await;
            if let Some(entry) = guard.get(key) {
                if !entry.is_expired(now) {
                    return Ok(Some(entry.value.clone()));
                }
            } else {
                return Ok(None);
            }
        }
        let mut guard = self.inner.write().await;
        if let Some(entry) = guard.get(key) {
            if entry.is_expired(now) {
                guard.remove(key);
                self.stats.set_size(guard.len() as u64);
                return Ok(None);
            }
            return Ok(Some(entry.value.clone()));
        }
        Ok(None)
    }

    async fn set(&self, key: &str, value: Value, ttl_secs: Option<u64>) -> Result<()> {
        let expires_at = self.resolve_ttl(ttl_secs);
        let mut guard = self.inner.write().await;
        let was_present = guard.contains_key(key);
        if !was_present && guard.len() >= self.max_size {
            // Evict the oldest-inserted entry.
            if let Some((oldest_key, _)) = guard
                .iter()
                .min_by_key(|(_, e)| e.inserted_at)
                .map(|(k, e)| (k.clone(), e.clone()))
            {
                guard.remove(&oldest_key);
                self.stats.record_eviction();
            }
        }
        guard.insert(
            key.to_string(),
            Entry {
                value,
                expires_at,
                inserted_at: Instant::now(),
            },
        );
        self.stats.set_size(guard.len() as u64);
        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<()> {
        let mut guard = self.inner.write().await;
        guard.remove(key);
        self.stats.set_size(guard.len() as u64);
        Ok(())
    }

    async fn clear(&self, pattern: Option<&str>) -> Result<usize> {
        let mut guard = self.inner.write().await;
        let count = match pattern {
            None => {
                let n = guard.len();
                guard.clear();
                n
            }
            Some(pat) => {
                let re = compile_glob(pat);
                let to_remove: Vec<String> =
                    guard.keys().filter(|k| re.is_match(k)).cloned().collect();
                for k in &to_remove {
                    guard.remove(k);
                }
                to_remove.len()
            }
        };
        self.stats.set_size(guard.len() as u64);
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
        MemoryCache::new(16, Duration::from_secs(60))
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
        let c = MemoryCache::new(4, Duration::from_secs(60));
        c.set("k", json!(1), Some(1)).await.unwrap();
        assert_eq!(c.get("k").await.unwrap(), Some(json!(1)));
        tokio::time::sleep(Duration::from_millis(1100)).await;
        assert_eq!(c.get("k").await.unwrap(), None);
        assert!(!c.exists("k").await.unwrap());
    }

    #[tokio::test]
    async fn eviction_on_capacity_overflow() {
        let c = MemoryCache::new(2, Duration::from_secs(60));
        c.set("a", json!(1), None).await.unwrap();
        // Ensure distinct insertion timestamps.
        tokio::time::sleep(Duration::from_millis(10)).await;
        c.set("b", json!(2), None).await.unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;
        c.set("c", json!(3), None).await.unwrap();
        assert_eq!(c.size().await, 2);
        // `a` is oldest; it must be the evicted one.
        assert_eq!(c.get("a").await.unwrap(), None);
        assert!(c.exists("b").await.unwrap());
        assert!(c.exists("c").await.unwrap());
        assert_eq!(c.stats().evictions(), 1);
    }
}
