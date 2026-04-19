//! High-level cache manager.
//!
//! Port of `surql/cache/manager.py::CacheManager`. Owns a backend,
//! tracks table->keys associations for invalidation, and records
//! hit/miss statistics.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use serde::Serialize;
use serde_json::Value;
use tokio::sync::Mutex;

use crate::error::Result;
#[cfg(not(feature = "cache-redis"))]
use crate::error::SurqlError;

use super::backend::CacheBackend;
use super::config::{CacheBackendKind, CacheConfig};
use super::memory::MemoryCache;
use super::stats::{CacheStats, CacheStatsSnapshot};

/// Orchestrates cache operations on top of a [`CacheBackend`].
///
/// The manager is cheap to clone: `Clone` produces a handle that
/// shares the same backend, table-tracking map, and statistics
/// counters with the original.
#[derive(Clone)]
pub struct CacheManager {
    inner: Arc<ManagerInner>,
}

struct ManagerInner {
    config: CacheConfig,
    backend: Arc<dyn CacheBackend>,
    table_keys: Mutex<HashMap<String, HashSet<String>>>,
    stats: CacheStats,
}

impl std::fmt::Debug for CacheManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CacheManager")
            .field("config", &self.inner.config)
            .finish_non_exhaustive()
    }
}

impl CacheManager {
    /// Build a manager using the backend implied by `config`.
    ///
    /// For `CacheBackendKind::Redis`, requires the `cache-redis`
    /// feature. Returns a `Validation` error if Redis is requested
    /// without the feature enabled.
    pub fn new(config: CacheConfig) -> Result<Self> {
        let backend: Arc<dyn CacheBackend> = match config.backend {
            CacheBackendKind::Memory => Arc::new(MemoryCache::new(
                config.max_size,
                std::time::Duration::from_secs(config.default_ttl_secs),
            )),
            CacheBackendKind::Redis => {
                #[cfg(feature = "cache-redis")]
                {
                    Arc::new(super::redis::RedisCache::new(
                        &config.redis_url,
                        config.key_prefix.clone(),
                        config.default_ttl_secs,
                    )?)
                }
                #[cfg(not(feature = "cache-redis"))]
                {
                    return Err(SurqlError::Validation {
                        reason: "Redis backend requires the 'cache-redis' feature".into(),
                    });
                }
            }
        };
        Ok(Self::with_backend(config, backend))
    }

    /// Build a manager around a caller-provided backend. Useful for
    /// tests and composition with custom implementations.
    pub fn with_backend(config: CacheConfig, backend: Arc<dyn CacheBackend>) -> Self {
        Self {
            inner: Arc::new(ManagerInner {
                config,
                backend,
                table_keys: Mutex::new(HashMap::new()),
                stats: CacheStats::new(),
            }),
        }
    }

    /// Shared reference to the manager's configuration.
    pub fn config(&self) -> &CacheConfig {
        &self.inner.config
    }

    /// Shared statistics handle (cloneable view).
    pub fn stats(&self) -> CacheStats {
        self.inner.stats.clone()
    }

    /// Take a snapshot of the manager's statistics.
    pub fn stats_snapshot(&self) -> CacheStatsSnapshot {
        self.inner.stats.snapshot()
    }

    /// Report whether the cache is globally enabled.
    pub fn is_enabled(&self) -> bool {
        self.inner.config.enabled
    }

    /// Build a fully-qualified cache key from one or more parts.
    pub fn build_key<I, S>(&self, parts: I) -> String
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let joined = parts
            .into_iter()
            .map(|p| p.as_ref().to_string())
            .collect::<Vec<_>>()
            .join(":");
        if joined.starts_with(&self.inner.config.key_prefix) {
            joined
        } else {
            format!("{}{}", self.inner.config.key_prefix, joined)
        }
    }

    /// Look up a raw JSON value by key.
    ///
    /// Returns `Ok(None)` when the cache is disabled, the key is
    /// absent, or the entry has expired. Hits and misses are recorded
    /// against [`CacheManager::stats`].
    pub async fn get_raw(&self, key: &str) -> Result<Option<Value>> {
        if !self.inner.config.enabled {
            return Ok(None);
        }
        let prefixed = self.build_key([key]);
        let result = self.inner.backend.get(&prefixed).await?;
        if result.is_some() {
            self.inner.stats.record_hit();
        } else {
            self.inner.stats.record_miss();
        }
        Ok(result)
    }

    /// Look up a typed value by key, deserialising the cached JSON.
    pub async fn get<T: for<'de> serde::Deserialize<'de>>(&self, key: &str) -> Result<Option<T>> {
        let Some(raw) = self.get_raw(key).await? else {
            return Ok(None);
        };
        let value = serde_json::from_value::<T>(raw)?;
        Ok(Some(value))
    }

    /// Store a serialisable value under `key`.
    ///
    /// Associates `key` with each table in `tables` so the entry can
    /// be invalidated through [`CacheManager::invalidate_table`].
    pub async fn set<T: Serialize + ?Sized>(
        &self,
        key: &str,
        value: &T,
        ttl_secs: Option<u64>,
        tables: &[&str],
    ) -> Result<()> {
        if !self.inner.config.enabled {
            return Ok(());
        }
        let prefixed = self.build_key([key]);
        let payload = serde_json::to_value(value)?;
        self.inner.backend.set(&prefixed, payload, ttl_secs).await?;
        if !tables.is_empty() {
            let mut map = self.inner.table_keys.lock().await;
            for table in tables {
                map.entry((*table).to_string())
                    .or_default()
                    .insert(prefixed.clone());
            }
        }
        Ok(())
    }

    /// Delete a key. No-op when the cache is disabled.
    pub async fn delete(&self, key: &str) -> Result<()> {
        if !self.inner.config.enabled {
            return Ok(());
        }
        let prefixed = self.build_key([key]);
        self.inner.backend.delete(&prefixed).await?;
        let mut map = self.inner.table_keys.lock().await;
        for keys in map.values_mut() {
            keys.remove(&prefixed);
        }
        Ok(())
    }

    /// Report whether `key` exists and has not expired.
    pub async fn exists(&self, key: &str) -> Result<bool> {
        if !self.inner.config.enabled {
            return Ok(false);
        }
        let prefixed = self.build_key([key]);
        self.inner.backend.exists(&prefixed).await
    }

    /// Fetch-or-populate: return the cached value if present, otherwise
    /// execute `factory`, cache its result, and return it.
    pub async fn get_or_set<T, F, Fut>(
        &self,
        key: &str,
        ttl_secs: Option<u64>,
        tables: &[&str],
        factory: F,
    ) -> Result<T>
    where
        T: Serialize + for<'de> serde::Deserialize<'de>,
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        if !self.inner.config.enabled {
            return factory().await;
        }
        if let Some(hit) = self.get::<T>(key).await? {
            return Ok(hit);
        }
        let value = factory().await?;
        self.set(key, &value, ttl_secs, tables).await?;
        Ok(value)
    }

    /// Invalidate a specific key. Returns the number of entries
    /// removed (0 or 1).
    pub async fn invalidate_key(&self, key: &str) -> Result<usize> {
        if !self.inner.config.enabled {
            return Ok(0);
        }
        let prefixed = self.build_key([key]);
        let existed = self.inner.backend.exists(&prefixed).await?;
        self.inner.backend.delete(&prefixed).await?;
        let mut map = self.inner.table_keys.lock().await;
        for keys in map.values_mut() {
            keys.remove(&prefixed);
        }
        Ok(usize::from(existed))
    }

    /// Invalidate every entry tagged with `table`.
    pub async fn invalidate_table(&self, table: &str) -> Result<usize> {
        if !self.inner.config.enabled {
            return Ok(0);
        }
        let keys = {
            let mut map = self.inner.table_keys.lock().await;
            map.remove(table).unwrap_or_default()
        };
        let mut count = 0usize;
        for key in keys {
            self.inner.backend.delete(&key).await?;
            count += 1;
        }
        Ok(count)
    }

    /// Invalidate every entry whose prefixed key matches a glob pattern.
    pub async fn invalidate_pattern(&self, pattern: &str) -> Result<usize> {
        if !self.inner.config.enabled {
            return Ok(0);
        }
        let prefixed = self.build_key([pattern]);
        self.inner.backend.clear(Some(&prefixed)).await
    }

    /// Clear every cache entry.
    pub async fn clear(&self) -> Result<usize> {
        if !self.inner.config.enabled {
            return Ok(0);
        }
        let n = self.inner.backend.clear(None).await?;
        self.inner.table_keys.lock().await.clear();
        self.inner.stats.reset();
        Ok(n)
    }

    /// Close the underlying backend; subsequent calls may reconnect.
    pub async fn close(&self) -> Result<()> {
        self.inner.backend.close().await
    }

    /// Return the set of keys recorded for `table` (for introspection/tests).
    pub async fn keys_for_table(&self, table: &str) -> Vec<String> {
        let map = self.inner.table_keys.lock().await;
        map.get(table)
            .map(|s| s.iter().cloned().collect())
            .unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn manager() -> CacheManager {
        let cfg = CacheConfig::builder()
            .backend(CacheBackendKind::Memory)
            .max_size(32)
            .default_ttl_secs(30)
            .key_prefix("t:")
            .build();
        CacheManager::new(cfg).unwrap()
    }

    #[tokio::test]
    async fn build_key_applies_prefix() {
        let m = manager();
        assert_eq!(m.build_key(["user", "123"]), "t:user:123");
        // Already-prefixed keys are not double-prefixed.
        assert_eq!(m.build_key(["t:user:123"]), "t:user:123");
    }

    #[tokio::test]
    async fn set_and_get_typed_roundtrip() {
        let m = manager();
        m.set("k", &42u32, None, &[]).await.unwrap();
        let v: Option<u32> = m.get("k").await.unwrap();
        assert_eq!(v, Some(42));
        let stats = m.stats_snapshot();
        assert_eq!(stats.hits, 1);
        assert_eq!(stats.misses, 0);
    }

    #[tokio::test]
    async fn get_or_set_populates() {
        let m = manager();
        let v = m
            .get_or_set::<u32, _, _>("x", None, &[], || async { Ok(7) })
            .await
            .unwrap();
        assert_eq!(v, 7);
        let v2 = m
            .get_or_set::<u32, _, _>("x", None, &[], || async { Ok(9) })
            .await
            .unwrap();
        assert_eq!(v2, 7, "second call must return cached value");
    }

    #[tokio::test]
    async fn invalidate_by_table_removes_entries() {
        let m = manager();
        m.set("u1", &1u32, None, &["user"]).await.unwrap();
        m.set("u2", &2u32, None, &["user"]).await.unwrap();
        m.set("p1", &3u32, None, &["product"]).await.unwrap();

        let removed = m.invalidate_table("user").await.unwrap();
        assert_eq!(removed, 2);
        assert!(m.get::<u32>("u1").await.unwrap().is_none());
        assert_eq!(m.get::<u32>("p1").await.unwrap(), Some(3));
    }

    #[tokio::test]
    async fn invalidate_pattern_matches_prefix_scope() {
        let m = manager();
        m.set("user:1", &1u32, None, &[]).await.unwrap();
        m.set("user:2", &2u32, None, &[]).await.unwrap();
        m.set("product:1", &3u32, None, &[]).await.unwrap();
        let n = m.invalidate_pattern("user:*").await.unwrap();
        assert_eq!(n, 2);
        assert_eq!(m.get::<u32>("product:1").await.unwrap(), Some(3));
    }

    #[tokio::test]
    async fn clear_empties_cache_and_resets_stats() {
        let m = manager();
        m.set("a", &1u32, None, &[]).await.unwrap();
        let _ = m.get::<u32>("a").await.unwrap();
        assert_eq!(m.stats_snapshot().hits, 1);
        let n = m.clear().await.unwrap();
        assert_eq!(n, 1);
        let snap = m.stats_snapshot();
        assert_eq!(snap.hits, 0);
        assert_eq!(snap.misses, 0);
    }

    #[tokio::test]
    async fn disabled_manager_is_noop() {
        let cfg = CacheConfig::builder().enabled(false).build();
        let m = CacheManager::new(cfg).unwrap();
        assert!(!m.is_enabled());
        m.set("k", &1u32, None, &[]).await.unwrap();
        assert!(m.get::<u32>("k").await.unwrap().is_none());
        let v: u32 = m
            .get_or_set("k", None, &[], || async { Ok(99) })
            .await
            .unwrap();
        assert_eq!(v, 99);
    }

    #[tokio::test]
    async fn redis_without_feature_fails() {
        #[cfg(not(feature = "cache-redis"))]
        {
            let cfg = CacheConfig::builder()
                .backend(CacheBackendKind::Redis)
                .build();
            assert!(CacheManager::new(cfg).is_err());
        }
    }
}
