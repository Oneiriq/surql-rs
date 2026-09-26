//! High-level cache manager.
//!
//! Port of `surql/cache/manager.py::CacheManager`. Owns a backend,
//! tracks table->keys associations for invalidation, and records
//! hit/miss statistics.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::Value;
use tokio::sync::Mutex;

use crate::error::Result;
#[cfg(not(feature = "cache-redis"))]
use crate::error::SurqlError;

use super::backend::{escape_glob, CacheBackend};
use super::config::{CacheBackendKind, CacheConfig};
use super::memory::MemoryCache;
use super::stats::{CacheStats, CacheStatsSnapshot};

/// Tracked table associations below which no sweep runs.
const MIN_SWEEP_THRESHOLD: usize = 1024;

/// Orchestrates cache operations on top of a [`CacheBackend`].
///
/// The manager is cheap to clone: `Clone` produces a handle that
/// shares the same backend, table-tracking map, and statistics
/// counters with the original.
///
/// Every key is stored under the configured `key_prefix`: `"x"` and
/// `"surql:x"` are two different keys, whatever the prefix is.
#[derive(Clone)]
pub struct CacheManager {
    inner: Arc<ManagerInner>,
}

struct ManagerInner {
    config: CacheConfig,
    backend: Arc<dyn CacheBackend>,
    table_keys: Mutex<HashMap<String, HashSet<String>>>,
    /// Tracked associations at which the next sweep of expired and
    /// evicted keys runs; doubles with the live count after each sweep so
    /// the cost stays amortised.
    sweep_at: AtomicUsize,
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
        match config.backend {
            CacheBackendKind::Memory => Ok(Self::in_memory(config)),
            CacheBackendKind::Redis => {
                #[cfg(feature = "cache-redis")]
                {
                    // The manager applies `key_prefix` to every key, so
                    // the backend adds none of its own.
                    let backend = super::redis::RedisCache::new(
                        &config.redis_url,
                        "",
                        config.default_ttl_secs,
                    )?;
                    Ok(Self::with_backend(config, Arc::new(backend)))
                }
                #[cfg(not(feature = "cache-redis"))]
                {
                    Err(SurqlError::Validation {
                        reason: "Redis backend requires the 'cache-redis' feature".into(),
                    })
                }
            }
        }
    }

    /// A manager over a fresh [`MemoryCache`] sized by `config`, whatever
    /// its `backend` says. The cache reports its size and evictions into
    /// the manager's statistics.
    pub(crate) fn in_memory(config: CacheConfig) -> Self {
        let stats = CacheStats::new();
        let backend = MemoryCache::with_stats(
            config.max_size,
            std::time::Duration::from_secs(config.default_ttl_secs),
            stats.clone(),
        );
        Self::assemble(config, Arc::new(backend), stats)
    }

    /// Build a manager around a caller-provided backend. Useful for
    /// tests and composition with custom implementations.
    pub fn with_backend(config: CacheConfig, backend: Arc<dyn CacheBackend>) -> Self {
        Self::assemble(config, backend, CacheStats::new())
    }

    fn assemble(config: CacheConfig, backend: Arc<dyn CacheBackend>, stats: CacheStats) -> Self {
        Self {
            inner: Arc::new(ManagerInner {
                config,
                backend,
                table_keys: Mutex::new(HashMap::new()),
                sweep_at: AtomicUsize::new(MIN_SWEEP_THRESHOLD),
                stats,
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
    ///
    /// Hits and misses are counted by the manager. Size and evictions are
    /// reported by the backend, which the built-in memory backend does;
    /// Redis and custom backends leave them at zero.
    pub fn stats_snapshot(&self) -> CacheStatsSnapshot {
        self.inner.stats.snapshot()
    }

    /// Report whether the cache is globally enabled.
    pub fn is_enabled(&self) -> bool {
        self.inner.config.enabled
    }

    /// Build a fully-qualified cache key from one or more parts: the
    /// configured prefix followed by the parts joined with `:`.
    ///
    /// The prefix is always added, so a part that already starts with it
    /// still names a distinct key.
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
        format!("{}{}", self.inner.config.key_prefix, joined)
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
        let result = self.inner.backend.get(&self.build_key([key])).await?;
        if result.is_some() {
            self.inner.stats.record_hit();
        } else {
            self.inner.stats.record_miss();
        }
        Ok(result)
    }

    /// Look up a typed value by key, deserialising the cached JSON.
    pub async fn get<T: DeserializeOwned>(&self, key: &str) -> Result<Option<T>> {
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
    /// Associations of entries that have since expired or been evicted
    /// are swept out periodically, so the tracking stays proportional to
    /// the live entries.
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
        if tables.is_empty() {
            return Ok(());
        }
        let mut map = self.inner.table_keys.lock().await;
        for table in tables {
            map.entry((*table).to_string())
                .or_default()
                .insert(prefixed.clone());
        }
        if tracked(&map) > self.inner.sweep_at.load(Ordering::Relaxed) {
            self.sweep(&mut map).await?;
        }
        Ok(())
    }

    /// Drop the associations whose entries no longer exist in the backend.
    ///
    /// Runs with the map locked, so a concurrent `set` (which stores its
    /// entry before it records the association) can never have its fresh
    /// association swept out.
    async fn sweep(&self, map: &mut HashMap<String, HashSet<String>>) -> Result<()> {
        let keys: HashSet<String> = map.values().flatten().cloned().collect();
        let mut dead = HashSet::new();
        for key in keys {
            if !self.inner.backend.exists(&key).await? {
                dead.insert(key);
            }
        }
        for keys in map.values_mut() {
            keys.retain(|k| !dead.contains(k));
        }
        map.retain(|_, keys| !keys.is_empty());
        let next = tracked(map).saturating_mul(2).max(MIN_SWEEP_THRESHOLD);
        self.inner.sweep_at.store(next, Ordering::Relaxed);
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
        self.inner.backend.exists(&self.build_key([key])).await
    }

    /// Fetch-or-populate: return the cached value if present, otherwise
    /// execute `factory`, cache its result, and return it.
    ///
    /// A cached value that does not deserialise as `T` (a different type
    /// stored under the same key, or a shape that changed between
    /// releases) counts as a miss: `factory` runs and its result replaces
    /// the entry.
    pub async fn get_or_set<T, F, Fut>(
        &self,
        key: &str,
        ttl_secs: Option<u64>,
        tables: &[&str],
        factory: F,
    ) -> Result<T>
    where
        T: Serialize + DeserializeOwned,
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        if !self.inner.config.enabled {
            return factory().await;
        }
        let cached = self.inner.backend.get(&self.build_key([key])).await?;
        if let Some(hit) = cached.and_then(|raw| serde_json::from_value::<T>(raw).ok()) {
            self.inner.stats.record_hit();
            return Ok(hit);
        }
        self.inner.stats.record_miss();
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

    /// Invalidate every entry whose key matches a glob pattern.
    ///
    /// The pattern is matched against the key as given to
    /// [`CacheManager::set`], under the configured prefix (applied as a
    /// literal): `*` matches any run of characters, `?` one character, and
    /// `\` makes the next character literal.
    pub async fn invalidate_pattern(&self, pattern: &str) -> Result<usize> {
        if !self.inner.config.enabled {
            return Ok(0);
        }
        let scoped = format!("{}{pattern}", escape_glob(&self.inner.config.key_prefix));
        self.inner.backend.clear(Some(&scoped)).await
    }

    /// Clear every entry stored under this manager's key prefix, and
    /// reset the statistics.
    ///
    /// With an empty prefix this clears the whole backend, which a Redis
    /// backend refuses rather than delete every key in its database.
    pub async fn clear(&self) -> Result<usize> {
        if !self.inner.config.enabled {
            return Ok(0);
        }
        let prefix = &self.inner.config.key_prefix;
        let scope = (!prefix.is_empty()).then(|| format!("{}*", escape_glob(prefix)));
        // Reset first: the memory backend reports its remaining size as
        // part of the clear.
        self.inner.stats.reset();
        let n = self.inner.backend.clear(scope.as_deref()).await?;
        self.inner.table_keys.lock().await.clear();
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

/// Number of tracked table associations.
fn tracked(map: &HashMap<String, HashSet<String>>) -> usize {
    map.values().map(HashSet::len).sum()
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
    }

    /// Regression: a key that already started with the prefix was left
    /// alone, so `"x"` and `"t:x"` were one entry and a caller could read
    /// (or overwrite) another caller's value by prefixing its key.
    #[tokio::test]
    async fn prefixed_looking_keys_are_distinct() {
        let m = manager();
        assert_eq!(m.build_key(["t:user:123"]), "t:t:user:123");
        m.set("x", &1u32, None, &[]).await.unwrap();
        m.set("t:x", &2u32, None, &[]).await.unwrap();
        assert_eq!(m.get::<u32>("x").await.unwrap(), Some(1));
        assert_eq!(m.get::<u32>("t:x").await.unwrap(), Some(2));
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

    /// Regression: a cached value of another type made `get_or_set` fail
    /// with a serialization error instead of refetching.
    #[tokio::test]
    async fn get_or_set_refetches_on_a_type_mismatch() {
        let m = manager();
        m.set("x", "not a number", None, &[]).await.unwrap();
        let v = m
            .get_or_set::<u32, _, _>("x", None, &[], || async { Ok(9) })
            .await
            .unwrap();
        assert_eq!(v, 9);
        assert_eq!(m.get::<u32>("x").await.unwrap(), Some(9));
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

    /// Regression: a non-ASCII pattern failed to compile and fell back to
    /// `.*`, so invalidating `café:*` wiped the whole cache.
    #[tokio::test]
    async fn invalidate_pattern_with_non_ascii_is_scoped() {
        let m = manager();
        m.set("café:1", &1u32, None, &[]).await.unwrap();
        m.set("user:1", &2u32, None, &[]).await.unwrap();
        assert_eq!(m.invalidate_pattern("café:*").await.unwrap(), 1);
        assert_eq!(m.get::<u32>("user:1").await.unwrap(), Some(2));
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
        assert_eq!(snap.size, 0);
    }

    /// Regression: the manager kept its own counters and the memory
    /// backend its own, so the snapshot always reported size and
    /// evictions as zero.
    #[tokio::test]
    async fn snapshot_reports_memory_size_and_evictions() {
        let cfg = CacheConfig::builder().max_size(2).key_prefix("s:").build();
        let m = CacheManager::new(cfg).unwrap();
        for k in ["a", "b", "c"] {
            m.set(k, &1u32, None, &[]).await.unwrap();
        }
        let snap = m.stats_snapshot();
        assert_eq!(snap.size, 2);
        assert_eq!(snap.evictions, 1);
    }

    /// Regression: associations of expired or evicted entries were never
    /// dropped, so the table map grew with every key ever cached.
    #[tokio::test]
    async fn table_tracking_forgets_evicted_entries() {
        let cfg = CacheConfig::builder().max_size(4).key_prefix("g:").build();
        let m = CacheManager::new(cfg).unwrap();
        for i in 0..(MIN_SWEEP_THRESHOLD * 3) {
            m.set(&format!("k{i}"), &i, None, &["t"]).await.unwrap();
        }
        let tracked = m.keys_for_table("t").await.len();
        assert!(
            tracked <= MIN_SWEEP_THRESHOLD + 1,
            "{tracked} associations tracked for 4 live entries"
        );
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

    /// Regression: the manager's `Debug` printed its config, whose
    /// `redis_url` carries the Redis password.
    #[test]
    fn debug_redacts_the_redis_password() {
        let cfg = CacheConfig::builder()
            .redis_url("redis://:hunter2@cache.example:6379/0")
            .build();
        let shown = format!("{:?}", CacheManager::new(cfg).unwrap());
        assert!(!shown.contains("hunter2"), "{shown}");
        assert!(shown.contains("cache.example"), "{shown}");
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
