//! Cache configuration types.
//!
//! Port of `surql/cache/config.py`. Provides the global [`CacheConfig`]
//! and per-query [`CacheOptions`] value objects used by the cache
//! subsystem.

use serde::{Deserialize, Serialize};

use crate::error::{Result, SurqlError};

/// Supported cache backend types.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CacheBackendKind {
    /// In-process [`MemoryCache`](super::memory::MemoryCache).
    #[default]
    Memory,
    /// Redis-backed distributed cache (requires `cache-redis` feature).
    Redis,
}

/// Global cache configuration.
///
/// Constructed directly, via [`CacheConfig::default`] or through
/// [`CacheConfigBuilder`]. Passed to
/// [`CacheManager::new`](super::manager::CacheManager::new) or the
/// top-level [`configure_cache`](super::configure_cache) function.
///
/// ## Examples
///
/// ```
/// # #[cfg(feature = "cache")] {
/// use surql::cache::{CacheBackendKind, CacheConfig};
///
/// let cfg = CacheConfig::builder()
///     .enabled(true)
///     .backend(CacheBackendKind::Memory)
///     .default_ttl_secs(600)
///     .max_size(2000)
///     .build();
/// assert_eq!(cfg.default_ttl_secs, 600);
/// # }
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CacheConfig {
    /// Whether caching is enabled globally.
    pub enabled: bool,
    /// Backend selection.
    pub backend: CacheBackendKind,
    /// Default time-to-live in seconds (default: 5 minutes).
    pub default_ttl_secs: u64,
    /// Maximum number of entries for the in-memory backend.
    pub max_size: usize,
    /// Redis connection URL for the Redis backend.
    pub redis_url: String,
    /// Prefix applied to all cache keys.
    pub key_prefix: String,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            backend: CacheBackendKind::Memory,
            default_ttl_secs: 300,
            max_size: 1000,
            redis_url: "redis://localhost:6379".into(),
            key_prefix: "surql:".into(),
        }
    }
}

impl CacheConfig {
    /// Start a builder with the default field values.
    pub fn builder() -> CacheConfigBuilder {
        CacheConfigBuilder::default()
    }
}

/// Fluent builder for [`CacheConfig`].
#[derive(Debug, Clone, Default)]
pub struct CacheConfigBuilder {
    inner: CacheConfig,
}

impl CacheConfigBuilder {
    /// Toggle the global cache enabled flag.
    pub fn enabled(mut self, enabled: bool) -> Self {
        self.inner.enabled = enabled;
        self
    }

    /// Select the cache backend kind.
    pub fn backend(mut self, backend: CacheBackendKind) -> Self {
        self.inner.backend = backend;
        self
    }

    /// Set the default TTL in seconds.
    pub fn default_ttl_secs(mut self, secs: u64) -> Self {
        self.inner.default_ttl_secs = secs;
        self
    }

    /// Set the maximum size for the memory backend.
    pub fn max_size(mut self, n: usize) -> Self {
        self.inner.max_size = n;
        self
    }

    /// Set the Redis connection URL.
    pub fn redis_url(mut self, url: impl Into<String>) -> Self {
        self.inner.redis_url = url.into();
        self
    }

    /// Set the global key prefix.
    pub fn key_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.inner.key_prefix = prefix.into();
        self
    }

    /// Finalise and return the configuration.
    pub fn build(self) -> CacheConfig {
        self.inner
    }
}

/// Per-query cache options.
///
/// Mirror of the Python `CacheOptions` dataclass; passed through call
/// sites that want to override TTL, provide an explicit key, or
/// register table-based invalidation triggers.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CacheOptions {
    /// TTL override in seconds (uses the manager default when `None`).
    pub ttl_secs: Option<u64>,
    /// Explicit cache key (auto-generated when `None`).
    pub key: Option<String>,
    /// Tables that, when modified, should invalidate this entry.
    pub invalidate_on: Vec<String>,
}

impl CacheOptions {
    /// Construct an empty options object.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the TTL in seconds. Returns an error if `ttl == 0`.
    pub fn with_ttl_secs(mut self, ttl: u64) -> Result<Self> {
        if ttl == 0 {
            return Err(SurqlError::Validation {
                reason: "TTL must be a positive integer".into(),
            });
        }
        self.ttl_secs = Some(ttl);
        Ok(self)
    }

    /// Set an explicit key.
    pub fn with_key(mut self, key: impl Into<String>) -> Self {
        self.key = Some(key.into());
        self
    }

    /// Associate one or more tables for invalidation tracking.
    pub fn with_tables<I, S>(mut self, tables: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.invalidate_on = tables.into_iter().map(Into::into).collect();
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_match_python_port() {
        let cfg = CacheConfig::default();
        assert!(cfg.enabled);
        assert_eq!(cfg.backend, CacheBackendKind::Memory);
        assert_eq!(cfg.default_ttl_secs, 300);
        assert_eq!(cfg.max_size, 1000);
        assert_eq!(cfg.redis_url, "redis://localhost:6379");
        assert_eq!(cfg.key_prefix, "surql:");
    }

    #[test]
    fn builder_overrides_fields() {
        let cfg = CacheConfig::builder()
            .enabled(false)
            .backend(CacheBackendKind::Redis)
            .default_ttl_secs(60)
            .max_size(42)
            .redis_url("redis://remote:6379")
            .key_prefix("test:")
            .build();
        assert!(!cfg.enabled);
        assert_eq!(cfg.backend, CacheBackendKind::Redis);
        assert_eq!(cfg.default_ttl_secs, 60);
        assert_eq!(cfg.max_size, 42);
        assert_eq!(cfg.redis_url, "redis://remote:6379");
        assert_eq!(cfg.key_prefix, "test:");
    }

    #[test]
    fn options_validate_ttl() {
        assert!(CacheOptions::new().with_ttl_secs(0).is_err());
        assert!(CacheOptions::new().with_ttl_secs(30).is_ok());
    }

    #[test]
    fn options_chain() {
        let opts = CacheOptions::new()
            .with_key("k")
            .with_tables(["user", "role"]);
        assert_eq!(opts.key.as_deref(), Some("k"));
        assert_eq!(opts.invalidate_on, vec!["user", "role"]);
    }

    #[test]
    fn serde_roundtrip() {
        let cfg = CacheConfig::default();
        let json = serde_json::to_string(&cfg).unwrap();
        let back: CacheConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(cfg, back);
    }
}
