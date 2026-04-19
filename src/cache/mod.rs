//! Query result caching.
//!
//! Port of `surql/cache/` from `oneiriq-surql` (Python). The module is
//! gated behind the `cache` feature to keep the default build free of
//! async runtime and `async-trait` dependencies.
//!
//! ## Components
//!
//! - [`CacheBackend`]: abstract backend trait (`async_trait`).
//! - [`MemoryCache`]: in-process `HashMap` + TTL backend.
//! - [`RedisCache`]: Redis 7-compatible backend (requires `cache-redis`).
//! - [`CacheManager`]: high-level operations (get/set/invalidate/stats).
//! - [`CacheConfig`] / [`CacheConfigBuilder`]: configuration types.
//! - [`CacheStats`]: atomic hit/miss/size/eviction counters.
//! - [`cached`]: idiomatic Rust replacement for Python's `@cache_query`
//!   decorator.
//!
//! ## Global manager
//!
//! Most applications configure a single global manager at startup via
//! [`configure_cache`] and then use the free functions
//! ([`invalidate`], [`clear_cache`], [`close_cache`]) throughout the
//! codebase. Tests can instantiate a [`CacheManager`] directly.
//!
//! ## Examples
//!
//! ```no_run
//! # #[cfg(feature = "cache")] {
//! use surql::cache::{cached, configure_cache, CacheConfig};
//! # async fn demo() -> surql::error::Result<()> {
//! configure_cache(CacheConfig::default())?;
//!
//! let users: Vec<String> = cached("users:active", Some(60), || async {
//!     // expensive fetch here
//!     Ok(vec!["alice".to_string(), "bob".to_string()])
//! }).await?;
//! # Ok(()) }
//! # }
//! ```

pub mod backend;
pub mod config;
pub mod decorator;
pub mod manager;
pub mod memory;
#[cfg(feature = "cache-redis")]
pub mod redis;
pub mod stats;

use std::sync::{Arc, OnceLock, RwLock};

pub use backend::CacheBackend;
pub use config::{CacheBackendKind, CacheConfig, CacheConfigBuilder, CacheOptions};
pub use decorator::{cache_key_for, cached, cached_with, is_cached};
pub use manager::CacheManager;
pub use memory::MemoryCache;
#[cfg(feature = "cache-redis")]
pub use redis::RedisCache;
pub use stats::{CacheStats, CacheStatsSnapshot};

use crate::error::Result;

static MANAGER: OnceLock<RwLock<Option<CacheManager>>> = OnceLock::new();

fn slot() -> &'static RwLock<Option<CacheManager>> {
    MANAGER.get_or_init(|| RwLock::new(None))
}

/// Install a global [`CacheManager`] from the provided configuration.
///
/// Replaces any existing manager. Returns the newly-installed handle.
pub fn configure_cache(config: CacheConfig) -> Result<CacheManager> {
    let manager = CacheManager::new(config)?;
    let clone = manager.clone();
    let mut guard = slot()
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    *guard = Some(clone);
    Ok(manager)
}

/// Install a caller-constructed [`CacheManager`] as the global handle.
pub fn set_cache_manager(manager: CacheManager) {
    let mut guard = slot()
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    *guard = Some(manager);
}

/// Get a clone of the currently-configured global [`CacheManager`].
pub fn get_cache_manager() -> Option<CacheManager> {
    let guard = slot()
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    guard.clone()
}

/// Get the global manager, initialising a default one if needed.
///
/// Intended for opportunistic use inside [`cached`] when no explicit
/// configuration has been performed. Uses [`CacheConfig::default`].
pub fn get_or_init_manager() -> CacheManager {
    if let Some(m) = get_cache_manager() {
        return m;
    }
    // Safe: default config cannot fail.
    let m = CacheManager::new(CacheConfig::default()).expect("default cache manager");
    set_cache_manager(m.clone());
    m
}

/// Invalidate one or more entries on the global manager.
///
/// Only one of `key`, `table`, or `pattern` is honoured per call; if
/// several are provided they are each applied in turn. Returns the
/// total number of entries removed, or `0` when no manager is set.
pub async fn invalidate(
    key: Option<&str>,
    table: Option<&str>,
    pattern: Option<&str>,
) -> Result<usize> {
    let Some(manager) = get_cache_manager() else {
        return Ok(0);
    };
    let mut total = 0usize;
    if let Some(k) = key {
        total += manager.invalidate_key(k).await?;
    }
    if let Some(t) = table {
        total += manager.invalidate_table(t).await?;
    }
    if let Some(p) = pattern {
        total += manager.invalidate_pattern(p).await?;
    }
    Ok(total)
}

/// Clear every entry in the global manager's backend.
pub async fn clear_cache() -> Result<usize> {
    match get_cache_manager() {
        Some(m) => m.clear().await,
        None => Ok(0),
    }
}

/// Close the global manager (flushing / disconnecting) and drop it.
pub async fn close_cache() -> Result<()> {
    let existing = {
        let mut guard = slot()
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        guard.take()
    };
    if let Some(m) = existing {
        m.close().await?;
    }
    Ok(())
}

/// Replace the global manager with a shared backend. Escape hatch for
/// advanced composition scenarios (e.g. wiring a custom backend into
/// the global slot without rebuilding the config).
pub fn install_backend(config: CacheConfig, backend: Arc<dyn CacheBackend>) -> CacheManager {
    let manager = CacheManager::with_backend(config, backend);
    set_cache_manager(manager.clone());
    manager
}

#[cfg(test)]
mod tests {
    //! Tests for the global slot serialize via a shared async mutex
    //! because Cargo runs tests in-process (so `OnceLock` and the
    //! `RwLock<Option<_>>` inside are shared between concurrent tasks).

    use super::*;
    use tokio::sync::Mutex;

    static GLOBAL_LOCK: Mutex<()> = Mutex::const_new(());

    #[tokio::test]
    async fn configure_and_get() {
        let _guard = GLOBAL_LOCK.lock().await;
        let cfg = CacheConfig::builder().key_prefix("cgt:").build();
        let m = configure_cache(cfg).unwrap();
        let from_slot = get_cache_manager().unwrap();
        assert_eq!(m.config().key_prefix, from_slot.config().key_prefix);
    }

    #[tokio::test]
    async fn invalidate_by_pattern_via_global() {
        let _guard = GLOBAL_LOCK.lock().await;
        let cfg = CacheConfig::builder().key_prefix("inv:").build();
        let m = configure_cache(cfg).unwrap();
        m.clear().await.unwrap();
        m.set("user:1", &1u32, None, &[]).await.unwrap();
        m.set("user:2", &2u32, None, &[]).await.unwrap();
        m.set("prod:1", &3u32, None, &[]).await.unwrap();
        let n = invalidate(None, None, Some("user:*")).await.unwrap();
        assert_eq!(n, 2);
    }

    #[tokio::test]
    async fn close_cache_drops_manager() {
        let _guard = GLOBAL_LOCK.lock().await;
        let cfg = CacheConfig::builder().key_prefix("cls:").build();
        let _ = configure_cache(cfg).unwrap();
        assert!(get_cache_manager().is_some());
        close_cache().await.unwrap();
        assert!(get_cache_manager().is_none());
    }
}
