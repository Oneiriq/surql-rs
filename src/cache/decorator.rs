//! `cache_query` equivalents for Rust.
//!
//! Port of `surql/cache/decorator.py`. Rust has no direct analogue to
//! Python's function decorator, so instead we provide:
//!
//! - [`cached`]: async wrapper that evaluates a fetch closure if the
//!   key is not already populated in the global cache manager.
//! - [`cache_key_for`]: stable key generation for a module/name and
//!   serialisable arguments.
//! - [`is_cached`]: report whether the global cache manager has a
//!   live entry for the generated key.

use std::future::Future;

use serde::Serialize;
use sha2::{Digest, Sha256};

use crate::error::Result;

use super::manager::CacheManager;
use super::{get_cache_manager, get_or_init_manager};

/// Evaluate `fetch` only if `key` is absent from the global cache.
///
/// Uses the globally-configured [`CacheManager`] (see
/// [`configure_cache`](super::configure_cache)). If no manager is
/// configured the closure is invoked every call and the result is
/// returned directly.
pub async fn cached<T, F, Fut>(key: &str, ttl_secs: Option<u64>, fetch: F) -> Result<T>
where
    T: Serialize + for<'de> serde::Deserialize<'de>,
    F: FnOnce() -> Fut,
    Fut: Future<Output = Result<T>>,
{
    let Some(manager) = get_cache_manager() else {
        return fetch().await;
    };
    manager.get_or_set(key, ttl_secs, &[], fetch).await
}

/// Evaluate `fetch` through `manager` rather than the global instance.
pub async fn cached_with<T, F, Fut>(
    manager: &CacheManager,
    key: &str,
    ttl_secs: Option<u64>,
    fetch: F,
) -> Result<T>
where
    T: Serialize + for<'de> serde::Deserialize<'de>,
    F: FnOnce() -> Fut,
    Fut: Future<Output = Result<T>>,
{
    manager.get_or_set(key, ttl_secs, &[], fetch).await
}

/// Generate a stable cache key from a module/function identifier and
/// a list of JSON-serialisable arguments.
///
/// Mirrors `cache_key_for` from the Python port; the digest is a
/// SHA-256 of the combined identifier + sorted-argument JSON.
pub fn cache_key_for<T: Serialize + ?Sized>(module: &str, name: &str, args: &T) -> Result<String> {
    let args_json = serde_json::to_string(args)?;
    let mut hasher = Sha256::new();
    hasher.update(module.as_bytes());
    hasher.update(b".");
    hasher.update(name.as_bytes());
    hasher.update(b"(");
    hasher.update(args_json.as_bytes());
    hasher.update(b")");
    let digest = hasher.finalize();
    let hex: String = digest.iter().take(8).fold(String::new(), |mut acc, byte| {
        use std::fmt::Write;
        let _ = write!(acc, "{byte:02x}");
        acc
    });
    Ok(format!("{module}.{name}:{hex}"))
}

/// Report whether the global cache has a live entry under `key`.
///
/// Returns `Ok(false)` if no manager is configured.
pub async fn is_cached(key: &str) -> Result<bool> {
    match get_cache_manager() {
        Some(m) => m.exists(key).await,
        None => Ok(false),
    }
}

/// Internal helper: get-or-init the manager, used by the decorator
/// when `configure_cache` has not been called but we still want a
/// default memory cache.
#[allow(dead_code)]
pub(crate) fn ensure_default_manager() -> CacheManager {
    get_or_init_manager()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cache_key_for_is_stable() {
        let k1 = cache_key_for("mod", "fun", &("a", 1)).unwrap();
        let k2 = cache_key_for("mod", "fun", &("a", 1)).unwrap();
        assert_eq!(k1, k2);
    }

    #[test]
    fn cache_key_for_differs_on_args() {
        let k1 = cache_key_for("mod", "fun", &("a", 1)).unwrap();
        let k2 = cache_key_for("mod", "fun", &("a", 2)).unwrap();
        assert_ne!(k1, k2);
    }

    #[test]
    fn cache_key_for_format() {
        let k = cache_key_for("mymod", "myfn", &serde_json::json!({})).unwrap();
        assert!(k.starts_with("mymod.myfn:"));
        let hash = k.split(':').nth(1).unwrap();
        assert_eq!(hash.len(), 16);
    }
}
