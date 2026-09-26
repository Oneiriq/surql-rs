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

use super::get_cache_manager;
use super::manager::CacheManager;

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
/// SHA-256 of the JSON array `[module, name, args]`. Hashing a JSON
/// array keeps the parts apart, so `("a.b", "c")` and `("a", "b.c")`
/// get different keys even though both read `a.b.c` in the key text.
pub fn cache_key_for<T: Serialize + ?Sized>(module: &str, name: &str, args: &T) -> Result<String> {
    let identity = serde_json::to_string(&(module, name, args))?;
    let digest = Sha256::digest(identity.as_bytes());
    let hex = digest.iter().take(8).fold(String::new(), |mut acc, byte| {
        acc.push_str(&format!("{byte:02x}"));
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

    /// Regression: the hash input was `module.name(args)`, so a dot moved
    /// between module and name hashed the same text and the two calls
    /// shared one cache entry.
    #[test]
    fn cache_key_for_keeps_module_and_name_apart() {
        let k1 = cache_key_for("a.b", "c", &()).unwrap();
        let k2 = cache_key_for("a", "b.c", &()).unwrap();
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
