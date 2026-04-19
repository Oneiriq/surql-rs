//! Cache backend trait.
//!
//! Port of `surql/cache/backends.py::CacheBackend` as an
//! `async_trait::async_trait`-powered Rust trait. Values are stored as
//! JSON (`serde_json::Value`) so backends remain generic across data
//! types and compatible with wire formats used by remote caches.

use async_trait::async_trait;
use serde_json::Value;

use crate::error::Result;

/// Abstract cache backend.
///
/// All cache backends implement this trait so the
/// [`CacheManager`](super::manager::CacheManager) can operate over them
/// uniformly. `Value` is used as the wire-level representation.
#[async_trait]
pub trait CacheBackend: Send + Sync {
    /// Retrieve a value from the cache.
    ///
    /// Returns `Ok(None)` if the key does not exist or has expired.
    async fn get(&self, key: &str) -> Result<Option<Value>>;

    /// Insert or replace a value in the cache.
    ///
    /// `ttl_secs` of `None` uses the backend's default TTL.
    async fn set(&self, key: &str, value: Value, ttl_secs: Option<u64>) -> Result<()>;

    /// Remove a key. A no-op when the key is missing.
    async fn delete(&self, key: &str) -> Result<()>;

    /// Clear entries matching a glob pattern (`*` and `?`).
    ///
    /// When `pattern` is `None` the entire cache is cleared. Returns
    /// the number of deleted entries.
    async fn clear(&self, pattern: Option<&str>) -> Result<usize>;

    /// Report whether `key` exists and has not expired.
    async fn exists(&self, key: &str) -> Result<bool>;

    /// Release backend resources (close connections, flush buffers).
    ///
    /// Default implementation is a no-op.
    async fn close(&self) -> Result<()> {
        Ok(())
    }
}

/// Compile a glob pattern into an equivalent regex.
///
/// Supports `*` (any run) and `?` (single char). Used internally by the
/// memory backend to honour the Python `fnmatch` semantics.
pub(crate) fn compile_glob(pattern: &str) -> regex::Regex {
    let mut out = String::from("^");
    for ch in pattern.chars() {
        match ch {
            '*' => out.push_str(".*"),
            '?' => out.push('.'),
            c if c.is_ascii_alphanumeric() => out.push(c),
            c => {
                out.push('\\');
                out.push(c);
            }
        }
    }
    out.push('$');
    // Invariant: we only produce characters we escape ourselves, so the
    // resulting string is a valid regex. Fall back to `.*` on the
    // (unreachable) parse error path to avoid panics.
    regex::Regex::new(&out).unwrap_or_else(|_| regex::Regex::new(".*").expect("trivial regex"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn glob_matches_star() {
        let re = compile_glob("user:*");
        assert!(re.is_match("user:123"));
        assert!(re.is_match("user:"));
        assert!(!re.is_match("product:1"));
    }

    #[test]
    fn glob_matches_question() {
        let re = compile_glob("a?c");
        assert!(re.is_match("abc"));
        assert!(re.is_match("axc"));
        assert!(!re.is_match("abbc"));
    }

    #[test]
    fn glob_escapes_regex_metachars() {
        let re = compile_glob("foo.bar");
        assert!(re.is_match("foo.bar"));
        assert!(!re.is_match("fooxbar"));
    }
}
