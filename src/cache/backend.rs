//! Cache backend trait.
//!
//! Port of `surql/cache/backends.py::CacheBackend` as an
//! `async_trait::async_trait`-powered Rust trait. Values are stored as
//! JSON (`serde_json::Value`) so backends remain generic across data
//! types and compatible with wire formats used by remote caches.

use async_trait::async_trait;
use serde_json::Value;

use crate::error::{Result, SurqlError};

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

    /// Clear entries matching a glob pattern.
    ///
    /// `*` matches any run of characters, `?` any single character, and
    /// `\` makes the next character literal; every other character,
    /// including `[` and `]`, matches itself. When `pattern` is `None` the
    /// entire cache is cleared. Returns the number of deleted entries.
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

/// Escape `literal` so it matches only itself in a [`CacheBackend::clear`]
/// pattern (and in a Redis `MATCH` pattern, which shares the escapes).
pub(crate) fn escape_glob(literal: &str) -> String {
    let mut out = String::with_capacity(literal.len());
    for c in literal.chars() {
        if matches!(c, '*' | '?' | '[' | ']' | '\\') {
            out.push('\\');
        }
        out.push(c);
    }
    out
}

/// Compile a [`CacheBackend::clear`] glob pattern into an anchored regex.
///
/// Literal runs go through [`regex::escape`], so any character, ASCII or
/// not, matches only itself. A pattern the regex engine still refuses (one
/// past its size limit) is an error, never a match-everything fallback.
pub(crate) fn compile_glob(pattern: &str) -> Result<regex::Regex> {
    let mut out = String::from("(?s)^");
    let mut literal = String::new();
    let mut chars = pattern.chars();
    while let Some(c) = chars.next() {
        match c {
            '*' | '?' => {
                out.push_str(&regex::escape(&literal));
                literal.clear();
                out.push_str(if c == '*' { ".*" } else { "." });
            }
            '\\' => literal.push(chars.next().unwrap_or('\\')),
            c => literal.push(c),
        }
    }
    out.push_str(&regex::escape(&literal));
    out.push('$');
    regex::Regex::new(&out).map_err(|e| SurqlError::Validation {
        reason: format!("invalid cache key pattern {pattern:?}: {e}"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn glob_matches_star() {
        let re = compile_glob("user:*").unwrap();
        assert!(re.is_match("user:123"));
        assert!(re.is_match("user:"));
        assert!(!re.is_match("product:1"));
    }

    #[test]
    fn glob_matches_question() {
        let re = compile_glob("a?c").unwrap();
        assert!(re.is_match("abc"));
        assert!(re.is_match("axc"));
        assert!(!re.is_match("abbc"));
    }

    #[test]
    fn glob_escapes_regex_metachars() {
        let re = compile_glob("foo.bar").unwrap();
        assert!(re.is_match("foo.bar"));
        assert!(!re.is_match("fooxbar"));
    }

    /// Regression: every non-alphanumeric character was backslash-escaped
    /// by hand, so `\é` failed to compile and the `.*` fallback matched
    /// every key, and `\<` / `\>` compiled as word boundaries.
    #[test]
    fn glob_is_literal_for_non_ascii_and_angle_brackets() {
        let re = compile_glob("café:*").unwrap();
        assert!(re.is_match("café:1"));
        assert!(!re.is_match("user:1"));
        let re = compile_glob("a<b>*").unwrap();
        assert!(re.is_match("a<b>c"));
        assert!(!re.is_match("ab"));
        let re = compile_glob("[x]").unwrap();
        assert!(re.is_match("[x]"));
        assert!(!re.is_match("x"));
    }

    #[test]
    fn glob_backslash_makes_the_next_char_literal() {
        let re = compile_glob(r"a\*b").unwrap();
        assert!(re.is_match("a*b"));
        assert!(!re.is_match("axxb"));
        let re = compile_glob(&format!("{}*", escape_glob("p*[1]?\\"))).unwrap();
        assert!(re.is_match("p*[1]?\\key"));
        assert!(!re.is_match("pz[1]x\\key"));
    }

    #[test]
    fn glob_star_spans_newlines() {
        assert!(compile_glob("a*").unwrap().is_match("a\nb"));
    }
}
