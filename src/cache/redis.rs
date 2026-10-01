//! Redis-backed cache implementation.
//!
//! Port of `surql/cache/backends.py::RedisCache`. Uses `redis` 1.x with
//! the `tokio-comp` feature. Values are JSON-encoded on the wire; keys
//! are prefixed per configuration.
//!
//! This backend is gated behind the `cache-redis` feature.

use async_trait::async_trait;
use redis::aio::{ConnectionManager, ConnectionManagerConfig};
use redis::{AsyncCommands, Client, RedisError};
use serde_json::Value;
use tokio::sync::Mutex;

use crate::error::{Result, SurqlError};

use super::backend::{escape_glob, CacheBackend};

/// Connection attempts after the first before an operation reports the
/// server unreachable. Each failed operation starts a new round, so a
/// cache in front of a down server fails fast instead of stalling every
/// caller through a long backoff.
const CONNECT_RETRIES: usize = 1;

/// Redis-backed cache.
///
/// The connection is established on first use and shared by every
/// operation after it. It is a redis `ConnectionManager`: when the server
/// drops it (a restart, a killed client, a network blip), the operation
/// that finds out fails and a new connection is made in the background, so
/// later operations succeed again without anything being rebuilt.
///
/// Every key is stored under `prefix`, and [`CacheBackend::clear`] only
/// ever touches keys under it. With an empty prefix the cache shares the
/// whole Redis database with everything else in it, so `clear(None)` is
/// refused rather than deleting every key there. A [`CacheManager`]
/// applies its own `key_prefix`, so the backend it builds carries none.
///
/// [`CacheManager`]: super::manager::CacheManager
pub struct RedisCache {
    client: Client,
    prefix: String,
    default_ttl_secs: u64,
    connection: Mutex<Option<ConnectionManager>>,
}

impl std::fmt::Debug for RedisCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The client holds the connection info, credentials included.
        f.debug_struct("RedisCache")
            .field("prefix", &self.prefix)
            .field("default_ttl_secs", &self.default_ttl_secs)
            .finish_non_exhaustive()
    }
}

impl RedisCache {
    /// Build a new Redis cache against `url` with the given key prefix
    /// and default TTL (seconds).
    pub fn new(url: &str, prefix: impl Into<String>, default_ttl_secs: u64) -> Result<Self> {
        let client = Client::open(url).map_err(|e| SurqlError::Database {
            reason: format!("redis client open failed: {e}"),
        })?;
        Ok(Self {
            client,
            prefix: prefix.into(),
            default_ttl_secs,
            connection: Mutex::new(None),
        })
    }

    fn prefixed(&self, key: &str) -> String {
        format!("{}{}", self.prefix, key)
    }

    /// The Redis `MATCH` pattern for a [`CacheBackend::clear`] pattern:
    /// the prefix as a literal, then the pattern with its `[` and `]`
    /// made literal (Redis would read them as a character class).
    fn match_pattern(&self, pattern: &str) -> String {
        let mut out = escape_glob(&self.prefix);
        let mut chars = pattern.chars();
        while let Some(c) = chars.next() {
            match c {
                '\\' => {
                    out.push('\\');
                    out.push(chars.next().unwrap_or('\\'));
                }
                '[' | ']' => {
                    out.push('\\');
                    out.push(c);
                }
                c => out.push(c),
            }
        }
        out
    }

    /// The shared connection, established on first use. A handle is cheap
    /// to clone and every clone uses the same connection.
    async fn connection(&self) -> Result<ConnectionManager> {
        let mut guard = self.connection.lock().await;
        if let Some(conn) = guard.as_ref() {
            return Ok(conn.clone());
        }
        let config = ConnectionManagerConfig::new().set_number_of_retries(CONNECT_RETRIES);
        let conn = self
            .client
            .get_connection_manager_with_config(config)
            .await
            .map_err(|e| SurqlError::Connection {
                reason: format!("redis connect failed: {e}"),
            })?;
        *guard = Some(conn.clone());
        Ok(conn)
    }
}

/// Map a failed command. A broken connection needs nothing here: the
/// connection manager has already started replacing it.
fn command_failed(command: &str, err: &RedisError) -> SurqlError {
    SurqlError::Database {
        reason: format!("redis {command} failed: {err}"),
    }
}

#[async_trait]
impl CacheBackend for RedisCache {
    async fn get(&self, key: &str) -> Result<Option<Value>> {
        let mut conn = self.connection().await?;
        let raw: Option<String> = match conn.get(self.prefixed(key)).await {
            Ok(raw) => raw,
            Err(e) => return Err(command_failed("GET", &e)),
        };
        let Some(raw) = raw else { return Ok(None) };
        match serde_json::from_str::<Value>(&raw) {
            Ok(v) => Ok(Some(v)),
            Err(_) => Ok(Some(Value::String(raw))),
        }
    }

    async fn set(&self, key: &str, value: Value, ttl_secs: Option<u64>) -> Result<()> {
        let mut conn = self.connection().await?;
        let prefixed = self.prefixed(key);
        let serialised = serde_json::to_string(&value)?;
        let ttl = ttl_secs.unwrap_or(self.default_ttl_secs);
        let outcome = if ttl == 0 {
            conn.set::<_, _, ()>(&prefixed, serialised).await
        } else {
            conn.set_ex::<_, _, ()>(&prefixed, serialised, ttl).await
        };
        match outcome {
            Ok(()) => Ok(()),
            Err(e) => Err(command_failed("SET", &e)),
        }
    }

    async fn delete(&self, key: &str) -> Result<()> {
        let mut conn = self.connection().await?;
        match conn.del::<_, ()>(self.prefixed(key)).await {
            Ok(()) => Ok(()),
            Err(e) => Err(command_failed("DEL", &e)),
        }
    }

    async fn clear(&self, pattern: Option<&str>) -> Result<usize> {
        let redis_pattern = match pattern {
            None if self.prefix.is_empty() => {
                return Err(SurqlError::Validation {
                    reason: "refusing to clear a Redis cache with an empty key prefix: it \
                             would delete every key in the database"
                        .into(),
                })
            }
            None => self.match_pattern("*"),
            Some(p) => self.match_pattern(p),
        };
        let mut conn = self.connection().await?;

        let mut count = 0usize;
        let mut cursor: u64 = 0;
        loop {
            let scanned: std::result::Result<(u64, Vec<String>), RedisError> = redis::cmd("SCAN")
                .arg(cursor)
                .arg("MATCH")
                .arg(&redis_pattern)
                .arg("COUNT")
                .arg(100)
                .query_async(&mut conn)
                .await;
            let (new_cursor, keys) = match scanned {
                Ok(page) => page,
                Err(e) => return Err(command_failed("SCAN", &e)),
            };
            if !keys.is_empty() {
                if let Err(e) = conn.del::<_, ()>(keys.as_slice()).await {
                    return Err(command_failed("DEL", &e));
                }
                count += keys.len();
            }
            if new_cursor == 0 {
                break;
            }
            cursor = new_cursor;
        }
        Ok(count)
    }

    async fn exists(&self, key: &str) -> Result<bool> {
        let mut conn = self.connection().await?;
        match conn.exists(self.prefixed(key)).await {
            Ok(present) => Ok(present),
            Err(e) => Err(command_failed("EXISTS", &e)),
        }
    }

    async fn close(&self) -> Result<()> {
        let mut guard = self.connection.lock().await;
        *guard = None;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prefixed_key_applies_prefix() {
        let cache = RedisCache::new("redis://127.0.0.1:6379", "surql:", 300).unwrap();
        assert_eq!(cache.prefixed("foo"), "surql:foo");
    }

    #[test]
    fn invalid_url_surfaces_database_error() {
        let err = RedisCache::new("not-a-url", "p:", 30).unwrap_err();
        assert!(matches!(err, SurqlError::Database { .. }));
    }

    /// Regression: the prefix went into `SCAN MATCH` unescaped, so
    /// `app[1]:` matched `app1:*` and cleared another application's keys.
    #[test]
    fn match_pattern_keeps_the_prefix_literal() {
        let cache = RedisCache::new("redis://127.0.0.1:6379", "app[1]*?:", 30).unwrap();
        assert_eq!(cache.match_pattern("*"), r"app\[1\]\*\?:*");
        assert_eq!(
            cache.match_pattern("user:[a]?"),
            r"app\[1\]\*\?:user:\[a\]?"
        );
        assert_eq!(cache.match_pattern(r"a\*b"), r"app\[1\]\*\?:a\*b");
    }

    /// Regression: with an empty prefix, `clear(None)` sent `SCAN MATCH *`
    /// and deleted every key in the Redis database. Refused before any
    /// connection is attempted.
    #[tokio::test]
    async fn clear_all_refuses_an_empty_prefix() {
        let cache = RedisCache::new("redis://127.0.0.1:1", "", 30).unwrap();
        let err = cache.clear(None).await.unwrap_err();
        assert!(matches!(err, SurqlError::Validation { .. }), "{err}");
    }

    /// The connection manager retries with a backoff; the cache keeps that
    /// short, so an unreachable server is an error, not a stall.
    #[tokio::test]
    async fn an_unreachable_server_fails_fast() {
        let cache = RedisCache::new("redis://127.0.0.1:1", "p:", 30).unwrap();
        let started = std::time::Instant::now();
        let err = cache.get("k").await.unwrap_err();
        assert!(matches!(err, SurqlError::Connection { .. }), "{err}");
        // Two attempts. The manager's default, seven attempts with a
        // doubling backoff, takes over six seconds before the first error.
        assert!(started.elapsed() < std::time::Duration::from_secs(8));
    }

    #[test]
    fn debug_redacts_the_url_credentials() {
        let cache = RedisCache::new("redis://svc:hunter2@127.0.0.1:6379", "p:", 30).unwrap();
        let shown = format!("{cache:?}");
        assert!(!shown.contains("hunter2"), "{shown}");
        assert!(!shown.contains("svc"), "{shown}");
    }
}
