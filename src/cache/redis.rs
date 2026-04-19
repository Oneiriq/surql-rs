//! Redis-backed cache implementation.
//!
//! Port of `surql/cache/backends.py::RedisCache`. Uses `redis` 0.27 with
//! the `tokio-comp` feature. Values are JSON-encoded on the wire; keys
//! are prefixed per configuration.
//!
//! This backend is gated behind the `cache-redis` feature.

use async_trait::async_trait;
use redis::{AsyncCommands, Client};
use serde_json::Value;
use tokio::sync::Mutex;

use crate::error::{Result, SurqlError};

use super::backend::CacheBackend;

/// Redis-backed cache.
///
/// The underlying connection is lazily established on first use and
/// reused across subsequent operations.
#[derive(Debug)]
pub struct RedisCache {
    client: Client,
    prefix: String,
    default_ttl_secs: u64,
    connection: Mutex<Option<redis::aio::MultiplexedConnection>>,
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

    async fn connection(&self) -> Result<redis::aio::MultiplexedConnection> {
        let mut guard = self.connection.lock().await;
        if let Some(conn) = guard.as_ref() {
            return Ok(conn.clone());
        }
        let conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| SurqlError::Connection {
                reason: format!("redis connect failed: {e}"),
            })?;
        *guard = Some(conn.clone());
        Ok(conn)
    }
}

#[async_trait]
impl CacheBackend for RedisCache {
    async fn get(&self, key: &str) -> Result<Option<Value>> {
        let mut conn = self.connection().await?;
        let prefixed = self.prefixed(key);
        let raw: Option<String> = conn
            .get(&prefixed)
            .await
            .map_err(|e| SurqlError::Database {
                reason: format!("redis GET failed: {e}"),
            })?;
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
        if ttl == 0 {
            conn.set::<_, _, ()>(&prefixed, serialised)
                .await
                .map_err(|e| SurqlError::Database {
                    reason: format!("redis SET failed: {e}"),
                })?;
        } else {
            conn.set_ex::<_, _, ()>(&prefixed, serialised, ttl)
                .await
                .map_err(|e| SurqlError::Database {
                    reason: format!("redis SETEX failed: {e}"),
                })?;
        }
        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<()> {
        let mut conn = self.connection().await?;
        let prefixed = self.prefixed(key);
        conn.del::<_, ()>(&prefixed)
            .await
            .map_err(|e| SurqlError::Database {
                reason: format!("redis DEL failed: {e}"),
            })?;
        Ok(())
    }

    async fn clear(&self, pattern: Option<&str>) -> Result<usize> {
        let mut conn = self.connection().await?;
        let redis_pattern = match pattern {
            None => format!("{}*", self.prefix),
            Some(p) => self.prefixed(p),
        };

        let mut count = 0usize;
        let mut cursor: u64 = 0;
        loop {
            let (new_cursor, keys): (u64, Vec<String>) = redis::cmd("SCAN")
                .arg(cursor)
                .arg("MATCH")
                .arg(&redis_pattern)
                .arg("COUNT")
                .arg(100)
                .query_async(&mut conn)
                .await
                .map_err(|e| SurqlError::Database {
                    reason: format!("redis SCAN failed: {e}"),
                })?;
            if !keys.is_empty() {
                conn.del::<_, ()>(keys.as_slice())
                    .await
                    .map_err(|e| SurqlError::Database {
                        reason: format!("redis DEL failed: {e}"),
                    })?;
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
        let prefixed = self.prefixed(key);
        let present: bool = conn
            .exists(&prefixed)
            .await
            .map_err(|e| SurqlError::Database {
                reason: format!("redis EXISTS failed: {e}"),
            })?;
        Ok(present)
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
}
