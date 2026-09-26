//! Live Redis integration tests for the cache backend.
//!
//! These tests are gated behind the `cache-redis` feature and only
//! execute when `REDIS_TEST_URL` is set in the environment. Start a
//! local Redis via `docker run --rm -p 6379:6379 redis:7` and run
//! `REDIS_TEST_URL=redis://127.0.0.1:6379 cargo test --features cache-redis`.

#![cfg(feature = "cache-redis")]

use std::time::Duration;

use surql::cache::{CacheBackend, CacheBackendKind, CacheConfig, CacheManager, RedisCache};

fn test_url() -> Option<String> {
    std::env::var("REDIS_TEST_URL").ok()
}

#[tokio::test]
async fn redis_set_get_delete_roundtrip() {
    let Some(url) = test_url() else {
        eprintln!("skipping: REDIS_TEST_URL unset");
        return;
    };
    let cache = RedisCache::new(&url, "surql-test:", 30).unwrap();
    // Ensure clean slate.
    cache.clear(None).await.unwrap();

    cache
        .set("key1", serde_json::json!({"v": 1}), None)
        .await
        .unwrap();
    let got = cache.get("key1").await.unwrap();
    assert_eq!(got, Some(serde_json::json!({"v": 1})));

    assert!(cache.exists("key1").await.unwrap());
    cache.delete("key1").await.unwrap();
    assert!(!cache.exists("key1").await.unwrap());
}

#[tokio::test]
async fn redis_clear_by_pattern() {
    let Some(url) = test_url() else {
        eprintln!("skipping: REDIS_TEST_URL unset");
        return;
    };
    let cache = RedisCache::new(&url, "surql-test-pat:", 30).unwrap();
    cache.clear(None).await.unwrap();

    cache
        .set("user:1", serde_json::json!(1), None)
        .await
        .unwrap();
    cache
        .set("user:2", serde_json::json!(2), None)
        .await
        .unwrap();
    cache
        .set("product:1", serde_json::json!(3), None)
        .await
        .unwrap();

    let removed = cache.clear(Some("user:*")).await.unwrap();
    assert_eq!(removed, 2);
    assert!(cache.exists("product:1").await.unwrap());
}

#[tokio::test]
async fn redis_ttl_expiry() {
    let Some(url) = test_url() else {
        eprintln!("skipping: REDIS_TEST_URL unset");
        return;
    };
    let cache = RedisCache::new(&url, "surql-test-ttl:", 60).unwrap();
    cache.clear(None).await.unwrap();

    cache
        .set("tmp", serde_json::json!("x"), Some(1))
        .await
        .unwrap();
    assert!(cache.exists("tmp").await.unwrap());
    tokio::time::sleep(Duration::from_millis(1200)).await;
    assert!(!cache.exists("tmp").await.unwrap());
}

async fn admin(url: &str) -> redis::aio::MultiplexedConnection {
    redis::Client::open(url)
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap()
}

async fn key_exists(conn: &mut redis::aio::MultiplexedConnection, key: &str) -> bool {
    redis::cmd("EXISTS")
        .arg(key)
        .query_async::<bool>(conn)
        .await
        .unwrap()
}

/// Regression: the multiplexed connection was cached forever, and it never
/// reconnects by itself, so once the server dropped it every later call
/// failed until the process restarted.
#[tokio::test]
async fn redis_recovers_after_the_connection_drops() {
    let Some(url) = test_url() else {
        eprintln!("skipping: REDIS_TEST_URL unset");
        return;
    };
    let cache = RedisCache::new(&url, "surql-test-drop:", 30).unwrap();
    cache.set("k", serde_json::json!(1), None).await.unwrap();

    // Drop every other client connection, the cache's included.
    let mut conn = admin(&url).await;
    let killed: u64 = redis::cmd("CLIENT")
        .arg("KILL")
        .arg("TYPE")
        .arg("normal")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert!(killed >= 1);

    // The call that discovers the drop may fail; the next must not.
    let mut outcome = cache.get("k").await;
    for _ in 0..2 {
        if outcome.is_ok() {
            break;
        }
        outcome = cache.get("k").await;
    }
    assert_eq!(outcome.unwrap(), Some(serde_json::json!(1)));
}

/// Regression: an empty prefix made `clear(None)` send `SCAN MATCH *` and
/// delete every key in the database, other applications' included.
#[tokio::test]
async fn redis_clear_never_reaches_past_the_prefix() {
    let Some(url) = test_url() else {
        eprintln!("skipping: REDIS_TEST_URL unset");
        return;
    };
    let mut conn = admin(&url).await;
    redis::cmd("SET")
        .arg("someone-else:key")
        .arg("1")
        .query_async::<()>(&mut conn)
        .await
        .unwrap();
    redis::cmd("SET")
        .arg("app1:key")
        .arg("1")
        .query_async::<()>(&mut conn)
        .await
        .unwrap();

    let unprefixed = RedisCache::new(&url, "", 30).unwrap();
    assert!(unprefixed.clear(None).await.is_err());
    // `[1]` in a prefix is literal, not a character class matching `1`.
    let bracketed = RedisCache::new(&url, "app[1]:", 30).unwrap();
    bracketed.clear(None).await.unwrap();

    assert!(key_exists(&mut conn, "someone-else:key").await);
    assert!(key_exists(&mut conn, "app1:key").await);
}

/// Regression: the manager prefixed every key and then handed it to a
/// backend built with the same prefix, so Redis held `p:p:key`.
#[tokio::test]
async fn manager_keys_are_prefixed_once() {
    let Some(url) = test_url() else {
        eprintln!("skipping: REDIS_TEST_URL unset");
        return;
    };
    let cfg = CacheConfig::builder()
        .backend(CacheBackendKind::Redis)
        .redis_url(url.clone())
        .key_prefix("surql-test-mgr:")
        .build();
    let manager = CacheManager::new(cfg).unwrap();
    manager.clear().await.unwrap();
    manager.set("k", &1u32, None, &[]).await.unwrap();

    let mut conn = admin(&url).await;
    assert!(key_exists(&mut conn, "surql-test-mgr:k").await);
    assert!(!key_exists(&mut conn, "surql-test-mgr:surql-test-mgr:k").await);
    assert_eq!(manager.clear().await.unwrap(), 1);
}
