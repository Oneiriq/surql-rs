//! Live Redis integration tests for the cache backend.
//!
//! These tests are gated behind the `cache-redis` feature and only
//! execute when `REDIS_TEST_URL` is set in the environment. Start a
//! local Redis via `docker run --rm -p 6379:6379 redis:7` and run
//! `REDIS_TEST_URL=redis://127.0.0.1:6379 cargo test --features cache-redis`.

#![cfg(feature = "cache-redis")]

use std::time::Duration;

use surql::cache::{CacheBackend, RedisCache};

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
