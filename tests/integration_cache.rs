//! Integration tests for the `cache` module.
//!
//! Exercises [`CacheManager`] end-to-end (get/set, TTL, invalidation,
//! table tracking) and the global helpers (`configure_cache`,
//! `invalidate`, `clear_cache`, `close_cache`).

#![cfg(feature = "cache")]

use std::time::Duration;

use surql::cache::{
    cache_key_for, cached, cached_with, clear_cache, close_cache, configure_cache,
    get_cache_manager, invalidate, is_cached, CacheBackendKind, CacheConfig, CacheManager,
    CacheOptions,
};

fn fresh_manager(prefix: &str) -> CacheManager {
    let cfg = CacheConfig::builder()
        .backend(CacheBackendKind::Memory)
        .default_ttl_secs(30)
        .key_prefix(prefix)
        .build();
    CacheManager::new(cfg).unwrap()
}

#[tokio::test]
async fn manager_roundtrip_typed_value() {
    let m = fresh_manager("it_rt:");
    m.set(
        "user:1",
        &serde_json::json!({"name": "alice"}),
        None,
        &["user"],
    )
    .await
    .unwrap();
    let v: Option<serde_json::Value> = m.get("user:1").await.unwrap();
    assert_eq!(v.unwrap()["name"], "alice");
    let stats = m.stats_snapshot();
    assert_eq!(stats.hits, 1);
    assert_eq!(stats.misses, 0);
}

#[tokio::test]
async fn manager_invalidates_by_table() {
    let m = fresh_manager("it_tab:");
    m.set("u1", &1u32, None, &["user"]).await.unwrap();
    m.set("u2", &2u32, None, &["user"]).await.unwrap();
    m.set("p1", &3u32, None, &["product"]).await.unwrap();
    let removed = m.invalidate_table("user").await.unwrap();
    assert_eq!(removed, 2);
    assert_eq!(m.get::<u32>("u1").await.unwrap(), None);
    assert_eq!(m.get::<u32>("p1").await.unwrap(), Some(3));
}

#[tokio::test]
async fn ttl_expiry_observed_in_manager() {
    let cfg = CacheConfig::builder()
        .default_ttl_secs(60)
        .key_prefix("it_ttl:")
        .build();
    let m = CacheManager::new(cfg).unwrap();
    m.set("tmp", &42u32, Some(1), &[]).await.unwrap();
    assert_eq!(m.get::<u32>("tmp").await.unwrap(), Some(42));
    tokio::time::sleep(Duration::from_millis(1100)).await;
    assert_eq!(m.get::<u32>("tmp").await.unwrap(), None);
}

#[tokio::test]
async fn cache_key_for_produces_stable_key() {
    let a = cache_key_for("test", "fn", &[1, 2, 3]).unwrap();
    let b = cache_key_for("test", "fn", &[1, 2, 3]).unwrap();
    assert_eq!(a, b);
    let c = cache_key_for("test", "fn", &[1, 2, 4]).unwrap();
    assert_ne!(a, c);
}

#[tokio::test]
async fn cached_with_avoids_refetch() {
    let m = fresh_manager("it_cw:");
    let counter = std::sync::atomic::AtomicU32::new(0);
    let compute = || async {
        counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok::<u32, surql::SurqlError>(100)
    };

    let v1: u32 = cached_with(&m, "k", None, compute).await.unwrap();
    let v2: u32 = cached_with(&m, "k", None, || async {
        counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(100)
    })
    .await
    .unwrap();
    assert_eq!(v1, v2);
    assert_eq!(counter.load(std::sync::atomic::Ordering::SeqCst), 1);
}

#[tokio::test]
async fn options_validation_rejects_zero_ttl() {
    let err = CacheOptions::new().with_ttl_secs(0).unwrap_err();
    assert!(err.to_string().contains("TTL must be"));
}

#[tokio::test]
async fn global_helpers_configure_and_invalidate() {
    let cfg = CacheConfig::builder().key_prefix("it_gbl:").build();
    let manager = configure_cache(cfg).unwrap();
    manager.clear().await.unwrap();
    manager.set("user:1", &1u32, None, &[]).await.unwrap();
    manager.set("user:2", &2u32, None, &[]).await.unwrap();
    manager.set("prod:1", &3u32, None, &[]).await.unwrap();

    let n = invalidate(None, None, Some("user:*")).await.unwrap();
    assert_eq!(n, 2);

    let v: u32 = cached("new-key", None, || async { Ok(42u32) })
        .await
        .unwrap();
    assert_eq!(v, 42);

    // Global clear_cache wipes everything including table tracking.
    let cleared = clear_cache().await.unwrap();
    assert!(cleared >= 1);
    assert!(get_cache_manager().is_some());
    close_cache().await.unwrap();
    assert!(get_cache_manager().is_none());
}

#[tokio::test]
async fn is_cached_returns_false_when_no_manager() {
    // Even if prior test installed a manager, call close_cache to make
    // `is_cached` observe the absent state; it should not error.
    close_cache().await.unwrap();
    assert!(!is_cached("nope").await.unwrap());
}
