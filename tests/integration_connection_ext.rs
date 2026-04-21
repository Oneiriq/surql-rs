//! Integration coverage for the connection extension layer.
//!
//! Exercises [`ConnectionRegistry`], [`AuthManager`], the [`context`]
//! helpers, and [`StreamingManager`] against a running SurrealDB v3.0.5
//! instance. Follows the same `SURREAL_URL`-gated pattern as the other
//! integration suites so `cargo test` stays green in environments
//! without a server.
//!
//! ```text
//! docker run -d -p 8000:8000 --name surrealdb \
//!   surrealdb/surrealdb:v3.0.5 start --user root --pass root memory
//! SURREAL_URL=ws://localhost:8000 SURREAL_USER=root SURREAL_PASS=root \
//!   cargo test --test integration_connection_ext --features client -- --test-threads=1
//! ```

#![cfg(any(feature = "client", feature = "client-rustls"))]

use std::env;
use std::sync::Arc;
use std::time::Duration;

use surql::connection::{
    connection_override, connection_scope, get_db, AuthManager, ConnectionConfig,
    ConnectionRegistry, DatabaseClient, RootCredentials, StreamingManager,
};

fn env_url() -> Option<String> {
    env::var("SURREAL_URL").ok()
}

fn env_user() -> String {
    env::var("SURREAL_USER").unwrap_or_else(|_| "root".into())
}

fn env_pass() -> String {
    env::var("SURREAL_PASS").unwrap_or_else(|_| "root".into())
}

fn unique_db() -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos());
    format!("it_conn_ext_{nanos}")
}

fn build_config(database: &str) -> Option<ConnectionConfig> {
    let url = env_url()?;
    Some(
        ConnectionConfig::builder()
            .url(url)
            .namespace("it_test")
            .database(database)
            .username(env_user())
            .password(env_pass())
            .timeout(10.0)
            .retry_max_attempts(2)
            .retry_min_wait(0.5)
            .retry_max_wait(2.0)
            .build()
            .expect("valid integration config"),
    )
}

#[tokio::test]
async fn registry_round_trip_against_live_server() {
    let Some(cfg) = build_config(&unique_db()) else {
        println!("skipped: SURREAL_URL not set");
        return;
    };

    let registry = ConnectionRegistry::new();
    let client = registry
        .register("primary", cfg, true, false)
        .await
        .expect("register primary");
    assert!(client.is_connected());

    // Round-trip fetch
    let fetched = registry.get(Some("primary")).await.expect("fetch");
    assert!(Arc::ptr_eq(&client, &fetched));

    // Default should be "primary" (first registered)
    assert_eq!(registry.default_name().await.as_deref(), Some("primary"));
    let default_fetched = registry.get(None).await.expect("default fetch");
    assert!(Arc::ptr_eq(&client, &default_fetched));

    // AuthManager drives signin against the live server
    let am = AuthManager::new();
    let token = am
        .signin(
            client.as_ref(),
            &RootCredentials::new(env_user(), env_pass()),
        )
        .await
        .expect("signin via auth manager");
    assert!(!token.token.is_empty());
    assert!(am.is_authenticated().await);
    assert_eq!(am.current_token().await.map(|t| t.token), Some(token.token));

    // refresh should succeed against a live authenticated client.
    am.refresh(client.as_ref()).await.expect("refresh");

    // context: scope the registered client and fetch via get_db
    connection_scope(client.clone(), async {
        let scoped = get_db().expect("scoped client");
        assert!(scoped.health().await.expect("health"));
    })
    .await;

    // connection_override restores prior client.
    let other_cfg = build_config(&unique_db()).expect("config");
    let other = Arc::new(DatabaseClient::new(other_cfg).expect("other client"));
    connection_scope(client.clone(), async {
        connection_override(other.clone(), async {
            let got = get_db().expect("override client");
            assert!(Arc::ptr_eq(&got, &other));
        })
        .await;
        let got = get_db().expect("primary restored");
        assert!(Arc::ptr_eq(&got, &client));
    })
    .await;

    // invalidate clears cached state and unwinds server session.
    am.invalidate(client.as_ref()).await.expect("invalidate");
    assert!(!am.is_authenticated().await);

    // clean up the registry
    registry.clear().await;
    assert!(registry.list().await.is_empty());
}

#[tokio::test]
async fn streaming_manager_drain_kills_subscriptions() {
    let Some(cfg) = build_config(&unique_db()) else {
        println!("skipped: SURREAL_URL not set");
        return;
    };

    let client = DatabaseClient::new(cfg).expect("client");
    client.connect().await.expect("connect");

    client
        .query("DEFINE TABLE drain_watched SCHEMALESS;")
        .await
        .expect("define table");

    let manager = StreamingManager::new();
    let id = manager
        .spawn::<serde_json::Value, _>(&client, "drain_watched", |_| {})
        .await
        .expect("spawn subscription");
    assert_eq!(manager.count().await, 1);
    assert!(manager.ids().await.contains(&id));

    // Give the server a moment to register the subscription.
    tokio::time::sleep(Duration::from_millis(100)).await;

    manager.drain_all().await;
    assert_eq!(manager.count().await, 0);

    client.disconnect().await.unwrap();
}
