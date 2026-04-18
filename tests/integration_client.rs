//! Integration tests for the async [`DatabaseClient`].
//!
//! Gated on the `SURREAL_URL` env var matching the CI docker job:
//!
//! ```text
//! docker run -d -p 8000:8000 surrealdb/surrealdb:v3.0.5 start --user root --pass root memory
//! SURREAL_URL=ws://localhost:8000 SURREAL_USER=root SURREAL_PASS=root \
//!   cargo test --all-features --test integration_client
//! ```
//!
//! Tests bail with `"skipped: SURREAL_URL not set"` when the variable is
//! absent so `cargo test` stays green in environments without a server.

#![cfg(feature = "client")]

use std::env;
use std::time::Duration;

use futures::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::json;
use surql::connection::{
    ConnectionConfig, DatabaseClient, LiveQuery, RootCredentials, Transaction,
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
    // Use a stable-ish per-test namespace so tests can run in parallel.
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos());
    format!("it_{nanos}")
}

async fn connected_client(database: &str) -> Option<DatabaseClient> {
    let url = env_url()?;
    let cfg = ConnectionConfig::builder()
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
        .expect("valid integration config");
    let client = DatabaseClient::new(cfg).expect("client constructs");
    client.connect().await.expect("connect to local surrealdb");
    Some(client)
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct User {
    name: String,
    age: u32,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct Person {
    name: String,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct Watched {
    value: i64,
}

#[tokio::test]
async fn connect_signin_root_uses_namespace_and_database() {
    let Some(client) = connected_client(&unique_db()).await else {
        println!("skipped: SURREAL_URL not set");
        return;
    };
    assert!(client.is_connected());

    // Already signed in via ConnectionConfig, but explicit signin must
    // also succeed and return a JWT.
    let creds = RootCredentials::new(env_user(), env_pass());
    let token = client.signin(&creds).await.expect("signin root");
    assert!(!token.token.is_empty(), "jwt token must be non-empty");

    assert!(client.health().await.unwrap_or(false));
    client.disconnect().await.unwrap();
}

#[tokio::test]
async fn crud_round_trip() {
    let Some(client) = connected_client(&unique_db()).await else {
        println!("skipped: SURREAL_URL not set");
        return;
    };

    let alice = User {
        name: "alice".into(),
        age: 30,
    };
    let created: User = client
        .create("user:alice", alice.clone())
        .await
        .expect("create succeeds");
    assert_eq!(created.name, "alice");
    assert_eq!(created.age, 30);

    let selected: Vec<User> = client.select("user:alice").await.expect("select");
    assert_eq!(selected.len(), 1);
    assert_eq!(selected[0].name, "alice");

    let merged: User = client
        .merge("user:alice", json!({ "age": 31 }))
        .await
        .expect("merge");
    assert_eq!(merged.age, 31);

    let deleted: Vec<User> = client.delete("user:alice").await.expect("delete");
    assert_eq!(deleted.len(), 1);

    let empty: Vec<User> = client.select("user:alice").await.expect("select empty");
    assert!(empty.is_empty());

    client.disconnect().await.unwrap();
}

#[tokio::test]
async fn raw_query_returns_array() {
    let Some(client) = connected_client(&unique_db()).await else {
        println!("skipped: SURREAL_URL not set");
        return;
    };

    let value = client
        .query("RETURN 1 + 2;")
        .await
        .expect("raw query succeeds");
    let arr = value.as_array().expect("top-level is an array");
    assert_eq!(arr.len(), 1);
    assert_eq!(arr[0], json!(3));
    client.disconnect().await.unwrap();
}

#[tokio::test]
async fn transaction_commit_persists() {
    let Some(client) = connected_client(&unique_db()).await else {
        println!("skipped: SURREAL_URL not set");
        return;
    };

    let mut txn = Transaction::begin(&client).await.expect("begin");
    txn.execute("CREATE person:txn_commit SET name = 'txn'")
        .await
        .expect("create in tx");
    txn.commit().await.expect("commit");

    let rows: Vec<Person> = client
        .select("person:txn_commit")
        .await
        .expect("post-commit select");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].name, "txn");

    client.disconnect().await.unwrap();
}

#[tokio::test]
async fn transaction_rollback_discards_writes() {
    let Some(client) = connected_client(&unique_db()).await else {
        println!("skipped: SURREAL_URL not set");
        return;
    };

    let mut txn = Transaction::begin(&client).await.expect("begin");
    txn.execute("CREATE person:txn_abort SET name = 'abort'")
        .await
        .expect("create in tx");
    txn.rollback().await.expect("rollback");

    let rows: Vec<Person> = client
        .select("person:txn_abort")
        .await
        .expect("post-rollback select");
    assert!(rows.is_empty(), "rolled-back record should not persist");

    client.disconnect().await.unwrap();
}

#[tokio::test]
async fn live_query_receives_change() {
    let Some(client) = connected_client(&unique_db()).await else {
        println!("skipped: SURREAL_URL not set");
        return;
    };

    // Pre-create the table so live SELECT attaches cleanly.
    client
        .query("DEFINE TABLE watched SCHEMALESS;")
        .await
        .expect("define table");

    let mut live: LiveQuery<Watched> = LiveQuery::start(&client, "watched").await.expect("live");

    let writer = client.clone();
    let producer = tokio::spawn(async move {
        // Give the subscription a moment to register server-side.
        tokio::time::sleep(Duration::from_millis(200)).await;
        writer
            .query("CREATE watched:alpha SET value = 1;")
            .await
            .expect("create watched:alpha");
    });

    let notification = tokio::time::timeout(Duration::from_secs(5), live.next())
        .await
        .expect("live notification arrives in time")
        .expect("stream yielded an item")
        .expect("notification not an error");
    assert!(
        !format!("{:?}", notification.action).is_empty(),
        "notification should carry an action"
    );

    producer.await.expect("writer task finished");
    client.disconnect().await.unwrap();
}
