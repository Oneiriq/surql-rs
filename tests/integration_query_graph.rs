//! Integration tests for the batch / graph / `GraphQuery` modules.
//!
//! Gated on the `SURREAL_URL` env var so `cargo test` stays green when no
//! SurrealDB server is reachable. Exercise with:
//!
//! ```text
//! docker run -d -p 8000:8000 surrealdb/surrealdb:v3.0.5 start --user root --pass root memory
//! SURREAL_URL=ws://localhost:8000 SURREAL_USER=root SURREAL_PASS=root \
//!   cargo test --all-features --test integration_query_graph
//! ```
//!
//! Tables use `person` rather than `user` because `USER` is reserved in
//! the SurrealDB v3 parser for identity management (causes parse errors
//! in bare `UPSERT INTO user ...` / `INSERT INTO user ...` statements
//! even without a `user:` record-id prefix).

#![cfg(any(feature = "client", feature = "client-rustls"))]

use std::env;
use std::sync::atomic::{AtomicU64, Ordering};

use serde_json::json;
use surql::connection::{ConnectionConfig, DatabaseClient};
use surql::query::{batch, graph, GraphQuery};

fn env_url() -> Option<String> {
    env::var("SURREAL_URL").ok()
}

fn env_user() -> String {
    env::var("SURREAL_USER").unwrap_or_else(|_| "root".into())
}

fn env_pass() -> String {
    env::var("SURREAL_PASS").unwrap_or_else(|_| "root".into())
}

static DB_COUNTER: AtomicU64 = AtomicU64::new(0);

fn unique_db() -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos());
    let seq = DB_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("it_graph_{nanos}_{seq}")
}

async fn connected_client(database: &str) -> Option<DatabaseClient> {
    let url = env_url()?;
    let namespace = format!("ns_{database}");
    let cfg = ConnectionConfig::builder()
        .url(url)
        .namespace(namespace)
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

#[tokio::test]
async fn batch_upsert_many_round_trip() {
    let Some(client) = connected_client(&unique_db()).await else {
        println!("skipped: SURREAL_URL not set");
        return;
    };

    let items = vec![
        json!({"id": "person:alice", "name": "Alice", "age": 30}),
        json!({"id": "person:bob", "name": "Bob", "age": 25}),
    ];
    let upserted = batch::upsert_many(&client, "person", items.clone(), None)
        .await
        .expect("upsert_many");
    assert_eq!(
        upserted.len(),
        2,
        "expected 2 upserted rows, got {upserted:?}"
    );

    // Re-upsert to confirm idempotency.
    let upserted_again = batch::upsert_many(&client, "person", items, None)
        .await
        .expect("upsert_many idempotent");
    assert_eq!(upserted_again.len(), 2);

    client.disconnect().await.unwrap();
}

#[tokio::test]
async fn graph_traverse_and_related() {
    let Some(client) = connected_client(&unique_db()).await else {
        println!("skipped: SURREAL_URL not set");
        return;
    };

    // Seed: alice -> bob -> charlie via `follows`.
    client
        .query(
            "CREATE person:alice SET name = 'alice';\n\
             CREATE person:bob SET name = 'bob';\n\
             CREATE person:charlie SET name = 'charlie';\n\
             RELATE person:alice->follows->person:bob;\n\
             RELATE person:bob->follows->person:charlie;",
        )
        .await
        .expect("seed graph");

    // Single-hop traversal.
    let followees = graph::traverse_raw(&client, "person:alice", "->follows->person")
        .await
        .expect("traverse_raw");
    assert!(
        followees.iter().any(|r| {
            r.get("id")
                .and_then(|v| v.as_str())
                .is_some_and(|id| id == "person:bob")
        }),
        "expected bob in alice's outgoing follows, got {followees:?}"
    );

    // Related records via convenience helper.
    let related = graph::get_related_records(
        &client,
        "person:alice",
        "follows",
        "person",
        graph::Direction::Out,
    )
    .await
    .expect("get_related_records");
    assert_eq!(related.len(), 1);

    // Count in + out.
    let count_out = graph::count_related(&client, "person:alice", "follows", graph::Direction::Out)
        .await
        .expect("count_related out");
    assert_eq!(count_out, 1);

    let count_in = graph::count_related(&client, "person:bob", "follows", graph::Direction::In)
        .await
        .expect("count_related in");
    assert_eq!(count_in, 1);

    // Shortest-path with a real connection.
    let path = graph::shortest_path(&client, "person:alice", "person:charlie", "follows", 4)
        .await
        .expect("shortest_path");
    assert!(!path.is_empty(), "expected non-empty path, got {path:?}");

    // Shortest-path with no connection.
    client
        .query("CREATE person:isolated SET name = 'isolated';")
        .await
        .expect("seed isolated");
    let empty = graph::shortest_path(&client, "person:alice", "person:isolated", "follows", 3)
        .await
        .expect("shortest_path no-match");
    assert!(
        empty.is_empty(),
        "expected empty path to isolated, got {empty:?}"
    );

    client.disconnect().await.unwrap();
}

#[tokio::test]
async fn graph_query_exists_and_count() {
    let Some(client) = connected_client(&unique_db()).await else {
        println!("skipped: SURREAL_URL not set");
        return;
    };

    client
        .query(
            "CREATE person:alice SET name = 'alice';\n\
             CREATE person:bob SET name = 'bob';\n\
             CREATE person:carol SET name = 'carol';\n\
             RELATE person:alice->follows->person:bob;\n\
             RELATE person:alice->follows->person:carol;",
        )
        .await
        .expect("seed graph");

    let has_followees = GraphQuery::new("person:alice")
        .out("follows", None)
        .exists(&client)
        .await
        .expect("graph_query exists");
    assert!(has_followees);

    let count = GraphQuery::new("person:alice")
        .out("follows", None)
        .count(&client)
        .await
        .expect("graph_query count");
    assert_eq!(count, 2);

    let rows = GraphQuery::new("person:alice")
        .out("follows", None)
        .execute(&client)
        .await
        .expect("graph_query execute");
    assert_eq!(rows.len(), 2);

    let empty = GraphQuery::new("person:carol")
        .out("follows", None)
        .exists(&client)
        .await
        .expect("graph_query exists empty");
    assert!(!empty);

    client.disconnect().await.unwrap();
}
