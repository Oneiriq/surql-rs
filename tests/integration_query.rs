//! Integration tests for the query execution layer
//! ([`executor`](surql::query::executor), [`crud`](surql::query::crud),
//! [`typed`](surql::query::typed)).
//!
//! Gated on the `SURREAL_URL` env var so `cargo test` stays green when no
//! SurrealDB server is reachable. Exercise with:
//!
//! ```text
//! docker run -d -p 8000:8000 surrealdb/surrealdb:v2.2 start --user root --pass root memory
//! SURREAL_URL=ws://localhost:8000 SURREAL_USER=root SURREAL_PASS=root \
//!   cargo test --all-features --test integration_query
//! ```

#![cfg(feature = "client")]

use std::env;
use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Serialize};
use serde_json::json;
use surql::connection::{ConnectionConfig, DatabaseClient};
use surql::query::builder::Query;
use surql::query::{crud, executor, typed};
use surql::types::operators::{eq, gt};
use surql::types::record_id::RecordID;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct User {
    name: String,
    age: u32,
}

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
    format!("it_query_{nanos}_{seq}")
}

async fn connected_client(database: &str) -> Option<DatabaseClient> {
    let url = env_url()?;
    // Use a unique namespace per test as well so the in-memory SurrealDB
    // engine (used in CI) does not hit write-conflict retries across
    // parallel tests.
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
async fn executor_execute_query_returns_rows() {
    let Some(client) = connected_client(&unique_db()).await else {
        println!("skipped: SURREAL_URL not set");
        return;
    };
    client
        .query("CREATE user:alice SET name = 'alice', age = 30;")
        .await
        .expect("seed alice");
    client
        .query("CREATE user:bob SET name = 'bob', age = 40;")
        .await
        .expect("seed bob");

    let q = Query::new().select(None).from_table("user").unwrap();
    let rows: Vec<User> = executor::fetch_all(&client, &q).await.expect("fetch_all");
    assert_eq!(rows.len(), 2);
    assert!(rows.iter().any(|u| u.name == "alice"));

    let one: Option<User> = executor::fetch_one(&client, &q).await.expect("fetch_one");
    assert!(one.is_some());

    let many = executor::fetch_many::<User>(
        &client,
        &Query::new()
            .select(None)
            .from_table("user")
            .unwrap()
            .limit(1)
            .unwrap(),
    )
    .await
    .expect("fetch_many");
    assert_eq!(many.len(), 1);
    assert_eq!(many.limit, Some(1));

    client.disconnect().await.unwrap();
}

#[tokio::test]
async fn executor_execute_raw_and_typed() {
    let Some(client) = connected_client(&unique_db()).await else {
        println!("skipped: SURREAL_URL not set");
        return;
    };
    client
        .query("CREATE user:alice SET name = 'alice', age = 30;")
        .await
        .expect("seed alice");

    let raw = executor::execute_raw(&client, "SELECT * FROM user", None)
        .await
        .expect("execute_raw");
    assert!(raw.is_array());

    let rows: Vec<User> = executor::execute_raw_typed(&client, "SELECT * FROM user")
        .await
        .expect("execute_raw_typed");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].name, "alice");

    client.disconnect().await.unwrap();
}

#[tokio::test]
async fn crud_create_get_update_merge_delete_round_trip() {
    let Some(client) = connected_client(&unique_db()).await else {
        println!("skipped: SURREAL_URL not set");
        return;
    };

    let id = RecordID::<()>::new("user", "alice").unwrap();

    let created = crud::create_record(&client, "user:alice", json!({"name": "alice", "age": 30}))
        .await
        .expect("create_record");
    assert!(created.exists);

    let fetched = crud::get_record(&client, &id)
        .await
        .expect("get_record")
        .expect("exists");
    assert_eq!(fetched["name"], "alice");

    let merged = crud::merge_record(&client, &id, json!({"age": 31}))
        .await
        .expect("merge_record");
    assert_eq!(merged["age"], 31);

    let updated = crud::update_record(&client, &id, json!({"name": "alice", "age": 32}))
        .await
        .expect("update_record");
    assert_eq!(updated["age"], 32);

    let upserted = crud::upsert_record(
        &client,
        &id,
        json!({"name": "alice", "age": 33, "vip": true}),
    )
    .await
    .expect("upsert_record");
    assert_eq!(upserted["vip"], true);

    assert!(crud::exists(&client, &id).await.expect("exists"));

    crud::delete_record(&client, &id)
        .await
        .expect("delete_record");
    assert!(!crud::exists(&client, &id).await.expect("exists after"));

    client.disconnect().await.unwrap();
}

#[tokio::test]
async fn crud_bulk_and_count_and_query() {
    let Some(client) = connected_client(&unique_db()).await else {
        println!("skipped: SURREAL_URL not set");
        return;
    };

    let created = crud::create_records(
        &client,
        "user",
        vec![
            json!({"name": "alice", "age": 30}),
            json!({"name": "bob", "age": 40}),
            json!({"name": "carol", "age": 50}),
        ],
    )
    .await
    .expect("create_records");
    assert_eq!(created.len(), 3);

    let total = crud::count_records(&client, "user", None)
        .await
        .expect("count_records");
    assert_eq!(total, 3);

    let over_30 = crud::count_records(&client, "user", Some(&gt("age", 30)))
        .await
        .expect("count with where");
    assert_eq!(over_30, 2);

    let q = Query::new()
        .select(None)
        .from_table("user")
        .unwrap()
        .where_(gt("age", 30))
        .order_by("age", "ASC")
        .unwrap();

    let rows: Vec<User> = crud::query_records(&client, &q)
        .await
        .expect("query_records");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].name, "bob");

    let firstly: Option<User> = crud::first(&client, &q).await.expect("first");
    assert_eq!(firstly.map(|u| u.name), Some("bob".into()));

    let lastly: Option<User> = crud::last(&client, &q).await.expect("last");
    assert_eq!(lastly.map(|u| u.name), Some("carol".into()));

    let deleted = crud::delete_records(&client, "user", Some(&eq("name", "bob")))
        .await
        .expect("delete_records");
    assert_eq!(deleted, 1);

    client.disconnect().await.unwrap();
}

#[tokio::test]
async fn typed_round_trip() {
    let Some(client) = connected_client(&unique_db()).await else {
        println!("skipped: SURREAL_URL not set");
        return;
    };

    let alice = User {
        name: "alice".into(),
        age: 30,
    };

    let created = typed::create_typed(&client, "user:alice", &alice)
        .await
        .expect("create_typed");
    assert_eq!(created.name, "alice");

    let id = RecordID::<()>::new("user", "alice").unwrap();
    let fetched: Option<User> = typed::get_typed(&client, &id).await.expect("get_typed");
    assert_eq!(fetched.as_ref().map(|u| u.age), Some(30));

    let updated = User {
        name: "alice".into(),
        age: 31,
    };
    let updated_back = typed::update_typed(&client, &id, &updated)
        .await
        .expect("update_typed");
    assert_eq!(updated_back.age, 31);

    let upserted = User {
        name: "alice".into(),
        age: 99,
    };
    let upserted_back = typed::upsert_typed(&client, &id, &upserted)
        .await
        .expect("upsert_typed");
    assert_eq!(upserted_back.age, 99);

    let q = Query::new().select(None).from_table("user").unwrap();
    let all: Vec<User> = typed::query_typed(&client, &q).await.expect("query_typed");
    assert_eq!(all.len(), 1);

    client.disconnect().await.unwrap();
}
