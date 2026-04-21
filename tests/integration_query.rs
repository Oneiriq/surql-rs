//! Integration tests for the query execution layer
//! ([`executor`](surql::query::executor), [`crud`](surql::query::crud),
//! [`typed`](surql::query::typed)).
//!
//! Gated on the `SURREAL_URL` env var so `cargo test` stays green when no
//! SurrealDB server is reachable. Exercise with:
//!
//! ```text
//! docker run -d -p 8000:8000 surrealdb/surrealdb:v3.0.5 start --user root --pass root memory
//! SURREAL_URL=ws://localhost:8000 SURREAL_USER=root SURREAL_PASS=root \
//!   cargo test --all-features --test integration_query
//! ```

#![cfg(any(feature = "client", feature = "client-rustls"))]

use std::env;
use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Serialize};
use serde_json::json;
use surql::connection::{ConnectionConfig, DatabaseClient};
use surql::query::builder::Query;
use surql::query::expressions::{as_, count_all, math_mean, math_sum};
use surql::query::results::{extract_many, extract_one, extract_scalar, has_result};
use surql::query::{crud, executor, typed, AggregateOpts};
use surql::types::operators::{eq, gt, type_record, type_thing};
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
async fn aggregate_records_group_all_returns_aggregates() {
    let Some(client) = connected_client(&unique_db()).await else {
        println!("skipped: SURREAL_URL not set");
        return;
    };

    // Seed three memory_entry rows across two networks.
    client
        .query("CREATE memory_entry:a SET network = 'default', strength = 1.0;")
        .await
        .expect("seed a");
    client
        .query("CREATE memory_entry:b SET network = 'default', strength = 3.0;")
        .await
        .expect("seed b");
    client
        .query("CREATE memory_entry:c SET network = 'other', strength = 5.0;")
        .await
        .expect("seed c");

    let opts = AggregateOpts {
        select: vec![
            ("total".to_string(), count_all()),
            ("strength_sum".to_string(), math_sum("strength")),
            ("strength_mean".to_string(), math_mean("strength")),
        ],
        group_all: true,
        ..Default::default()
    };

    let rows = crud::aggregate_records(&client, "memory_entry", opts)
        .await
        .expect("aggregate_records group_all");
    assert_eq!(rows.len(), 1);
    let row = &rows[0];
    assert_eq!(row["total"].as_i64(), Some(3));
    assert_eq!(row["strength_sum"].as_f64(), Some(9.0));
    assert_eq!(row["strength_mean"].as_f64(), Some(3.0));

    client.disconnect().await.unwrap();
}

#[tokio::test]
async fn aggregate_records_group_by_splits_rows() {
    let Some(client) = connected_client(&unique_db()).await else {
        println!("skipped: SURREAL_URL not set");
        return;
    };

    client
        .query("CREATE memory_entry:a SET network = 'default', strength = 1.0;")
        .await
        .expect("seed a");
    client
        .query("CREATE memory_entry:b SET network = 'default', strength = 3.0;")
        .await
        .expect("seed b");
    client
        .query("CREATE memory_entry:c SET network = 'other', strength = 5.0;")
        .await
        .expect("seed c");

    let opts = AggregateOpts {
        select: vec![
            ("network".to_string(), surql::query::raw("network")),
            ("count".to_string(), count_all()),
            ("sum".to_string(), math_sum("strength")),
        ],
        group_by: vec!["network".into()],
        order_by: vec![("network".into(), "ASC".into())],
        ..Default::default()
    };

    let rows = crud::aggregate_records(&client, "memory_entry", opts)
        .await
        .expect("aggregate_records group_by");
    assert_eq!(rows.len(), 2);
    // Ordered ASC by network -> "default", "other".
    assert_eq!(rows[0]["network"].as_str(), Some("default"));
    assert_eq!(rows[0]["count"].as_i64(), Some(2));
    assert_eq!(rows[1]["network"].as_str(), Some("other"));
    assert_eq!(rows[1]["sum"].as_f64(), Some(5.0));

    client.disconnect().await.unwrap();
}

#[tokio::test]
async fn type_record_target_round_trip_with_update() {
    let Some(client) = connected_client(&unique_db()).await else {
        println!("skipped: SURREAL_URL not set");
        return;
    };

    // Create using the SurrealQL `type::record` literal so we can verify
    // that targets rendered by our `type_record()` helper address the same
    // record at update time.
    client
        .query("CREATE type::record('task', 'abc') SET name = 'draft', status = 'pending';")
        .await
        .expect("seed task");

    // Render a target via type_record() and UPDATE through it.
    let target = type_record("task", "abc");
    let target_sql = target.to_surql();
    assert_eq!(target_sql, "type::record('task', 'abc')");

    let updated = crud::update_record_target(
        &client,
        &target_sql,
        json!({"name": "draft", "status": "done"}),
    )
    .await
    .expect("update via type::record target");
    assert_eq!(updated["status"], "done");

    // And UPSERT through a `type_record()` target rendering.
    let upsert_target = type_record("task", "abc").to_surql();
    let upserted = crud::upsert_record_target(
        &client,
        &upsert_target,
        json!({"name": "final", "status": "archived"}),
    )
    .await
    .expect("upsert via type::record target");
    assert_eq!(upserted["status"], "archived");

    // type_thing renders the SurrealQL `type::thing(...)` form deterministically;
    // v3.0.5 only honours it in some positions so we assert the shape
    // rather than executing it here.
    assert_eq!(
        type_thing("task", "abc").to_surql(),
        "type::thing('task', 'abc')",
    );

    client.disconnect().await.unwrap();
}

#[tokio::test]
async fn extract_helpers_round_trip_against_raw_response() {
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

    let raw = executor::execute_raw(&client, "SELECT * FROM user", None)
        .await
        .expect("execute_raw");

    assert!(has_result(&raw));
    assert_eq!(extract_many(&raw).len(), 2);
    let first = extract_one(&raw).expect("first row");
    assert!(first.get("name").is_some());

    let count_raw =
        executor::execute_raw(&client, "SELECT count() AS count FROM user GROUP ALL", None)
            .await
            .expect("count raw");
    let total = extract_scalar(&count_raw, "count", json!(0));
    assert_eq!(total.as_i64(), Some(2));

    client.disconnect().await.unwrap();
}

#[tokio::test]
async fn query_builder_execute_convenience() {
    let Some(client) = connected_client(&unique_db()).await else {
        println!("skipped: SURREAL_URL not set");
        return;
    };
    client
        .query("CREATE user:alice SET name = 'alice', age = 30;")
        .await
        .expect("seed alice");

    let q = Query::new().select(None).from_table("user").unwrap();
    let raw = q.execute(&client).await.expect("builder execute");
    assert!(has_result(&raw));

    // And the builder's select_expr / group_all path.
    let agg = Query::new()
        .select_expr(vec![as_(&count_all(), "count")])
        .from_table("user")
        .unwrap()
        .group_all()
        .execute(&client)
        .await
        .expect("agg execute");
    let row = extract_one(&agg).expect("one aggregate row");
    assert_eq!(
        row.get("count").and_then(serde_json::Value::as_i64),
        Some(1)
    );

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
