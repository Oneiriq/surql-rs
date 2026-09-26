//! Engine-backed coverage for the schema diff: every statement the diff
//! renders must apply, a rollback must put back what the forward removed,
//! and the definition read back afterwards must diff clean.
//!
//! Gated on the `SURREAL_URL` env var like `integration_migration`, so
//! `cargo test` stays green when no SurrealDB server is reachable:
//!
//! ```text
//! docker run -d -p 8000:8000 surrealdb/surrealdb:v3.0.5 start --user root --pass root memory
//! SURREAL_URL=ws://localhost:8000 SURREAL_USER=root SURREAL_PASS=root \
//!   cargo test --all-features --test integration_diff -- --test-threads=1
//! ```

#![cfg(any(feature = "client", feature = "client-rustls"))]

use std::env;
use std::sync::atomic::{AtomicU64, Ordering};

use serde_json::Value;
use surql::connection::{ConnectionConfig, DatabaseClient};
use surql::migration::diff::{diff_edges, diff_fields};
use surql::migration::{DiffOperation, SchemaDiff};
use surql::schema::edge::typed_edge;
use surql::schema::parser::parse_table_full;
use surql::schema::{
    record_field, string_field, FieldDefinition, ReferenceAction, TableDefinition,
};

static DB_COUNTER: AtomicU64 = AtomicU64::new(0);

async fn connected_client() -> Option<DatabaseClient> {
    let url = env::var("SURREAL_URL").ok()?;
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos());
    let seq = DB_COUNTER.fetch_add(1, Ordering::Relaxed);
    let database = format!("it_diff_{nanos}_{seq}");
    let cfg = ConnectionConfig::builder()
        .url(url)
        .namespace(format!("ns_{database}"))
        .database(database)
        .username(env::var("SURREAL_USER").unwrap_or_else(|_| "root".into()))
        .password(env::var("SURREAL_PASS").unwrap_or_else(|_| "root".into()))
        .timeout(10.0)
        .build()
        .expect("valid integration config");
    let client = DatabaseClient::new(cfg).expect("client constructs");
    client.connect().await.expect("connect to local surrealdb");
    Some(client)
}

/// Run `statements` as one script, panicking with the script on failure.
async fn apply(client: &DatabaseClient, statements: &[String]) {
    let script = statements.join("\n");
    client
        .query(&script)
        .await
        .unwrap_or_else(|e| panic!("apply failed: {e}\n{script}"));
}

/// The `up` half of a migration built from `diffs`, in order.
fn forward(diffs: &[SchemaDiff]) -> Vec<String> {
    diffs
        .iter()
        .map(|d| d.forward_sql.clone())
        .filter(|s| !s.trim().is_empty())
        .collect()
}

/// The `down` half of a migration built from `diffs`: reverse order.
fn backward(diffs: &[SchemaDiff]) -> Vec<String> {
    diffs
        .iter()
        .rev()
        .map(|d| d.backward_sql.clone())
        .filter(|s| !s.trim().is_empty())
        .collect()
}

/// `DatabaseClient::query` wraps every statement result in an array.
fn first(value: &Value) -> Value {
    value
        .as_array()
        .and_then(|a| a.first())
        .cloned()
        .unwrap_or(Value::Null)
}

/// The engine's own `DEFINE TABLE` echo for `table`, if it exists.
async fn table_echo(client: &DatabaseClient, table: &str) -> Option<String> {
    let info = first(&client.query("INFO FOR DB;").await.expect("INFO FOR DB"));
    info.get("tables")
        .and_then(|t| t.get(table))
        .and_then(Value::as_str)
        .map(str::to_owned)
}

async fn table_info(client: &DatabaseClient, table: &str) -> Value {
    first(
        &client
            .query(&format!("INFO FOR TABLE {table};"))
            .await
            .expect("INFO FOR TABLE"),
    )
}

/// Read one table back through both `INFO` levels.
async fn read_table(client: &DatabaseClient, table: &str) -> TableDefinition {
    let define = table_echo(client, table).await.expect("table exists");
    parse_table_full(table, &define, &table_info(client, table).await).expect("parse table")
}

/// Adding an edge that carries permissions is one statement the engine
/// accepts; it used to be a bare `DEFINE TABLE e PERMISSIONS ...` after the
/// `DEFINE TABLE e TYPE RELATION ...`, which the engine refuses because the
/// table already exists.
#[tokio::test]
async fn an_added_edge_with_permissions_applies_and_rolls_back() {
    let Some(client) = connected_client().await else {
        return;
    };
    apply(
        &client,
        &[
            "DEFINE TABLE person SCHEMAFULL;".into(),
            "DEFINE TABLE post SCHEMAFULL;".into(),
        ],
    )
    .await;

    let likes = typed_edge("likes", "person", "post")
        .with_permissions([("select", "$auth.id = in"), ("create", "true")]);
    let diffs = diff_edges(std::slice::from_ref(&likes), &[]);
    apply(&client, &forward(&diffs)).await;

    let echo = table_echo(&client, "likes").await.expect("edge defined");
    assert!(echo.contains("TYPE RELATION IN person OUT post"), "{echo}");
    assert!(echo.contains("FOR select WHERE $auth.id = in"), "{echo}");
    assert!(echo.contains("FOR create WHERE true"), "{echo}");

    apply(&client, &backward(&diffs)).await;
    assert!(table_echo(&client, "likes").await.is_none());
}

/// Walk one field change through the engine: define `old`, apply the diff's
/// forward, check the stored field now diffs clean against `new`, apply the
/// backward, and check it diffs clean against `old` again.
async fn field_change_round_trips(old: FieldDefinition, new: FieldDefinition) {
    let Some(client) = connected_client().await else {
        return;
    };
    apply(
        &client,
        &[
            "DEFINE TABLE user SCHEMAFULL;".into(),
            "DEFINE TABLE post SCHEMAFULL;".into(),
            "DEFINE TABLE doc SCHEMAFULL;".into(),
            old.to_surql("doc"),
        ],
    )
    .await;

    let diffs = diff_fields(
        "doc",
        std::slice::from_ref(&new),
        std::slice::from_ref(&old),
    );
    assert_eq!(diffs.len(), 1, "the change went unnoticed: {diffs:#?}");
    assert_eq!(diffs[0].operation, DiffOperation::ModifyField);

    apply(&client, &forward(&diffs)).await;
    let stored = read_table(&client, "doc").await;
    let residual = diff_fields("doc", std::slice::from_ref(&new), &stored.fields);
    assert!(residual.is_empty(), "forward left drift: {residual:#?}");

    apply(&client, &backward(&diffs)).await;
    let stored = read_table(&client, "doc").await;
    let residual = diff_fields("doc", std::slice::from_ref(&old), &stored.fields);
    assert!(residual.is_empty(), "rollback left drift: {residual:#?}");
}

#[tokio::test]
async fn a_field_losing_option_is_migrated_both_ways() {
    let old = string_field("title")
        .nullable(true)
        .build_unchecked()
        .unwrap();
    let new = string_field("title").build_unchecked().unwrap();
    field_change_round_trips(old, new).await;
}

#[tokio::test]
async fn a_record_link_changing_target_is_migrated_both_ways() {
    let old = record_field("owner", Some("user"))
        .build_unchecked()
        .unwrap();
    let new = record_field("owner", Some("post"))
        .build_unchecked()
        .unwrap();
    field_change_round_trips(old, new).await;
}

#[tokio::test]
async fn a_reference_action_change_is_migrated_both_ways() {
    let old = record_field("owner", Some("user"))
        .reference(ReferenceAction::Cascade)
        .build_unchecked()
        .unwrap();
    let new = record_field("owner", Some("user"))
        .reference(ReferenceAction::Reject)
        .build_unchecked()
        .unwrap();
    field_change_round_trips(old, new).await;
}
