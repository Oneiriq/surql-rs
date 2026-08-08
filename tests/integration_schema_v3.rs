//! End-to-end coverage for the SurrealDB 3.x DDL surface against a real
//! engine.
//!
//! Like `integration_fulltext` this suite drives the in-process `mem://`
//! engine rather than gating on a reachable `SURREAL_URL`, so it always runs
//! under `cargo test` and in CI.
//!
//! Every case follows the same shape, because that is what schema
//! reconciliation stands on:
//!
//! 1. render DDL from the builders (never hand-authored SurrealQL),
//! 2. apply it to the engine,
//! 3. read `INFO FOR DB` / `INFO FOR TABLE` back through the parser,
//! 4. assert the diff against the code-side definition is **empty**.
//!
//! Step 4 is the guard against the failure mode where a feature renders but
//! does not parse: every boot would re-apply the same statement forever.

#![cfg(any(feature = "client", feature = "client-rustls"))]

use std::sync::atomic::{AtomicU64, Ordering};

use serde_json::Value;

use surql::connection::{ConnectionConfig, DatabaseClient};
use surql::migration::diff::{diff_fields, diff_indexes};
use surql::query::references::reverse_reference_query;
use surql::schema::parser::{parse_db_info, parse_table_full};
use surql::schema::{
    array_field, generate_table_sql, info_for_index_surql, record_field, reverse_reference_field,
    string_field, table_schema, unique_index, FieldDefinition, IndexBuildStatus, ReferenceAction,
    TableDefinition, TableMode,
};

static DB_COUNTER: AtomicU64 = AtomicU64::new(0);

async fn memory_client() -> DatabaseClient {
    let seq = DB_COUNTER.fetch_add(1, Ordering::Relaxed);
    let name = format!("it_v3_{seq}");
    let cfg = ConnectionConfig::builder()
        .url("mem://")
        .namespace(name.clone())
        .database(name)
        .build()
        .expect("valid mem config");
    let client = DatabaseClient::new(cfg).expect("client constructs");
    client.connect().await.expect("connect to embedded engine");
    client
}

/// `DatabaseClient::query` wraps every statement result in an array; the
/// parsers want the single `INFO` object.
fn first(value: &Value) -> Value {
    value
        .as_array()
        .and_then(|a| a.first())
        .cloned()
        .unwrap_or(Value::Null)
}

async fn info_for_db(client: &DatabaseClient) -> Value {
    first(&client.query("INFO FOR DB;").await.expect("INFO FOR DB"))
}

async fn info_for_table(client: &DatabaseClient, table: &str) -> Value {
    first(
        &client
            .query(&format!("INFO FOR TABLE {table};"))
            .await
            .expect("INFO FOR TABLE"),
    )
}

/// Read one table back through both `INFO` levels, the composition schema
/// reconciliation uses.
async fn read_back(client: &DatabaseClient, table: &str) -> TableDefinition {
    let db_info = info_for_db(client).await;
    let parsed_db = parse_db_info(&db_info).expect("parse INFO FOR DB");
    let define = parsed_db
        .tables
        .get(table)
        .map(TableDefinition::to_surql)
        .unwrap_or_default();
    let table_info = info_for_table(client, table).await;
    parse_table_full(table, &define, &table_info).expect("parse INFO FOR TABLE")
}

async fn apply(client: &DatabaseClient, statements: &[String]) {
    let script = statements.join("\n");
    client
        .query(&script)
        .await
        .unwrap_or_else(|e| panic!("apply DDL failed: {e}\n{script}"));
}

fn field_named<'a>(table: &'a TableDefinition, name: &str) -> &'a FieldDefinition {
    table
        .fields
        .iter()
        .find(|f| f.name == name)
        .unwrap_or_else(|| panic!("{} has no field {name}", table.name))
}

/// A comic collection: `person.comics` is a to-many reference,
/// `person.favourite` a to-one reference that cascades, and
/// `comic_book.owners` the `COMPUTED <~person` reverse half of both.
fn reference_schema() -> (TableDefinition, TableDefinition) {
    let comic = table_schema("comic_book")
        .with_mode(TableMode::Schemafull)
        .with_fields([
            string_field("title").build_unchecked().unwrap(),
            reverse_reference_field("owners", "person")
                .build_unchecked()
                .unwrap(),
        ]);
    let person = table_schema("person")
        .with_mode(TableMode::Schemafull)
        .with_fields([
            string_field("name").build_unchecked().unwrap(),
            array_field("comics")
                .target_table("comic_book")
                .nullable(true)
                .reference(ReferenceAction::Unset)
                .build_unchecked()
                .unwrap(),
            record_field("favourite", Some("comic_book"))
                .nullable(true)
                .reference(ReferenceAction::Cascade)
                .build_unchecked()
                .unwrap(),
        ]);
    (comic, person)
}

async fn apply_reference_schema(client: &DatabaseClient) -> (TableDefinition, TableDefinition) {
    let (comic, person) = reference_schema();
    let mut ddl = generate_table_sql(&comic, true);
    ddl.extend(generate_table_sql(&person, true));
    apply(client, &ddl).await;
    (comic, person)
}

/// A `REFERENCE` field and its `COMPUTED <~` reverse half survive the whole
/// code -> DDL -> engine -> `INFO` -> parser -> diff cycle with no residual
/// change.
#[tokio::test]
async fn record_references_round_trip_through_the_parser() {
    let client = memory_client().await;
    let (comic, person) = apply_reference_schema(&client).await;

    for code in [&comic, &person] {
        let stored = read_back(&client, &code.name).await;
        let diffs = diff_fields(&code.name, &code.fields, &stored.fields);
        assert!(
            diffs.is_empty(),
            "{} re-applies on every boot: {:#?}",
            code.name,
            diffs
        );
    }

    // The stored shape is the one the engine reports, including the ON DELETE
    // action a bare REFERENCE is normalised to.
    let stored_person = read_back(&client, "person").await;
    let comics = field_named(&stored_person, "comics");
    assert_eq!(comics.reference, Some(ReferenceAction::Unset));
    assert_eq!(comics.target_table.as_deref(), Some("comic_book"));
    let stored_comic = read_back(&client, "comic_book").await;
    assert_eq!(
        field_named(&stored_comic, "owners").computed.as_deref(),
        Some("<~person")
    );
}

/// The `<~` projection reads incoming references back, and `ON DELETE
/// CASCADE` removes the record that held one.
#[tokio::test]
async fn reverse_traversal_reads_back_and_cascade_deletes() {
    let client = memory_client().await;
    apply_reference_schema(&client).await;

    client
        .query(
            "CREATE comic_book:one SET title = 'Loki, God of Stories';\
             CREATE person:mat SET name = 'Mat', comics = [comic_book:one];\
             CREATE person:nynaeve SET name = 'Nynaeve', comics = [comic_book:one];",
        )
        .await
        .expect("seed records");

    let query = reverse_reference_query("comic_book", "person", Some(&["name"]), "owners")
        .expect("build reverse-reference query");
    let rows = client
        .query(&query.to_surql().expect("render"))
        .await
        .expect("run reverse traversal");
    let names = serde_json::to_string(&rows).expect("serialise rows");
    assert!(
        names.contains("Mat"),
        "reverse traversal found Mat: {names}"
    );
    assert!(
        names.contains("Nynaeve"),
        "reverse traversal found Nynaeve: {names}"
    );

    // ON DELETE CASCADE propagates: deleting the comic deletes whoever named
    // it as a favourite.
    client
        .query("UPDATE person:mat SET favourite = comic_book:one; DELETE comic_book:one;")
        .await
        .expect("set favourite then delete the comic");
    let remaining = client
        .query("SELECT name FROM person;")
        .await
        .expect("list people");
    let remaining = serde_json::to_string(&remaining).expect("serialise");
    assert!(
        !remaining.contains("Mat"),
        "CASCADE removed the referencing record: {remaining}"
    );
    assert!(
        remaining.contains("Nynaeve"),
        "the non-referencing record survives: {remaining}"
    );
}

/// The engine refuses `REFERENCE` on a nested field; the builder refuses it
/// before a statement is ever sent.
#[tokio::test]
async fn nested_reference_is_rejected_before_it_reaches_the_engine() {
    let err = record_field("metadata.comics", Some("comic_book"))
        .reference(ReferenceAction::Ignore)
        .build()
        .expect_err("nested reference rejected");
    assert!(err.to_string().contains("top-level"), "{err}");

    // And the engine agrees, which is why the check exists.
    let client = memory_client().await;
    let applied = client
        .query(
            "DEFINE TABLE person SCHEMAFULL; \
             DEFINE FIELD metadata ON person TYPE option<object>; \
             DEFINE FIELD metadata.comics ON person TYPE option<array<record<comic_book>>> \
             REFERENCE;",
        )
        .await;
    assert!(applied.is_err(), "engine rejects a nested REFERENCE");
}

/// A `CONCURRENTLY` index is accepted by the engine, still enforces its
/// constraint, and — because the engine drops the directive from the stored
/// definition — produces no residual diff on the next reconcile.
#[tokio::test]
async fn concurrent_index_builds_and_leaves_no_residual_diff() {
    let client = memory_client().await;

    let user = table_schema("user")
        .with_mode(TableMode::Schemafull)
        .with_fields([string_field("email").build_unchecked().unwrap()])
        .with_indexes([unique_index("email_idx", ["email"]).with_concurrently(true)]);

    let ddl = generate_table_sql(&user, true);
    assert!(
        ddl.iter().any(|s| s.contains("UNIQUE CONCURRENTLY;")),
        "the directive reaches the statement: {ddl:?}"
    );
    apply(&client, &ddl).await;

    let stored = read_back(&client, "user").await;
    let diffs = diff_indexes("user", &user.indexes, &stored.indexes);
    assert!(
        diffs.is_empty(),
        "a CONCURRENTLY index re-applies on every boot: {diffs:#?}"
    );
    assert!(
        !stored.indexes[0].concurrently,
        "the engine does not store the build directive"
    );

    // Progress is readable, and by the time the statement returns on an
    // in-memory engine with no rows the build is already done.
    let info = client
        .query(&info_for_index_surql("email_idx", "user"))
        .await
        .expect("INFO FOR INDEX");
    let status = IndexBuildStatus::from_info(&info).expect("build status");
    assert!(status.is_ready(), "index build reported ready: {status:?}");

    // The index is real: the uniqueness constraint bites.
    client
        .query("CREATE user:a SET email = 'a@example.com';")
        .await
        .expect("first row");
    let clash = client
        .query("CREATE user:b SET email = 'a@example.com';")
        .await;
    assert!(clash.is_err(), "the concurrent index enforces UNIQUE");
}
