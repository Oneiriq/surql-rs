//! End-to-end coverage for schema validation (`surql schema validate`).
//!
//! The round-trip cases drive the in-process `mem://` engine, so they always
//! run: render DDL from the builders, apply it, read the live schema back the
//! way the CLI does ([`fetch_live_schema`]), and assert the validator finds
//! nothing. That is the guard against a validator that flags the engine's
//! own echo (auto-defined `<array>.*` fields, default HNSW tuning, reformatted
//! event bodies) as drift, and against one that misses real drift.
//!
//! The CLI case needs a server (the CLI opens its own connection, and every
//! `mem://` connection is a fresh engine), so it is gated on `SURREAL_URL`:
//!
//! ```text
//! docker run -d -p 8000:8000 surrealdb/surrealdb:v3.0.5 start --user root --pass root memory
//! SURREAL_URL=ws://localhost:8000 SURREAL_USER=root SURREAL_PASS=root \
//!   cargo test --all-features --test integration_validate -- --test-threads=1
//! ```

#![cfg(feature = "cli")]

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};

use clap::Parser;
use surql::cli::schema::fetch_live_schema;
use surql::cli::{execute, Cli};
use surql::connection::{ConnectionConfig, DatabaseClient};
use surql::schema::{
    clear_registry, event, generate_schema_sql, has_errors, hnsw_index, register_edge,
    register_table, table_schema, typed_edge, unique_index, validate_schema, EdgeDefinition,
    FieldDefinition, FieldType, HnswDistanceType, IndexDefinition, MTreeVectorType,
    ReferenceAction, TableDefinition, ValidationResult, ValidationSeverity,
};

static DB_COUNTER: AtomicU64 = AtomicU64::new(0);

fn unique_name(prefix: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos());
    let seq = DB_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{prefix}_{nanos}_{seq}")
}

async fn memory_client() -> DatabaseClient {
    let name = unique_name("it_validate");
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

fn user_table() -> TableDefinition {
    table_schema("user")
        .with_fields([
            FieldDefinition::new("name", FieldType::String)
                .with_assertion("string::len($value) > 0"),
            FieldDefinition::new("nick", FieldType::String).with_nullable(true),
            FieldDefinition::new("best_friend", FieldType::Record)
                .with_target_table("user")
                .with_nullable(true)
                .with_reference(ReferenceAction::Unset),
            FieldDefinition::new("friends", FieldType::Array).with_target_table("user"),
            FieldDefinition::new("emb", FieldType::Array),
        ])
        .with_indexes([
            unique_index("user_name", ["name"]),
            IndexDefinition::new("user_name_nick", ["name", "nick"]),
            hnsw_index(
                "user_emb",
                "emb",
                4,
                HnswDistanceType::Cosine,
                MTreeVectorType::F32,
                None,
                None,
            ),
        ])
        .with_events([event(
            "audit",
            "$event = \"CREATE\"",
            "CREATE audit_log SET at = time::now()",
        )])
}

fn post_table() -> TableDefinition {
    table_schema("post").with_fields([FieldDefinition::new("title", FieldType::String)])
}

fn likes_edge() -> EdgeDefinition {
    typed_edge("likes", "user", "post")
        .with_fields([FieldDefinition::new("weight", FieldType::Int)])
        .with_events([event(
            "bump",
            "true",
            "{ UPDATE $after.out SET liked = true }",
        )])
}

fn code_schema() -> (
    HashMap<String, TableDefinition>,
    HashMap<String, EdgeDefinition>,
) {
    let tables = [user_table(), post_table()]
        .into_iter()
        .map(|t| (t.name.clone(), t))
        .collect();
    let edges = [likes_edge()]
        .into_iter()
        .map(|e| (e.name.clone(), e))
        .collect();
    (tables, edges)
}

fn schema_sql(
    tables: &HashMap<String, TableDefinition>,
    edges: &HashMap<String, EdgeDefinition>,
) -> String {
    let tables: BTreeMap<_, _> = tables.clone().into_iter().collect();
    let edges: BTreeMap<_, _> = edges.clone().into_iter().collect();
    generate_schema_sql(Some(&tables), Some(&edges), false).expect("schema renders")
}

async fn validate_live(
    client: &DatabaseClient,
    tables: &HashMap<String, TableDefinition>,
    edges: &HashMap<String, EdgeDefinition>,
) -> Vec<ValidationResult> {
    let live = fetch_live_schema(client, edges)
        .await
        .expect("live schema reads");
    validate_schema(tables, &live.tables, Some(edges), Some(&live.edges))
}

/// Endpoint results the edge parser cannot avoid yet: v3 echoes a relation's
/// endpoints as `IN a OUT b`, which the parser (reading `FROM` / `TO`) does
/// not pick up, so the database side of every relation reads unconstrained.
fn is_unparsed_endpoint(r: &ValidationResult) -> bool {
    r.severity == ValidationSeverity::Warning
        && (r.message == "Edge FROM table mismatch" || r.message == "Edge TO table mismatch")
        && r.db_value.is_none()
}

#[tokio::test]
async fn applied_schema_validates_clean() {
    let client = memory_client().await;
    let (tables, edges) = code_schema();
    client
        .query(&schema_sql(&tables, &edges))
        .await
        .expect("schema applies");

    let results = validate_live(&client, &tables, &edges).await;
    let unexpected: Vec<_> = results
        .iter()
        .filter(|r| !is_unparsed_endpoint(r))
        .collect();
    assert!(unexpected.is_empty(), "{unexpected:#?}");
    assert!(!has_errors(&results), "{results:#?}");
}

#[tokio::test]
async fn live_drift_is_reported() {
    let client = memory_client().await;
    let (tables, edges) = code_schema();
    client
        .query(&schema_sql(&tables, &edges))
        .await
        .expect("schema applies");
    client
        .query(
            "DEFINE FIELD OVERWRITE best_friend ON TABLE user TYPE option<record<post>> \
             REFERENCE ON DELETE REJECT;\
             DEFINE FIELD OVERWRITE nick ON TABLE user TYPE string;\
             DEFINE TABLE OVERWRITE post SCHEMAFULL PERMISSIONS FOR select WHERE false;\
             DEFINE EVENT OVERWRITE audit ON TABLE user WHEN $event = 'DELETE' \
             THEN (CREATE audit_log SET at = time::now());\
             DEFINE TABLE stray SCHEMALESS;",
        )
        .await
        .expect("drift applies");

    let results = validate_live(&client, &tables, &edges).await;
    let has = |field: Option<&str>, message: &str, severity: ValidationSeverity| {
        results.iter().any(|r| {
            r.field.as_deref() == field && r.message.starts_with(message) && r.severity == severity
        })
    };
    assert!(
        has(
            Some("best_friend"),
            "Field record target mismatch",
            ValidationSeverity::Error
        ),
        "{results:#?}"
    );
    assert!(
        has(
            Some("best_friend"),
            "Field REFERENCE mismatch",
            ValidationSeverity::Error
        ),
        "{results:#?}"
    );
    assert!(
        has(Some("nick"), "Field nullability", ValidationSeverity::Error),
        "{results:#?}"
    );
    assert!(
        has(
            Some("event:audit"),
            "Event condition (WHEN) mismatch",
            ValidationSeverity::Warning
        ),
        "{results:#?}"
    );
    assert!(
        has(
            None,
            "Table exists in database but not defined in code",
            ValidationSeverity::Warning
        ),
        "{results:#?}"
    );
    assert!(has_errors(&results));
}

// --- CLI, against a server -------------------------------------------------

fn env_url() -> Option<String> {
    std::env::var("SURREAL_URL").ok()
}

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

fn server_config(namespace: &str, database: &str) -> ConnectionConfig {
    ConnectionConfig::builder()
        .url(env_url().unwrap_or_default())
        .namespace(namespace)
        .database(database)
        .username(env_or("SURREAL_USER", "root"))
        .password(env_or("SURREAL_PASS", "root"))
        .timeout(10.0)
        .build()
        .expect("valid server config")
}

fn run_on_server(namespace: &str, database: &str, surql: &str) {
    let runtime = tokio::runtime::Runtime::new().expect("runtime");
    runtime.block_on(async {
        let client = DatabaseClient::new(server_config(namespace, database)).expect("client");
        client.connect().await.expect("connect to server");
        client.query(surql).await.expect("statements apply");
    });
}

/// Run `surql schema validate` in-process, so it sees this process's
/// registry, against the server named in a throwaway settings file.
fn cli_validate(dir: &std::path::Path, namespace: &str, database: &str) -> surql::Result<()> {
    let cargo = dir.join("Cargo.toml");
    std::fs::write(
        &cargo,
        format!(
            "[package]\nname = \"validate-e2e\"\nversion = \"0.0.0\"\n\n\
             [package.metadata.surql.database]\n\
             url = \"{url}\"\nnamespace = \"{namespace}\"\ndatabase = \"{database}\"\n\
             username = \"{user}\"\npassword = \"{pass}\"\n",
            url = env_url().unwrap_or_default(),
            user = env_or("SURREAL_USER", "root"),
            pass = env_or("SURREAL_PASS", "root"),
        ),
    )
    .expect("settings file written");
    let cli = Cli::try_parse_from([
        "surql",
        "--config",
        cargo.to_str().expect("utf-8 temp path"),
        "schema",
        "validate",
    ])
    .expect("cli parses");
    execute(cli)
}

#[test]
fn cli_schema_validate_against_server() {
    if env_url().is_none() {
        eprintln!("SURREAL_URL not set; skipping the schema validate CLI test");
        return;
    }
    let namespace = unique_name("ns_validate");
    let database = unique_name("db_validate");
    let dir = tempfile::tempdir().expect("tempdir");

    clear_registry();
    let (tables, edges) = code_schema();
    for table in tables.values() {
        register_table(table.clone());
    }
    for edge in edges.values() {
        register_edge(edge.clone());
    }
    run_on_server(&namespace, &database, &schema_sql(&tables, &edges));

    let clean = cli_validate(dir.path(), &namespace, &database);
    assert!(clean.is_ok(), "matching schema must validate: {clean:?}");

    run_on_server(
        &namespace,
        &database,
        "DEFINE FIELD OVERWRITE weight ON TABLE likes TYPE string;",
    );
    let drifted = cli_validate(dir.path(), &namespace, &database);
    clear_registry();
    assert!(
        drifted.is_err(),
        "a changed edge field must fail validation"
    );
}
