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
use surql::migration::diff::{diff_fields, diff_indexes, diff_tables};
use surql::migration::diff_objects::{diff_functions, diff_params, diff_sequences};
use surql::query::builder::Query;
use surql::query::changes::{show_changes_surql, ChangeSet, ChangeSince};
use surql::query::references::reverse_reference_query;
use surql::schema::parser::{parse_db_info, parse_table_full};
use surql::schema::{
    array_field, diskann_index, function_schema, generate_function_sql, generate_param_sql,
    generate_sequence_sql, generate_table_sql, hnsw_index, info_for_index_surql, param_schema,
    record_field, reverse_reference_field, sequence_schema, string_field, table_schema,
    unique_index, ChangeFeed, DiskAnnDistanceType, FieldDefinition, FunctionDefinition,
    HnswDistanceType, IndexBuildStatus, MTreeVectorType, ParamDefinition, ReferenceAction,
    SequenceDefinition, TableDefinition, TableMode, ViewDefinition, ViewGroup,
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

/// A `CHANGEFEED` survives the round trip and actually records mutations,
/// which `SHOW CHANGES FOR TABLE` replays.
#[tokio::test]
async fn changefeed_round_trips_and_replays_mutations() {
    let client = memory_client().await;

    let audit = table_schema("audit")
        .with_mode(TableMode::Schemaless)
        .with_changefeed(ChangeFeed::new("1d").include_original(true));
    apply(&client, &generate_table_sql(&audit, true)).await;

    let stored = read_back(&client, "audit").await;
    assert_eq!(
        stored.changefeed, audit.changefeed,
        "the engine echoes the feed the code declared"
    );
    let diffs = diff_tables(std::slice::from_ref(&audit), std::slice::from_ref(&stored));
    assert!(
        diffs.is_empty(),
        "a CHANGEFEED table re-applies on every boot: {diffs:#?}"
    );

    client
        .query("CREATE audit:a SET action = 'created'; UPDATE audit:a SET action = 'edited';")
        .await
        .expect("mutate the table");

    let statement = show_changes_surql("audit", &ChangeSince::Versionstamp(1), Some(50))
        .expect("render SHOW CHANGES");
    let response = client
        .query(&statement)
        .await
        .expect("read the change feed");
    let sets = ChangeSet::from_response(&response);
    assert!(
        !sets.is_empty(),
        "the feed replayed at least one changeset: {response}"
    );
    assert!(
        sets.windows(2)
            .all(|w| w[0].versionstamp <= w[1].versionstamp),
        "versionstamps come back in order: {sets:?}"
    );
    let body = serde_json::to_string(&sets).expect("serialise");
    assert!(body.contains("edited"), "the update is in the feed: {body}");

    // Resuming past the last versionstamp yields nothing new.
    let last = sets.last().expect("a changeset").versionstamp;
    let resumed = client
        .query(&show_changes_surql("audit", &ChangeSince::Versionstamp(last + 1), None).unwrap())
        .await
        .expect("resume the change feed");
    assert!(
        ChangeSet::from_response(&resumed).is_empty(),
        "resuming after the last versionstamp is empty: {resumed}"
    );
}

/// A pre-computed view table round-trips through the parser and is actually
/// maintained by the engine as its source table changes.
#[tokio::test]
async fn view_table_round_trips_and_is_maintained() {
    let client = memory_client().await;

    let comment = table_schema("comment")
        .with_mode(TableMode::Schemaless)
        .with_fields([string_field("author").build_unchecked().unwrap()]);
    let stats = table_schema("comment_stats")
        .with_mode(TableMode::Schemaless)
        .with_view(
            ViewDefinition::new(["count() AS total", "author"], ["comment"])
                .with_group(ViewGroup::by(["author"])),
        );
    stats
        .validate()
        .expect("a view with no declared fields is valid");

    let mut ddl = generate_table_sql(&comment, true);
    ddl.extend(generate_table_sql(&stats, true));
    assert!(
        ddl.iter()
            .any(|s| s.contains("TYPE NORMAL") && s.contains("AS SELECT")),
        "the view clause reaches the statement: {ddl:?}"
    );
    apply(&client, &ddl).await;

    let stored = read_back(&client, "comment_stats").await;
    let diffs = diff_tables(std::slice::from_ref(&stats), std::slice::from_ref(&stored));
    assert!(
        diffs.is_empty(),
        "a view re-applies on every boot: {diffs:#?}\nstored: {stored:#?}"
    );

    // The engine maintains it: three comments across two authors produce two
    // rows with the right counts, with no explicit refresh.
    client
        .query(
            "CREATE comment SET author = 'mat';\
             CREATE comment SET author = 'mat';\
             CREATE comment SET author = 'nynaeve';",
        )
        .await
        .expect("seed comments");
    let rows = client
        .query("SELECT * FROM comment_stats ORDER BY author;")
        .await
        .expect("read the view");
    let body = serde_json::to_string(&rows).expect("serialise");
    assert!(
        body.contains("\"total\":2") && body.contains("\"total\":1"),
        "the view aggregates its source: {body}"
    );
}

/// A view holds no field definitions of its own, so declaring them is
/// rejected before the reconciler could start dropping them every boot.
#[tokio::test]
async fn a_view_with_declared_fields_is_rejected() {
    let stats = table_schema("comment_stats")
        .with_view(ViewDefinition::new(["count() AS total"], ["comment"]))
        .with_fields([string_field("total").build_unchecked().unwrap()]);
    let err = stats.validate().expect_err("fields on a view are rejected");
    assert!(err.to_string().contains("view"), "{err}");
}

/// A sequence round-trips through `INFO FOR DB` and actually hands out
/// increasing values.
#[tokio::test]
async fn sequence_round_trips_and_hands_out_values() {
    let client = memory_client().await;

    let invoice = sequence_schema("invoice_no")
        .batch(100)
        .start(500)
        .build()
        .expect("valid sequence");
    apply(
        &client,
        &generate_sequence_sql(&invoice).expect("sequence sql"),
    )
    .await;

    let parsed = parse_db_info(&info_for_db(&client).await).expect("parse INFO FOR DB");
    let stored: Vec<_> = parsed.sequences.values().cloned().collect();
    assert_eq!(
        stored,
        vec![invoice.clone()],
        "the engine echoes what we declared"
    );
    let diffs = diff_sequences(std::slice::from_ref(&invoice), &stored);
    assert!(
        diffs.is_empty(),
        "a sequence re-applies on every boot: {diffs:#?}"
    );

    // A bare sequence takes the engine's defaults, which the renderer spells
    // out, so it too compares equal after a round trip.
    let bare = SequenceDefinition::new("plain");
    apply(
        &client,
        &generate_sequence_sql(&bare).expect("sequence sql"),
    )
    .await;
    let parsed = parse_db_info(&info_for_db(&client).await).expect("parse INFO FOR DB");
    assert_eq!(parsed.sequences.get("plain"), Some(&bare));

    let first = client
        .query(&format!(
            "RETURN {};",
            SequenceDefinition::nextval_surql("invoice_no")
        ))
        .await
        .expect("draw a value");
    let second = client
        .query(&format!(
            "RETURN {};",
            SequenceDefinition::nextval_surql("invoice_no")
        ))
        .await
        .expect("draw another value");
    let first = serde_json::to_string(&first).expect("serialise");
    let second = serde_json::to_string(&second).expect("serialise");
    assert_ne!(
        first, second,
        "the sequence advances: {first} then {second}"
    );

    // A modification applies through the OVERWRITE form the diff renders.
    let widened = sequence_schema("invoice_no")
        .batch(250)
        .start(500)
        .build()
        .unwrap();
    let diffs = diff_sequences(std::slice::from_ref(&widened), &stored);
    assert_eq!(diffs.len(), 1);
    apply(&client, &[diffs[0].forward_sql.clone()]).await;
    let parsed = parse_db_info(&info_for_db(&client).await).expect("parse INFO FOR DB");
    assert_eq!(
        parsed.sequences.get("invoice_no").map(|s| s.batch),
        Some(250)
    );

    // And the removal the drop diff renders takes it away again.
    apply(&client, &[SequenceDefinition::remove_surql("invoice_no")]).await;
    let parsed = parse_db_info(&info_for_db(&client).await).expect("parse INFO FOR DB");
    assert!(!parsed.sequences.contains_key("invoice_no"));
}

/// A custom function round-trips through `INFO FOR DB` despite the engine
/// rewriting what it stores, and is callable.
#[tokio::test]
async fn function_round_trips_and_is_callable() {
    let client = memory_client().await;

    let greet = function_schema("greet", "RETURN 'hi ' + $name;")
        .arg("name", "string")
        .arg("loud", "option<bool>")
        .returns("string")
        .comment("greeter")
        .build()
        .expect("valid function");
    apply(
        &client,
        &generate_function_sql(&greet).expect("function sql"),
    )
    .await;

    let parsed = parse_db_info(&info_for_db(&client).await).expect("parse INFO FOR DB");
    let stored: Vec<_> = parsed.functions.values().cloned().collect();
    assert_eq!(stored.len(), 1);
    // The engine rewrites option<bool> to none | bool, drops the trailing
    // semicolon, and adds PERMISSIONS FULL; the diff must see through all of
    // that or the function re-applies on every boot.
    assert_eq!(stored[0].args[1].arg_type, "none | bool");
    let diffs = diff_functions(std::slice::from_ref(&greet), &stored);
    assert!(
        diffs.is_empty(),
        "a function re-applies on every boot: {diffs:#?}\nstored: {stored:#?}"
    );

    let greeting = client
        .query("RETURN fn::greet('Mat', NONE);")
        .await
        .expect("call the function");
    let greeting = serde_json::to_string(&greeting).expect("serialise");
    assert!(greeting.contains("hi Mat"), "the function runs: {greeting}");

    // A changed body applies through the OVERWRITE form the diff renders.
    let shouty = function_schema("greet", "RETURN 'HI ' + $name")
        .arg("name", "string")
        .arg("loud", "option<bool>")
        .returns("string")
        .comment("greeter")
        .build()
        .unwrap();
    let diffs = diff_functions(std::slice::from_ref(&shouty), &stored);
    assert_eq!(diffs.len(), 1);
    apply(&client, &[diffs[0].forward_sql.clone()]).await;
    let greeting = client
        .query("RETURN fn::greet('Mat', NONE);")
        .await
        .expect("call the replaced function");
    assert!(serde_json::to_string(&greeting).unwrap().contains("HI Mat"));

    apply(&client, &[FunctionDefinition::remove_surql("greet")]).await;
    let parsed = parse_db_info(&info_for_db(&client).await).expect("parse INFO FOR DB");
    assert!(parsed.functions.is_empty());
}

/// A database-level param round-trips through `INFO FOR DB` and is readable
/// from a query without being bound.
#[tokio::test]
async fn param_round_trips_and_is_readable() {
    let client = memory_client().await;

    let app = param_schema("APP_NAME", "'oneiriq'")
        .comment("display name")
        .build()
        .expect("valid param");
    apply(&client, &generate_param_sql(&app).expect("param sql")).await;

    let parsed = parse_db_info(&info_for_db(&client).await).expect("parse INFO FOR DB");
    let stored: Vec<_> = parsed.params.values().cloned().collect();
    assert_eq!(stored.len(), 1);
    assert_eq!(stored[0].value, "'oneiriq'");
    // The engine adds PERMISSIONS FULL the code never wrote; the diff must
    // see through it or the param re-applies on every boot.
    assert_eq!(stored[0].permissions.as_deref(), Some("FULL"));
    let diffs = diff_params(std::slice::from_ref(&app), &stored);
    assert!(
        diffs.is_empty(),
        "a param re-applies on every boot: {diffs:#?}\nstored: {stored:#?}"
    );

    let read = client
        .query("RETURN $APP_NAME;")
        .await
        .expect("read the param");
    assert!(serde_json::to_string(&read).unwrap().contains("oneiriq"));

    // A changed value applies through the OVERWRITE form the diff renders.
    let renamed = param_schema("APP_NAME", "'oneiriq-rs'")
        .comment("display name")
        .build()
        .unwrap();
    let diffs = diff_params(std::slice::from_ref(&renamed), &stored);
    assert_eq!(diffs.len(), 1);
    apply(&client, &[diffs[0].forward_sql.clone()]).await;
    let read = client
        .query("RETURN $APP_NAME;")
        .await
        .expect("read the replaced param");
    assert!(serde_json::to_string(&read).unwrap().contains("oneiriq-rs"));

    apply(&client, &[ParamDefinition::remove_surql("APP_NAME")]).await;
    let parsed = parse_db_info(&info_for_db(&client).await).expect("parse INFO FOR DB");
    assert!(parsed.params.is_empty());
}

/// The exact call the CLI makes: the raw `query` response, wrapper and
/// all, straight into the parser. This shape used to fail to parse,
/// and `unwrap_or_default` at the CLI call sites turned the failure
/// into `surql schema tables` reporting every database as empty.
#[tokio::test]
async fn parse_db_info_reads_the_raw_query_response() {
    let client = memory_client().await;
    client
        .query("DEFINE TABLE user SCHEMAFULL; DEFINE FIELD name ON user TYPE string;")
        .await
        .expect("define table");
    let raw = client.query("INFO FOR DB;").await.expect("INFO FOR DB");
    let parsed = parse_db_info(&raw).expect("parse the wrapped response");
    assert!(parsed.tables.contains_key("user"), "{parsed:?}");
}

/// The engine backfills nothing when a field gains `REFERENCE`, and a
/// self-assignment registers nothing either: after the DDL alone every
/// pre-existing row is invisible to `<~`. The diff knows this and
/// carries the rewrite; applying both halves makes the tracking true,
/// and the result round-trips with no residual change.
#[tokio::test]
async fn a_gained_reference_backfills_pre_existing_rows() {
    let client = memory_client().await;

    // v1: a plain record link, no REFERENCE, with a row already in it.
    let target = table_schema("b")
        .with_mode(TableMode::Schemafull)
        .with_fields([string_field("name").build_unchecked().unwrap()]);
    let old_link = record_field("link", Some("b"))
        .nullable(true)
        .build_unchecked()
        .unwrap();
    let holder_v1 = table_schema("f")
        .with_mode(TableMode::Schemafull)
        .with_fields([old_link.clone()]);
    let mut ddl = generate_table_sql(&target, true);
    ddl.extend(generate_table_sql(&holder_v1, true));
    apply(&client, &ddl).await;
    client
        .query("CREATE b:one SET name = 'target'; CREATE f:alpha SET link = b:one;")
        .await
        .expect("pre-reference rows");

    // v2: the same field gains REFERENCE. The diff carries both halves.
    let new_link = record_field("link", Some("b"))
        .nullable(true)
        .reference(ReferenceAction::Ignore)
        .build_unchecked()
        .unwrap();
    let diffs = diff_fields("f", std::slice::from_ref(&new_link), &[old_link]);
    assert_eq!(diffs.len(), 1, "{diffs:#?}");
    let backfill = diffs[0]
        .reference_backfill_sql()
        .expect("a gained reference carries its backfill")
        .to_owned();

    // The DDL alone: the pre-existing row is invisible. This is the
    // engine behaviour the rewrite exists for; if this half ever fails,
    // the engine learned to backfill and the rewrite can retire.
    apply(&client, &[diffs[0].forward_sql.clone()]).await;
    let unseen = client
        .query("SELECT VALUE <~f FROM b:one;")
        .await
        .expect("reverse read");
    assert_eq!(
        serde_json::to_string(&unseen).unwrap(),
        "[[[]]]",
        "the engine now backfills on its own; retire the rewrite",
    );

    // The rewrite the diff carries makes the tracking true.
    apply(&client, &[backfill]).await;
    let seen = client
        .query("SELECT VALUE <~f FROM b:one;")
        .await
        .expect("reverse read");
    assert!(
        serde_json::to_string(&seen).unwrap().contains("f:alpha"),
        "{seen:?}"
    );

    // And the field round-trips: no residual diff, no boot loop.
    let stored = read_back(&client, "f").await;
    let residual = diff_fields("f", &[new_link], &stored.fields);
    assert!(residual.is_empty(), "{residual:#?}");
}

/// Registration works when the rewrite shares one buffered transaction
/// with the DDL that defines the clause, which is exactly how the
/// migration executor runs a generated file: DDL, then dance, one
/// `BEGIN ... COMMIT`.
#[tokio::test]
async fn the_backfill_registers_inside_the_ddl_transaction() {
    use surql::connection::Transaction;
    use surql::schema::reference_backfill_sql;

    let client = memory_client().await;
    client
        .query(
            "DEFINE TABLE b SCHEMAFULL; DEFINE FIELD name ON b TYPE string;\
             DEFINE TABLE f SCHEMAFULL; DEFINE FIELD link ON f TYPE option<record<b>>;",
        )
        .await
        .expect("base schema");
    client
        .query("CREATE b:one SET name = 'target'; CREATE f:alpha SET link = b:one;")
        .await
        .expect("pre-reference rows");

    let mut tx = Transaction::begin(&client).await.expect("begin");
    tx.execute(
        "DEFINE FIELD OVERWRITE link ON f TYPE option<record<b>> REFERENCE ON DELETE IGNORE;",
    )
    .await
    .expect("buffer ddl");
    tx.execute(&reference_backfill_sql("f", "link").expect("render backfill"))
        .await
        .expect("buffer dance");
    tx.commit().await.expect("commit");

    let seen = client
        .query("SELECT VALUE <~f FROM b:one;")
        .await
        .expect("reverse read");
    assert!(
        serde_json::to_string(&seen).unwrap().contains("f:alpha"),
        "{seen:?}"
    );
}

/// The vector table every DISKANN / F16 case below builds on: one array
/// column carrying three DISKANN/HNSW variants of the new surface.
fn vector_schema() -> TableDefinition {
    table_schema("vec")
        .with_mode(TableMode::Schemafull)
        .with_fields([array_field("v").build_unchecked().unwrap()])
        .with_indexes([
            // F16 elements on the established HNSW kind. EFC/M are spelled
            // because the engine echoes its 150/12 defaults regardless.
            hnsw_index(
                "pb",
                "v",
                3,
                HnswDistanceType::Cosine,
                MTreeVectorType::F16,
                Some(150),
                Some(12),
            ),
            // A bare DISKANN: the builder fills DEGREE/L_BUILD/ALPHA with
            // the engine defaults the echo always spells.
            diskann_index(
                "pc",
                "v",
                3,
                DiskAnnDistanceType::Cosine,
                MTreeVectorType::F32,
            ),
            // The full tail: tuned DEGREE/L_BUILD/ALPHA plus HASHED_VECTOR.
            diskann_index(
                "pd",
                "v",
                3,
                DiskAnnDistanceType::CosineNormalized,
                MTreeVectorType::F16,
            )
            .with_degree(48)
            .with_l_build(90)
            .with_alpha(1.5)
            .with_hashed_vector(true),
        ])
}

/// DISKANN and F16 definitions survive the whole code -> DDL -> engine ->
/// `INFO` -> parser -> diff cycle with no residual change — the guard
/// against every boot re-applying the same index forever.
#[tokio::test]
async fn diskann_and_f16_indexes_round_trip_through_the_parser() {
    let client = memory_client().await;
    let vec_table = vector_schema();
    apply(&client, &generate_table_sql(&vec_table, true)).await;

    let stored = read_back(&client, "vec").await;
    let diffs = diff_indexes("vec", &vec_table.indexes, &stored.indexes);
    assert!(
        diffs.is_empty(),
        "a DISKANN/F16 index re-applies on every boot: {diffs:#?}"
    );

    // Stronger than the name-keyed diff: every member the engine echoes
    // parses back to exactly what the builder declared, ALPHA `f` suffix
    // and all.
    for code in &vec_table.indexes {
        let stored_idx = stored
            .indexes
            .iter()
            .find(|i| i.name == code.name)
            .unwrap_or_else(|| panic!("stored table lost index {}", code.name));
        assert_eq!(stored_idx, code, "{} drifted through the echo", code.name);
    }
}

/// The `<|k,ef|>` KNN operator reaches a DISKANN index through the same
/// `KnnScan` plan HNSW gets, and the bare `<|k|>` form stays a hard error.
/// DISKANN is the only index on the table, so the plan cannot be riding
/// anything else.
#[tokio::test]
async fn knn_scan_reaches_a_diskann_index() {
    let client = memory_client().await;
    let near = table_schema("near")
        .with_mode(TableMode::Schemafull)
        .with_fields([array_field("v").build_unchecked().unwrap()])
        .with_indexes([diskann_index(
            "pc",
            "v",
            3,
            DiskAnnDistanceType::Cosine,
            MTreeVectorType::F16,
        )]);
    apply(&client, &generate_table_sql(&near, true)).await;
    client
        .query("CREATE near:one SET v = [1.0, 2.0, 3.0]; CREATE near:two SET v = [1.1, 2.0, 3.0];")
        .await
        .expect("seed vectors");

    let query = Query::new()
        .from_table("near")
        .expect("table")
        .select(Some(vec!["id".into()]))
        .vector_search_indexed("v", vec![1.0, 2.0, 3.0], 2, 40)
        .expect("knn query")
        .to_surql()
        .expect("render");
    let explain = client
        .query(&format!("{} EXPLAIN;", query.trim_end_matches(';')))
        .await
        .expect("EXPLAIN");
    let plan = serde_json::to_string(&explain).expect("serialise plan");
    assert!(
        plan.contains("KnnScan"),
        "the index carries the scan: {plan}"
    );
    assert!(
        plan.contains("\"index\":\"pc\""),
        "the plan names the DISKANN index: {plan}"
    );

    let rows = client.query(&query).await.expect("run KNN");
    let rows = serde_json::to_string(&rows).expect("serialise rows");
    assert!(rows.contains("near:one"), "{rows}");
    assert!(rows.contains("near:two"), "{rows}");

    // The KTree-era bare form is gone; only `<|k,EF|>` (or an explicit
    // metric for brute force) survives in 3.x.
    let bare = client
        .query("SELECT id FROM near WHERE v <|2|> [1.0, 2.0, 3.0];")
        .await;
    assert!(bare.is_err(), "the engine refuses the bare <|k|> form");
}

/// The exact shape a caller gets from `query`: the raw response,
/// wrapper and all, straight into the table parser. Mirrors
/// `parse_db_info_reads_the_raw_query_response` — same tolerance, same
/// unambiguity argument, so no caller has to remember which parsers
/// need the wrapper indexed off.
#[tokio::test]
async fn parse_table_full_reads_the_raw_query_response() {
    let client = memory_client().await;
    client
        .query("DEFINE TABLE gadget SCHEMAFULL; DEFINE FIELD name ON gadget TYPE string;")
        .await
        .expect("define table");
    let raw = client
        .query("INFO FOR TABLE gadget;")
        .await
        .expect("INFO FOR TABLE");
    let parsed = parse_table_full("gadget", "DEFINE TABLE gadget SCHEMAFULL", &raw)
        .expect("parse the wrapped response");
    assert_eq!(parsed.mode, TableMode::Schemafull);
    assert!(parsed.fields.iter().any(|f| f.name == "name"), "{parsed:?}");
}
