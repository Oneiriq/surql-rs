//! Integration tests pinning the query layer's escaping and rendering
//! against a real engine: hostile data and names stay data, graph depth and
//! direction reach the intended records, and responses keep their rows.
//!
//! Gated on the `SURREAL_URL` env var so `cargo test` stays green when no
//! SurrealDB server is reachable. Exercise with:
//!
//! ```text
//! docker run -d -p 8000:8000 surrealdb/surrealdb:v3.0.5 start --user root --pass root memory
//! SURREAL_URL=ws://localhost:8000 SURREAL_USER=root SURREAL_PASS=root \
//!   cargo test --all-features --test integration_query_hardening -- --test-threads=1
//! ```

#![cfg(any(feature = "client", feature = "client-rustls"))]

use std::env;
use std::sync::atomic::{AtomicU64, Ordering};

use serde_json::{json, Value};
use surql::connection::{ConnectionConfig, DatabaseClient};
use surql::query::builder::Query;
use surql::query::expressions::time_now;
use surql::query::hints::{IndexHint, QueryHint, TimeoutHint};
use surql::query::{batch, crud, executor, graph, DataMap, GraphQuery};
use surql::types::operators::{eq_expr, type_thing};
use surql::types::record_ref;

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
    format!("it_hardening_{nanos}_{seq}")
}

async fn connected_client() -> Option<DatabaseClient> {
    let url = env_url()?;
    let database = unique_db();
    let cfg = ConnectionConfig::builder()
        .url(url)
        .namespace(format!("ns_{database}"))
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

async fn count(client: &DatabaseClient, table: &str) -> i64 {
    crud::count_records(client, table, None)
        .await
        .expect("count")
}

fn data(pairs: &[(&str, Value)]) -> DataMap {
    pairs
        .iter()
        .map(|(k, v)| ((*k).to_owned(), v.clone()))
        .collect()
}

#[tokio::test]
async fn expression_shaped_data_is_stored_as_data() {
    let Some(client) = connected_client().await else {
        println!("skipped: SURREAL_URL not set");
        return;
    };
    client
        .query("CREATE person:keep SET name = 'keep';")
        .await
        .expect("seed");

    let hostile = json!({"expression": "1}; DELETE person; --", "a b": {"": 1}});
    let q = Query::new()
        .insert("post", data(&[("body", hostile.clone())]))
        .unwrap();
    let raw = q
        .execute(&client)
        .await
        .expect("insert runs as one statement");
    let rows = executor::execute_raw(&client, "SELECT VALUE body FROM post", None)
        .await
        .expect("read back");
    assert_eq!(rows, json!([[hostile]]), "insert returned {raw:?}");
    assert_eq!(count(&client, "person").await, 1, "person table untouched");

    client.disconnect().await.unwrap();
}

#[tokio::test]
async fn hostile_targets_name_a_record_and_nothing_else() {
    let Some(client) = connected_client().await else {
        println!("skipped: SURREAL_URL not set");
        return;
    };
    client
        .query("CREATE person:a, person:b, person:c;")
        .await
        .expect("seed");

    Query::new()
        .delete("person:x; REMOVE TABLE person; --")
        .unwrap()
        .execute(&client)
        .await
        .expect("delete of a missing record");
    assert_eq!(count(&client, "person").await, 3);

    let relate =
        batch::build_relate_query("person:a; REMOVE TABLE person", "knows", "person:b", None)
            .unwrap();
    client.query(&relate).await.expect("relate runs");
    assert_eq!(count(&client, "person").await, 3);

    graph::create_relation(
        &client,
        "knows",
        "person:a",
        "person:b; DELETE person",
        None,
    )
    .await
    .expect("relation to a missing record");
    assert_eq!(count(&client, "person").await, 3);

    client.disconnect().await.unwrap();
}

#[tokio::test]
async fn rows_keep_their_shape() {
    let Some(client) = connected_client().await else {
        println!("skipped: SURREAL_URL not set");
        return;
    };
    client
        .query(
            "CREATE job:1 SET result = 'ok';\n\
             CREATE job:2 SET result = NULL;\n\
             CREATE post:1 SET tags = ['a', 'b'];\n\
             CREATE post:2 SET tags = ['c'];",
        )
        .await
        .expect("seed");

    let jobs = Query::new()
        .select(None)
        .from_table("job")
        .unwrap()
        .order_by("id", "ASC")
        .unwrap();
    let rows: Vec<Value> = executor::fetch_all(&client, &jobs).await.expect("jobs");
    assert_eq!(
        rows,
        vec![
            json!({"id": "job:1", "result": "ok"}),
            json!({"id": "job:2", "result": null})
        ]
    );

    // `SELECT VALUE tags` returns one array per row; each stays one row.
    let tags = Query::new()
        .select(Some(vec!["VALUE tags".into()]))
        .from_table("post")
        .unwrap();
    let mut rows: Vec<Vec<String>> = executor::fetch_all(&client, &tags).await.expect("tags");
    rows.sort();
    assert_eq!(rows, vec![vec!["a", "b"], vec!["c"]]);

    client.disconnect().await.unwrap();
}

#[tokio::test]
async fn upsert_many_resolves_ids_and_conflicts_on_both_paths() {
    let Some(client) = connected_client().await else {
        println!("skipped: SURREAL_URL not set");
        return;
    };

    // A bare id is a key of the table; an integer id an integer key.
    batch::upsert_many(
        &client,
        "person",
        vec![
            json!({"id": "alice", "name": "Alice"}),
            json!({"id": 7, "name": "Seven"}),
        ],
        None,
    )
    .await
    .expect("upsert by id");
    let alice = executor::execute_raw(&client, "SELECT VALUE name FROM person:alice", None)
        .await
        .unwrap();
    assert_eq!(alice, json!([["Alice"]]));
    let seven = executor::execute_raw(&client, "SELECT VALUE name FROM person:7", None)
        .await
        .unwrap();
    assert_eq!(seven, json!([["Seven"]]));

    // Conflict fields make the autocommit path update the matching row
    // instead of inserting a second one.
    let fields = vec!["email".to_string()];
    for name in ["first", "second"] {
        batch::upsert_many(
            &client,
            "account",
            vec![json!({"email": "a@x.com", "name": name})],
            Some(&fields),
        )
        .await
        .expect("upsert by conflict field");
    }
    assert_eq!(count(&client, "account").await, 1);
    let name = executor::execute_raw(&client, "SELECT VALUE name FROM account", None)
        .await
        .unwrap();
    assert_eq!(name, json!([["second"]]));

    // Without an id or conflict fields there is nothing to upsert against.
    let err = batch::upsert_many(&client, "account", vec![json!({"name": "x"})], None).await;
    assert!(err.is_err());
    assert_eq!(count(&client, "account").await, 1);

    // A qualified id must stay in the table.
    let err = batch::delete_many(&client, "person", vec!["account:1".into()]).await;
    assert!(err.is_err());
    let deleted = batch::delete_many(&client, "person", vec!["alice".into(), "7".into()])
        .await
        .expect("delete_many");
    assert_eq!(deleted.len(), 2);

    client.disconnect().await.unwrap();
}

#[tokio::test]
async fn graph_depth_and_direction_reach_the_intended_records() {
    let Some(client) = connected_client().await else {
        println!("skipped: SURREAL_URL not set");
        return;
    };
    client
        .query(
            "CREATE person:alice, person:bob, person:charlie;\n\
             RELATE person:alice->follows->person:bob;\n\
             RELATE person:bob->follows->person:charlie;",
        )
        .await
        .expect("seed graph");

    let ids = |rows: Vec<Value>| -> Vec<String> {
        rows.iter()
            .filter_map(|r| r.get("id").and_then(Value::as_str).map(str::to_owned))
            .collect()
    };

    let two_hops = GraphQuery::new("person:alice")
        .out("follows", Some(2))
        .to("person")
        .execute(&client)
        .await
        .expect("two hops");
    assert_eq!(ids(two_hops), vec!["person:charlie"]);

    let followers = GraphQuery::new("person:bob")
        .r#in("follows", None)
        .to("person")
        .execute(&client)
        .await
        .expect("incoming");
    assert_eq!(ids(followers), vec!["person:alice"]);

    let fetched = GraphQuery::new("person:alice")
        .out("follows", None)
        .fetch(["out"])
        .limit(1)
        .unwrap()
        .execute(&client)
        .await
        .expect("limit then fetch parses");
    assert_eq!(fetched.len(), 1);
    assert_eq!(fetched[0]["out"]["id"], json!("person:bob"));

    let deep: Vec<Value> = graph::traverse_with_depth(
        &client,
        "person:alice",
        "follows",
        "person",
        graph::Direction::Out,
        Some(2),
        None,
    )
    .await
    .expect("traverse_with_depth");
    assert_eq!(ids(deep), vec!["person:charlie"]);

    let err = graph::shortest_path(
        &client,
        "person:alice",
        "person:charlie",
        "follows",
        33,
        None,
    )
    .await;
    assert!(err.is_err());

    client.disconnect().await.unwrap();
}

#[tokio::test]
async fn typed_channels_render_raw_values() {
    let Some(client) = connected_client().await else {
        println!("skipped: SURREAL_URL not set");
        return;
    };
    client
        .query("CREATE person:alice; CREATE post:1 SET author = person:alice;")
        .await
        .expect("seed");

    let by_author = Query::new()
        .select(None)
        .from_table("post")
        .unwrap()
        .where_(eq_expr("author", record_ref("person", "alice")));
    let rows: Vec<Value> = executor::fetch_all(&client, &by_author).await.unwrap();
    assert_eq!(rows.len(), 1);

    let thing = type_thing("person", "alice").to_surql();
    let raw = executor::execute_raw(&client, &format!("RETURN {thing}"), None)
        .await
        .expect("type_thing parses on v3");
    assert_eq!(raw, json!(["person:alice"]));

    Query::new()
        .insert("event", data(&[("kind", json!("login"))]))
        .unwrap()
        .set_expr("at", time_now())
        .unwrap()
        .execute(&client)
        .await
        .expect("create with an expression field");
    let is_datetime = executor::execute_raw(
        &client,
        "SELECT VALUE type::is_datetime(at) FROM event",
        None,
    )
    .await
    .unwrap();
    assert_eq!(is_datetime, json!([[true]]));

    client.disconnect().await.unwrap();
}

#[tokio::test]
async fn hints_are_inert_comments() {
    let Some(client) = connected_client().await else {
        println!("skipped: SURREAL_URL not set");
        return;
    };
    client.query("CREATE person:a;").await.expect("seed");

    let hinted = Query::new()
        .select(None)
        .from_table("person")
        .unwrap()
        .hint(QueryHint::Timeout(TimeoutHint::new(5.0).unwrap()))
        .hint(QueryHint::Index(IndexHint::new("person", "no_such_index")));
    let rows: Vec<Value> = executor::fetch_all(&client, &hinted).await.unwrap();
    assert_eq!(rows.len(), 1);

    let breakout = Query::new()
        .select(None)
        .from_table("person")
        .unwrap()
        .hint(QueryHint::Index(IndexHint::new(
            "person",
            "x */ REMOVE TABLE person; /*",
        )));
    assert!(breakout.to_surql().is_err());
    assert_eq!(count(&client, "person").await, 1);

    client.disconnect().await.unwrap();
}

#[tokio::test]
async fn last_returns_the_final_row() {
    let Some(client) = connected_client().await else {
        println!("skipped: SURREAL_URL not set");
        return;
    };
    client
        .query("CREATE person:1 SET n = 1; CREATE person:2 SET n = 2; CREATE person:3 SET n = 3;")
        .await
        .expect("seed");

    let unordered = Query::new().select(None).from_table("person").unwrap();
    let last: Option<Value> = crud::last(&client, &unordered).await.unwrap();
    let first: Option<Value> = crud::first(&client, &unordered).await.unwrap();
    assert_ne!(last, first, "last must not repeat first");

    let paged = Query::new()
        .select(None)
        .from_table("person")
        .unwrap()
        .order_by("n", "ASC")
        .unwrap()
        .limit(2)
        .unwrap();
    let last: Option<Value> = crud::last(&client, &paged).await.unwrap();
    assert_eq!(last.and_then(|r| r.get("n").cloned()), Some(json!(2)));

    client.disconnect().await.unwrap();
}

/// Every word the engine's lexer knows (the `KEYWORDS` map in surrealdb-core
/// 3.2's `syn/lexer/keywords.rs`), lowercased. Each one has to work as a
/// table name through the builder and the CRUD helpers: that is the promise
/// `quote_ident`'s reserved-word rule makes.
const LEXER_WORDS: &[&str] = &[
    "access",
    "after",
    "algorithm",
    "all",
    "allinside",
    "alter",
    "always",
    "analyzer",
    "and",
    "andkw",
    "any",
    "anyinside",
    "api",
    "ar",
    "ara",
    "arabic",
    "array",
    "as",
    "asc",
    "ascending",
    "ascii",
    "assert",
    "async",
    "at",
    "authenticate",
    "auto",
    "backend",
    "batch",
    "bearer",
    "before",
    "begin",
    "blank",
    "bm25",
    "bool",
    "break",
    "bucket",
    "by",
    "bytes",
    "camel",
    "cancel",
    "capacity",
    "cascade",
    "changefeed",
    "changes",
    "chebyshev",
    "class",
    "collate",
    "collection",
    "columns",
    "comment",
    "commit",
    "compact",
    "computed",
    "concurrently",
    "config",
    "contains",
    "containsall",
    "containsany",
    "containsnone",
    "containsnot",
    "content",
    "continue",
    "cosine",
    "cosine_normalized",
    "count",
    "create",
    "da",
    "dan",
    "danish",
    "database",
    "datetime",
    "db",
    "de",
    "decimal",
    "default",
    "define",
    "delete",
    "desc",
    "descending",
    "deu",
    "diff",
    "dimension",
    "dist",
    "distance",
    "drop",
    "duplicate",
    "duration",
    "dutch",
    "eddsa",
    "edgengram",
    "efc",
    "el",
    "ell",
    "else",
    "en",
    "end",
    "enforced",
    "eng",
    "english",
    "es",
    "es256",
    "es384",
    "es512",
    "euclidean",
    "event",
    "exclude",
    "exists",
    "expired",
    "explain",
    "expunge",
    "extend_candidates",
    "f16",
    "f32",
    "f64",
    "false",
    "feature",
    "fetch",
    "fi",
    "field",
    "fields",
    "file",
    "filters",
    "fin",
    "finnish",
    "flex",
    "flexi",
    "flexible",
    "float",
    "fn",
    "for",
    "fr",
    "fra",
    "french",
    "from",
    "full",
    "fulltext",
    "function",
    "functions",
    "geometry",
    "german",
    "get",
    "grant",
    "graphql",
    "graphql_alias",
    "graphql_deprecated",
    "greek",
    "group",
    "hamming",
    "hashed_vector",
    "headers",
    "highlights",
    "hnsw",
    "hs256",
    "hs384",
    "hs512",
    "hu",
    "hun",
    "hungarian",
    "i16",
    "i32",
    "i64",
    "i8",
    "if",
    "ignore",
    "in",
    "include",
    "index",
    "info",
    "inner_product",
    "insert",
    "inside",
    "int",
    "intersects",
    "into",
    "is",
    "issuer",
    "it",
    "ita",
    "italian",
    "jaccard",
    "jwks",
    "jwt",
    "keep_pruned_connections",
    "key",
    "kill",
    "kv",
    "let",
    "limit",
    "line",
    "live",
    "lm",
    "lowercase",
    "m",
    "m0",
    "manhattan",
    "mapper",
    "maxdepth",
    "merge",
    "middleware",
    "minkowski",
    "ml",
    "mod",
    "model",
    "module",
    "multiline",
    "multipoint",
    "multipolygon",
    "namespace",
    "ngram",
    "nl",
    "nld",
    "no",
    "noindex",
    "none",
    "noneinside",
    "nor",
    "normal",
    "norwegian",
    "not",
    "notinside",
    "ns",
    "null",
    "number",
    "numeric",
    "object",
    "omit",
    "on",
    "only",
    "option",
    "or",
    "order",
    "original",
    "out",
    "outside",
    "overwrite",
    "parallel",
    "param",
    "passhash",
    "password",
    "patch",
    "pearson",
    "permissions",
    "point",
    "polygon",
    "por",
    "portuguese",
    "post",
    "postings_cache",
    "postings_order",
    "prepare",
    "ps256",
    "ps384",
    "ps512",
    "pt",
    "punct",
    "purge",
    "put",
    "rand",
    "range",
    "readonly",
    "rebuild",
    "record",
    "reference",
    "references",
    "refresh",
    "regex",
    "reject",
    "relate",
    "relation",
    "remove",
    "replace",
    "retry",
    "return",
    "revoke",
    "revoked",
    "ro",
    "roles",
    "romanian",
    "ron",
    "root",
    "rs256",
    "rs384",
    "rs512",
    "ru",
    "rus",
    "russian",
    "sc",
    "schemaful",
    "schemafull",
    "schemaless",
    "select",
    "sequence",
    "session",
    "set",
    "show",
    "signin",
    "signup",
    "silo",
    "since",
    "sleep",
    "snowball",
    "spa",
    "spanish",
    "split",
    "start",
    "strict",
    "string",
    "structure",
    "sv",
    "swe",
    "swedish",
    "system",
    "ta",
    "table",
    "tables",
    "tam",
    "tamil",
    "tb",
    "tempfiles",
    "terms_cache",
    "terms_order",
    "then",
    "throw",
    "timeout",
    "to",
    "token",
    "tokenizers",
    "tr",
    "trace",
    "transaction",
    "true",
    "tur",
    "turkish",
    "type",
    "u8",
    "ulid",
    "unique",
    "unset",
    "update",
    "uppercase",
    "upsert",
    "url",
    "use",
    "user",
    "uuid",
    "value",
    "values",
    "version",
    "vs",
    "when",
    "where",
    "with",
];

#[tokio::test]
async fn every_lexer_word_is_a_usable_table_name() {
    let Some(client) = connected_client().await else {
        println!("skipped: SURREAL_URL not set");
        return;
    };
    let mixed_case = ["SELECT", "Value", "NaN", "Infinity"];
    let mut failures = Vec::new();
    for word in LEXER_WORDS.iter().chain(mixed_case.iter()) {
        let q = Query::new()
            .insert(*word, data(&[("a", json!(1))]))
            .unwrap_or_else(|e| panic!("{word}: {e}"));
        if let Err(e) = q.execute(&client).await {
            failures.push(format!("CREATE {word}: {e}"));
            continue;
        }
        match crud::count_records(&client, word, None).await {
            Ok(1) => {}
            Ok(n) => failures.push(format!("count {word}: {n} rows")),
            Err(e) => failures.push(format!("count {word}: {e}")),
        }
    }
    client.disconnect().await.unwrap();
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
