//! Round trips through a real engine: definition -> `to_surql` -> engine ->
//! `INFO FOR` -> parser -> definition.
//!
//! Every case renders DDL from the builders, applies it, reads the engine's
//! echo back through the parser, and compares with the definition it came
//! from. The echo is the authority: a shape the parser misreads makes a
//! reconcile re-apply (or silently weaken) a definition forever.
//!
//! Runs against `SURREAL_URL` when it is set (CI uses v3.0.5), otherwise
//! against the in-process `mem://` engine:
//!
//! ```text
//! SURREAL_URL=ws://localhost:8000 SURREAL_USER=root SURREAL_PASS=root \
//!   cargo test --all-features --test integration_schema_roundtrip -- --test-threads=1
//! ```

#![cfg(any(feature = "client", feature = "client-rustls"))]

use std::collections::BTreeMap;
use std::env;
use std::sync::atomic::{AtomicU64, Ordering};

use serde_json::Value;

use surql::connection::{ConnectionConfig, DatabaseClient};
use surql::schema::parser::{parse_db_info, parse_table_full};
use surql::schema::{
    analyzer, bm25_index, event, function_schema, index, jwt_access, param_schema,
    record_access, string_field, table_schema, typed_edge, AccessType, FieldDefinition,
    FieldType, JwtConfig, RecordAccessConfig, TableDefinition, TokenFilter, Tokenizer,
};

static DB_COUNTER: AtomicU64 = AtomicU64::new(0);

async fn client() -> DatabaseClient {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos());
    let seq = DB_COUNTER.fetch_add(1, Ordering::Relaxed);
    let name = format!("it_rt_{nanos}_{seq}");
    let builder = ConnectionConfig::builder()
        .namespace(format!("ns_{name}"))
        .database(name);
    let cfg = match env::var("SURREAL_URL") {
        Ok(url) => builder
            .url(url)
            .username(env::var("SURREAL_USER").unwrap_or_else(|_| "root".into()))
            .password(env::var("SURREAL_PASS").unwrap_or_else(|_| "root".into()))
            .timeout(10.0),
        Err(_) => builder.url("mem://"),
    }
    .build()
    .expect("valid config");
    let client = DatabaseClient::new(cfg).expect("client constructs");
    client.connect().await.expect("connect");
    client
}

fn first(value: &Value) -> Value {
    value
        .as_array()
        .and_then(|a| a.first())
        .cloned()
        .unwrap_or(Value::Null)
}

async fn apply(client: &DatabaseClient, statements: &[String]) {
    let script = statements.join("\n");
    client
        .query(&script)
        .await
        .unwrap_or_else(|e| panic!("apply DDL failed: {e}\n{script}"));
}

async fn info_for_db(client: &DatabaseClient) -> Value {
    first(&client.query("INFO FOR DB;").await.expect("INFO FOR DB"))
}

/// The raw echo of one database-level definition.
fn echo<'a>(info: &'a Value, kind: &str, name: &str) -> &'a str {
    info[kind][name]
        .as_str()
        .unwrap_or_else(|| panic!("no {kind}.{name} in {info}"))
}

/// Read one table back through both `INFO` levels, from the raw echo.
async fn read_table(client: &DatabaseClient, name: &str) -> TableDefinition {
    let db = info_for_db(client).await;
    let define = echo(&db, "tables", name).to_string();
    let info = first(
        &client
            .query(&format!("INFO FOR TABLE {name};"))
            .await
            .expect("INFO FOR TABLE"),
    );
    parse_table_full(name, &define, &info).expect("parse INFO FOR TABLE")
}

fn field<'a>(table: &'a TableDefinition, name: &str) -> &'a FieldDefinition {
    table
        .fields
        .iter()
        .find(|f| f.name == name)
        .unwrap_or_else(|| panic!("{} has no field {name}", table.name))
}

fn map(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
    pairs
        .iter()
        .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
        .collect()
}

#[tokio::test]
async fn field_permissions_reach_the_engine_and_read_back() {
    let client = client().await;
    let ssn = string_field("ssn")
        .permissions([("select", "$auth.admin = true"), ("update", "NONE")])
        .build_unchecked()
        .unwrap();
    let open = string_field("nick").build_unchecked().unwrap();
    let table = table_schema("person").with_fields([ssn.clone(), open]);
    apply(&client, &table.to_surql_all()).await;

    let parsed = read_table(&client, "person").await;
    assert_eq!(field(&parsed, "ssn").permissions, ssn.permissions);
    assert!(field(&parsed, "nick").permissions.is_none());
}

#[tokio::test]
async fn table_permissions_read_back_without_the_separator() {
    let client = client().await;
    let tenant = table_schema("tenant_doc").with_permissions([("select", "tenant = $auth.tenant")]);
    let open_delete = table_schema("open_delete")
        .with_permissions([("select", "a = 1"), ("delete", "FULL")]);
    let full = table_schema("everyone").with_permissions([
        ("select", "FULL"),
        ("create", "FULL"),
        ("update", "FULL"),
        ("delete", "FULL"),
    ]);
    let plain = table_schema("plain");
    apply(
        &client,
        &[
            tenant.to_surql(),
            open_delete.to_surql(),
            full.to_surql(),
            plain.to_surql(),
        ],
    )
    .await;

    let db = parse_db_info(&info_for_db(&client).await).unwrap();
    assert_eq!(
        db.tables["tenant_doc"].permissions,
        Some(map(&[("select", "tenant = $auth.tenant")]))
    );
    assert_eq!(db.tables["open_delete"].permissions, open_delete.permissions);
    assert_eq!(db.tables["everyone"].permissions, full.permissions);
    assert!(db.tables["plain"].permissions.is_none());
}

#[tokio::test]
async fn edge_endpoints_read_back_and_render_again() {
    let client = client().await;
    let likes = typed_edge("likes", "user", "post");
    apply(
        &client,
        &[
            table_schema("user").to_surql(),
            table_schema("post").to_surql(),
            likes.to_surql().unwrap(),
        ],
    )
    .await;

    let db = parse_db_info(&info_for_db(&client).await).unwrap();
    let parsed = &db.edges["likes"];
    assert_eq!(parsed.from_table.as_deref(), Some("user"));
    assert_eq!(parsed.to_table.as_deref(), Some("post"));
    assert_eq!(parsed.to_surql().unwrap(), likes.to_surql().unwrap());
}

#[tokio::test]
async fn keyword_named_fields_and_quoted_clauses_read_back() {
    let client = client().await;
    let fields = [
        FieldDefinition::new("default", FieldType::Bool).with_default("false"),
        FieldDefinition::new("reference", FieldType::String),
        FieldDefinition::new("value", FieldType::Int).with_value("1"),
        FieldDefinition::new("note", FieldType::String).with_default("'no comment'"),
        FieldDefinition::new("tag", FieldType::String)
            .with_assertion("$value INSIDE (SELECT VALUE name FROM label)"),
        FieldDefinition::new("mode", FieldType::String)
            .with_assertion("$value INSIDE ['readonly', 'x']"),
    ];
    let table = table_schema("card").with_fields(fields.clone());
    apply(&client, &table.to_surql_all()).await;

    let parsed = read_table(&client, "card").await;
    for code in &fields {
        let db = field(&parsed, &code.name);
        assert_eq!(db.field_type, code.field_type, "{}", code.name);
        assert_eq!(db.default, code.default, "{}", code.name);
        assert_eq!(db.value, code.value, "{}", code.name);
        assert_eq!(db.assertion, code.assertion, "{}", code.name);
        assert_eq!(db.reference, code.reference, "{}", code.name);
        assert_eq!(db.readonly, code.readonly, "{}", code.name);
    }
}

#[tokio::test]
async fn a_fulltext_index_reads_back_its_one_column() {
    let client = client().await;
    let table = table_schema("research_paper")
        .with_fields([
            string_field("body").build_unchecked().unwrap(),
            FieldDefinition::new("year", FieldType::Int),
        ])
        .with_indexes([
            bm25_index("body_search", ["body"], "english_text").with_highlights(),
            index("by_year", ["year"]),
        ]);
    let text = analyzer("english_text")
        .with_tokenizer(Tokenizer::Class)
        .with_filters([TokenFilter::Lowercase, TokenFilter::snowball("english")]);
    let mut statements = vec![text.to_surql()];
    statements.extend(table.to_surql_all());
    apply(&client, &statements).await;

    let parsed = read_table(&client, "research_paper").await;
    let search = parsed
        .indexes
        .iter()
        .find(|i| i.name == "body_search")
        .unwrap();
    assert_eq!(search, &table.indexes[0]);
    let by_year = parsed.indexes.iter().find(|i| i.name == "by_year").unwrap();
    assert_eq!(by_year, &table.indexes[1]);

    let db = parse_db_info(&info_for_db(&client).await).unwrap();
    assert_eq!(db.analyzers["english_text"], text);
}

#[tokio::test]
async fn a_multi_statement_event_stays_inside_the_event() {
    let client = client().await;
    apply(
        &client,
        &[
            table_schema("counter").to_surql(),
            table_schema("scratch").to_surql(),
            "CREATE scratch:keep;".to_string(),
        ],
    )
    .await;
    let bump = event(
        "bump",
        "$event = 'CREATE'",
        "UPDATE counter:main SET n += 1; DELETE scratch",
    );
    let table = table_schema("doc").with_events([bump.clone()]);
    apply(&client, &table.to_surql_all()).await;

    // Applying the definition must not have run the action.
    let kept = client.query("SELECT * FROM scratch;").await.unwrap();
    assert_eq!(first(&kept).as_array().map_or(0, Vec::len), 1, "{kept}");

    let parsed = read_table(&client, "doc").await;
    assert_eq!(parsed.events[0].action, bump.action);
}

#[tokio::test]
async fn jwt_access_in_the_engine_grammar_applies_and_reads_back() {
    let client = client().await;
    let jwks = jwt_access(
        "sso",
        JwtConfig::new("RS256")
            .with_url("https://auth.example.com/jwks")
            .with_issuer("private-key"),
    );
    let keyed = jwt_access(
        "api",
        JwtConfig::new("RS256")
            .with_key("public-key")
            .with_issuer("private-key"),
    );
    let record = record_access(
        "account",
        RecordAccessConfig::new()
            .with_signup("CREATE user SET name = $name")
            .with_signin("SELECT * FROM user WHERE name = $name")
            .with_jwt(JwtConfig::hs256("it's a \\ secret")),
    )
    .with_session("2h");
    apply(
        &client,
        &[
            jwks.to_surql().unwrap(),
            keyed.to_surql().unwrap(),
            record.to_surql().unwrap(),
        ],
    )
    .await;

    let db = parse_db_info(&info_for_db(&client).await).unwrap();
    let sso = db.accesses["sso"].jwt.as_ref().unwrap();
    assert_eq!(sso.url.as_deref(), Some("https://auth.example.com/jwks"));
    assert!(sso.issuer.is_some());
    let api = db.accesses["api"].jwt.as_ref().unwrap();
    assert_eq!(api.algorithm, "RS256");
    assert_eq!(api.key.as_deref(), Some("public-key"));
    let account = &db.accesses["account"];
    assert_eq!(account.access_type, AccessType::Record);
    let rec = account.record.as_ref().unwrap();
    assert_eq!(
        rec.signin.as_deref(),
        Some("SELECT * FROM user WHERE name = $name")
    );
    assert_eq!(rec.jwt.as_ref().unwrap().algorithm, "HS256");
    assert_eq!(account.duration_session.as_deref(), Some("2h"));
    assert!(account.duration_token.is_none());
}

#[tokio::test]
async fn functions_params_and_comments_round_trip_and_cannot_inject() {
    let client = client().await;
    apply(&client, &[table_schema("user").to_surql()]).await;
    let strip = function_schema("strip", "RETURN string::replace($x, '}', '')")
        .arg("x", "string")
        .returns("string")
        .comment("x'; REMOVE TABLE user; --")
        .build()
        .unwrap();
    let open = function_schema("open", "RETURN '{' + $x")
        .arg("x", "string")
        .comment("line1\nit's")
        .build()
        .unwrap();
    let obj = param_schema("obj", "{ comment: 'x', permissions: 'y' }")
        .comment("user's")
        .build()
        .unwrap();
    apply(
        &client,
        &[
            strip.to_surql().unwrap(),
            open.to_surql().unwrap(),
            obj.to_surql().unwrap(),
        ],
    )
    .await;

    let raw = info_for_db(&client).await;
    assert!(
        raw["tables"]["user"].is_string(),
        "the comment was executed: {raw}"
    );
    let db = parse_db_info(&raw).unwrap();
    assert_eq!(db.functions["strip"].normalized(), strip.normalized());
    assert_eq!(db.functions["open"].normalized(), open.normalized());
    assert_eq!(db.params["obj"].normalized(), obj.normalized());
}
