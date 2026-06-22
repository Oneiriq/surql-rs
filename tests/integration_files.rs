//! Integration tests for the object-storage (files / buckets) surface.
//!
//! Buckets are an **experimental, hidden** SurrealDB v3 feature. The embedded
//! `mem://` engine used by the other always-on integration suites does not
//! enable experimental features, so the live round-trip here is split in two:
//!
//! * [`bucket_roundtrip_embedded_if_supported`] drives the in-process
//!   `mem://` engine and **skips gracefully** if `DEFINE BUCKET` is rejected
//!   (i.e. the embedded build has files disabled). When the embedded build
//!   does support it, this runs under plain `cargo test`.
//! * [`bucket_roundtrip_live_server`] is `#[ignore]`d and targets a real
//!   server reachable at `SURREAL_FILES_URL`. Run it explicitly with
//!   `cargo test --all-features --test integration_files -- --ignored`.
//!
//! ## Starting a server with files enabled
//!
//! The files feature is experimental *and hidden*: it is **not** turned on by
//! `--allow-all`, and the `--allow-experimental files` *flag* form is broken
//! (the bare `files` argument swallows the `memory` datastore positional,
//! producing `invalid experimental target name 'memory'`). Enable it with the
//! **environment variable** instead:
//!
//! ```powershell
//! $env:SURREAL_CAPS_ALLOW_EXPERIMENTAL='files'
//! surreal start --bind 127.0.0.1:8201 --user root --pass root --allow-all memory
//! ```
//!
//! then point the test at it:
//!
//! ```text
//! $env:SURREAL_FILES_URL='ws://127.0.0.1:8201'
//! cargo test --all-features --test integration_files -- --ignored
//! ```
//!
//! Pure DDL / SurrealQL-string / `FileRef` / bytes-binding behaviour is unit
//! tested in-crate (see `schema::bucket`, `types::file`, `query::files`); this
//! file is only the end-to-end execution proof.

#![cfg(any(feature = "client", feature = "client-rustls"))]

use serde_json::Value;
use surql::connection::{ConnectionConfig, DatabaseClient};
use surql::schema::memory_bucket;
use surql::types::FileRef;

/// Connect to the in-process embedded engine (`mem://`).
async fn embedded_client(ns: &str) -> DatabaseClient {
    let cfg = ConnectionConfig::builder()
        .url("mem://")
        .namespace(ns)
        .database(ns)
        .build()
        .expect("valid mem config");
    let client = DatabaseClient::new(cfg).expect("client constructs");
    client.connect().await.expect("connect to embedded engine");
    client
}

/// Connect to a live server that has the files feature enabled (started with
/// the `SURREAL_CAPS_ALLOW_EXPERIMENTAL=files` environment variable).
async fn live_files_client() -> Option<DatabaseClient> {
    let url = std::env::var("SURREAL_FILES_URL").ok()?;
    let cfg = ConnectionConfig::builder()
        .url(url)
        .namespace("it_files")
        .database("it_files")
        .username(std::env::var("SURREAL_USER").unwrap_or_else(|_| "root".into()))
        .password(std::env::var("SURREAL_PASS").unwrap_or_else(|_| "root".into()))
        .timeout(10.0)
        .build()
        .expect("valid live config");
    let client = DatabaseClient::new(cfg).expect("client constructs");
    client.connect().await.expect("connect to files server");
    Some(client)
}

/// Define a bucket; return `false` if the engine rejects experimental files.
async fn try_define_bucket(client: &DatabaseClient, name: &str) -> bool {
    let bucket = memory_bucket(name);
    let sql = bucket.to_surql().expect("valid bucket ddl");
    match client.query(&sql).await {
        Ok(_) => true,
        Err(e) => {
            let msg = e.to_string().to_lowercase();
            // The engine signals a disabled experimental feature with an
            // "experimental" / "not allowed" / "unknown" style message.
            eprintln!("DEFINE BUCKET not supported by this engine: {e}");
            assert!(
                msg.contains("experimental")
                    || msg.contains("bucket")
                    || msg.contains("not allowed")
                    || msg.contains("unknown")
                    || msg.contains("parse"),
                "unexpected bucket-define error: {e}"
            );
            false
        }
    }
}

/// Run the full file round-trip against an already-connected client whose
/// engine is known to support buckets.
async fn run_roundtrip(client: &DatabaseClient, bucket_name: &str) {
    let bucket = client.bucket(bucket_name);

    // put + get_text
    bucket
        .put("greeting.txt", "hello world")
        .await
        .expect("put text");
    let text = bucket.get_text("greeting.txt").await.expect("get_text");
    assert_eq!(text, "hello world");

    // exists
    assert!(bucket.exists("greeting.txt").await.expect("exists true"));
    assert!(!bucket.exists("missing.txt").await.expect("exists false"));

    // binary put + get (the bytes-binding path)
    let blob = vec![0u8, 1, 2, 3, 255, 128];
    bucket
        .put("blob.bin", blob.clone())
        .await
        .expect("put bytes");
    let got = bucket.get("blob.bin").await.expect("get bytes");
    assert_eq!(got, blob, "binary payload must round-trip byte-for-byte");

    // put_if_not_exists is a no-op when present
    bucket
        .put_if_not_exists("greeting.txt", "OVERWRITE?")
        .await
        .expect("put_if_not_exists no-op");
    assert_eq!(
        bucket.get_text("greeting.txt").await.expect("unchanged"),
        "hello world"
    );

    // copy + copy_if_not_exists
    bucket
        .copy("greeting.txt", "greeting_copy.txt")
        .await
        .expect("copy");
    assert!(bucket
        .exists("greeting_copy.txt")
        .await
        .expect("copy exists"));

    // rename
    bucket
        .rename("greeting_copy.txt", "renamed.txt")
        .await
        .expect("rename");
    assert!(bucket.exists("renamed.txt").await.expect("renamed exists"));
    assert!(!bucket
        .exists("greeting_copy.txt")
        .await
        .expect("source gone"));

    // head metadata
    let head: Option<Value> = bucket.head("greeting.txt").await.expect("head");
    assert!(head.is_some(), "head returns metadata for an existing file");

    // The SDK decodes `file` values to the canonical `f"bucket:/key"` literal,
    // so head/list/record-field all expose the leading-slash key verbatim
    // (no `file::bucket`/`file::key` projection needed).
    assert_canonical_keys(client, bucket_name).await;

    // delete
    bucket.delete("greeting.txt").await.expect("delete");
    assert!(!bucket.exists("greeting.txt").await.expect("deleted gone"));
}

/// Verify that `file::list`, `.head()`, and a record `file` field all decode to
/// the canonical `f"bucket:/key"` literal (leading-slash key preserved). Run
/// after [`run_roundtrip`] has populated `blob.bin`.
async fn assert_canonical_keys(client: &DatabaseClient, bucket_name: &str) {
    let bucket = client.bucket(bucket_name);

    // list rows embed the decoded `file` literal.
    let files = bucket.list().await.expect("list");
    assert!(
        files.len() >= 3,
        "expected at least greeting/blob/renamed, got {}",
        files.len()
    );
    let mut keys: Vec<String> = Vec::new();
    for entry in &files {
        let file_field = entry
            .get("file")
            .and_then(Value::as_str)
            .unwrap_or_else(|| panic!("list entry missing string `file` field: {entry}"));
        assert!(
            FileRef::is_file_literal(file_field),
            "list `file` should be the f\"...\" literal, got {file_field:?}"
        );
        let parsed = FileRef::parse(file_field).expect("list file literal parses");
        assert_eq!(parsed.bucket(), bucket_name);
        // Canonical keys carry a leading slash and are preserved verbatim.
        assert!(
            parsed.key().starts_with('/'),
            "canonical key must keep its leading slash, got {:?}",
            parsed.key()
        );
        keys.push(parsed.key().to_string());
    }
    assert!(
        keys.iter().any(|k| k == "/blob.bin"),
        "list should surface the canonical key /blob.bin, got {keys:?}"
    );

    // head exposes the same decoded literal.
    let head_obj: Value = bucket
        .head("blob.bin")
        .await
        .expect("head")
        .expect("head returns metadata for an existing file");
    let head_file = head_obj
        .get("file")
        .and_then(Value::as_str)
        .expect("head `file` field is a string literal");
    let head_ref = FileRef::parse(head_file).expect("head file literal parses");
    assert_eq!(
        head_ref.key(),
        "/blob.bin",
        "head exposes the canonical key"
    );

    // A record carrying a `file` field decodes the same way (round-trip through
    // a normal SELECT). bucket/key are bound — never interpolated.
    let mut vars = std::collections::BTreeMap::new();
    vars.insert("b".to_string(), Value::String(bucket_name.to_string()));
    vars.insert("k".to_string(), Value::String("blob.bin".to_string()));
    client
        .query_with_vars("CREATE doc_with_file:1 SET f = type::file($b, $k);", vars)
        .await
        .expect("create record with file field");
    let selected = client
        .query("SELECT f FROM doc_with_file:1;")
        .await
        .expect("select record with file field");
    // selected is [[ { f: "f\"bucket:/blob.bin\"" } ]]
    let f_str = selected
        .get(0)
        .and_then(|stmt| stmt.get(0))
        .and_then(|row| row.get("f"))
        .and_then(Value::as_str)
        .unwrap_or_else(|| panic!("record `f` field is not a file literal: {selected}"));
    let rec_ref = FileRef::parse(f_str).expect("record file field parses");
    assert_eq!(rec_ref.bucket(), bucket_name);
    assert_eq!(
        rec_ref.key(),
        "/blob.bin",
        "record file field exposes the canonical key"
    );
}

#[tokio::test]
async fn bucket_roundtrip_embedded_if_supported() {
    let client = embedded_client("it_files_mem").await;
    if !try_define_bucket(&client, "embedded_bucket").await {
        eprintln!("skipping: embedded engine has experimental files disabled");
        return;
    }
    run_roundtrip(&client, "embedded_bucket").await;
}

#[tokio::test]
#[ignore = "requires a server with SURREAL_CAPS_ALLOW_EXPERIMENTAL=files; set SURREAL_FILES_URL"]
async fn bucket_roundtrip_live_server() {
    let Some(client) = live_files_client().await else {
        eprintln!("skipping: SURREAL_FILES_URL not set");
        return;
    };
    assert!(
        try_define_bucket(&client, "live_bucket").await,
        "a server with SURREAL_CAPS_ALLOW_EXPERIMENTAL=files must accept DEFINE BUCKET"
    );
    run_roundtrip(&client, "live_bucket").await;
}
