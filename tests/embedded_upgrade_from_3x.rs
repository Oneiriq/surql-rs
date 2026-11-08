//! An embedded SurrealDB 4 engine and a datastore a 3.x release wrote.
//!
//! `tests/fixtures/surrealkv-3.3.5` is a `SurrealKV` directory written by
//! SurrealDB 3.3.5 (namespace and database `fixture`, records `item:1` and
//! `item:2` with `n = 1` and `n = 2`). SurrealDB 4.0 refuses to open it
//! unless the connection opts in to the experimental in-place upgrade, so a
//! default connect must fail with `SurqlError::DatastoreUpgradeRefused` and
//! leave the files as they were, and an opted-in connect must upgrade it
//! and read the records back. Every test works on its own copy.
//!
//! Runs under plain `cargo test`: the `SurrealKV` engine comes from the
//! `kv-surrealkv` dev-dependency feature, no server needed.

#![cfg(any(feature = "client", feature = "client-rustls"))]

use std::fs;
use std::path::{Path, PathBuf};

use serde_json::Value;
use surql::connection::{ConnectionConfig, DatabaseClient};
use surql::SurqlError;

const FIXTURE: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/surrealkv-3.3.5"
);

/// The wait before a connect retry, long enough that a refusal answered
/// faster proves it was not retried.
const RETRY_WAIT_SECS: f64 = 3.0;

/// A fresh copy of the 3.x datastore in a temporary directory.
fn copy_of_fixture() -> (tempfile::TempDir, PathBuf) {
    let tmp = tempfile::tempdir().expect("tempdir");
    let store = tmp.path().join("store");
    copy_dir(Path::new(FIXTURE), &store);
    (tmp, store)
}

fn copy_dir(from: &Path, to: &Path) {
    fs::create_dir_all(to).expect("create the copy");
    for entry in fs::read_dir(from).expect("read the fixture") {
        let entry = entry.expect("a fixture entry");
        let target = to.join(entry.file_name());
        if entry.file_type().expect("an entry type").is_dir() {
            copy_dir(&entry.path(), &target);
        } else {
            fs::copy(entry.path(), &target).expect("copy a fixture file");
        }
    }
}

/// Every file under `dir` with its bytes, by path relative to `dir`.
fn snapshot(dir: &Path) -> Vec<(PathBuf, Vec<u8>)> {
    let mut out = Vec::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(next) = stack.pop() {
        for entry in fs::read_dir(&next).expect("read a directory") {
            let entry = entry.expect("an entry");
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if entry.file_name() != "LOCK" {
                let relative = path.strip_prefix(dir).expect("under dir").to_path_buf();
                out.push((relative, fs::read(&path).expect("read a file")));
            }
        }
    }
    out.sort();
    out
}

fn config(store: &Path, upgrade: bool) -> ConnectionConfig {
    ConnectionConfig::builder()
        .url(format!("surrealkv://{}", store.display()))
        .namespace("fixture")
        .database("fixture")
        .retry_max_attempts(3)
        .retry_min_wait(RETRY_WAIT_SECS)
        .retry_max_wait(RETRY_WAIT_SECS * 2.0)
        .experimental_upgrade_from_3x(upgrade)
        .build()
        .expect("valid config")
}

#[tokio::test]
async fn a_default_connect_refuses_a_3x_datastore_and_leaves_it_as_it_was() {
    let (_tmp, store) = copy_of_fixture();
    let before = snapshot(&store);

    let client = DatabaseClient::new(config(&store, false)).expect("client");
    let started = std::time::Instant::now();
    let err = client
        .connect()
        .await
        .expect_err("a 3.x datastore is refused");
    assert!(
        matches!(&err, SurqlError::DatastoreUpgradeRefused { reason }
            if reason.contains("SurrealDB 3")),
        "{err:?}"
    );
    assert!(err.is_datastore_upgrade_refused());
    assert!(
        err.to_string()
            .contains("experimental_upgrade_from_3x(true)"),
        "the message names the surql opt-in: {err}"
    );
    // A retry would first wait out the backoff; a refusal is final.
    assert!(
        started.elapsed() < std::time::Duration::from_secs_f64(RETRY_WAIT_SECS),
        "a refusal is not retried ({:?})",
        started.elapsed()
    );
    assert!(!client.is_connected());
    drop(client);

    assert_eq!(
        snapshot(&store),
        before,
        "the refusal changed the datastore"
    );
}

#[tokio::test]
async fn an_opted_in_connect_upgrades_a_3x_datastore() {
    let (_tmp, store) = copy_of_fixture();

    let client = DatabaseClient::new(config(&store, true)).expect("client");
    client
        .connect()
        .await
        .expect("the opt-in upgrades the datastore");
    let rows: Vec<Value> = client.select("item").await.expect("select");
    let ns: Vec<i64> = rows.iter().filter_map(|r| r["n"].as_i64()).collect();
    assert_eq!(ns, [1, 2], "{rows:?}");
}
