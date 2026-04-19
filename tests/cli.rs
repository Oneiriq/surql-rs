//! End-to-end CLI tests.
//!
//! These exercise the `surql` binary by invoking it through
//! [`assert_cmd`]. Tests that require a live SurrealDB instance are
//! gated behind `SURQL_TEST_DB_URL`: when that env var is absent, the
//! live-connection subcommands are skipped.

#![cfg(feature = "cli")]

use assert_cmd::Command;
use predicates::prelude::*;

fn bin() -> Command {
    Command::cargo_bin("surql").expect("surql binary")
}

#[test]
fn version_flag_prints_crate_version() {
    bin()
        .arg("--version")
        .assert()
        .success()
        .stdout(predicate::str::contains(env!("CARGO_PKG_VERSION")));
}

#[test]
fn version_subcommand_prints_banner() {
    bin()
        .arg("version")
        .assert()
        .success()
        .stdout(predicate::str::contains("surql"));
}

#[test]
fn help_lists_all_command_groups() {
    let output = bin().arg("--help").assert().success().to_string();
    let body = output;
    for keyword in ["db", "migrate", "schema", "orchestrate"] {
        assert!(
            body.contains(keyword),
            "expected `{keyword}` in help output"
        );
    }
}

#[test]
fn unknown_command_exits_with_usage_error() {
    bin()
        .arg("bogus-command")
        .assert()
        .failure()
        .code(predicate::eq(2));
}

#[test]
fn db_ping_against_live_server() {
    let Ok(url) = std::env::var("SURQL_TEST_DB_URL") else {
        eprintln!("SURQL_TEST_DB_URL not set; skipping db ping live test");
        return;
    };
    bin()
        .env("SURQL_URL", url)
        .env(
            "SURQL_NAMESPACE",
            std::env::var("SURQL_TEST_NS").unwrap_or_else(|_| "test".into()),
        )
        .env(
            "SURQL_DATABASE",
            std::env::var("SURQL_TEST_DB").unwrap_or_else(|_| "test".into()),
        )
        .env(
            "SURQL_USERNAME",
            std::env::var("SURQL_TEST_USER").unwrap_or_else(|_| "root".into()),
        )
        .env(
            "SURQL_PASSWORD",
            std::env::var("SURQL_TEST_PASS").unwrap_or_else(|_| "root".into()),
        )
        .args(["db", "ping"])
        .assert()
        .success()
        .stdout(predicate::str::contains("pong"));
}

#[test]
fn migrate_status_with_empty_dir() {
    let tmp = tempfile::tempdir().expect("tempdir");
    // Write a minimal Cargo.toml so settings loader picks migration_path.
    let cargo = tmp.path().join("Cargo.toml");
    std::fs::write(
        &cargo,
        format!(
            r#"[package]
name = "cli-test"
version = "0.0.0"

[package.metadata.surql]
migration_path = "{}"
"#,
            tmp.path().join("migrations").display()
        ),
    )
    .unwrap();
    std::fs::create_dir_all(tmp.path().join("migrations")).unwrap();

    let Ok(url) = std::env::var("SURQL_TEST_DB_URL") else {
        eprintln!("SURQL_TEST_DB_URL not set; skipping migrate status live test");
        return;
    };

    bin()
        .current_dir(tmp.path())
        .env("SURQL_URL", url)
        .env(
            "SURQL_NAMESPACE",
            std::env::var("SURQL_TEST_NS").unwrap_or_else(|_| "test".into()),
        )
        .env(
            "SURQL_DATABASE",
            std::env::var("SURQL_TEST_DB").unwrap_or_else(|_| "test".into()),
        )
        .env(
            "SURQL_USERNAME",
            std::env::var("SURQL_TEST_USER").unwrap_or_else(|_| "root".into()),
        )
        .env(
            "SURQL_PASSWORD",
            std::env::var("SURQL_TEST_PASS").unwrap_or_else(|_| "root".into()),
        )
        .args(["migrate", "status"])
        .assert()
        .success()
        .stdout(predicate::str::contains("total: 0"));
}

#[test]
fn schema_tables_against_live_server() {
    let Ok(url) = std::env::var("SURQL_TEST_DB_URL") else {
        eprintln!("SURQL_TEST_DB_URL not set; skipping schema tables live test");
        return;
    };
    bin()
        .env("SURQL_URL", url)
        .env(
            "SURQL_NAMESPACE",
            std::env::var("SURQL_TEST_NS").unwrap_or_else(|_| "test".into()),
        )
        .env(
            "SURQL_DATABASE",
            std::env::var("SURQL_TEST_DB").unwrap_or_else(|_| "test".into()),
        )
        .env(
            "SURQL_USERNAME",
            std::env::var("SURQL_TEST_USER").unwrap_or_else(|_| "root".into()),
        )
        .env(
            "SURQL_PASSWORD",
            std::env::var("SURQL_TEST_PASS").unwrap_or_else(|_| "root".into()),
        )
        .args(["schema", "tables"])
        .assert()
        .success();
}

#[test]
fn migrate_create_writes_blank_file() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let migrations = tmp.path().join("migrations");
    std::fs::create_dir_all(&migrations).unwrap();

    bin()
        .current_dir(tmp.path())
        .args(["migrate", "create", "add initial table", "--schema-dir"])
        .arg(migrations.to_string_lossy().to_string())
        .assert()
        .success();

    let entries: Vec<_> = std::fs::read_dir(&migrations)
        .unwrap()
        .filter_map(std::result::Result::ok)
        .collect();
    assert_eq!(entries.len(), 1, "expected one migration file");
    let filename = entries[0].file_name();
    assert!(filename.to_string_lossy().ends_with(".surql"));
}

#[test]
fn schema_hook_config_outputs_yaml_snippet() {
    bin()
        .args(["schema", "hook-config"])
        .assert()
        .success()
        .stdout(predicate::str::contains("surql"));
}

#[test]
fn db_query_requires_input() {
    // Must fail with validation error rather than hang.
    bin().args(["db", "query"]).assert().failure();
}
