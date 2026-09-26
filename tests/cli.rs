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
    for keyword in ["db", "migrate", "schema", "bucket", "orchestrate"] {
        assert!(
            body.contains(keyword),
            "expected `{keyword}` in help output"
        );
    }
}

#[test]
fn bucket_help_lists_file_subcommands() {
    let output = bin()
        .args(["bucket", "--help"])
        .assert()
        .success()
        .to_string();
    for keyword in [
        "define", "list", "rm", "put", "get", "delete", "exists", "files",
    ] {
        assert!(
            output.contains(keyword),
            "expected `{keyword}` in `bucket --help` output"
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
fn non_utf8_argument_is_a_usage_error_not_a_panic() {
    #[cfg(windows)]
    let arg = {
        use std::os::windows::ffi::OsStringExt;
        std::ffi::OsString::from_wide(&[0xD800])
    };
    #[cfg(unix)]
    let arg = {
        use std::os::unix::ffi::OsStringExt;
        std::ffi::OsString::from_vec(vec![0xff])
    };
    bin()
        .args(["migrate", "create"])
        .arg(arg)
        .assert()
        .failure()
        .code(predicate::eq(2));
}

#[test]
fn config_flag_rejects_a_file_it_would_not_read() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let prod = tmp.path().join("prod.toml");
    std::fs::write(&prod, "[package.metadata.surql]\nmigration_path = \"x\"\n").unwrap();
    bin()
        .arg("--config")
        .arg(&prod)
        .args(["migrate", "status"])
        .assert()
        .failure()
        .code(predicate::eq(1))
        .stderr(predicate::str::contains("Cargo.toml"));
    bin()
        .arg("--config")
        .arg(tmp.path().join("missing").join("Cargo.toml"))
        .args(["migrate", "status"])
        .assert()
        .failure()
        .code(predicate::eq(1))
        .stderr(predicate::str::contains("does not exist"));
}

#[test]
fn destructive_commands_require_yes_without_a_terminal() {
    // stdin is not a terminal here, so nobody can answer the prompt; the
    // command must refuse before it connects anywhere.
    for args in [
        vec!["bucket", "rm", "avatars"],
        vec!["bucket", "delete", "avatars", "alice.png"],
        vec!["db", "reset"],
    ] {
        bin()
            .env("SURQL_URL", "ws://127.0.0.1:9")
            .args(&args)
            .assert()
            .failure()
            .code(predicate::eq(1))
            .stderr(predicate::str::contains("pass --yes"));
    }
}

/// A project directory with an empty migrations folder and an
/// environments file holding one environment at `url`.
fn orchestrate_project(url: &str, database: &str, require_approval: bool) -> tempfile::TempDir {
    let tmp = tempfile::tempdir().expect("tempdir");
    std::fs::create_dir_all(tmp.path().join("migrations")).unwrap();
    let env = serde_json::json!({
        "environments": [{
            "name": "prod",
            "require_approval": require_approval,
            "connection": {
                "db_url": url,
                "db_ns": "cli_test",
                "db": database,
                "db_user": std::env::var("SURQL_TEST_USER").unwrap_or_else(|_| "root".into()),
                "db_pass": std::env::var("SURQL_TEST_PASS").unwrap_or_else(|_| "root".into()),
            }
        }]
    });
    std::fs::write(tmp.path().join("environments.json"), env.to_string()).unwrap();
    tmp
}

#[test]
fn orchestrate_deploy_requires_yes_without_a_terminal() {
    let project = orchestrate_project("ws://127.0.0.1:9", "unused", false);
    bin()
        .current_dir(project.path())
        .args(["orchestrate", "deploy"])
        .assert()
        .failure()
        .code(predicate::eq(1))
        .stderr(predicate::str::contains("pass --yes"));
}

#[test]
fn orchestrate_deploy_needs_approve_for_guarded_environments() {
    let Ok(url) = std::env::var("SURQL_TEST_DB_URL") else {
        eprintln!("SURQL_TEST_DB_URL not set; skipping");
        return;
    };
    let project = orchestrate_project(&url, &unique_db("cli_orch"), true);
    std::fs::write(
        project
            .path()
            .join("migrations")
            .join("20260101_000001_one.surql"),
        "-- @metadata\n-- version: 20260101_000001\n-- description: one\n\
         -- @up\nDEFINE TABLE orchestrated;\n-- @down\nREMOVE TABLE orchestrated;\n",
    )
    .unwrap();
    bin()
        .current_dir(project.path())
        .args(["orchestrate", "deploy", "--yes"])
        .assert()
        .failure()
        .code(predicate::eq(1))
        .stderr(predicate::str::contains("require approval"));
    bin()
        .current_dir(project.path())
        .args(["orchestrate", "deploy", "--yes", "--approve"])
        .assert()
        .success()
        .stdout(predicate::str::contains("20260101_000001"));
    // A second run has nothing left to apply and still succeeds.
    bin()
        .current_dir(project.path())
        .args(["orchestrate", "deploy", "--yes", "--approve"])
        .assert()
        .success();
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

/// A unique database name so live tests never share state.
fn unique_db(prefix: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos());
    format!("{prefix}_{nanos}")
}

/// `surql` pointed at the live test server, namespace `cli_test`, and `db`.
fn live(url: &str, db: &str) -> Command {
    let mut cmd = bin();
    cmd.env("SURQL_URL", url)
        .env("SURQL_NAMESPACE", "cli_test")
        .env("SURQL_DATABASE", db)
        .env(
            "SURQL_USERNAME",
            std::env::var("SURQL_TEST_USER").unwrap_or_else(|_| "root".into()),
        )
        .env(
            "SURQL_PASSWORD",
            std::env::var("SURQL_TEST_PASS").unwrap_or_else(|_| "root".into()),
        );
    cmd
}

/// A project directory whose `Cargo.toml` points `migration_path` at
/// `<dir>/migrations`, holding one migration with the given bodies.
fn project_with_migration(up: &str, down: &str) -> tempfile::TempDir {
    let tmp = tempfile::tempdir().expect("tempdir");
    let migrations = tmp.path().join("migrations");
    std::fs::create_dir_all(&migrations).unwrap();
    std::fs::write(
        tmp.path().join("Cargo.toml"),
        format!(
            "[package]\nname = \"cli-test\"\nversion = \"0.0.0\"\n\n\
             [package.metadata.surql]\nmigration_path = {:?}\n",
            migrations.display().to_string()
        ),
    )
    .unwrap();
    std::fs::write(
        migrations.join("20260101_000001_one.surql"),
        format!(
            "-- @metadata\n-- version: 20260101_000001\n-- description: one\n\
             -- @up\n{up}\n-- @down\n{down}\n"
        ),
    )
    .unwrap();
    tmp
}

fn query_stdout(url: &str, db: &str, surql: &str) -> String {
    let out = live(url, db)
        .args(["db", "query", surql])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    String::from_utf8_lossy(&out).into_owned()
}

#[test]
fn db_reset_quotes_table_names() {
    let Ok(url) = std::env::var("SURQL_TEST_DB_URL") else {
        eprintln!("SURQL_TEST_DB_URL not set; skipping");
        return;
    };
    let db = unique_db("cli_reset");
    let victim = unique_db("cli_victim");
    query_stdout(
        &url,
        &db,
        &format!(
            "DEFINE NAMESPACE {victim}; DEFINE TABLE `x; REMOVE NAMESPACE {victim}`; \
             DEFINE TABLE `foo-bar`; DEFINE TABLE plain;"
        ),
    );

    live(&url, &db)
        .args(["db", "reset", "--yes"])
        .assert()
        .success()
        .stdout(predicate::str::contains("removed 3 table(s)"));

    assert!(
        query_stdout(&url, &db, "INFO FOR ROOT;").contains(&victim),
        "a table name must not run as a statement"
    );
    let tables = query_stdout(&url, &db, "INFO FOR DB;");
    for name in ["foo-bar", "plain", "REMOVE NAMESPACE"] {
        assert!(
            !tables.contains(name),
            "{name} survived the reset: {tables}"
        );
    }
}

#[test]
fn migrate_up_exits_non_zero_when_a_migration_fails() {
    let Ok(url) = std::env::var("SURQL_TEST_DB_URL") else {
        eprintln!("SURQL_TEST_DB_URL not set; skipping");
        return;
    };
    let project = project_with_migration("THROW 'boom';", "SELECT 1;");
    live(&url, &unique_db("cli_up_fail"))
        .current_dir(project.path())
        .args(["migrate", "up"])
        .assert()
        .failure()
        .code(predicate::eq(1))
        .stderr(predicate::str::contains("boom"));
}

#[test]
fn migrate_down_exits_non_zero_when_a_rollback_fails() {
    let Ok(url) = std::env::var("SURQL_TEST_DB_URL") else {
        eprintln!("SURQL_TEST_DB_URL not set; skipping");
        return;
    };
    let project = project_with_migration("DEFINE TABLE down_fails;", "THROW 'nope';");
    let db = unique_db("cli_down_fail");
    live(&url, &db)
        .current_dir(project.path())
        .args(["migrate", "up"])
        .assert()
        .success();
    live(&url, &db)
        .current_dir(project.path())
        .args(["migrate", "down"])
        .assert()
        .failure()
        .code(predicate::eq(1))
        .stderr(predicate::str::contains("nope"));
}
