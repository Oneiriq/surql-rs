//! Integration tests for the `settings` module.
//!
//! Uses a temporary working directory so layered loads (Cargo.toml,
//! .env) can be exercised without touching the repository's real
//! files.

#![cfg(feature = "settings")]

use std::fs;

use surql::settings::{Environment, LogLevel, Settings};
use tempfile::TempDir;

fn write_cargo(dir: &std::path::Path, body: &str) {
    fs::write(dir.join("Cargo.toml"), body).unwrap();
}

#[test]
fn loads_from_cargo_metadata_only() {
    let tmp = TempDir::new().unwrap();
    write_cargo(
        tmp.path(),
        r#"
[package]
name = "demo"
version = "0.0.1"

[package.metadata.surql]
environment = "staging"
debug = false
log_level = "WARNING"
app_name = "demo-app"
version = "9.9.9"
migration_path = "db/migrations"
"#,
    );
    let settings = Settings::builder()
        .cwd(tmp.path())
        .skip_dotenv(true)
        .load()
        .unwrap();
    assert_eq!(settings.environment, Environment::Staging);
    assert!(!settings.debug);
    assert_eq!(settings.log_level, LogLevel::Warning);
    assert_eq!(settings.app_name, "demo-app");
    assert_eq!(settings.version, "9.9.9");
    assert_eq!(settings.migration_path.to_str(), Some("db/migrations"));
}

#[test]
fn dotenv_overrides_cargo_metadata() {
    let tmp = TempDir::new().unwrap();
    write_cargo(
        tmp.path(),
        r#"
[package]
name = "demo"
version = "0.0.1"

[package.metadata.surql]
app_name = "cargo-name"
log_level = "WARNING"
"#,
    );
    fs::write(
        tmp.path().join(".env"),
        "SURQL_APP_NAME=dotenv-name\nSURQL_LOG_LEVEL=DEBUG\n",
    )
    .unwrap();
    let settings = Settings::builder().cwd(tmp.path()).load().unwrap();
    assert_eq!(settings.app_name, "dotenv-name");
    assert_eq!(settings.log_level, LogLevel::Debug);
}

#[test]
fn explicit_builder_wins_over_all_sources() {
    let tmp = TempDir::new().unwrap();
    write_cargo(
        tmp.path(),
        r#"
[package]
name = "demo"
version = "0.0.1"

[package.metadata.surql]
environment = "production"
app_name = "cargo-name"
"#,
    );
    fs::write(tmp.path().join(".env"), "SURQL_APP_NAME=dotenv-name\n").unwrap();
    let settings = Settings::builder()
        .cwd(tmp.path())
        .environment(Environment::Development)
        .app_name("explicit")
        .load()
        .unwrap();
    assert_eq!(settings.environment, Environment::Development);
    assert_eq!(settings.app_name, "explicit");
}

#[test]
fn nested_database_override_passes_validation() {
    let tmp = TempDir::new().unwrap();
    write_cargo(
        tmp.path(),
        r#"
[package]
name = "demo"
version = "0.0.1"

[package.metadata.surql.database]
url = "wss://db.example.com/rpc"
namespace = "prod"
database = "core"
timeout = 60
retry_min_wait = 1.0
retry_max_wait = 5.0
"#,
    );
    let settings = Settings::builder()
        .cwd(tmp.path())
        .skip_dotenv(true)
        .load()
        .unwrap();
    assert_eq!(settings.database.url(), "wss://db.example.com/rpc");
    assert_eq!(settings.database.namespace(), "prod");
    assert_eq!(settings.database.database(), "core");
    assert!((settings.database.timeout() - 60.0).abs() < f64::EPSILON);
}

#[test]
fn invalid_environment_is_rejected() {
    let tmp = TempDir::new().unwrap();
    write_cargo(
        tmp.path(),
        r#"
[package]
name = "demo"
version = "0.0.1"

[package.metadata.surql]
environment = "broken"
"#,
    );
    let err = Settings::builder()
        .cwd(tmp.path())
        .skip_dotenv(true)
        .load()
        .unwrap_err();
    assert!(err.to_string().contains("invalid environment"));
}
