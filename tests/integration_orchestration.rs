//! Integration tests for the `orchestration` module.
//!
//! Runs a real sequential deployment against a live SurrealDB instance
//! (defaults to the `v3.0.5` container the umbrella issue pins for CI).
//! The test is gated on the `SURREAL_URL` environment variable so the
//! rest of `cargo test` stays green on machines without a server.
//!
//! To exercise locally:
//!
//! ```text
//! docker run -d -p 8000:8000 surrealdb/surrealdb:v3.0.5 start --user root --pass root memory
//! SURREAL_URL=ws://localhost:8000 SURREAL_USER=root SURREAL_PASS=root \
//!   cargo test --all-features --test integration_orchestration -- --test-threads=1
//! ```

#![cfg(all(
    feature = "orchestration",
    any(feature = "client", feature = "client-rustls")
))]

use std::env;
use std::sync::atomic::{AtomicU64, Ordering};

use surql::connection::{ConnectionConfig, DatabaseClient};
use surql::migration::{
    discover_migrations, ensure_migration_table, get_applied_migrations, MIGRATION_TABLE_NAME,
};
use surql::orchestration::{
    check_environment_health, deploy_to_environments, verify_connectivity, DeploymentPlan,
    DeploymentStatus, EnvironmentConfig, EnvironmentRegistry, MigrationCoordinator, StrategyKind,
};

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

fn unique_db(prefix: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos());
    let seq = DB_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{prefix}_{nanos}_{seq}")
}

fn integration_config(database: &str) -> Option<ConnectionConfig> {
    let url = env_url()?;
    Some(
        ConnectionConfig::builder()
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
            .expect("valid integration config"),
    )
}

fn write_migration(dir: &std::path::Path, filename: &str, body: &str) {
    std::fs::write(dir.join(filename), body).expect("write migration");
}

fn seed_migrations(dir: &std::path::Path, table: &str) {
    let body = format!(
        "-- @metadata\n\
         -- version: 20260101_000001\n\
         -- description: orchestration smoke {table}\n\
         -- @up\n\
         DEFINE TABLE {table} SCHEMAFULL;\n\
         DEFINE FIELD name ON {table} TYPE string;\n\
         -- @down\n\
         REMOVE TABLE {table};\n"
    );
    write_migration(dir, "20260101_000001_orchestration_smoke.surql", &body);
}

async fn connected_client(cfg: ConnectionConfig) -> DatabaseClient {
    let client = DatabaseClient::new(cfg).expect("client");
    client.connect().await.expect("connect");
    client
}

#[tokio::test]
async fn verify_connectivity_roundtrip() {
    let database = unique_db("it_orch_conn");
    let Some(cfg) = integration_config(&database) else {
        eprintln!("SURREAL_URL not set; skipping orchestration integration");
        return;
    };
    let env = EnvironmentConfig::builder(&database, cfg)
        .build()
        .expect("valid env");
    let ok = verify_connectivity(&env).await.expect("connectivity");
    assert!(ok, "expected live connectivity to succeed");
}

#[tokio::test]
async fn health_check_reports_migration_table_presence() {
    let database = unique_db("it_orch_health");
    let Some(cfg) = integration_config(&database) else {
        eprintln!("SURREAL_URL not set; skipping");
        return;
    };

    // Before ensuring the migration table, health should report "missing".
    let env = EnvironmentConfig::builder(&database, cfg.clone())
        .build()
        .expect("env");
    let before = check_environment_health(&env).await.unwrap();
    assert!(before.is_healthy, "db should be reachable");
    assert!(
        !before.migration_table_exists,
        "migration table should not exist yet"
    );

    // Create table, re-check.
    let client = connected_client(cfg).await;
    ensure_migration_table(&client).await.unwrap();
    let _ = client.disconnect().await;

    let after = check_environment_health(&env).await.unwrap();
    assert!(after.is_healthy);
    assert!(
        after.migration_table_exists,
        "migration table should now exist"
    );
}

#[tokio::test]
async fn sequential_deploy_applies_migration_against_live_surrealdb() {
    let database = unique_db("it_orch_seq");
    let Some(cfg) = integration_config(&database) else {
        eprintln!("SURREAL_URL not set; skipping");
        return;
    };

    let tmp = tempfile::tempdir().expect("tmpdir");
    seed_migrations(tmp.path(), "orch_smoke");

    let migrations = discover_migrations(tmp.path()).expect("discover");
    assert_eq!(migrations.len(), 1);

    let registry = EnvironmentRegistry::new();
    let env = EnvironmentConfig::builder(&database, cfg.clone())
        .build()
        .expect("env");
    registry.register(env).await;

    // Pre-create migration history table so the coordinator skips the
    // deploy-time SCHEMA auto-create dance the executor already handles.
    let client = connected_client(cfg.clone()).await;
    ensure_migration_table(&client).await.unwrap();
    let _ = client.disconnect().await;

    let results = deploy_to_environments(
        registry.clone(),
        vec![database.clone()],
        migrations,
        StrategyKind::Sequential,
        1,
        10.0,
        1,
        true,
        false,
        false,
    )
    .await
    .expect("sequential deploy succeeds");

    assert_eq!(results.len(), 1);
    let result = results.get(&database).expect("result present");
    assert_eq!(result.status, DeploymentStatus::Success);
    assert_eq!(result.migrations_applied, 1);

    // Verify migration actually applied via history table.
    let client = connected_client(cfg).await;
    let applied = get_applied_migrations(&client).await.expect("history");
    assert!(
        applied.iter().any(|h| h.version == "20260101_000001"),
        "expected applied version, got: {:?}",
        applied
    );
    let _ = client.disconnect().await;

    // Ensure the history table name constant is still used (prevents accidental rename).
    assert_eq!(MIGRATION_TABLE_NAME, "_migration_history");
}

#[tokio::test]
async fn coordinator_errors_on_missing_environment() {
    let registry = EnvironmentRegistry::new();
    let coordinator = MigrationCoordinator::with_strategy_label(
        registry.clone(),
        StrategyKind::Sequential,
        1,
        10.0,
        1,
    )
    .unwrap();
    let plan = DeploymentPlan::builder(registry)
        .environment("ghost")
        .verify_health(false)
        .dry_run(true)
        .build();
    let err = coordinator.deploy(&plan).await.unwrap_err();
    assert!(err.to_string().contains("Environment not found"));
}
