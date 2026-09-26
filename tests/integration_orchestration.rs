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

    let plan = DeploymentPlan::builder(registry.clone())
        .environment(database.clone())
        .migrations(migrations)
        .strategy(StrategyKind::Sequential)
        .max_concurrent(1)
        .auto_rollback(false)
        .build();
    let results = deploy_to_environments(&plan)
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
        "expected applied version, got: {applied:?}"
    );
    let _ = client.disconnect().await;

    // Ensure the history table name constant is still used (prevents accidental rename).
    assert_eq!(MIGRATION_TABLE_NAME, "_migration_history");
}

fn migration_file(version: &str, up: &str, down: &str) -> String {
    format!(
        "-- @metadata\n-- version: {version}\n-- description: m{version}\n-- @up\n{up}\n-- @down\n{down}\n"
    )
}

async fn query(cfg: &ConnectionConfig, surql: &str) -> serde_json::Value {
    let client = connected_client(cfg.clone()).await;
    let out = client.query(surql).await.expect("query");
    let _ = client.disconnect().await;
    out
}

async fn applied_versions(cfg: &ConnectionConfig) -> Vec<String> {
    let client = connected_client(cfg.clone()).await;
    let rows = get_applied_migrations(&client).await.expect("history");
    let _ = client.disconnect().await;
    rows.into_iter().map(|h| h.version).collect()
}

async fn table_names(cfg: &ConnectionConfig) -> Vec<String> {
    let info = query(cfg, "INFO FOR DB;").await;
    let tables = info
        .as_array()
        .and_then(|stmts| stmts.first())
        .and_then(|stmt| stmt.get("tables"))
        .and_then(serde_json::Value::as_object)
        .map(|t| t.keys().cloned().collect())
        .unwrap_or_default();
    tables
}

async fn register(registry: &EnvironmentRegistry, name: &str, cfg: &ConnectionConfig) {
    let env = EnvironmentConfig::builder(name, cfg.clone())
        .build()
        .expect("env");
    registry.register(env).await;
}

fn coordinator(registry: &EnvironmentRegistry) -> MigrationCoordinator {
    MigrationCoordinator::with_strategy_label(
        registry.clone(),
        StrategyKind::Sequential,
        1,
        10.0,
        1,
    )
    .expect("coordinator")
}

#[tokio::test]
async fn failed_migration_marks_environment_failed_and_stops() {
    let database = unique_db("it_orch_fail");
    let Some(cfg) = integration_config(&database) else {
        eprintln!("SURREAL_URL not set; skipping");
        return;
    };
    let tmp = tempfile::tempdir().expect("tmpdir");
    write_migration(
        tmp.path(),
        "20260101_000001_boom.surql",
        &migration_file("20260101_000001", "THROW 'boom';", "SELECT 1;"),
    );
    write_migration(
        tmp.path(),
        "20260101_000002_after.surql",
        &migration_file(
            "20260101_000002",
            "DEFINE TABLE after_boom;",
            "REMOVE TABLE after_boom;",
        ),
    );
    let migrations = discover_migrations(tmp.path()).expect("discover");

    let registry = EnvironmentRegistry::new();
    register(&registry, &database, &cfg).await;
    let plan = DeploymentPlan::builder(registry.clone())
        .environment(database.clone())
        .migrations(migrations)
        .verify_health(false)
        .auto_rollback(false)
        .build();
    let results = coordinator(&registry).deploy(&plan).await.expect("deploy");
    let result = results.get(&database).expect("result");

    assert_eq!(result.status, DeploymentStatus::Failed, "{result:?}");
    assert_eq!(result.migrations_applied, 0);
    assert!(result.error.as_deref().unwrap_or("").contains("boom"));
    assert!(applied_versions(&cfg).await.is_empty());
    assert!(!table_names(&cfg).await.contains(&"after_boom".to_string()));
}

#[tokio::test]
async fn concurrent_strategies_deploy_each_environment_once() {
    let tmp = tempfile::tempdir().expect("tmpdir");
    seed_migrations(tmp.path(), "fanned_out");
    let migrations = discover_migrations(tmp.path()).expect("discover");
    for kind in [
        StrategyKind::Parallel,
        StrategyKind::Rolling,
        StrategyKind::Canary,
    ] {
        let db_a = unique_db("it_orch_fan_a");
        let db_b = unique_db("it_orch_fan_b");
        let (Some(cfg_a), Some(cfg_b)) = (integration_config(&db_a), integration_config(&db_b))
        else {
            eprintln!("SURREAL_URL not set; skipping");
            return;
        };
        let registry = EnvironmentRegistry::new();
        register(&registry, "a", &cfg_a).await;
        register(&registry, "b", &cfg_b).await;
        let plan = DeploymentPlan::builder(registry.clone())
            .environments(["a", "b", "a", "b"])
            .migrations(migrations.clone())
            .strategy(kind)
            .batch_size(1)
            .canary_percentage(50.0)
            .max_concurrent(4)
            .verify_health(false)
            .build();
        let results = deploy_to_environments(&plan).await.expect("deploy");
        assert_eq!(results.len(), 2, "{kind:?}: {results:?}");
        for (name, cfg) in [("a", &cfg_a), ("b", &cfg_b)] {
            assert_eq!(results[name].status, DeploymentStatus::Success, "{kind:?}");
            assert_eq!(results[name].applied_versions, vec!["20260101_000001"]);
            assert_eq!(applied_versions(cfg).await, vec!["20260101_000001"]);
        }
    }
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
#[tokio::test]
async fn deploy_applies_only_pending_and_rolls_back_only_this_run() {
    let db_a = unique_db("it_orch_a");
    let db_b = unique_db("it_orch_b");
    let (Some(cfg_a), Some(cfg_b)) = (integration_config(&db_a), integration_config(&db_b)) else {
        eprintln!("SURREAL_URL not set; skipping");
        return;
    };
    let tmp = tempfile::tempdir().expect("tmpdir");
    write_migration(
        tmp.path(),
        "20260101_000001_one.surql",
        &migration_file(
            "20260101_000001",
            "DEFINE TABLE keep_me;",
            "REMOVE TABLE keep_me;",
        ),
    );
    let v1_only = discover_migrations(tmp.path()).expect("discover");
    write_migration(
        tmp.path(),
        "20260101_000002_two.surql",
        &migration_file(
            "20260101_000002",
            "DEFINE TABLE clash;",
            "REMOVE TABLE clash;",
        ),
    );
    let both = discover_migrations(tmp.path()).expect("discover");

    let registry = EnvironmentRegistry::new();
    register(&registry, "env_a", &cfg_a).await;
    register(&registry, "env_b", &cfg_b).await;

    // Environment A is already at v1 and holds data in the v1 table.
    let seed = DeploymentPlan::builder(registry.clone())
        .environment("env_a")
        .migrations(v1_only)
        .verify_health(false)
        .build();
    let seeded = coordinator(&registry).deploy(&seed).await.expect("seed");
    assert_eq!(seeded["env_a"].status, DeploymentStatus::Success);
    query(&cfg_a, "CREATE keep_me:1 SET n = 1;").await;

    // Environment B already has a `clash` table, so v2 fails there.
    query(&cfg_b, "DEFINE TABLE clash;").await;

    let plan = DeploymentPlan::builder(registry.clone())
        .environments(["env_a", "env_b"])
        .migrations(both)
        .verify_health(false)
        .auto_rollback(true)
        .build();
    let results = coordinator(&registry).deploy(&plan).await.expect("deploy");

    let a = &results["env_a"];
    assert_eq!(a.status, DeploymentStatus::RolledBack, "{a:?}");
    assert_eq!(a.migrations_applied, 1, "A only had v2 pending: {a:?}");
    let b = &results["env_b"];
    assert_eq!(b.status, DeploymentStatus::Failed, "{b:?}");

    // A is back where it started: v1 applied, its table and data intact.
    assert_eq!(applied_versions(&cfg_a).await, vec!["20260101_000001"]);
    let tables_a = table_names(&cfg_a).await;
    assert!(tables_a.contains(&"keep_me".to_string()), "{tables_a:?}");
    assert!(!tables_a.contains(&"clash".to_string()), "{tables_a:?}");
    let rows = query(&cfg_a, "SELECT * FROM keep_me;").await;
    assert_eq!(rows[0].as_array().map(Vec::len), Some(1), "{rows:?}");

    // B's partial v1 was reverted; its pre-existing table was not touched.
    assert!(applied_versions(&cfg_b).await.is_empty());
    let tables_b = table_names(&cfg_b).await;
    assert!(!tables_b.contains(&"keep_me".to_string()), "{tables_b:?}");
    assert!(tables_b.contains(&"clash".to_string()), "{tables_b:?}");
}
#[tokio::test]
async fn require_approval_refuses_unapproved_deploy() {
    let database = unique_db("it_orch_appr");
    let Some(cfg) = integration_config(&database) else {
        eprintln!("SURREAL_URL not set; skipping");
        return;
    };
    let tmp = tempfile::tempdir().expect("tmpdir");
    seed_migrations(tmp.path(), "needs_ok");
    let migrations = discover_migrations(tmp.path()).expect("discover");

    let registry = EnvironmentRegistry::new();
    let env = EnvironmentConfig::builder("prod", cfg.clone())
        .require_approval(true)
        .build()
        .expect("env");
    registry.register(env).await;
    let plan = DeploymentPlan::builder(registry.clone())
        .environment("prod")
        .migrations(migrations)
        .verify_health(false)
        .build();
    let outcome = coordinator(&registry).deploy(&plan).await;
    assert!(
        outcome.is_err(),
        "unapproved deploy must be refused: {outcome:?}"
    );
    assert!(!table_names(&cfg).await.contains(&"needs_ok".to_string()));

    let approved = DeploymentPlan {
        approved: true,
        ..plan
    };
    let results = coordinator(&registry)
        .deploy(&approved)
        .await
        .expect("approved deploy");
    assert_eq!(results["prod"].status, DeploymentStatus::Success);
    assert!(table_names(&cfg).await.contains(&"needs_ok".to_string()));
}

#[tokio::test]
async fn allow_destructive_false_refuses_a_destructive_auto_rollback() {
    let db_a = unique_db("it_orch_keep");
    let db_b = unique_db("it_orch_break");
    let (Some(cfg_a), Some(cfg_b)) = (integration_config(&db_a), integration_config(&db_b)) else {
        eprintln!("SURREAL_URL not set; skipping");
        return;
    };
    let tmp = tempfile::tempdir().expect("tmpdir");
    write_migration(
        tmp.path(),
        "20260101_000001_one.surql",
        &migration_file(
            "20260101_000001",
            "DEFINE TABLE ledger;",
            "REMOVE TABLE ledger;",
        ),
    );
    let migrations = discover_migrations(tmp.path()).expect("discover");
    // B already has the table, so the migration fails there.
    query(&cfg_b, "DEFINE TABLE ledger;").await;

    let registry = EnvironmentRegistry::new();
    let guarded = EnvironmentConfig::builder("guarded", cfg_a.clone())
        .allow_destructive(false)
        .build()
        .expect("env");
    registry.register(guarded).await;
    register(&registry, "breaks", &cfg_b).await;
    let plan = DeploymentPlan::builder(registry.clone())
        .environments(["guarded", "breaks"])
        .migrations(migrations)
        .verify_health(false)
        .build();
    let results = coordinator(&registry).deploy(&plan).await.expect("deploy");

    let guarded = &results["guarded"];
    assert_eq!(guarded.status, DeploymentStatus::Success, "{guarded:?}");
    assert!(guarded.rolled_back_versions.is_empty());
    assert!(
        guarded
            .error
            .as_deref()
            .unwrap_or("")
            .contains("allow_destructive"),
        "{guarded:?}"
    );
    assert!(table_names(&cfg_a).await.contains(&"ledger".to_string()));
    assert_eq!(results["breaks"].status, DeploymentStatus::Failed);
}

#[tokio::test]
async fn allow_destructive_false_refuses_destructive_migration() {
    let database = unique_db("it_orch_destr");
    let Some(cfg) = integration_config(&database) else {
        eprintln!("SURREAL_URL not set; skipping");
        return;
    };
    query(&cfg, "DEFINE TABLE precious; CREATE precious:1;").await;
    let tmp = tempfile::tempdir().expect("tmpdir");
    write_migration(
        tmp.path(),
        "20260101_000001_drop.surql",
        &migration_file(
            "20260101_000001",
            "REMOVE TABLE precious;",
            "DEFINE TABLE precious;",
        ),
    );
    let migrations = discover_migrations(tmp.path()).expect("discover");

    let registry = EnvironmentRegistry::new();
    let env = EnvironmentConfig::builder("prod", cfg.clone())
        .allow_destructive(false)
        .build()
        .expect("env");
    registry.register(env).await;
    let plan = DeploymentPlan::builder(registry.clone())
        .environment("prod")
        .migrations(migrations)
        .verify_health(false)
        .build();
    let results = coordinator(&registry).deploy(&plan).await.expect("deploy");
    let result = &results["prod"];
    assert_eq!(result.status, DeploymentStatus::Failed, "{result:?}");
    assert!(
        result
            .error
            .as_deref()
            .unwrap_or("")
            .contains("destructive"),
        "{result:?}"
    );
    assert!(table_names(&cfg).await.contains(&"precious".to_string()));
}
