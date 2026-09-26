//! Integration tests for the migration runtime
//! ([`executor`](surql::migration::executor),
//! [`history`](surql::migration::history),
//! [`rollback`](surql::migration::rollback)).
//!
//! Gated on the `SURREAL_URL` env var so `cargo test` stays green when no
//! SurrealDB server is reachable. Exercise with:
//!
//! ```text
//! docker run -d -p 8000:8000 surrealdb/surrealdb:v3.0.5 start --user root --pass root memory
//! SURREAL_URL=ws://localhost:8000 SURREAL_USER=root SURREAL_PASS=root \
//!   cargo test --all-features --test integration_migration
//! ```

#![cfg(any(feature = "client", feature = "client-rustls"))]

use std::env;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

use chrono::Utc;
use surql::connection::{ConnectionConfig, DatabaseClient};
use surql::migration::{
    create_migration_plan, create_rollback_plan, discover_migrations, ensure_migration_table,
    execute_migration, execute_migration_plan, execute_rollback, get_applied_migrations,
    get_migration_status, get_modified_migrations, get_pending_migrations, is_migration_applied,
    migrate_down, migrate_up, record_migration, rehash_migrations, squash_migrations,
    MigrateUpOptions, Migration, MigrationDirection, MigrationHistory, MigrationState,
    SquashOptions,
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

fn unique_db() -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos());
    let seq = DB_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("it_mig_{nanos}_{seq}")
}

async fn connected_client(database: &str) -> Option<DatabaseClient> {
    let url = env_url()?;
    let namespace = format!("ns_{database}");
    let cfg = ConnectionConfig::builder()
        .url(url)
        .namespace(namespace)
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

fn write_migration(dir: &Path, filename: &str, body: &str) {
    std::fs::write(dir.join(filename), body).expect("write migration");
}

fn sample_up_down(dir: &Path, version: &str, table: &str) {
    let body = format!(
        "-- @metadata\n\
         -- version: {version}\n\
         -- description: create {table}\n\
         -- @up\n\
         DEFINE TABLE {table} SCHEMAFULL;\n\
         DEFINE FIELD name ON TABLE {table} TYPE string;\n\
         -- @down\n\
         REMOVE TABLE {table};\n",
    );
    write_migration(dir, &format!("{version}_create_{table}.surql"), &body);
}

#[tokio::test]
async fn ensure_migration_table_is_idempotent() {
    let Some(client) = connected_client(&unique_db()).await else {
        return;
    };
    ensure_migration_table(&client).await.unwrap();
    ensure_migration_table(&client).await.unwrap();
    ensure_migration_table(&client).await.unwrap();
    // Running three times back-to-back should be a no-op on the schema.
    let applied = get_applied_migrations(&client).await.unwrap();
    assert!(applied.is_empty());
}

#[tokio::test]
async fn record_migration_and_is_applied_round_trip() {
    let Some(client) = connected_client(&unique_db()).await else {
        return;
    };
    ensure_migration_table(&client).await.unwrap();

    let entry = MigrationHistory {
        version: "20260102_120000".into(),
        description: "round trip".into(),
        applied_at: Utc::now(),
        checksum: "abc123".into(),
        execution_time_ms: Some(7),
    };
    record_migration(&client, &entry).await.unwrap();

    assert!(is_migration_applied(&client, "20260102_120000")
        .await
        .unwrap());
    assert!(!is_migration_applied(&client, "does_not_exist")
        .await
        .unwrap());

    let applied = get_applied_migrations(&client).await.unwrap();
    assert_eq!(applied.len(), 1);
    assert_eq!(applied[0].version, "20260102_120000");
    assert_eq!(applied[0].execution_time_ms, Some(7));
}

#[tokio::test]
async fn migrate_up_applies_pending_migration() {
    let Some(client) = connected_client(&unique_db()).await else {
        return;
    };
    let tmp = tempfile::tempdir().unwrap();
    sample_up_down(tmp.path(), "20260101_000001", "fruit");

    let statuses = migrate_up(&client, tmp.path(), MigrateUpOptions::default())
        .await
        .unwrap();
    assert_eq!(statuses.len(), 1);
    assert_eq!(statuses[0].state, MigrationState::Applied);

    let applied = get_applied_migrations(&client).await.unwrap();
    assert_eq!(applied.len(), 1);
    assert_eq!(applied[0].version, "20260101_000001");

    let pending = get_pending_migrations(&client, tmp.path()).await.unwrap();
    assert!(pending.is_empty());

    let report = get_migration_status(&client, tmp.path()).await.unwrap();
    assert_eq!(report.total, 1);
    assert_eq!(report.applied_count(), 1);
    assert_eq!(report.pending_count(), 0);
}

#[tokio::test]
async fn migrate_down_returns_migration_to_pending() {
    let Some(client) = connected_client(&unique_db()).await else {
        return;
    };
    let tmp = tempfile::tempdir().unwrap();
    sample_up_down(tmp.path(), "20260101_000002", "berry");

    let _ = migrate_up(&client, tmp.path(), MigrateUpOptions::default())
        .await
        .unwrap();
    let rolled = migrate_down(&client, tmp.path(), 1).await.unwrap();
    assert_eq!(rolled.len(), 1);
    assert_eq!(rolled[0].state, MigrationState::Pending);

    let report = get_migration_status(&client, tmp.path()).await.unwrap();
    assert_eq!(report.pending_count(), 1);
    assert_eq!(report.applied_count(), 0);
}

/// Comments carrying `;` and `'` used to derail the statement splitter,
/// and the transaction then re-joined the fragments with `;\n`: the
/// migration "succeeded" and stored `'a;\nb'`.
#[tokio::test]
async fn comments_do_not_change_what_a_migration_stores() {
    let Some(client) = connected_client(&unique_db()).await else {
        return;
    };
    let tmp = tempfile::tempdir().unwrap();
    write_migration(
        tmp.path(),
        "20260101_000010_notes.surql",
        "-- @up\n\
         DEFINE TABLE note SCHEMALESS; -- step 1; see JIRA-12\n\
         -- the user's note\n\
         CREATE note:one SET text = 'a;b'; # trailing\n\
         CREATE note:two SET text = \"it's; fine\" -- no terminator\n\
         -- @down\n\
         REMOVE TABLE note;\n",
    );

    let statuses = migrate_up(&client, tmp.path(), MigrateUpOptions::default())
        .await
        .unwrap();
    assert_eq!(statuses[0].state, MigrationState::Applied, "{statuses:?}");

    let raw = client
        .query("RETURN [note:one.text, note:two.text];")
        .await
        .unwrap();
    assert_eq!(raw[0], serde_json::json!(["a;b", "it's; fine"]));
}

#[tokio::test]
async fn migration_plan_execution_applies_all() {
    let Some(client) = connected_client(&unique_db()).await else {
        return;
    };
    let tmp = tempfile::tempdir().unwrap();
    sample_up_down(tmp.path(), "20260101_000003", "alpha");
    sample_up_down(tmp.path(), "20260101_000004", "beta");

    let plan = create_migration_plan(&client, tmp.path()).await.unwrap();
    assert_eq!(plan.migrations.len(), 2);
    assert_eq!(plan.direction, MigrationDirection::Up);

    let statuses = execute_migration_plan(&client, plan).await.unwrap();
    assert_eq!(statuses.len(), 2);
    assert!(statuses.iter().all(|s| s.state == MigrationState::Applied));

    let applied = get_applied_migrations(&client).await.unwrap();
    assert_eq!(applied.len(), 2);
}

fn only_migration(dir: &Path) -> Migration {
    let mut all = discover_migrations(dir).unwrap();
    assert_eq!(all.len(), 1);
    all.remove(0)
}

async fn table_names(client: &DatabaseClient) -> Vec<String> {
    let info = client.query("INFO FOR DB;").await.unwrap();
    info[0]["tables"]
        .as_object()
        .map(|tables| tables.keys().cloned().collect())
        .unwrap_or_default()
}

/// The history row used to be written after the migration's own
/// transaction committed, so a failed write left the schema applied with
/// no record of it. Now a rejected row rejects the whole migration.
#[tokio::test]
async fn a_rejected_history_row_rolls_the_migration_back() {
    let Some(client) = connected_client(&unique_db()).await else {
        return;
    };
    let tmp = tempfile::tempdir().unwrap();
    sample_up_down(tmp.path(), "20260101_000020", "marker");
    let migration = only_migration(tmp.path());

    ensure_migration_table(&client).await.unwrap();
    let entry = MigrationHistory {
        version: migration.version.clone(),
        description: "recorded by someone else".into(),
        applied_at: Utc::now(),
        checksum: String::new(),
        execution_time_ms: None,
    };
    record_migration(&client, &entry).await.unwrap();

    let status = execute_migration(&client, &migration, MigrationDirection::Up)
        .await
        .unwrap();
    assert_eq!(status.state, MigrationState::Failed, "{status:?}");
    assert!(!table_names(&client).await.contains(&"marker".to_string()));
}

/// Two runners that both saw the migration as pending used to both apply
/// it, running its data statements twice.
#[tokio::test]
async fn concurrent_runners_apply_a_migration_once() {
    let db = unique_db();
    let (Some(first), Some(second)) = (connected_client(&db).await, connected_client(&db).await)
    else {
        return;
    };
    let tmp = tempfile::tempdir().unwrap();
    write_migration(
        tmp.path(),
        "20260101_000021_seed.surql",
        "-- @up\nCREATE counter SET n = 1;\n-- @down\nDELETE counter;\n",
    );
    let migration = only_migration(tmp.path());
    ensure_migration_table(&first).await.unwrap();

    let (a, b) = tokio::join!(
        execute_migration(&first, &migration, MigrationDirection::Up),
        execute_migration(&second, &migration, MigrationDirection::Up),
    );
    let states = [a.unwrap().state, b.unwrap().state];
    let applied = states
        .iter()
        .filter(|s| **s == MigrationState::Applied)
        .count();
    assert_eq!(applied, 1, "{states:?}");

    let raw = first
        .query("RETURN count(SELECT * FROM counter);")
        .await
        .unwrap();
    assert_eq!(raw[0], serde_json::json!(1));
    assert_eq!(get_applied_migrations(&first).await.unwrap().len(), 1);
}

#[tokio::test]
async fn history_records_the_execution_time() {
    let Some(client) = connected_client(&unique_db()).await else {
        return;
    };
    let tmp = tempfile::tempdir().unwrap();
    sample_up_down(tmp.path(), "20260101_000022", "timed");
    migrate_up(&client, tmp.path(), MigrateUpOptions::default())
        .await
        .unwrap();
    let applied = get_applied_migrations(&client).await.unwrap();
    assert!(applied[0].execution_time_ms.is_some(), "{applied:?}");
}

/// A squashed or blank migration has no down statements; rolling it back
/// used to delete its history row and report success while the schema
/// stayed.
#[tokio::test]
async fn rolling_back_without_down_statements_is_refused() {
    let Some(client) = connected_client(&unique_db()).await else {
        return;
    };
    let tmp = tempfile::tempdir().unwrap();
    write_migration(
        tmp.path(),
        "20260101_000023_one_way.surql",
        "-- @up\nDEFINE TABLE one_way;\n-- @down\n-- nothing to undo\n",
    );
    migrate_up(&client, tmp.path(), MigrateUpOptions::default())
        .await
        .unwrap();

    let rolled = migrate_down(&client, tmp.path(), 1).await.unwrap();
    assert_eq!(rolled.len(), 1);
    assert_eq!(rolled[0].state, MigrationState::Failed);
    assert!(rolled[0]
        .error
        .as_deref()
        .is_some_and(|e| e.contains("no down statements")));
    assert!(is_migration_applied(&client, "20260101_000023")
        .await
        .unwrap());
}

#[tokio::test]
async fn rolling_back_an_unrecorded_migration_runs_nothing() {
    let Some(client) = connected_client(&unique_db()).await else {
        return;
    };
    let tmp = tempfile::tempdir().unwrap();
    sample_up_down(tmp.path(), "20260101_000024", "keep");
    let migration = only_migration(tmp.path());
    client.query("DEFINE TABLE keep;").await.unwrap();

    let status = execute_migration(&client, &migration, MigrationDirection::Down)
        .await
        .unwrap();
    assert_eq!(status.state, MigrationState::Failed, "{status:?}");
    assert!(table_names(&client).await.contains(&"keep".to_string()));
}

/// A squashed migration gets a version of its own; on a database that had
/// applied its sources it used to be pending, and `migrate_up` re-ran every
/// statement in it.
#[tokio::test]
async fn a_squashed_migration_is_not_reapplied() {
    let Some(client) = connected_client(&unique_db()).await else {
        return;
    };
    let tmp = tempfile::tempdir().unwrap();
    write_migration(
        tmp.path(),
        "20260101_000030_seed.surql",
        "-- @up\nCREATE seed SET n = 1;\n-- @down\nDELETE seed;\n",
    );
    write_migration(
        tmp.path(),
        "20260101_000031_other.surql",
        "-- @up\nDEFINE TABLE other;\n-- @down\nREMOVE TABLE other;\n",
    );
    let first = migrate_up(&client, tmp.path(), MigrateUpOptions::default())
        .await
        .unwrap();
    assert_eq!(first.len(), 2);

    squash_migrations(tmp.path(), &SquashOptions::new()).unwrap();
    assert_eq!(discover_migrations(tmp.path()).unwrap().len(), 3);

    let second = migrate_up(&client, tmp.path(), MigrateUpOptions::default())
        .await
        .unwrap();
    assert!(second.is_empty(), "{second:?}");
    let raw = client
        .query("RETURN count(SELECT * FROM seed);")
        .await
        .unwrap();
    assert_eq!(raw[0], serde_json::json!(1));

    let report = get_migration_status(&client, tmp.path()).await.unwrap();
    assert_eq!(report.applied_count(), 3);
    assert_eq!(report.pending_count(), 0);
}

/// Versions used to compare as strings, so `v10` applied before `v9` and
/// a rollback to `v9` was "not older than" `v10`.
#[tokio::test]
async fn versions_order_numerically_when_applying_and_rolling_back() {
    let Some(client) = connected_client(&unique_db()).await else {
        return;
    };
    let tmp = tempfile::tempdir().unwrap();
    for (file, version, table) in [
        ("20260101_000040_nine.surql", "v9", "nine"),
        ("20260101_000041_ten.surql", "v10", "ten"),
    ] {
        write_migration(
            tmp.path(),
            file,
            &format!(
                "-- @metadata\n-- version: {version}\n-- description: {table}\n\
                 -- @up\nDEFINE TABLE {table};\n-- @down\nREMOVE INDEX IF EXISTS nothing ON {table};\n"
            ),
        );
    }
    let applied = migrate_up(&client, tmp.path(), MigrateUpOptions::default())
        .await
        .unwrap();
    let order: Vec<&str> = applied
        .iter()
        .map(|s| s.migration.version.as_str())
        .collect();
    assert_eq!(order, vec!["v9", "v10"]);

    let plan = create_rollback_plan(&client, tmp.path(), "v9")
        .await
        .unwrap();
    assert_eq!(plan.from_version, "v10");
    let planned: Vec<&str> = plan.migrations.iter().map(|m| m.version.as_str()).collect();
    assert_eq!(planned, vec!["v10"]);
}

#[tokio::test]
async fn a_destructive_rollback_waits_for_approval() {
    let Some(client) = connected_client(&unique_db()).await else {
        return;
    };
    let tmp = tempfile::tempdir().unwrap();
    sample_up_down(tmp.path(), "20260101_000025", "first");
    sample_up_down(tmp.path(), "20260101_000026", "second");
    migrate_up(&client, tmp.path(), MigrateUpOptions::default())
        .await
        .unwrap();

    let plan = create_rollback_plan(&client, tmp.path(), "20260101_000025")
        .await
        .unwrap();
    assert!(plan.requires_approval);
    assert!(execute_rollback(&client, plan.clone()).await.is_err());
    assert!(table_names(&client).await.contains(&"second".to_string()));

    let result = execute_rollback(&client, plan.approve()).await.unwrap();
    assert!(result.success, "{result:?}");
    assert!(!table_names(&client).await.contains(&"second".to_string()));
}

#[tokio::test]
async fn an_edited_applied_migration_blocks_up_until_rehashed() {
    let Some(client) = connected_client(&unique_db()).await else {
        return;
    };
    let tmp = tempfile::tempdir().unwrap();
    sample_up_down(tmp.path(), "20260101_000001", "fruit");
    migrate_up(&client, tmp.path(), MigrateUpOptions::default())
        .await
        .unwrap();

    // Edit the applied file, and add a new migration to apply.
    let path = tmp.path().join("20260101_000001_create_fruit.surql");
    let edited = std::fs::read_to_string(&path).unwrap().replace(
        "TYPE string;",
        "TYPE string; -- a note added after applying",
    );
    std::fs::write(&path, edited).unwrap();
    sample_up_down(tmp.path(), "20260101_000002", "veg");

    let report = get_migration_status(&client, tmp.path()).await.unwrap();
    assert_eq!(report.applied_count(), 1);
    assert_eq!(report.pending_count(), 1);
    assert_eq!(report.modified.len(), 1);
    assert_eq!(report.modified[0].version, "20260101_000001");
    assert_eq!(
        get_modified_migrations(&client, tmp.path()).await.unwrap(),
        report.modified
    );

    let err = migrate_up(&client, tmp.path(), MigrateUpOptions::default())
        .await
        .expect_err("an edited applied migration refuses the run");
    assert!(err.to_string().contains("20260101_000001"), "{err}");
    assert!(!table_names(&client).await.contains(&"veg".to_string()));

    let err = rehash_migrations(&client, tmp.path(), &["20260101_000002".into()])
        .await
        .expect_err("only a modified applied migration can be re-hashed");
    assert!(err.to_string().contains("20260101_000002"), "{err}");

    let rehashed = rehash_migrations(&client, tmp.path(), &[]).await.unwrap();
    assert_eq!(rehashed, report.modified);
    let history = get_applied_migrations(&client).await.unwrap();
    assert_eq!(history[0].checksum, rehashed[0].current_checksum);
    assert!(get_modified_migrations(&client, tmp.path())
        .await
        .unwrap()
        .is_empty());

    let statuses = migrate_up(&client, tmp.path(), MigrateUpOptions::default())
        .await
        .unwrap();
    assert_eq!(statuses.len(), 1);
    assert_eq!(statuses[0].migration.version, "20260101_000002");
}
