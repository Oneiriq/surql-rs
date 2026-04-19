//! Integration tests for the `squash`, `watcher`, and auto-snapshot
//! hook additions to the migration subsystem. These do not require a
//! running SurrealDB server.
//!
//! Covers:
//!
//! * `squash_migrations` — end-to-end read + write of a real migration
//!   directory and assertion that the resulting `.surql` file is
//!   loadable via the discovery module.
//! * `SchemaWatcher` — spawn, touch a file, assert a debounced
//!   `DriftReport` is delivered through the async channel.
//! * `create_snapshot_on_migration` — round-trip using the full
//!   enable-toggle plumbing.

use std::fmt::Write as _;
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use surql::migration::diff::SchemaSnapshot;
use surql::migration::{
    create_snapshot_on_migration, disable_auto_snapshots, discover_migrations,
    enable_auto_snapshots, squash_migrations, SnapshotHooks, SquashOptions,
};
use surql::schema::registry::SchemaRegistry;
use surql::schema::table::table_schema;

static TEST_DIR_COUNTER: AtomicU64 = AtomicU64::new(0);

fn unique_temp_dir(tag: &str) -> PathBuf {
    let nanos: u128 = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos());
    let n = TEST_DIR_COUNTER.fetch_add(1, Ordering::Relaxed);
    let pid = std::process::id();
    let dir = std::env::temp_dir().join(format!("surql-int-mig-polish-{tag}-{pid}-{nanos}-{n}"));
    fs::create_dir_all(&dir).expect("create temp dir");
    dir
}

fn write_migration(
    dir: &std::path::Path,
    version: &str,
    description: &str,
    up: &[&str],
    down: &[&str],
) -> PathBuf {
    let path = dir.join(format!("{version}_{description}.surql"));
    let mut buf = String::new();
    buf.push_str("-- @metadata\n");
    let _ = writeln!(buf, "-- version: {version}");
    let _ = writeln!(buf, "-- description: {description}");
    buf.push_str("-- @up\n");
    for s in up {
        buf.push_str(s);
        buf.push('\n');
    }
    buf.push_str("-- @down\n");
    for s in down {
        buf.push_str(s);
        buf.push('\n');
    }
    fs::write(&path, buf).expect("write migration");
    path
}

#[test]
fn squash_migrations_writes_loadable_surql_file() {
    let dir = unique_temp_dir("squash-ok");
    write_migration(
        &dir,
        "20260101_000000",
        "create_user",
        &["DEFINE TABLE user SCHEMAFULL;"],
        &["REMOVE TABLE user;"],
    );
    write_migration(
        &dir,
        "20260102_000000",
        "add_email",
        &["DEFINE FIELD email ON TABLE user TYPE string;"],
        &["REMOVE FIELD email ON TABLE user;"],
    );
    write_migration(
        &dir,
        "20260103_000000",
        "add_age",
        &["DEFINE FIELD age ON TABLE user TYPE int;"],
        &["REMOVE FIELD age ON TABLE user;"],
    );

    let result = squash_migrations(&dir, &SquashOptions::new()).expect("squash succeeds");
    assert_eq!(result.original_count, 3);
    assert!(result.squashed_path.exists());

    // The squashed file is itself a valid migration (discoverable via
    // `discover_migrations`). We re-discover the directory and expect
    // the squashed file to parse cleanly alongside the originals.
    let migrations = discover_migrations(&dir).expect("rediscovery succeeds");
    assert!(migrations.len() >= 4);

    // The squashed migration's UP section must contain the definitions
    // from every source migration.
    let content = fs::read_to_string(&result.squashed_path).expect("read squashed");
    assert!(content.contains("DEFINE TABLE user"));
    assert!(content.contains("DEFINE FIELD email"));
    assert!(content.contains("DEFINE FIELD age"));
    assert!(content.contains("-- squashed-from: 20260101_000000,20260102_000000,20260103_000000"));

    fs::remove_dir_all(&dir).ok();
}

#[test]
fn squash_migrations_dry_run_does_not_write() {
    let dir = unique_temp_dir("squash-dry");
    write_migration(
        &dir,
        "20260101_000000",
        "a",
        &["DEFINE TABLE a SCHEMAFULL;"],
        &[],
    );
    write_migration(
        &dir,
        "20260102_000000",
        "b",
        &["DEFINE TABLE b SCHEMAFULL;"],
        &[],
    );
    let result =
        squash_migrations(&dir, &SquashOptions::new().dry_run(true)).expect("squash succeeds");
    assert!(!result.squashed_path.exists());

    let files: Vec<_> = fs::read_dir(&dir).unwrap().filter_map(Result::ok).collect();
    // Only the two original inputs.
    assert_eq!(files.len(), 2);
    fs::remove_dir_all(&dir).ok();
}

#[cfg(feature = "watcher")]
#[tokio::test]
async fn watcher_delivers_debounced_drift_report_on_touch() {
    use surql::migration::{SchemaWatcher, WatcherConfig};

    let dir = unique_temp_dir("watcher-live");

    // Code snapshot registers `user`; recorded is empty so every debounce
    // tick should yield `drift_detected = true`.
    let provider = || SchemaSnapshot {
        tables: vec![table_schema("user")],
        edges: vec![],
    };
    let recorded = SchemaSnapshot::new();
    let (watcher, mut rx) = SchemaWatcher::start(
        std::slice::from_ref(&dir),
        &WatcherConfig::new().debounce_ms(100),
        provider,
        recorded,
    )
    .expect("start watcher");

    // Touch a `.surql` file to trigger an event.
    fs::write(
        dir.join("user.surql"),
        "-- @up\nSELECT 1;\n-- @down\nSELECT 2;\n",
    )
    .unwrap();

    let report = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("debounced report arrives")
        .expect("channel yields a report");
    assert!(report.drift_detected);

    watcher.stop();
    fs::remove_dir_all(&dir).ok();
}

#[test]
fn create_snapshot_on_migration_roundtrip() {
    // Serialise access to the global AUTO_SNAPSHOT_ENABLED toggle: this
    // integration test runs in its own binary so it cannot clash with
    // the lib-internal tests, but we still disable at the end so a
    // follow-up test never sees a leaked `true`.
    let registry = SchemaRegistry::new();
    registry.register_table(table_schema("user"));
    let dir = unique_temp_dir("auto-snap");

    // Disabled -> no op.
    disable_auto_snapshots();
    let out =
        create_snapshot_on_migration(&registry, &dir, "20260101_000000", 0, SnapshotHooks::none())
            .expect("disabled call is a no-op");
    assert!(out.is_none());
    assert!(fs::read_dir(&dir).unwrap().count() == 0);

    // Enabled -> writes a snapshot and fires both hooks.
    enable_auto_snapshots();
    let pre_cell = std::sync::Arc::new(std::sync::Mutex::new(0_u32));
    let post_cell = std::sync::Arc::new(std::sync::Mutex::new(String::new()));
    let pre = std::sync::Arc::clone(&pre_cell);
    let post = std::sync::Arc::clone(&post_cell);
    let hooks = SnapshotHooks::none()
        .pre(move |_: &str| {
            *pre.lock().unwrap() += 1;
        })
        .post(move |s| {
            *post.lock().unwrap() = s.version.clone();
        });

    let snap = create_snapshot_on_migration(&registry, &dir, "20260109_120000", 5, hooks)
        .expect("enabled call writes snapshot")
        .expect("snapshot returned");
    disable_auto_snapshots();

    assert_eq!(snap.migration_count, 5);
    assert_eq!(*pre_cell.lock().unwrap(), 1);
    assert_eq!(post_cell.lock().unwrap().as_str(), snap.version.as_str());
    let files: Vec<_> = fs::read_dir(&dir).unwrap().collect();
    assert_eq!(files.len(), 1);

    fs::remove_dir_all(&dir).ok();
}
