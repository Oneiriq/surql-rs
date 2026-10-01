use super::*;
use crate::migration::models::{DiffOperation, SchemaDiff};
use crate::migration::versioning::{store_snapshot, VersionedSnapshot};
use crate::schema::registry::SchemaRegistry;
use crate::schema::table::table_schema;
use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

static TEST_DIR_COUNTER: AtomicU64 = AtomicU64::new(0);

fn unique_temp_dir(tag: &str) -> PathBuf {
    let nanos: u128 = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos());
    let n = TEST_DIR_COUNTER.fetch_add(1, Ordering::Relaxed);
    let pid = std::process::id();
    let dir = std::env::temp_dir().join(format!("surql-hooks-{tag}-{pid}-{nanos}-{n}"));
    fs::create_dir_all(&dir).expect("create temp dir");
    dir
}

/// Spin up an ephemeral git repository in `dir`. Returns `true` on
/// success. If `git` is not available, returns `false` so individual
/// tests can skip gracefully.
///
/// Strips inherited git env vars so the temp repo is isolated from
/// any outer git invocation that spawned the test (e.g. running
/// `cargo test` from inside a `git push -> pre-push` hook, where
/// `GIT_DIR` etc. are exported for child processes).
fn init_git_repo(dir: &Path) -> bool {
    let Ok(status) = clean_git(dir).args(["init", "-q"]).status() else {
        return false;
    };
    if !status.success() {
        return false;
    }
    let _ = clean_git(dir)
        .args(["config", "user.email", "test@example.com"])
        .status();
    let _ = clean_git(dir)
        .args(["config", "user.name", "surql-test"])
        .status();
    true
}

fn git_add(dir: &Path, relpath: &str) -> bool {
    clean_git(dir)
        .args(["add", "--", relpath])
        .status()
        .is_ok_and(|s| s.success())
}

/// `Command::new("git")` rooted at `dir` with all inherited git env
/// vars stripped. Mirrors the production `get_staged_schema_files`
/// behaviour and stops the outer repo's index leaking into temp-dir
/// fixtures.
fn clean_git(dir: &Path) -> Command {
    let mut cmd = Command::new("git");
    cmd.current_dir(dir)
        .env_remove("GIT_DIR")
        .env_remove("GIT_WORK_TREE")
        .env_remove("GIT_INDEX_FILE")
        .env_remove("GIT_OBJECT_DIRECTORY")
        .env_remove("GIT_COMMON_DIR");
    cmd
}

fn make_diff(op: DiffOperation, table: &str, field: Option<&str>, desc: &str) -> SchemaDiff {
    SchemaDiff {
        operation: op,
        table: table.to_string(),
        field: field.map(ToString::to_string),
        index: None,
        event: None,
        bucket: None,
        analyzer: None,
        object: None,
        description: desc.to_string(),
        forward_sql: String::new(),
        backward_sql: String::new(),
        details: BTreeMap::new(),
    }
}

// --- DriftSeverity ------------------------------------------------------

#[test]
fn severity_as_str_round_trip() {
    assert_eq!(DriftSeverity::Info.as_str(), "info");
    assert_eq!(DriftSeverity::Warning.as_str(), "warning");
    assert_eq!(DriftSeverity::Critical.as_str(), "critical");
}

#[test]
fn severity_display_matches_as_str() {
    assert_eq!(format!("{}", DriftSeverity::Info), "info");
    assert_eq!(format!("{}", DriftSeverity::Critical), "critical");
}

#[test]
fn severity_for_add_is_info() {
    assert_eq!(
        severity_for_operation(DiffOperation::AddTable),
        DriftSeverity::Info
    );
    assert_eq!(
        severity_for_operation(DiffOperation::AddField),
        DriftSeverity::Info
    );
    assert_eq!(
        severity_for_operation(DiffOperation::AddIndex),
        DriftSeverity::Info
    );
}

#[test]
fn severity_for_modify_is_warning() {
    assert_eq!(
        severity_for_operation(DiffOperation::ModifyField),
        DriftSeverity::Warning
    );
    assert_eq!(
        severity_for_operation(DiffOperation::ModifyPermissions),
        DriftSeverity::Warning
    );
    assert_eq!(
        severity_for_operation(DiffOperation::ModifyIndex),
        DriftSeverity::Warning
    );
    assert_eq!(
        severity_for_operation(DiffOperation::ModifyEvent),
        DriftSeverity::Warning
    );
}

#[test]
fn severity_for_drop_is_critical() {
    assert_eq!(
        severity_for_operation(DiffOperation::DropTable),
        DriftSeverity::Critical
    );
    assert_eq!(
        severity_for_operation(DiffOperation::DropField),
        DriftSeverity::Critical
    );
    assert_eq!(
        severity_for_operation(DiffOperation::DropIndex),
        DriftSeverity::Critical
    );
}

// --- DriftIssue / DriftReport ------------------------------------------

#[test]
fn issue_from_diff_carries_fields() {
    let diff = make_diff(
        DiffOperation::AddField,
        "user",
        Some("email"),
        "add email field",
    );
    let issue = DriftIssue::from_diff(&diff);
    assert_eq!(issue.severity, DriftSeverity::Info);
    assert_eq!(issue.operation, DiffOperation::AddField);
    assert_eq!(issue.table, "user");
    assert_eq!(issue.field.as_deref(), Some("email"));
    assert_eq!(issue.description, "add email field");
}

#[test]
fn report_empty_has_no_drift() {
    let r = DriftReport::empty();
    assert!(!r.drift_detected);
    assert_eq!(r.issues, [] as [crate::migration::hooks::DriftIssue; 0]);
    assert!(r.suggested_migration.is_none());
}

#[test]
fn report_from_empty_diffs_is_empty() {
    let r = DriftReport::from_diffs(&[]);
    assert_eq!(r, DriftReport::empty());
}

#[test]
fn report_from_diffs_populates_issues() {
    let diffs = vec![
        make_diff(DiffOperation::AddTable, "user", None, "create user"),
        make_diff(DiffOperation::DropTable, "stale", None, "drop stale"),
    ];
    let r = DriftReport::from_diffs(&diffs);
    assert!(r.drift_detected);
    assert_eq!(r.issues.len(), 2);
    assert!(r.suggested_migration.is_some());
    assert_eq!(r.critical_count(), 1);
}

#[test]
fn report_summary_no_drift() {
    assert!(DriftReport::empty()
        .to_summary()
        .contains("No schema drift"));
}

#[test]
fn report_summary_mentions_each_issue() {
    let diffs = vec![make_diff(
        DiffOperation::AddField,
        "user",
        Some("email"),
        "add email",
    )];
    let summary = DriftReport::from_diffs(&diffs).to_summary();
    assert!(summary.contains("Schema drift detected"));
    assert!(summary.contains("user.email"));
    assert!(summary.contains("AddField"));
    assert!(summary.contains("add email"));
    assert!(summary.contains("Suggested:"));
}

#[test]
fn report_summary_singular_vs_plural() {
    let one = DriftReport::from_diffs(&[make_diff(DiffOperation::AddTable, "user", None, "add")]);
    assert!(one.to_summary().contains("1 issue)"));

    let two = DriftReport::from_diffs(&[
        make_diff(DiffOperation::AddTable, "a", None, "a"),
        make_diff(DiffOperation::AddTable, "b", None, "b"),
    ]);
    assert!(two.to_summary().contains("2 issues)"));
}

// --- check_schema_drift_from_snapshots ---------------------------------

#[test]
fn drift_from_snapshots_no_change_is_clean() {
    let snap = SchemaSnapshot {
        tables: vec![table_schema("user")],
        edges: vec![],
        buckets: vec![],
        ..Default::default()
    };
    let report = check_schema_drift_from_snapshots(&snap, &snap);
    assert!(!report.drift_detected);
    assert_eq!(
        report.issues,
        [] as [crate::migration::hooks::DriftIssue; 0]
    );
}

#[test]
fn drift_from_snapshots_detects_new_table() {
    let code = SchemaSnapshot {
        tables: vec![table_schema("user")],
        edges: vec![],
        buckets: vec![],
        ..Default::default()
    };
    let recorded = SchemaSnapshot::new();
    let report = check_schema_drift_from_snapshots(&code, &recorded);
    assert!(report.drift_detected);
    assert_ne!(
        report.issues,
        [] as [crate::migration::hooks::DriftIssue; 0]
    );
    assert!(report
        .issues
        .iter()
        .any(|i| i.operation == DiffOperation::AddTable && i.table == "user"));
}

#[test]
fn drift_from_snapshots_detects_dropped_table() {
    let code = SchemaSnapshot::new();
    let recorded = SchemaSnapshot {
        tables: vec![table_schema("old")],
        edges: vec![],
        buckets: vec![],
        ..Default::default()
    };
    let report = check_schema_drift_from_snapshots(&code, &recorded);
    assert!(report.drift_detected);
    assert!(report
        .issues
        .iter()
        .any(|i| i.operation == DiffOperation::DropTable && i.table == "old"));
    assert!(report.critical_count() >= 1);
}

#[test]
fn drift_report_serde_round_trip() {
    let diffs = vec![make_diff(
        DiffOperation::AddTable,
        "user",
        None,
        "create user",
    )];
    let report = DriftReport::from_diffs(&diffs);
    let json = serde_json::to_string(&report).expect("serialise");
    let parsed: DriftReport = serde_json::from_str(&json).expect("deserialise");
    assert_eq!(parsed, report);
}

// --- check_schema_drift (with snapshots dir) ---------------------------

#[test]
fn check_drift_with_no_snapshots_dir_treats_recorded_as_empty() {
    let registry = SchemaRegistry::new();
    registry.register_table(table_schema("user"));
    let report = check_schema_drift(&registry, None, None).expect("check_schema_drift succeeds");
    assert!(report.drift_detected);
}

#[test]
fn check_drift_with_empty_snapshots_dir_treats_recorded_as_empty() {
    let registry = SchemaRegistry::new();
    registry.register_table(table_schema("user"));
    let dir = unique_temp_dir("empty-snaps");
    let report =
        check_schema_drift(&registry, Some(&dir), None).expect("check_schema_drift succeeds");
    assert!(report.drift_detected);
}

#[test]
fn check_drift_with_nonexistent_snapshots_dir_is_ok() {
    let registry = SchemaRegistry::new();
    let missing = std::env::temp_dir().join("surql-hooks-does-not-exist-xyz-123");
    let report =
        check_schema_drift(&registry, Some(&missing), None).expect("check_schema_drift succeeds");
    assert!(!report.drift_detected);
}

#[test]
fn check_drift_matching_snapshot_has_no_drift() {
    let registry = SchemaRegistry::new();
    registry.register_table(table_schema("user"));

    let dir = unique_temp_dir("match-snap");
    let mut tables = BTreeMap::new();
    tables.insert("user".to_string(), table_schema("user"));
    let snap = VersionedSnapshot {
        version: "20260101_000000".to_string(),
        timestamp: chrono::Utc::now(),
        description: "baseline".to_string(),
        tables,
        edges: BTreeMap::new(),
        accesses: BTreeMap::new(),
        buckets: BTreeMap::new(),
        checksum: String::new(),
        migration_count: 0,
    };
    store_snapshot(&snap, &dir).expect("store snapshot");

    let report =
        check_schema_drift(&registry, Some(&dir), None).expect("check_schema_drift succeeds");
    assert!(!report.drift_detected, "report: {report:?}");
}

#[test]
fn check_drift_uses_latest_snapshot() {
    let registry = SchemaRegistry::new();
    registry.register_table(table_schema("user"));
    registry.register_table(table_schema("post"));

    let dir = unique_temp_dir("latest-snap");

    // Older snapshot only has `user`.
    let mut older_tables = BTreeMap::new();
    older_tables.insert("user".to_string(), table_schema("user"));
    let older = VersionedSnapshot {
        version: "20260101_000000".to_string(),
        timestamp: chrono::Utc::now(),
        description: "older".to_string(),
        tables: older_tables,
        edges: BTreeMap::new(),
        accesses: BTreeMap::new(),
        buckets: BTreeMap::new(),
        checksum: String::new(),
        migration_count: 0,
    };
    store_snapshot(&older, &dir).expect("store older");

    // Newer snapshot has both; makes registry match exactly.
    let mut newer_tables = BTreeMap::new();
    newer_tables.insert("user".to_string(), table_schema("user"));
    newer_tables.insert("post".to_string(), table_schema("post"));
    let newer = VersionedSnapshot {
        version: "20260301_000000".to_string(),
        timestamp: chrono::Utc::now(),
        description: "newer".to_string(),
        tables: newer_tables,
        edges: BTreeMap::new(),
        accesses: BTreeMap::new(),
        buckets: BTreeMap::new(),
        checksum: String::new(),
        migration_count: 0,
    };
    store_snapshot(&newer, &dir).expect("store newer");

    let report =
        check_schema_drift(&registry, Some(&dir), None).expect("check_schema_drift succeeds");
    assert!(!report.drift_detected, "report: {report:?}");
}

#[test]
fn registry_to_snapshot_collects_all_tables() {
    let registry = SchemaRegistry::new();
    registry.register_table(table_schema("user"));
    registry.register_table(table_schema("post"));
    let snap = registry_to_snapshot(&registry);
    assert_eq!(snap.tables.len(), 2);
    let names: Vec<&str> = snap.tables.iter().map(|t| t.name.as_str()).collect();
    assert!(names.contains(&"user"));
    assert!(names.contains(&"post"));
}

#[test]
fn versioned_to_snapshot_preserves_tables() {
    let mut tables = BTreeMap::new();
    tables.insert("user".to_string(), table_schema("user"));
    let snap = VersionedSnapshot {
        version: "v1".to_string(),
        timestamp: chrono::Utc::now(),
        description: String::new(),
        tables,
        edges: BTreeMap::new(),
        accesses: BTreeMap::new(),
        buckets: BTreeMap::new(),
        checksum: String::new(),
        migration_count: 0,
    };
    let schema = versioned_to_snapshot(&snap);
    assert_eq!(schema.tables.len(), 1);
    assert_eq!(schema.tables[0].name, "user");
}

// --- default_schema_filter ---------------------------------------------

#[test]
fn default_filter_accepts_rs() {
    assert!(default_schema_filter(&PathBuf::from("src/schemas/user.rs")));
}

#[test]
fn default_filter_accepts_surql() {
    assert!(default_schema_filter(&PathBuf::from(
        "migrations/20260101_000000_init.surql"
    )));
}

#[test]
fn default_filter_rejects_non_schema() {
    assert!(!default_schema_filter(&PathBuf::from("README.md")));
    assert!(!default_schema_filter(&PathBuf::from("src/main.py")));
    assert!(!default_schema_filter(&PathBuf::from("Cargo.toml")));
}

// --- get_staged_schema_files: fake-git fixtures ------------------------

#[test]
fn staged_returns_empty_when_dir_missing() {
    let missing = std::env::temp_dir().join("surql-hooks-never-exists-xyz");
    let files = get_staged_schema_files(&missing, default_schema_filter)
        .expect("get_staged_schema_files succeeds");
    assert_eq!(files, [] as [std::path::PathBuf; 0]);
}

#[test]
fn staged_returns_empty_outside_git_repo() {
    let dir = unique_temp_dir("no-git");
    // No `git init` here; call should gracefully return empty.
    let files = get_staged_schema_files(&dir, default_schema_filter)
        .expect("get_staged_schema_files succeeds");
    assert_eq!(files, [] as [std::path::PathBuf; 0]);
}

#[test]
fn staged_returns_empty_when_nothing_staged() {
    let dir = unique_temp_dir("empty-stage");
    if !init_git_repo(&dir) {
        eprintln!("skipping: git not available");
        return;
    }
    // Create an untracked file; don't stage it.
    fs::write(dir.join("untracked.surql"), "-- @up\nSELECT 1;\n").unwrap();
    let files = get_staged_schema_files(&dir, default_schema_filter)
        .expect("get_staged_schema_files succeeds");
    assert_eq!(files, [] as [std::path::PathBuf; 0]);
}

#[test]
fn staged_detects_newly_staged_schema_file() {
    let dir = unique_temp_dir("stage-one");
    if !init_git_repo(&dir) {
        eprintln!("skipping: git not available");
        return;
    }
    let schema_subdir = dir.join("schemas");
    fs::create_dir_all(&schema_subdir).unwrap();
    let file = schema_subdir.join("user.surql");
    fs::write(&file, "-- schema\n").unwrap();
    assert!(git_add(&dir, "schemas/user.surql"));

    let files = get_staged_schema_files(&schema_subdir, default_schema_filter)
        .expect("get_staged_schema_files succeeds");
    assert_eq!(files.len(), 1);
    assert!(files[0].to_string_lossy().ends_with("user.surql"));
}

#[test]
fn staged_filters_by_custom_predicate() {
    let dir = unique_temp_dir("stage-filter");
    if !init_git_repo(&dir) {
        eprintln!("skipping: git not available");
        return;
    }
    let schema_subdir = dir.join("schemas");
    fs::create_dir_all(&schema_subdir).unwrap();
    fs::write(schema_subdir.join("user.surql"), "-- surql\n").unwrap();
    fs::write(schema_subdir.join("README.md"), "docs\n").unwrap();
    assert!(git_add(&dir, "schemas/user.surql"));
    assert!(git_add(&dir, "schemas/README.md"));

    // Custom filter: only accept `.md` files.
    let md_only = |p: &Path| p.extension().and_then(|e| e.to_str()) == Some("md");
    let md_files =
        get_staged_schema_files(&schema_subdir, md_only).expect("get_staged_schema_files succeeds");
    assert_eq!(md_files.len(), 1);
    assert!(md_files[0].to_string_lossy().ends_with("README.md"));

    // Default filter only accepts the surql file.
    let rs_surql_only = get_staged_schema_files(&schema_subdir, default_schema_filter)
        .expect("get_staged_schema_files succeeds");
    assert_eq!(rs_surql_only.len(), 1);
    assert!(rs_surql_only[0].to_string_lossy().ends_with("user.surql"));
}

#[test]
fn staged_excludes_files_outside_schema_dir() {
    let dir = unique_temp_dir("stage-outside");
    if !init_git_repo(&dir) {
        eprintln!("skipping: git not available");
        return;
    }
    let schema_subdir = dir.join("schemas");
    let other_subdir = dir.join("other");
    fs::create_dir_all(&schema_subdir).unwrap();
    fs::create_dir_all(&other_subdir).unwrap();
    fs::write(schema_subdir.join("keep.surql"), "x").unwrap();
    fs::write(other_subdir.join("skip.surql"), "x").unwrap();
    assert!(git_add(&dir, "schemas/keep.surql"));
    assert!(git_add(&dir, "other/skip.surql"));

    let files = get_staged_schema_files(&schema_subdir, default_schema_filter)
        .expect("get_staged_schema_files succeeds");
    assert_eq!(files.len(), 1);
    assert!(files[0].to_string_lossy().ends_with("keep.surql"));
}

#[test]
fn staged_handles_multiple_files() {
    let dir = unique_temp_dir("stage-multi");
    if !init_git_repo(&dir) {
        eprintln!("skipping: git not available");
        return;
    }
    let schema_subdir = dir.join("schemas");
    fs::create_dir_all(&schema_subdir).unwrap();
    fs::write(schema_subdir.join("a.surql"), "x").unwrap();
    fs::write(schema_subdir.join("b.surql"), "x").unwrap();
    fs::write(schema_subdir.join("c.rs"), "x").unwrap();
    assert!(git_add(&dir, "schemas/a.surql"));
    assert!(git_add(&dir, "schemas/b.surql"));
    assert!(git_add(&dir, "schemas/c.rs"));

    let files = get_staged_schema_files(&schema_subdir, default_schema_filter)
        .expect("get_staged_schema_files succeeds");
    assert_eq!(files.len(), 3);
}

/// git quotes non-ASCII paths (`"sch\303\251ma.surql"`) unless told
/// not to, and the quoted name failed the extension filter.
#[test]
fn staged_includes_non_ascii_file_names() {
    let dir = unique_temp_dir("stage-unicode");
    if !init_git_repo(&dir) {
        eprintln!("skipping: git not available");
        return;
    }
    fs::write(dir.join("schéma.surql"), "x").unwrap();
    fs::write(dir.join("with space.surql"), "x").unwrap();
    assert!(git_add(&dir, "schéma.surql"));
    assert!(git_add(&dir, "with space.surql"));
    let mut files = get_staged_schema_files(&dir, default_schema_filter)
        .expect("get_staged_schema_files succeeds");
    files.sort();
    assert_eq!(
        files,
        vec![
            PathBuf::from("schéma.surql"),
            PathBuf::from("with space.surql")
        ]
    );
}

/// A corrupt newest snapshot used to be skipped, so drift was checked
/// against an older baseline without a word.
#[test]
fn check_drift_refuses_a_corrupt_newest_snapshot() {
    let registry = SchemaRegistry::new();
    registry.register_table(table_schema("user"));
    let dir = unique_temp_dir("corrupt-newest");
    store_snapshot(&VersionedSnapshot::builder("20260101_000000").build(), &dir)
        .expect("store older");
    fs::write(dir.join("20260301_000000.json"), "{ not json").unwrap();

    let err = check_schema_drift(&registry, Some(&dir), None).unwrap_err();
    assert!(matches!(err, SurqlError::Serialization { .. }), "{err}");
}

#[test]
fn staged_accepts_schema_dir_pointing_to_repo_root() {
    let dir = unique_temp_dir("stage-root");
    if !init_git_repo(&dir) {
        eprintln!("skipping: git not available");
        return;
    }
    fs::write(dir.join("init.surql"), "x").unwrap();
    assert!(git_add(&dir, "init.surql"));
    let files = get_staged_schema_files(&dir, default_schema_filter)
        .expect("get_staged_schema_files succeeds");
    assert_eq!(files.len(), 1);
    assert!(files[0].to_string_lossy().ends_with("init.surql"));
}

// --- generate_precommit_config -----------------------------------------

#[test]
fn precommit_config_starts_with_repos() {
    let yaml = generate_precommit_config("schemas/", true);
    assert!(yaml.starts_with("repos:"));
}

#[test]
fn precommit_config_contains_hook_id() {
    let yaml = generate_precommit_config("schemas/", true);
    assert!(yaml.contains("id: surql-schema-check"));
}

#[test]
fn precommit_config_embeds_schema_path() {
    let yaml = generate_precommit_config("custom/schemas/", true);
    assert!(yaml.contains("--schema custom/schemas/"));
}

#[test]
fn precommit_config_toggles_fail_on_drift() {
    let with_flag = generate_precommit_config("schemas/", true);
    assert!(with_flag.contains("--fail-on-drift"));

    let without_flag = generate_precommit_config("schemas/", false);
    assert!(!without_flag.contains("--fail-on-drift"));
}

#[test]
fn precommit_config_has_expected_yaml_keys() {
    // Rather than pulling in a YAML parser, verify the structural
    // invariants the snippet is contractually required to hold: a
    // single top-level `repos:` key, exactly one repo entry, and one
    // hook entry with a name / entry / language / pass_filenames.
    let yaml = generate_precommit_config("schemas/", true);
    assert_eq!(
        yaml.matches("\nrepos:").count() + usize::from(yaml.starts_with("repos:")),
        1
    );
    assert_eq!(yaml.matches("- repo: local").count(), 1);
    assert_eq!(yaml.matches("- id: surql-schema-check").count(), 1);
    assert!(yaml.contains("name: Check schema migrations"));
    assert!(yaml.contains("entry: surql schema check"));
    assert!(yaml.contains("language: system"));
    assert!(yaml.contains("pass_filenames: false"));
}

#[test]
fn precommit_config_is_nonempty() {
    let yaml = generate_precommit_config("schemas/", true);
    assert_ne!(yaml, "");
    assert!(yaml.len() > 100);
}

// --- auto-snapshot hooks -----------------------------------------------

// NOTE: these tests mutate the global AUTO_SNAPSHOT_ENABLED toggle.
// They are serialised via AUTO_SNAPSHOT_TEST_LOCK because cargo test
// runs tests in parallel by default and a shared atomic toggle
// cannot be partitioned per-thread.

static AUTO_SNAPSHOT_TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn with_flag_lock<R>(f: impl FnOnce() -> R) -> R {
    let guard = AUTO_SNAPSHOT_TEST_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let out = f();
    drop(guard);
    out
}

#[test]
fn enable_disable_is_enabled_roundtrip() {
    with_flag_lock(|| {
        disable_auto_snapshots();
        assert!(!is_auto_snapshot_enabled());
        enable_auto_snapshots();
        assert!(is_auto_snapshot_enabled());
        disable_auto_snapshots();
        assert!(!is_auto_snapshot_enabled());
    });
}

#[test]
fn create_snapshot_on_migration_no_op_when_disabled() {
    with_flag_lock(|| {
        disable_auto_snapshots();
        let registry = SchemaRegistry::new();
        registry.register_table(table_schema("user"));
        let dir = unique_temp_dir("auto-off");
        let hooks = SnapshotHooks::none();
        let out = create_snapshot_on_migration(&registry, &dir, "20260101_000000", 0, hooks)
            .expect("hook runs");
        assert!(out.is_none());
        let list = std::fs::read_dir(&dir).unwrap();
        assert_eq!(list.count(), 0);
    });
}

#[test]
fn create_snapshot_on_migration_writes_file_when_enabled() {
    with_flag_lock(|| {
        enable_auto_snapshots();
        let registry = SchemaRegistry::new();
        registry.register_table(table_schema("user"));
        let dir = unique_temp_dir("auto-on");
        let snap = create_snapshot_on_migration(
            &registry,
            &dir,
            "20260101_000000",
            7,
            SnapshotHooks::none(),
        )
        .expect("hook runs")
        .expect("snapshot present");
        disable_auto_snapshots();
        assert_eq!(snap.migration_count, 7);
        let files: Vec<_> = std::fs::read_dir(&dir).unwrap().collect();
        assert_eq!(files.len(), 1);
    });
}

#[test]
fn create_snapshot_on_migration_runs_pre_and_post_hooks() {
    with_flag_lock(|| {
        enable_auto_snapshots();
        let registry = SchemaRegistry::new();
        registry.register_table(table_schema("user"));
        let dir = unique_temp_dir("auto-hooks");

        let pre_cell = std::sync::Arc::new(std::sync::Mutex::new(Option::<String>::None));
        let post_cell = std::sync::Arc::new(std::sync::Mutex::new(Option::<String>::None));

        let pre_cell_cb = std::sync::Arc::clone(&pre_cell);
        let post_cell_cb = std::sync::Arc::clone(&post_cell);
        let hooks = SnapshotHooks::none()
            .pre(move |v: &str| {
                *pre_cell_cb.lock().unwrap() = Some(v.to_string());
            })
            .post(move |s: &VersionedSnapshot| {
                *post_cell_cb.lock().unwrap() = Some(s.version.clone());
            });

        let snap = create_snapshot_on_migration(&registry, &dir, "20260109_120000", 3, hooks)
            .expect("hook runs")
            .expect("snapshot present");
        disable_auto_snapshots();

        assert_eq!(pre_cell.lock().unwrap().as_deref(), Some("20260109_120000"));
        assert_eq!(
            post_cell.lock().unwrap().as_deref(),
            Some(snap.version.as_str())
        );
    });
}

#[test]
fn create_snapshot_on_migration_surfaces_validation_error_on_empty_version() {
    with_flag_lock(|| {
        enable_auto_snapshots();
        let registry = SchemaRegistry::new();
        let dir = unique_temp_dir("auto-empty");
        let err = create_snapshot_on_migration(&registry, &dir, "", 0, SnapshotHooks::none())
            .expect_err("must reject empty version");
        disable_auto_snapshots();
        assert!(matches!(err, SurqlError::Validation { .. }));
    });
}
