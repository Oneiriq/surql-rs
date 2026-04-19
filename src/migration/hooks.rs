//! Git hook utilities for schema drift detection.
//!
//! Port of `surql/migration/hooks.py`. Provides helpers for integrating
//! schema drift detection into git pre-commit hooks and CI/CD pipelines.
//! Drift is detected by diffing a code-side [`SchemaSnapshot`] against a
//! recorded (on-disk) snapshot; no database connection is required.
//!
//! ## Deviation from Python
//!
//! The Python implementation imports staged `.py` files via `importlib`
//! and uses file modification-time heuristics to detect drift. Rust cannot
//! execute arbitrary Python at runtime, so this port:
//!
//! * Takes two [`SchemaSnapshot`] values (code vs recorded) and compares
//!   them with [`crate::migration::diff::diff_schemas`], returning a
//!   structured [`DriftReport`].
//! * Exposes a higher-level [`check_schema_drift`] that derives the
//!   code-side snapshot from a [`SchemaRegistry`] and loads the recorded
//!   snapshot from the latest JSON file in a snapshots directory.
//! * Shells out to `git diff --cached --name-only --relative` via
//!   [`std::process::Command`] with no external dependency.
//! * Returns the pre-commit YAML snippet as a [`String`] (the caller is
//!   responsible for writing it to `.pre-commit-config.yaml`).
//!
//! ## Examples
//!
//! ```
//! use surql::migration::diff::SchemaSnapshot;
//! use surql::migration::hooks::check_schema_drift_from_snapshots;
//! use surql::schema::table::table_schema;
//!
//! let code = SchemaSnapshot {
//!     tables: vec![table_schema("user")],
//!     edges: vec![],
//! };
//! let recorded = SchemaSnapshot::new();
//! let report = check_schema_drift_from_snapshots(&code, &recorded);
//! assert!(report.drift_detected);
//! ```

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicBool, Ordering};

use serde::{Deserialize, Serialize};

use crate::error::{Result, SurqlError};
use crate::migration::diff::{diff_schemas, SchemaSnapshot};
use crate::migration::models::{DiffOperation, SchemaDiff};
use crate::migration::versioning::{
    create_snapshot, list_snapshots, store_snapshot, VersionedSnapshot,
};
use crate::schema::registry::SchemaRegistry;

/// Severity of a single drift issue.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DriftSeverity {
    /// Additive change (e.g. new table, new field, new index).
    Info,
    /// Non-destructive modification (e.g. field type change).
    Warning,
    /// Destructive change (e.g. dropped table or field).
    Critical,
}

impl DriftSeverity {
    /// Render the severity as a lowercase string.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Info => "info",
            Self::Warning => "warning",
            Self::Critical => "critical",
        }
    }
}

impl std::fmt::Display for DriftSeverity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Classify a [`DiffOperation`] as a [`DriftSeverity`].
#[must_use]
pub fn severity_for_operation(op: DiffOperation) -> DriftSeverity {
    match op {
        DiffOperation::AddTable
        | DiffOperation::AddField
        | DiffOperation::AddIndex
        | DiffOperation::AddEvent => DriftSeverity::Info,
        DiffOperation::ModifyField
        | DiffOperation::ModifyPermissions
        | DiffOperation::DropEvent => DriftSeverity::Warning,
        DiffOperation::DropTable | DiffOperation::DropField | DiffOperation::DropIndex => {
            DriftSeverity::Critical
        }
    }
}

/// A single drift issue derived from one [`SchemaDiff`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DriftIssue {
    /// Severity of this issue.
    pub severity: DriftSeverity,
    /// The underlying diff operation.
    pub operation: DiffOperation,
    /// Table affected by the change.
    pub table: String,
    /// Field affected, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub field: Option<String>,
    /// Human-readable description.
    pub description: String,
}

impl DriftIssue {
    /// Construct an issue from a [`SchemaDiff`] using [`severity_for_operation`].
    #[must_use]
    pub fn from_diff(diff: &SchemaDiff) -> Self {
        Self {
            severity: severity_for_operation(diff.operation),
            operation: diff.operation,
            table: diff.table.clone(),
            field: diff.field.clone(),
            description: diff.description.clone(),
        }
    }
}

/// Structured drift report returned by the `check_schema_drift*` helpers.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DriftReport {
    /// `true` if any drift issues were detected.
    pub drift_detected: bool,
    /// One issue per underlying [`SchemaDiff`].
    pub issues: Vec<DriftIssue>,
    /// Suggested `surql` CLI invocation to create a migration, or [`None`]
    /// if no drift was detected.
    pub suggested_migration: Option<String>,
}

impl DriftReport {
    /// Build an empty (no-drift) report.
    #[must_use]
    pub fn empty() -> Self {
        Self::default()
    }

    /// Construct a report from a slice of [`SchemaDiff`] entries.
    #[must_use]
    pub fn from_diffs(diffs: &[SchemaDiff]) -> Self {
        if diffs.is_empty() {
            return Self::empty();
        }
        let issues: Vec<DriftIssue> = diffs.iter().map(DriftIssue::from_diff).collect();
        let suggested =
            Some("surql schema generate -s <schema-file> -m '<description>'".to_string());
        Self {
            drift_detected: true,
            issues,
            suggested_migration: suggested,
        }
    }

    /// Count issues at [`DriftSeverity::Critical`].
    #[must_use]
    pub fn critical_count(&self) -> usize {
        self.issues
            .iter()
            .filter(|i| i.severity == DriftSeverity::Critical)
            .count()
    }

    /// Render the report as a human-readable multi-line summary.
    #[must_use]
    pub fn to_summary(&self) -> String {
        if !self.drift_detected {
            return "No schema drift detected.".to_string();
        }
        let mut lines: Vec<String> = Vec::with_capacity(self.issues.len() + 2);
        lines.push(format!(
            "Schema drift detected ({} issue{}):",
            self.issues.len(),
            if self.issues.len() == 1 { "" } else { "s" }
        ));
        for issue in &self.issues {
            let field_part = issue
                .field
                .as_ref()
                .map_or(String::new(), |f| format!(".{f}"));
            lines.push(format!(
                "  [{severity}] {op:?} {table}{field}: {desc}",
                severity = issue.severity,
                op = issue.operation,
                table = issue.table,
                field = field_part,
                desc = issue.description,
            ));
        }
        if let Some(cmd) = &self.suggested_migration {
            lines.push(format!("Suggested: {cmd}"));
        }
        lines.join("\n")
    }
}

/// Compute a [`DriftReport`] from a pair of [`SchemaSnapshot`]s.
///
/// Delegates to [`diff_schemas`] and wraps every returned [`SchemaDiff`]
/// in a [`DriftIssue`]. Returns an empty report when the snapshots are
/// structurally identical.
#[must_use]
pub fn check_schema_drift_from_snapshots(
    code: &SchemaSnapshot,
    recorded: &SchemaSnapshot,
) -> DriftReport {
    let diffs = diff_schemas(code, recorded);
    DriftReport::from_diffs(&diffs)
}

/// Compute a [`DriftReport`] by comparing a code-side [`SchemaRegistry`]
/// against the latest snapshot stored under `snapshots_dir`.
///
/// If `snapshots_dir` is [`None`] or contains no snapshots, the recorded
/// snapshot is treated as empty. This mirrors the Python behaviour of
/// returning "all tables are new" drift when no migrations have been
/// generated yet.
///
/// The `_migrations_dir` parameter is accepted for signature-parity with
/// the Python implementation; the Rust port derives the recorded snapshot
/// solely from the versioned snapshot files in `snapshots_dir`.
///
/// # Errors
///
/// Returns [`SurqlError::MigrationHistory`] when `snapshots_dir` exists
/// but cannot be enumerated, or [`SurqlError::Io`] when a snapshot file
/// cannot be read.
pub fn check_schema_drift(
    registry: &SchemaRegistry,
    snapshots_dir: Option<&Path>,
    _migrations_dir: Option<&Path>,
) -> Result<DriftReport> {
    let code_snapshot = registry_to_snapshot(registry);
    let recorded_snapshot = match snapshots_dir {
        Some(dir) if dir.exists() => {
            latest_snapshot(dir)?.map_or_else(SchemaSnapshot::new, |s| versioned_to_snapshot(&s))
        }
        _ => SchemaSnapshot::new(),
    };
    Ok(check_schema_drift_from_snapshots(
        &code_snapshot,
        &recorded_snapshot,
    ))
}

/// Convert a [`SchemaRegistry`] into a [`SchemaSnapshot`].
#[must_use]
pub fn registry_to_snapshot(registry: &SchemaRegistry) -> SchemaSnapshot {
    SchemaSnapshot {
        tables: registry.tables().into_values().collect(),
        edges: registry.edges().into_values().collect(),
    }
}

/// Convert a [`VersionedSnapshot`] into a [`SchemaSnapshot`].
#[must_use]
pub fn versioned_to_snapshot(snapshot: &VersionedSnapshot) -> SchemaSnapshot {
    SchemaSnapshot {
        tables: snapshot.tables.values().cloned().collect(),
        edges: snapshot.edges.values().cloned().collect(),
    }
}

fn latest_snapshot(dir: &Path) -> Result<Option<VersionedSnapshot>> {
    let mut snaps = list_snapshots(dir)?;
    if snaps.is_empty() {
        return Ok(None);
    }
    // `list_snapshots` sorts ascending by version; take the last.
    Ok(snaps.pop())
}

// ---------------------------------------------------------------------------
// Staged file discovery (via `git diff --cached`)
// ---------------------------------------------------------------------------

/// Default predicate used by [`get_staged_schema_files`]: accepts paths
/// whose final extension is `.rs` or `.surql`.
#[must_use]
pub fn default_schema_filter(path: &Path) -> bool {
    matches!(
        path.extension().and_then(|e| e.to_str()),
        Some("rs" | "surql")
    )
}

/// Return the list of files currently staged in git under `schema_dir`.
///
/// Runs `git diff --cached --name-only --diff-filter=ACMR --relative`
/// with `schema_dir` as the current working directory. The `--relative`
/// flag makes git scope output to `schema_dir` and emit paths relative
/// to it, which matches the filtered view the caller wants.
///
/// The `filter` closure decides which of those relative paths to include.
/// If `schema_dir` does not exist, an empty vector is returned.
///
/// # Errors
///
/// Returns [`SurqlError::Io`] if the `git` binary cannot be invoked at
/// the process level. A non-zero exit from `git` is not treated as an
/// error: an empty list is returned instead (matching the Python
/// behaviour of "no repo = no staged files").
pub fn get_staged_schema_files<F>(schema_dir: &Path, filter: F) -> Result<Vec<PathBuf>>
where
    F: Fn(&Path) -> bool,
{
    if !schema_dir.exists() {
        return Ok(Vec::new());
    }

    let cwd = if schema_dir.is_file() {
        schema_dir.parent().unwrap_or(schema_dir)
    } else {
        schema_dir
    };

    let output = Command::new("git")
        .args([
            "diff",
            "--cached",
            "--name-only",
            "--diff-filter=ACMR",
            "--relative",
        ])
        .current_dir(cwd)
        .output()
        .map_err(|e| SurqlError::Io {
            reason: format!("failed to invoke git: {e}"),
        })?;

    if !output.status.success() {
        // Not a git repo, or some other failure; mirror Python and return
        // an empty list rather than surfacing a hard error.
        return Ok(Vec::new());
    }

    let stdout = String::from_utf8_lossy(&output.stdout);

    let mut staged: Vec<PathBuf> = Vec::new();
    for line in stdout.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let path = PathBuf::from(trimmed);
        if !filter(&path) {
            continue;
        }
        staged.push(path);
    }

    Ok(staged)
}

// ---------------------------------------------------------------------------
// Pre-commit config snippet
// ---------------------------------------------------------------------------

/// Render a `.pre-commit-config.yaml` snippet that wires the `surql`
/// schema-check CLI into a pre-commit hook.
///
/// The returned string is a valid YAML document; the caller is
/// responsible for writing it to disk or merging it into an existing
/// config.
///
/// ## Examples
///
/// ```
/// use surql::migration::hooks::generate_precommit_config;
///
/// let yaml = generate_precommit_config("schemas/", true);
/// assert!(yaml.starts_with("repos:"));
/// assert!(yaml.contains("surql-schema-check"));
/// ```
#[must_use]
pub fn generate_precommit_config(schema_path: &str, fail_on_drift: bool) -> String {
    let fail_flag = if fail_on_drift {
        " --fail-on-drift"
    } else {
        ""
    };
    format!(
        "repos:\n  - repo: local\n    hooks:\n      - id: surql-schema-check\n        name: Check schema migrations\n        entry: surql schema check --schema {schema_path}{fail_flag}\n        language: system\n        pass_filenames: false\n"
    )
}

// ---------------------------------------------------------------------------
// Auto-snapshot hooks (parity with `surql/migration/hooks.py`)
// ---------------------------------------------------------------------------

/// Global toggle for automatic post-migration snapshots.
///
/// Mirrors the Python `AUTO_SNAPSHOT_ENABLED` module-level boolean. The
/// toggle lives in the always-on [`hooks`](self) module so it can be
/// read from both client-gated (history/executor) and pure (watcher,
/// squash) call sites.
static AUTO_SNAPSHOT_ENABLED: AtomicBool = AtomicBool::new(false);

/// Enable automatic schema snapshots after successful migrations.
///
/// Subsequent calls to [`create_snapshot_on_migration`] will take a
/// snapshot; callers that honour the flag (e.g. the client-gated
/// migration executor) will start taking snapshots on apply.
pub fn enable_auto_snapshots() {
    AUTO_SNAPSHOT_ENABLED.store(true, Ordering::Relaxed);
}

/// Disable automatic schema snapshots.
pub fn disable_auto_snapshots() {
    AUTO_SNAPSHOT_ENABLED.store(false, Ordering::Relaxed);
}

/// `true` when automatic snapshots are enabled.
#[must_use]
pub fn is_auto_snapshot_enabled() -> bool {
    AUTO_SNAPSHOT_ENABLED.load(Ordering::Relaxed)
}

/// Callback run immediately before the snapshot is taken; receives the
/// migration version that triggered the snapshot.
pub type PreSnapshotHook<'a> = Box<dyn FnOnce(&str) + 'a>;
/// Callback run after the snapshot has been stored; receives a reference
/// to the stored [`VersionedSnapshot`].
pub type PostSnapshotHook<'a> = Box<dyn FnOnce(&VersionedSnapshot) + 'a>;

/// Hook invoked around [`create_snapshot_on_migration`].
///
/// The `pre` hook runs before the snapshot is created; the `post` hook
/// runs after a successful store with the resulting [`VersionedSnapshot`].
/// Either hook may be [`None`]. Hooks are `FnOnce` so they can capture
/// state by move.
pub struct SnapshotHooks<'a> {
    /// Callback run immediately before creating the snapshot. Receives
    /// the migration version that triggered the snapshot.
    pub pre: Option<PreSnapshotHook<'a>>,
    /// Callback run after the snapshot has been stored. Receives a
    /// reference to the stored [`VersionedSnapshot`].
    pub post: Option<PostSnapshotHook<'a>>,
}

impl<'a> SnapshotHooks<'a> {
    /// Construct a hook pair with no pre- or post-callback.
    #[must_use]
    pub fn none() -> Self {
        Self {
            pre: None,
            post: None,
        }
    }

    /// Attach a pre-snapshot callback.
    #[must_use]
    pub fn pre<F>(mut self, f: F) -> Self
    where
        F: FnOnce(&str) + 'a,
    {
        self.pre = Some(Box::new(f));
        self
    }

    /// Attach a post-snapshot callback.
    #[must_use]
    pub fn post<F>(mut self, f: F) -> Self
    where
        F: FnOnce(&VersionedSnapshot) + 'a,
    {
        self.post = Some(Box::new(f));
        self
    }
}

impl Default for SnapshotHooks<'_> {
    fn default() -> Self {
        Self::none()
    }
}

/// Create and persist a schema snapshot on behalf of a just-applied
/// migration.
///
/// Honours [`is_auto_snapshot_enabled`]: when the flag is `false` the
/// function is a no-op and returns `Ok(None)`. When enabled it captures
/// the current [`SchemaRegistry`] state via
/// [`create_snapshot`] and persists it to `snapshots_dir` via
/// [`store_snapshot`].
///
/// `migration_count` is stored on the snapshot for later inspection and
/// matches the Python signature.
///
/// `hooks.pre` runs before the snapshot is created; `hooks.post` runs
/// after a successful store. Hooks are best-effort: they must not
/// panic, and their execution is not reported through the returned
/// `Result` (errors are swallowed by the hook closure itself).
///
/// # Errors
///
/// Returns [`SurqlError::Validation`] if `version` is empty (surfaced
/// from [`create_snapshot`]), or [`SurqlError::Io`] /
/// [`SurqlError::Serialization`] if the snapshot cannot be written.
pub fn create_snapshot_on_migration(
    registry: &SchemaRegistry,
    snapshots_dir: &Path,
    version: &str,
    migration_count: u64,
    hooks: SnapshotHooks<'_>,
) -> Result<Option<VersionedSnapshot>> {
    if !is_auto_snapshot_enabled() {
        return Ok(None);
    }

    if let Some(pre) = hooks.pre {
        pre(version);
    }

    let mut snapshot = create_snapshot(registry, version, format!("auto: {version}"))?;
    snapshot.migration_count = migration_count;
    store_snapshot(&snapshot, snapshots_dir)?;

    if let Some(post) = hooks.post {
        post(&snapshot);
    }

    Ok(Some(snapshot))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
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
    fn init_git_repo(dir: &Path) -> bool {
        let Ok(status) = Command::new("git")
            .args(["init", "-q"])
            .current_dir(dir)
            .status()
        else {
            return false;
        };
        if !status.success() {
            return false;
        }
        let _ = Command::new("git")
            .args(["config", "user.email", "test@example.com"])
            .current_dir(dir)
            .status();
        let _ = Command::new("git")
            .args(["config", "user.name", "surql-test"])
            .current_dir(dir)
            .status();
        true
    }

    fn git_add(dir: &Path, relpath: &str) -> bool {
        Command::new("git")
            .args(["add", "--", relpath])
            .current_dir(dir)
            .status()
            .is_ok_and(|s| s.success())
    }

    fn make_diff(op: DiffOperation, table: &str, field: Option<&str>, desc: &str) -> SchemaDiff {
        SchemaDiff {
            operation: op,
            table: table.to_string(),
            field: field.map(ToString::to_string),
            index: None,
            event: None,
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
        assert!(r.issues.is_empty());
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
        let one =
            DriftReport::from_diffs(&[make_diff(DiffOperation::AddTable, "user", None, "add")]);
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
        };
        let report = check_schema_drift_from_snapshots(&snap, &snap);
        assert!(!report.drift_detected);
        assert!(report.issues.is_empty());
    }

    #[test]
    fn drift_from_snapshots_detects_new_table() {
        let code = SchemaSnapshot {
            tables: vec![table_schema("user")],
            edges: vec![],
        };
        let recorded = SchemaSnapshot::new();
        let report = check_schema_drift_from_snapshots(&code, &recorded);
        assert!(report.drift_detected);
        assert!(!report.issues.is_empty());
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
        let report =
            check_schema_drift(&registry, None, None).expect("check_schema_drift succeeds");
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
        let report = check_schema_drift(&registry, Some(&missing), None)
            .expect("check_schema_drift succeeds");
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
        assert!(files.is_empty());
    }

    #[test]
    fn staged_returns_empty_outside_git_repo() {
        let dir = unique_temp_dir("no-git");
        // No `git init` here; call should gracefully return empty.
        let files = get_staged_schema_files(&dir, default_schema_filter)
            .expect("get_staged_schema_files succeeds");
        assert!(files.is_empty());
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
        assert!(files.is_empty());
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
        let md_files = get_staged_schema_files(&schema_subdir, md_only)
            .expect("get_staged_schema_files succeeds");
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
        assert!(!yaml.is_empty());
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
}
