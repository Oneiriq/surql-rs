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
//!     ..Default::default()
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
use crate::migration::discovery::compare_versions;
use crate::migration::models::{DiffOperation, SchemaDiff};
use crate::migration::versioning::{
    create_snapshot, load_snapshot, store_snapshot, VersionedSnapshot,
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
        | DiffOperation::AddEvent
        | DiffOperation::AddAnalyzer
        | DiffOperation::AddBucket
        | DiffOperation::AddSequence
        | DiffOperation::AddFunction
        | DiffOperation::AddParam => DriftSeverity::Info,
        DiffOperation::ModifyField
        | DiffOperation::ModifyTable
        | DiffOperation::ModifyIndex
        | DiffOperation::ModifyEvent
        | DiffOperation::ModifyPermissions
        | DiffOperation::DropEvent
        | DiffOperation::ModifyAnalyzer
        | DiffOperation::ModifyBucket
        | DiffOperation::ModifySequence
        | DiffOperation::ModifyFunction
        | DiffOperation::ModifyParam => DriftSeverity::Warning,
        DiffOperation::DropTable
        | DiffOperation::DropField
        | DiffOperation::DropIndex
        | DiffOperation::DropAnalyzer
        | DiffOperation::DropBucket
        | DiffOperation::DropSequence
        | DiffOperation::DropFunction
        | DiffOperation::DropParam => DriftSeverity::Critical,
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
/// but cannot be enumerated, [`SurqlError::Io`] when the newest snapshot
/// file cannot be read, or [`SurqlError::Serialization`] when it is not a
/// valid snapshot.
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
        buckets: registry.buckets().into_values().collect(),
        ..Default::default()
    }
}

/// Convert a [`VersionedSnapshot`] into a [`SchemaSnapshot`].
#[must_use]
pub fn versioned_to_snapshot(snapshot: &VersionedSnapshot) -> SchemaSnapshot {
    SchemaSnapshot {
        tables: snapshot.tables.values().cloned().collect(),
        edges: snapshot.edges.values().cloned().collect(),
        buckets: snapshot.buckets.values().cloned().collect(),
        ..Default::default()
    }
}

/// The newest snapshot in `dir`, by the version in its file name.
///
/// The newest file is loaded strictly: a corrupt newest snapshot is an
/// error, not a silent fall-back to an older baseline (which would report
/// drift against the wrong schema).
fn latest_snapshot(dir: &Path) -> Result<Option<VersionedSnapshot>> {
    let entries = std::fs::read_dir(dir).map_err(|e| SurqlError::MigrationHistory {
        reason: format!("failed to read snapshot directory {}: {e}", dir.display()),
    })?;
    let newest = entries
        .filter_map(std::result::Result::ok)
        .map(|entry| entry.path())
        .filter(|path| {
            path.extension()
                .and_then(|ext| ext.to_str())
                .is_some_and(|ext| ext.eq_ignore_ascii_case(VersionedSnapshot::FILE_EXTENSION))
        })
        .filter_map(|path| {
            let version = path.file_stem()?.to_str()?.to_owned();
            Some((version, path))
        })
        .max_by(|(a, _), (b, _)| compare_versions(a, b));
    newest.map(|(_, path)| load_snapshot(&path)).transpose()
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
/// Runs `git diff --cached --name-only --diff-filter=ACMR --relative -z`
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

    // Strip inherited git env vars so `current_dir(cwd)` actually picks
    // up the repo rooted at `cwd`. Without this, callers invoked from
    // inside a git hook (where `git` exports `GIT_DIR` /
    // `GIT_WORK_TREE` / `GIT_INDEX_FILE` for child processes) would
    // accidentally read the outer repo's index. This also makes our
    // own `migration::hooks` tests deterministic when run under
    // `git push -> pre-push -> cargo test`.
    // `-z` separates paths with NUL and turns off `core.quotePath`
    // quoting, which otherwise wraps any path with a non-ASCII or special
    // character in quotes and octal escapes (and the extension check then
    // sees `surql"`).
    let output = Command::new("git")
        .args([
            "diff",
            "--cached",
            "--name-only",
            "--diff-filter=ACMR",
            "--relative",
            "-z",
        ])
        .current_dir(cwd)
        .env_remove("GIT_DIR")
        .env_remove("GIT_WORK_TREE")
        .env_remove("GIT_INDEX_FILE")
        .env_remove("GIT_OBJECT_DIRECTORY")
        .env_remove("GIT_COMMON_DIR")
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
    for name in stdout.split('\0') {
        if name.is_empty() {
            continue;
        }
        let path = PathBuf::from(name);
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
mod tests;
