//! Migration squashing for combining multiple migrations into one.
//!
//! Port of `surql/migration/squash.py`. Provides functionality to combine
//! a contiguous range of migration files into a single consolidated
//! migration with merged `UP`/`DOWN` statements, optional optimisation of
//! redundant operations, and safety warnings for data-manipulation
//! statements.
//!
//! ## Deviation from Python
//!
//! * Python emits a `.py` migration file with a `metadata` dict plus
//!   `up()` / `down()` callables. The Rust port writes a `.surql` file
//!   that follows the same grammar as
//!   [`crate::migration::discovery`]: `-- @metadata`, `-- @up`, `-- @down`
//!   section markers plus a `-- @squashed-from:` metadata key listing the
//!   original versions.
//! * Python's `squash_migrations` is `async`; the Rust implementation is
//!   fully synchronous because no I/O needs `tokio`.
//! * The Python port's `down()` is always empty; we preserve that
//!   behaviour and additionally emit a comment explaining why.
//!
//! ## Example
//!
//! ```no_run
//! use std::path::Path;
//! use surql::migration::squash::{squash_migrations, SquashOptions};
//!
//! let result = squash_migrations(
//!     Path::new("migrations"),
//!     &SquashOptions::new().from_version("20260101_000000").dry_run(true),
//! )
//! .unwrap();
//! assert!(result.original_count >= 2);
//! ```

use std::collections::HashSet;
use std::fmt::Write as _;
use std::fs;
use std::path::{Path, PathBuf};

use chrono::Utc;

use crate::error::{Result, SurqlError};
use crate::migration::discovery::{discover_migrations, sha2_lite};
use crate::migration::models::Migration;

/// Error raised by the squash subsystem.
///
/// Wrapper over [`SurqlError::MigrationSquash`]; re-exported for API
/// parity with the Python `SquashError` class.
pub type SquashError = SurqlError;

/// Severity of a [`SquashWarning`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SquashSeverity {
    /// Benign note; squash proceeds.
    Low,
    /// Data-modifying statement; squash proceeds but warns.
    Medium,
    /// Destructive statement (e.g. `DELETE`); squash refuses unless
    /// `force` is set.
    High,
}

impl SquashSeverity {
    /// Render the severity as a lowercase string.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Low => "low",
            Self::Medium => "medium",
            Self::High => "high",
        }
    }
}

impl std::fmt::Display for SquashSeverity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Warning about a potential issue detected during squash validation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SquashWarning {
    /// Version of the migration that triggered this warning.
    pub migration: String,
    /// Human-readable message.
    pub message: String,
    /// Severity of the warning.
    pub severity: SquashSeverity,
}

/// Outcome of a successful squash.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SquashResult {
    /// Path to the squashed migration file (whether written or not).
    pub squashed_path: PathBuf,
    /// Number of source migrations combined.
    pub original_count: usize,
    /// Number of `UP` statements in the squashed output.
    pub statement_count: usize,
    /// Number of statement-level optimisations applied.
    pub optimizations_applied: usize,
    /// Ordered list of source migration versions.
    pub original_migrations: Vec<String>,
}

/// Behavioural options for [`squash_migrations`].
///
/// Constructed via [`SquashOptions::new`] and fluent setters. All fields
/// are `pub` so callers can also build one via a struct literal if they
/// prefer.
#[derive(Debug, Clone, Default)]
pub struct SquashOptions {
    /// Start version (inclusive). `None` for the very first migration.
    pub from_version: Option<String>,
    /// End version (inclusive). `None` for the very last migration.
    pub to_version: Option<String>,
    /// Explicit output path. `None` to auto-name based on the timestamp.
    pub output_path: Option<PathBuf>,
    /// Apply the statement-level optimiser.
    pub optimize: bool,
    /// When `true`, compute the result but do not write the output file.
    pub dry_run: bool,
    /// When `true`, high-severity warnings do not abort the squash.
    pub force: bool,
}

impl SquashOptions {
    /// Construct a default [`SquashOptions`] (optimise on, dry-run off,
    /// force off, no version bounds, auto-named output).
    #[must_use]
    pub fn new() -> Self {
        Self {
            optimize: true,
            ..Self::default()
        }
    }

    /// Set the start version (inclusive).
    #[must_use]
    pub fn from_version(mut self, version: impl Into<String>) -> Self {
        self.from_version = Some(version.into());
        self
    }

    /// Set the end version (inclusive).
    #[must_use]
    pub fn to_version(mut self, version: impl Into<String>) -> Self {
        self.to_version = Some(version.into());
        self
    }

    /// Set an explicit output path for the squashed migration file.
    #[must_use]
    pub fn output_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.output_path = Some(path.into());
        self
    }

    /// Enable or disable the statement-level optimiser (default: on).
    #[must_use]
    pub fn optimize(mut self, enabled: bool) -> Self {
        self.optimize = enabled;
        self
    }

    /// Toggle dry-run mode (no file is written).
    #[must_use]
    pub fn dry_run(mut self, enabled: bool) -> Self {
        self.dry_run = enabled;
        self
    }

    /// Allow high-severity warnings without aborting.
    #[must_use]
    pub fn force(mut self, enabled: bool) -> Self {
        self.force = enabled;
        self
    }
}

// ---------------------------------------------------------------------------
// Parsed statements (statement-level optimiser)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum Operation {
    Define,
    Remove,
    Insert,
    Update,
    Delete,
    Create,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum ObjectType {
    Table,
    Field,
    Index,
    Event,
}

#[derive(Debug, Clone)]
struct ParsedStatement {
    statement: String,
    operation: Operation,
    object_type: Option<ObjectType>,
    table_name: Option<String>,
    field_name: Option<String>,
    index_name: Option<String>,
}

/// Composite key used to deduplicate repeated `DEFINE` statements.
type DefineKey = (
    Option<ObjectType>,
    Option<String>,
    Option<String>,
    Option<String>,
);

fn tokens(upper: &str) -> Vec<&str> {
    upper.split_whitespace().collect()
}

fn strip_trailing_punct(s: &str) -> &str {
    s.trim_end_matches(|c: char| !c.is_ascii_alphanumeric() && c != '_')
}

fn parse_statement(statement: &str) -> ParsedStatement {
    let original = statement.trim().to_string();
    let upper = original.to_ascii_uppercase();

    let op = if upper.starts_with("DEFINE") {
        Operation::Define
    } else if upper.starts_with("REMOVE") {
        Operation::Remove
    } else if upper.starts_with("INSERT") {
        Operation::Insert
    } else if upper.starts_with("UPDATE") {
        Operation::Update
    } else if upper.starts_with("DELETE") {
        Operation::Delete
    } else if upper.starts_with("CREATE") {
        Operation::Create
    } else {
        return ParsedStatement {
            statement: original,
            operation: Operation::Unknown,
            object_type: None,
            table_name: None,
            field_name: None,
            index_name: None,
        };
    };

    let mut object_type: Option<ObjectType> = None;
    let mut table_name: Option<String> = None;
    let mut field_name: Option<String> = None;
    let mut index_name: Option<String> = None;

    // Tokenise the uppercased form for simple pattern matching. Python
    // uses regexes with `IGNORECASE`; we use case-folded token walking
    // which is easier to audit and avoids adding regex deps here.
    let toks = tokens(&upper);
    if matches!(op, Operation::Define | Operation::Remove) && toks.len() >= 3 {
        // DEFINE/REMOVE <KIND> <NAME> [ON TABLE <table>]
        match toks[1] {
            "TABLE" => {
                object_type = Some(ObjectType::Table);
                table_name = Some(strip_trailing_punct(toks[2]).to_ascii_lowercase());
            }
            "FIELD" => {
                object_type = Some(ObjectType::Field);
                field_name = Some(strip_trailing_punct(toks[2]).to_ascii_lowercase());
                if let Some(table) = extract_on_table(&toks) {
                    table_name = Some(table);
                }
            }
            "INDEX" => {
                object_type = Some(ObjectType::Index);
                index_name = Some(strip_trailing_punct(toks[2]).to_ascii_lowercase());
                if let Some(table) = extract_on_table(&toks) {
                    table_name = Some(table);
                }
            }
            "EVENT" => {
                object_type = Some(ObjectType::Event);
                // Events reuse index_name for the identifier slot, matching py.
                index_name = Some(strip_trailing_punct(toks[2]).to_ascii_lowercase());
                if let Some(table) = extract_on_table(&toks) {
                    table_name = Some(table);
                }
            }
            _ => {}
        }
    }

    ParsedStatement {
        statement: original,
        operation: op,
        object_type,
        table_name,
        field_name,
        index_name,
    }
}

fn extract_on_table(toks: &[&str]) -> Option<String> {
    for (i, tok) in toks.iter().enumerate() {
        if *tok == "ON" && i + 2 < toks.len() && toks[i + 1] == "TABLE" && !toks[i + 2].is_empty() {
            return Some(strip_trailing_punct(toks[i + 2]).to_ascii_lowercase());
        }
    }
    None
}

/// Remove redundant SurrealQL statements from a list.
///
/// Applies three passes:
///
/// 1. Drop `DEFINE` + matching `REMOVE` pairs for the same object.
/// 2. When the same object is defined more than once, drop all earlier
///    definitions and keep the last (mirrors Python behaviour).
/// 3. Drop `UPDATE` statements that reference a field whose `DEFINE` and
///    `REMOVE` have both been dropped in pass 1 (orphaned data migrations).
///
/// Returns the optimised list and the count of individual statements
/// that were elided.
#[must_use]
pub fn optimize_statements(statements: &[String]) -> (Vec<String>, usize) {
    let parsed: Vec<ParsedStatement> = statements
        .iter()
        .map(|s| parse_statement(s.as_str()))
        .collect();

    let mut to_remove: HashSet<usize> = HashSet::new();
    let mut optimisations: usize = 0;

    pass_drop_define_remove_pairs(&parsed, &mut to_remove, &mut optimisations);
    pass_drop_duplicate_defines(&parsed, &mut to_remove, &mut optimisations);
    pass_drop_orphaned_updates(&parsed, &mut to_remove, &mut optimisations);

    let optimised: Vec<String> = statements
        .iter()
        .enumerate()
        .filter_map(|(i, s)| {
            if to_remove.contains(&i) {
                None
            } else {
                Some(s.clone())
            }
        })
        .collect();
    (optimised, optimisations)
}

fn object_pair_matches(a: &ParsedStatement, b: &ParsedStatement) -> bool {
    if a.object_type != b.object_type || a.table_name != b.table_name {
        return false;
    }
    match &a.object_type {
        Some(ObjectType::Table) => true,
        Some(ObjectType::Field) => a.field_name == b.field_name,
        Some(ObjectType::Index | ObjectType::Event) => a.index_name == b.index_name,
        None => false,
    }
}

fn pass_drop_define_remove_pairs(
    parsed: &[ParsedStatement],
    to_remove: &mut HashSet<usize>,
    optimisations: &mut usize,
) {
    for i in 0..parsed.len() {
        if to_remove.contains(&i) || parsed[i].operation != Operation::Define {
            continue;
        }
        for j in (i + 1)..parsed.len() {
            if to_remove.contains(&j) || parsed[j].operation != Operation::Remove {
                continue;
            }
            if object_pair_matches(&parsed[i], &parsed[j]) {
                to_remove.insert(i);
                to_remove.insert(j);
                *optimisations += 2;
                break;
            }
        }
    }
}

fn pass_drop_duplicate_defines(
    parsed: &[ParsedStatement],
    to_remove: &mut HashSet<usize>,
    optimisations: &mut usize,
) {
    let mut last_define_idx: std::collections::HashMap<DefineKey, usize> =
        std::collections::HashMap::new();

    for (i, stmt) in parsed.iter().enumerate() {
        if to_remove.contains(&i) || stmt.operation != Operation::Define {
            continue;
        }
        let key: DefineKey = (
            stmt.object_type.clone(),
            stmt.table_name.clone(),
            stmt.field_name.clone(),
            stmt.index_name.clone(),
        );
        if let Some(&earlier) = last_define_idx.get(&key) {
            if !to_remove.contains(&earlier) {
                to_remove.insert(earlier);
                *optimisations += 1;
            }
        }
        last_define_idx.insert(key, i);
    }
}

fn pass_drop_orphaned_updates(
    parsed: &[ParsedStatement],
    to_remove: &mut HashSet<usize>,
    optimisations: &mut usize,
) {
    let mut removed_fields: HashSet<(Option<String>, Option<String>)> = HashSet::new();
    for i in to_remove.iter() {
        let s = &parsed[*i];
        if matches!(s.object_type, Some(ObjectType::Field)) {
            removed_fields.insert((s.table_name.clone(), s.field_name.clone()));
        }
    }

    for (i, stmt) in parsed.iter().enumerate() {
        if to_remove.contains(&i) || stmt.operation != Operation::Update {
            continue;
        }
        let upper = stmt.statement.to_ascii_uppercase();
        for (table, field) in &removed_fields {
            let (Some(table), Some(field)) = (table.as_ref(), field.as_ref()) else {
                continue;
            };
            let set_token = format!("SET {}", field.to_ascii_uppercase());
            let tab_token = format!("UPDATE {}", table.to_ascii_uppercase());
            if upper.contains(&set_token) && upper.contains(&tab_token) {
                to_remove.insert(i);
                *optimisations += 1;
                break;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Safety validation
// ---------------------------------------------------------------------------

/// Inspect `migrations` and return a list of warnings about
/// data-manipulation statements, ordering issues, and other known
/// squash hazards.
#[must_use]
pub fn validate_squash_safety(migrations: &[Migration]) -> Vec<SquashWarning> {
    let mut warnings: Vec<SquashWarning> = Vec::new();

    for migration in migrations {
        let version = &migration.version;
        for stmt in &migration.up {
            let upper = stmt.to_ascii_uppercase();
            let preview = preview_statement(stmt);

            if upper.trim_start().starts_with("INSERT") {
                warnings.push(SquashWarning {
                    migration: version.clone(),
                    message: format!("Contains INSERT statement: {preview}..."),
                    severity: SquashSeverity::Medium,
                });
            } else if upper.trim_start().starts_with("UPDATE") && upper.contains(" SET ") {
                if !upper.contains(" IS NONE") {
                    warnings.push(SquashWarning {
                        migration: version.clone(),
                        message: format!("Contains UPDATE statement: {preview}..."),
                        severity: SquashSeverity::Medium,
                    });
                }
            } else if upper.trim_start().starts_with("DELETE") {
                warnings.push(SquashWarning {
                    migration: version.clone(),
                    message: format!("Contains DELETE statement: {preview}..."),
                    severity: SquashSeverity::High,
                });
            } else if upper.trim_start().starts_with("CREATE") && !upper.contains("CREATE TABLE") {
                warnings.push(SquashWarning {
                    migration: version.clone(),
                    message: format!("Contains CREATE statement: {preview}..."),
                    severity: SquashSeverity::Low,
                });
            }

            if upper.contains("RECORD") && upper.contains("TYPE") {
                warnings.push(SquashWarning {
                    migration: version.clone(),
                    message: "Contains record reference - verify table order".to_string(),
                    severity: SquashSeverity::Low,
                });
            }
        }
    }

    warnings
}

fn preview_statement(stmt: &str) -> String {
    let trimmed = stmt.trim();
    if trimmed.len() > 50 {
        trimmed[..50].to_string()
    } else {
        trimmed.to_string()
    }
}

// ---------------------------------------------------------------------------
// File content generation
// ---------------------------------------------------------------------------

/// Render the `.surql` file body for a squashed migration.
///
/// Produces a file that conforms to
/// [`crate::migration::discovery`]'s grammar: a `-- @metadata` section,
/// an `-- @up` section containing the merged statements, and a stub
/// `-- @down` section explaining that rollback is unsupported.
///
/// `original_migrations` is rendered into a `-- @squashed-from:`
/// metadata key for documentation.
#[must_use]
pub fn generate_squashed_migration_content(
    statements: &[String],
    version: &str,
    description: &str,
    original_migrations: &[String],
) -> String {
    let now = Utc::now();
    let mut buf = String::new();

    buf.push_str("-- @metadata\n");
    let _ = writeln!(buf, "-- version: {version}");
    let _ = writeln!(buf, "-- description: {description}");
    buf.push_str("-- author: surql\n");
    if !original_migrations.is_empty() {
        let _ = writeln!(buf, "-- squashed-from: {}", original_migrations.join(","));
    }
    let _ = writeln!(buf, "-- generated_at: {}", now.to_rfc3339());

    buf.push_str("-- @up\n");
    if statements.is_empty() {
        buf.push_str("-- (no statements)\n");
    } else {
        for stmt in statements {
            let stmt = stmt.trim();
            if stmt.is_empty() {
                continue;
            }
            buf.push_str(stmt);
            if !stmt.ends_with(';') {
                buf.push(';');
            }
            buf.push('\n');
        }
    }

    buf.push_str("-- @down\n");
    buf.push_str("-- NOTE: squashed migrations do not emit a backward statement list.\n");
    buf.push_str("-- Restore from the snapshot corresponding to the pre-squash version.\n");

    buf
}

// ---------------------------------------------------------------------------
// Top-level entry point
// ---------------------------------------------------------------------------

/// Squash a contiguous range of migrations into a single file.
///
/// # Errors
///
/// Returns [`SurqlError::MigrationSquash`] when:
///
/// * the migrations directory is missing or contains no migrations;
/// * fewer than two migrations match the version range;
/// * high-severity warnings are detected and `force` is `false`.
///
/// Other variants (`MigrationDiscovery`, `Io`) may be returned from the
/// underlying discovery / filesystem layer.
pub fn squash_migrations(directory: &Path, opts: &SquashOptions) -> Result<SquashResult> {
    let all_migrations =
        discover_migrations(directory).map_err(|e| SurqlError::MigrationSquash {
            reason: format!("failed to discover migrations: {e}"),
        })?;

    if all_migrations.is_empty() {
        return Err(SurqlError::MigrationSquash {
            reason: "No migrations found in directory".to_string(),
        });
    }

    let migrations = filter_migrations_by_version(
        &all_migrations,
        opts.from_version.as_deref(),
        opts.to_version.as_deref(),
    );

    if migrations.is_empty() {
        return Err(SurqlError::MigrationSquash {
            reason: "No migrations match the specified version range".to_string(),
        });
    }
    if migrations.len() < 2 {
        return Err(SurqlError::MigrationSquash {
            reason: "At least 2 migrations required for squashing".to_string(),
        });
    }

    let warnings = validate_squash_safety(&migrations);
    if !opts.force {
        let high: Vec<&SquashWarning> = warnings
            .iter()
            .filter(|w| w.severity == SquashSeverity::High)
            .collect();
        if !high.is_empty() {
            let msgs: Vec<&str> = high.iter().map(|w| w.message.as_str()).collect();
            return Err(SurqlError::MigrationSquash {
                reason: format!(
                    "High severity warnings prevent squashing: {}",
                    msgs.join("; ")
                ),
            });
        }
    }

    let statements: Vec<String> = migrations.iter().flat_map(|m| m.up.clone()).collect();
    let (statements, optimisations_applied) = if opts.optimize {
        optimize_statements(&statements)
    } else {
        (statements, 0_usize)
    };

    let original_versions: Vec<String> = migrations.iter().map(|m| m.version.clone()).collect();
    let description = describe_range(&original_versions);
    let version = Utc::now().format("%Y%m%d_%H%M%S").to_string();

    let content = generate_squashed_migration_content(
        &statements,
        &version,
        &description,
        &original_versions,
    );

    let output_path = opts
        .output_path
        .clone()
        .unwrap_or_else(|| directory.join(format!("{version}_{description}.surql")));

    if opts.dry_run {
        tracing::info!(
            target: "surql::migration::squash",
            version = %version,
            path = %output_path.display(),
            "dry_run_complete",
        );
    } else {
        persist_squashed_migration(&output_path, &content, &version)?;
    }

    Ok(SquashResult {
        squashed_path: output_path,
        original_count: migrations.len(),
        statement_count: statements.len(),
        optimizations_applied: optimisations_applied,
        original_migrations: original_versions,
    })
}

fn describe_range(versions: &[String]) -> String {
    let first = versions.first().map_or("unknown", String::as_str);
    let last = versions.last().map_or("unknown", String::as_str);
    format!("squashed_{first}_to_{last}")
}

fn persist_squashed_migration(output_path: &Path, content: &str, version: &str) -> Result<()> {
    if let Some(parent) = output_path.parent() {
        fs::create_dir_all(parent).map_err(|e| SurqlError::Io {
            reason: format!(
                "failed to create output directory {}: {e}",
                parent.display(),
            ),
        })?;
    }
    fs::write(output_path, content.as_bytes()).map_err(|e| SurqlError::Io {
        reason: format!(
            "failed to write squashed migration {}: {e}",
            output_path.display()
        ),
    })?;
    let checksum = sha2_lite::sha256_hex(content.as_bytes());
    tracing::info!(
        target: "surql::migration::squash",
        version = %version,
        path = %output_path.display(),
        checksum = %checksum,
        "squashed_migration_written",
    );
    Ok(())
}

/// Filter `migrations` to the `[from_version, to_version]` inclusive
/// range.
///
/// Either bound may be `None`. `from` bound is compared with `>=`; `to`
/// bound is compared with `<=`. Versions are compared lexicographically,
/// which matches Python and works correctly for the `YYYYMMDD_HHMMSS`
/// format used throughout `surql`.
#[must_use]
pub fn filter_migrations_by_version(
    migrations: &[Migration],
    from_version: Option<&str>,
    to_version: Option<&str>,
) -> Vec<Migration> {
    migrations
        .iter()
        .filter(|m| {
            if let Some(from) = from_version {
                if m.version.as_str() < from {
                    return false;
                }
            }
            if let Some(to) = to_version {
                if m.version.as_str() > to {
                    return false;
                }
            }
            true
        })
        .cloned()
        .collect()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    static TEST_DIR_COUNTER: AtomicU64 = AtomicU64::new(0);

    fn unique_temp_dir(tag: &str) -> PathBuf {
        let nanos: u128 = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos());
        let n = TEST_DIR_COUNTER.fetch_add(1, Ordering::Relaxed);
        let pid = std::process::id();
        let dir = std::env::temp_dir().join(format!("surql-squash-{tag}-{pid}-{nanos}-{n}"));
        fs::create_dir_all(&dir).expect("create temp dir");
        dir
    }

    fn write_migration(
        dir: &Path,
        version: &str,
        description: &str,
        up: &[&str],
        down: &[&str],
    ) -> PathBuf {
        let path = dir.join(format!("{version}_{description}.surql"));
        let mut content = String::new();
        content.push_str("-- @metadata\n");
        let _ = writeln!(content, "-- version: {version}");
        let _ = writeln!(content, "-- description: {description}");
        content.push_str("-- @up\n");
        for stmt in up {
            content.push_str(stmt);
            content.push('\n');
        }
        content.push_str("-- @down\n");
        for stmt in down {
            content.push_str(stmt);
            content.push('\n');
        }
        fs::write(&path, content).expect("write migration");
        path
    }

    // --- parse_statement --------------------------------------------------

    #[test]
    fn parse_define_table() {
        let p = parse_statement("DEFINE TABLE user SCHEMAFULL;");
        assert_eq!(p.operation, Operation::Define);
        assert_eq!(p.object_type, Some(ObjectType::Table));
        assert_eq!(p.table_name.as_deref(), Some("user"));
    }

    #[test]
    fn parse_remove_table() {
        let p = parse_statement("REMOVE TABLE user;");
        assert_eq!(p.operation, Operation::Remove);
        assert_eq!(p.object_type, Some(ObjectType::Table));
        assert_eq!(p.table_name.as_deref(), Some("user"));
    }

    #[test]
    fn parse_define_field() {
        let p = parse_statement("DEFINE FIELD email ON TABLE user TYPE string;");
        assert_eq!(p.operation, Operation::Define);
        assert_eq!(p.object_type, Some(ObjectType::Field));
        assert_eq!(p.field_name.as_deref(), Some("email"));
        assert_eq!(p.table_name.as_deref(), Some("user"));
    }

    #[test]
    fn parse_define_index() {
        let p = parse_statement("DEFINE INDEX email_idx ON TABLE user COLUMNS email UNIQUE;");
        assert_eq!(p.object_type, Some(ObjectType::Index));
        assert_eq!(p.index_name.as_deref(), Some("email_idx"));
        assert_eq!(p.table_name.as_deref(), Some("user"));
    }

    #[test]
    fn parse_define_event() {
        let p = parse_statement(
            "DEFINE EVENT user_created ON TABLE user WHEN $event = \"CREATE\" THEN {};",
        );
        assert_eq!(p.object_type, Some(ObjectType::Event));
        assert_eq!(p.index_name.as_deref(), Some("user_created"));
        assert_eq!(p.table_name.as_deref(), Some("user"));
    }

    #[test]
    fn parse_unknown_statement() {
        let p = parse_statement("SELECT * FROM user;");
        assert_eq!(p.operation, Operation::Unknown);
    }

    // --- optimize_statements ---------------------------------------------

    #[test]
    fn optimise_empty_list() {
        let (out, count) = optimize_statements(&[]);
        assert!(out.is_empty());
        assert_eq!(count, 0);
    }

    #[test]
    fn optimise_removes_field_define_remove_pair() {
        let stmts = vec![
            "DEFINE TABLE user SCHEMAFULL;".to_string(),
            "DEFINE FIELD temp ON TABLE user TYPE string;".to_string(),
            "REMOVE FIELD temp ON TABLE user;".to_string(),
        ];
        let (out, count) = optimize_statements(&stmts);
        assert_eq!(count, 2);
        let joined = out.join(" ");
        assert!(!joined.contains("DEFINE FIELD temp"));
        assert!(!joined.contains("REMOVE FIELD temp"));
    }

    #[test]
    fn optimise_removes_index_define_remove_pair() {
        let stmts = vec![
            "DEFINE TABLE user SCHEMAFULL;".to_string(),
            "DEFINE INDEX email_idx ON TABLE user COLUMNS email UNIQUE;".to_string(),
            "REMOVE INDEX email_idx ON TABLE user;".to_string(),
        ];
        let (out, count) = optimize_statements(&stmts);
        assert_eq!(count, 2);
        assert_eq!(out.len(), 1);
    }

    #[test]
    fn optimise_removes_event_define_remove_pair() {
        let stmts = vec![
            "DEFINE EVENT user_created ON TABLE user WHEN $event = \"CREATE\" THEN {};".into(),
            "REMOVE EVENT user_created ON TABLE user;".into(),
        ];
        let (out, count) = optimize_statements(&stmts);
        assert_eq!(count, 2);
        assert!(out.is_empty());
    }

    #[test]
    fn optimise_removes_duplicate_defines_keeping_last() {
        let stmts = vec![
            "DEFINE FIELD email ON TABLE user TYPE string;".into(),
            "DEFINE FIELD age ON TABLE user TYPE int;".into(),
            "DEFINE FIELD email ON TABLE user TYPE string ASSERT string::is::email($value);".into(),
        ];
        let (out, count) = optimize_statements(&stmts);
        assert_eq!(count, 1);
        assert!(out.iter().any(|s| s.contains("ASSERT")));
    }

    #[test]
    fn optimise_preserves_unrelated() {
        let stmts = vec![
            "DEFINE TABLE user SCHEMAFULL;".into(),
            "DEFINE FIELD email ON TABLE user TYPE string;".into(),
            "DEFINE TABLE post SCHEMAFULL;".into(),
        ];
        let (out, count) = optimize_statements(&stmts);
        assert_eq!(count, 0);
        assert_eq!(out.len(), 3);
    }

    #[test]
    fn optimise_removes_orphaned_updates() {
        let stmts = vec![
            "DEFINE FIELD temp ON TABLE user TYPE string;".into(),
            "UPDATE user SET temp = \"value\" WHERE temp IS NONE;".into(),
            "REMOVE FIELD temp ON TABLE user;".into(),
        ];
        let (out, count) = optimize_statements(&stmts);
        // DEFINE + REMOVE pair drops 2, orphaned UPDATE drops 1.
        assert!(out.is_empty(), "got {out:?}");
        assert!(count >= 3, "got count {count}");
    }

    // --- validate_squash_safety ------------------------------------------

    fn mock_migration(version: &str, up: &[&str]) -> Migration {
        Migration {
            version: version.to_string(),
            description: "test".to_string(),
            path: PathBuf::from(format!("migrations/{version}_test.surql")),
            up: up.iter().map(|s| (*s).to_string()).collect(),
            down: Vec::new(),
            checksum: Some("abc".to_string()),
            depends_on: Vec::new(),
        }
    }

    #[test]
    fn warn_on_insert_statement() {
        let m = mock_migration("v1", &["INSERT INTO user (name) VALUES (\"t\");"]);
        let w = validate_squash_safety(&[m]);
        assert_eq!(w.len(), 1);
        assert_eq!(w[0].severity, SquashSeverity::Medium);
        assert!(w[0].message.contains("INSERT"));
    }

    #[test]
    fn warn_on_update_statement() {
        let m = mock_migration("v1", &["UPDATE user SET name = \"t\" WHERE id = 1;"]);
        let w = validate_squash_safety(&[m]);
        assert_eq!(w.len(), 1);
        assert_eq!(w[0].severity, SquashSeverity::Medium);
    }

    #[test]
    fn warn_on_delete_statement() {
        let m = mock_migration("v1", &["DELETE FROM user WHERE id = 1;"]);
        let w = validate_squash_safety(&[m]);
        assert_eq!(w.len(), 1);
        assert_eq!(w[0].severity, SquashSeverity::High);
    }

    #[test]
    fn warn_on_record_reference() {
        let m = mock_migration(
            "v1",
            &["DEFINE FIELD author ON TABLE post TYPE record<user>;"],
        );
        let warnings = validate_squash_safety(&[m]);
        assert!(warnings
            .iter()
            .any(|w| w.severity == SquashSeverity::Low && w.message.contains("record reference")));
    }

    #[test]
    fn no_warning_on_define_only() {
        let m = mock_migration(
            "v1",
            &[
                "DEFINE TABLE user SCHEMAFULL;",
                "DEFINE FIELD email ON TABLE user TYPE string;",
            ],
        );
        let w = validate_squash_safety(&[m]);
        assert!(w.is_empty(), "got {w:?}");
    }

    #[test]
    fn backfill_update_is_silent() {
        let m = mock_migration(
            "v1",
            &["UPDATE user SET new_field = \"d\" WHERE new_field IS NONE;"],
        );
        let w = validate_squash_safety(&[m]);
        assert!(w.is_empty(), "got {w:?}");
    }

    // --- generate_squashed_migration_content ------------------------------

    #[test]
    fn generated_content_has_all_sections() {
        let content = generate_squashed_migration_content(
            &["DEFINE TABLE user SCHEMAFULL;".to_string()],
            "20260102_120000",
            "squashed_v1_to_v2",
            &["v1".to_string(), "v2".to_string()],
        );
        assert!(content.contains("-- @metadata"));
        assert!(content.contains("-- @up"));
        assert!(content.contains("-- @down"));
        assert!(content.contains("DEFINE TABLE user SCHEMAFULL;"));
        assert!(content.contains("-- squashed-from: v1,v2"));
        assert!(content.contains("-- version: 20260102_120000"));
    }

    #[test]
    fn generated_content_no_migrations_section_omits_squashed_from() {
        let content = generate_squashed_migration_content(
            &["DEFINE TABLE a SCHEMAFULL;".to_string()],
            "20260101_000000",
            "squashed_x",
            &[],
        );
        assert!(!content.contains("-- squashed-from:"));
    }

    #[test]
    fn generated_content_empty_statements_notes_marker() {
        let content = generate_squashed_migration_content(
            &[],
            "20260101_000000",
            "empty",
            &["v1".to_string()],
        );
        assert!(content.contains("-- (no statements)"));
    }

    // --- filter_migrations_by_version -------------------------------------

    #[test]
    fn filter_no_constraints_is_identity() {
        let mig = vec![
            mock_migration("20260101_000000", &[]),
            mock_migration("20260102_000000", &[]),
        ];
        let out = filter_migrations_by_version(&mig, None, None);
        assert_eq!(out.len(), 2);
    }

    #[test]
    fn filter_from_only() {
        let mig = vec![
            mock_migration("20260101_000000", &[]),
            mock_migration("20260102_000000", &[]),
            mock_migration("20260103_000000", &[]),
        ];
        let out = filter_migrations_by_version(&mig, Some("20260102_000000"), None);
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].version, "20260102_000000");
    }

    #[test]
    fn filter_to_only() {
        let mig = vec![
            mock_migration("20260101_000000", &[]),
            mock_migration("20260102_000000", &[]),
            mock_migration("20260103_000000", &[]),
        ];
        let out = filter_migrations_by_version(&mig, None, Some("20260102_000000"));
        assert_eq!(out.len(), 2);
        assert_eq!(out[1].version, "20260102_000000");
    }

    #[test]
    fn filter_both_bounds_inclusive() {
        let mig = vec![
            mock_migration("20260101_000000", &[]),
            mock_migration("20260102_000000", &[]),
            mock_migration("20260103_000000", &[]),
            mock_migration("20260104_000000", &[]),
        ];
        let out =
            filter_migrations_by_version(&mig, Some("20260102_000000"), Some("20260103_000000"));
        assert_eq!(out.len(), 2);
    }

    // --- squash_migrations -----------------------------------------------

    #[test]
    fn squash_missing_directory_errors() {
        let missing = std::env::temp_dir().join("surql-squash-nope-xyz-123");
        let err = squash_migrations(&missing, &SquashOptions::new()).unwrap_err();
        assert!(matches!(err, SurqlError::MigrationSquash { .. }));
    }

    #[test]
    fn squash_empty_directory_errors() {
        let dir = unique_temp_dir("empty");
        let err = squash_migrations(&dir, &SquashOptions::new()).unwrap_err();
        assert!(matches!(err, SurqlError::MigrationSquash { .. }));
        assert!(err.to_string().contains("No migrations found"));
    }

    #[test]
    fn squash_single_migration_errors() {
        let dir = unique_temp_dir("single");
        write_migration(
            &dir,
            "20260101_000000",
            "only",
            &["DEFINE TABLE a SCHEMAFULL;"],
            &["REMOVE TABLE a;"],
        );
        let err = squash_migrations(&dir, &SquashOptions::new()).unwrap_err();
        assert!(err.to_string().contains("At least 2 migrations required"));
    }

    #[test]
    fn squash_range_matches_nothing_errors() {
        let dir = unique_temp_dir("no-match");
        write_migration(
            &dir,
            "20260101_000000",
            "a",
            &["DEFINE TABLE a SCHEMAFULL;"],
            &["REMOVE TABLE a;"],
        );
        write_migration(
            &dir,
            "20260102_000000",
            "b",
            &["DEFINE TABLE b SCHEMAFULL;"],
            &["REMOVE TABLE b;"],
        );
        let err = squash_migrations(
            &dir,
            &SquashOptions::new()
                .from_version("20270101_000000")
                .to_version("20270102_000000"),
        )
        .unwrap_err();
        assert!(err.to_string().contains("No migrations match"));
    }

    #[test]
    fn squash_dry_run_returns_result_without_writing() {
        let dir = unique_temp_dir("dry");
        write_migration(
            &dir,
            "20260101_000000",
            "first",
            &["DEFINE TABLE first SCHEMAFULL;"],
            &["REMOVE TABLE first;"],
        );
        write_migration(
            &dir,
            "20260102_000000",
            "second",
            &["DEFINE TABLE second SCHEMAFULL;"],
            &["REMOVE TABLE second;"],
        );
        let result = squash_migrations(&dir, &SquashOptions::new().dry_run(true)).unwrap();
        assert_eq!(result.original_count, 2);
        assert_eq!(result.statement_count, 2);
        assert!(!result.squashed_path.exists());
    }

    #[test]
    fn squash_writes_file_when_not_dry_run() {
        let dir = unique_temp_dir("write");
        write_migration(
            &dir,
            "20260101_000000",
            "first",
            &["DEFINE TABLE first SCHEMAFULL;"],
            &["REMOVE TABLE first;"],
        );
        write_migration(
            &dir,
            "20260102_000000",
            "second",
            &["DEFINE TABLE second SCHEMAFULL;"],
            &["REMOVE TABLE second;"],
        );
        let result = squash_migrations(&dir, &SquashOptions::new()).unwrap();
        assert!(result.squashed_path.exists());
        let content = fs::read_to_string(&result.squashed_path).unwrap();
        assert!(content.contains("-- @up"));
        assert!(content.contains("DEFINE TABLE first"));
        assert!(content.contains("DEFINE TABLE second"));
    }

    #[test]
    fn squash_optimise_on_reduces_statement_count() {
        let dir = unique_temp_dir("opt-on");
        write_migration(
            &dir,
            "20260101_000000",
            "create_temp",
            &["DEFINE FIELD temp ON TABLE user TYPE string;"],
            &[],
        );
        write_migration(
            &dir,
            "20260102_000000",
            "remove_temp",
            &["REMOVE FIELD temp ON TABLE user;"],
            &[],
        );
        let r =
            squash_migrations(&dir, &SquashOptions::new().dry_run(true).optimize(true)).unwrap();
        assert!(r.optimizations_applied >= 2);
        assert_eq!(r.statement_count, 0);
    }

    #[test]
    fn squash_optimise_off_preserves_statements() {
        let dir = unique_temp_dir("opt-off");
        write_migration(
            &dir,
            "20260101_000000",
            "create_temp",
            &["DEFINE FIELD temp ON TABLE user TYPE string;"],
            &[],
        );
        write_migration(
            &dir,
            "20260102_000000",
            "remove_temp",
            &["REMOVE FIELD temp ON TABLE user;"],
            &[],
        );
        let r =
            squash_migrations(&dir, &SquashOptions::new().dry_run(true).optimize(false)).unwrap();
        assert_eq!(r.optimizations_applied, 0);
        assert_eq!(r.statement_count, 2);
    }

    #[test]
    fn squash_high_severity_aborts_without_force() {
        let dir = unique_temp_dir("high-sev");
        write_migration(
            &dir,
            "20260101_000000",
            "a",
            &["DEFINE TABLE user SCHEMAFULL;"],
            &[],
        );
        write_migration(
            &dir,
            "20260102_000000",
            "b",
            &["DELETE user WHERE inactive = true;"],
            &[],
        );
        let err = squash_migrations(&dir, &SquashOptions::new().dry_run(true)).unwrap_err();
        assert!(err.to_string().contains("High severity"));
    }

    #[test]
    fn squash_force_bypasses_high_severity() {
        let dir = unique_temp_dir("force");
        write_migration(
            &dir,
            "20260101_000000",
            "a",
            &["DEFINE TABLE user SCHEMAFULL;"],
            &[],
        );
        write_migration(
            &dir,
            "20260102_000000",
            "b",
            &["DELETE user WHERE inactive = true;"],
            &[],
        );
        let r = squash_migrations(&dir, &SquashOptions::new().dry_run(true).force(true)).unwrap();
        assert_eq!(r.original_count, 2);
    }

    #[test]
    fn squash_range_filters_migrations() {
        let dir = unique_temp_dir("range");
        for (i, v) in [
            "20260101_000000",
            "20260102_000000",
            "20260103_000000",
            "20260104_000000",
        ]
        .iter()
        .enumerate()
        {
            write_migration(
                &dir,
                v,
                &format!("m{i}"),
                &[&format!("DEFINE TABLE t{i} SCHEMAFULL;")],
                &[],
            );
        }
        let r = squash_migrations(
            &dir,
            &SquashOptions::new()
                .from_version("20260102_000000")
                .to_version("20260103_000000")
                .dry_run(true),
        )
        .unwrap();
        assert_eq!(r.original_count, 2);
        assert!(r
            .original_migrations
            .contains(&"20260102_000000".to_string()));
        assert!(r
            .original_migrations
            .contains(&"20260103_000000".to_string()));
    }

    #[test]
    fn squash_custom_output_path_is_honoured() {
        let dir = unique_temp_dir("custom-out");
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
        let custom = dir.join("custom_squash.surql");
        let r = squash_migrations(
            &dir,
            &SquashOptions::new().dry_run(true).output_path(&custom),
        )
        .unwrap();
        assert_eq!(r.squashed_path, custom);
    }
}
