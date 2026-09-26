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

use std::cmp::Ordering;
use std::fmt::Write as _;
use std::fs;
use std::io::Write as _;
use std::path::{Path, PathBuf};

use chrono::Utc;

use crate::error::{Result, SurqlError};
use crate::migration::discovery::{compare_versions, discover_migrations, sha256_hex};
use crate::migration::generator::{next_free_version, single_line};
use crate::migration::lexer::{self, existence_clause, Clause, Token};
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Operation {
    Define,
    Remove,
    Other,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum ObjectType {
    Table,
    Field,
    Index,
    Event,
}

/// The schema object a `DEFINE` / `REMOVE` statement names. Names are
/// rendered the way the engine prints them, so `` `user` `` and `user`
/// are the same object; a table's `name` is the table itself.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ObjectKey {
    kind: ObjectType,
    table: String,
    name: String,
}

#[derive(Debug, Clone)]
struct ParsedStatement {
    operation: Operation,
    clause: Clause,
    /// `None` for anything the optimiser does not understand: data
    /// statements, other kinds of definition, and names it cannot read.
    /// Such a statement is never removed, and nothing is moved across it.
    object: Option<ObjectKey>,
}

fn parse_statement(statement: &str) -> ParsedStatement {
    let toks = lexer::tokens(statement);
    let operation = match toks.first() {
        Some(t) if t.is_keyword("DEFINE") => Operation::Define,
        Some(t) if t.is_keyword("REMOVE") => Operation::Remove,
        _ => Operation::Other,
    };
    let (clause, object) = match (operation, toks.split_first()) {
        (Operation::Define | Operation::Remove, Some((_, rest))) => parse_object(rest),
        _ => (Clause::Plain, None),
    };
    ParsedStatement {
        operation,
        clause,
        object,
    }
}

/// Parse `<KIND> [IF NOT EXISTS | IF EXISTS | OVERWRITE] <name> [ON [TABLE] <table>]`.
fn parse_object(toks: &[Token<'_>]) -> (Clause, Option<ObjectKey>) {
    let Some((kind_tok, rest)) = toks.split_first() else {
        return (Clause::Plain, None);
    };
    let kind = if kind_tok.is_keyword("TABLE") {
        ObjectType::Table
    } else if kind_tok.is_keyword("FIELD") {
        ObjectType::Field
    } else if kind_tok.is_keyword("INDEX") {
        ObjectType::Index
    } else if kind_tok.is_keyword("EVENT") {
        ObjectType::Event
    } else {
        return (Clause::Plain, None);
    };
    let (clause, rest) = existence_clause(rest);
    let object = if kind == ObjectType::Table {
        rest.first().and_then(Token::name).map(|table| ObjectKey {
            kind,
            name: table.clone(),
            table,
        })
    } else {
        parse_table_scoped(kind, rest)
    };
    (clause, object)
}

/// `<name> ON [TABLE] <table>`, where a field name may be a path such as
/// `address.city` or `tags[*]`.
fn parse_table_scoped(kind: ObjectType, toks: &[Token<'_>]) -> Option<ObjectKey> {
    let on = toks.iter().position(|t| t.is_keyword("ON"))?;
    let (name_toks, after_on) = toks.split_at(on);
    let name = name_toks
        .iter()
        .map(|t| match t {
            Token::Punct(c @ ('.' | '[' | ']' | '*')) => Some(c.to_string()),
            other => other.name(),
        })
        .collect::<Option<String>>()
        .filter(|name| !name.is_empty())?;
    let after_on = after_on.get(1..)?;
    let table_tok = match after_on {
        [t, table, ..] if t.is_keyword("TABLE") && table.name().is_some() => table,
        [table, ..] => table,
        [] => return None,
    };
    Some(ObjectKey {
        kind,
        table: table_tok.name()?,
        name,
    })
}

/// Remove redundant SurrealQL statements from a list.
///
/// Understands `DEFINE` / `REMOVE` of tables, fields, indexes and events
/// (with or without `IF NOT EXISTS`, `IF EXISTS`, `OVERWRITE`, and with
/// `ON t` or `ON TABLE t`) and applies two rewrites:
///
/// 1. A plain `DEFINE` (which fails when the object already exists) that
///    is later removed again with nothing in between that could observe
///    the object is dropped together with its `REMOVE`. A `DEFINE … IF NOT
///    EXISTS` or `OVERWRITE` is kept: the object may have existed before,
///    and then the `REMOVE` still has work to do.
/// 2. Of two definitions of the same object with nothing observing it in
///    between, the earlier is dropped when the later one `OVERWRITE`s it,
///    and a later `IF NOT EXISTS` (a no-op at that point) is dropped.
///
/// "Could observe" is judged conservatively: any statement the optimiser
/// does not understand (data statements included), any statement on the
/// same table (other than a definition of an unrelated field of it), and
/// any change to the table itself blocks a rewrite. Data statements are
/// never removed.
///
/// Returns the optimised list and the count of individual statements
/// that were elided.
#[must_use]
pub fn optimize_statements(statements: &[String]) -> (Vec<String>, usize) {
    let parsed: Vec<ParsedStatement> = statements
        .iter()
        .map(|s| parse_statement(s.as_str()))
        .collect();

    let mut removed = vec![false; parsed.len()];
    drop_define_remove_pairs(&parsed, &mut removed);
    drop_superseded_defines(&parsed, &mut removed);

    let optimised: Vec<String> = statements
        .iter()
        .zip(&removed)
        .filter(|(_, gone)| !**gone)
        .map(|(s, _)| s.clone())
        .collect();
    let count = removed.iter().filter(|gone| **gone).count();
    (optimised, count)
}

/// The next live statement after `i` that names the same object, provided
/// nothing between the two could observe or depend on that object.
fn next_same_object(parsed: &[ParsedStatement], removed: &[bool], i: usize) -> Option<usize> {
    let key = parsed.get(i)?.object.as_ref()?;
    for (j, stmt) in parsed.iter().enumerate().skip(i + 1) {
        if removed.get(j).copied().unwrap_or(false) {
            continue;
        }
        if stmt.object.as_ref() == Some(key) {
            return Some(j);
        }
        if interferes(key, stmt) {
            return None;
        }
    }
    None
}

/// `true` when `stmt` may observe or depend on the object `key` names, so
/// a rewrite may not move `key`'s definition across it.
fn interferes(key: &ObjectKey, stmt: &ParsedStatement) -> bool {
    let Some(other) = &stmt.object else {
        return true;
    };
    if other.table != key.table {
        return false;
    }
    match (key.kind, other.kind) {
        (ObjectType::Field, ObjectType::Field) => field_root(&key.name) == field_root(&other.name),
        _ => true,
    }
}

/// The top-level field a field path belongs to (`address` for
/// `address.city`, `tags` for `tags[*]`).
fn field_root(path: &str) -> &str {
    path.split(['.', '[']).next().unwrap_or(path)
}

fn drop_define_remove_pairs(parsed: &[ParsedStatement], removed: &mut [bool]) {
    for (i, stmt) in parsed.iter().enumerate() {
        if removed[i] || stmt.operation != Operation::Define || stmt.clause != Clause::Plain {
            continue;
        }
        if let Some(j) = next_same_object(parsed, removed, i) {
            if parsed[j].operation == Operation::Remove {
                removed[i] = true;
                removed[j] = true;
            }
        }
    }
}

fn drop_superseded_defines(parsed: &[ParsedStatement], removed: &mut [bool]) {
    let mut i = 0;
    while i < parsed.len() {
        if !removed[i] && parsed[i].operation == Operation::Define {
            if let Some(j) = next_same_object(parsed, removed, i) {
                if parsed[j].operation == Operation::Define {
                    match parsed[j].clause {
                        Clause::Overwrite => removed[i] = true,
                        Clause::IfNotExists => {
                            // Look past the no-op for another definition.
                            removed[j] = true;
                            continue;
                        }
                        Clause::Plain | Clause::IfExists => {}
                    }
                }
            }
        }
        i += 1;
    }
}

// ---------------------------------------------------------------------------
// Safety validation
// ---------------------------------------------------------------------------

/// Inspect `migrations` and return a list of warnings about
/// data-manipulation statements, ordering issues, and other known
/// squash hazards.
///
/// Statements are classified by their first keyword after any leading
/// comments, so a `-- purge` line above a `DELETE` does not hide it.
#[must_use]
pub fn validate_squash_safety(migrations: &[Migration]) -> Vec<SquashWarning> {
    let mut warnings: Vec<SquashWarning> = Vec::new();

    for migration in migrations {
        let version = &migration.version;
        for stmt in &migration.up {
            let code = lexer::strip_leading_comments(stmt);
            let toks = lexer::tokens(code);
            let has = |kw: &str| toks.iter().any(|t| t.is_keyword(kw));
            let is_backfill = toks
                .windows(2)
                .any(|w| w[0].is_keyword("IS") && w[1].is_keyword("NONE"));
            let preview = preview_statement(code);
            let verb = toks.first();

            let warning = match verb {
                Some(t) if t.is_keyword("INSERT") => Some(("INSERT", SquashSeverity::Medium)),
                Some(t) if t.is_keyword("UPDATE") && has("SET") && !is_backfill => {
                    Some(("UPDATE", SquashSeverity::Medium))
                }
                Some(t) if t.is_keyword("DELETE") => Some(("DELETE", SquashSeverity::High)),
                Some(t)
                    if t.is_keyword("CREATE")
                        && !toks.get(1).is_some_and(|t| t.is_keyword("TABLE")) =>
                {
                    Some(("CREATE", SquashSeverity::Low))
                }
                _ => None,
            };
            if let Some((kind, severity)) = warning {
                warnings.push(SquashWarning {
                    migration: version.clone(),
                    message: format!("Contains {kind} statement: {preview}..."),
                    severity,
                });
            }

            if has("RECORD") && has("TYPE") {
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

/// The first 50 characters of a statement (never cut inside a character).
fn preview_statement(stmt: &str) -> String {
    stmt.trim().chars().take(50).collect()
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
    let _ = writeln!(buf, "-- version: {}", single_line(version));
    let _ = writeln!(buf, "-- description: {}", single_line(description));
    buf.push_str("-- author: surql\n");
    if !original_migrations.is_empty() {
        let _ = writeln!(
            buf,
            "-- squashed-from: {}",
            single_line(&original_migrations.join(","))
        );
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
            buf.push_str(&lexer::terminate_statement(stmt));
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
    let version = next_free_version(directory)?;

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

/// `squashed_<first>_to_<last>`, reduced to filename-safe characters: it
/// names the output file, and versions come from file metadata.
fn describe_range(versions: &[String]) -> String {
    let first = versions.first().map_or("unknown", String::as_str);
    let last = versions.last().map_or("unknown", String::as_str);
    format!("squashed_{first}_to_{last}")
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
        .collect()
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
    // Never replace an existing file: the default name carries a fresh
    // version, so a clash means an explicit output path points at one.
    fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(output_path)
        .and_then(|mut file| file.write_all(content.as_bytes()))
        .map_err(|e| SurqlError::Io {
            reason: format!(
                "failed to write squashed migration {}: {e}",
                output_path.display()
            ),
        })?;
    let checksum = sha256_hex(content.as_bytes());
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
/// bound is compared with `<=`. Versions are compared with runs of digits
/// read as numbers (so `v9` < `v10`), which for the `YYYYMMDD_HHMMSS`
/// format used throughout `surql` is plain string order.
#[must_use]
pub fn filter_migrations_by_version(
    migrations: &[Migration],
    from_version: Option<&str>,
    to_version: Option<&str>,
) -> Vec<Migration> {
    migrations
        .iter()
        .filter(|m| {
            from_version.is_none_or(|from| compare_versions(&m.version, from) != Ordering::Less)
                && to_version.is_none_or(|to| compare_versions(&m.version, to) != Ordering::Greater)
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

    fn key(kind: ObjectType, table: &str, name: &str) -> ObjectKey {
        ObjectKey {
            kind,
            table: table.to_string(),
            name: name.to_string(),
        }
    }

    #[test]
    fn parse_define_table() {
        let p = parse_statement("DEFINE TABLE user SCHEMAFULL;");
        assert_eq!(p.operation, Operation::Define);
        assert_eq!(p.clause, Clause::Plain);
        assert_eq!(p.object, Some(key(ObjectType::Table, "user", "user")));
    }

    #[test]
    fn parse_remove_table() {
        let p = parse_statement("REMOVE TABLE user;");
        assert_eq!(p.operation, Operation::Remove);
        assert_eq!(p.object, Some(key(ObjectType::Table, "user", "user")));
    }

    #[test]
    fn parse_define_field() {
        let p = parse_statement("DEFINE FIELD email ON TABLE user TYPE string;");
        assert_eq!(p.operation, Operation::Define);
        assert_eq!(p.object, Some(key(ObjectType::Field, "user", "email")));
    }

    #[test]
    fn parse_define_index() {
        let p = parse_statement("DEFINE INDEX email_idx ON TABLE user COLUMNS email UNIQUE;");
        assert_eq!(p.object, Some(key(ObjectType::Index, "user", "email_idx")));
    }

    #[test]
    fn parse_define_event() {
        let p = parse_statement(
            "DEFINE EVENT user_created ON TABLE user WHEN $event = \"CREATE\" THEN {};",
        );
        assert_eq!(
            p.object,
            Some(key(ObjectType::Event, "user", "user_created"))
        );
    }

    #[test]
    fn parse_unknown_statement() {
        let p = parse_statement("SELECT * FROM user;");
        assert_eq!(p.operation, Operation::Other);
        assert_eq!(p.object, None);
    }

    /// The name used to be read from the third token, which is `IF` or
    /// `OVERWRITE` in these forms, and the table only after `ON TABLE`.
    #[test]
    fn parse_skips_existence_clauses_and_accepts_bare_on() {
        let p = parse_statement("DEFINE TABLE IF NOT EXISTS user SCHEMAFULL;");
        assert_eq!(p.clause, Clause::IfNotExists);
        assert_eq!(p.object, Some(key(ObjectType::Table, "user", "user")));

        let p = parse_statement("DEFINE FIELD OVERWRITE f ON TABLE t TYPE int;");
        assert_eq!(p.clause, Clause::Overwrite);
        assert_eq!(p.object, Some(key(ObjectType::Field, "t", "f")));

        let p = parse_statement("REMOVE FIELD IF EXISTS email ON user;");
        assert_eq!(p.clause, Clause::IfExists);
        assert_eq!(p.object, Some(key(ObjectType::Field, "user", "email")));

        let p = parse_statement("define index i on post fields a;");
        assert_eq!(p.object, Some(key(ObjectType::Index, "post", "i")));
    }

    #[test]
    fn parse_resolves_quoted_names_and_field_paths() {
        let p = parse_statement("DEFINE TABLE `user` SCHEMAFULL;");
        assert_eq!(p.object, Some(key(ObjectType::Table, "user", "user")));
        let p = parse_statement("DEFINE FIELD `first-name` ON ⟨my-table⟩ TYPE string;");
        assert_eq!(
            p.object,
            Some(key(ObjectType::Field, "`my-table`", "`first-name`"))
        );
        let p = parse_statement("DEFINE FIELD address.city ON user TYPE string;");
        assert_eq!(
            p.object,
            Some(key(ObjectType::Field, "user", "address.city"))
        );
        let p = parse_statement("DEFINE FIELD tags[*] ON user TYPE string;");
        assert_eq!(p.object, Some(key(ObjectType::Field, "user", "tags[*]")));
        let p = parse_statement("-- note\nDEFINE TABLE post;");
        assert_eq!(p.object, Some(key(ObjectType::Table, "post", "post")));
    }

    #[test]
    fn parse_leaves_unreadable_definitions_unkeyed() {
        for stmt in [
            "DEFINE FUNCTION fn::a() { RETURN 1; };",
            "DEFINE PARAM $x VALUE 1;",
            "DEFINE FIELD ON user;",
            "DEFINE FIELD 'str' ON user;",
            "DEFINE FIELD f;",
        ] {
            assert_eq!(parse_statement(stmt).object, None, "{stmt}");
        }
    }

    // --- optimize_statements ---------------------------------------------

    fn strings(stmts: &[&str]) -> Vec<String> {
        stmts.iter().map(|s| (*s).to_string()).collect()
    }

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
    fn optimise_drops_a_definition_its_overwrite_replaces() {
        let stmts = strings(&[
            "DEFINE FIELD email ON TABLE user TYPE string;",
            "DEFINE FIELD age ON TABLE user TYPE int;",
            "DEFINE FIELD OVERWRITE email ON TABLE user TYPE string ASSERT string::is::email($value);",
        ]);
        let (out, count) = optimize_statements(&stmts);
        assert_eq!(count, 1);
        assert_eq!(out, stmts[1..].to_vec());
    }

    #[test]
    fn optimise_drops_a_redundant_if_not_exists_and_keeps_the_first() {
        let stmts = strings(&[
            "DEFINE FIELD email ON user TYPE string ASSERT $value != NONE;",
            "DEFINE FIELD IF NOT EXISTS email ON user TYPE string;",
        ]);
        let (out, count) = optimize_statements(&stmts);
        assert_eq!(count, 1);
        assert_eq!(out, stmts[..1].to_vec());
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

    /// `generate_initial_migration` writes every definition with `IF NOT
    /// EXISTS`; they all used to key as the table `if` and all but the last
    /// were dropped as duplicates.
    #[test]
    fn optimise_keeps_every_if_not_exists_table() {
        let stmts = strings(&[
            "DEFINE TABLE IF NOT EXISTS user SCHEMAFULL;",
            "DEFINE FIELD IF NOT EXISTS email ON TABLE user TYPE string;",
            "DEFINE TABLE IF NOT EXISTS post SCHEMAFULL;",
            "DEFINE FIELD IF NOT EXISTS email ON TABLE post TYPE string;",
            "DEFINE TABLE IF NOT EXISTS comment SCHEMAFULL;",
        ]);
        let (out, count) = optimize_statements(&stmts);
        assert_eq!(count, 0, "{out:#?}");
        assert_eq!(out, stmts);
    }

    #[test]
    fn optimise_keys_fields_by_their_table_under_bare_on() {
        let stmts = strings(&[
            "DEFINE FIELD email ON user TYPE string;",
            "DEFINE FIELD email ON post TYPE string;",
            "REMOVE FIELD email ON post;",
        ]);
        let (out, _) = optimize_statements(&stmts);
        assert_eq!(out, stmts[..1].to_vec());

        let stmts = strings(&["DEFINE TABLE user;", "REMOVE TABLE post;"]);
        assert_eq!(optimize_statements(&stmts).0, stmts);
    }

    #[test]
    fn optimise_never_pairs_a_conditional_define_with_a_remove() {
        // The object may predate the squashed range; then the REMOVE is
        // what deletes it.
        for define in [
            "DEFINE TABLE IF NOT EXISTS legacy;",
            "DEFINE TABLE OVERWRITE legacy;",
        ] {
            let stmts = strings(&[define, "REMOVE TABLE legacy;"]);
            assert_eq!(optimize_statements(&stmts).0, stmts, "{define}");
        }
    }

    #[test]
    fn optimise_never_removes_unreadable_definitions() {
        let stmts = strings(&[
            "DEFINE FUNCTION fn::a() { RETURN 1; };",
            "DEFINE FUNCTION fn::b() { RETURN 2; };",
            "DEFINE ANALYZER one TOKENIZERS blank;",
            "DEFINE ANALYZER two TOKENIZERS blank;",
        ]);
        assert_eq!(optimize_statements(&stmts), (stmts, 0));
    }

    /// Copy-through-temp-column: the fill step and the copy-back both read
    /// or write `temp`, so neither the pair nor the UPDATEs may go.
    #[test]
    fn optimise_keeps_everything_around_a_data_statement() {
        let stmts = strings(&[
            "DEFINE FIELD temp ON TABLE user TYPE string;",
            "UPDATE user SET temp = <string> age;",
            "REMOVE FIELD age ON TABLE user;",
            "DEFINE FIELD age ON TABLE user TYPE string;",
            "UPDATE user SET age = temp;",
            "REMOVE FIELD temp ON TABLE user;",
        ]);
        assert_eq!(optimize_statements(&stmts), (stmts, 0));
    }

    #[test]
    fn optimise_keeps_a_table_pair_with_children_in_between() {
        let stmts = strings(&[
            "DEFINE TABLE scratch;",
            "DEFINE FIELD x ON scratch TYPE int;",
            "REMOVE TABLE scratch;",
        ]);
        assert_eq!(optimize_statements(&stmts), (stmts, 0));
    }

    #[test]
    fn optimise_moves_nothing_across_a_statement_on_the_same_field_root() {
        let stmts = strings(&[
            "DEFINE FIELD address ON user TYPE object;",
            "DEFINE FIELD address.city ON user TYPE string;",
            "REMOVE FIELD address ON user;",
        ]);
        assert_eq!(optimize_statements(&stmts), (stmts, 0));

        // An unrelated field of the same table does not block.
        let stmts = strings(&[
            "DEFINE FIELD temp ON user TYPE int;",
            "DEFINE FIELD other ON user TYPE int;",
            "REMOVE FIELD temp ON user;",
        ]);
        assert_eq!(optimize_statements(&stmts), (stmts[1..2].to_vec(), 2));
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
            squashed_from: Vec::new(),
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
    fn a_leading_comment_does_not_hide_a_delete() {
        let m = mock_migration(
            "v1",
            &["-- purge\nDELETE FROM user", "/* x */ INSERT INTO t {};"],
        );
        let w = validate_squash_safety(&[m]);
        assert_eq!(w.len(), 2, "{w:?}");
        assert_eq!(w[0].severity, SquashSeverity::High);
        assert!(
            w[0].message.contains("DELETE FROM user"),
            "{}",
            w[0].message
        );
        assert_eq!(w[1].severity, SquashSeverity::Medium);
    }

    #[test]
    fn update_with_set_on_its_own_line_warns() {
        let m = mock_migration("v1", &["UPDATE user\nSET name = 'x';"]);
        assert_eq!(validate_squash_safety(&[m]).len(), 1);
    }

    /// The preview was cut at byte 50, which panics inside a multi-byte
    /// character.
    #[test]
    fn preview_never_splits_a_character() {
        let stmt = "CREATE tt SET c = 'éééééééééééééééééééééééééééééé';";
        let m = mock_migration("v1", &[stmt]);
        let w = validate_squash_safety(&[m]);
        assert_eq!(w.len(), 1);
        assert_eq!(preview_statement(stmt).chars().count(), 50);
        assert_eq!(preview_statement("short"), "short");
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

    /// A statement ending in a line comment used to get its `;` appended
    /// inside the comment, gluing the next statement onto it on reload.
    #[test]
    fn generated_content_round_trips_a_trailing_comment() {
        let dir = unique_temp_dir("trailing-comment");
        let content = generate_squashed_migration_content(
            &[
                "DEFINE TABLE a SCHEMAFULL -- no terminator".to_string(),
                "DEFINE TABLE b SCHEMAFULL;".to_string(),
            ],
            "20260102_120000",
            "squashed",
            &[],
        );
        let path = dir.join("20260102_120000_squashed.surql");
        fs::write(&path, content).unwrap();
        let m = crate::migration::discovery::load_migration(&path).unwrap();
        assert_eq!(m.up.len(), 2, "{:#?}", m.up);
        assert_eq!(m.up[1], "DEFINE TABLE b SCHEMAFULL;");
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
    fn squash_keeps_every_table_of_an_initial_migration() {
        use crate::migration::generator::generate_initial_migration;
        use crate::schema::fields::{FieldDefinition, FieldType};
        use crate::schema::registry::SchemaRegistry;
        use crate::schema::table::table_schema;

        let dir = unique_temp_dir("initial");
        let registry = SchemaRegistry::new();
        for name in ["user", "post", "comment"] {
            registry.register_table(
                table_schema(name).with_fields([FieldDefinition::new("email", FieldType::String)]),
            );
        }
        let initial = generate_initial_migration(&registry, &dir).unwrap();
        write_migration(
            &dir,
            "29990101_000000",
            "later",
            &["DEFINE TABLE tag SCHEMAFULL;"],
            &[],
        );

        let r = squash_migrations(&dir, &SquashOptions::new().dry_run(true)).unwrap();
        assert_eq!(r.optimizations_applied, 0);
        assert_eq!(r.statement_count, initial.up.len() + 1);
    }

    #[test]
    fn squash_never_overwrites_an_existing_output() {
        let dir = unique_temp_dir("no-clobber");
        write_migration(&dir, "20260101_000000", "a", &["DEFINE TABLE a;"], &[]);
        write_migration(&dir, "20260102_000000", "b", &["DEFINE TABLE b;"], &[]);
        let existing = dir.join("keep.surql");
        fs::write(&existing, "precious").unwrap();

        let err =
            squash_migrations(&dir, &SquashOptions::new().output_path(&existing)).unwrap_err();
        assert!(matches!(err, SurqlError::Io { .. }), "{err}");
        assert_eq!(fs::read_to_string(&existing).unwrap(), "precious");

        // Two squashes in the same second get distinct versions and files.
        let first =
            squash_migrations(&dir, &SquashOptions::new().to_version("20260102_000000")).unwrap();
        let second =
            squash_migrations(&dir, &SquashOptions::new().to_version("20260102_000000")).unwrap();
        assert_ne!(first.squashed_path, second.squashed_path);
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
