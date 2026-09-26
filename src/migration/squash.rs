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
mod tests;
