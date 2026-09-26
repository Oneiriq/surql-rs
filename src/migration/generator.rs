//! Migration file generation.
//!
//! Port of `surql/migration/generator.py`. Provides functions for writing
//! migration files to disk from raw SurrealQL statements, from a
//! [`SchemaRegistry`] (initial migration), or from a list of
//! [`SchemaDiff`] entries.
//!
//! ## File format
//!
//! Generated files follow the format documented in
//! [`crate::migration::discovery`]: a `.surql` file with `-- @metadata`,
//! `-- @up`, and `-- @down` section markers. The filename pattern is
//! `YYYYMMDD_HHMMSS_<sanitized_description>.surql`.
//!
//! Every generated file is guaranteed to round-trip through
//! [`load_migration`]: loading the generated file returns a [`Migration`]
//! whose `up` and `down` statement vectors match what the caller passed
//! in (modulo the checksum, which is computed from file content).
//!
//! ## Atomic writes
//!
//! Files are written via a temporary sibling file that is then linked
//! (or renamed) into place, so that a crash mid-write cannot leave a
//! partially-written migration on disk. Readers that enumerate the
//! directory will either see the old state (no file) or the new state
//! (complete file), never a torn write. An existing file is never
//! replaced: a version already used in the directory is bumped to the
//! next free second, and a name clash that remains is an error.
//!
//! ## Deviation from Python
//!
//! The Python helper took old/new schema snapshots and computed diffs
//! internally. In Rust, diffing is already exposed by
//! [`crate::migration::diff`], so [`generate_migration`] accepts raw
//! `up` / `down` statements and the caller drives the diff pipeline.
//!
//! [`load_migration`]: crate::migration::load_migration
//! [`SchemaRegistry`]: crate::schema::SchemaRegistry

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::fs;
use std::io::Write as _;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use chrono::{TimeDelta, Utc};

use crate::error::{Result, SurqlError};
use crate::migration::diff::SchemaSnapshot;
use crate::migration::discovery::{get_version_from_filename, load_migration};
use crate::migration::lexer;
use crate::migration::models::{Migration, SchemaDiff};
use crate::schema::bucket::BucketDefinition;
use crate::schema::edge::EdgeDefinition;
use crate::schema::sql::generate_schema_sql;
use crate::schema::table::TableDefinition;
use crate::schema::SchemaRegistry;
use crate::types::escape::quote_ident;

/// Default author string written to the `-- @metadata` section.
const DEFAULT_AUTHOR: &str = "surql";

/// Generate a migration file from explicit up/down statement lists.
///
/// Writes the migration atomically to `directory`, using the current UTC
/// timestamp for the version (or the next second no migration in
/// `directory` uses yet), and returns the loaded [`Migration`] so callers
/// can use it immediately without re-parsing from disk.
///
/// The `name` parameter is used to derive the filename and the
/// human-readable description. It is sanitised to lowercase
/// alphanumeric-plus-underscore.
///
/// # Errors
///
/// Returns [`SurqlError::MigrationGeneration`] if:
/// * `name` sanitises to an empty string.
/// * `directory` cannot be created or written to.
/// * The target file already exists (it is never overwritten).
/// * The round-trip load after write fails.
///
/// # Examples
///
/// ```no_run
/// use std::path::Path;
/// use surql::migration::generator::generate_migration;
///
/// let m = generate_migration(
///     "create_user",
///     &["DEFINE TABLE user SCHEMAFULL;".to_string()],
///     &["REMOVE TABLE user;".to_string()],
///     Path::new("migrations"),
/// ).unwrap();
/// assert_eq!(m.description, "Create user");
/// ```
pub fn generate_migration(
    name: &str,
    up_statements: &[String],
    down_statements: &[String],
    directory: &Path,
) -> Result<Migration> {
    let sanitized = sanitize_name(name)?;
    let version = next_free_version(directory)?;
    let description = description_from_name(name);

    let content = render_content(
        &version,
        &description,
        DEFAULT_AUTHOR,
        &[],
        up_statements,
        down_statements,
    );

    let filename = format!("{version}_{sanitized}.surql");
    write_migration_file(directory, &filename, &content)
}

/// Generate an initial migration from a [`SchemaRegistry`] snapshot.
///
/// The `up` section contains the full `DEFINE` script for every
/// registered table and edge (rendered via
/// [`crate::schema::generate_schema_sql`]). The `down` section contains
/// matching `REMOVE TABLE` statements in reverse order so rollback
/// produces a clean database.
///
/// `IF NOT EXISTS` is added to every `DEFINE` statement so the
/// migration can be safely re-applied to an already-initialised
/// database without error.
///
/// # Errors
///
/// Returns [`SurqlError::MigrationGeneration`] if the registry contains
/// no tables and no edges, if SQL generation fails (for example an
/// edge in relation mode with missing `from_table`/`to_table`), or if
/// the file cannot be written.
///
/// # Examples
///
/// ```no_run
/// use std::path::Path;
/// use surql::migration::generator::generate_initial_migration;
/// use surql::schema::SchemaRegistry;
///
/// let r = SchemaRegistry::new();
/// let m = generate_initial_migration(&r, Path::new("migrations")).unwrap();
/// assert_eq!(m.description, "Initial schema");
/// # drop(m);
/// ```
pub fn generate_initial_migration(
    registry: &SchemaRegistry,
    directory: &Path,
) -> Result<Migration> {
    let tables = registry.tables();
    let edges = registry.edges();
    let buckets = registry.buckets();

    if tables.is_empty() && edges.is_empty() && buckets.is_empty() {
        return Err(SurqlError::MigrationGeneration {
            reason: "registry is empty: cannot generate initial migration".to_string(),
        });
    }

    let snapshot = SchemaSnapshot {
        tables: tables.values().cloned().collect(),
        edges: edges.values().cloned().collect(),
        buckets: buckets.values().cloned().collect(),
        ..Default::default()
    };

    let (up_statements, down_statements) = build_initial_statements(&snapshot)?;

    generate_migration(
        "initial_schema",
        &up_statements,
        &down_statements,
        directory,
    )
}

/// Create an empty template migration for manual editing.
///
/// Writes a valid migration file whose `up` and `down` bodies are empty
/// comment placeholders, so the author can fill them in by hand. The
/// file still round-trips through [`load_migration`] as a migration
/// with empty statement vectors.
///
/// # Errors
///
/// Returns [`SurqlError::MigrationGeneration`] if `name` sanitises to
/// empty, or if the file cannot be written.
///
/// # Examples
///
/// ```no_run
/// use std::path::Path;
/// use surql::migration::generator::create_blank_migration;
///
/// let m = create_blank_migration(
///     "backfill_users",
///     "Backfill missing user.email values",
///     Path::new("migrations"),
/// ).unwrap();
/// assert!(m.up.is_empty());
/// ```
pub fn create_blank_migration(
    name: &str,
    description: &str,
    directory: &Path,
) -> Result<Migration> {
    let sanitized = sanitize_name(name)?;
    let version = next_free_version(directory)?;
    let resolved_description = if description.is_empty() {
        description_from_name(name)
    } else {
        description.to_string()
    };

    let content = render_blank_content(&version, &resolved_description, DEFAULT_AUTHOR);

    let filename = format!("{version}_{sanitized}.surql");
    write_migration_file(directory, &filename, &content)
}

/// Generate a migration from a list of [`SchemaDiff`] entries.
///
/// The `up` statements are the `forward_sql` of each diff in the input
/// order. The `down` statements are the `backward_sql` of each diff in
/// *reverse* input order so that the rollback undoes the migration
/// bottom-up.
///
/// Diffs whose `forward_sql` / `backward_sql` is empty contribute
/// nothing to that section (matching the Python behaviour). Each
/// statement is trimmed and, if missing, a trailing `;` is added so
/// the round-trip through the statement splitter stays stable.
///
/// # Errors
///
/// Returns [`SurqlError::MigrationGeneration`] if `diffs` is empty or
/// if the file cannot be written.
///
/// # Examples
///
/// ```no_run
/// use std::path::Path;
/// use surql::migration::generator::generate_migration_from_diffs;
/// use surql::migration::{DiffOperation, SchemaDiff};
///
/// let diff = SchemaDiff {
///     operation: DiffOperation::AddTable,
///     table: "user".into(),
///     field: None,
///     index: None,
///     event: None,
///     bucket: None,
///     analyzer: None,
///     object: None,
///     description: "Add user table".into(),
///     forward_sql: "DEFINE TABLE user SCHEMAFULL;".into(),
///     backward_sql: "REMOVE TABLE user;".into(),
///     details: Default::default(),
/// };
/// let m = generate_migration_from_diffs(
///     "add_user",
///     &[diff],
///     Path::new("migrations"),
/// ).unwrap();
/// assert_eq!(m.up.len(), 1);
/// ```
pub fn generate_migration_from_diffs(
    name: &str,
    diffs: &[SchemaDiff],
    directory: &Path,
) -> Result<Migration> {
    if diffs.is_empty() {
        return Err(SurqlError::MigrationGeneration {
            reason: "no diffs provided".to_string(),
        });
    }

    // A field that gained REFERENCE carries the rewrite its
    // pre-existing rows need; the generated file is the reviewed
    // artifact, so the DML belongs here, right after the DDL it
    // completes. The executor wraps the whole migration in one
    // transaction, and registration inside the transaction that
    // defines the clause is probed behaviour.
    let up_statements: Vec<String> = diffs
        .iter()
        .flat_map(|d| {
            normalise_statement(&d.forward_sql)
                .into_iter()
                .chain(d.reference_backfill_sql().and_then(normalise_statement))
        })
        .collect();

    let down_statements: Vec<String> = diffs
        .iter()
        .rev()
        .filter_map(|d| normalise_statement(&d.backward_sql))
        .collect();

    generate_migration(name, &up_statements, &down_statements, directory)
}

// ---------------------------------------------------------------------------
// Internals
// ---------------------------------------------------------------------------

/// A UTC timestamp version (`YYYYMMDD_HHMMSS`) for a new migration in
/// `directory`: the current second, or the first later second no
/// migration file in `directory` uses yet. Versions have one-second
/// resolution, so two migrations generated within one second would
/// otherwise share a version (and, with the same name, a file).
pub(crate) fn next_free_version(directory: &Path) -> Result<String> {
    let taken: BTreeSet<String> = fs::read_dir(directory)
        .into_iter()
        .flatten()
        .filter_map(std::result::Result::ok)
        .filter_map(|entry| get_version_from_filename(entry.file_name().to_str()?))
        .collect();
    let mut at = Utc::now();
    loop {
        let version = at.format("%Y%m%d_%H%M%S").to_string();
        if !taken.contains(&version) {
            return Ok(version);
        }
        at = at
            .checked_add_signed(TimeDelta::seconds(1))
            .ok_or_else(|| SurqlError::MigrationGeneration {
                reason: "no free migration version left".to_string(),
            })?;
    }
}

/// `text` on a single line: line breaks and other control characters
/// become spaces, so a description cannot end the metadata line and start
/// a `-- @up` section marker of its own.
pub(crate) fn single_line(text: &str) -> String {
    text.chars()
        .map(|c| {
            if c.is_control() || matches!(c, '\u{2028}' | '\u{2029}') {
                ' '
            } else {
                c
            }
        })
        .collect()
}

/// Sanitize a human-supplied name into a safe filename component.
///
/// Lower-cases the input, replaces spaces with underscores, and strips
/// every character that is not ASCII-alphanumeric or underscore.
/// Returns an error when the result is empty.
fn sanitize_name(name: &str) -> Result<String> {
    let lower = name.to_lowercase();
    let with_underscores = lower.replace(' ', "_");
    let sanitized: String = with_underscores
        .chars()
        .filter(|c| c.is_ascii_alphanumeric() || *c == '_')
        .collect();

    // Reject names that contain no alphanumerics — purely-underscore results
    // (e.g. from "   " or "___") produce unusable filenames.
    if sanitized.is_empty() || !sanitized.chars().any(|c| c.is_ascii_alphanumeric()) {
        return Err(SurqlError::MigrationGeneration {
            reason: format!("name {name:?} sanitises to empty string"),
        });
    }
    Ok(sanitized)
}

/// Derive a human-readable description from a raw name.
///
/// Converts underscores to spaces and capitalises the first letter.
/// Used when the caller does not provide an explicit description.
fn description_from_name(name: &str) -> String {
    let with_spaces = name.replace('_', " ").trim().to_string();
    if with_spaces.is_empty() {
        return name.to_string();
    }
    let mut chars = with_spaces.chars();
    match chars.next() {
        Some(first) => first.to_uppercase().collect::<String>() + chars.as_str(),
        None => with_spaces,
    }
}

/// Trim a statement and ensure it ends with `;` (on a line of its own when
/// the statement ends in a line comment).
///
/// Returns `None` when the trimmed input is empty.
fn normalise_statement(stmt: &str) -> Option<String> {
    let trimmed = stmt.trim();
    (!trimmed.is_empty()).then(|| lexer::terminate_statement(trimmed))
}

/// Render the complete file content for a non-blank migration.
fn render_content(
    version: &str,
    description: &str,
    author: &str,
    depends_on: &[String],
    up_statements: &[String],
    down_statements: &[String],
) -> String {
    let mut out = String::new();
    out.push_str("-- @metadata\n");
    let _ = writeln!(out, "-- version: {}", single_line(version));
    let _ = writeln!(out, "-- description: {}", single_line(description));
    let _ = writeln!(out, "-- author: {}", single_line(author));
    if depends_on.is_empty() {
        out.push_str("-- depends_on: \n");
    } else {
        let _ = writeln!(
            out,
            "-- depends_on: [{}]",
            single_line(&depends_on.join(", "))
        );
    }

    out.push_str("-- @up\n");
    for stmt in up_statements {
        out.push_str(stmt);
        out.push('\n');
    }

    out.push_str("-- @down\n");
    for stmt in down_statements {
        out.push_str(stmt);
        out.push('\n');
    }

    out
}

/// Render the complete file content for an empty-template migration.
fn render_blank_content(version: &str, description: &str, author: &str) -> String {
    let mut out = String::new();
    out.push_str("-- @metadata\n");
    let _ = writeln!(out, "-- version: {}", single_line(version));
    let _ = writeln!(out, "-- description: {}", single_line(description));
    let _ = writeln!(out, "-- author: {}", single_line(author));
    out.push_str("-- depends_on: \n");
    out.push_str("-- @up\n");
    // Intentionally left blank: fill in with forward migration statements.
    out.push('\n');
    out.push_str("-- @down\n");
    // Intentionally left blank: fill in with rollback statements.
    out.push('\n');
    out
}

/// Monotonic counter to disambiguate temp filenames within a single process.
static TEMP_FILE_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Write a migration file atomically and return the loaded [`Migration`].
///
/// The write goes to a sibling temp file (`{filename}.tmp.{pid}.{n}`),
/// then `rename`s into place. If any step fails the temp file is best-
/// effort removed. After a successful rename the file is parsed back
/// via [`load_migration`] to guarantee round-trip correctness.
fn write_migration_file(directory: &Path, filename: &str, content: &str) -> Result<Migration> {
    fs::create_dir_all(directory).map_err(|e| SurqlError::MigrationGeneration {
        reason: format!(
            "failed to create migration directory {}: {e}",
            directory.display()
        ),
    })?;

    let target = directory.join(filename);
    let temp = directory.join(temp_filename(filename));

    let write_result = (|| -> Result<()> {
        let mut file = fs::File::create(&temp).map_err(|e| SurqlError::MigrationGeneration {
            reason: format!(
                "failed to create temp migration file {}: {e}",
                temp.display()
            ),
        })?;
        file.write_all(content.as_bytes())
            .map_err(|e| SurqlError::MigrationGeneration {
                reason: format!("failed to write migration content: {e}"),
            })?;
        file.sync_all()
            .map_err(|e| SurqlError::MigrationGeneration {
                reason: format!("failed to flush migration file: {e}"),
            })?;
        drop(file);

        publish_without_clobbering(&temp, &target)
    })();

    // The temp file is gone after a rename, and a leftover after a link.
    let _ = fs::remove_file(&temp);
    write_result?;

    load_migration(&target).map_err(|e| SurqlError::MigrationGeneration {
        reason: format!(
            "generated file {} failed to round-trip through load_migration: {e}",
            target.display()
        ),
    })
}

/// Move the finished `temp` file to `target`, refusing to replace an
/// existing migration. A hard link fails atomically when `target` exists;
/// where the filesystem cannot link, an existence check guards a rename.
fn publish_without_clobbering(temp: &Path, target: &Path) -> Result<()> {
    let exists = || SurqlError::MigrationGeneration {
        reason: format!(
            "refusing to overwrite existing migration {}",
            target.display()
        ),
    };
    match fs::hard_link(temp, target) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => Err(exists()),
        Err(_) if target.exists() => Err(exists()),
        Err(_) => fs::rename(temp, target).map_err(|e| SurqlError::MigrationGeneration {
            reason: format!(
                "failed to rename {} to {}: {e}",
                temp.display(),
                target.display()
            ),
        }),
    }
}

/// Build a unique temp filename for atomic writes.
fn temp_filename(base: &str) -> String {
    let pid = std::process::id();
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos());
    let n = TEMP_FILE_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{base}.tmp.{pid}.{nanos}.{n}")
}

/// Build the initial-migration up/down statement lists from a snapshot.
///
/// Up: the full `DEFINE` script with `IF NOT EXISTS` split into one
/// statement per line.
///
/// Down: `REMOVE TABLE IF EXISTS {name};` for every edge then every
/// table, in the same name order as the registry (stable).
fn build_initial_statements(snapshot: &SchemaSnapshot) -> Result<(Vec<String>, Vec<String>)> {
    let tables_map: BTreeMap<String, TableDefinition> = snapshot
        .tables
        .iter()
        .map(|t| (t.name.clone(), t.clone()))
        .collect();
    let edges_map: BTreeMap<String, EdgeDefinition> = snapshot
        .edges
        .iter()
        .map(|e| (e.name.clone(), e.clone()))
        .collect();

    let raw = generate_schema_sql(Some(&tables_map), Some(&edges_map), true).map_err(|e| {
        SurqlError::MigrationGeneration {
            reason: format!("failed to render initial schema SQL: {e}"),
        }
    })?;

    let mut up_statements: Vec<String> = raw
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(str::to_string)
        .collect();

    // Buckets are database-level objects independent of tables; append their
    // `DEFINE BUCKET ... IF NOT EXISTS` statements after the table/edge DDL.
    let buckets_map: BTreeMap<String, BucketDefinition> = snapshot
        .buckets
        .iter()
        .map(|b| (b.name.clone(), b.clone()))
        .collect();
    for bucket in buckets_map.values() {
        let stmt = bucket.to_surql_with_options(true, false).map_err(|e| {
            SurqlError::MigrationGeneration {
                reason: format!("failed to render bucket {}: {e}", bucket.name),
            }
        })?;
        up_statements.push(stmt);
    }

    let mut down_statements: Vec<String> = Vec::new();
    // Drop buckets first (independent), then edges (reference tables), then
    // tables.
    for bucket_name in buckets_map.keys().rev() {
        down_statements.push(format!("REMOVE BUCKET {};", quote_ident(bucket_name)));
    }
    for edge_name in edges_map.keys().rev() {
        down_statements.push(format!(
            "REMOVE TABLE IF EXISTS {};",
            quote_ident(edge_name)
        ));
    }
    for table_name in tables_map.keys().rev() {
        down_statements.push(format!(
            "REMOVE TABLE IF EXISTS {};",
            quote_ident(table_name)
        ));
    }

    Ok((up_statements, down_statements))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests;
