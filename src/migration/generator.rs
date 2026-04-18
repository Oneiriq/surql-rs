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
//! Files are written via a temporary sibling file + `rename` so that a
//! crash mid-write cannot leave a partially-written migration on disk.
//! Readers that enumerate the directory will either see the old state
//! (no file) or the new state (complete file), never a torn write.
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

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::fs;
use std::io::Write as _;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use chrono::Utc;

use crate::error::{Result, SurqlError};
use crate::migration::diff::SchemaSnapshot;
use crate::migration::discovery::load_migration;
use crate::migration::models::{Migration, SchemaDiff};
use crate::schema::edge::EdgeDefinition;
use crate::schema::sql::generate_schema_sql;
use crate::schema::table::TableDefinition;
use crate::schema::SchemaRegistry;

/// Default author string written to the `-- @metadata` section.
const DEFAULT_AUTHOR: &str = "surql";

/// Generate a migration file from explicit up/down statement lists.
///
/// Writes the migration atomically to `directory`, using the current UTC
/// timestamp for the version, and returns the loaded [`Migration`] so
/// callers can use it immediately without re-parsing from disk.
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
    let version = generate_version();
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

    if tables.is_empty() && edges.is_empty() {
        return Err(SurqlError::MigrationGeneration {
            reason: "registry is empty: cannot generate initial migration".to_string(),
        });
    }

    let snapshot = SchemaSnapshot {
        tables: tables.values().cloned().collect(),
        edges: edges.values().cloned().collect(),
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
    let version = generate_version();
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

    let up_statements: Vec<String> = diffs
        .iter()
        .filter_map(|d| normalise_statement(&d.forward_sql))
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

/// Generate a UTC timestamp version string (`YYYYMMDD_HHMMSS`).
fn generate_version() -> String {
    Utc::now().format("%Y%m%d_%H%M%S").to_string()
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

/// Trim a statement and ensure it ends with `;`.
///
/// Returns `None` when the trimmed input is empty.
fn normalise_statement(stmt: &str) -> Option<String> {
    let trimmed = stmt.trim();
    if trimmed.is_empty() {
        return None;
    }
    if trimmed.ends_with(';') {
        Some(trimmed.to_string())
    } else {
        Some(format!("{trimmed};"))
    }
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
    let _ = writeln!(out, "-- version: {version}");
    let _ = writeln!(out, "-- description: {description}");
    let _ = writeln!(out, "-- author: {author}");
    if depends_on.is_empty() {
        out.push_str("-- depends_on: \n");
    } else {
        let _ = writeln!(out, "-- depends_on: [{}]", depends_on.join(", "));
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
    let _ = writeln!(out, "-- version: {version}");
    let _ = writeln!(out, "-- description: {description}");
    let _ = writeln!(out, "-- author: {author}");
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

        fs::rename(&temp, &target).map_err(|e| SurqlError::MigrationGeneration {
            reason: format!(
                "failed to rename {} to {}: {e}",
                temp.display(),
                target.display()
            ),
        })?;
        Ok(())
    })();

    if let Err(err) = write_result {
        let _ = fs::remove_file(&temp);
        return Err(err);
    }

    load_migration(&target).map_err(|e| SurqlError::MigrationGeneration {
        reason: format!(
            "generated file {} failed to round-trip through load_migration: {e}",
            target.display()
        ),
    })
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

    let up_statements: Vec<String> = raw
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(str::to_string)
        .collect();

    let mut down_statements: Vec<String> = Vec::new();
    // Drop edges first (they reference tables), then tables.
    for edge_name in edges_map.keys().rev() {
        down_statements.push(format!("REMOVE TABLE IF EXISTS {edge_name};"));
    }
    for table_name in tables_map.keys().rev() {
        down_statements.push(format!("REMOVE TABLE IF EXISTS {table_name};"));
    }

    Ok((up_statements, down_statements))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::migration::models::DiffOperation;
    use crate::schema::edge::typed_edge;
    use crate::schema::table::{table_schema, TableMode};
    use std::path::PathBuf;

    fn unique_temp_dir(tag: &str) -> PathBuf {
        let nanos: u128 = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos());
        let n = TEMP_FILE_COUNTER.fetch_add(1, Ordering::Relaxed);
        let pid = std::process::id();
        let dir = std::env::temp_dir().join(format!("surql-gen-{tag}-{pid}-{nanos}-{n}"));
        fs::create_dir_all(&dir).expect("create temp dir");
        dir
    }

    fn cleanup(dir: &Path) {
        let _ = fs::remove_dir_all(dir);
    }

    // --- generate_version ---------------------------------------------------

    #[test]
    fn version_has_expected_format() {
        let v = generate_version();
        assert_eq!(v.len(), 15, "expected YYYYMMDD_HHMMSS (15 chars)");
        assert_eq!(v.chars().nth(8), Some('_'));
        let (date, time) = v.split_once('_').unwrap();
        assert!(date.chars().all(|c| c.is_ascii_digit()));
        assert!(time.chars().all(|c| c.is_ascii_digit()));
        assert_eq!(date.len(), 8);
        assert_eq!(time.len(), 6);
    }

    #[test]
    fn version_is_monotonic_across_calls() {
        let v1 = generate_version();
        std::thread::sleep(std::time::Duration::from_secs(1));
        let v2 = generate_version();
        assert!(v2 >= v1, "expected {v2} >= {v1}");
    }

    // --- sanitize_name ------------------------------------------------------

    #[test]
    fn sanitize_lowercases_and_replaces_spaces() {
        assert_eq!(
            sanitize_name("Create User Table").unwrap(),
            "create_user_table"
        );
    }

    #[test]
    fn sanitize_strips_punctuation() {
        assert_eq!(sanitize_name("fix bug #123!").unwrap(), "fix_bug_123");
    }

    #[test]
    fn sanitize_keeps_underscores() {
        assert_eq!(sanitize_name("add_user_email").unwrap(), "add_user_email");
    }

    #[test]
    fn sanitize_rejects_empty() {
        assert!(sanitize_name("").is_err());
        assert!(sanitize_name("!!!").is_err());
        assert!(sanitize_name("   ").is_err());
    }

    // --- description_from_name ---------------------------------------------

    #[test]
    fn description_from_name_replaces_underscores() {
        assert_eq!(
            description_from_name("create_user_table"),
            "Create user table"
        );
    }

    #[test]
    fn description_from_name_capitalises_first() {
        assert_eq!(description_from_name("fix"), "Fix");
    }

    // --- normalise_statement -----------------------------------------------

    #[test]
    fn normalise_adds_trailing_semicolon() {
        assert_eq!(
            normalise_statement("SELECT 1").as_deref(),
            Some("SELECT 1;"),
        );
    }

    #[test]
    fn normalise_preserves_trailing_semicolon() {
        assert_eq!(
            normalise_statement("SELECT 1;").as_deref(),
            Some("SELECT 1;"),
        );
    }

    #[test]
    fn normalise_trims_whitespace() {
        assert_eq!(
            normalise_statement("  SELECT 1;\n").as_deref(),
            Some("SELECT 1;"),
        );
    }

    #[test]
    fn normalise_empty_returns_none() {
        assert!(normalise_statement("").is_none());
        assert!(normalise_statement("   \n\t  ").is_none());
    }

    // --- filename layout ---------------------------------------------------

    #[test]
    fn filename_matches_pattern() {
        let dir = unique_temp_dir("filename");
        let m = generate_migration(
            "Create user",
            &["DEFINE TABLE user SCHEMAFULL;".to_string()],
            &["REMOVE TABLE user;".to_string()],
            &dir,
        )
        .unwrap();
        let filename = m.path.file_name().unwrap().to_str().unwrap();
        assert!(filename.ends_with("_create_user.surql"));
        assert_eq!(&filename[8..9], "_");
        assert_eq!(&filename[15..16], "_");

        cleanup(&dir);
    }

    // --- generate_migration happy path -------------------------------------

    #[test]
    fn generate_migration_round_trips_through_load_migration() {
        let dir = unique_temp_dir("roundtrip");

        let up = vec![
            "DEFINE TABLE user SCHEMAFULL;".to_string(),
            "DEFINE FIELD email ON TABLE user TYPE string;".to_string(),
        ];
        let down = vec!["REMOVE TABLE user;".to_string()];

        let m = generate_migration("create_user", &up, &down, &dir).unwrap();

        let reloaded = load_migration(&m.path).unwrap();
        assert_eq!(reloaded.up, up);
        assert_eq!(reloaded.down, down);
        assert_eq!(reloaded.description, "Create user");
        assert_eq!(m, reloaded);

        cleanup(&dir);
    }

    #[test]
    fn generate_migration_creates_missing_directory() {
        let parent = unique_temp_dir("mkdir");
        let dir = parent.join("nested/a/b");
        assert!(!dir.exists());

        let m = generate_migration(
            "init",
            &["SELECT 1;".to_string()],
            &["SELECT 2;".to_string()],
            &dir,
        )
        .unwrap();
        assert!(dir.exists());
        assert!(m.path.starts_with(&dir));

        cleanup(&parent);
    }

    #[test]
    fn generate_migration_rejects_invalid_name() {
        let dir = unique_temp_dir("invalid-name");
        let err = generate_migration("!!!", &[], &[], &dir).unwrap_err();
        assert!(matches!(err, SurqlError::MigrationGeneration { .. }));
        assert!(err.to_string().contains("sanitises"));

        cleanup(&dir);
    }

    #[test]
    fn generate_migration_writes_metadata_section() {
        let dir = unique_temp_dir("metadata");
        let m = generate_migration(
            "demo_feature",
            &["SELECT 1;".to_string()],
            &["SELECT 2;".to_string()],
            &dir,
        )
        .unwrap();
        let text = fs::read_to_string(&m.path).unwrap();
        assert!(text.contains("-- @metadata"));
        assert!(text.contains("-- version: "));
        assert!(text.contains("-- description: Demo feature"));
        assert!(text.contains("-- author: surql"));
        assert!(text.contains("-- @up"));
        assert!(text.contains("-- @down"));

        cleanup(&dir);
    }

    #[test]
    fn generate_migration_empty_statements_round_trip_to_empty_vectors() {
        let dir = unique_temp_dir("empty");
        let m = generate_migration("noop", &[], &[], &dir).unwrap();
        assert!(m.up.is_empty());
        assert!(m.down.is_empty());

        cleanup(&dir);
    }

    // --- atomic write -------------------------------------------------------

    #[test]
    fn atomic_write_leaves_no_temp_file_on_success() {
        let dir = unique_temp_dir("atomic-ok");
        generate_migration(
            "ok",
            &["SELECT 1;".to_string()],
            &["SELECT 2;".to_string()],
            &dir,
        )
        .unwrap();

        let leftover = fs::read_dir(&dir)
            .unwrap()
            .filter_map(std::result::Result::ok)
            .any(|e| e.file_name().to_string_lossy().contains(".tmp."));
        assert!(!leftover, "temp files should be gone after success");

        cleanup(&dir);
    }

    #[test]
    fn atomic_write_rejects_when_directory_is_a_file() {
        let parent = unique_temp_dir("atomic-bad");
        let path_as_file = parent.join("nota_dir");
        fs::write(&path_as_file, "blocker").unwrap();

        let err = generate_migration(
            "x",
            &["SELECT 1;".to_string()],
            &["SELECT 2;".to_string()],
            &path_as_file,
        )
        .unwrap_err();
        assert!(matches!(err, SurqlError::MigrationGeneration { .. }));

        cleanup(&parent);
    }

    // --- create_blank_migration --------------------------------------------

    #[test]
    fn blank_migration_round_trips_to_empty_statements() {
        let dir = unique_temp_dir("blank");
        let m = create_blank_migration("manual_fix", "Manual data fix", &dir).unwrap();
        assert!(m.up.is_empty());
        assert!(m.down.is_empty());
        assert_eq!(m.description, "Manual data fix");

        let reloaded = load_migration(&m.path).unwrap();
        assert_eq!(m, reloaded);

        cleanup(&dir);
    }

    #[test]
    fn blank_migration_uses_name_when_description_empty() {
        let dir = unique_temp_dir("blank-nodesc");
        let m = create_blank_migration("seed_users", "", &dir).unwrap();
        assert_eq!(m.description, "Seed users");

        cleanup(&dir);
    }

    #[test]
    fn blank_migration_rejects_empty_name() {
        let dir = unique_temp_dir("blank-empty");
        let err = create_blank_migration("", "desc", &dir).unwrap_err();
        assert!(matches!(err, SurqlError::MigrationGeneration { .. }));

        cleanup(&dir);
    }

    #[test]
    fn blank_migration_filename_has_surql_extension() {
        let dir = unique_temp_dir("blank-ext");
        let m = create_blank_migration("test", "Test", &dir).unwrap();
        assert!(m.path.extension().and_then(|s| s.to_str()) == Some("surql"));

        cleanup(&dir);
    }

    // --- generate_initial_migration ----------------------------------------

    #[test]
    fn initial_migration_from_single_table() {
        let dir = unique_temp_dir("initial-one");
        let registry = SchemaRegistry::new();
        registry.register_table(table_schema("user").with_mode(TableMode::Schemafull));

        let m = generate_initial_migration(&registry, &dir).unwrap();
        assert_eq!(m.description, "Initial schema");
        assert!(m
            .up
            .iter()
            .any(|s| s.contains("DEFINE TABLE IF NOT EXISTS user")));
        assert!(m
            .down
            .iter()
            .any(|s| s.contains("REMOVE TABLE IF EXISTS user")));

        cleanup(&dir);
    }

    #[test]
    fn initial_migration_from_multi_table_registry() {
        let dir = unique_temp_dir("initial-multi");
        let registry = SchemaRegistry::new();
        registry.register_table(table_schema("user").with_mode(TableMode::Schemafull));
        registry.register_table(table_schema("post").with_mode(TableMode::Schemafull));
        registry.register_table(table_schema("comment").with_mode(TableMode::Schemafull));
        registry.register_edge(typed_edge("likes", "user", "post"));

        let m = generate_initial_migration(&registry, &dir).unwrap();

        // All tables + edges present in up.
        for name in ["user", "post", "comment", "likes"] {
            assert!(
                m.up.iter()
                    .any(|s| s.contains(&format!("DEFINE TABLE IF NOT EXISTS {name}"))),
                "expected DEFINE for {name} in up"
            );
        }

        // All tables + edges present in down.
        for name in ["user", "post", "comment", "likes"] {
            assert!(
                m.down
                    .iter()
                    .any(|s| s.contains(&format!("REMOVE TABLE IF EXISTS {name}"))),
                "expected REMOVE for {name} in down"
            );
        }

        // Round-trip.
        let reloaded = load_migration(&m.path).unwrap();
        assert_eq!(m, reloaded);

        cleanup(&dir);
    }

    #[test]
    fn initial_migration_errors_on_empty_registry() {
        let dir = unique_temp_dir("initial-empty");
        let registry = SchemaRegistry::new();
        let err = generate_initial_migration(&registry, &dir).unwrap_err();
        assert!(matches!(err, SurqlError::MigrationGeneration { .. }));
        assert!(err.to_string().contains("registry is empty"));

        cleanup(&dir);
    }

    #[test]
    fn initial_migration_up_uses_if_not_exists() {
        let dir = unique_temp_dir("initial-ifne");
        let registry = SchemaRegistry::new();
        registry.register_table(table_schema("user").with_mode(TableMode::Schemafull));

        let m = generate_initial_migration(&registry, &dir).unwrap();
        assert!(m
            .up
            .iter()
            .all(|s| !s.contains("DEFINE") || s.contains("IF NOT EXISTS")));

        cleanup(&dir);
    }

    // --- generate_migration_from_diffs -------------------------------------

    fn make_add_table_diff(name: &str) -> SchemaDiff {
        SchemaDiff {
            operation: DiffOperation::AddTable,
            table: name.to_string(),
            field: None,
            index: None,
            event: None,
            description: format!("Add {name} table"),
            forward_sql: format!("DEFINE TABLE {name} SCHEMAFULL;"),
            backward_sql: format!("REMOVE TABLE {name};"),
            details: BTreeMap::new(),
        }
    }

    #[test]
    fn from_diffs_combines_forward_and_backward_sql() {
        let dir = unique_temp_dir("diffs-basic");
        let diffs = vec![make_add_table_diff("user"), make_add_table_diff("post")];

        let m = generate_migration_from_diffs("initial_tables", &diffs, &dir).unwrap();
        assert_eq!(
            m.up,
            vec![
                "DEFINE TABLE user SCHEMAFULL;",
                "DEFINE TABLE post SCHEMAFULL;"
            ]
        );
        // Down is reverse order.
        assert_eq!(m.down, vec!["REMOVE TABLE post;", "REMOVE TABLE user;"]);

        cleanup(&dir);
    }

    #[test]
    fn from_diffs_errors_on_empty_input() {
        let dir = unique_temp_dir("diffs-empty");
        let err = generate_migration_from_diffs("noop", &[], &dir).unwrap_err();
        assert!(matches!(err, SurqlError::MigrationGeneration { .. }));

        cleanup(&dir);
    }

    #[test]
    fn from_diffs_filters_empty_sql_entries() {
        let dir = unique_temp_dir("diffs-skip");
        let diffs = vec![
            SchemaDiff {
                operation: DiffOperation::AddTable,
                table: "x".into(),
                field: None,
                index: None,
                event: None,
                description: "x".into(),
                forward_sql: String::new(),
                backward_sql: String::new(),
                details: BTreeMap::new(),
            },
            make_add_table_diff("keep"),
        ];

        let m = generate_migration_from_diffs("mixed", &diffs, &dir).unwrap();
        assert_eq!(m.up, vec!["DEFINE TABLE keep SCHEMAFULL;"]);
        assert_eq!(m.down, vec!["REMOVE TABLE keep;"]);

        cleanup(&dir);
    }

    #[test]
    fn from_diffs_normalises_missing_semicolons() {
        let dir = unique_temp_dir("diffs-semi");
        let diff = SchemaDiff {
            operation: DiffOperation::AddTable,
            table: "x".into(),
            field: None,
            index: None,
            event: None,
            description: "x".into(),
            forward_sql: "DEFINE TABLE x SCHEMAFULL".into(),
            backward_sql: "REMOVE TABLE x".into(),
            details: BTreeMap::new(),
        };

        let m = generate_migration_from_diffs("semi", &[diff], &dir).unwrap();
        assert_eq!(m.up, vec!["DEFINE TABLE x SCHEMAFULL;"]);
        assert_eq!(m.down, vec!["REMOVE TABLE x;"]);

        cleanup(&dir);
    }

    #[test]
    fn from_diffs_round_trips_through_load_migration() {
        let dir = unique_temp_dir("diffs-rt");
        let diffs = vec![make_add_table_diff("user"), make_add_table_diff("post")];

        let m = generate_migration_from_diffs("initial", &diffs, &dir).unwrap();
        let reloaded = load_migration(&m.path).unwrap();
        assert_eq!(m, reloaded);

        cleanup(&dir);
    }

    // --- render_content formatting -----------------------------------------

    #[test]
    fn rendered_content_orders_sections_correctly() {
        let text = render_content(
            "20260102_120000",
            "demo",
            "surql",
            &[],
            &["SELECT 1;".into()],
            &["SELECT 2;".into()],
        );

        let meta_idx = text.find("-- @metadata").unwrap();
        let up_idx = text.find("-- @up").unwrap();
        let down_idx = text.find("-- @down").unwrap();
        assert!(meta_idx < up_idx);
        assert!(up_idx < down_idx);
    }

    #[test]
    fn rendered_content_includes_depends_on_list() {
        let text = render_content(
            "20260102_120000",
            "demo",
            "surql",
            &["v0".to_string(), "v00".to_string()],
            &[],
            &[],
        );
        assert!(text.contains("-- depends_on: [v0, v00]"));
    }

    #[test]
    fn rendered_blank_content_has_both_sections() {
        let text = render_blank_content("20260102_120000", "demo", "surql");
        assert!(text.contains("-- @up"));
        assert!(text.contains("-- @down"));
        assert!(text.contains("-- version: 20260102_120000"));
    }

    // --- temp_filename uniqueness ------------------------------------------

    #[test]
    fn temp_filename_has_expected_prefix_and_counter() {
        let a = temp_filename("x.surql");
        let b = temp_filename("x.surql");
        assert!(a.starts_with("x.surql.tmp."));
        assert!(b.starts_with("x.surql.tmp."));
        assert_ne!(a, b);
    }

    // --- path stability ----------------------------------------------------

    #[test]
    fn generate_migration_returns_path_inside_directory() {
        let dir = unique_temp_dir("path-in");
        let m = generate_migration(
            "x",
            &["SELECT 1;".to_string()],
            &["SELECT 2;".to_string()],
            &dir,
        )
        .unwrap();
        assert!(m.path.starts_with(&dir));

        cleanup(&dir);
    }

    #[test]
    fn generated_file_exists_after_successful_write() {
        let dir = unique_temp_dir("path-exists");
        let m = generate_migration(
            "x",
            &["SELECT 1;".to_string()],
            &["SELECT 2;".to_string()],
            &dir,
        )
        .unwrap();
        assert!(m.path.is_file());

        cleanup(&dir);
    }
}
