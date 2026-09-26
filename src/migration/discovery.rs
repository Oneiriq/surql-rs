//! Migration file discovery and loading.
//!
//! Port of `surql/migration/discovery.py`. Provides functions for discovering
//! migration files in a directory and loading them into [`Migration`] objects.
//!
//! ## File format
//!
//! Python migrations are `.py` modules imported at runtime via `importlib`
//! with a `metadata` dict and `up()` / `down()` functions. Rust cannot
//! execute Python at runtime, so the port uses flat `.surql` files with
//! comment-based section markers:
//!
//! ```surql,ignore
//! -- @metadata
//! -- version: 20260102_120000
//! -- description: Create user table
//! -- author: surql
//! -- depends_on: v0,v00
//! -- @up
//! DEFINE TABLE user SCHEMAFULL;
//! DEFINE FIELD email ON TABLE user TYPE string;
//! -- @down
//! REMOVE TABLE user;
//! ```
//!
//! Filename pattern: `YYYYMMDD_HHMMSS_description.surql`.
//!
//! Parsing rules:
//! * `-- @metadata` / `-- @up` / `-- @down` are section markers on their own
//!   line (trailing whitespace tolerated).
//! * Inside `-- @metadata`, each `-- key: value` line sets a field. Unknown
//!   keys are ignored.
//! * `-- @up` and `-- @down` bodies are split into statements on `;`. A `;`
//!   inside a comment (`--`, `//`, `#`, `/* */`), a string literal, a quoted
//!   identifier, or a `{ }` / `( )` / `[ ]` block does not end a statement.
//!   Each statement keeps its text verbatim, trailing `;` included; pieces
//!   holding only whitespace and comments are discarded.
//! * `@up` and `@down` are both required; `@metadata` is optional (version
//!   and description fall back to the filename when absent).
//! * A leading UTF-8 byte-order mark is ignored.

use std::cmp::Ordering;
use std::collections::BTreeSet;
use std::fmt::Write as _;
use std::fs;
use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};

use crate::error::{Result, SurqlError};
use crate::migration::lexer;
use crate::migration::models::{Migration, MigrationMetadata};

/// Discover all migration files in a directory.
///
/// Scans a directory for `.surql` files matching the migration filename
/// pattern and loads them in sorted order by version.
///
/// Files whose names do not match the migration pattern (e.g. `README.surql`)
/// are skipped with no error. Files whose names start with `_` are also
/// skipped, matching the Python behaviour for `__init__.py` and private
/// files.
///
/// # Errors
///
/// Returns [`SurqlError::MigrationDiscovery`] if the path exists but is not
/// a directory, or if any individual migration fails to load. If the
/// directory simply does not exist, an empty vector is returned (matching
/// Python's behaviour).
///
/// ## Examples
///
/// ```no_run
/// use std::path::Path;
/// use surql::migration::discover_migrations;
///
/// let migrations = discover_migrations(Path::new("migrations")).unwrap();
/// for m in &migrations {
///     println!("{} - {}", m.version, m.description);
/// }
/// ```
pub fn discover_migrations(directory: &Path) -> Result<Vec<Migration>> {
    if !directory.exists() {
        return Ok(Vec::new());
    }

    if !directory.is_dir() {
        return Err(SurqlError::MigrationDiscovery {
            reason: format!("path is not a directory: {}", directory.display()),
        });
    }

    let entries = fs::read_dir(directory).map_err(|e| SurqlError::MigrationDiscovery {
        reason: format!("failed to read directory {}: {e}", directory.display()),
    })?;

    let mut migration_files: Vec<PathBuf> = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|e| SurqlError::MigrationDiscovery {
            reason: format!("failed to read entry in {}: {e}", directory.display()),
        })?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }

        let Some(file_name) = path.file_name().and_then(|n| n.to_str()) else {
            continue;
        };

        if file_name.starts_with('_') {
            continue;
        }

        if !validate_migration_name(file_name) {
            continue;
        }

        migration_files.push(path);
    }

    migration_files.sort();

    let mut migrations = Vec::with_capacity(migration_files.len());
    for file_path in migration_files {
        let migration = load_migration(&file_path)?;
        migrations.push(migration);
    }

    order_migrations(migrations)
}

/// Order migrations for applying: by version, comparing digit runs as
/// numbers (so `v9` sorts before `v10`), and then moved as little as
/// needed for every migration to follow the ones it `depends_on`.
/// Dependencies on versions outside `migrations` do not constrain the
/// order ([`crate::migration::validate_migrations`] reports them).
///
/// # Errors
///
/// Returns [`SurqlError::MigrationDiscovery`] when the dependencies form a
/// cycle.
pub(crate) fn order_migrations(mut migrations: Vec<Migration>) -> Result<Vec<Migration>> {
    migrations.sort_by(|a, b| compare_versions(&a.version, &b.version));
    let mut ordered: Vec<Migration> = Vec::with_capacity(migrations.len());
    let mut placed: BTreeSet<String> = BTreeSet::new();
    let known: BTreeSet<String> = migrations.iter().map(|m| m.version.clone()).collect();
    while !migrations.is_empty() {
        // The earliest migration whose dependencies are all placed.
        let ready = migrations.iter().position(|m| {
            m.depends_on
                .iter()
                .all(|dep| placed.contains(dep) || !known.contains(dep) || *dep == m.version)
        });
        let Some(idx) = ready else {
            let stuck: Vec<&str> = migrations.iter().map(|m| m.version.as_str()).collect();
            return Err(SurqlError::MigrationDiscovery {
                reason: format!(
                    "migration dependencies form a cycle among: {}",
                    stuck.join(", ")
                ),
            });
        };
        let next = migrations.remove(idx);
        placed.insert(next.version.clone());
        ordered.push(next);
    }
    Ok(ordered)
}

/// Compare migration versions, reading runs of ASCII digits as numbers:
/// `v9` < `v10`, and `YYYYMMDD_HHMMSS` timestamps compare as they always
/// did. Versions whose runs are numerically equal (`v01`, `v1`) fall back
/// to plain string order, so only equal strings compare equal.
pub(crate) fn compare_versions(a: &str, b: &str) -> Ordering {
    let mut left = version_runs(a);
    let mut right = version_runs(b);
    loop {
        match (left.next(), right.next()) {
            (None, None) => return a.cmp(b),
            (None, Some(_)) => return Ordering::Less,
            (Some(_), None) => return Ordering::Greater,
            (Some(x), Some(y)) => {
                let digits = |s: &str| s.bytes().all(|b| b.is_ascii_digit());
                let order = if digits(x) && digits(y) {
                    let (x, y) = (x.trim_start_matches('0'), y.trim_start_matches('0'));
                    x.len().cmp(&y.len()).then_with(|| x.cmp(y))
                } else {
                    x.cmp(y)
                };
                if order != Ordering::Equal {
                    return order;
                }
            }
        }
    }
}

/// Split `s` into maximal runs of ASCII digits and of everything else.
fn version_runs(s: &str) -> impl Iterator<Item = &str> {
    let mut rest = s;
    std::iter::from_fn(move || {
        let first = rest.chars().next()?;
        let digit = first.is_ascii_digit();
        let len = rest
            .find(|c: char| c.is_ascii_digit() != digit)
            .unwrap_or(rest.len());
        let (run, tail) = rest.split_at(len);
        rest = tail;
        Some(run)
    })
}

/// Load a single migration file.
///
/// Reads the file at `path`, parses the `@metadata`, `@up` and `@down`
/// sections, and returns a [`Migration`] with a SHA-256 checksum of the
/// file content. The checksum ignores a byte-order mark and `\r\n` versus
/// `\n` line endings, so a checkout on any platform hashes the same.
///
/// # Errors
///
/// Returns [`SurqlError::MigrationLoad`] if the file does not exist, is not
/// a regular file, cannot be read, or is missing required sections (`@up`
/// or `@down`).
///
/// ## Examples
///
/// ```no_run
/// use std::path::Path;
/// use surql::migration::load_migration;
///
/// let m = load_migration(Path::new("migrations/20260102_120000_create_user.surql")).unwrap();
/// assert_eq!(m.version, "20260102_120000");
/// ```
pub fn load_migration(path: &Path) -> Result<Migration> {
    if !path.exists() {
        return Err(SurqlError::MigrationLoad {
            reason: format!("migration file not found: {}", path.display()),
        });
    }

    if !path.is_file() {
        return Err(SurqlError::MigrationLoad {
            reason: format!("path is not a file: {}", path.display()),
        });
    }

    let content = fs::read_to_string(path).map_err(|e| SurqlError::MigrationLoad {
        reason: format!("failed to read migration file {}: {e}", path.display()),
    })?;

    let parsed = parse_migration_content(&content, path)?;

    let file_name =
        path.file_name()
            .and_then(|n| n.to_str())
            .ok_or_else(|| SurqlError::MigrationLoad {
                reason: format!("invalid migration path: {}", path.display()),
            })?;

    let (version, description) = resolve_identity(parsed.metadata.as_ref(), file_name, path)?;

    let (depends_on, squashed_from) = parsed
        .metadata
        .map(|m| (m.depends_on, m.squashed_from))
        .unwrap_or_default();

    let checksum = content_checksum(&content);

    Ok(Migration {
        version,
        description,
        path: path.to_path_buf(),
        up: parsed.up,
        down: parsed.down,
        checksum: Some(checksum),
        depends_on,
        squashed_from,
    })
}

/// Validate migration filename format.
///
/// Expected format: `YYYYMMDD_HHMMSS_description.surql`.
///
/// ## Examples
///
/// ```
/// use surql::migration::validate_migration_name;
///
/// assert!(validate_migration_name("20260102_120000_create_user.surql"));
/// assert!(!validate_migration_name("invalid.surql"));
/// assert!(!validate_migration_name("20260102_120000_create_user.py"));
/// ```
pub fn validate_migration_name(filename: &str) -> bool {
    name_parts(filename).is_some()
}

/// The `(date, time, description)` parts of a valid migration filename.
fn name_parts(filename: &str) -> Option<(&str, &str, &str)> {
    let stem = filename.strip_suffix(".surql")?;
    let mut parts = stem.splitn(3, '_');
    let (date, time, description) = (parts.next()?, parts.next()?, parts.next()?);
    let digits = |s: &str, len: usize| s.len() == len && s.bytes().all(|b| b.is_ascii_digit());
    // The description must hold something other than separators.
    (digits(date, 8) && digits(time, 6) && description.contains(|c| c != '_')).then_some((
        date,
        time,
        description,
    ))
}

/// Extract version from a migration filename.
///
/// Returns `Some("YYYYMMDD_HHMMSS")` for a valid filename, `None` otherwise.
///
/// ## Examples
///
/// ```
/// use surql::migration::get_version_from_filename;
///
/// assert_eq!(
///     get_version_from_filename("20260102_120000_create_user.surql").as_deref(),
///     Some("20260102_120000"),
/// );
/// assert_eq!(get_version_from_filename("invalid.surql"), None);
/// ```
pub fn get_version_from_filename(filename: &str) -> Option<String> {
    name_parts(filename).map(|(date, time, _)| format!("{date}_{time}"))
}

/// Extract the description portion from a migration filename.
///
/// Joins the third and subsequent underscore-separated parts.
///
/// ## Examples
///
/// ```
/// use surql::migration::get_description_from_filename;
///
/// assert_eq!(
///     get_description_from_filename("20260102_120000_create_user_table.surql").as_deref(),
///     Some("create_user_table"),
/// );
/// assert_eq!(get_description_from_filename("invalid.surql"), None);
/// ```
pub fn get_description_from_filename(filename: &str) -> Option<String> {
    name_parts(filename).map(|(_, _, description)| description.to_string())
}

// ---------------------------------------------------------------------------
// Internals
// ---------------------------------------------------------------------------

struct ParsedMigration {
    metadata: Option<MigrationMetadata>,
    up: Vec<String>,
    down: Vec<String>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Section {
    None,
    Metadata,
    Up,
    Down,
}

fn parse_migration_content(content: &str, path: &Path) -> Result<ParsedMigration> {
    // An editor-added byte-order mark would otherwise hide the first
    // line's `-- @metadata` marker.
    let content = content.strip_prefix(BYTE_ORDER_MARK).unwrap_or(content);
    let mut section = Section::None;

    let mut metadata_version: Option<String> = None;
    let mut metadata_description: Option<String> = None;
    let mut metadata_author: Option<String> = None;
    let mut metadata_depends_on: Vec<String> = Vec::new();
    let mut metadata_squashed_from: Vec<String> = Vec::new();
    let mut saw_metadata = false;

    let mut up_lines: Vec<String> = Vec::new();
    let mut down_lines: Vec<String> = Vec::new();
    let mut saw_up = false;
    let mut saw_down = false;

    for raw_line in content.lines() {
        let line = raw_line.trim_end();
        let trimmed = line.trim_start();

        if let Some(marker) = parse_section_marker(trimmed) {
            section = marker;
            match marker {
                Section::Metadata => saw_metadata = true,
                Section::Up => saw_up = true,
                Section::Down => saw_down = true,
                Section::None => {}
            }
            continue;
        }

        match section {
            Section::None => {
                // Content before any section marker is ignored (allows
                // top-of-file comments or blank lines).
            }
            Section::Metadata => {
                if let Some((key, value)) = parse_metadata_line(trimmed) {
                    match key.as_str() {
                        "version" => metadata_version = Some(value),
                        "description" => metadata_description = Some(value),
                        "author" => metadata_author = Some(value),
                        "depends_on" => metadata_depends_on = parse_version_list(&value),
                        "squashed-from" | "squashed_from" => {
                            metadata_squashed_from = parse_version_list(&value);
                        }
                        _ => {}
                    }
                }
            }
            Section::Up => up_lines.push(line.to_string()),
            Section::Down => down_lines.push(line.to_string()),
        }
    }

    if !saw_up {
        return Err(SurqlError::MigrationLoad {
            reason: format!("migration {} missing -- @up section", path.display()),
        });
    }
    if !saw_down {
        return Err(SurqlError::MigrationLoad {
            reason: format!("migration {} missing -- @down section", path.display()),
        });
    }

    let up = lexer::split_statements(&up_lines.join("\n"));
    let down = lexer::split_statements(&down_lines.join("\n"));

    let metadata = if saw_metadata {
        let version = metadata_version.ok_or_else(|| SurqlError::MigrationLoad {
            reason: format!(
                "migration {} @metadata section missing `version`",
                path.display()
            ),
        })?;
        let description = metadata_description.ok_or_else(|| SurqlError::MigrationLoad {
            reason: format!(
                "migration {} @metadata section missing `description`",
                path.display()
            ),
        })?;
        Some(MigrationMetadata {
            version,
            description,
            author: metadata_author.unwrap_or_else(MigrationMetadata::default_author),
            depends_on: metadata_depends_on,
            squashed_from: metadata_squashed_from,
        })
    } else {
        None
    };

    Ok(ParsedMigration { metadata, up, down })
}

fn parse_section_marker(line: &str) -> Option<Section> {
    let rest = line.strip_prefix("--")?;
    let rest = rest.trim();
    let name = rest.strip_prefix('@')?;
    match name {
        "metadata" => Some(Section::Metadata),
        "up" => Some(Section::Up),
        "down" => Some(Section::Down),
        _ => None,
    }
}

/// `a, b` or `[a, b]` as a list of versions.
fn parse_version_list(value: &str) -> Vec<String> {
    value
        .trim_matches(|c| c == '[' || c == ']')
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

fn parse_metadata_line(line: &str) -> Option<(String, String)> {
    let rest = line.strip_prefix("--")?;
    let rest = rest.trim();
    let (key, value) = rest.split_once(':')?;
    Some((key.trim().to_string(), value.trim().to_string()))
}

const BYTE_ORDER_MARK: char = '\u{FEFF}';

/// SHA-256 of a migration file's text with a byte-order mark and `\r\n`
/// line endings normalised away, so the same file checked out on Windows
/// and Unix hashes the same.
fn content_checksum(content: &str) -> String {
    let content = content.strip_prefix(BYTE_ORDER_MARK).unwrap_or(content);
    sha256_hex(content.replace("\r\n", "\n").as_bytes())
}

/// Lowercase hex SHA-256 of `bytes`.
pub(crate) fn sha256_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .fold(String::with_capacity(64), |mut out, byte| {
            // Writing to a `String` cannot fail.
            let _ = write!(out, "{byte:02x}");
            out
        })
}

fn resolve_identity(
    metadata: Option<&MigrationMetadata>,
    file_name: &str,
    path: &Path,
) -> Result<(String, String)> {
    if let Some(m) = metadata {
        return Ok((m.version.clone(), m.description.clone()));
    }

    let version =
        get_version_from_filename(file_name).ok_or_else(|| SurqlError::MigrationLoad {
            reason: format!(
                "cannot infer version from filename {} and no @metadata section provided",
                path.display()
            ),
        })?;
    let description =
        get_description_from_filename(file_name).ok_or_else(|| SurqlError::MigrationLoad {
            reason: format!(
                "cannot infer description from filename {} and no @metadata section provided",
                path.display()
            ),
        })?;
    Ok((version, description))
}

#[cfg(test)]
mod tests;
