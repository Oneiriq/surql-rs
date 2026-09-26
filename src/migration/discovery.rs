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

    migrations.sort_by(|a, b| a.version.cmp(&b.version));

    Ok(migrations)
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

    let depends_on = parsed
        .metadata
        .as_ref()
        .map(|m| m.depends_on.clone())
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
    let Some(stem) = filename.strip_suffix(".surql") else {
        return false;
    };

    let parts: Vec<&str> = stem.split('_').collect();
    if parts.len() < 3 {
        return false;
    }

    if parts[0].len() != 8 || !parts[0].chars().all(|c| c.is_ascii_digit()) {
        return false;
    }

    if parts[1].len() != 6 || !parts[1].chars().all(|c| c.is_ascii_digit()) {
        return false;
    }

    // Description part must be non-empty.
    !parts[2..].iter().all(|p| p.is_empty())
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
    if !validate_migration_name(filename) {
        return None;
    }
    let stem = filename.strip_suffix(".surql")?;
    let parts: Vec<&str> = stem.split('_').collect();
    Some(format!("{}_{}", parts[0], parts[1]))
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
    if !validate_migration_name(filename) {
        return None;
    }
    let stem = filename.strip_suffix(".surql")?;
    let parts: Vec<&str> = stem.split('_').collect();
    Some(parts[2..].join("_"))
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
                        "depends_on" => {
                            metadata_depends_on = value
                                .trim_matches(|c| c == '[' || c == ']')
                                .split(',')
                                .map(|s| s.trim().to_string())
                                .filter(|s| !s.is_empty())
                                .collect();
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
mod tests {
    use super::*;
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
        let dir = std::env::temp_dir().join(format!("surql-mig-{tag}-{pid}-{nanos}-{n}"));
        fs::create_dir_all(&dir).expect("create temp dir");
        dir
    }

    fn sample_migration_text() -> String {
        String::from(
            "-- @metadata\n\
             -- version: 20260102_120000\n\
             -- description: Create user table\n\
             -- author: surql\n\
             -- depends_on: \n\
             -- @up\n\
             DEFINE TABLE user SCHEMAFULL;\n\
             DEFINE FIELD email ON TABLE user TYPE string;\n\
             -- @down\n\
             REMOVE TABLE user;\n",
        )
    }

    // --- validate_migration_name -------------------------------------------

    #[test]
    fn validate_name_accepts_valid_surql() {
        assert!(validate_migration_name("20260102_120000_create_user.surql"));
    }

    #[test]
    fn validate_name_accepts_description_with_underscores() {
        assert!(validate_migration_name(
            "20260102_120000_create_user_table.surql"
        ));
    }

    #[test]
    fn validate_name_rejects_non_surql_extension() {
        assert!(!validate_migration_name("20260102_120000_create_user.py"));
        assert!(!validate_migration_name("20260102_120000_create_user.sql"));
        assert!(!validate_migration_name("20260102_120000_create_user"));
    }

    #[test]
    fn validate_name_rejects_bad_date_part() {
        assert!(!validate_migration_name(
            "2026_010_120000_create_user.surql"
        ));
        assert!(!validate_migration_name(
            "20260aa2_120000_create_user.surql"
        ));
        assert!(!validate_migration_name("0260102_120000_create_user.surql"));
    }

    #[test]
    fn validate_name_rejects_bad_time_part() {
        assert!(!validate_migration_name("20260102_12000_create_user.surql"));
        assert!(!validate_migration_name(
            "20260102_abcdef_create_user.surql"
        ));
        assert!(!validate_migration_name(
            "20260102_1200000_create_user.surql"
        ));
    }

    #[test]
    fn validate_name_rejects_too_few_parts() {
        assert!(!validate_migration_name("20260102_120000.surql"));
    }

    #[test]
    fn validate_name_rejects_empty_description() {
        assert!(!validate_migration_name("20260102_120000_.surql"));
    }

    #[test]
    fn validate_name_rejects_empty_string() {
        assert!(!validate_migration_name(""));
        assert!(!validate_migration_name(".surql"));
    }

    // --- get_version_from_filename -----------------------------------------

    #[test]
    fn version_from_valid_filename() {
        assert_eq!(
            get_version_from_filename("20260102_120000_create_user.surql").as_deref(),
            Some("20260102_120000"),
        );
    }

    #[test]
    fn version_from_multi_underscore_description() {
        assert_eq!(
            get_version_from_filename("20260102_120000_create_user_table.surql").as_deref(),
            Some("20260102_120000"),
        );
    }

    #[test]
    fn version_from_invalid_filename_is_none() {
        assert!(get_version_from_filename("invalid.surql").is_none());
        assert!(get_version_from_filename("20260102_120000_create_user.py").is_none());
    }

    // --- get_description_from_filename -------------------------------------

    #[test]
    fn description_from_valid_filename() {
        assert_eq!(
            get_description_from_filename("20260102_120000_create_user.surql").as_deref(),
            Some("create_user"),
        );
    }

    #[test]
    fn description_joins_multiple_parts() {
        assert_eq!(
            get_description_from_filename("20260102_120000_create_user_table.surql").as_deref(),
            Some("create_user_table"),
        );
    }

    #[test]
    fn description_from_invalid_filename_is_none() {
        assert!(get_description_from_filename("invalid.surql").is_none());
    }

    // --- load_migration ----------------------------------------------------

    fn split(text: &str) -> Vec<String> {
        lexer::split_statements(text)
    }

    #[test]
    fn sha256_hex_matches_the_fips_vectors() {
        assert_eq!(
            sha256_hex(b""),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
        assert_eq!(
            sha256_hex(b"abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }

    /// A comment carrying a `;` and an apostrophe used to split the
    /// statement after it inside its string literal; the executor then
    /// re-joined the halves with `;\n`, silently changing the stored value.
    #[test]
    fn load_migration_keeps_literals_intact_around_comments() {
        let dir = unique_temp_dir("load-comments");
        let path = dir.join("20260102_120000_notes.surql");
        let text = "-- @up\n\
             DEFINE TABLE a SCHEMAFULL; -- step 1; see JIRA-12\n\
             -- user's table\n\
             CREATE user SET note = 'a;b';\n\
             -- @down\n\
             DELETE user;\n";
        fs::write(&path, text).unwrap();

        let m = load_migration(&path).unwrap();
        assert_eq!(m.up.len(), 2, "{:#?}", m.up);
        assert_eq!(m.up[0], "DEFINE TABLE a SCHEMAFULL;");
        assert!(
            m.up[1].ends_with("CREATE user SET note = 'a;b';"),
            "{}",
            m.up[1]
        );

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn load_migration_ignores_a_byte_order_mark() {
        let dir = unique_temp_dir("load-bom");
        let path = dir.join("20260102_120000_bom.surql");
        let text = "\u{FEFF}-- @metadata\n\
             -- version: 20260102_120000\n\
             -- description: with bom\n\
             -- @up\n\
             SELECT 1;\n\
             -- @down\n\
             SELECT 2;\n";
        fs::write(&path, text).unwrap();

        let m = load_migration(&path).unwrap();
        assert_eq!(m.description, "with bom");

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn checksum_ignores_line_endings_and_byte_order_mark() {
        let unix = "-- @up\nSELECT 1;\n-- @down\nSELECT 2;\n";
        let windows = "\u{FEFF}-- @up\r\nSELECT 1;\r\n-- @down\r\nSELECT 2;\r\n";
        assert_eq!(content_checksum(unix), content_checksum(windows));
        assert_ne!(content_checksum(unix), content_checksum("SELECT 3;"));
    }

    /// A `DEFINE FUNCTION` body and a `FOR` loop both hold semicolons
    /// inside braces; splitting there shatters one statement into
    /// fragments that individually fail to parse. The reference
    /// backfill rewrite is exactly this shape, and it must survive a
    /// trip through a migration file.
    #[test]
    fn split_statements_respects_nesting_and_strings() {
        let function = split(
            "DEFINE FUNCTION fn::double($n: int) { LET $d = $n * 2; RETURN $d; };\n\
             DEFINE TABLE t SCHEMAFULL;",
        );
        assert_eq!(function.len(), 2, "{function:#?}");
        assert!(function[0].contains("RETURN $d;"), "{}", function[0]);

        let dance = split(
            "FOR $rid IN ((SELECT VALUE id FROM f WHERE link IS NOT NONE) ?? []) \
             { LET $held = $rid.link; UPDATE $rid SET link = NONE; \
             UPDATE $rid SET link = $held; };\n\
             DEFINE TABLE t SCHEMAFULL;",
        );
        assert_eq!(dance.len(), 2, "{dance:#?}");
        assert!(dance[0].starts_with("FOR $rid"), "{}", dance[0]);
        assert!(dance[0].ends_with("};"), "{}", dance[0]);

        let strings = split("CREATE t SET s = 'a;{b}(c'; CREATE u SET n = \"d;e\";");
        assert_eq!(strings.len(), 2, "{strings:#?}");

        let escaped = split("CREATE t SET s = 'it\\'s; fine'; CREATE u SET n = 1;");
        assert_eq!(escaped.len(), 2, "{escaped:#?}");

        // The old behaviour survives for the plain cases.
        let plain = split("DEFINE TABLE a SCHEMAFULL; DEFINE TABLE b SCHEMAFULL");
        assert_eq!(plain.len(), 2, "{plain:#?}");
    }

    #[test]
    fn load_migration_happy_path() {
        let dir = unique_temp_dir("load-ok");
        let path = dir.join("20260102_120000_create_user.surql");
        fs::write(&path, sample_migration_text()).unwrap();

        let m = load_migration(&path).unwrap();
        assert_eq!(m.version, "20260102_120000");
        assert_eq!(m.description, "Create user table");
        assert_eq!(m.up.len(), 2);
        assert!(m.up[0].starts_with("DEFINE TABLE user"));
        assert!(m.up[1].starts_with("DEFINE FIELD email"));
        assert_eq!(m.down.len(), 1);
        assert!(m.down[0].starts_with("REMOVE TABLE user"));
        assert!(m.checksum.as_ref().is_some_and(|c| c.len() == 64));
        assert!(m.depends_on.is_empty());

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn load_migration_parses_depends_on_list() {
        let dir = unique_temp_dir("load-deps");
        let path = dir.join("20260102_120000_create_user.surql");
        let text = "-- @metadata\n\
             -- version: 20260102_120000\n\
             -- description: demo\n\
             -- depends_on: [20260101_000000_init, 20260101_000001_seed]\n\
             -- @up\n\
             SELECT 1;\n\
             -- @down\n\
             SELECT 2;\n";
        fs::write(&path, text).unwrap();

        let m = load_migration(&path).unwrap();
        assert_eq!(
            m.depends_on,
            vec![
                "20260101_000000_init".to_string(),
                "20260101_000001_seed".to_string()
            ]
        );

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn load_migration_falls_back_to_filename_when_no_metadata() {
        let dir = unique_temp_dir("load-nometa");
        let path = dir.join("20260102_120000_seed_users.surql");
        let text = "-- @up\nSELECT 1;\n-- @down\nSELECT 2;\n";
        fs::write(&path, text).unwrap();

        let m = load_migration(&path).unwrap();
        assert_eq!(m.version, "20260102_120000");
        assert_eq!(m.description, "seed_users");

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn load_migration_missing_up_section_errors() {
        let dir = unique_temp_dir("load-no-up");
        let path = dir.join("20260102_120000_x.surql");
        fs::write(&path, "-- @down\nSELECT 1;\n").unwrap();

        let err = load_migration(&path).unwrap_err();
        assert!(matches!(err, SurqlError::MigrationLoad { .. }));
        assert!(err.to_string().contains("@up"));

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn load_migration_missing_down_section_errors() {
        let dir = unique_temp_dir("load-no-down");
        let path = dir.join("20260102_120000_x.surql");
        fs::write(&path, "-- @up\nSELECT 1;\n").unwrap();

        let err = load_migration(&path).unwrap_err();
        assert!(matches!(err, SurqlError::MigrationLoad { .. }));
        assert!(err.to_string().contains("@down"));

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn load_migration_missing_metadata_version_errors() {
        let dir = unique_temp_dir("load-no-ver");
        let path = dir.join("20260102_120000_x.surql");
        let text = "-- @metadata\n\
             -- description: demo\n\
             -- @up\n\
             SELECT 1;\n\
             -- @down\n\
             SELECT 2;\n";
        fs::write(&path, text).unwrap();

        let err = load_migration(&path).unwrap_err();
        assert!(matches!(err, SurqlError::MigrationLoad { .. }));
        assert!(err.to_string().contains("version"));

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn load_migration_missing_metadata_description_errors() {
        let dir = unique_temp_dir("load-no-desc");
        let path = dir.join("20260102_120000_x.surql");
        let text = "-- @metadata\n\
             -- version: v1\n\
             -- @up\n\
             SELECT 1;\n\
             -- @down\n\
             SELECT 2;\n";
        fs::write(&path, text).unwrap();

        let err = load_migration(&path).unwrap_err();
        assert!(matches!(err, SurqlError::MigrationLoad { .. }));
        assert!(err.to_string().contains("description"));

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn load_migration_nonexistent_file_errors() {
        let err =
            load_migration(Path::new("/nonexistent/path/to/nothing_xyzzy.surql")).unwrap_err();
        assert!(matches!(err, SurqlError::MigrationLoad { .. }));
    }

    #[test]
    fn load_migration_directory_instead_of_file_errors() {
        let dir = unique_temp_dir("load-is-dir");
        let err = load_migration(&dir).unwrap_err();
        assert!(matches!(err, SurqlError::MigrationLoad { .. }));

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn load_migration_default_author_when_omitted() {
        let dir = unique_temp_dir("load-def-author");
        let path = dir.join("20260102_120000_x.surql");
        let text = "-- @metadata\n\
             -- version: v1\n\
             -- description: d\n\
             -- @up\n\
             SELECT 1;\n\
             -- @down\n\
             SELECT 2;\n";
        fs::write(&path, text).unwrap();

        let m = load_migration(&path).unwrap();
        // Author is not part of Migration; we only check metadata didn't error.
        assert_eq!(m.version, "v1");
        assert_eq!(m.description, "d");

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn load_migration_checksum_changes_with_content() {
        let dir = unique_temp_dir("load-checksum");
        let p1 = dir.join("20260102_120000_a.surql");
        let p2 = dir.join("20260102_120001_b.surql");
        let t1 = "-- @up\nSELECT 1;\n-- @down\nSELECT 2;\n";
        let t2 = "-- @up\nSELECT 3;\n-- @down\nSELECT 4;\n";
        fs::write(&p1, t1).unwrap();
        fs::write(&p2, t2).unwrap();

        let m1 = load_migration(&p1).unwrap();
        let m2 = load_migration(&p2).unwrap();
        assert_ne!(m1.checksum, m2.checksum);

        fs::remove_dir_all(&dir).ok();
    }

    // --- discover_migrations -----------------------------------------------

    #[test]
    fn discover_returns_empty_for_missing_directory() {
        let path = std::env::temp_dir().join("surql-mig-does-not-exist-xyzzy-123");
        let migrations = discover_migrations(&path).unwrap();
        assert!(migrations.is_empty());
    }

    #[test]
    fn discover_errors_when_path_is_file() {
        let dir = unique_temp_dir("disc-is-file");
        let path = dir.join("not_a_dir.surql");
        fs::write(&path, "-- @up\nSELECT 1;\n-- @down\nSELECT 2;\n").unwrap();

        let err = discover_migrations(&path).unwrap_err();
        assert!(matches!(err, SurqlError::MigrationDiscovery { .. }));

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn discover_empty_directory_returns_empty() {
        let dir = unique_temp_dir("disc-empty");
        let migrations = discover_migrations(&dir).unwrap();
        assert!(migrations.is_empty());

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn discover_loads_valid_migrations_sorted() {
        let dir = unique_temp_dir("disc-valid");
        let p1 = dir.join("20260102_120000_a.surql");
        let p2 = dir.join("20260103_120000_b.surql");
        let p3 = dir.join("20260101_120000_c.surql");
        for p in [&p1, &p2, &p3] {
            fs::write(p, "-- @up\nSELECT 1;\n-- @down\nSELECT 2;\n").unwrap();
        }

        let migrations = discover_migrations(&dir).unwrap();
        assert_eq!(migrations.len(), 3);
        assert_eq!(migrations[0].version, "20260101_120000");
        assert_eq!(migrations[1].version, "20260102_120000");
        assert_eq!(migrations[2].version, "20260103_120000");

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn discover_skips_non_matching_files() {
        let dir = unique_temp_dir("disc-skip");
        fs::write(dir.join("README.md"), "readme").unwrap();
        fs::write(dir.join("notes.txt"), "notes").unwrap();
        fs::write(dir.join("not_a_migration.surql"), "-- @up\n-- @down\n").unwrap();
        fs::write(
            dir.join("20260101_120000_good.surql"),
            "-- @up\nSELECT 1;\n-- @down\nSELECT 2;\n",
        )
        .unwrap();

        let migrations = discover_migrations(&dir).unwrap();
        assert_eq!(migrations.len(), 1);
        assert_eq!(migrations[0].version, "20260101_120000");

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn discover_skips_underscore_prefixed_files() {
        let dir = unique_temp_dir("disc-underscore");
        fs::write(
            dir.join("_20260101_120000_private.surql"),
            "-- @up\nSELECT 1;\n-- @down\nSELECT 2;\n",
        )
        .unwrap();
        fs::write(
            dir.join("20260101_120000_ok.surql"),
            "-- @up\nSELECT 1;\n-- @down\nSELECT 2;\n",
        )
        .unwrap();

        let migrations = discover_migrations(&dir).unwrap();
        assert_eq!(migrations.len(), 1);
        assert_eq!(migrations[0].version, "20260101_120000");

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn discover_propagates_load_errors() {
        let dir = unique_temp_dir("disc-badload");
        // Valid filename pattern but missing @up/@down -> load error.
        fs::write(dir.join("20260101_120000_broken.surql"), "no sections").unwrap();

        let err = discover_migrations(&dir).unwrap_err();
        assert!(matches!(err, SurqlError::MigrationLoad { .. }));

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn discover_ignores_subdirectories() {
        let dir = unique_temp_dir("disc-subdir");
        let sub = dir.join("20260101_120000_subdir.surql");
        fs::create_dir_all(&sub).unwrap();
        fs::write(
            dir.join("20260101_120000_real.surql"),
            "-- @up\nSELECT 1;\n-- @down\nSELECT 2;\n",
        )
        .unwrap();

        let migrations = discover_migrations(&dir).unwrap();
        assert_eq!(migrations.len(), 1);
        assert_eq!(migrations[0].version, "20260101_120000");

        fs::remove_dir_all(&dir).ok();
    }

    // --- parse_migration_content corner cases -------------------------------

    #[test]
    fn parse_allows_blank_lines_before_sections() {
        let dir = unique_temp_dir("parse-blank");
        let path = dir.join("20260101_120000_x.surql");
        let text = "\n\n-- preamble comment\n-- @up\nSELECT 1;\n-- @down\nSELECT 2;\n";
        fs::write(&path, text).unwrap();

        let m = load_migration(&path).unwrap();
        assert_eq!(m.up, vec!["SELECT 1;".to_string()]);
        assert_eq!(m.down, vec!["SELECT 2;".to_string()]);

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn parse_splits_multiple_statements_on_semicolons() {
        let dir = unique_temp_dir("parse-split");
        let path = dir.join("20260101_120000_x.surql");
        let text = "-- @up\nSELECT 1; SELECT 2;\nSELECT 3;\n-- @down\nSELECT 4;\n";
        fs::write(&path, text).unwrap();

        let m = load_migration(&path).unwrap();
        assert_eq!(
            m.up,
            vec![
                "SELECT 1;".to_string(),
                "SELECT 2;".to_string(),
                "SELECT 3;".to_string(),
            ]
        );

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn parse_trailing_statement_without_semicolon_is_preserved() {
        let dir = unique_temp_dir("parse-nosc");
        let path = dir.join("20260101_120000_x.surql");
        let text = "-- @up\nSELECT 1\n-- @down\nSELECT 2;\n";
        fs::write(&path, text).unwrap();

        let m = load_migration(&path).unwrap();
        assert_eq!(m.up, vec!["SELECT 1".to_string()]);

        fs::remove_dir_all(&dir).ok();
    }
}
