use super::*;
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
use std::time::{SystemTime, UNIX_EPOCH};

static TEST_DIR_COUNTER: AtomicU64 = AtomicU64::new(0);

fn unique_temp_dir(tag: &str) -> PathBuf {
    let nanos: u128 = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos());
    let n = TEST_DIR_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
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
fn compare_versions_reads_digit_runs_as_numbers() {
    assert_eq!(compare_versions("v9", "v10"), Ordering::Less);
    assert_eq!(compare_versions("v10", "v9"), Ordering::Greater);
    assert_eq!(
        compare_versions("20260101_000000", "20260102_000000"),
        Ordering::Less
    );
    assert_eq!(compare_versions("1.2.10", "1.10.2"), Ordering::Less);
    assert_eq!(compare_versions("v1", "v1"), Ordering::Equal);
    assert_ne!(compare_versions("v01", "v1"), Ordering::Equal);
    assert_eq!(compare_versions("v1", "v1a"), Ordering::Less);
}

fn bare(version: &str, depends_on: &[&str]) -> Migration {
    Migration {
        version: version.to_string(),
        description: String::new(),
        path: PathBuf::new(),
        up: vec![],
        down: vec![],
        checksum: None,
        depends_on: depends_on.iter().map(|d| (*d).to_string()).collect(),
        squashed_from: vec![],
    }
}

fn versions(migrations: &[Migration]) -> Vec<&str> {
    migrations.iter().map(|m| m.version.as_str()).collect()
}

#[test]
fn order_migrations_puts_dependencies_first() {
    let ordered = order_migrations(vec![
        bare("v10", &[]),
        bare("v2", &["v3"]),
        bare("v3", &["unknown"]),
        bare("v9", &[]),
    ])
    .unwrap();
    assert_eq!(versions(&ordered), vec!["v3", "v2", "v9", "v10"]);
}

#[test]
fn order_migrations_rejects_a_cycle() {
    let err =
        order_migrations(vec![bare("a", &["b"]), bare("b", &["a"]), bare("c", &[])]).unwrap_err();
    assert!(matches!(err, SurqlError::MigrationDiscovery { .. }));
    assert!(err.to_string().contains("a, b"), "{err}");
}

#[test]
fn discover_orders_versions_numerically() {
    let dir = unique_temp_dir("disc-natural");
    for (file, version) in [
        ("20260101_000000_a.surql", "v10"),
        ("20260101_000001_b.surql", "v9"),
    ] {
        let text = format!(
            "-- @metadata\n-- version: {version}\n-- description: d\n\
                 -- @up\nSELECT 1;\n-- @down\nSELECT 2;\n"
        );
        fs::write(dir.join(file), text).unwrap();
    }
    let migrations = discover_migrations(&dir).unwrap();
    assert_eq!(versions(&migrations), vec!["v9", "v10"]);

    fs::remove_dir_all(&dir).ok();
}

#[test]
fn load_migration_reads_squashed_from() {
    let dir = unique_temp_dir("load-squashed");
    let path = dir.join("20260105_000000_squashed.surql");
    let text = "-- @metadata\n\
             -- version: 20260105_000000\n\
             -- description: squashed\n\
             -- squashed-from: 20260101_000000,20260102_000000\n\
             -- @up\n\
             SELECT 1;\n\
             -- @down\n";
    fs::write(&path, text).unwrap();

    let m = load_migration(&path).unwrap();
    assert_eq!(m.squashed_from, vec!["20260101_000000", "20260102_000000"]);
    assert!(m.down.is_empty());

    fs::remove_dir_all(&dir).ok();
}

#[test]
fn checksum_ignores_line_endings_and_byte_order_mark() {
    let unix = "-- @up\nSELECT 1;\n-- @down\nSELECT 2;\n";
    let windows = "\u{FEFF}-- @up\r\nSELECT 1;\r\n-- @down\r\nSELECT 2;\r\n";
    assert_eq!(content_checksum(unix), content_checksum(windows));
    assert_ne!(content_checksum(unix), content_checksum("SELECT 3;"));
}

fn history_row(version: &str, checksum: &str) -> MigrationHistory {
    MigrationHistory {
        version: version.into(),
        description: String::new(),
        applied_at: chrono::Utc::now(),
        checksum: checksum.into(),
        execution_time_ms: None,
    }
}

#[test]
fn an_edited_applied_migration_is_modified() {
    let dir = unique_temp_dir("modified");
    let path = dir.join("20260101_000000_a.surql");
    fs::write(&path, "-- @up\nSELECT 1;\n-- @down\nSELECT 2;\n").unwrap();
    let applied = load_migration(&path).unwrap();
    let recorded = applied.checksum.clone().unwrap();

    let history = [history_row("20260101_000000", &recorded)];
    assert!(modified_migrations(std::slice::from_ref(&applied), &history).is_empty());

    fs::write(&path, "-- @up\nSELECT 3;\n-- @down\nSELECT 2;\n").unwrap();
    let edited = load_migration(&path).unwrap();
    let modified = modified_migrations(std::slice::from_ref(&edited), &history);
    assert_eq!(modified.len(), 1);
    assert_eq!(modified[0].version, "20260101_000000");
    assert_eq!(modified[0].recorded_checksum, recorded);
    assert_eq!(modified[0].current_checksum, edited.checksum.unwrap());

    fs::remove_dir_all(&dir).ok();
}

/// Rows recorded before checksums ignored line endings hold the SHA-256
/// of the raw bytes, and a checkout on another platform has the other
/// line ending, so any of those variants is the same file.
#[test]
fn rows_hashed_from_raw_bytes_still_match() {
    let dir = unique_temp_dir("legacy");
    let path = dir.join("20260101_000000_a.surql");
    let unix = "-- @up\nSELECT 1;\n-- @down\nSELECT 2;\n";
    fs::write(&path, unix).unwrap();
    let on_disk = [load_migration(&path).unwrap()];

    let windows = unix.replace('\n', "\r\n");
    for raw in [
        unix.to_string(),
        windows.clone(),
        format!("\u{FEFF}{unix}"),
        format!("\u{FEFF}{windows}"),
    ] {
        let history = [history_row("20260101_000000", &sha256_hex(raw.as_bytes()))];
        assert!(
            modified_migrations(&on_disk, &history).is_empty(),
            "{raw:?} should match"
        );
    }

    let other = [history_row(
        "20260101_000000",
        &sha256_hex(b"-- @up\nSELECT 9;\n"),
    )];
    assert_eq!(modified_migrations(&on_disk, &other).len(), 1);

    fs::remove_dir_all(&dir).ok();
}

#[test]
fn rows_without_a_checksum_or_a_file_are_not_compared() {
    let dir = unique_temp_dir("unknown");
    let path = dir.join("20260101_000000_a.surql");
    fs::write(&path, "-- @up\nSELECT 1;\n-- @down\nSELECT 2;\n").unwrap();
    let on_disk = [load_migration(&path).unwrap()];

    assert!(modified_migrations(&on_disk, &[history_row("20260101_000000", "")]).is_empty());
    assert!(modified_migrations(&on_disk, &[history_row("20250101_000000", "x")]).is_empty());
    let mut unhashed = on_disk[0].clone();
    unhashed.checksum = None;
    assert!(modified_migrations(&[unhashed], &[history_row("20260101_000000", "x")]).is_empty());

    fs::remove_dir_all(&dir).ok();
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
    let err = load_migration(Path::new("/nonexistent/path/to/nothing_xyzzy.surql")).unwrap_err();
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
