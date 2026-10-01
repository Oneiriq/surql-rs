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
    let dir = unique_temp_dir("version-format");
    let v = next_free_version(&dir).unwrap();
    assert_eq!(v.len(), 15, "expected YYYYMMDD_HHMMSS (15 chars)");
    assert_eq!(v.chars().nth(8), Some('_'));
    let (date, time) = v.split_once('_').unwrap();
    assert!(date.chars().all(|c| c.is_ascii_digit()));
    assert!(time.chars().all(|c| c.is_ascii_digit()));
    assert_eq!(date.len(), 8);
    assert_eq!(time.len(), 6);

    cleanup(&dir);
}

#[test]
fn version_skips_versions_already_in_the_directory() {
    let dir = unique_temp_dir("version-taken");
    let first = next_free_version(&dir).unwrap();
    fs::write(dir.join(format!("{first}_other.surql")), "").unwrap();
    let second = next_free_version(&dir).unwrap();
    assert!(second > first, "expected {second} > {first}");

    cleanup(&dir);
}

/// Versions have one-second resolution, and the second file used to
/// replace the first through the rename.
#[test]
fn two_migrations_in_one_second_both_survive() {
    let dir = unique_temp_dir("same-second");
    let a = generate_migration("same", &["SELECT 1;".into()], &[], &dir).unwrap();
    let b = generate_migration("same", &["SELECT 2;".into()], &[], &dir).unwrap();
    assert_ne!(a.version, b.version);
    assert_ne!(a.path, b.path);
    assert_eq!(load_migration(&a.path).unwrap().up, vec!["SELECT 1;"]);
    assert_eq!(load_migration(&b.path).unwrap().up, vec!["SELECT 2;"]);

    cleanup(&dir);
}

#[test]
fn an_existing_file_is_never_overwritten() {
    let dir = unique_temp_dir("no-clobber");
    let target = dir.join("20260101_000000_x.surql");
    fs::write(&target, "keep me").unwrap();
    let err = write_migration_file(&dir, "20260101_000000_x.surql", "new").unwrap_err();
    assert!(err.to_string().contains("refusing to overwrite"), "{err}");
    assert_eq!(fs::read_to_string(&target).unwrap(), "keep me");
    let leftover = fs::read_dir(&dir)
        .unwrap()
        .filter_map(std::result::Result::ok)
        .any(|e| e.file_name().to_string_lossy().contains(".tmp."));
    assert!(!leftover);

    cleanup(&dir);
}

/// A description is written on the metadata line; a line break in it
/// used to start a section marker of its own.
#[test]
fn a_line_break_in_a_description_cannot_inject_a_section() {
    let dir = unique_temp_dir("inject");
    let m = create_blank_migration(
        "inject",
        "harmless\n-- @up\nREMOVE TABLE user;\n-- @down",
        &dir,
    )
    .unwrap();
    assert!(m.up.is_empty(), "{:?}", m.up);
    assert!(m.description.starts_with("harmless"));
    assert!(!m.description.contains('\n'));

    cleanup(&dir);
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
fn normalise_keeps_the_terminator_out_of_a_trailing_comment() {
    assert_eq!(
        normalise_statement("SELECT 1 -- note").as_deref(),
        Some("SELECT 1 -- note\n;"),
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

/// A field that gains `REFERENCE` puts its rewrite in the file,
/// right after the DDL, and the whole thing survives the trip back
/// through `load_migration` as one statement: the rewrite is a
/// `FOR` body full of semicolons, which is exactly what the
/// statement splitter used to shatter.
#[test]
fn a_gained_reference_rides_the_generated_file() {
    use crate::migration::diff::diff_fields;
    use crate::schema::{record_field, ReferenceAction};

    let dir = unique_temp_dir("reference-backfill");
    let old = record_field("link", Some("b"))
        .nullable(true)
        .build_unchecked()
        .unwrap();
    let new = record_field("link", Some("b"))
        .nullable(true)
        .reference(ReferenceAction::Ignore)
        .build_unchecked()
        .unwrap();
    let diffs = diff_fields("f", &[new], &[old]);
    assert_eq!(diffs.len(), 1);

    let m = generate_migration_from_diffs("gain_reference", &diffs, &dir).unwrap();
    let reloaded = load_migration(&m.path).unwrap();
    assert_eq!(reloaded.up.len(), 2, "{:#?}", reloaded.up);
    assert!(reloaded.up[0].contains("REFERENCE ON DELETE IGNORE"));
    assert!(reloaded.up[1].starts_with("FOR $rid"), "{}", reloaded.up[1]);
    assert!(
        reloaded.up[1].contains("SET link = $held"),
        "the dance survives the file as one statement: {}",
        reloaded.up[1]
    );
    // Removing the clause needs no un-backfill: down is the old DDL alone.
    assert_eq!(reloaded.down.len(), 1, "{:#?}", reloaded.down);

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
    assert_eq!(m.up, [] as [std::string::String; 0]);
    assert_eq!(m.down, [] as [std::string::String; 0]);

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
    assert_eq!(m.up, [] as [std::string::String; 0]);
    assert_eq!(m.down, [] as [std::string::String; 0]);
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
    assert_eq!(m.path.extension().and_then(|s| s.to_str()), Some("surql"));

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
fn initial_migration_quotes_names_in_down() {
    use crate::schema::bucket::memory_bucket;

    let dir = unique_temp_dir("initial-quoted");
    let registry = SchemaRegistry::new();
    registry.register_table(table_schema("select"));
    registry.register_bucket(memory_bucket("my-files"));
    let m = generate_initial_migration(&registry, &dir).unwrap();
    assert!(
        m.down
            .contains(&"REMOVE TABLE IF EXISTS `select`;".to_string()),
        "{:?}",
        m.down
    );
    assert!(
        m.down.contains(&"REMOVE BUCKET `my-files`;".to_string()),
        "{:?}",
        m.down
    );

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
        bucket: None,
        analyzer: None,
        object: None,
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
            bucket: None,
            analyzer: None,
            object: None,
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
        bucket: None,
        analyzer: None,
        object: None,
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
