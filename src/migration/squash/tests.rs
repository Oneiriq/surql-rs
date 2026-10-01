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
    assert_eq!(out, [] as [std::string::String; 0]);
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
    assert_eq!(out, [] as [std::string::String; 0]);
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
    let content =
        generate_squashed_migration_content(&[], "20260101_000000", "empty", &["v1".to_string()]);
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
    let out = filter_migrations_by_version(&mig, Some("20260102_000000"), Some("20260103_000000"));
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
    let r = squash_migrations(&dir, &SquashOptions::new().dry_run(true).optimize(true)).unwrap();
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
    let r = squash_migrations(&dir, &SquashOptions::new().dry_run(true).optimize(false)).unwrap();
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

    let err = squash_migrations(&dir, &SquashOptions::new().output_path(&existing)).unwrap_err();
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
