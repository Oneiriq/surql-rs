use super::*;

// ----- diff_tables: ADD -----

#[test]
fn diff_tables_adds_new_table() {
    let code = vec![tbl("user")];
    let db: Vec<TableDefinition> = vec![];
    let diffs = diff_tables(&code, &db);
    assert_eq!(diffs.len(), 1);
    assert_eq!(diffs[0].operation, DiffOperation::AddTable);
    assert_eq!(diffs[0].table, "user");
    assert!(diffs[0].forward_sql.starts_with("DEFINE TABLE user"));
    assert_eq!(diffs[0].backward_sql, "REMOVE TABLE user;");
}

#[test]
fn diff_tables_adds_new_table_with_field_and_index() {
    let code_table = tbl("user")
        .with_fields([f("email", FieldType::String)])
        .with_indexes([unique_index("email_idx", ["email"])]);
    let diffs = diff_tables(&[code_table], &[]);
    // 1 table + 1 field + 1 index = 3 diffs.
    assert_eq!(diffs.len(), 3);
    assert_eq!(diffs[0].operation, DiffOperation::AddTable);
    assert!(diffs.iter().any(|d| d.operation == DiffOperation::AddField));
    assert!(diffs.iter().any(|d| d.operation == DiffOperation::AddIndex));
}

#[test]
fn diff_tables_adds_table_with_event_and_perms() {
    let code_table = tbl("user")
        .with_events([event("on_upd", "true", "RETURN 1")])
        .with_permissions([("select", "true")]);
    let diffs = diff_tables(&[code_table], &[]);
    // Table (permissions ride the DEFINE TABLE itself) + event.
    assert_eq!(diffs.len(), 2);
    assert!(diffs.iter().any(|d| d.operation == DiffOperation::AddEvent));
    assert!(
        diffs[0].forward_sql.contains("PERMISSIONS"),
        "{}",
        diffs[0].forward_sql
    );
}

// ----- diff_tables: DROP -----

#[test]
fn diff_tables_drops_missing_table() {
    let db = vec![tbl("old").with_mode(TableMode::Schemaless)];
    let diffs = diff_tables(&[], &db);
    assert_eq!(diffs.len(), 1);
    assert_eq!(diffs[0].operation, DiffOperation::DropTable);
    assert_eq!(diffs[0].forward_sql, "REMOVE TABLE old;");
    assert_eq!(diffs[0].backward_sql, "DEFINE TABLE old SCHEMALESS;");
}

/// REMOVE TABLE takes everything on the table with it, so the rollback
/// re-creates everything, as the add would have.
#[test]
fn a_dropped_table_rolls_back_to_its_whole_definition() {
    use crate::schema::ChangeFeed;
    let doc = tbl("doc")
        .with_fields([f("email", FieldType::String)])
        .with_indexes([unique_index("email_idx", ["email"])])
        .with_events([event("audit", "true", "CREATE log")])
        .with_permissions([("select", "true")])
        .with_changefeed(ChangeFeed::new("1d"));
    let diffs = diff_tables(&[], std::slice::from_ref(&doc));
    assert_eq!(diffs.len(), 1);
    let restore: Vec<String> = diff_tables(std::slice::from_ref(&doc), &[])
        .iter()
        .map(|d| d.forward_sql.clone())
        .collect();
    assert_eq!(diffs[0].backward_sql, restore.join("\n"));
    assert!(diffs[0]
        .backward_sql
        .contains("DEFINE TABLE doc SCHEMAFULL CHANGEFEED 1d PERMISSIONS FOR select WHERE true;"));
    assert!(diffs[0]
        .backward_sql
        .contains("DEFINE INDEX email_idx ON TABLE doc COLUMNS email UNIQUE;"));
}

#[test]
fn a_dropped_edge_rolls_back_to_its_whole_definition() {
    let likes = relation_edge("likes")
        .with_fields([f("weight", FieldType::Int)])
        .with_indexes([unique_index("pair", ["in", "out"])])
        .with_permissions([("select", "true")]);
    let diffs = diff_edges(&[], std::slice::from_ref(&likes));
    assert_eq!(diffs.len(), 1);
    assert_eq!(
        diffs[0].backward_sql,
        "DEFINE TABLE likes TYPE RELATION FROM user TO post PERMISSIONS FOR select WHERE true;\n\
             DEFINE FIELD weight ON TABLE likes TYPE int;\n\
             DEFINE INDEX pair ON TABLE likes COLUMNS in, out UNIQUE;"
    );
}

#[test]
fn a_dropped_unique_index_comes_back_unique() {
    let diffs = diff_indexes("user", &[], &[unique_index("email_idx", ["email"])]);
    assert_eq!(
        diffs[0].backward_sql,
        "DEFINE INDEX email_idx ON TABLE user COLUMNS email UNIQUE;"
    );
}

// ----- diff_tables: MODIFY (no-op when identical) -----

#[test]
fn diff_tables_identical_produces_no_diff() {
    let a = tbl("user").with_fields([f("email", FieldType::String)]);
    let diffs = diff_tables(std::slice::from_ref(&a), std::slice::from_ref(&a));
    assert_eq!(diffs, [] as [crate::migration::models::SchemaDiff; 0]);
}

#[test]
fn diff_schemas_includes_buckets() {
    use crate::schema::bucket::memory_bucket;
    let code = SchemaSnapshot::from_all_parts([tbl("user")], [], [memory_bucket("avatars")]);
    let db = SchemaSnapshot::default();
    let diffs = diff_schemas(&code, &db);
    assert!(diffs
        .iter()
        .any(|d| d.operation == DiffOperation::AddBucket));
    assert!(diffs.iter().any(|d| d.operation == DiffOperation::AddTable));
}
#[test]
fn snapshot_without_buckets_key_deserialises() {
    // Older snapshots predate the `buckets` field; #[serde(default)]
    // must let them load with an empty bucket list.
    let json = r#"{ "tables": [], "edges": [] }"#;
    let snap: SchemaSnapshot = serde_json::from_str(json).unwrap();
    assert_eq!(
        snap.buckets,
        [] as [crate::schema::bucket::BucketDefinition; 0]
    );
}

// ----- change feeds -----

#[test]
fn diff_tables_detects_an_added_changefeed() {
    use crate::schema::ChangeFeed;
    let db = vec![table_schema("audit")];
    let code = vec![table_schema("audit").with_changefeed(ChangeFeed::new("1d"))];
    let diffs = diff_tables(&code, &db);
    assert_eq!(diffs.len(), 1);
    assert_eq!(diffs[0].operation, DiffOperation::ModifyTable);
    assert_eq!(diffs[0].table, "audit");
    assert_eq!(
        diffs[0].forward_sql,
        "DEFINE TABLE OVERWRITE audit SCHEMAFULL CHANGEFEED 1d;"
    );
    assert_eq!(
        diffs[0].backward_sql,
        "DEFINE TABLE OVERWRITE audit SCHEMAFULL;"
    );
}

#[test]
fn diff_tables_detects_a_dropped_changefeed() {
    use crate::schema::ChangeFeed;
    let db = vec![table_schema("audit").with_changefeed(ChangeFeed::new("1d"))];
    let code = vec![table_schema("audit")];
    let diffs = diff_tables(&code, &db);
    assert_eq!(diffs.len(), 1);
    assert_eq!(diffs[0].operation, DiffOperation::ModifyTable);
    assert!(!diffs[0].forward_sql.contains("CHANGEFEED"));
    assert!(diffs[0].backward_sql.contains("CHANGEFEED 1d"));
}

#[test]
fn diff_tables_detects_a_changed_retention_window() {
    use crate::schema::ChangeFeed;
    let db = vec![table_schema("audit").with_changefeed(ChangeFeed::new("1d"))];
    let code =
        vec![table_schema("audit").with_changefeed(ChangeFeed::new("3d").include_original(true))];
    let diffs = diff_tables(&code, &db);
    assert_eq!(diffs.len(), 1);
    assert!(diffs[0]
        .forward_sql
        .contains("CHANGEFEED 3d INCLUDE ORIGINAL"));
}

#[test]
fn diff_tables_ignores_an_unchanged_changefeed() {
    use crate::schema::ChangeFeed;
    let t = table_schema("audit").with_changefeed(ChangeFeed::new("1d"));
    assert_eq!(
        diff_tables(std::slice::from_ref(&t), std::slice::from_ref(&t)),
        [] as [crate::migration::models::SchemaDiff; 0]
    );
}

// ----- views -----

#[test]
fn diff_tables_detects_an_added_view() {
    use crate::schema::{ViewDefinition, ViewGroup};
    let db = vec![table_schema("stats")];
    let code = vec![table_schema("stats").with_view(
        ViewDefinition::new(["count() AS total"], ["comment"]).with_group(ViewGroup::All),
    )];
    let diffs = diff_tables(&code, &db);
    assert_eq!(diffs.len(), 1);
    assert_eq!(diffs[0].operation, DiffOperation::ModifyTable);
    assert!(diffs[0].description.contains("view"));
    assert!(diffs[0].forward_sql.contains("TYPE NORMAL"));
    assert!(diffs[0]
        .forward_sql
        .contains("AS SELECT count() AS total FROM comment GROUP ALL"));
    assert!(!diffs[0].backward_sql.contains("AS SELECT"));
}

#[test]
fn diff_tables_detects_a_changed_view_body() {
    use crate::schema::ViewDefinition;
    let db = vec![table_schema("stats").with_view(ViewDefinition::new(["id"], ["comment"]))];
    let code = vec![table_schema("stats")
        .with_view(ViewDefinition::new(["id"], ["comment"]).with_condition("n > 2"))];
    let diffs = diff_tables(&code, &db);
    assert_eq!(diffs.len(), 1);
    assert!(diffs[0].forward_sql.contains("WHERE n > 2"));
}

#[test]
fn diff_tables_ignores_view_whitespace_reformatting() {
    use crate::schema::ViewDefinition;
    let db = vec![table_schema("stats")
        .with_view(ViewDefinition::new(["id"], ["comment"]).with_condition("n   >    2"))];
    let code = vec![table_schema("stats")
        .with_view(ViewDefinition::new(["id"], ["comment"]).with_condition("n > 2"))];
    assert!(
        diff_tables(&code, &db).is_empty(),
        "the engine reformats freely; only the meaning may differ"
    );
}

// ----- diff_buckets -----

// ----- diff_schemas aggregator -----

#[test]
fn diff_schemas_empty_snapshots_are_equal() {
    let a = SchemaSnapshot::default();
    let b = SchemaSnapshot::default();
    assert_eq!(
        diff_schemas(&a, &b),
        [] as [crate::migration::models::SchemaDiff; 0]
    );
}

#[test]
fn diff_schemas_add_tables_and_edges() {
    let code = SchemaSnapshot::from_parts([tbl("user")], [relation_edge("likes")]);
    let db = SchemaSnapshot::default();
    let diffs = diff_schemas(&code, &db);
    let ops: Vec<DiffOperation> = diffs.iter().map(|d| d.operation).collect();
    // At least one AddTable for the user table and one AddTable for the edge.
    assert!(
        ops.iter()
            .filter(|o| **o == DiffOperation::AddTable)
            .count()
            >= 2
    );
}

#[test]
fn diff_schemas_drops_removed_items() {
    let code = SchemaSnapshot::default();
    let db = SchemaSnapshot::from_parts([tbl("old")], [relation_edge("old_rel")]);
    let diffs = diff_schemas(&code, &db);
    let drops = diffs
        .iter()
        .filter(|d| d.operation == DiffOperation::DropTable)
        .count();
    assert_eq!(drops, 2);
}

#[test]
fn diff_schemas_handles_mixed_add_drop_modify() {
    let shared = tbl("user").with_fields([f("email", FieldType::String)]);
    let shared_modified = tbl("user").with_fields([f("email", FieldType::Int)]);
    let code = SchemaSnapshot::from_parts([tbl("new"), shared_modified], []);
    let db = SchemaSnapshot::from_parts([shared, tbl("obsolete")], []);
    let diffs = diff_schemas(&code, &db);
    let ops: BTreeSet<DiffOperation> = diffs.iter().map(|d| d.operation).collect();
    assert!(ops.contains(&DiffOperation::AddTable));
    assert!(ops.contains(&DiffOperation::DropTable));
    assert!(ops.contains(&DiffOperation::ModifyField));
}

fn operations(diffs: &[SchemaDiff]) -> Vec<DiffOperation> {
    diffs.iter().map(|d| d.operation).collect()
}

/// An index that names an analyzer (or a backfill that calls a function)
/// needs it defined first.
#[test]
fn diff_schemas_defines_objects_before_the_tables_that_use_them() {
    use crate::schema::{bm25_index, standard_analyzer, FunctionDefinition};
    let doc = tbl("doc").with_indexes([bm25_index("body_ft", ["body"], "words")]);
    let code = SchemaSnapshot {
        tables: vec![doc],
        analyzers: vec![standard_analyzer("words")],
        functions: vec![FunctionDefinition::new("greet", "RETURN 'hi'")],
        ..SchemaSnapshot::default()
    };
    let ops = operations(&diff_schemas(&code, &SchemaSnapshot::default()));
    assert_eq!(
        ops,
        vec![
            DiffOperation::AddFunction,
            DiffOperation::AddAnalyzer,
            DiffOperation::AddTable,
            DiffOperation::AddIndex,
        ]
    );
}

/// An edge turning into a table of the same name is removed before the
/// table is defined, whichever pass each half comes from.
#[test]
fn diff_schemas_drops_before_it_defines() {
    let code = SchemaSnapshot::from_parts([tbl("x")], []);
    let db = SchemaSnapshot::from_parts([], [relation_edge("x")]);
    let diffs = diff_schemas(&code, &db);
    assert_eq!(
        operations(&diffs),
        vec![DiffOperation::DropTable, DiffOperation::AddTable]
    );
    assert_eq!(diffs[0].forward_sql, "REMOVE TABLE x;");

    let back = diff_schemas(&db, &code);
    assert_eq!(
        operations(&back),
        vec![DiffOperation::DropTable, DiffOperation::AddTable]
    );
    assert!(back[1].forward_sql.contains("TYPE RELATION"));
}

/// Objects go once nothing that used them is left: the engine refuses to
/// remove an analyzer a full-text index still names.
#[test]
fn diff_schemas_removes_objects_after_the_tables_that_used_them() {
    use crate::schema::{bm25_index, standard_analyzer};
    let doc = tbl("doc").with_indexes([bm25_index("body_ft", ["body"], "words")]);
    let db = SchemaSnapshot {
        tables: vec![doc.clone()],
        analyzers: vec![standard_analyzer("words")],
        buckets: vec![crate::schema::memory_bucket("files")],
        ..SchemaSnapshot::default()
    };
    let code = SchemaSnapshot::from_parts([tbl("doc")], []);
    let ops = operations(&diff_schemas(&code, &db));
    assert_eq!(
        ops,
        vec![
            DiffOperation::DropIndex,
            DiffOperation::DropBucket,
            DiffOperation::DropAnalyzer,
        ]
    );
}

// ----- pair-wise helpers -----

#[test]
fn diff_table_pair_add_is_same_as_slice_form() {
    let t = tbl("user");
    let pair = diff_table_pair(Some(&t), None);
    let slice = diff_tables(std::slice::from_ref(&t), &[]);
    assert_eq!(pair, slice);
}

#[test]
fn diff_table_pair_drop_is_same_as_slice_form() {
    let t = tbl("user");
    let pair = diff_table_pair(None, Some(&t));
    let slice = diff_tables(&[], std::slice::from_ref(&t));
    assert_eq!(pair, slice);
}

#[test]
fn diff_table_pair_none_none_is_empty() {
    assert_eq!(
        diff_table_pair(None, None),
        [] as [crate::migration::models::SchemaDiff; 0]
    );
}

#[test]
fn diff_edge_pair_none_none_is_empty() {
    assert_eq!(
        diff_edge_pair(None, None),
        [] as [crate::migration::models::SchemaDiff; 0]
    );
}

#[test]
fn diff_edge_pair_add_matches_slice_form() {
    let e = relation_edge("likes");
    let pair = diff_edge_pair(Some(&e), None);
    let slice = diff_edges(std::slice::from_ref(&e), &[]);
    assert_eq!(pair, slice);
}

// ----- round-trip & details shape -----

#[test]
fn modify_field_details_contains_both_types() {
    let code = vec![f("n", FieldType::Int)];
    let db = vec![f("n", FieldType::Float)];
    let diffs = diff_fields("t", &code, &db);
    assert_eq!(diffs.len(), 1);
    let d = &diffs[0];
    assert_eq!(d.details.get("old_type"), Some(&serde_json::json!("float")));
    assert_eq!(d.details.get("new_type"), Some(&serde_json::json!("int")));
}

#[test]
fn add_field_details_contains_type() {
    let code = vec![f("age", FieldType::Int)];
    let diffs = diff_fields("u", &code, &[]);
    assert_eq!(
        diffs[0].details.get("type"),
        Some(&serde_json::json!("int"))
    );
}

#[test]
fn diff_permissions_multiple_entries_render_space_separated() {
    let mut code = BTreeMap::new();
    code.insert("select".into(), "true".into());
    code.insert("create".into(), "true".into());
    let diffs = diff_permissions("t", Some(&code), None);
    let fwd = &diffs[0].forward_sql;
    // One table-level statement carrying both actions inline (the valid
    // placement), not separate malformed DEFINE FIELD statements.
    assert_eq!(fwd.matches("ALTER TABLE").count(), 1);
    assert!(fwd.contains("FOR select WHERE true"));
    assert!(fwd.contains("FOR create WHERE true"));
    assert!(!fwd.contains("DEFINE FIELD PERMISSIONS"));
}

#[test]
fn event_action_is_wrapped_in_braces() {
    let ev = event("e", "true", "RETURN 1");
    let diffs = diff_events("t", &[ev], &[]);
    assert!(diffs[0].forward_sql.contains("THEN { RETURN 1 }"));
}

#[test]
fn modify_field_preserves_name_as_context() {
    let code = vec![f("email", FieldType::String)];
    let db = vec![f("email", FieldType::Int)];
    let diffs = diff_fields("user", &code, &db);
    assert_eq!(diffs[0].table, "user");
    assert_eq!(diffs[0].field.as_deref(), Some("email"));
}

// ----- snapshot round-trip -----

#[test]
fn snapshot_serde_roundtrip() {
    use crate::schema::bucket::memory_bucket;
    let snap = SchemaSnapshot::from_all_parts(
        [tbl("user")],
        [relation_edge("likes")],
        [memory_bucket("avatars")],
    );
    let j = serde_json::to_string(&snap).unwrap();
    let back: SchemaSnapshot = serde_json::from_str(&j).unwrap();
    assert_eq!(snap, back);
    assert_eq!(back.buckets.len(), 1);
}

#[test]
fn snapshot_default_is_empty() {
    let s = SchemaSnapshot::default();
    assert_eq!(s.tables, [] as [crate::schema::table::TableDefinition; 0]);
    assert_eq!(s.edges, [] as [crate::schema::edge::EdgeDefinition; 0]);
}

#[test]
fn snapshot_new_matches_default() {
    assert_eq!(SchemaSnapshot::new(), SchemaSnapshot::default());
}

// ----- sorted_keys / index_by_name are tested indirectly via diff_* -----

#[test]
fn diff_tables_sort_stable_across_multiple_adds_drops() {
    let code = vec![tbl("a"), tbl("c")];
    let db = vec![tbl("b"), tbl("d")];
    let diffs = diff_tables(&code, &db);
    let adds: Vec<&str> = diffs
        .iter()
        .filter(|d| d.operation == DiffOperation::AddTable)
        .map(|d| d.table.as_str())
        .collect();
    let drops: Vec<&str> = diffs
        .iter()
        .filter(|d| d.operation == DiffOperation::DropTable)
        .map(|d| d.table.as_str())
        .collect();
    assert_eq!(adds, vec!["a", "c"]);
    assert_eq!(drops, vec!["b", "d"]);
}

#[test]
fn field_expr_comparison_treats_value_whitespace() {
    let a = vec![f("x", FieldType::String).with_value("a  +  b")];
    let b = vec![f("x", FieldType::String).with_value("a + b")];
    assert_eq!(
        diff_fields("t", &a, &b),
        [] as [crate::migration::models::SchemaDiff; 0]
    );
}

#[test]
fn field_expr_comparison_treats_default_whitespace() {
    let a = vec![f("x", FieldType::Int).with_default("42  ")];
    let b = vec![f("x", FieldType::Int).with_default("42")];
    assert_eq!(
        diff_fields("t", &a, &b),
        [] as [crate::migration::models::SchemaDiff; 0]
    );
}
