use super::*;

// ----- diff_fields: ADD / DROP / MODIFY -----

#[test]
fn diff_fields_detects_added() {
    let code = vec![f("email", FieldType::String)];
    let diffs = diff_fields("user", &code, &[]);
    assert_eq!(diffs.len(), 1);
    assert_eq!(diffs[0].operation, DiffOperation::AddField);
    assert_eq!(diffs[0].field.as_deref(), Some("email"));
    assert!(diffs[0].forward_sql.contains("DEFINE FIELD email"));
    assert!(diffs[0].backward_sql.contains("REMOVE FIELD email"));
}

#[test]
fn diff_fields_detects_dropped() {
    let db = vec![f("old", FieldType::String)];
    let diffs = diff_fields("user", &[], &db);
    assert_eq!(diffs.len(), 1);
    assert_eq!(diffs[0].operation, DiffOperation::DropField);
    assert!(diffs[0].forward_sql.contains("REMOVE FIELD old"));
    assert!(diffs[0].backward_sql.contains("DEFINE FIELD old"));
}

#[test]
fn diff_fields_detects_modified_type() {
    let code = vec![f("age", FieldType::Int)];
    let db = vec![f("age", FieldType::String)];
    let diffs = diff_fields("user", &code, &db);
    assert_eq!(diffs.len(), 1);
    assert_eq!(diffs[0].operation, DiffOperation::ModifyField);
    assert_eq!(
        diffs[0].details.get("old_type"),
        Some(&serde_json::json!("string"))
    );
    assert_eq!(
        diffs[0].details.get("new_type"),
        Some(&serde_json::json!("int"))
    );
}

fn linked(action: Option<crate::schema::ReferenceAction>) -> FieldDefinition {
    let mut field = f("blob", FieldType::Record);
    field.target_table = Some("blob".into());
    field.reference = action;
    field
}

/// Gaining `REFERENCE` is the one field change whose DDL alone
/// leaves the tracking wrong for every pre-existing row, so that
/// diff carries the rewrite and says so.
#[test]
fn a_gained_reference_carries_its_backfill() {
    use crate::schema::ReferenceAction;
    let diffs = diff_fields(
        "file",
        &[linked(Some(ReferenceAction::Ignore))],
        &[linked(None)],
    );
    assert_eq!(diffs.len(), 1);
    let backfill = diffs[0]
        .reference_backfill_sql()
        .expect("the rewrite rides the diff");
    assert!(backfill.contains("SELECT VALUE id FROM file"), "{backfill}");
    assert!(backfill.contains("?? []"), "{backfill}");
    assert!(backfill.contains("SET blob = NONE"), "{backfill}");
    assert!(
        diffs[0].description.contains("backfill"),
        "{}",
        diffs[0].description
    );
}

/// Everything else about a reference leaves the rewrite out: a
/// changed action re-renders DDL over tracking that already exists,
/// and a removed clause has nothing to register.
#[test]
fn other_reference_changes_carry_no_backfill() {
    use crate::schema::ReferenceAction;
    let changed = diff_fields(
        "file",
        &[linked(Some(ReferenceAction::Cascade))],
        &[linked(Some(ReferenceAction::Ignore))],
    );
    assert_eq!(changed.len(), 1);
    assert!(changed[0].reference_backfill_sql().is_none());

    let removed = diff_fields(
        "file",
        &[linked(None)],
        &[linked(Some(ReferenceAction::Ignore))],
    );
    assert_eq!(removed.len(), 1);
    assert!(removed[0].reference_backfill_sql().is_none());

    // A NEW field with REFERENCE has no pre-existing values to
    // register; the add diff stays plain DDL.
    let added = diff_fields("file", &[linked(Some(ReferenceAction::Ignore))], &[]);
    assert_eq!(added.len(), 1);
    assert_eq!(added[0].operation, DiffOperation::AddField);
    assert!(added[0].reference_backfill_sql().is_none());
}

/// Every clause the field renders is a clause a change can land in.
#[test]
fn every_rendered_field_clause_is_compared() {
    let base = f("owner", FieldType::Record).with_target_table("user");
    let changes = [
        ("nullable", base.clone().with_nullable(true)),
        ("target", base.clone().with_target_table("post")),
        (
            "reference",
            base.clone()
                .with_reference(crate::schema::ReferenceAction::Reject),
        ),
        ("computed", base.clone().with_computed("<~post")),
        (
            "permissions",
            base.clone().with_permissions([("update", "$auth.admin")]),
        ),
    ];
    for (what, changed) in changes {
        let diffs = diff_fields(
            "t",
            std::slice::from_ref(&changed),
            std::slice::from_ref(&base),
        );
        assert_eq!(diffs.len(), 1, "a {what} change went unnoticed");
        assert_eq!(diffs[0].operation, DiffOperation::ModifyField);
        assert_eq!(
            diffs[0].forward_sql,
            changed.to_surql_overwrite("t"),
            "{what}"
        );
        assert_eq!(
            diffs[0].backward_sql,
            base.to_surql_overwrite("t"),
            "{what}"
        );
    }
}

/// A target table only renders on a record or array field, so on any
/// other type it is not a difference.
#[test]
fn a_target_table_the_type_ignores_is_not_a_change() {
    let code = f("name", FieldType::String).with_target_table("user");
    let db = f("name", FieldType::String);
    assert!(diff_fields("t", &[code], &[db]).is_empty());
}

/// The engine spells out `FULL` for every action a field's rules leave
/// out, which is the field default; it is not a change.
#[test]
fn default_field_permissions_are_not_a_change() {
    let code = f("x", FieldType::Int).with_permissions([("select", "$auth.id = id")]);
    let db = f("x", FieldType::Int).with_permissions([
        ("select", "$auth.id = id"),
        ("create", "FULL"),
        ("update", "FULL"),
    ]);
    assert!(diff_fields("t", &[code], &[db]).is_empty());
    let full = f("x", FieldType::Int).with_permissions([("select, create, update", "FULL")]);
    assert!(diff_fields("t", &[f("x", FieldType::Int)], &[full]).is_empty());
}

#[test]
fn diff_fields_identical_yields_nothing() {
    let a = vec![f("x", FieldType::Int)];
    assert!(diff_fields("t", &a, &a).is_empty());
}

#[test]
fn diff_fields_whitespace_different_assertion_is_not_a_diff() {
    let code = vec![f("x", FieldType::Int).with_assertion("$value  > 0")];
    let db = vec![f("x", FieldType::Int).with_assertion("$value > 0")];
    assert!(diff_fields("t", &code, &db).is_empty());
}

#[test]
fn diff_fields_modify_detects_assertion_semantic_change() {
    let code = vec![f("x", FieldType::Int).with_assertion("$value > 0")];
    let db = vec![f("x", FieldType::Int).with_assertion("$value >= 0")];
    let diffs = diff_fields("t", &code, &db);
    assert_eq!(diffs.len(), 1);
    assert_eq!(diffs[0].operation, DiffOperation::ModifyField);
}

#[test]
fn diff_fields_add_with_default_emits_backfill() {
    let code = vec![f("age", FieldType::Int).with_default("0")];
    let diffs = diff_fields("user", &code, &[]);
    assert!(diffs[0].forward_sql.contains("DEFAULT 0"));
    assert!(diffs[0]
        .forward_sql
        .contains("UPDATE user SET age = 0 WHERE age IS NONE;"));
}

#[test]
fn diff_fields_add_with_unsafe_default_skips_backfill() {
    let code = vec![f("age", FieldType::Int).with_default("DROP TABLE x")];
    let diffs = diff_fields("user", &code, &[]);
    assert!(!diffs[0].forward_sql.contains("UPDATE"));
}

#[test]
fn diff_fields_readonly_toggle_is_a_modify() {
    let code = vec![f("x", FieldType::Int).readonly(true)];
    let db = vec![f("x", FieldType::Int)];
    let diffs = diff_fields("t", &code, &db);
    assert_eq!(diffs.len(), 1);
    assert_eq!(diffs[0].operation, DiffOperation::ModifyField);
}

// ----- diff_indexes -----

#[test]
fn diff_indexes_detects_added_standard() {
    let code = vec![index("title_idx", ["title"])];
    let diffs = diff_indexes("post", &code, &[]);
    assert_eq!(diffs.len(), 1);
    assert_eq!(diffs[0].operation, DiffOperation::AddIndex);
    assert_eq!(
        diffs[0].forward_sql,
        "DEFINE INDEX title_idx ON TABLE post COLUMNS title;"
    );
}

#[test]
fn diff_indexes_detects_added_unique() {
    let code = vec![unique_index("email_idx", ["email"])];
    let diffs = diff_indexes("user", &code, &[]);
    assert!(diffs[0].forward_sql.contains("UNIQUE"));
}

#[test]
fn diff_indexes_detects_dropped() {
    let db = vec![index("old_idx", ["x"])];
    let diffs = diff_indexes("t", &[], &db);
    assert_eq!(diffs.len(), 1);
    assert_eq!(diffs[0].operation, DiffOperation::DropIndex);
    assert!(diffs[0].forward_sql.contains("REMOVE INDEX old_idx"));
    assert!(diffs[0].backward_sql.contains("DEFINE INDEX old_idx"));
}

#[test]
fn diff_indexes_identical_yields_nothing() {
    let a = vec![index("x", ["a"])];
    assert!(diff_indexes("t", &a, &a).is_empty());
}

#[test]
fn diff_indexes_added_mtree() {
    let idx = mtree_index(
        "e_idx",
        "embedding",
        1536,
        MTreeDistanceType::Cosine,
        MTreeVectorType::F32,
    );
    let diffs = diff_indexes("doc", &[idx], &[]);
    assert_eq!(diffs.len(), 1);
    assert!(diffs[0].forward_sql.contains("MTREE DIMENSION 1536"));
    assert!(diffs[0].forward_sql.contains("DIST COSINE"));
    assert!(diffs[0].forward_sql.contains("TYPE F32"));
}

#[test]
fn diff_indexes_dropped_mtree_recreates_in_backward() {
    let idx = mtree_index(
        "e_idx",
        "embedding",
        8,
        MTreeDistanceType::Euclidean,
        MTreeVectorType::F64,
    );
    let diffs = diff_indexes("doc", &[], &[idx]);
    assert!(diffs[0].forward_sql.starts_with("REMOVE INDEX e_idx"));
    assert!(diffs[0].backward_sql.contains("MTREE DIMENSION 8"));
}

#[test]
fn diff_indexes_added_hnsw() {
    let idx = hnsw_index(
        "h_idx",
        "v",
        64,
        HnswDistanceType::Cosine,
        MTreeVectorType::F32,
        Some(200),
        Some(16),
    );
    let diffs = diff_indexes("doc", &[idx], &[]);
    let sql = &diffs[0].forward_sql;
    assert!(sql.contains("HNSW DIMENSION 64"));
    assert!(sql.contains("DIST COSINE"));
    assert!(sql.contains("EFC 200"));
    assert!(sql.contains("M 16"));
}

#[test]
fn diff_indexes_added_hnsw_without_tuning() {
    let idx = hnsw_index(
        "h_idx",
        "v",
        64,
        HnswDistanceType::Euclidean,
        MTreeVectorType::F64,
        None,
        None,
    );
    let diffs = diff_indexes("doc", &[idx], &[]);
    let sql = &diffs[0].forward_sql;
    assert!(!sql.contains("EFC"));
}

#[test]
fn diff_indexes_added_diskann_spells_the_full_tail() {
    let idx = diskann_index(
        "d_idx",
        "v",
        3,
        DiskAnnDistanceType::Cosine,
        MTreeVectorType::F16,
    );
    let diffs = diff_indexes("doc", &[idx], &[]);
    assert_eq!(diffs.len(), 1);
    assert_eq!(
        diffs[0].forward_sql,
        "DEFINE INDEX d_idx ON TABLE doc COLUMNS v DISKANN DIMENSION 3 \
             DIST COSINE TYPE F16 DEGREE 64 L_BUILD 100 ALPHA 1.2;"
    );
    assert_eq!(diffs[0].backward_sql, "REMOVE INDEX d_idx ON TABLE doc;");
}

#[test]
fn diff_indexes_dropped_diskann_recreates_in_backward() {
    let idx = diskann_index(
        "d_idx",
        "v",
        3,
        DiskAnnDistanceType::InnerProduct,
        MTreeVectorType::U8,
    )
    .with_hashed_vector(true);
    let diffs = diff_indexes("doc", &[], &[idx]);
    assert!(diffs[0].forward_sql.starts_with("REMOVE INDEX d_idx"));
    assert!(diffs[0].backward_sql.contains("DISKANN DIMENSION 3"));
    assert!(diffs[0].backward_sql.contains("DIST INNER_PRODUCT"));
    assert!(diffs[0].backward_sql.contains("HASHED_VECTOR"));
}

#[test]
fn diff_indexes_search_index_emits_fulltext_keyword() {
    let idx = IndexDefinition::new("s_idx", ["body"]).with_type(IndexType::Search);
    let diffs = diff_indexes("post", &[idx], &[]);
    // SurrealDB 3.x renders the full-text index with the `FULLTEXT` keyword
    // (renamed from v1/v2 `SEARCH`).
    assert!(diffs[0].forward_sql.contains("FULLTEXT"));
}

/// An index that keeps its name but changes kind or columns is
/// re-defined whole, both ways.
#[test]
fn a_changed_index_is_redefined_both_ways() {
    let diffs = diff_indexes(
        "user",
        &[unique_index("email_idx", ["email"])],
        &[index("email_idx", ["email"])],
    );
    assert_eq!(diffs.len(), 1, "{diffs:#?}");
    assert_eq!(diffs[0].operation, DiffOperation::ModifyIndex);
    assert_eq!(diffs[0].index.as_deref(), Some("email_idx"));
    assert_eq!(
        diffs[0].forward_sql,
        "DEFINE INDEX OVERWRITE email_idx ON TABLE user COLUMNS email UNIQUE;"
    );
    assert_eq!(
        diffs[0].backward_sql,
        "DEFINE INDEX OVERWRITE email_idx ON TABLE user COLUMNS email;"
    );

    let diffs = diff_indexes("t", &[index("i", ["a", "b"])], &[index("i", ["a"])]);
    assert_eq!(diffs.len(), 1, "{diffs:#?}");
    assert!(diffs[0].forward_sql.contains("COLUMNS a, b"));
}

/// What the engine fills in or never stores is not a change.
#[test]
fn index_defaults_and_directives_are_not_changes() {
    let bare_hnsw = IndexDefinition {
        dimension: Some(4),
        ..IndexDefinition::new("h", ["v"]).with_type(IndexType::Hnsw)
    };
    let echoed_hnsw = hnsw_index(
        "h",
        "v",
        4,
        HnswDistanceType::Euclidean,
        MTreeVectorType::F32,
        Some(150),
        Some(12),
    );
    assert!(indexes_equal(&bare_hnsw, &echoed_hnsw));

    let fulltext = IndexDefinition::new("s", ["body"]).with_type(IndexType::Search);
    let echoed_fulltext = fulltext.clone().with_analyzer("ascii").with_bm25();
    assert!(indexes_equal(&fulltext, &echoed_fulltext));
    assert!(!indexes_equal(
        &fulltext,
        &fulltext.clone().with_analyzer("english")
    ));

    let concurrent = unique_index("u", ["a"]).with_concurrently(true);
    assert!(diff_indexes("t", &[concurrent], &[unique_index("u", ["a"])]).is_empty());
}

#[test]
fn a_changed_hnsw_tuning_is_a_change() {
    let tuned = |efc| {
        hnsw_index(
            "h",
            "v",
            4,
            HnswDistanceType::Cosine,
            MTreeVectorType::F32,
            Some(efc),
            None,
        )
    };
    let diffs = diff_indexes("t", &[tuned(200)], &[tuned(150)]);
    assert_eq!(diffs.len(), 1, "{diffs:#?}");
    assert!(diffs[0].forward_sql.starts_with("DEFINE INDEX OVERWRITE h"));
    assert!(diffs[0].forward_sql.contains("EFC 200"));
    assert!(diffs[0].backward_sql.contains("EFC 150"));
}

// ----- diff_events -----

#[test]
fn a_changed_event_is_redefined_both_ways() {
    let old = event("audit", "true", "CREATE log SET n = 1");
    let new = event("audit", "$event = 'CREATE'", "CREATE log SET n = 2");
    let diffs = diff_events("t", std::slice::from_ref(&new), std::slice::from_ref(&old));
    assert_eq!(diffs.len(), 1, "{diffs:#?}");
    assert_eq!(diffs[0].operation, DiffOperation::ModifyEvent);
    assert_eq!(diffs[0].event.as_deref(), Some("audit"));
    assert_eq!(
        diffs[0].forward_sql,
        "DEFINE EVENT OVERWRITE audit ON TABLE t WHEN $event = 'CREATE' \
             THEN { CREATE log SET n = 2 };"
    );
    assert_eq!(
        diffs[0].backward_sql,
        "DEFINE EVENT OVERWRITE audit ON TABLE t WHEN true THEN { CREATE log SET n = 1 };"
    );
}

/// The engine wraps a bare action in parentheses, keeps a block's
/// trailing `;`, and drops parentheses around the condition.
#[test]
fn the_engine_echo_of_an_event_is_not_a_change() {
    let code = event(
        "audit",
        "($before.a != $after.a)",
        "LET $x = 1; CREATE log SET x = $x",
    );
    let echo = event(
        "audit",
        "$before.a != $after.a",
        "{ LET $x = 1; CREATE log SET x = $x; }",
    );
    assert!(diff_events("t", &[code], &[echo]).is_empty());
    let bare = event("e", "true", "CREATE log SET n = 1");
    let bare_echo = event("e", "true", "(CREATE log SET n = 1)");
    assert!(diff_events("t", &[bare], &[bare_echo]).is_empty());
}

#[test]
fn diff_events_detects_added() {
    let ev = event("on_upd", "true", "RETURN 1");
    let diffs = diff_events("t", &[ev], &[]);
    assert_eq!(diffs.len(), 1);
    assert_eq!(diffs[0].operation, DiffOperation::AddEvent);
    assert_eq!(diffs[0].event.as_deref(), Some("on_upd"));
    assert!(diffs[0].forward_sql.contains("DEFINE EVENT on_upd"));
}

#[test]
fn diff_events_detects_dropped() {
    let ev = event("on_upd", "true", "RETURN 1");
    let diffs = diff_events("t", &[], &[ev]);
    assert_eq!(diffs.len(), 1);
    assert_eq!(diffs[0].operation, DiffOperation::DropEvent);
    assert!(diffs[0].forward_sql.starts_with("REMOVE EVENT on_upd"));
}

#[test]
fn diff_events_identical_yields_nothing() {
    let ev = event("on_upd", "true", "RETURN 1");
    let a = vec![ev];
    assert!(diff_events("t", &a, &a).is_empty());
}

// ----- diff_permissions -----

#[test]
fn diff_permissions_added() {
    let mut new_perms = BTreeMap::new();
    new_perms.insert("select".into(), "true".into());
    let diffs = diff_permissions("t", Some(&new_perms), None);
    assert_eq!(diffs.len(), 1);
    assert_eq!(diffs[0].operation, DiffOperation::ModifyPermissions);
    assert_eq!(
        diffs[0].forward_sql,
        "ALTER TABLE t PERMISSIONS FOR select WHERE true;"
    );
    assert!(!diffs[0].forward_sql.contains("DEFINE FIELD PERMISSIONS"));
    // The rollback restores the table default rather than doing nothing.
    assert_eq!(diffs[0].backward_sql, "ALTER TABLE t PERMISSIONS NONE;");
}

#[test]
fn diff_permissions_removed_roundtrip() {
    let mut old_perms = BTreeMap::new();
    old_perms.insert("select".into(), "$auth.id = id".into());
    let diffs = diff_permissions("t", None, Some(&old_perms));
    assert_eq!(diffs.len(), 1);
    assert_eq!(diffs[0].forward_sql, "ALTER TABLE t PERMISSIONS NONE;");
    assert_eq!(
        diffs[0].backward_sql,
        "ALTER TABLE t PERMISSIONS FOR select WHERE $auth.id = id;"
    );
}

#[test]
fn diff_permissions_modified_carries_old_in_backward() {
    let mut old_perms = BTreeMap::new();
    old_perms.insert("select".into(), "$auth.id = id".into());
    let mut new_perms = BTreeMap::new();
    new_perms.insert("select".into(), "true".into());

    let diffs = diff_permissions("t", Some(&new_perms), Some(&old_perms));
    assert_eq!(diffs.len(), 1);
    assert!(diffs[0].forward_sql.contains("true"));
    assert!(diffs[0].backward_sql.contains("$auth.id = id"));
}

#[test]
fn diff_permissions_identical_yields_nothing() {
    let mut p = BTreeMap::new();
    p.insert("select".into(), "true".into());
    assert!(diff_permissions("t", Some(&p), Some(&p)).is_empty());
}

#[test]
fn diff_permissions_whitespace_variance_is_equal() {
    let mut code = BTreeMap::new();
    code.insert("select".into(), "$auth.id  =  id".into());
    let mut db = BTreeMap::new();
    db.insert("select".into(), "$auth.id = id".into());
    assert!(diff_permissions("t", Some(&code), Some(&db)).is_empty());
}

#[test]
fn diff_permissions_none_and_empty_are_equal() {
    let empty: BTreeMap<String, String> = BTreeMap::new();
    assert!(diff_permissions("t", Some(&empty), None).is_empty());
    assert!(diff_permissions("t", None, Some(&empty)).is_empty());
}

/// `NONE` is a table's default, so an action set to it is an action left
/// out; `FULL` is a posture, rendered as the keyword.
#[test]
fn table_permission_postures() {
    let explicit_none = BTreeMap::from([("delete".to_owned(), "none".to_owned())]);
    assert!(diff_permissions("t", Some(&explicit_none), None).is_empty());

    let full = BTreeMap::from([("select, create".to_owned(), "FULL".to_owned())]);
    let diffs = diff_permissions("t", Some(&full), None);
    assert_eq!(
        diffs[0].forward_sql,
        "ALTER TABLE t PERMISSIONS FOR select, create FULL;"
    );
    let echo = BTreeMap::from([
        ("select".to_owned(), "full".to_owned()),
        ("create".to_owned(), "FULL".to_owned()),
    ]);
    assert!(diff_permissions("t", Some(&full), Some(&echo)).is_empty());
}

/// A name the engine would read as a keyword is quoted in every
/// statement the diff renders, the removals included.
#[test]
fn removals_quote_names_like_the_definitions_do() {
    let table = tbl("select").with_fields([f("address.city", FieldType::String)]);
    let dropped = diff_tables(&[], std::slice::from_ref(&table));
    assert_eq!(dropped[0].forward_sql, "REMOVE TABLE `select`;");
    let fields = diff_fields("select", &[], &table.fields);
    assert_eq!(
        fields[0].forward_sql,
        "REMOVE FIELD address.city ON TABLE `select`;"
    );
    let indexes = diff_indexes("select", &[], &[index("value", ["a"])]);
    assert_eq!(
        indexes[0].forward_sql,
        "REMOVE INDEX `value` ON TABLE `select`;"
    );
    let defaulted = f("n", FieldType::Int).with_default("0");
    let added = diff_fields("select", &[defaulted], &[]);
    assert!(
        added[0]
            .forward_sql
            .ends_with("UPDATE `select` SET n = 0 WHERE n IS NONE;"),
        "{}",
        added[0].forward_sql
    );
}

// ----- diff_edges: ADD / DROP / MODIFY -----

#[test]
fn diff_edges_detects_added_relation() {
    let code = vec![relation_edge("likes")];
    let diffs = diff_edges(&code, &[]);
    assert!(!diffs.is_empty());
    assert_eq!(diffs[0].operation, DiffOperation::AddTable);
    assert!(diffs[0].forward_sql.contains("TYPE RELATION"));
    assert!(diffs[0].forward_sql.contains("FROM user"));
    assert!(diffs[0].forward_sql.contains("TO post"));
}

/// The permissions ride the one `DEFINE TABLE` that creates the edge; a
/// second statement would fail on the table the first one made.
#[test]
fn an_added_edge_carries_its_permissions_inline() {
    let code = vec![relation_edge("likes").with_permissions([("select", "true")])];
    let diffs = diff_edges(&code, &[]);
    assert_eq!(diffs.len(), 1, "{diffs:#?}");
    assert_eq!(
        diffs[0].forward_sql,
        "DEFINE TABLE likes TYPE RELATION FROM user TO post \
             PERMISSIONS FOR select WHERE true;"
    );
}

/// A relation edge naming one endpoint is valid to the engine; its
/// permission change still renders one whole `OVERWRITE` statement.
#[test]
fn a_half_constrained_edge_changes_permissions_in_one_statement() {
    let db = EdgeDefinition::new("tagged").with_from_table("user");
    let code = db.clone().with_permissions([("select", "true")]);
    let diffs = diff_edges(&[code], &[db]);
    assert_eq!(diffs.len(), 1, "{diffs:#?}");
    assert_eq!(diffs[0].operation, DiffOperation::ModifyPermissions);
    assert_eq!(
        diffs[0].forward_sql,
        "DEFINE TABLE OVERWRITE tagged TYPE RELATION FROM user \
             PERMISSIONS FOR select WHERE true;"
    );
    assert_eq!(
        diffs[0].backward_sql,
        "DEFINE TABLE OVERWRITE tagged TYPE RELATION FROM user;"
    );
}

#[test]
fn diff_edges_detects_added_schemafull() {
    let code = vec![EdgeDefinition::new("rel").with_mode(EdgeMode::Schemafull)];
    let diffs = diff_edges(&code, &[]);
    assert_eq!(diffs.len(), 1);
    assert!(diffs[0].forward_sql.contains("SCHEMAFULL"));
}

#[test]
fn diff_edges_detects_dropped() {
    let db = vec![relation_edge("likes")];
    let diffs = diff_edges(&[], &db);
    assert_eq!(diffs.len(), 1);
    assert_eq!(diffs[0].operation, DiffOperation::DropTable);
    assert!(diffs[0].forward_sql.starts_with("REMOVE TABLE likes"));
}

#[test]
fn diff_edges_field_added() {
    let old = relation_edge("likes");
    let new = relation_edge("likes").with_fields([f("weight", FieldType::Int)]);
    let diffs = diff_edges(&[new], &[old]);
    assert_eq!(diffs.len(), 1);
    assert_eq!(diffs[0].operation, DiffOperation::AddField);
    assert_eq!(diffs[0].field.as_deref(), Some("weight"));
}

#[test]
fn diff_edges_field_removed() {
    let old = relation_edge("likes").with_fields([f("weight", FieldType::Int)]);
    let new = relation_edge("likes");
    let diffs = diff_edges(&[new], &[old]);
    assert_eq!(diffs.len(), 1);
    assert_eq!(diffs[0].operation, DiffOperation::DropField);
}

#[test]
fn diff_edges_field_modified() {
    let old = relation_edge("likes").with_fields([f("weight", FieldType::Int)]);
    let new = relation_edge("likes").with_fields([f("weight", FieldType::Float)]);
    let diffs = diff_edges(&[new], &[old]);
    assert_eq!(diffs.len(), 1);
    assert_eq!(diffs[0].operation, DiffOperation::ModifyField);
}

#[test]
fn diff_edges_index_and_event_and_perms() {
    let old = relation_edge("likes");
    let new = relation_edge("likes")
        .with_indexes([index("w_idx", ["weight"])])
        .with_events([event("on_like", "true", "RETURN 1")])
        .with_permissions([("select", "true")]);
    let diffs = diff_edges(&[new], &[old]);
    let ops: BTreeSet<DiffOperation> = diffs.iter().map(|d| d.operation).collect();
    assert!(ops.contains(&DiffOperation::AddIndex));
    assert!(ops.contains(&DiffOperation::AddEvent));
    assert!(ops.contains(&DiffOperation::ModifyPermissions));
}

/// A relation that changes endpoint or an edge that changes mode is
/// re-defined whole, both ways.
#[test]
fn a_changed_edge_shape_is_redefined_both_ways() {
    let old = relation_edge("likes");
    let new = relation_edge("likes").with_to_table("comment");
    let diffs = diff_edges(std::slice::from_ref(&new), std::slice::from_ref(&old));
    assert_eq!(diffs.len(), 1, "{diffs:#?}");
    assert_eq!(diffs[0].operation, DiffOperation::ModifyTable);
    assert_eq!(
        diffs[0].forward_sql,
        "DEFINE TABLE OVERWRITE likes TYPE RELATION FROM user TO comment;"
    );
    assert_eq!(
        diffs[0].backward_sql,
        "DEFINE TABLE OVERWRITE likes TYPE RELATION FROM user TO post;"
    );

    let schemafull = relation_edge("likes").with_mode(EdgeMode::Schemafull);
    let diffs = diff_edges(&[schemafull], std::slice::from_ref(&old));
    assert_eq!(diffs.len(), 1, "{diffs:#?}");
    assert_eq!(
        diffs[0].forward_sql,
        "DEFINE TABLE OVERWRITE likes SCHEMAFULL;"
    );
}

/// Endpoints only render on a relation, so on any other edge they are
/// not part of its shape.
#[test]
fn endpoints_off_a_relation_are_not_a_change() {
    let plain = EdgeDefinition::new("rel").with_mode(EdgeMode::Schemafull);
    let stray = plain.clone().with_from_table("user");
    assert!(diff_edges(&[stray], &[plain]).is_empty());
}

#[test]
fn diff_edges_identical_yields_nothing() {
    let e = relation_edge("likes").with_fields([f("weight", FieldType::Int)]);
    assert!(diff_edges(std::slice::from_ref(&e), std::slice::from_ref(&e)).is_empty());
}
