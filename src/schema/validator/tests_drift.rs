//! Validator tests: edges, the shared table / edge namespace, attributes
//! compared beyond type and presence, and expression normalisation.

use super::*;
use crate::schema::fields::{FieldDefinition, FieldType};
use crate::schema::table::{
    event, table_schema, IndexDefinition, IndexType, MTreeVectorType, TableDefinition,
};

// -- Edge validation -------------------------------------------------------

#[test]
fn edge_missing_from_database() {
    use crate::schema::edge::typed_edge;

    let mut code_edges = HashMap::new();
    code_edges.insert("likes".into(), typed_edge("likes", "user", "post"));
    let db_edges: HashMap<String, EdgeDefinition> = HashMap::new();

    let results = validate_schema(
        &HashMap::new(),
        &HashMap::new(),
        Some(&code_edges),
        Some(&db_edges),
    );
    let missing: Vec<_> = results
        .iter()
        .filter(|r| r.table == "likes" && r.message.to_lowercase().contains("missing"))
        .collect();
    assert_ne!(
        missing,
        [] as [&crate::schema::validator::ValidationResult; 0]
    );
    assert_eq!(missing[0].severity, ValidationSeverity::Error);
}

#[test]
fn edge_field_mismatch() {
    use crate::schema::edge::typed_edge;

    let code_edge = typed_edge("likes", "user", "post")
        .with_fields([FieldDefinition::new("weight", FieldType::Int)]);
    let db_edge = typed_edge("likes", "user", "post");

    let mut code_edges = HashMap::new();
    code_edges.insert("likes".into(), code_edge);
    let mut db_edges = HashMap::new();
    db_edges.insert("likes".into(), db_edge);

    let results = validate_schema(
        &HashMap::new(),
        &HashMap::new(),
        Some(&code_edges),
        Some(&db_edges),
    );
    let field_issues: Vec<_> = results
        .iter()
        .filter(|r| r.field.as_deref() == Some("weight"))
        .collect();
    assert_ne!(
        field_issues,
        [] as [&crate::schema::validator::ValidationResult; 0]
    );
}

#[test]
fn edge_field_type_mismatch_via_validate_field() {
    use crate::schema::edge::typed_edge;

    let code_edge =
        typed_edge("r", "user", "post").with_fields([FieldDefinition::new("w", FieldType::Int)]);
    let db_edge =
        typed_edge("r", "user", "post").with_fields([FieldDefinition::new("w", FieldType::String)]);

    let mut code_edges = HashMap::new();
    code_edges.insert("r".into(), code_edge);
    let mut db_edges = HashMap::new();
    db_edges.insert("r".into(), db_edge);

    let results = validate_schema(
        &HashMap::new(),
        &HashMap::new(),
        Some(&code_edges),
        Some(&db_edges),
    );
    assert!(results
        .iter()
        .any(|r| r.message.contains("Field type mismatch")));
}

#[test]
fn edge_index_missing_from_database() {
    use crate::schema::edge::typed_edge;

    let code_edge =
        typed_edge("r", "user", "post").with_indexes([IndexDefinition::new("idx", ["w"])]);
    let db_edge = typed_edge("r", "user", "post");

    let mut code_edges = HashMap::new();
    code_edges.insert("r".into(), code_edge);
    let mut db_edges = HashMap::new();
    db_edges.insert("r".into(), db_edge);

    let results = validate_schema(
        &HashMap::new(),
        &HashMap::new(),
        Some(&code_edges),
        Some(&db_edges),
    );
    assert!(results
        .iter()
        .any(|r| r.field.as_deref() == Some("index:idx")
            && r.message.contains("missing from database")));
}

fn edges(list: impl IntoIterator<Item = EdgeDefinition>) -> HashMap<String, EdgeDefinition> {
    list.into_iter().map(|e| (e.name.clone(), e)).collect()
}

#[test]
fn matching_relation_edge_yields_nothing() {
    use crate::schema::edge::typed_edge;
    let e = typed_edge("r", "user", "post");
    let results = validate_schema(
        &HashMap::new(),
        &HashMap::new(),
        Some(&edges([e.clone()])),
        Some(&edges([e])),
    );
    assert!(results.is_empty(), "{results:?}");
}

#[test]
fn edge_only_in_database_is_reported() {
    use crate::schema::edge::typed_edge;
    let results = validate_schema(
        &HashMap::new(),
        &HashMap::new(),
        Some(&edges([])),
        Some(&edges([typed_edge("legacy", "a", "b")])),
    );
    assert_eq!(results.len(), 1, "{results:?}");
    assert_eq!(results[0].severity, ValidationSeverity::Warning);
    assert!(results[0].message.contains("not defined in code"));
}

#[test]
fn edge_mode_endpoints_and_permissions_are_compared() {
    use crate::schema::edge::{typed_edge, EdgeMode};
    let code = typed_edge("r", "user", "post").with_permissions([("select", "in = $auth.id")]);
    let db = typed_edge("r", "user", "comment").with_mode(EdgeMode::Schemafull);
    let r = validate_edge(&code, &db);
    assert!(r.iter().any(|r| r.message == "Edge mode mismatch"), "{r:?}");
    assert!(
        r.iter()
            .any(|r| r.message == "Edge TO table mismatch"
                && r.severity == ValidationSeverity::Error),
        "{r:?}"
    );
    assert!(!r.iter().any(|r| r.message == "Edge FROM table mismatch"));
    assert!(r.iter().any(|r| r.message == "Edge permissions mismatch"));
}

#[test]
fn edge_endpoint_missing_on_one_side_is_a_warning() {
    use crate::schema::edge::typed_edge;
    let code = typed_edge("r", "user", "post");
    let mut db = typed_edge("r", "user", "post");
    db.to_table = None;
    let r = validate_edge(&code, &db);
    assert_eq!(r.len(), 1, "{r:?}");
    assert_eq!(r[0].severity, ValidationSeverity::Warning);
}

#[test]
fn edge_db_only_fields_indexes_and_events_are_reported() {
    use crate::schema::edge::typed_edge;
    let code = typed_edge("r", "user", "post");
    let db = typed_edge("r", "user", "post")
        .with_fields([FieldDefinition::new("w", FieldType::Int)])
        .with_indexes([IndexDefinition::new("w_idx", ["w"])])
        .with_events([event("e", "true", "RETURN 1")]);
    let r = validate_edge(&code, &db);
    let fields: Vec<_> = r.iter().filter_map(|r| r.field.as_deref()).collect();
    assert_eq!(fields, vec!["w", "index:w_idx", "event:e"], "{r:?}");
    assert!(r.iter().all(|r| r.severity == ValidationSeverity::Warning));
}

#[test]
fn edge_event_body_drift_is_reported() {
    use crate::schema::edge::typed_edge;
    let code = typed_edge("r", "a", "b").with_events([event("e", "true", "CREATE x")]);
    let db = typed_edge("r", "a", "b").with_events([event("e", "true", "CREATE y")]);
    let r = validate_edge(&code, &db);
    assert_eq!(r.len(), 1, "{r:?}");
    assert!(r[0].message.contains("THEN"));
}

#[test]
fn code_edge_held_as_plain_table_is_compared_as_an_edge() {
    use crate::schema::edge::{edge_schema, EdgeMode};
    let code_edge = edge_schema("follows").with_mode(EdgeMode::Schemafull);
    let db_tables = one_table(table_schema("follows"));
    let results = validate_schema(
        &HashMap::new(),
        &db_tables,
        Some(&edges([code_edge])),
        Some(&edges([])),
    );
    assert!(results.is_empty(), "{results:?}");
}

#[test]
fn code_table_defined_as_edge_in_database_is_one_error() {
    use crate::schema::edge::typed_edge;
    let results = validate_schema(
        &one_table(table_schema("likes")),
        &HashMap::new(),
        Some(&edges([])),
        Some(&edges([typed_edge("likes", "a", "b")])),
    );
    assert_eq!(results.len(), 1, "{results:?}");
    assert_eq!(results[0].severity, ValidationSeverity::Error);
    assert!(results[0].message.contains("as an edge"));
}

#[test]
fn results_are_ordered_by_name() {
    let names = ["zeta", "alpha", "mid", "beta", "omega"];
    let code: HashMap<String, TableDefinition> = names
        .iter()
        .map(|n| ((*n).to_string(), table_schema(*n)))
        .collect();
    let results = validate_schema(&code, &HashMap::new(), None, None);
    let got: Vec<&str> = results.iter().map(|r| r.table.as_str()).collect();
    assert_eq!(got, vec!["alpha", "beta", "mid", "omega", "zeta"]);
}

// -- Attributes that used to be skipped -----------------------------------

fn one_table(table: TableDefinition) -> HashMap<String, TableDefinition> {
    let mut m = HashMap::new();
    m.insert(table.name.clone(), table);
    m
}

#[test]
fn field_reference_and_target_drift_is_reported() {
    use crate::schema::reference::ReferenceAction;
    let code = FieldDefinition::new("owner", FieldType::Record)
        .with_target_table("user")
        .with_reference(ReferenceAction::Cascade);
    let db = FieldDefinition::new("owner", FieldType::Record)
        .with_target_table("post")
        .with_reference(ReferenceAction::Reject);
    let r = validate_field("t", &code, &db);
    assert!(r
        .iter()
        .any(|r| r.message.contains("record target") && r.severity == ValidationSeverity::Error));
    assert!(r
        .iter()
        .any(|r| r.message.contains("REFERENCE") && r.severity == ValidationSeverity::Error));
}

#[test]
fn field_nullability_drift_is_reported() {
    let code = FieldDefinition::new("nick", FieldType::String).with_nullable(true);
    let db = FieldDefinition::new("nick", FieldType::String);
    let r = validate_field("t", &code, &db);
    assert_eq!(r.len(), 1, "{r:?}");
    assert_eq!(r[0].severity, ValidationSeverity::Error);
    assert!(r[0].message.contains("nullab"));
}

#[test]
fn field_computed_drift_is_reported() {
    let code = FieldDefinition::new("n", FieldType::Any).with_computed("<~post");
    let db = FieldDefinition::new("n", FieldType::Any).with_computed("<~comment");
    let r = validate_field("t", &code, &db);
    assert!(r.iter().any(|r| r.message.contains("COMPUTED")));
}

#[test]
fn field_permissions_drift_is_reported() {
    let code = FieldDefinition::new("secret", FieldType::String)
        .with_permissions([("select", "$auth.admin = true")]);
    let db = FieldDefinition::new("secret", FieldType::String);
    let r = validate_field("t", &code, &db);
    assert!(r
        .iter()
        .any(|r| r.message.contains("permissions") && r.severity == ValidationSeverity::Error));
}

#[test]
fn field_permissions_default_full_matches_explicit_full() {
    let code = FieldDefinition::new("f", FieldType::String);
    let db = FieldDefinition::new("f", FieldType::String).with_permissions([
        ("select", "FULL"),
        ("create", "FULL"),
        ("update", "WHERE true"),
    ]);
    assert_eq!(
        validate_field("t", &code, &db),
        [] as [crate::schema::validator::ValidationResult; 0]
    );
}

#[test]
fn table_permissions_drift_is_reported() {
    let code = table_schema("doc").with_permissions([("select", "owner = $auth.id")]);
    let db = table_schema("doc");
    let r = validate_table(&code, &db);
    assert_eq!(r.len(), 1, "{r:?}");
    assert_eq!(r[0].severity, ValidationSeverity::Error);
    assert!(r[0].message.contains("permissions"));
}

#[test]
fn table_permissions_grouped_actions_match_split_echo() {
    let code = table_schema("doc").with_permissions([("select, update", "owner = $auth.id")]);
    let db = table_schema("doc").with_permissions([
        ("select", "owner  =  $auth.id"),
        ("update", "owner = $auth.id"),
    ]);
    assert_eq!(
        validate_table(&code, &db),
        [] as [crate::schema::validator::ValidationResult; 0]
    );
}

#[test]
fn table_changefeed_view_and_drop_drift_is_reported() {
    use crate::schema::changefeed::ChangeFeed;
    use crate::schema::view::ViewDefinition;
    let code = table_schema("t")
        .with_changefeed(ChangeFeed::new("1d"))
        .with_view(ViewDefinition::new(["count() AS n"], ["post"]))
        .with_drop(true);
    let db = table_schema("t").with_changefeed(ChangeFeed::new("7d"));
    let r = validate_table(&code, &db);
    assert!(r.iter().any(|r| r.message.contains("change feed")), "{r:?}");
    assert!(r.iter().any(|r| r.message.contains("view")), "{r:?}");
    assert!(r.iter().any(|r| r.message.contains("DROP")), "{r:?}");
}

#[test]
fn db_edges_none_skips_edge_validation() {
    use crate::schema::edge::typed_edge;
    let mut code_edges = HashMap::new();
    code_edges.insert("likes".to_string(), typed_edge("likes", "user", "post"));
    let results = validate_schema(
        &one_table(table_schema("user")),
        &one_table(table_schema("user")),
        Some(&code_edges),
        None,
    );
    assert!(results.is_empty(), "{results:?}");
}

#[test]
fn event_condition_and_action_drift_is_reported() {
    let code = table_schema("t").with_events([event("e", "$event = 'CREATE'", "CREATE log")]);
    let db = table_schema("t").with_events([event("e", "$event = 'DELETE'", "DELETE log")]);
    let r = validate_table(&code, &db);
    assert!(r.iter().any(|r| r.message.contains("condition")), "{r:?}");
    assert!(r.iter().any(|r| r.message.contains("action")), "{r:?}");
}

#[test]
fn event_echo_reformatting_is_not_drift() {
    let code = table_schema("t").with_events([
        event("a", "$event = \"CREATE\"", "CREATE log SET x = 1"),
        event("b", "true", "{ LET $a = 1; RETURN $a }"),
    ]);
    let db = table_schema("t").with_events([
        event("a", "$event = 'CREATE'", "(CREATE log SET x = 1)"),
        event("b", "true", "LET $a = 1; RETURN $a;"),
    ]);
    let r = validate_table(&code, &db);
    assert!(r.is_empty(), "{r:?}");
}

#[test]
fn index_column_order_is_significant() {
    let code = IndexDefinition::new("ab", ["a", "b"]);
    let db = IndexDefinition::new("ab", ["b", "a"]);
    let r = validate_index("t", &code, &db);
    assert!(r.iter().any(|r| r.message.contains("columns mismatch")));
}

#[test]
fn hnsw_engine_default_efc_and_m_match_unset() {
    use crate::schema::table::{hnsw_index, HnswDistanceType};
    let code = hnsw_index(
        "v",
        "emb",
        4,
        HnswDistanceType::Cosine,
        MTreeVectorType::F32,
        None,
        None,
    );
    let db = hnsw_index(
        "v",
        "emb",
        4,
        HnswDistanceType::Cosine,
        MTreeVectorType::F32,
        Some(150),
        Some(12),
    );
    assert_eq!(
        validate_index("t", &code, &db),
        [] as [crate::schema::validator::ValidationResult; 0]
    );
}

#[test]
fn fulltext_analyzer_and_highlights_drift_is_reported() {
    let code = IndexDefinition::new("s", ["body"])
        .with_type(IndexType::Search)
        .with_analyzer("en")
        .with_highlights();
    let db = IndexDefinition::new("s", ["body"])
        .with_type(IndexType::Search)
        .with_analyzer("fr");
    let r = validate_index("t", &code, &db);
    assert!(r.iter().any(|r| r.message.contains("analyzer")), "{r:?}");
    assert!(r.iter().any(|r| r.message.contains("HIGHLIGHTS")), "{r:?}");
}

#[test]
fn engine_array_child_fields_are_not_extra() {
    let code = table_schema("t").with_fields([FieldDefinition::new("tags", FieldType::Array)]);
    let db = table_schema("t").with_fields([
        FieldDefinition::new("tags", FieldType::Array),
        FieldDefinition::new("tags.*", FieldType::String),
    ]);
    assert_eq!(
        validate_table(&code, &db),
        [] as [crate::schema::validator::ValidationResult; 0]
    );
}

// -- normalize_expression --------------------------------------------------

#[test]
fn normalize_expression_none() {
    assert_eq!(normalize_expression(None), None);
}

#[test]
fn normalize_expression_empty() {
    assert_eq!(normalize_expression(Some("   ")), None);
}

#[test]
fn normalize_expression_collapses_whitespace() {
    assert_eq!(
        normalize_expression(Some("  $value   !=    NONE  ")).as_deref(),
        Some("$value != NONE")
    );
}
