//! Validator tests: result types, presence, and table / field / index /
//! event comparisons.

use super::*;
use crate::schema::fields::{FieldDefinition, FieldType};
use crate::schema::table::{
    event, mtree_index, table_schema, EventDefinition, IndexDefinition, IndexType,
    MTreeDistanceType, MTreeVectorType, TableDefinition, TableMode,
};

fn user_with_name() -> TableDefinition {
    table_schema("user").with_fields([FieldDefinition::new("name", FieldType::String)])
}

// -- ValidationSeverity ----------------------------------------------------

#[test]
fn severity_error_value() {
    assert_eq!(ValidationSeverity::Error.as_str(), "error");
}

#[test]
fn severity_warning_value() {
    assert_eq!(ValidationSeverity::Warning.as_str(), "warning");
}

#[test]
fn severity_info_value() {
    assert_eq!(ValidationSeverity::Info.as_str(), "info");
}

#[test]
fn severity_display_is_lowercase() {
    assert_eq!(format!("{}", ValidationSeverity::Error), "error");
}

#[test]
fn severity_upper_tags() {
    assert_eq!(ValidationSeverity::Error.as_upper_str(), "ERROR");
    assert_eq!(ValidationSeverity::Warning.as_upper_str(), "WARNING");
    assert_eq!(ValidationSeverity::Info.as_upper_str(), "INFO");
}

// -- ValidationResult ------------------------------------------------------

#[test]
fn validation_result_creation_basic() {
    let r = ValidationResult::new(
        ValidationSeverity::Error,
        "user",
        Some("email".into()),
        "Field type mismatch",
        Some("string".into()),
        Some("int".into()),
    );
    assert_eq!(r.severity, ValidationSeverity::Error);
    assert_eq!(r.table, "user");
    assert_eq!(r.field.as_deref(), Some("email"));
    assert_eq!(r.message, "Field type mismatch");
    assert_eq!(r.code_value.as_deref(), Some("string"));
    assert_eq!(r.db_value.as_deref(), Some("int"));
}

#[test]
fn validation_result_none_field() {
    let r = ValidationResult::new(
        ValidationSeverity::Error,
        "user",
        None,
        "Table missing",
        Some("exists".into()),
        Some("missing".into()),
    );
    assert!(r.field.is_none());
}

#[test]
fn validation_result_none_values() {
    let r = ValidationResult::new(
        ValidationSeverity::Info,
        "user",
        Some("name".into()),
        "info",
        None,
        None,
    );
    assert!(r.code_value.is_none());
    assert!(r.db_value.is_none());
}

#[test]
fn validation_result_display_with_field() {
    let r = ValidationResult::new(
        ValidationSeverity::Error,
        "user",
        Some("email".into()),
        "Field type mismatch",
        Some("string".into()),
        Some("int".into()),
    );
    let s = r.to_string();
    assert!(s.contains("[ERROR]"));
    assert!(s.contains("user.email"));
    assert!(s.contains("Field type mismatch"));
    assert!(s.contains("code: string"));
    assert!(s.contains("db: int"));
}

#[test]
fn validation_result_display_without_field() {
    let r = ValidationResult::new(
        ValidationSeverity::Warning,
        "post",
        None,
        "Table missing",
        Some("missing".into()),
        Some("exists".into()),
    );
    let s = r.to_string();
    assert!(s.contains("[WARNING]"));
    assert!(s.contains("post"));
    assert!(!s.contains("post."));
}

#[test]
fn validation_result_display_without_values() {
    let r = ValidationResult::new(
        ValidationSeverity::Info,
        "user",
        Some("name".into()),
        "Some info",
        None,
        None,
    );
    let s = r.to_string();
    assert!(s.contains("[INFO]"));
    assert!(!s.contains("code:"));
}

// -- Missing tables --------------------------------------------------------

#[test]
fn table_missing_from_database() {
    let mut code = HashMap::new();
    code.insert("user".into(), user_with_name());
    let db = HashMap::new();

    let results = validate_schema(&code, &db, None, None);
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].severity, ValidationSeverity::Error);
    assert_eq!(results[0].table, "user");
    assert!(results[0].message.contains("missing from database"));
}

#[test]
fn multiple_tables_missing() {
    let mut code = HashMap::new();
    code.insert("user".into(), table_schema("user"));
    code.insert("post".into(), table_schema("post"));
    let db = HashMap::new();

    let results = validate_schema(&code, &db, None, None);
    assert_eq!(results.len(), 2);
    let names: BTreeSet<&str> = results.iter().map(|r| r.table.as_str()).collect();
    assert!(names.contains("user"));
    assert!(names.contains("post"));
}

// -- Extra tables ----------------------------------------------------------

#[test]
fn table_in_database_not_in_code() {
    let code: HashMap<String, TableDefinition> = HashMap::new();
    let mut db = HashMap::new();
    db.insert("legacy_table".into(), table_schema("legacy_table"));

    let results = validate_schema(&code, &db, None, None);
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].severity, ValidationSeverity::Warning);
    assert_eq!(results[0].table, "legacy_table");
    assert!(results[0].message.contains("not defined in code"));
}

// -- Matching schemas ------------------------------------------------------

#[test]
fn schemas_match_returns_empty() {
    let mut code = HashMap::new();
    code.insert("user".into(), user_with_name());
    let mut db = HashMap::new();
    db.insert("user".into(), user_with_name());

    let results = validate_schema(&code, &db, None, None);
    assert!(results.is_empty());
}

// -- Field mismatches ------------------------------------------------------

#[test]
fn field_type_mismatch() {
    let code_table =
        table_schema("user").with_fields([FieldDefinition::new("age", FieldType::Int)]);
    let db_table =
        table_schema("user").with_fields([FieldDefinition::new("age", FieldType::String)]);

    let mut code = HashMap::new();
    code.insert("user".into(), code_table);
    let mut db = HashMap::new();
    db.insert("user".into(), db_table);

    let results = validate_schema(&code, &db, None, None);
    let mismatches: Vec<_> = results
        .iter()
        .filter(|r| r.message.to_lowercase().contains("type mismatch"))
        .collect();
    assert!(!mismatches.is_empty());
    assert_eq!(mismatches[0].severity, ValidationSeverity::Error);
}

#[test]
fn field_missing_from_database() {
    let code_table = table_schema("user").with_fields([
        FieldDefinition::new("name", FieldType::String),
        FieldDefinition::new("email", FieldType::String),
    ]);
    let db_table =
        table_schema("user").with_fields([FieldDefinition::new("name", FieldType::String)]);

    let mut code = HashMap::new();
    code.insert("user".into(), code_table);
    let mut db = HashMap::new();
    db.insert("user".into(), db_table);

    let results = validate_schema(&code, &db, None, None);
    let missing: Vec<_> = results
        .iter()
        .filter(|r| r.field.as_deref() == Some("email"))
        .collect();
    assert_eq!(missing.len(), 1);
    assert_eq!(missing[0].severity, ValidationSeverity::Error);
    assert!(missing[0].message.contains("missing from database"));
}

#[test]
fn extra_field_in_database() {
    let code_table =
        table_schema("user").with_fields([FieldDefinition::new("name", FieldType::String)]);
    let db_table = table_schema("user").with_fields([
        FieldDefinition::new("name", FieldType::String),
        FieldDefinition::new("legacy_field", FieldType::Int),
    ]);

    let mut code = HashMap::new();
    code.insert("user".into(), code_table);
    let mut db = HashMap::new();
    db.insert("user".into(), db_table);

    let results = validate_schema(&code, &db, None, None);
    let extra: Vec<_> = results
        .iter()
        .filter(|r| r.field.as_deref() == Some("legacy_field"))
        .collect();
    assert_eq!(extra.len(), 1);
    assert_eq!(extra[0].severity, ValidationSeverity::Warning);
    assert!(extra[0].message.contains("not defined in code"));
}

#[test]
fn field_assertion_mismatch() {
    let code_table =
        table_schema("user").with_fields([FieldDefinition::new("email", FieldType::String)
            .with_assertion("string::is::email($value)")]);
    let db_table =
        table_schema("user").with_fields([
            FieldDefinition::new("email", FieldType::String).with_assertion("$value != NONE")
        ]);

    let mut code = HashMap::new();
    code.insert("user".into(), code_table);
    let mut db = HashMap::new();
    db.insert("user".into(), db_table);

    let results = validate_schema(&code, &db, None, None);
    let assertions: Vec<_> = results
        .iter()
        .filter(|r| r.message.to_lowercase().contains("assertion"))
        .collect();
    assert!(!assertions.is_empty());
    assert_eq!(assertions[0].severity, ValidationSeverity::Warning);
}

#[test]
fn field_default_mismatch() {
    let code_table = table_schema("t")
        .with_fields([FieldDefinition::new("x", FieldType::Int).with_default("0")]);
    let db_table = table_schema("t")
        .with_fields([FieldDefinition::new("x", FieldType::Int).with_default("1")]);

    let results = validate_field("t", &code_table.fields[0], &db_table.fields[0]);
    assert!(results
        .iter()
        .any(|r| r.message.contains("default value mismatch")));
}

#[test]
fn field_value_mismatch() {
    let code_table = table_schema("t")
        .with_fields([FieldDefinition::new("x", FieldType::Int).with_value("1 + 1")]);
    let db_table = table_schema("t")
        .with_fields([FieldDefinition::new("x", FieldType::Int).with_value("2 + 2")]);

    let results = validate_field("t", &code_table.fields[0], &db_table.fields[0]);
    assert!(results
        .iter()
        .any(|r| r.message.contains("computed value mismatch")));
}

#[test]
fn field_readonly_mismatch_is_info() {
    let code_field = FieldDefinition::new("x", FieldType::Int).readonly(true);
    let db_field = FieldDefinition::new("x", FieldType::Int).readonly(false);
    let r = validate_field("t", &code_field, &db_field);
    let msg = r.iter().find(|r| r.message.contains("readonly")).unwrap();
    assert_eq!(msg.severity, ValidationSeverity::Info);
}

#[test]
fn field_flexible_mismatch_is_info() {
    let code_field = FieldDefinition::new("x", FieldType::Object).flexible(true);
    let db_field = FieldDefinition::new("x", FieldType::Object).flexible(false);
    let r = validate_field("t", &code_field, &db_field);
    let msg = r.iter().find(|r| r.message.contains("flexible")).unwrap();
    assert_eq!(msg.severity, ValidationSeverity::Info);
}

#[test]
fn field_assertion_whitespace_normalized() {
    let code_field =
        FieldDefinition::new("x", FieldType::String).with_assertion("$value  !=  NONE");
    let db_field = FieldDefinition::new("x", FieldType::String).with_assertion("$value != NONE");
    let r = validate_field("t", &code_field, &db_field);
    assert!(!r.iter().any(|r| r.message.contains("assertion")));
}

// -- Index mismatches ------------------------------------------------------

#[test]
fn index_missing_from_database() {
    let code_table =
        table_schema("user").with_indexes([
            IndexDefinition::new("email_idx", ["email"]).with_type(IndexType::Unique)
        ]);
    let db_table = table_schema("user");

    let mut code = HashMap::new();
    code.insert("user".into(), code_table);
    let mut db = HashMap::new();
    db.insert("user".into(), db_table);

    let results = validate_schema(&code, &db, None, None);
    let missing: Vec<_> = results
        .iter()
        .filter(|r| r.field.as_deref() == Some("index:email_idx"))
        .collect();
    assert_eq!(missing.len(), 1);
    assert_eq!(missing[0].severity, ValidationSeverity::Error);
    assert!(missing[0].message.contains("missing from database"));
}

#[test]
fn extra_index_in_database() {
    let code_table = table_schema("user");
    let db_table =
        table_schema("user").with_indexes([IndexDefinition::new("legacy_idx", ["legacy"])]);

    let mut code = HashMap::new();
    code.insert("user".into(), code_table);
    let mut db = HashMap::new();
    db.insert("user".into(), db_table);

    let results = validate_schema(&code, &db, None, None);
    let extra: Vec<_> = results
        .iter()
        .filter(|r| r.field.as_deref() == Some("index:legacy_idx"))
        .collect();
    assert_eq!(extra.len(), 1);
    assert_eq!(extra[0].severity, ValidationSeverity::Warning);
}

#[test]
fn index_type_mismatch() {
    let code_table =
        table_schema("user").with_indexes([
            IndexDefinition::new("email_idx", ["email"]).with_type(IndexType::Unique)
        ]);
    let db_table =
        table_schema("user").with_indexes([IndexDefinition::new("email_idx", ["email"])]);

    let mut code = HashMap::new();
    code.insert("user".into(), code_table);
    let mut db = HashMap::new();
    db.insert("user".into(), db_table);

    let results = validate_schema(&code, &db, None, None);
    let mismatches: Vec<_> = results
        .iter()
        .filter(|r| r.message.contains("Index type mismatch"))
        .collect();
    assert!(!mismatches.is_empty());
    assert_eq!(mismatches[0].severity, ValidationSeverity::Error);
}

#[test]
fn index_columns_mismatch() {
    let code_table = table_schema("user").with_indexes([IndexDefinition::new(
        "name_idx",
        ["first_name", "last_name"],
    )]);
    let db_table =
        table_schema("user").with_indexes([IndexDefinition::new("name_idx", ["first_name"])]);

    let mut code = HashMap::new();
    code.insert("user".into(), code_table);
    let mut db = HashMap::new();
    db.insert("user".into(), db_table);

    let results = validate_schema(&code, &db, None, None);
    let mismatches: Vec<_> = results
        .iter()
        .filter(|r| r.message.contains("columns mismatch"))
        .collect();
    assert!(!mismatches.is_empty());
    assert_eq!(mismatches[0].severity, ValidationSeverity::Error);
}

#[test]
fn mtree_index_dimension_mismatch() {
    let code_table = table_schema("document").with_indexes([mtree_index(
        "vec_idx",
        "embedding",
        1024,
        MTreeDistanceType::Cosine,
        MTreeVectorType::F32,
    )]);
    let db_table = table_schema("document").with_indexes([mtree_index(
        "vec_idx",
        "embedding",
        768,
        MTreeDistanceType::Cosine,
        MTreeVectorType::F32,
    )]);

    let mut code = HashMap::new();
    code.insert("document".into(), code_table);
    let mut db = HashMap::new();
    db.insert("document".into(), db_table);

    let results = validate_schema(&code, &db, None, None);
    let dims: Vec<_> = results
        .iter()
        .filter(|r| r.message.contains("dimension mismatch"))
        .collect();
    assert!(!dims.is_empty());
    assert_eq!(dims[0].severity, ValidationSeverity::Error);
}

#[test]
fn mtree_index_distance_mismatch_is_warning() {
    let code_idx = mtree_index(
        "v",
        "emb",
        32,
        MTreeDistanceType::Cosine,
        MTreeVectorType::F32,
    );
    let db_idx = mtree_index(
        "v",
        "emb",
        32,
        MTreeDistanceType::Euclidean,
        MTreeVectorType::F32,
    );
    let results = validate_index("t", &code_idx, &db_idx);
    let msg = results
        .iter()
        .find(|r| r.message.contains("distance metric mismatch"))
        .unwrap();
    assert_eq!(msg.severity, ValidationSeverity::Warning);
}

#[test]
fn mtree_index_vector_type_mismatch_is_warning() {
    let code_idx = mtree_index(
        "v",
        "emb",
        32,
        MTreeDistanceType::Cosine,
        MTreeVectorType::F32,
    );
    let db_idx = mtree_index(
        "v",
        "emb",
        32,
        MTreeDistanceType::Cosine,
        MTreeVectorType::F64,
    );
    let results = validate_index("t", &code_idx, &db_idx);
    let msg = results
        .iter()
        .find(|r| r.message.contains("vector type mismatch"))
        .unwrap();
    assert_eq!(msg.severity, ValidationSeverity::Warning);
}

#[test]
fn hnsw_index_dimension_mismatch() {
    use crate::schema::table::{hnsw_index, HnswDistanceType};
    let code_idx = hnsw_index(
        "v",
        "emb",
        128,
        HnswDistanceType::Cosine,
        MTreeVectorType::F32,
        None,
        None,
    );
    let db_idx = hnsw_index(
        "v",
        "emb",
        64,
        HnswDistanceType::Cosine,
        MTreeVectorType::F32,
        None,
        None,
    );
    let results = validate_index("t", &code_idx, &db_idx);
    let msg = results
        .iter()
        .find(|r| r.message.contains("HNSW index dimension mismatch"))
        .unwrap();
    assert_eq!(msg.severity, ValidationSeverity::Error);
}

#[test]
fn hnsw_index_efc_m_mismatches() {
    use crate::schema::table::{hnsw_index, HnswDistanceType};
    let code_idx = hnsw_index(
        "v",
        "emb",
        64,
        HnswDistanceType::Cosine,
        MTreeVectorType::F32,
        Some(200),
        Some(16),
    );
    let db_idx = hnsw_index(
        "v",
        "emb",
        64,
        HnswDistanceType::Cosine,
        MTreeVectorType::F32,
        Some(400),
        Some(32),
    );
    let results = validate_index("t", &code_idx, &db_idx);
    assert!(results
        .iter()
        .any(|r| r.message.contains("HNSW index EFC mismatch")));
    assert!(results
        .iter()
        .any(|r| r.message.contains("HNSW index M mismatch")));
}

#[test]
fn hnsw_index_distance_vector_type_mismatches() {
    use crate::schema::table::{hnsw_index, HnswDistanceType};
    let code_idx = hnsw_index(
        "v",
        "emb",
        64,
        HnswDistanceType::Cosine,
        MTreeVectorType::F32,
        None,
        None,
    );
    let db_idx = hnsw_index(
        "v",
        "emb",
        64,
        HnswDistanceType::Euclidean,
        MTreeVectorType::F64,
        None,
        None,
    );
    let results = validate_index("t", &code_idx, &db_idx);
    assert!(results
        .iter()
        .any(|r| r.message.contains("HNSW index distance metric mismatch")));
    assert!(results
        .iter()
        .any(|r| r.message.contains("HNSW index vector type mismatch")));
}

// -- Table mode mismatch --------------------------------------------------

#[test]
fn table_mode_mismatch_schemafull_vs_schemaless() {
    let code_table = table_schema("user").with_mode(TableMode::Schemafull);
    let db_table = table_schema("user").with_mode(TableMode::Schemaless);

    let mut code = HashMap::new();
    code.insert("user".into(), code_table);
    let mut db = HashMap::new();
    db.insert("user".into(), db_table);

    let results = validate_schema(&code, &db, None, None);
    let modes: Vec<_> = results
        .iter()
        .filter(|r| r.message.to_lowercase().contains("mode mismatch"))
        .collect();
    assert!(!modes.is_empty());
    assert_eq!(modes[0].severity, ValidationSeverity::Error);
}

// -- Event mismatches ------------------------------------------------------

#[test]
fn event_missing_from_database() {
    let code_table = table_schema("user").with_events([event("e", "true", "RETURN 1")]);
    let db_table = table_schema("user");

    let mut code = HashMap::new();
    code.insert("user".into(), code_table);
    let mut db = HashMap::new();
    db.insert("user".into(), db_table);

    let results = validate_schema(&code, &db, None, None);
    let missing: Vec<_> = results
        .iter()
        .filter(|r| r.field.as_deref() == Some("event:e"))
        .collect();
    assert_eq!(missing.len(), 1);
    assert_eq!(missing[0].severity, ValidationSeverity::Error);
}

#[test]
fn extra_event_in_database() {
    let code_table = table_schema("user");
    let db_table =
        table_schema("user").with_events([EventDefinition::new("e", "true", "RETURN 1")]);

    let mut code = HashMap::new();
    code.insert("user".into(), code_table);
    let mut db = HashMap::new();
    db.insert("user".into(), db_table);

    let results = validate_schema(&code, &db, None, None);
    let extra: Vec<_> = results
        .iter()
        .filter(|r| r.field.as_deref() == Some("event:e"))
        .collect();
    assert_eq!(extra.len(), 1);
    assert_eq!(extra[0].severity, ValidationSeverity::Warning);
}
