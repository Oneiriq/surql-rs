//! Schema INFO parser.
//!
//! Port of `surql/schema/parser.py`. Parses SurrealDB `INFO FOR DB` /
//! `INFO FOR TABLE` response JSON back into [`TableDefinition`],
//! [`EdgeDefinition`], [`FieldDefinition`], [`IndexDefinition`],
//! [`EventDefinition`], and [`AccessDefinition`] values.
//!
//! This is the inverse of the schema-definition → SurrealQL path: given the
//! JSON blob Surreal returns from `INFO FOR ...`, reconstruct the schema
//! definition objects. The parser accepts both shapes that `surql-py` handles:
//!
//! - object-keyed maps (`{"fields": { "name": "DEFINE FIELD ..." }}`);
//! - short-key maps (`{"fd": { "name": "..." }}`) as observed from SurrealDB.
//!
//! Input is always [`serde_json::Value`]; there is no tight coupling to the
//! `surrealdb` crate.
//!
//! The implementation is split into cohesive submodules so no file exceeds
//! the repository's 1000-LOC budget:
//!
//! - [`field`] — `DEFINE FIELD` parsing.
//! - [`index`] — `DEFINE INDEX` parsing (UNIQUE / SEARCH / MTREE / HNSW).
//! - [`event`] — `DEFINE EVENT` parsing.
//! - [`access`] — `DEFINE ACCESS` parsing (JWT + RECORD).
//! - [`table`] — `DEFINE TABLE` + `INFO FOR TABLE` parsing.
//! - [`db`] — `INFO FOR DB` parsing + edge partitioning.
//!
//! ## Example
//!
//! ```
//! use serde_json::json;
//!
//! use surql::schema::parser::{parse_table_info, parse_db_info};
//!
//! let info = json!({
//!     "tb": "DEFINE TABLE user SCHEMAFULL",
//!     "fields": { "name": "DEFINE FIELD name ON TABLE user TYPE string" }
//! });
//! let table = parse_table_info("user", &info).expect("valid table info");
//! assert_eq!(table.name, "user");
//! assert_eq!(table.fields.len(), 1);
//!
//! let db = json!({
//!     "tb": {
//!         "user": "DEFINE TABLE user SCHEMAFULL",
//!         "likes": "DEFINE TABLE likes TYPE RELATION FROM user TO post"
//!     }
//! });
//! let info = parse_db_info(&db).unwrap();
//! assert!(info.tables.contains_key("user"));
//! assert!(info.edges.contains_key("likes"));
//! ```

use std::collections::BTreeMap;

use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::{Result, SurqlError};
use crate::schema::access::AccessDefinition;
use crate::schema::edge::EdgeDefinition;
use crate::schema::table::TableDefinition;

mod access;
mod db;
mod event;
mod field;
mod index;
mod table;

pub use access::parse_access;
pub use db::parse_db_info;
pub use event::{parse_event, parse_events};
pub use field::{parse_field, parse_fields};
pub use index::{parse_index, parse_indexes};
pub use table::{parse_table_info, parse_table_mode};

// --- Shared regex helper -----------------------------------------------------

/// Build a case-insensitive [`Regex`] from a body pattern. Shared across
/// the parser submodules.
pub(super) fn regex_case_insensitive(pattern: &str) -> Regex {
    Regex::new(&format!("(?i){pattern}")).expect("valid regex pattern")
}

// --- Shared JSON helpers -----------------------------------------------------

pub(super) fn expect_object<'a>(
    value: &'a Value,
    context: &str,
) -> Result<&'a serde_json::Map<String, Value>> {
    value.as_object().ok_or_else(|| SurqlError::SchemaParse {
        reason: format!(
            "{context}: expected JSON object, got {}",
            type_name_of(value)
        ),
    })
}

fn type_name_of(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

/// Coerce a map-of-string JSON value into a `BTreeMap<String, String>`.
///
/// Non-string values are skipped so callers can tolerate server responses that
/// stash additional metadata under the same key.
pub(super) fn value_to_string_map(value: &Value) -> BTreeMap<String, String> {
    value
        .as_object()
        .map(|m| {
            m.iter()
                .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                .collect()
        })
        .unwrap_or_default()
}

/// Pick the first populated child object from `info` under any of `keys`.
pub(super) fn pick_map<'a>(
    info: &'a serde_json::Map<String, Value>,
    keys: &[&str],
) -> Option<&'a Value> {
    for k in keys {
        if let Some(v) = info.get(*k) {
            if v.as_object().is_some_and(|m| !m.is_empty()) {
                return Some(v);
            }
        }
    }
    None
}

// --- Parser state output -----------------------------------------------------

/// Collected `INFO FOR DB` response parsed into typed schema objects.
///
/// Tables and edges are keyed by name. `accesses` holds database-level
/// `DEFINE ACCESS` definitions.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DatabaseInfo {
    /// Regular (non-relation) tables.
    pub tables: BTreeMap<String, TableDefinition>,
    /// Relation-mode edge tables.
    pub edges: BTreeMap<String, EdgeDefinition>,
    /// Database-level access definitions.
    pub accesses: BTreeMap<String, AccessDefinition>,
}

#[cfg(test)]
mod tests {
    use super::db::extract_relation_endpoints;
    use super::*;
    use crate::schema::access::{
        jwt_access, record_access, AccessType, JwtConfig, RecordAccessConfig,
    };
    use crate::schema::edge::{typed_edge, EdgeMode};
    use crate::schema::fields::{datetime_field, int_field, string_field, FieldType};
    use crate::schema::table::{
        mtree_index, search_index, table_schema, unique_index, HnswDistanceType, IndexType,
        MTreeDistanceType, MTreeVectorType, TableMode,
    };
    use serde_json::json;

    // ---- parse_table_mode ---------------------------------------------------

    #[test]
    fn parse_table_mode_empty_is_schemaless() {
        assert_eq!(parse_table_mode(""), TableMode::Schemaless);
    }

    #[test]
    fn parse_table_mode_schemafull() {
        assert_eq!(
            parse_table_mode("DEFINE TABLE user SCHEMAFULL"),
            TableMode::Schemafull,
        );
    }

    #[test]
    fn parse_table_mode_schemaless() {
        assert_eq!(
            parse_table_mode("DEFINE TABLE user SCHEMALESS"),
            TableMode::Schemaless,
        );
    }

    #[test]
    fn parse_table_mode_drop() {
        assert_eq!(parse_table_mode("DEFINE TABLE user DROP"), TableMode::Drop,);
    }

    #[test]
    fn parse_table_mode_case_insensitive() {
        assert_eq!(
            parse_table_mode("define table user schemafull"),
            TableMode::Schemafull,
        );
    }

    // ---- parse_field --------------------------------------------------------

    #[test]
    fn parse_field_empty_definition_is_none() {
        assert!(parse_field("x", "").is_none());
    }

    #[test]
    fn parse_field_basic_string() {
        let f = parse_field("email", "DEFINE FIELD email ON TABLE user TYPE string").unwrap();
        assert_eq!(f.name, "email");
        assert_eq!(f.field_type, FieldType::String);
        assert!(f.assertion.is_none());
        assert!(!f.readonly);
    }

    #[test]
    fn parse_field_unknown_type_falls_back_to_any() {
        let f = parse_field("x", "DEFINE FIELD x ON TABLE t TYPE unknown_type_value").unwrap();
        assert_eq!(f.field_type, FieldType::Any);
    }

    #[test]
    fn parse_field_no_type_clause_falls_back_to_any() {
        let f = parse_field("x", "DEFINE FIELD x ON TABLE t").unwrap();
        assert_eq!(f.field_type, FieldType::Any);
    }

    #[test]
    fn parse_field_with_assertion_and_default() {
        let f = parse_field(
            "age",
            "DEFINE FIELD age ON TABLE user TYPE int ASSERT $value >= 0 DEFAULT 0",
        )
        .unwrap();
        assert_eq!(f.field_type, FieldType::Int);
        assert_eq!(f.assertion.as_deref(), Some("$value >= 0"));
        assert_eq!(f.default.as_deref(), Some("0"));
    }

    #[test]
    fn parse_field_with_value_readonly_flexible() {
        let f = parse_field(
            "full",
            "DEFINE FIELD full ON TABLE user TYPE string VALUE string::concat(a,b) READONLY FLEXIBLE",
        )
        .unwrap();
        assert_eq!(f.value.as_deref(), Some("string::concat(a,b)"));
        assert!(f.readonly);
        assert!(f.flexible);
    }

    // ---- parse_index --------------------------------------------------------

    #[test]
    fn parse_index_empty_is_none() {
        assert!(parse_index("idx", "").is_none());
    }

    #[test]
    fn parse_index_standard() {
        let idx = parse_index(
            "title_idx",
            "DEFINE INDEX title_idx ON TABLE post COLUMNS title",
        )
        .unwrap();
        assert_eq!(idx.index_type, IndexType::Standard);
        assert_eq!(idx.columns, vec!["title".to_string()]);
    }

    #[test]
    fn parse_index_unique_with_fields_keyword() {
        let idx = parse_index(
            "email_idx",
            "DEFINE INDEX email_idx ON TABLE user FIELDS email UNIQUE",
        )
        .unwrap();
        assert_eq!(idx.index_type, IndexType::Unique);
        assert_eq!(idx.columns, vec!["email".to_string()]);
    }

    #[test]
    fn parse_index_search() {
        let idx = parse_index(
            "content_search",
            "DEFINE INDEX content_search ON TABLE post COLUMNS title, content SEARCH ANALYZER ascii",
        )
        .unwrap();
        assert_eq!(idx.index_type, IndexType::Search);
        assert_eq!(
            idx.columns,
            vec!["title".to_string(), "content".to_string()]
        );
    }

    #[test]
    fn parse_index_mtree() {
        let idx = parse_index(
            "emb_idx",
            "DEFINE INDEX emb_idx ON TABLE doc COLUMNS embedding MTREE DIMENSION 1536 DIST COSINE TYPE F32",
        )
        .unwrap();
        assert_eq!(idx.index_type, IndexType::Mtree);
        assert_eq!(idx.dimension, Some(1536));
        assert_eq!(idx.distance, Some(MTreeDistanceType::Cosine));
        assert_eq!(idx.vector_type, Some(MTreeVectorType::F32));
    }

    #[test]
    fn parse_index_hnsw_with_efc_m() {
        let idx = parse_index(
            "feat_idx",
            "DEFINE INDEX feat_idx ON TABLE doc COLUMNS features HNSW DIMENSION 128 DIST COSINE TYPE F32 EFC 500 M 16",
        )
        .unwrap();
        assert_eq!(idx.index_type, IndexType::Hnsw);
        assert_eq!(idx.dimension, Some(128));
        assert_eq!(idx.hnsw_distance, Some(HnswDistanceType::Cosine));
        assert_eq!(idx.vector_type, Some(MTreeVectorType::F32));
        assert_eq!(idx.efc, Some(500));
        assert_eq!(idx.m, Some(16));
    }

    // ---- parse_event --------------------------------------------------------

    #[test]
    fn parse_event_empty_is_none() {
        assert!(parse_event("e", "").is_none());
    }

    #[test]
    fn parse_event_plain_action() {
        let ev = parse_event(
            "email_changed",
            "DEFINE EVENT email_changed ON TABLE user WHEN $before.email != $after.email THEN CREATE audit_log;",
        )
        .unwrap();
        assert_eq!(ev.condition, "$before.email != $after.email");
        assert_eq!(ev.action, "CREATE audit_log");
    }

    #[test]
    fn parse_event_brace_action() {
        let ev = parse_event(
            "n",
            "DEFINE EVENT n ON TABLE t WHEN true THEN { CREATE audit_log };",
        )
        .unwrap();
        assert_eq!(ev.action, "CREATE audit_log");
    }

    #[test]
    fn parse_event_missing_then_is_none() {
        assert!(parse_event("n", "DEFINE EVENT n ON TABLE t WHEN true;").is_none());
    }

    // ---- parse_access -------------------------------------------------------

    #[test]
    fn parse_access_jwt_hs256() {
        let acc = parse_access(
            "api",
            "DEFINE ACCESS api ON DATABASE TYPE JWT ALGORITHM HS256 KEY 'secret';",
        )
        .unwrap();
        assert_eq!(acc.access_type, AccessType::Jwt);
        let jwt = acc.jwt.expect("jwt config");
        assert_eq!(jwt.algorithm, "HS256");
        assert_eq!(jwt.key.as_deref(), Some("secret"));
    }

    #[test]
    fn parse_access_jwt_with_url_and_issuer() {
        let acc = parse_access(
            "api",
            "DEFINE ACCESS api ON DATABASE TYPE JWT ALGORITHM RS256 URL 'https://auth.example.com/jwks' WITH ISSUER 'https://auth.example.com';",
        )
        .unwrap();
        let jwt = acc.jwt.unwrap();
        assert_eq!(jwt.algorithm, "RS256");
        assert_eq!(jwt.url.as_deref(), Some("https://auth.example.com/jwks"));
        assert_eq!(jwt.issuer.as_deref(), Some("https://auth.example.com"));
    }

    #[test]
    fn parse_access_record_signup_signin_duration() {
        let acc = parse_access(
            "user_auth",
            "DEFINE ACCESS user_auth ON DATABASE TYPE RECORD SIGNUP (CREATE user) SIGNIN (SELECT * FROM user) DURATION FOR SESSION 24h, FOR TOKEN 15m;",
        )
        .unwrap();
        assert_eq!(acc.access_type, AccessType::Record);
        let rec = acc.record.unwrap();
        assert_eq!(rec.signup.as_deref(), Some("CREATE user"));
        assert_eq!(rec.signin.as_deref(), Some("SELECT * FROM user"));
        assert_eq!(acc.duration_session.as_deref(), Some("24h"));
        assert_eq!(acc.duration_token.as_deref(), Some("15m"));
    }

    #[test]
    fn parse_access_unknown_type_is_none() {
        assert!(parse_access("x", "DEFINE ACCESS x ON DATABASE TYPE BOGUS;").is_none());
    }

    // ---- parse_table_info ---------------------------------------------------

    #[test]
    fn parse_table_info_requires_object() {
        let err = parse_table_info("user", &json!("not an object")).unwrap_err();
        assert!(matches!(err, SurqlError::SchemaParse { .. }));
    }

    #[test]
    fn parse_table_info_short_keys() {
        let info = json!({
            "tb": "DEFINE TABLE user SCHEMAFULL",
            "fd": { "email": "DEFINE FIELD email ON TABLE user TYPE string" },
            "ix": { "e_idx": "DEFINE INDEX e_idx ON TABLE user COLUMNS email UNIQUE" },
            "ev": {
                "on_change": "DEFINE EVENT on_change ON TABLE user WHEN true THEN CREATE log;"
            }
        });
        let t = parse_table_info("user", &info).unwrap();
        assert_eq!(t.mode, TableMode::Schemafull);
        assert_eq!(t.fields.len(), 1);
        assert_eq!(t.indexes.len(), 1);
        assert_eq!(t.events.len(), 1);
    }

    #[test]
    fn parse_table_info_long_keys() {
        let info = json!({
            "tb": "DEFINE TABLE post SCHEMALESS",
            "fields": { "title": "DEFINE FIELD title ON TABLE post TYPE string" },
            "indexes": {},
            "events": {}
        });
        let t = parse_table_info("post", &info).unwrap();
        assert_eq!(t.mode, TableMode::Schemaless);
        assert_eq!(t.fields.len(), 1);
    }

    #[test]
    fn parse_table_info_missing_tb_defaults_schemaless() {
        let info = json!({});
        let t = parse_table_info("post", &info).unwrap();
        assert_eq!(t.mode, TableMode::Schemaless);
        assert!(t.fields.is_empty());
    }

    // ---- parse_db_info ------------------------------------------------------

    #[test]
    fn parse_db_info_requires_object() {
        let err = parse_db_info(&json!([])).unwrap_err();
        assert!(matches!(err, SurqlError::SchemaParse { .. }));
    }

    #[test]
    fn parse_db_info_partitions_tables_and_edges() {
        let info = json!({
            "tb": {
                "user": "DEFINE TABLE user SCHEMAFULL",
                "post": "DEFINE TABLE post SCHEMALESS",
                "likes": "DEFINE TABLE likes TYPE RELATION FROM user TO post"
            }
        });
        let db = parse_db_info(&info).unwrap();
        assert_eq!(db.tables.len(), 2);
        assert_eq!(db.edges.len(), 1);
        let edge = db.edges.get("likes").unwrap();
        assert_eq!(edge.from_table.as_deref(), Some("user"));
        assert_eq!(edge.to_table.as_deref(), Some("post"));
    }

    #[test]
    fn parse_db_info_accepts_long_keys_and_accesses() {
        let info = json!({
            "tables": {
                "user": "DEFINE TABLE user SCHEMAFULL"
            },
            "accesses": {
                "api": "DEFINE ACCESS api ON DATABASE TYPE JWT ALGORITHM HS256 KEY 'secret';"
            }
        });
        let db = parse_db_info(&info).unwrap();
        assert_eq!(db.tables.len(), 1);
        assert_eq!(db.accesses.len(), 1);
        assert_eq!(db.accesses.get("api").unwrap().access_type, AccessType::Jwt);
    }

    #[test]
    fn parse_db_info_empty_object_returns_empty() {
        let db = parse_db_info(&json!({})).unwrap();
        assert!(db.tables.is_empty());
        assert!(db.edges.is_empty());
        assert!(db.accesses.is_empty());
    }

    // ---- Round-trip: table --------------------------------------------------

    #[test]
    fn round_trip_table_schemafull_with_fields_and_index() {
        let code_table = table_schema("user")
            .with_fields([
                string_field("email").build_unchecked().unwrap(),
                int_field("age")
                    .assertion("$value >= 0")
                    .default("0")
                    .build_unchecked()
                    .unwrap(),
                datetime_field("created_at")
                    .default("time::now()")
                    .readonly(true)
                    .build_unchecked()
                    .unwrap(),
            ])
            .with_indexes([unique_index("email_idx", ["email"])]);

        let stmts = code_table.to_surql_all();
        let tb_stmt = stmts[0].trim_end_matches(';').to_string();
        let email_fd = stmts[1].trim_end_matches(';').to_string();
        let age_fd = stmts[2].trim_end_matches(';').to_string();
        let created_fd = stmts[3].trim_end_matches(';').to_string();
        let email_ix = stmts[4].trim_end_matches(';').to_string();

        let info = json!({
            "tb": tb_stmt,
            "fd": {
                "email": email_fd,
                "age": age_fd,
                "created_at": created_fd
            },
            "ix": { "email_idx": email_ix }
        });
        let parsed = parse_table_info("user", &info).unwrap();
        assert_eq!(parsed.mode, TableMode::Schemafull);
        assert_eq!(parsed.fields.len(), 3);
        assert_eq!(parsed.indexes.len(), 1);
        let ix = &parsed.indexes[0];
        assert_eq!(ix.index_type, IndexType::Unique);
        assert_eq!(ix.columns, vec!["email".to_string()]);
        let age = parsed
            .fields
            .iter()
            .find(|f| f.name == "age")
            .expect("age field present");
        assert_eq!(age.field_type, FieldType::Int);
        assert_eq!(age.assertion.as_deref(), Some("$value >= 0"));
    }

    #[test]
    fn round_trip_table_with_mtree_index() {
        let code_table = table_schema("doc").with_indexes([mtree_index(
            "emb_idx",
            "embedding",
            1536,
            MTreeDistanceType::Cosine,
            MTreeVectorType::F32,
        )]);

        let stmts = code_table.to_surql_all();
        let info = json!({
            "tb": stmts[0].trim_end_matches(';'),
            "ix": { "emb_idx": stmts[1].trim_end_matches(';') }
        });
        let parsed = parse_table_info("doc", &info).unwrap();
        let ix = &parsed.indexes[0];
        assert_eq!(ix.index_type, IndexType::Mtree);
        assert_eq!(ix.dimension, Some(1536));
        assert_eq!(ix.distance, Some(MTreeDistanceType::Cosine));
        assert_eq!(ix.vector_type, Some(MTreeVectorType::F32));
    }

    #[test]
    fn round_trip_table_with_search_index() {
        let code_table = table_schema("post")
            .with_indexes([search_index("content_search", ["title", "content"])]);

        let stmts = code_table.to_surql_all();
        let info = json!({
            "tb": stmts[0].trim_end_matches(';'),
            "ix": { "content_search": stmts[1].trim_end_matches(';') }
        });
        let parsed = parse_table_info("post", &info).unwrap();
        let ix = &parsed.indexes[0];
        assert_eq!(ix.index_type, IndexType::Search);
        assert_eq!(ix.columns.len(), 2);
    }

    // ---- Round-trip: DB + edge ----------------------------------------------

    #[test]
    fn round_trip_db_info_with_edge() {
        let user = table_schema("user");
        let post = table_schema("post");
        let likes = typed_edge("likes", "user", "post");

        let info = json!({
            "tb": {
                "user": user.to_surql().trim_end_matches(';'),
                "post": post.to_surql().trim_end_matches(';'),
                "likes": likes.to_surql().unwrap().trim_end_matches(';')
            }
        });
        let db = parse_db_info(&info).unwrap();
        assert_eq!(db.tables.len(), 2);
        let edge = db.edges.get("likes").expect("edge present");
        assert_eq!(edge.mode, EdgeMode::Relation);
        assert_eq!(edge.from_table.as_deref(), Some("user"));
        assert_eq!(edge.to_table.as_deref(), Some("post"));
    }

    // ---- Round-trip: access -------------------------------------------------

    #[test]
    fn round_trip_access_jwt() {
        let code = jwt_access("api", JwtConfig::hs256("secret"))
            .with_session("24h")
            .with_token("15m");
        let sql = code.to_surql().unwrap();
        let info = json!({ "ac": { "api": sql } });
        let db = parse_db_info(&info).unwrap();
        let parsed = db.accesses.get("api").unwrap();
        assert_eq!(parsed.access_type, AccessType::Jwt);
        assert_eq!(parsed.duration_session.as_deref(), Some("24h"));
        assert_eq!(parsed.duration_token.as_deref(), Some("15m"));
        let jwt = parsed.jwt.as_ref().unwrap();
        assert_eq!(jwt.algorithm, "HS256");
        assert_eq!(jwt.key.as_deref(), Some("secret"));
    }

    #[test]
    fn round_trip_access_record() {
        let code = record_access(
            "user_auth",
            RecordAccessConfig::new()
                .with_signup("CREATE user SET a = 1")
                .with_signin("SELECT * FROM user"),
        );
        let sql = code.to_surql().unwrap();
        let info = json!({ "ac": { "user_auth": sql } });
        let db = parse_db_info(&info).unwrap();
        let parsed = db.accesses.get("user_auth").unwrap();
        assert_eq!(parsed.access_type, AccessType::Record);
        let rec = parsed.record.as_ref().unwrap();
        assert_eq!(rec.signup.as_deref(), Some("CREATE user SET a = 1"));
        assert_eq!(rec.signin.as_deref(), Some("SELECT * FROM user"));
    }

    // ---- Negative cases -----------------------------------------------------

    #[test]
    fn parse_field_with_malformed_keywords_still_returns_any() {
        let f = parse_field("x", "DEFINE FIELD x ON TABLE t TYPE 123not_a_type").unwrap();
        // `123not_a_type` starts with a digit; `\w+` captures it but it is not
        // a known type keyword — we fall back to ANY.
        assert_eq!(f.field_type, FieldType::Any);
    }

    #[test]
    fn parse_db_info_rejects_array_input() {
        let err = parse_db_info(&json!([1, 2, 3])).unwrap_err();
        assert!(matches!(err, SurqlError::SchemaParse { .. }));
    }

    #[test]
    fn parse_table_info_rejects_null_input() {
        let err = parse_table_info("t", &Value::Null).unwrap_err();
        assert!(matches!(err, SurqlError::SchemaParse { .. }));
    }

    #[test]
    fn parse_index_without_columns_returns_empty_list() {
        let idx = parse_index("x", "DEFINE INDEX x ON TABLE t").unwrap();
        assert!(idx.columns.is_empty());
        assert_eq!(idx.index_type, IndexType::Standard);
    }

    #[test]
    fn parse_db_info_ignores_non_string_entries() {
        let info = json!({
            "tb": {
                "user": "DEFINE TABLE user SCHEMAFULL",
                "bogus": 42
            }
        });
        let db = parse_db_info(&info).unwrap();
        assert_eq!(db.tables.len(), 1);
        assert!(db.tables.contains_key("user"));
    }

    #[test]
    fn parse_table_info_prefers_long_keys_when_both_present() {
        let info = json!({
            "tb": "DEFINE TABLE user SCHEMAFULL",
            "fields": { "a": "DEFINE FIELD a ON TABLE user TYPE string" },
            "fd": { "b": "DEFINE FIELD b ON TABLE user TYPE int" }
        });
        let t = parse_table_info("user", &info).unwrap();
        assert_eq!(t.fields.len(), 1);
        assert_eq!(t.fields[0].name, "a");
    }

    #[test]
    fn parse_field_readonly_case_insensitive() {
        let f = parse_field("x", "DEFINE FIELD x ON TABLE t TYPE string readonly").unwrap();
        assert!(f.readonly);
    }

    #[test]
    fn parse_index_hnsw_without_efc_m() {
        let idx = parse_index(
            "feat_idx",
            "DEFINE INDEX feat_idx ON TABLE doc COLUMNS features HNSW DIMENSION 64 DIST EUCLIDEAN TYPE F64",
        )
        .unwrap();
        assert!(idx.efc.is_none());
        assert!(idx.m.is_none());
        assert_eq!(idx.hnsw_distance, Some(HnswDistanceType::Euclidean));
        assert_eq!(idx.vector_type, Some(MTreeVectorType::F64));
    }

    #[test]
    fn extract_relation_endpoints_parses_correctly() {
        let (from, to) =
            extract_relation_endpoints("DEFINE TABLE likes TYPE RELATION FROM user TO post")
                .unwrap();
        assert_eq!(from, "user");
        assert_eq!(to, "post");
    }

    #[test]
    fn extract_relation_endpoints_missing_returns_none() {
        assert!(extract_relation_endpoints("DEFINE TABLE likes SCHEMAFULL").is_none());
    }
}
