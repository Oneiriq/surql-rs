//! SurrealQL schema generation from definition objects.
//!
//! Port of `surql/schema/sql.py`. Exposes free functions that compose the
//! `DEFINE` statements produced by [`TableDefinition`], [`EdgeDefinition`],
//! and [`AccessDefinition`] into reusable scripts.
//!
//! Per-definition rendering lives on the types themselves
//! ([`TableDefinition::to_surql_all_with_options`],
//! [`EdgeDefinition::to_surql_all_with_options`],
//! [`AccessDefinition::to_surql`]); the helpers in this module mirror the
//! Python API that delivers those lists to consumers plus the combined
//! [`generate_schema_sql`] script builder.

use std::collections::BTreeMap;

use crate::error::Result;

use super::access::AccessDefinition;
use super::edge::EdgeDefinition;
use super::table::TableDefinition;

/// Render all `DEFINE` statements required to create `table`.
///
/// The first entry is the `DEFINE TABLE` line, followed by each field,
/// index, event, and permission statement in Python-compatible order.
///
/// `if_not_exists` adds the `IF NOT EXISTS` clause to every emitted
/// `DEFINE` statement where SurrealDB supports it.
///
/// # Examples
///
/// ```
/// use surql::schema::{generate_table_sql, table_schema, TableMode};
///
/// let t = table_schema("user").with_mode(TableMode::Schemafull);
/// let stmts = generate_table_sql(&t, false);
/// assert_eq!(stmts[0], "DEFINE TABLE user SCHEMAFULL;");
/// ```
pub fn generate_table_sql(table: &TableDefinition, if_not_exists: bool) -> Vec<String> {
    table.to_surql_all_with_options(if_not_exists)
}

/// Render all `DEFINE` statements required to create `edge`.
///
/// Returns [`SurqlError::Validation`](crate::error::SurqlError::Validation)
/// when an edge in [`EdgeMode::Relation`](super::edge::EdgeMode::Relation)
/// is missing either `from_table` or `to_table`.
///
/// # Examples
///
/// ```
/// use surql::schema::{generate_edge_sql, typed_edge};
///
/// let e = typed_edge("likes", "user", "post");
/// let stmts = generate_edge_sql(&e, false).unwrap();
/// assert_eq!(stmts[0], "DEFINE TABLE likes TYPE RELATION FROM user TO post;");
/// ```
pub fn generate_edge_sql(edge: &EdgeDefinition, if_not_exists: bool) -> Result<Vec<String>> {
    edge.to_surql_all_with_options(if_not_exists)
}

/// Render the `DEFINE ACCESS` statement(s) for `access`.
///
/// Wraps [`AccessDefinition::to_surql`] into a `Vec<String>` so the output
/// type matches the other generators. Validation runs first; an invalid
/// definition yields
/// [`SurqlError::Validation`](crate::error::SurqlError::Validation).
///
/// # Examples
///
/// ```
/// use surql::schema::{generate_access_sql, jwt_access, JwtConfig};
///
/// let a = jwt_access("api", JwtConfig::hs256("secret"));
/// let stmts = generate_access_sql(&a).unwrap();
/// assert_eq!(
///     stmts[0],
///     "DEFINE ACCESS api ON DATABASE TYPE JWT ALGORITHM HS256 KEY 'secret';"
/// );
/// ```
pub fn generate_access_sql(access: &AccessDefinition) -> Result<Vec<String>> {
    Ok(vec![access.to_surql()?])
}

/// Render a complete SurrealQL schema script.
///
/// Tables render first, followed by edges. Each definition block is
/// separated by a blank line for readability, matching the Python port.
/// When both `tables` and `edges` are empty, the returned string is empty.
///
/// # Examples
///
/// ```
/// use std::collections::BTreeMap;
/// use surql::schema::{generate_schema_sql, table_schema, typed_edge, TableMode};
///
/// let mut tables = BTreeMap::new();
/// tables.insert("user".to_string(), table_schema("user").with_mode(TableMode::Schemafull));
///
/// let mut edges = BTreeMap::new();
/// edges.insert("likes".to_string(), typed_edge("likes", "user", "post"));
///
/// let sql = generate_schema_sql(Some(&tables), Some(&edges), false).unwrap();
/// assert!(sql.contains("DEFINE TABLE user SCHEMAFULL"));
/// assert!(sql.contains("DEFINE TABLE likes TYPE RELATION FROM user TO post"));
/// ```
pub fn generate_schema_sql(
    tables: Option<&BTreeMap<String, TableDefinition>>,
    edges: Option<&BTreeMap<String, EdgeDefinition>>,
    if_not_exists: bool,
) -> Result<String> {
    let mut all_statements: Vec<String> = Vec::new();

    if let Some(tables) = tables {
        for table in tables.values() {
            all_statements.extend(generate_table_sql(table, if_not_exists));
            all_statements.push(String::new());
        }
    }

    if let Some(edges) = edges {
        for edge in edges.values() {
            all_statements.extend(generate_edge_sql(edge, if_not_exists)?);
            all_statements.push(String::new());
        }
    }

    Ok(all_statements.join("\n").trim().to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::access::{jwt_access, record_access, JwtConfig, RecordAccessConfig};
    use crate::schema::edge::{edge_schema, typed_edge, EdgeMode};
    use crate::schema::fields::{datetime_field, int_field, string_field};
    use crate::schema::table::{
        event, index, search_index, table_schema, unique_index, IndexType, TableMode,
    };

    // ---- generate_table_sql ----

    #[test]
    fn generate_table_sql_schemafull_minimal() {
        let t = table_schema("user").with_mode(TableMode::Schemafull);
        let stmts = generate_table_sql(&t, false);
        assert_eq!(stmts[0], "DEFINE TABLE user SCHEMAFULL;");
    }

    #[test]
    fn generate_table_sql_schemaless() {
        let t = table_schema("log").with_mode(TableMode::Schemaless);
        let stmts = generate_table_sql(&t, false);
        assert_eq!(stmts[0], "DEFINE TABLE log SCHEMALESS;");
    }

    #[test]
    fn generate_table_sql_with_fields() {
        let t = table_schema("user")
            .with_mode(TableMode::Schemafull)
            .with_fields([
                string_field("name").build_unchecked().unwrap(),
                int_field("age").build_unchecked().unwrap(),
            ]);
        let stmts = generate_table_sql(&t, false);
        assert!(stmts
            .iter()
            .any(|s| s.contains("DEFINE FIELD name ON TABLE user TYPE string")));
        assert!(stmts
            .iter()
            .any(|s| s.contains("DEFINE FIELD age ON TABLE user TYPE int")));
    }

    #[test]
    fn generate_table_sql_with_field_assertion() {
        let t = table_schema("user").with_fields([string_field("email")
            .assertion("string::is::email($value)")
            .build_unchecked()
            .unwrap()]);
        let stmts = generate_table_sql(&t, false);
        assert!(stmts
            .iter()
            .any(|s| s.contains("ASSERT string::is::email($value)")));
    }

    #[test]
    fn generate_table_sql_with_field_default() {
        let t = table_schema("event").with_fields([datetime_field("created_at")
            .default("time::now()")
            .build_unchecked()
            .unwrap()]);
        let stmts = generate_table_sql(&t, false);
        assert!(stmts.iter().any(|s| s.contains("DEFAULT time::now()")));
    }

    #[test]
    fn generate_table_sql_with_readonly_field() {
        let t = table_schema("event").with_fields([datetime_field("created_at")
            .readonly(true)
            .build_unchecked()
            .unwrap()]);
        let stmts = generate_table_sql(&t, false);
        assert!(stmts.iter().any(|s| s.contains("READONLY")));
    }

    #[test]
    fn generate_table_sql_with_unique_index() {
        let t = table_schema("user").with_indexes([unique_index("email_idx", ["email"])]);
        let stmts = generate_table_sql(&t, false);
        assert!(stmts
            .iter()
            .any(|s| s.contains("DEFINE INDEX email_idx ON TABLE user COLUMNS email UNIQUE")));
    }

    #[test]
    fn generate_table_sql_with_standard_index() {
        let t = table_schema("post")
            .with_indexes([index("title_idx", ["title"]).with_type(IndexType::Standard)]);
        let stmts = generate_table_sql(&t, false);
        assert!(stmts
            .iter()
            .any(|s| s.contains("DEFINE INDEX title_idx ON TABLE post COLUMNS title")));
    }

    #[test]
    fn generate_table_sql_with_search_index() {
        let t = table_schema("post").with_indexes([search_index("content_search", ["content"])]);
        let stmts = generate_table_sql(&t, false);
        assert!(stmts.iter().any(|s| s.contains("SEARCH ANALYZER ascii")));
    }

    #[test]
    fn generate_table_sql_with_event() {
        let t = table_schema("user").with_events([event(
            "email_changed",
            "$before.email != $after.email",
            "CREATE audit_log SET user = $value.id",
        )]);
        let stmts = generate_table_sql(&t, false);
        assert!(stmts
            .iter()
            .any(|s| s.contains("DEFINE EVENT email_changed ON TABLE user")));
        assert!(stmts
            .iter()
            .any(|s| s.contains("WHEN $before.email != $after.email")));
    }

    #[test]
    fn generate_table_sql_with_permissions() {
        let t = table_schema("user").with_permissions([("select", "$auth.id = id")]);
        let stmts = generate_table_sql(&t, false);
        assert!(stmts
            .iter()
            .any(|s| s.contains("FOR SELECT") && s.contains("$auth.id = id")));
    }

    #[test]
    fn generate_table_sql_minimal_returns_single_statement() {
        let t = table_schema("empty");
        let stmts = generate_table_sql(&t, false);
        assert_eq!(stmts.len(), 1);
    }

    #[test]
    fn generate_table_sql_statement_order_define_table_first() {
        let t = table_schema("user")
            .with_fields([string_field("name").build_unchecked().unwrap()])
            .with_indexes([unique_index("name_idx", ["name"])]);
        let stmts = generate_table_sql(&t, false);
        assert!(stmts[0].starts_with("DEFINE TABLE"));
    }

    // ---- generate_edge_sql ----

    #[test]
    fn generate_edge_sql_relation_with_from_to() {
        let e = typed_edge("likes", "user", "post");
        let stmts = generate_edge_sql(&e, false).unwrap();
        assert_eq!(
            stmts[0],
            "DEFINE TABLE likes TYPE RELATION FROM user TO post;"
        );
    }

    #[test]
    fn generate_edge_sql_schemafull() {
        let e = edge_schema("entity_relation").with_mode(EdgeMode::Schemafull);
        let stmts = generate_edge_sql(&e, false).unwrap();
        assert_eq!(stmts[0], "DEFINE TABLE entity_relation SCHEMAFULL;");
    }

    #[test]
    fn generate_edge_sql_schemaless() {
        let e = edge_schema("loose_rel").with_mode(EdgeMode::Schemaless);
        let stmts = generate_edge_sql(&e, false).unwrap();
        assert_eq!(stmts[0], "DEFINE TABLE loose_rel SCHEMALESS;");
    }

    #[test]
    fn generate_edge_sql_with_fields() {
        let e = typed_edge("likes", "user", "post").with_fields([datetime_field("created_at")
            .default("time::now()")
            .build_unchecked()
            .unwrap()]);
        let stmts = generate_edge_sql(&e, false).unwrap();
        assert!(stmts
            .iter()
            .any(|s| s.contains("DEFINE FIELD created_at ON TABLE likes TYPE datetime")));
    }

    #[test]
    fn generate_edge_sql_relation_missing_from_errors() {
        let e = edge_schema("likes").with_to_table("post");
        let err = generate_edge_sql(&e, false).unwrap_err();
        assert!(err.to_string().contains("RELATION"));
    }

    #[test]
    fn generate_edge_sql_relation_missing_to_errors() {
        let e = edge_schema("likes").with_from_table("user");
        assert!(generate_edge_sql(&e, false).is_err());
    }

    #[test]
    fn generate_edge_sql_relation_missing_both_errors() {
        let e = edge_schema("likes");
        assert!(generate_edge_sql(&e, false).is_err());
    }

    #[test]
    fn generate_edge_sql_schemafull_does_not_require_tables() {
        let e = edge_schema("entity_rel").with_mode(EdgeMode::Schemafull);
        let stmts = generate_edge_sql(&e, false).unwrap();
        assert!(!stmts.is_empty());
    }

    #[test]
    fn generate_edge_sql_starts_with_define_table() {
        let e = typed_edge("follows", "user", "user");
        let stmts = generate_edge_sql(&e, false).unwrap();
        assert!(stmts[0].starts_with("DEFINE TABLE"));
    }

    // ---- generate_access_sql ----

    #[test]
    fn generate_access_sql_jwt_hs256() {
        let a = jwt_access("api", JwtConfig::hs256("secret"));
        let stmts = generate_access_sql(&a).unwrap();
        assert_eq!(stmts.len(), 1);
        assert_eq!(
            stmts[0],
            "DEFINE ACCESS api ON DATABASE TYPE JWT ALGORITHM HS256 KEY 'secret';"
        );
    }

    #[test]
    fn generate_access_sql_record() {
        let a = record_access(
            "user_auth",
            RecordAccessConfig::new()
                .with_signup("CREATE user SET a = 1")
                .with_signin("SELECT * FROM user"),
        );
        let stmts = generate_access_sql(&a).unwrap();
        assert!(stmts[0].contains("TYPE RECORD"));
        assert!(stmts[0].contains("SIGNUP (CREATE user SET a = 1)"));
    }

    #[test]
    fn generate_access_sql_validates() {
        let mut a = jwt_access("api", JwtConfig::hs256("secret"));
        a.jwt = None;
        assert!(generate_access_sql(&a).is_err());
    }

    // ---- generate_schema_sql ----

    #[test]
    fn generate_schema_sql_combines_tables_and_edges() {
        let mut tables = BTreeMap::new();
        tables.insert(
            "user".to_string(),
            table_schema("user").with_mode(TableMode::Schemafull),
        );
        let mut edges = BTreeMap::new();
        edges.insert("likes".to_string(), typed_edge("likes", "user", "post"));
        let sql = generate_schema_sql(Some(&tables), Some(&edges), false).unwrap();
        assert!(sql.contains("DEFINE TABLE user SCHEMAFULL"));
        assert!(sql.contains("DEFINE TABLE likes TYPE RELATION FROM user TO post"));
    }

    #[test]
    fn generate_schema_sql_tables_only() {
        let mut tables = BTreeMap::new();
        tables.insert("user".to_string(), table_schema("user"));
        let sql = generate_schema_sql(Some(&tables), None, false).unwrap();
        assert!(sql.contains("DEFINE TABLE user"));
    }

    #[test]
    fn generate_schema_sql_edges_only() {
        let mut edges = BTreeMap::new();
        edges.insert("follows".to_string(), typed_edge("follows", "user", "user"));
        let sql = generate_schema_sql(None, Some(&edges), false).unwrap();
        assert!(sql.contains("DEFINE TABLE follows TYPE RELATION"));
    }

    #[test]
    fn generate_schema_sql_empty_returns_empty_string() {
        let sql = generate_schema_sql(None, None, false).unwrap();
        assert_eq!(sql, "");
    }

    #[test]
    fn generate_schema_sql_multiple_tables() {
        let mut tables = BTreeMap::new();
        tables.insert("user".to_string(), table_schema("user"));
        tables.insert("post".to_string(), table_schema("post"));
        let sql = generate_schema_sql(Some(&tables), None, false).unwrap();
        assert!(sql.contains("DEFINE TABLE user"));
        assert!(sql.contains("DEFINE TABLE post"));
    }

    #[test]
    fn generate_schema_sql_tables_separated_by_blank_lines() {
        let mut tables = BTreeMap::new();
        tables.insert("user".to_string(), table_schema("user"));
        tables.insert("post".to_string(), table_schema("post"));
        let sql = generate_schema_sql(Some(&tables), None, false).unwrap();
        assert!(sql.split('\n').any(str::is_empty));
    }

    #[test]
    fn generate_schema_sql_propagates_edge_errors() {
        let mut edges = BTreeMap::new();
        edges.insert("likes".to_string(), edge_schema("likes"));
        assert!(generate_schema_sql(None, Some(&edges), false).is_err());
    }

    // ---- IF NOT EXISTS support ----

    #[test]
    fn table_definition_includes_if_not_exists() {
        let t = table_schema("user").with_mode(TableMode::Schemafull);
        let stmts = generate_table_sql(&t, true);
        assert_eq!(stmts[0], "DEFINE TABLE IF NOT EXISTS user SCHEMAFULL;");
    }

    #[test]
    fn field_definition_includes_if_not_exists() {
        let t = table_schema("user")
            .with_mode(TableMode::Schemafull)
            .with_fields([string_field("name").build_unchecked().unwrap()]);
        let stmts = generate_table_sql(&t, true);
        assert!(stmts
            .iter()
            .any(|s| s == "DEFINE FIELD IF NOT EXISTS name ON TABLE user TYPE string;"));
    }

    #[test]
    fn index_definition_includes_if_not_exists() {
        let t = table_schema("user").with_indexes([unique_index("email_idx", ["email"])]);
        let stmts = generate_table_sql(&t, true);
        assert!(stmts.iter().any(
            |s| s == "DEFINE INDEX IF NOT EXISTS email_idx ON TABLE user COLUMNS email UNIQUE;"
        ));
    }

    #[test]
    fn standard_index_includes_if_not_exists() {
        let t = table_schema("post")
            .with_indexes([index("title_idx", ["title"]).with_type(IndexType::Standard)]);
        let stmts = generate_table_sql(&t, true);
        assert!(stmts
            .iter()
            .any(|s| s == "DEFINE INDEX IF NOT EXISTS title_idx ON TABLE post COLUMNS title;"));
    }

    #[test]
    fn event_definition_includes_if_not_exists() {
        let t = table_schema("user").with_events([event(
            "email_changed",
            "$before.email != $after.email",
            "CREATE audit_log",
        )]);
        let stmts = generate_table_sql(&t, true);
        assert!(stmts
            .iter()
            .any(|s| s.starts_with("DEFINE EVENT IF NOT EXISTS email_changed ON TABLE user")));
    }

    #[test]
    fn edge_relation_includes_if_not_exists() {
        let e = typed_edge("likes", "user", "post");
        let stmts = generate_edge_sql(&e, true).unwrap();
        assert_eq!(
            stmts[0],
            "DEFINE TABLE IF NOT EXISTS likes TYPE RELATION FROM user TO post;"
        );
    }

    #[test]
    fn edge_schemafull_includes_if_not_exists() {
        let e = edge_schema("entity_relation").with_mode(EdgeMode::Schemafull);
        let stmts = generate_edge_sql(&e, true).unwrap();
        assert_eq!(
            stmts[0],
            "DEFINE TABLE IF NOT EXISTS entity_relation SCHEMAFULL;"
        );
    }

    #[test]
    fn edge_schemaless_includes_if_not_exists() {
        let e = edge_schema("loose_rel").with_mode(EdgeMode::Schemaless);
        let stmts = generate_edge_sql(&e, true).unwrap();
        assert_eq!(stmts[0], "DEFINE TABLE IF NOT EXISTS loose_rel SCHEMALESS;");
    }

    #[test]
    fn edge_fields_include_if_not_exists() {
        let e = typed_edge("likes", "user", "post").with_fields([datetime_field("created_at")
            .default("time::now()")
            .build_unchecked()
            .unwrap()]);
        let stmts = generate_edge_sql(&e, true).unwrap();
        assert!(stmts
            .iter()
            .any(|s| s.contains("DEFINE FIELD IF NOT EXISTS created_at ON TABLE likes")));
    }

    #[test]
    fn generate_schema_sql_forwards_if_not_exists() {
        let mut tables = BTreeMap::new();
        tables.insert(
            "user".to_string(),
            table_schema("user").with_mode(TableMode::Schemafull),
        );
        let mut edges = BTreeMap::new();
        edges.insert("likes".to_string(), typed_edge("likes", "user", "post"));
        let sql = generate_schema_sql(Some(&tables), Some(&edges), true).unwrap();
        assert!(sql.contains("DEFINE TABLE IF NOT EXISTS user SCHEMAFULL"));
        assert!(sql.contains("DEFINE TABLE IF NOT EXISTS likes TYPE RELATION FROM user TO post"));
    }

    #[test]
    fn default_false_omits_if_not_exists() {
        let t = table_schema("user")
            .with_mode(TableMode::Schemafull)
            .with_fields([string_field("name").build_unchecked().unwrap()])
            .with_indexes([unique_index("email_idx", ["email"])])
            .with_events([event("ec", "$before != $after", "CREATE x")]);
        let stmts = generate_table_sql(&t, false);
        assert_eq!(stmts[0], "DEFINE TABLE user SCHEMAFULL;");
        assert!(!stmts.iter().any(|s| s.contains("IF NOT EXISTS")));
    }
}
