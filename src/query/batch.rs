//! Batch operation helpers for efficient multi-record operations.
//!
//! Port of `surql/query/batch.py`. Provides async functions for batch
//! `UPSERT` / `INSERT` / `DELETE` and bulk `RELATE`, plus the pure
//! `build_upsert_query` / `build_relate_query` helpers that render
//! SurrealQL without executing it.
//!
//! All async functions are `#[cfg(feature = "client")]` (same as
//! [`super::crud`]). The `build_*_query` helpers are available in every
//! build because they only render strings.
//!
//! ## Examples
//!
//! ```no_run
//! # #[cfg(feature = "client")]
//! # async fn demo() -> surql::error::Result<()> {
//! use serde_json::json;
//! use surql::connection::{ConnectionConfig, DatabaseClient};
//! use surql::query::batch;
//!
//! let client = DatabaseClient::new(ConnectionConfig::default())?;
//! client.connect().await?;
//!
//! let _ = batch::upsert_many(
//!     &client,
//!     "person",
//!     vec![
//!         json!({"id": "person:alice", "name": "Alice"}),
//!         json!({"id": "person:bob", "name": "Bob"}),
//!     ],
//!     None,
//! )
//! .await?;
//! # Ok(()) }
//! ```

use serde_json::Value;

use crate::error::{Result, SurqlError};
use crate::types::operators::quote_value_public;

use super::builder::{table_part, validate_identifier};

#[cfg(feature = "client")]
use crate::connection::DatabaseClient;
#[cfg(feature = "client")]
use crate::query::executor::flatten_rows;

// ---------------------------------------------------------------------------
// SurrealQL rendering helpers
// ---------------------------------------------------------------------------

/// Render one dict-style `Value` as a SurrealQL object literal, validating
/// every field name against the identifier pattern. Used by `*_many` helpers
/// and `build_upsert_query`.
fn format_item_for_surql(item: &Value) -> Result<String> {
    let obj = item.as_object().ok_or_else(|| SurqlError::Validation {
        reason: "Batch items must be JSON objects".to_string(),
    })?;

    let mut parts: Vec<String> = Vec::with_capacity(obj.len());
    for (key, value) in obj {
        validate_identifier(key, "field name")?;
        parts.push(format!("{key}: {}", quote_value_public(value)));
    }
    Ok(format!("{{ {} }}", parts.join(", ")))
}

/// Render a list of dicts as a SurrealQL array literal (one item per line).
fn format_items_array(items: &[Value]) -> Result<String> {
    let mut lines: Vec<String> = Vec::with_capacity(items.len());
    for item in items {
        lines.push(format_item_for_surql(item)?);
    }
    Ok(format!("[\n  {}\n]", lines.join(",\n  ")))
}

/// Render a `SET a = v1, b = v2` fragment for `RELATE` edge data.
fn render_set_clause(data: &serde_json::Map<String, Value>) -> Result<String> {
    let mut parts: Vec<String> = Vec::with_capacity(data.len());
    for (key, value) in data {
        validate_identifier(key, "field name")?;
        parts.push(format!("{key} = {}", quote_value_public(value)));
    }
    Ok(parts.join(", "))
}

// ---------------------------------------------------------------------------
// Pure query builders (available without the `client` feature)
// ---------------------------------------------------------------------------

/// Build a `UPSERT INTO <table> [...]` SurrealQL string without executing it.
///
/// Returns an empty string when `items` is empty (matches the Python helper).
///
/// When `conflict_fields` is `Some`, appends a `WHERE` clause of the form
/// `field = $item.field [AND ...]` to match against existing rows.
///
/// Mirrors the Python renderer verbatim for parity — note that the emitted
/// `UPSERT INTO <table> [...]` statement is **not** valid SurrealDB v3
/// SurrealQL (v3 requires a single target after `UPSERT`, not an array
/// literal). [`upsert_many`] iterates per record to work around this; this
/// helper is preserved for log / preview output that matches the Python
/// implementation byte-for-byte.
pub fn build_upsert_query(
    table: &str,
    items: &[Value],
    conflict_fields: Option<&[String]>,
) -> Result<String> {
    if items.is_empty() {
        return Ok(String::new());
    }

    validate_identifier(table, "table name")?;
    if let Some(fields) = conflict_fields {
        for f in fields {
            validate_identifier(f, "conflict field name")?;
        }
    }

    let items_array = format_items_array(items)?;
    if let Some(fields) = conflict_fields {
        if !fields.is_empty() {
            let conditions = fields
                .iter()
                .map(|f| format!("{f} = $item.{f}"))
                .collect::<Vec<_>>()
                .join(" AND ");
            return Ok(format!(
                "UPSERT INTO {table} {items_array} WHERE {conditions};"
            ));
        }
    }
    Ok(format!("UPSERT INTO {table} {items_array};"))
}

/// Build a `RELATE <from>-><edge>-><to> [SET ...]` SurrealQL string.
///
/// The `from_id` / `to_id` values should be complete record IDs
/// (`"user:alice"`). The table portion of each is validated against the
/// identifier regex to guard against injection.
pub fn build_relate_query(
    from_id: &str,
    edge: &str,
    to_id: &str,
    data: Option<&serde_json::Map<String, Value>>,
) -> Result<String> {
    validate_identifier(edge, "edge table name")?;
    validate_identifier(table_part(from_id), "from record table")?;
    validate_identifier(table_part(to_id), "to record table")?;

    let mut stmt = format!("RELATE {from_id}->{edge}->{to_id}");
    if let Some(data) = data {
        if !data.is_empty() {
            let set_clause = render_set_clause(data)?;
            stmt.push_str(" SET ");
            stmt.push_str(&set_clause);
        }
    }
    stmt.push(';');
    Ok(stmt)
}

// ---------------------------------------------------------------------------
// Async helpers (require the `client` feature)
// ---------------------------------------------------------------------------

/// Batch upsert multiple records. Per item, emits
/// `UPSERT <target> CONTENT $data` (with the payload bound as a
/// variable), falling back to `UPSERT <table> CONTENT $data` when an
/// item lacks an `id` field. Deviates from the Python source's single
/// `UPSERT INTO <table> [...]` statement because SurrealDB v3 rejects
/// array literals after `UPSERT INTO`; the pure renderer
/// [`build_upsert_query`] still emits the py form so callers relying on
/// it for logging / previews get 1:1 output.
///
/// The `conflict_fields` argument is accepted for signature parity with
/// the Python helper. It is currently only validated against the
/// identifier regex; resolution against the target still happens through
/// the explicit `id` on each item.
///
/// Returns the upserted rows. An empty `items` slice short-circuits to
/// `Ok(vec![])` without contacting the database.
#[cfg(feature = "client")]
pub async fn upsert_many(
    client: &DatabaseClient,
    table: &str,
    items: Vec<Value>,
    conflict_fields: Option<&[String]>,
) -> Result<Vec<Value>> {
    if items.is_empty() {
        return Ok(Vec::new());
    }
    validate_identifier(table, "table name")?;
    if let Some(fields) = conflict_fields {
        for f in fields {
            validate_identifier(f, "conflict field name")?;
        }
    }

    let mut rows: Vec<Value> = Vec::with_capacity(items.len());
    for item in items {
        let target = item
            .as_object()
            .and_then(|o| o.get("id"))
            .and_then(Value::as_str)
            .map_or_else(|| table.to_string(), ToOwned::to_owned);

        // Validate the target table-part (guards against id values like
        // `drop_table:1` smuggling through).
        validate_identifier(table_part(&target), "record ID table")?;

        let mut vars = std::collections::BTreeMap::new();
        vars.insert("data".to_owned(), item);
        let surql = format!("UPSERT {target} CONTENT $data");
        let raw = client.query_with_vars(&surql, vars).await?;
        rows.extend(flatten_rows(&raw));
    }
    Ok(rows)
}

/// Batch insert multiple records via `INSERT INTO <table> [...]`.
///
/// Fails if any record already exists (SurrealDB `INSERT` semantics).
#[cfg(feature = "client")]
pub async fn insert_many(
    client: &DatabaseClient,
    table: &str,
    items: Vec<Value>,
) -> Result<Vec<Value>> {
    if items.is_empty() {
        return Ok(Vec::new());
    }
    validate_identifier(table, "table name")?;
    let items_array = format_items_array(&items)?;
    let surql = format!("INSERT INTO {table} {items_array};");
    let raw = client.query(&surql).await?;
    Ok(flatten_rows(&raw))
}

/// Describe a single relation for [`relate_many`].
///
/// `data` is a serde `Map` (rather than a typed struct) so callers can pass
/// arbitrary edge properties without defining a dedicated type.
#[derive(Debug, Clone, Default)]
pub struct RelateItem {
    /// Source record ID (e.g. `"person:alice"`).
    pub from: String,
    /// Target record ID (e.g. `"person:bob"`).
    pub to: String,
    /// Optional edge properties.
    pub data: Option<serde_json::Map<String, Value>>,
}

impl RelateItem {
    /// Build a [`RelateItem`] without edge data.
    pub fn new(from: impl Into<String>, to: impl Into<String>) -> Self {
        Self {
            from: from.into(),
            to: to.into(),
            data: None,
        }
    }

    /// Attach edge data to this relation.
    pub fn with_data(mut self, data: serde_json::Map<String, Value>) -> Self {
        self.data = Some(data);
        self
    }
}

/// Batch create graph relations via a series of `RELATE` statements.
///
/// `from_table` and `to_table` are used for validation only; the actual
/// query uses the full record IDs present in each [`RelateItem`].
///
/// All statements are sent in a single query and the aggregated rows are
/// returned.
#[cfg(feature = "client")]
pub async fn relate_many(
    client: &DatabaseClient,
    from_table: &str,
    edge: &str,
    to_table: &str,
    relations: Vec<RelateItem>,
) -> Result<Vec<Value>> {
    if relations.is_empty() {
        return Ok(Vec::new());
    }

    validate_identifier(from_table, "from table name")?;
    validate_identifier(edge, "edge table name")?;
    validate_identifier(to_table, "to table name")?;

    let mut stmts: Vec<String> = Vec::with_capacity(relations.len());
    for rel in &relations {
        stmts.push(build_relate_query(
            &rel.from,
            edge,
            &rel.to,
            rel.data.as_ref(),
        )?);
    }
    let surql = stmts.join("\n");
    let raw = client.query(&surql).await?;
    Ok(flatten_rows(&raw))
}

/// Delete multiple records by ID via individual `DELETE ... RETURN BEFORE`
/// statements.
///
/// IDs may be bare (`"alice"`) or fully qualified (`"user:alice"`); bare
/// IDs are prefixed with `<table>:` automatically.
#[cfg(feature = "client")]
pub async fn delete_many(
    client: &DatabaseClient,
    table: &str,
    ids: Vec<String>,
) -> Result<Vec<Value>> {
    if ids.is_empty() {
        return Ok(Vec::new());
    }
    validate_identifier(table, "table name")?;

    let mut rows: Vec<Value> = Vec::new();
    for record_id in ids {
        if record_id.contains(':') {
            validate_identifier(table_part(&record_id), "record ID table")?;
        }
        let target = if record_id.contains(':') {
            record_id.clone()
        } else {
            format!("{table}:{record_id}")
        };
        let surql = format!("DELETE {target} RETURN BEFORE;");
        let raw = client.query(&surql).await?;
        rows.extend(flatten_rows(&raw));
    }
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn build_upsert_query_renders_array_literal() {
        let items = vec![
            json!({"id": "user:1", "name": "Alice"}),
            json!({"id": "user:2", "name": "Bob"}),
        ];
        let sql = build_upsert_query("user", &items, None).unwrap();
        assert!(sql.starts_with("UPSERT INTO user ["));
        assert!(sql.contains("id: 'user:1'"));
        assert!(sql.contains("name: 'Alice'"));
        assert!(sql.ends_with("];"));
    }

    #[test]
    fn build_upsert_query_appends_where_clause_for_conflict_fields() {
        let items = vec![json!({"email": "a@x.com", "name": "Alice"})];
        let fields = vec!["email".to_string()];
        let sql = build_upsert_query("user", &items, Some(&fields)).unwrap();
        assert!(sql.contains("WHERE email = $item.email"));
    }

    #[test]
    fn build_upsert_query_returns_empty_string_for_empty_items() {
        let sql = build_upsert_query("user", &[], None).unwrap();
        assert!(sql.is_empty());
    }

    #[test]
    fn build_upsert_query_rejects_invalid_identifier() {
        let items = vec![json!({"bad field": 1})];
        let err = build_upsert_query("user", &items, None).unwrap_err();
        assert!(matches!(err, SurqlError::Validation { .. }));
    }

    #[test]
    fn build_relate_query_includes_set_clause() {
        let mut data = serde_json::Map::new();
        data.insert("since".into(), json!("2024-01-01"));
        let sql = build_relate_query("person:alice", "knows", "person:bob", Some(&data)).unwrap();
        assert_eq!(
            sql,
            "RELATE person:alice->knows->person:bob SET since = '2024-01-01';"
        );
    }

    #[test]
    fn build_relate_query_without_data() {
        let sql = build_relate_query("person:alice", "knows", "person:bob", None).unwrap();
        assert_eq!(sql, "RELATE person:alice->knows->person:bob;");
    }

    #[test]
    fn build_relate_query_rejects_invalid_edge() {
        let err = build_relate_query("person:alice", "bad edge", "person:bob", None).unwrap_err();
        assert!(matches!(err, SurqlError::Validation { .. }));
    }

    #[test]
    fn format_item_for_surql_handles_nested_array() {
        let item = json!({"tags": ["a", "b"]});
        let rendered = format_item_for_surql(&item).unwrap();
        assert_eq!(rendered, "{ tags: ['a', 'b'] }");
    }

    #[test]
    fn format_item_for_surql_rejects_non_object() {
        let item = json!([1, 2, 3]);
        let err = format_item_for_surql(&item).unwrap_err();
        assert!(matches!(err, SurqlError::Validation { .. }));
    }
}
