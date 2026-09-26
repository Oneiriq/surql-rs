//! Batch operation helpers for efficient multi-record operations.
//!
//! Port of `surql/query/batch.py`. Provides async functions for batch
//! `UPSERT` / `INSERT` / `DELETE` and bulk `RELATE`, plus the pure
//! `build_upsert_query` / `build_relate_query` helpers that render
//! SurrealQL without executing it.
//!
//! All async functions are `#[cfg(any(feature = "client", feature = "client-rustls", feature = "client-wasm"))]` (same as
//! [`super::crud`]). The `build_*_query` helpers are available in every
//! build because they only render strings.
//!
//! ## Examples
//!
//! ```no_run
//! # #[cfg(any(feature = "client", feature = "client-rustls", feature = "client-wasm"))]
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

use serde_json::{Map, Value};

use crate::error::{Result, SurqlError};
use crate::types::operators::{quote_object_key, quote_value_public};
use crate::types::record_id::RecordID;

use super::validate::{record_in_table, render_target, validate_identifier, validate_set_target};

#[cfg(any(feature = "client", feature = "client-rustls", feature = "client-wasm"))]
use crate::connection::transaction::Transaction;
#[cfg(any(feature = "client", feature = "client-rustls", feature = "client-wasm"))]
use crate::connection::DatabaseClient;
#[cfg(any(feature = "client", feature = "client-rustls", feature = "client-wasm"))]
use crate::query::executor::flatten_rows;

// ---------------------------------------------------------------------------
// SurrealQL rendering helpers
// ---------------------------------------------------------------------------

/// The item as a JSON object, or a validation error.
fn as_object(item: &Value) -> Result<&Map<String, Value>> {
    item.as_object().ok_or_else(|| SurqlError::Validation {
        reason: "Batch items must be JSON objects".to_string(),
    })
}

/// Render a JSON object as a SurrealQL object literal, validating every
/// field name against the identifier pattern. Keys are quoted as object keys
/// and values rendered as literals, at any depth.
fn render_object(obj: &Map<String, Value>) -> Result<String> {
    let mut parts: Vec<String> = Vec::with_capacity(obj.len());
    for (key, value) in obj {
        validate_identifier(key, "field name")?;
        parts.push(format!(
            "{}: {}",
            quote_object_key(key),
            quote_value_public(value)
        ));
    }
    Ok(format!("{{ {} }}", parts.join(", ")))
}

/// Render a list of dicts as a SurrealQL array literal (one item per line).
///
/// Used by [`insert_many`] (which is gated behind the `client` features),
/// so the helper itself only needs to compile when one of those features
/// is enabled.
#[cfg(any(feature = "client", feature = "client-rustls", feature = "client-wasm"))]
fn format_items_array(items: &[Value]) -> Result<String> {
    let lines = items
        .iter()
        .map(|item| as_object(item).and_then(render_object))
        .collect::<Result<Vec<_>>>()?;
    Ok(format!("[\n  {}\n]", lines.join(",\n  ")))
}

/// Render a `SET a = v1, b = v2` fragment for `RELATE` edge data. Each key
/// is a `SET` target (a field path).
fn render_set_clause(data: &Map<String, Value>) -> Result<String> {
    let mut parts: Vec<String> = Vec::with_capacity(data.len());
    for (key, value) in data {
        validate_set_target(key)?;
        parts.push(format!("{key} = {}", quote_value_public(value)));
    }
    Ok(parts.join(", "))
}

// ---------------------------------------------------------------------------
// Upsert planning
// ---------------------------------------------------------------------------

/// One item of an upsert batch, resolved: the record or table it targets,
/// the fields it writes, and the `WHERE` its conflict fields add.
///
/// Every upsert path ([`build_upsert_query`], [`upsert_many`],
/// [`upsert_many_in_tx`]) renders from this one plan, so they target the
/// same records.
struct UpsertPlan {
    target: String,
    payload: Map<String, Value>,
    condition: Option<String>,
}

impl UpsertPlan {
    /// `UPSERT <target> CONTENT <content> [WHERE <condition>]`.
    fn statement(&self, content: &str) -> String {
        match &self.condition {
            Some(condition) => {
                format!("UPSERT {} CONTENT {content} WHERE {condition}", self.target)
            }
            None => format!("UPSERT {} CONTENT {content}", self.target),
        }
    }
}

/// Validate the table and conflict field names shared by a batch.
fn validate_upsert_args<'a>(
    table: &str,
    conflict_fields: Option<&'a [String]>,
) -> Result<&'a [String]> {
    validate_identifier(table, "table name")?;
    let fields = conflict_fields.unwrap_or(&[]);
    for f in fields {
        validate_identifier(f, "conflict field name")?;
    }
    Ok(fields)
}

/// Resolve one upsert item.
///
/// The target comes from the item's `id`: a bare string is a key of `table`
/// (`"alice"` is `table:alice`), a qualified string must name a record of
/// `table`, and an integer is an integer key. Without an `id` the statement
/// targets the whole table, which is only an upsert when `conflict_fields`
/// narrows it with a `WHERE`; otherwise `UPSERT <table>` matches nothing and
/// inserts a fresh record on every run, so such an item is refused.
fn plan_upsert(table: &str, item: &Value, conflict_fields: &[String]) -> Result<UpsertPlan> {
    let obj = as_object(item)?;
    let target = match obj.get("id") {
        None if conflict_fields.is_empty() => {
            return Err(SurqlError::Validation {
                reason: "Upsert items need an `id` or conflict_fields: without either, \
                         UPSERT inserts a new record on every run"
                    .to_string(),
            })
        }
        None => table.to_owned(),
        Some(Value::String(id)) if id.contains(':') => record_in_table(table, id)?,
        Some(Value::String(id)) => RecordID::<()>::new(table, id.as_str())?.to_string(),
        Some(Value::Number(n)) => match n.as_i64() {
            Some(n) => RecordID::<()>::new(table, n)?.to_string(),
            None => {
                return Err(SurqlError::Validation {
                    reason: format!("Upsert item id must be an integer or a string, got {n}"),
                })
            }
        },
        Some(other) => {
            return Err(SurqlError::Validation {
                reason: format!("Upsert item id must be an integer or a string, got {other}"),
            })
        }
    };

    // v3 rejects a CONTENT that repeats the target's id, so `id` is the
    // target only.
    let payload: Map<String, Value> = obj
        .iter()
        .filter(|(key, _)| key.as_str() != "id")
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();
    for key in payload.keys() {
        validate_identifier(key, "field name")?;
    }

    let condition = (!conflict_fields.is_empty()).then(|| {
        conflict_fields
            .iter()
            .map(|f| {
                let v = obj.get(f).unwrap_or(&Value::Null);
                format!("{f} = {}", quote_value_public(v))
            })
            .collect::<Vec<_>>()
            .join(" AND ")
    });

    Ok(UpsertPlan {
        target,
        payload,
        condition,
    })
}

// ---------------------------------------------------------------------------
// Pure query builders (available without the `client` feature)
// ---------------------------------------------------------------------------

/// Build a multi-statement `UPSERT <target> CONTENT { … }` SurrealQL
/// string without executing it.
///
/// One statement per item, joined by `;`. Each item needs an `id` (the
/// upsert target; it is stripped from the CONTENT payload so v3 does not
/// reject the duplicate) or non-empty `conflict_fields`: a bare id is a key
/// of `table`, a qualified id must name a record of `table`, and an integer
/// id is an integer key.
///
/// When `conflict_fields` is non-empty, appends a `WHERE` clause of the
/// form `field = <value> [AND …]` to each statement. The conflict
/// values are inlined rather than parameterised because callers that
/// pass the rendered string to [`Transaction::execute`] cannot bind
/// `$item.field` — the buffered-transaction implementation does not
/// thread params through the queue.
///
/// ## v3 correctness
///
/// Pre-0.2.5 this helper emitted `UPSERT INTO <table> [ … ]`, which
/// SurrealDB v3 rejects with a parse error — v3 wants a single record-id
/// or table target after `UPSERT`, not an array literal. The 0.2.5
/// rewrite aligns the renderer with the surql-py 1.7.0 / surql 1.5.0
/// per-record `UPSERT <target> CONTENT { … }` shape, which is the only
/// portable form across the sibling ports.
pub fn build_upsert_query(
    table: &str,
    items: &[Value],
    conflict_fields: Option<&[String]>,
) -> Result<String> {
    if items.is_empty() {
        return Ok(String::new());
    }
    let fields = validate_upsert_args(table, conflict_fields)?;
    let statements = items
        .iter()
        .map(|item| {
            let plan = plan_upsert(table, item, fields)?;
            Ok(format!(
                "{};",
                plan.statement(&render_object(&plan.payload)?)
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(statements.join("\n"))
}

/// Build a `RELATE <from>-><edge>-><to> [SET ...]` SurrealQL string.
///
/// The `from_id` / `to_id` values should be complete record IDs
/// (`"user:alice"`). Each is parsed and re-rendered, so its key is escaped
/// and cannot extend the statement; the edge must be an identifier.
pub fn build_relate_query(
    from_id: &str,
    edge: &str,
    to_id: &str,
    data: Option<&Map<String, Value>>,
) -> Result<String> {
    validate_identifier(edge, "edge table name")?;
    let from = render_target(from_id)?;
    let to = render_target(to_id)?;

    let mut stmt = format!("RELATE {from}->{edge}->{to}");
    if let Some(data) = data.filter(|d| !d.is_empty()) {
        stmt.push_str(" SET ");
        stmt.push_str(&render_set_clause(data)?);
    }
    stmt.push(';');
    Ok(stmt)
}

// ---------------------------------------------------------------------------
// Async helpers (require the `client` feature)
// ---------------------------------------------------------------------------

/// Batch upsert multiple records in **autocommit** mode.
///
/// Emits one `UPSERT <target> CONTENT $data [WHERE …]` statement per item,
/// with the payload bound as a `$data` variable so the query plan can be
/// cached. Items resolve exactly as in [`build_upsert_query`]: each needs an
/// `id` (stripped from the payload, because v3 rejects
/// `UPSERT person:alice CONTENT {id: 'person:alice', ...}` — the target is
/// already pinned) or non-empty `conflict_fields`, which add the same
/// `WHERE` clause as the other upsert paths.
///
/// ## Atomicity
///
/// SurrealDB v3 autocommits each statement in a multi-statement query
/// unless wrapped in `BEGIN … COMMIT`, so a single bad record mid-batch
/// leaves the earlier records already persisted. When that partial-
/// success window is unacceptable, use [`upsert_many_in_tx`] instead —
/// it queues the same per-record `UPSERT` statements on an active
/// [`Transaction`] so the whole batch rolls back if any record fails.
///
/// Returns the upserted rows. An empty `items` slice short-circuits to
/// `Ok(vec![])` without contacting the database.
#[cfg(any(feature = "client", feature = "client-rustls", feature = "client-wasm"))]
pub async fn upsert_many(
    client: &DatabaseClient,
    table: &str,
    items: Vec<Value>,
    conflict_fields: Option<&[String]>,
) -> Result<Vec<Value>> {
    if items.is_empty() {
        return Ok(Vec::new());
    }
    let fields = validate_upsert_args(table, conflict_fields)?;

    let mut rows: Vec<Value> = Vec::with_capacity(items.len());
    for item in &items {
        let plan = plan_upsert(table, item, fields)?;
        let surql = plan.statement("$data");
        let mut vars = std::collections::BTreeMap::new();
        vars.insert("data".to_owned(), Value::Object(plan.payload));
        let raw = client.query_with_vars(&surql, vars).await?;
        rows.extend(flatten_rows(&raw));
    }
    Ok(rows)
}

/// Batch upsert multiple records as part of an **atomic** transaction.
///
/// Queues one `UPSERT <target> CONTENT { … }` statement per item on
/// `txn`'s buffer. The statements inherit the surrounding
/// `BEGIN TRANSACTION` / `COMMIT TRANSACTION` framing, so a single bad
/// record rolls back the *entire* batch when [`Transaction::commit`] is
/// called — no half-seeded tables. Use this whenever partial-success
/// from [`upsert_many`]'s autocommit path would leave the database in a
/// shape downstream code can't recover from.
///
/// ## Differences from autocommit
///
/// - **Values are inlined.** [`Transaction::execute`] queues raw SQL
///   strings without param bindings, so the payload is rendered through
///   [`quote_value_public`] into a SurrealQL object literal rather than
///   bound as `$data`. This matches surql 1.5.0's `upsert_many(trx, …)`
///   path; surql-py 1.7.0 routes a per-statement `bind` dict through
///   `Transaction.execute` but that path does not exist in the Rust
///   port today.
/// - **No results.** `Transaction.execute` returns
///   `Value::Null` regardless of statement, so this function returns
///   `Vec<Value>` with one `Null` entry per queued statement. The real
///   per-statement results land in the array returned by
///   [`Transaction::commit`].
///
/// ## Usage
///
/// ```no_run
/// # #[cfg(any(feature = "client", feature = "client-rustls", feature = "client-wasm"))]
/// # async fn demo() -> surql::error::Result<()> {
/// use serde_json::json;
/// use surql::connection::{ConnectionConfig, DatabaseClient, Transaction};
/// use surql::query::batch;
///
/// let client = DatabaseClient::new(ConnectionConfig::default())?;
/// client.connect().await?;
/// let mut txn = Transaction::begin(&client).await?;
/// batch::upsert_many_in_tx(
///     &mut txn,
///     "person",
///     vec![
///         json!({"id": "person:alice", "name": "Alice"}),
///         json!({"id": "person:bob", "name": "Bob"}),
///     ],
///     None,
/// )
/// .await?;
/// // Commits both upserts atomically. If either fails, both roll back.
/// txn.commit().await?;
/// # Ok(()) }
/// ```
#[cfg(any(feature = "client", feature = "client-rustls", feature = "client-wasm"))]
pub async fn upsert_many_in_tx(
    txn: &mut Transaction<'_>,
    table: &str,
    items: Vec<Value>,
    conflict_fields: Option<&[String]>,
) -> Result<Vec<Value>> {
    if items.is_empty() {
        return Ok(Vec::new());
    }
    let fields = validate_upsert_args(table, conflict_fields)?;

    let mut results: Vec<Value> = Vec::with_capacity(items.len());
    for item in &items {
        let plan = plan_upsert(table, item, fields)?;
        let stmt = plan.statement(&render_object(&plan.payload)?);
        results.push(txn.execute(&stmt).await?);
    }
    Ok(results)
}

/// Batch insert multiple records via `INSERT INTO <table> [...]`.
///
/// Fails if any record already exists (SurrealDB `INSERT` semantics).
#[cfg(any(feature = "client", feature = "client-rustls", feature = "client-wasm"))]
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
    pub data: Option<Map<String, Value>>,
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
    pub fn with_data(mut self, data: Map<String, Value>) -> Self {
        self.data = Some(data);
        self
    }
}

/// Batch create graph relations via a series of `RELATE` statements.
///
/// Each [`RelateItem`]'s `from` must be a record of `from_table` and its
/// `to` a record of `to_table`; a bare key (`"alice"`) is taken as a key of
/// that table.
///
/// All statements are sent in a single query and the aggregated rows are
/// returned.
#[cfg(any(feature = "client", feature = "client-rustls", feature = "client-wasm"))]
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

    validate_identifier(edge, "edge table name")?;

    let mut stmts: Vec<String> = Vec::with_capacity(relations.len());
    for rel in &relations {
        let from = record_in_table(from_table, &rel.from)?;
        let to = record_in_table(to_table, &rel.to)?;
        stmts.push(build_relate_query(&from, edge, &to, rel.data.as_ref())?);
    }
    let surql = stmts.join("\n");
    let raw = client.query(&surql).await?;
    Ok(flatten_rows(&raw))
}

/// Delete multiple records by ID via individual `DELETE ... RETURN BEFORE`
/// statements.
///
/// IDs may be bare (`"alice"`) or fully qualified (`"user:alice"`); bare
/// IDs are keys of `table` (digits name the integer key, as they do in
/// SurrealQL), and a qualified ID must name a record of `table`.
#[cfg(any(feature = "client", feature = "client-rustls", feature = "client-wasm"))]
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
    for record_id in &ids {
        let target = record_in_table(table, record_id)?;
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
    fn build_upsert_query_renders_per_record_content_form() {
        // v3-correct shape: one `UPSERT <target> CONTENT { … }` statement
        // per item. The pre-0.2.5 helper emitted `UPSERT INTO <table>
        // [ … ]` which v3 rejects with a parse error.
        let items = vec![
            json!({"id": "user:1", "name": "Alice"}),
            json!({"id": "user:2", "name": "Bob"}),
        ];
        let sql = build_upsert_query("user", &items, None).unwrap();
        // Two records → two `UPSERT … CONTENT` statements.
        assert_eq!(sql.matches("UPSERT user:").count(), 2);
        assert!(sql.contains("UPSERT user:1 CONTENT"));
        assert!(sql.contains("UPSERT user:2 CONTENT"));
        // The `id` field is the target, not part of the CONTENT payload.
        assert!(!sql.contains("id: 'user:1'"));
        assert!(sql.contains("name: 'Alice'"));
        assert!(sql.contains("name: 'Bob'"));
        assert!(sql.ends_with(';'));
    }

    #[test]
    fn build_upsert_query_targets_the_table_when_conflict_fields_match() {
        let items = vec![json!({"email": "a@x.com", "name": "Alice"})];
        let fields = vec!["email".to_string()];
        let sql = build_upsert_query("user", &items, Some(&fields)).unwrap();
        assert!(sql.starts_with("UPSERT user CONTENT"), "{sql}");
    }

    #[test]
    fn build_upsert_query_appends_inline_where_clause_for_conflict_fields() {
        // The conflict values are inlined, not `$item.<field>` — the
        // rendered string has no `$item` binding in scope, especially
        // when fed to `Transaction::execute` which queues raw SQL.
        let items = vec![json!({"email": "a@x.com", "name": "Alice"})];
        let fields = vec!["email".to_string()];
        let sql = build_upsert_query("user", &items, Some(&fields)).unwrap();
        assert!(sql.contains("WHERE email = 'a@x.com'"));
    }

    #[test]
    fn build_upsert_query_combines_multiple_conflict_fields_with_and() {
        let items = vec![json!({"email": "a@x.com", "tenant": "BFS"})];
        let fields = vec!["email".to_string(), "tenant".to_string()];
        let sql = build_upsert_query("user", &items, Some(&fields)).unwrap();
        assert!(sql.contains("email = 'a@x.com' AND tenant = 'BFS'"));
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
    fn build_relate_query_cannot_carry_a_second_statement() {
        let sql = build_relate_query(
            "person:a->knows->person:b; REMOVE TABLE person; --",
            "knows",
            "person:c",
            None,
        )
        .unwrap();
        assert_eq!(
            sql,
            "RELATE person:⟨a->knows->person:b; REMOVE TABLE person; --⟩->knows->person:c;"
        );
    }

    #[test]
    fn build_upsert_query_requires_an_id_or_conflict_fields() {
        let err = build_upsert_query("user", &[json!({"name": "Alice"})], None).unwrap_err();
        assert!(matches!(err, SurqlError::Validation { .. }));
        let fields: Vec<String> = Vec::new();
        assert!(build_upsert_query("user", &[json!({"name": "Alice"})], Some(&fields)).is_err());
    }

    #[test]
    fn build_upsert_query_prefixes_bare_and_numeric_ids() {
        let sql = build_upsert_query("user", &[json!({"id": "alice", "n": 1})], None).unwrap();
        assert_eq!(sql, "UPSERT user:alice CONTENT { n: 1 };");
        let sql = build_upsert_query("user", &[json!({"id": 42, "n": 1})], None).unwrap();
        assert_eq!(sql, "UPSERT user:42 CONTENT { n: 1 };");
        let sql = build_upsert_query("user", &[json!({"id": "a-b; DELETE user"})], None).unwrap();
        assert_eq!(sql, "UPSERT user:⟨a-b; DELETE user⟩ CONTENT {  };");
    }

    #[test]
    fn build_upsert_query_rejects_an_id_in_another_table() {
        assert!(build_upsert_query("user", &[json!({"id": "admin:root"})], None).is_err());
        assert!(build_upsert_query("user", &[json!({"id": true})], None).is_err());
    }

    #[test]
    fn build_upsert_query_applies_conflict_fields_to_id_targets() {
        let fields = vec!["email".to_string()];
        let sql = build_upsert_query(
            "user",
            &[json!({"id": "user:a", "email": "a@x.com"})],
            Some(&fields),
        )
        .unwrap();
        assert_eq!(
            sql,
            "UPSERT user:a CONTENT { email: 'a@x.com' } WHERE email = 'a@x.com';"
        );
    }

    #[test]
    fn relate_set_clause_takes_field_paths_only() {
        let mut data = serde_json::Map::new();
        data.insert("meta.since".into(), json!(1));
        let sql = build_relate_query("person:a", "knows", "person:b", Some(&data)).unwrap();
        assert_eq!(sql, "RELATE person:a->knows->person:b SET meta.since = 1;");
        data.insert("x = 1, y".into(), json!(1));
        assert!(build_relate_query("person:a", "knows", "person:b", Some(&data)).is_err());
    }

    #[test]
    fn render_object_handles_nested_array() {
        let item = json!({"tags": ["a", "b"]});
        let rendered = render_object(as_object(&item).unwrap()).unwrap();
        assert_eq!(rendered, "{ tags: ['a', 'b'] }");
    }

    #[test]
    fn as_object_rejects_non_object() {
        let err = as_object(&json!([1, 2, 3])).unwrap_err();
        assert!(matches!(err, SurqlError::Validation { .. }));
    }
}
