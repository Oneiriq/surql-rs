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

use serde_json::Value;

use crate::error::{Result, SurqlError};
use crate::types::operators::quote_value_public;

use super::builder::{table_part, validate_identifier};

#[cfg(any(feature = "client", feature = "client-rustls", feature = "client-wasm"))]
use crate::connection::transaction::Transaction;
#[cfg(any(feature = "client", feature = "client-rustls", feature = "client-wasm"))]
use crate::connection::DatabaseClient;
#[cfg(any(feature = "client", feature = "client-rustls", feature = "client-wasm"))]
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
///
/// Used by [`insert_many`] (which is gated behind the `client` features),
/// so the helper itself only needs to compile when one of those features
/// is enabled.
#[cfg(any(feature = "client", feature = "client-rustls", feature = "client-wasm"))]
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

/// Render one item as the SurrealQL UPSERT statement that
/// [`upsert_many`] / [`upsert_many_in_tx`] / [`build_upsert_query`]
/// all emit. The `id` field — when present — is stripped from the
/// payload and used as the UPSERT target so v3 does not double-write it.
///
/// Returns a tuple of `(target, payload_literal)`; the caller appends
/// the WHERE clause and `;` suffix.
fn render_upsert_statement(table: &str, item: &Value) -> Result<(String, String)> {
    // Walk the item once: pluck `id` for the target, validate every
    // other key, and accumulate the payload literal.
    let obj = item.as_object().ok_or_else(|| SurqlError::Validation {
        reason: "Batch items must be JSON objects".to_string(),
    })?;

    let id_target = obj.get("id").and_then(Value::as_str);
    let target = match id_target {
        Some(id) => {
            validate_identifier(table_part(id), "record ID table")?;
            id.to_string()
        }
        None => table.to_string(),
    };

    let mut parts: Vec<String> = Vec::with_capacity(obj.len());
    for (key, value) in obj {
        if key == "id" {
            continue;
        }
        validate_identifier(key, "field name")?;
        parts.push(format!("{key}: {}", quote_value_public(value)));
    }
    let payload = format!("{{ {} }}", parts.join(", "));
    Ok((target, payload))
}

/// Build a multi-statement `UPSERT <target> CONTENT { … }` SurrealQL
/// string without executing it.
///
/// One statement per item, joined by `;`. Items with an `id` field are
/// upserted by record id (the `id` is stripped from the CONTENT payload
/// so v3 does not reject the duplicate); items without one are upserted
/// into the bare table.
///
/// When `conflict_fields` is `Some`, appends a `WHERE` clause of the
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

    validate_identifier(table, "table name")?;
    if let Some(fields) = conflict_fields {
        for f in fields {
            validate_identifier(f, "conflict field name")?;
        }
    }

    let mut statements: Vec<String> = Vec::with_capacity(items.len());
    for item in items {
        let (target, payload) = render_upsert_statement(table, item)?;
        let stmt = if let Some(fields) = conflict_fields.filter(|f| !f.is_empty()) {
            let obj = item
                .as_object()
                .expect("validated by render_upsert_statement");
            let conditions = fields
                .iter()
                .map(|f| {
                    let v = obj.get(f).cloned().unwrap_or(Value::Null);
                    format!("{f} = {}", quote_value_public(&v))
                })
                .collect::<Vec<_>>()
                .join(" AND ");
            format!("UPSERT {target} CONTENT {payload} WHERE {conditions};")
        } else {
            format!("UPSERT {target} CONTENT {payload};")
        };
        statements.push(stmt);
    }

    Ok(statements.join("\n"))
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

/// Batch upsert multiple records in **autocommit** mode.
///
/// Emits one `UPSERT <target> CONTENT $data` statement per item, with
/// the payload bound as a `$data` variable so the query plan can be
/// cached. Items with an `id` field are upserted by record id (the
/// `id` is stripped from the payload because v3 rejects
/// `UPSERT person:alice CONTENT {id: 'person:alice', ...}` —
/// the target is already pinned); items without one are upserted into
/// the bare table.
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
/// `conflict_fields` is accepted for cross-port signature parity. It is
/// validated against the identifier regex; conflict resolution against
/// the target still happens through the explicit `id` on each item.
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
    validate_identifier(table, "table name")?;
    if let Some(fields) = conflict_fields {
        for f in fields {
            validate_identifier(f, "conflict field name")?;
        }
    }

    let mut rows: Vec<Value> = Vec::with_capacity(items.len());
    for item in items {
        let (target, payload, payload_for_bind) = prepare_tx_upsert(table, item)?;
        // Validate the target table-part (guards against id values like
        // `drop_table:1` smuggling through).
        validate_identifier(table_part(&target), "record ID table")?;

        // Autocommit path uses parameter binding so the v3 query planner
        // can reuse the prepared statement across the batch.
        let _ = payload; // payload literal is unused on this path.
        let mut vars = std::collections::BTreeMap::new();
        vars.insert("data".to_owned(), payload_for_bind);
        let surql = format!("UPSERT {target} CONTENT $data");
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
    validate_identifier(table, "table name")?;
    if let Some(fields) = conflict_fields {
        for f in fields {
            validate_identifier(f, "conflict field name")?;
        }
    }

    let mut results: Vec<Value> = Vec::with_capacity(items.len());
    for item in items {
        let (target, payload, payload_for_bind) = prepare_tx_upsert(table, &item)?;
        let _ = payload_for_bind; // bound path is unused inside a transaction.
                                  // Validate the target table-part (guards against id values like
                                  // `drop_table:1` smuggling through).
        validate_identifier(table_part(&target), "record ID table")?;

        let stmt = if let Some(fields) = conflict_fields.filter(|f| !f.is_empty()) {
            let obj = item.as_object().expect("validated by prepare_tx_upsert");
            let conditions = fields
                .iter()
                .map(|f| {
                    let v = obj.get(f).cloned().unwrap_or(Value::Null);
                    format!("{f} = {}", quote_value_public(&v))
                })
                .collect::<Vec<_>>()
                .join(" AND ");
            format!("UPSERT {target} CONTENT {payload} WHERE {conditions}")
        } else {
            format!("UPSERT {target} CONTENT {payload}")
        };
        results.push(txn.execute(&stmt).await?);
    }
    Ok(results)
}

/// Owned-by-call-site helper used by both [`upsert_many`] (which binds
/// the payload as `$data`) and [`upsert_many_in_tx`] (which inlines the
/// rendered literal). Returns a tuple of
/// `(target, inline_payload_literal, payload_for_$data_binding)`.
///
/// The function accepts both an owned and a borrowed `item` because the
/// autocommit path consumes the value (it becomes the bind variable) and
/// the transaction path only borrows it (the literal is rendered from
/// the borrowed value). To keep one call site, the autocommit caller
/// passes the owned `item` directly; the transaction caller passes a
/// borrow.
#[cfg(any(feature = "client", feature = "client-rustls", feature = "client-wasm"))]
fn prepare_tx_upsert<I>(table: &str, item: I) -> Result<(String, String, Value)>
where
    I: PreparedUpsertItem,
{
    item.prepare(table)
}

#[cfg(any(feature = "client", feature = "client-rustls", feature = "client-wasm"))]
trait PreparedUpsertItem {
    fn prepare(self, table: &str) -> Result<(String, String, Value)>;
}

#[cfg(any(feature = "client", feature = "client-rustls", feature = "client-wasm"))]
impl PreparedUpsertItem for Value {
    fn prepare(self, table: &str) -> Result<(String, String, Value)> {
        let (target, payload) = render_upsert_statement(table, &self)?;
        // For autocommit binding: clone the item and strip the `id`
        // field so v3 does not reject the duplicate.
        let mut bind = self;
        if let Some(obj) = bind.as_object_mut() {
            obj.remove("id");
        }
        Ok((target, payload, bind))
    }
}

#[cfg(any(feature = "client", feature = "client-rustls", feature = "client-wasm"))]
impl PreparedUpsertItem for &Value {
    fn prepare(self, table: &str) -> Result<(String, String, Value)> {
        let (target, payload) = render_upsert_statement(table, self)?;
        // Transaction path doesn't bind, so the `Value::Null` here is
        // just a placeholder — callers should ignore it.
        Ok((target, payload, Value::Null))
    }
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
    fn build_upsert_query_targets_bare_table_when_no_id_field() {
        let items = vec![json!({"name": "Alice"})];
        let sql = build_upsert_query("user", &items, None).unwrap();
        assert!(sql.starts_with("UPSERT user CONTENT"));
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
