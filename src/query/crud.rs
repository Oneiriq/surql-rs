//! High-level record CRUD helpers on top of [`DatabaseClient`].
//!
//! Port of `surql/query/crud.py`. Provides JSON-in / JSON-out wrappers around
//! the typed SDK methods on [`DatabaseClient`], along with a small set of
//! convenience queries (`count_records`, `exists`, `first`, `last`,
//! `query_records`). Typed (serde-round-trip) variants live in
//! [`super::typed`].
//!
//! All functions are `#[cfg(feature = "client")]`.
//!
//! ## Examples
//!
//! ```no_run
//! # #[cfg(feature = "client")]
//! # async fn demo() -> surql::error::Result<()> {
//! use serde_json::json;
//! use surql::connection::{ConnectionConfig, DatabaseClient};
//! use surql::query::crud;
//! use surql::types::RecordID;
//!
//! let client = DatabaseClient::new(ConnectionConfig::default())?;
//! client.connect().await?;
//!
//! let id = RecordID::<()>::new("user", "alice")?;
//! let created = crud::create_record(&client, "user", json!({"name": "Alice"})).await?;
//! let _ = crud::get_record(&client, &id).await?;
//! # let _ = created;
//! # Ok(()) }
//! ```

use std::collections::BTreeMap;
use std::fmt::Write;

use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::connection::DatabaseClient;
use crate::error::Result;
use crate::query::builder::Query;
use crate::query::executor::flatten_rows;
use crate::query::expressions::Expression;
use crate::query::results::{record, RecordResult};
use crate::types::operators::{Operator, OperatorExpr};
use crate::types::record_id::RecordID;

/// Create a record in `table` with the given JSON payload.
///
/// Uses a raw `CREATE <table> CONTENT $data` with a bound variable so the
/// payload is passed through as JSON without the SurrealDB SDK attempting
/// to coerce it into the CBOR-tagged format. Returns a [`RecordResult`]
/// wrapping the created record.
pub async fn create_record(
    client: &DatabaseClient,
    table: &str,
    data: Value,
) -> Result<RecordResult<Value>> {
    let mut vars = BTreeMap::new();
    vars.insert("data".to_owned(), data);
    let surql = format!("CREATE {table} CONTENT $data");
    let raw = client.query_with_vars(&surql, vars).await?;
    let first = flatten_rows(&raw).into_iter().next();
    let present = first.is_some();
    Ok(record(first, present))
}

/// Create multiple records in `table`. Each payload is created serially and
/// the resulting rows are collected.
pub async fn create_records(
    client: &DatabaseClient,
    table: &str,
    data: Vec<Value>,
) -> Result<Vec<Value>> {
    let mut out = Vec::with_capacity(data.len());
    for item in data {
        let mut vars = BTreeMap::new();
        vars.insert("data".to_owned(), item);
        let surql = format!("CREATE {table} CONTENT $data");
        let raw = client.query_with_vars(&surql, vars).await?;
        if let Some(row) = flatten_rows(&raw).into_iter().next() {
            out.push(row);
        }
    }
    Ok(out)
}

/// Fetch a single record by [`RecordID`].
///
/// Returns `Ok(None)` when the record does not exist.
pub async fn get_record<T>(
    client: &DatabaseClient,
    record_id: &RecordID<T>,
) -> Result<Option<Value>> {
    let target = record_id.to_string();
    let surql = format!("SELECT * FROM {target}");
    let raw = client.query(&surql).await?;
    Ok(flatten_rows(&raw).into_iter().next())
}

/// Update (replace) an existing record.
pub async fn update_record<T>(
    client: &DatabaseClient,
    record_id: &RecordID<T>,
    data: Value,
) -> Result<Value> {
    let target = record_id.to_string();
    update_record_target(client, &target, data).await
}

/// Update (replace) an existing record identified by a raw SurrealQL target
/// string (e.g. `"user:alice"` or the rendering of a
/// [`type_record`](crate::types::operators::type_record) expression).
///
/// Additive companion to [`update_record`] introduced for the query-UX
/// release: accepts any target that can be rendered to SurrealQL without
/// requiring a statically-typed [`RecordID`]. Pair with
/// [`crate::types::operators::type_record`] to update targets produced by
/// `type::record(...)` helpers.
pub async fn update_record_target(
    client: &DatabaseClient,
    target: &str,
    data: Value,
) -> Result<Value> {
    let mut vars = BTreeMap::new();
    vars.insert("data".to_owned(), data);
    let surql = format!("UPDATE {target} CONTENT $data");
    let raw = client.query_with_vars(&surql, vars).await?;
    Ok(flatten_rows(&raw).into_iter().next().unwrap_or(Value::Null))
}

/// Merge (patch) an existing record with the supplied partial.
pub async fn merge_record<T>(
    client: &DatabaseClient,
    record_id: &RecordID<T>,
    patch: Value,
) -> Result<Value> {
    let target = record_id.to_string();
    let mut vars = BTreeMap::new();
    vars.insert("patch".to_owned(), patch);
    let surql = format!("UPDATE {target} MERGE $patch");
    let raw = client.query_with_vars(&surql, vars).await?;
    Ok(flatten_rows(&raw).into_iter().next().unwrap_or(Value::Null))
}

/// Upsert (create-or-replace) a record via `UPSERT <id> CONTENT $data`.
pub async fn upsert_record<T>(
    client: &DatabaseClient,
    record_id: &RecordID<T>,
    data: Value,
) -> Result<Value> {
    let target = record_id.to_string();
    upsert_record_target(client, &target, data).await
}

/// Upsert using a raw SurrealQL target string (additive companion to
/// [`upsert_record`]).
pub async fn upsert_record_target(
    client: &DatabaseClient,
    target: &str,
    data: Value,
) -> Result<Value> {
    let mut vars = BTreeMap::new();
    vars.insert("data".to_owned(), data);
    let surql = format!("UPSERT {target} CONTENT $data");
    let raw = client.query_with_vars(&surql, vars).await?;
    Ok(flatten_rows(&raw).into_iter().next().unwrap_or(Value::Null))
}

/// Delete a single record by [`RecordID`].
pub async fn delete_record<T>(client: &DatabaseClient, record_id: &RecordID<T>) -> Result<()> {
    let target = record_id.to_string();
    let surql = format!("DELETE {target}");
    client.query(&surql).await?;
    Ok(())
}

/// Delete every record in `table` optionally filtered by an [`Operator`].
///
/// Returns the number of rows reported as deleted by the server.
pub async fn delete_records(
    client: &DatabaseClient,
    table: &str,
    where_: Option<&Operator>,
) -> Result<u64> {
    let surql = if let Some(op) = where_ {
        format!("DELETE {table} WHERE ({}) RETURN BEFORE", op.to_surql())
    } else {
        format!("DELETE {table} RETURN BEFORE")
    };
    let raw = client.query(&surql).await?;
    Ok(flatten_rows(&raw).len() as u64)
}

/// Execute a rendered [`Query`] and deserialize each row into `T`.
///
/// Thin re-export of [`executor::fetch_all`](crate::query::executor::fetch_all)
/// kept here so the CRUD module is self-contained.
pub async fn query_records<T: DeserializeOwned>(
    client: &DatabaseClient,
    query: &Query,
) -> Result<Vec<T>> {
    super::executor::fetch_all(client, query).await
}

/// Count the number of rows in `table`, optionally filtered by a `WHERE`.
///
/// Renders `SELECT count() FROM <table> [WHERE ...] GROUP ALL` and pulls the
/// scalar `count` out of the response.
pub async fn count_records(
    client: &DatabaseClient,
    table: &str,
    where_: Option<&Operator>,
) -> Result<i64> {
    let mut surql = format!("SELECT count() FROM {table}");
    if let Some(op) = where_ {
        write!(surql, " WHERE ({})", op.to_surql()).expect("write to String cannot fail");
    }
    surql.push_str(" GROUP ALL");

    let raw = client.query(&surql).await?;
    let row = flatten_rows(&raw).into_iter().next();
    Ok(row
        .as_ref()
        .and_then(|r| r.get("count").and_then(Value::as_i64))
        .unwrap_or(0))
}

/// Report whether the record identified by `record_id` exists.
pub async fn exists<T>(client: &DatabaseClient, record_id: &RecordID<T>) -> Result<bool> {
    Ok(get_record(client, record_id).await?.is_some())
}

/// Return the first row matching `query`, deserialized as `T`.
///
/// Composes with the builder's own `LIMIT` if set; if the query has no
/// explicit limit, this helper appends `LIMIT 1` for efficiency.
pub async fn first<T: DeserializeOwned>(
    client: &DatabaseClient,
    query: &Query,
) -> Result<Option<T>> {
    let q_with_limit = if query.limit_value.is_some() {
        query.clone()
    } else {
        query.clone().limit(1)?
    };
    super::executor::fetch_one(client, &q_with_limit).await
}

/// Return the *last* row matching `query` (mirrors Python's `last`).
///
/// Reverses any explicit `ORDER BY` direction on the query, caps the result
/// at `LIMIT 1`, and returns the first (now last) row.
pub async fn last<T: DeserializeOwned>(
    client: &DatabaseClient,
    query: &Query,
) -> Result<Option<T>> {
    let mut cloned = query.clone();
    for entry in &mut cloned.order_fields {
        entry.direction = if entry.direction.eq_ignore_ascii_case("ASC") {
            "DESC".to_owned()
        } else {
            "ASC".to_owned()
        };
    }
    if cloned.limit_value.is_none() {
        cloned = cloned.limit(1)?;
    }
    super::executor::fetch_one(client, &cloned).await
}

// ---------------------------------------------------------------------------
// Aggregation (sub-feature 4)
// ---------------------------------------------------------------------------

/// Options controlling the SurrealQL rendered by [`aggregate_records`].
///
/// Mirrors the surql-py `AggregateOpts` shape:
///
/// ```text
/// AggregateOpts {
///     select: [(alias, Expression), ...],
///     group_by: [field, ...],
///     where_: Option<Operator>,
///     group_all: false,
///     order_by: [(field, ASC|DESC)],
///     limit: None,
/// }
/// ```
///
/// `select` is a list of `(alias, expression)` pairs - each aggregate
/// projected into the output is always aliased so row shape is stable and
/// downstream `serde` / `extract_scalar` calls can rely on named columns.
#[derive(Debug, Clone, Default)]
pub struct AggregateOpts {
    /// `(alias, expression)` pairs rendered as `<expr> AS <alias>`.
    pub select: Vec<(String, Expression)>,
    /// `GROUP BY` field list.
    pub group_by: Vec<String>,
    /// `WHERE` condition (rendered via [`OperatorExpr::to_surql`]).
    pub where_: Option<Operator>,
    /// Emit `GROUP ALL` instead of `GROUP BY <fields>`.
    pub group_all: bool,
    /// `ORDER BY` entries as `(field, direction)` pairs. Direction is
    /// validated on [`aggregate_records`] call to be `ASC` / `DESC`.
    pub order_by: Vec<(String, String)>,
    /// Optional `LIMIT` value.
    pub limit: Option<i64>,
}

/// Build (but do not execute) the SurrealQL query described by `opts`.
///
/// Exposed as a standalone step so callers (and unit tests) can inspect
/// the rendered SurrealQL without requiring a live connection. The
/// returned [`Query`] is ready to be dispatched via [`Query::execute`] or
/// [`super::executor::fetch_all`].
///
/// Errors when `opts.select` is empty or when any builder-step validation
/// fails (invalid table name, invalid order direction, negative limit).
pub fn build_aggregate_query(table: &str, opts: &AggregateOpts) -> Result<Query> {
    if opts.select.is_empty() {
        return Err(crate::error::SurqlError::Query {
            reason: "aggregate_records requires at least one select entry".into(),
        });
    }

    let fields: Vec<String> = opts
        .select
        .iter()
        .map(|(alias, expr)| format!("{} AS {alias}", expr.to_surql()))
        .collect();

    let mut query = Query::new().select(Some(fields)).from_table(table)?;

    if let Some(op) = opts.where_.as_ref() {
        query = query.where_str(op.to_surql());
    }
    if opts.group_all {
        query = query.group_all();
    } else if !opts.group_by.is_empty() {
        query = query.group_by(opts.group_by.iter().cloned());
    }
    for (field, direction) in &opts.order_by {
        query = query.order_by(field.clone(), direction.clone())?;
    }
    if let Some(n) = opts.limit {
        query = query.limit(n)?;
    }

    Ok(query)
}

/// Execute a SurrealQL aggregation query against `table`.
///
/// Builds:
///
/// ```text
/// SELECT <expr1> AS <alias1>, <expr2> AS <alias2>, ...
///   FROM <table>
///   [WHERE <condition>]
///   [GROUP BY <fields> | GROUP ALL]
///   [ORDER BY ...]
///   [LIMIT n]
/// ```
///
/// Each returned [`Value`] row is a JSON object keyed by the aliases in
/// [`AggregateOpts::select`]. Pair with [`crate::query::results::extract_scalar`]
/// to pull a single field out of the single-row `GROUP ALL` case.
pub async fn aggregate_records(
    client: &DatabaseClient,
    table: &str,
    opts: AggregateOpts,
) -> Result<Vec<Value>> {
    let query = build_aggregate_query(table, &opts)?;
    let raw = super::executor::execute_query(client, &query).await?;
    Ok(flatten_rows(&raw))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::operators::eq;
    use serde_json::json;

    #[test]
    fn delete_records_renders_where_clause() {
        // Smoke-test the SurrealQL we render for delete_records (no DB needed).
        let op = eq("status", "inactive");
        let rendered = format!("DELETE user WHERE ({}) RETURN BEFORE", op.to_surql());
        assert_eq!(
            rendered,
            "DELETE user WHERE (status = 'inactive') RETURN BEFORE"
        );
    }

    #[test]
    fn json_payload_serializes_stably() {
        let v = json!({"name": "Alice", "age": 30});
        let rendered = serde_json::to_string(&v).unwrap();
        assert!(rendered.contains("\"name\":\"Alice\""));
        assert!(rendered.contains("\"age\":30"));
    }

    // -----------------------------------------------------------------------
    // Sub-feature 4: aggregation rendering
    // -----------------------------------------------------------------------

    #[test]
    fn build_aggregate_query_rejects_empty_select() {
        let err = build_aggregate_query("memory_entry", &AggregateOpts::default());
        assert!(matches!(err, Err(crate::error::SurqlError::Query { .. })));
    }

    #[test]
    fn build_aggregate_query_renders_select_group_by() {
        use crate::query::expressions::{count_all, math_sum};

        let opts = AggregateOpts {
            select: vec![
                ("count".to_string(), count_all()),
                ("total".to_string(), math_sum("strength")),
            ],
            group_by: vec!["network".into()],
            ..Default::default()
        };

        let q = build_aggregate_query("memory_entry", &opts).unwrap();
        assert_eq!(
            q.to_surql().unwrap(),
            "SELECT count() AS count, math::sum(strength) AS total FROM memory_entry \
             GROUP BY network",
        );
    }

    #[test]
    fn build_aggregate_query_renders_group_all() {
        use crate::query::expressions::{count_all, math_mean};

        let opts = AggregateOpts {
            select: vec![
                ("total".to_string(), count_all()),
                ("mean".to_string(), math_mean("strength")),
            ],
            group_all: true,
            ..Default::default()
        };
        let q = build_aggregate_query("memory_entry", &opts).unwrap();
        assert_eq!(
            q.to_surql().unwrap(),
            "SELECT count() AS total, math::mean(strength) AS mean FROM memory_entry GROUP ALL",
        );
    }

    #[test]
    fn build_aggregate_query_renders_where_order_limit() {
        use crate::query::expressions::count_all;

        let opts = AggregateOpts {
            select: vec![("count".to_string(), count_all())],
            where_: Some(eq("status", "active")),
            order_by: vec![("count".to_string(), "DESC".into())],
            limit: Some(5),
            ..Default::default()
        };
        let q = build_aggregate_query("user", &opts).unwrap();
        assert_eq!(
            q.to_surql().unwrap(),
            "SELECT count() AS count FROM user WHERE (status = 'active') \
             ORDER BY count DESC LIMIT 5",
        );
    }
}
