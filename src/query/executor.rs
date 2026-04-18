//! Async query execution on top of [`DatabaseClient`].
//!
//! Port of `surql/query/executor.py`. Every function is a thin wrapper that
//! renders a [`Query`] (or accepts a raw SurrealQL string) and dispatches to
//! [`DatabaseClient::query_with_vars`](crate::DatabaseClient::query_with_vars),
//! then extracts / deserializes the result.
//!
//! All functions are `#[cfg(feature = "client")]` because they depend on the
//! async SurrealDB SDK handle.
//!
//! ## Examples
//!
//! ```no_run
//! # #[cfg(feature = "client")]
//! # async fn demo() -> surql::error::Result<()> {
//! use serde::{Deserialize, Serialize};
//! use surql::connection::{ConnectionConfig, DatabaseClient};
//! use surql::query::{executor, Query};
//!
//! #[derive(Debug, Serialize, Deserialize)]
//! struct User { name: String, age: u32 }
//!
//! let client = DatabaseClient::new(ConnectionConfig::default())?;
//! client.connect().await?;
//! let q = Query::new().select(None).from_table("user")?;
//! let users: Vec<User> = executor::fetch_all(&client, &q).await?;
//! # let _ = users;
//! # Ok(()) }
//! ```

use std::collections::BTreeMap;

use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::connection::DatabaseClient;
use crate::error::{Result, SurqlError};
use crate::query::builder::Query;
use crate::query::results::{extract_result, records, ListResult};
// `extract_result` is used by `flatten_rows` as a legacy fallback.

/// Execute a rendered [`Query`] against the database.
///
/// Returns the raw `serde_json::Value` array produced by the driver - one
/// entry per SurrealQL statement. See
/// [`DatabaseClient::query`](crate::DatabaseClient::query) for the exact
/// wire format.
pub async fn execute_query(client: &DatabaseClient, query: &Query) -> Result<Value> {
    let surql = query.to_surql()?;
    client.query(&surql).await
}

/// Execute a raw SurrealQL string with optional bound variables.
///
/// Mirrors [`DatabaseClient::query_with_vars`](crate::DatabaseClient::query_with_vars).
pub async fn execute_raw(
    client: &DatabaseClient,
    surql: &str,
    vars: Option<BTreeMap<String, Value>>,
) -> Result<Value> {
    match vars {
        Some(v) => client.query_with_vars(surql, v).await,
        None => client.query(surql).await,
    }
}

/// Execute a rendered [`Query`] and deserialize the **first** row into `T`.
///
/// Returns `Ok(None)` when the query produces no rows.
pub async fn fetch_one<T: DeserializeOwned>(
    client: &DatabaseClient,
    query: &Query,
) -> Result<Option<T>> {
    let raw = execute_query(client, query).await?;
    let mut rows = flatten_rows(&raw);
    let Some(first) = rows.drain(..).next() else {
        return Ok(None);
    };
    match first {
        Value::Object(obj) => deserialize_row(obj).map(Some),
        other => {
            serde_json::from_value::<T>(other)
                .map(Some)
                .map_err(|e| SurqlError::Serialization {
                    reason: e.to_string(),
                })
        }
    }
}

/// Execute a rendered [`Query`] and deserialize every row into `T`.
pub async fn fetch_all<T: DeserializeOwned>(
    client: &DatabaseClient,
    query: &Query,
) -> Result<Vec<T>> {
    let raw = execute_query(client, query).await?;
    extract_rows::<T>(&raw)
}

/// Execute a rendered [`Query`] and return a [`ListResult`] that honours the
/// `LIMIT` / `START` metadata from the builder.
pub async fn fetch_many<T: DeserializeOwned>(
    client: &DatabaseClient,
    query: &Query,
) -> Result<ListResult<T>> {
    let items = fetch_all::<T>(client, query).await?;
    let limit = query.limit_value.and_then(|v| u64::try_from(v).ok());
    let offset = query.offset_value.and_then(|v| u64::try_from(v).ok());
    Ok(records(items, None, limit, offset))
}

/// Execute a raw SurrealQL string and deserialize every row into `T`.
pub async fn execute_raw_typed<T: DeserializeOwned>(
    client: &DatabaseClient,
    surql: &str,
) -> Result<Vec<T>> {
    let raw = client.query(surql).await?;
    extract_rows::<T>(&raw)
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

pub(crate) fn extract_rows<T: DeserializeOwned>(raw: &Value) -> Result<Vec<T>> {
    let rows = flatten_rows(raw);
    rows.into_iter()
        .map(|row| {
            serde_json::from_value::<T>(row).map_err(|e| SurqlError::Serialization {
                reason: e.to_string(),
            })
        })
        .collect()
}

fn deserialize_row<T: DeserializeOwned>(row: serde_json::Map<String, Value>) -> Result<T> {
    serde_json::from_value::<T>(Value::Object(row)).map_err(|e| SurqlError::Serialization {
        reason: e.to_string(),
    })
}

/// Flatten the response produced by [`DatabaseClient::query`] (array of
/// per-statement results) into a single list of row values.
///
/// [`DatabaseClient::query`] yields a `Value::Array` with one entry per
/// SurrealQL statement. Each entry is itself either:
///
/// - an array of records (most `SELECT` / `CREATE` / `UPDATE` responses), or
/// - a single object (e.g. `RETURN {...}` / aggregate statements), or
/// - a `null` / scalar (e.g. `RETURN 42`).
///
/// The legacy nested shape (`[{"result": [...]}, ...]`) returned by the
/// Python SDK is also accepted so this helper doubles as a compat layer.
pub(crate) fn flatten_rows(raw: &Value) -> Vec<Value> {
    let mut out: Vec<Value> = Vec::new();
    match raw {
        Value::Array(items) => {
            for item in items {
                append_flattened(&mut out, item);
            }
        }
        other => append_flattened(&mut out, other),
    }
    if out.is_empty() {
        // Fall back to the legacy extractor so callers seeing the older
        // `[{"result": [...]}]` wrapping still get something.
        return extract_result(raw).into_iter().map(Value::Object).collect();
    }
    out
}

fn append_flattened(out: &mut Vec<Value>, value: &Value) {
    match value {
        Value::Null => {}
        Value::Array(inner) => {
            for v in inner {
                append_flattened(out, v);
            }
        }
        Value::Object(obj) => {
            if let Some(inner) = obj.get("result") {
                append_flattened(out, inner);
            } else {
                out.push(Value::Object(obj.clone()));
            }
        }
        other => out.push(other.clone()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;
    use serde_json::json;

    #[derive(Debug, Deserialize, PartialEq)]
    struct Row {
        name: String,
        age: u32,
    }

    #[test]
    fn extract_rows_nested_format() {
        let raw = json!([
            {"result": [
                {"name": "Alice", "age": 30},
                {"name": "Bob", "age": 40}
            ]}
        ]);
        let rows: Vec<Row> = extract_rows(&raw).unwrap();
        assert_eq!(
            rows,
            vec![
                Row {
                    name: "Alice".into(),
                    age: 30
                },
                Row {
                    name: "Bob".into(),
                    age: 40
                }
            ]
        );
    }

    #[test]
    fn extract_rows_flat_format() {
        let raw = json!([{"name": "Alice", "age": 30}]);
        let rows: Vec<Row> = extract_rows(&raw).unwrap();
        assert_eq!(
            rows,
            vec![Row {
                name: "Alice".into(),
                age: 30
            }]
        );
    }

    #[test]
    fn extract_rows_empty() {
        let raw = json!([]);
        let rows: Vec<Row> = extract_rows(&raw).unwrap();
        assert!(rows.is_empty());
    }

    #[test]
    fn extract_rows_returns_serialization_error_on_shape_mismatch() {
        let raw = json!([{"result": [{"name": "Alice", "age": "not-a-number"}]}]);
        let err = extract_rows::<Row>(&raw).unwrap_err();
        assert!(matches!(err, SurqlError::Serialization { .. }));
    }
}
