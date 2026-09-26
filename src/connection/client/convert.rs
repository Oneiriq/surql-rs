//! Shaping values between the SDK and the typed client API: statement
//! results, typed rows, CRUD targets, credential payloads and durations.

use std::time::Duration;

use serde::de::DeserializeOwned;
use serde_json::Value;
use surrealdb::IndexedResults;

use super::errors::query_err;
use crate::error::{Result, SurqlError};
use crate::types::escape::{is_identifier, quote_ident};
use crate::types::RecordID;

/// Unpack every statement of a response into one JSON array entry each.
/// A statement's error fails the whole call (and is never retried).
pub(super) fn statement_results(mut response: IndexedResults) -> Result<Value> {
    let count = response.num_statements();
    let mut out = Vec::with_capacity(count);
    for i in 0..count {
        // `IndexedResults::take(usize)` in 3.x only accepts
        // `surrealdb::types::Value` / `Vec<T>` / `Option<T>` for
        // index-based retrieval. Take the core `Value` (which
        // preserves record IDs, durations, decimals, etc.) and
        // downgrade to `serde_json::Value` via `into_json_value`.
        let raw: surrealdb::types::Value = response.take(i).map_err(|e| query_err(&e))?;
        out.push(raw.into_json_value());
    }
    Ok(Value::Array(out))
}

/// Render a typed-CRUD or `LIVE SELECT` target as SurrealQL.
///
/// The target is data, never SurrealQL: a table name renders through
/// [`quote_ident`], anything with a `:` is parsed as a [`RecordID`], whose
/// `Display` quotes the key so it cannot end the statement, and anything
/// else is refused.
pub(crate) fn render_target(target: &str) -> Result<String> {
    let target = target.trim();
    if is_identifier(target) {
        return Ok(quote_ident(target));
    }
    if target.contains(':') {
        return RecordID::<()>::parse(target).map(|id| id.to_string());
    }
    Err(SurqlError::Validation {
        reason: format!("invalid target {target:?}: expected a table name or a table:id record id"),
    })
}

/// A config duration in seconds as a [`Duration`], refusing what
/// `Duration` cannot hold instead of panicking.
pub(super) fn seconds(secs: f64) -> Result<Duration> {
    Duration::try_from_secs_f64(secs).map_err(|e| SurqlError::Validation {
        reason: format!("invalid duration of {secs} seconds: {e}"),
    })
}

/// Flatten every row in the raw `query()` response into a typed vector.
pub(super) fn flatten_rows_typed<T: DeserializeOwned>(raw: &Value) -> Result<Vec<T>> {
    let mut out: Vec<T> = Vec::new();
    collect_rows(raw, &mut out)?;
    Ok(out)
}

fn collect_rows<T: DeserializeOwned>(value: &Value, out: &mut Vec<T>) -> Result<()> {
    match value {
        Value::Null => Ok(()),
        Value::Array(items) => {
            for item in items {
                collect_rows(item, out)?;
            }
            Ok(())
        }
        Value::Object(obj) => {
            if let Some(inner) = obj.get("result") {
                return collect_rows(inner, out);
            }
            let row: T = serde_json::from_value(Value::Object(obj.clone())).map_err(|e| {
                SurqlError::Serialization {
                    reason: e.to_string(),
                }
            })?;
            out.push(row);
            Ok(())
        }
        other => {
            let row: T =
                serde_json::from_value(other.clone()).map_err(|e| SurqlError::Serialization {
                    reason: e.to_string(),
                })?;
            out.push(row);
            Ok(())
        }
    }
}

pub(super) fn first_row_typed<T: DeserializeOwned>(raw: &Value) -> Result<Option<T>> {
    let rows: Vec<T> = flatten_rows_typed(raw)?;
    Ok(rows.into_iter().next())
}

pub(super) fn payload_str(map: &serde_json::Map<String, Value>, key: &str) -> Result<String> {
    match map.get(key) {
        Some(Value::String(s)) => Ok(s.clone()),
        Some(_) => Err(SurqlError::Validation {
            reason: format!("credential field {key:?} must be a string"),
        }),
        None => Err(SurqlError::Validation {
            reason: format!("credential field {key:?} is missing"),
        }),
    }
}
