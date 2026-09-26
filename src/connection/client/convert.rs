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

/// Every row of a raw `query()` response, typed.
///
/// The response holds one entry per statement. Each statement's result is
/// spread exactly one level: an array contributes its elements as rows
/// (whatever they are, arrays included), `null` contributes nothing, and
/// any other value is one row. The legacy `{"result": …}` envelope is
/// unwrapped at statement level only, and only when the object has no
/// keys besides `result`, `status`, `time` and `type`: a row that merely
/// has a `result` field is a row.
pub(super) fn flatten_rows_typed<T: DeserializeOwned>(raw: &Value) -> Result<Vec<T>> {
    let statements = match raw {
        Value::Array(statements) => statements.as_slice(),
        single => std::slice::from_ref(single),
    };
    statements
        .iter()
        .map(unwrap_envelope)
        .flat_map(|statement| match statement {
            Value::Array(rows) => rows.as_slice(),
            Value::Null => &[],
            row => std::slice::from_ref(row),
        })
        .map(|row| {
            serde_json::from_value(row.clone()).map_err(|e| SurqlError::Serialization {
                reason: e.to_string(),
            })
        })
        .collect()
}

/// The `result` of a legacy `{"result", "status", "time", "type"}`
/// statement envelope, or `statement` itself when it is not one.
fn unwrap_envelope(statement: &Value) -> &Value {
    match statement {
        Value::Object(obj)
            if obj
                .keys()
                .all(|k| matches!(k.as_str(), "result" | "status" | "time" | "type")) =>
        {
            obj.get("result").unwrap_or(statement)
        }
        other => other,
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
