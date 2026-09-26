//! Shaping values between the SDK and the typed client API: statement
//! results, typed rows, CRUD targets, credential payloads and durations.

use std::time::Duration;

use serde::de::DeserializeOwned;
use serde_json::Value;
use surrealdb::IndexedResults;

use super::errors::{query_err, statement_was_not_executed};
use crate::error::{Result, SurqlError};
use crate::query::results::response_rows;
use crate::query::validate::render_target as query_target;

/// Unpack every statement of a response into one JSON array entry each.
///
/// A statement's error fails the whole call (and is never retried). When a
/// transaction fails, the engine marks every other statement in it "not
/// executed"; the error reported is the one that says why, and a
/// not-executed error only when no statement says more.
pub(super) fn statement_results(mut response: IndexedResults) -> Result<Value> {
    let count = response.num_statements();
    let mut out = Vec::with_capacity(count);
    let mut skipped = None;
    for i in 0..count {
        // `IndexedResults::take(usize)` in 3.x only accepts
        // `surrealdb::types::Value` / `Vec<T>` / `Option<T>` for
        // index-based retrieval. Take the core `Value` (which
        // preserves record IDs, durations, decimals, etc.) and
        // downgrade to `serde_json::Value` via `into_json_value`.
        let taken: std::result::Result<surrealdb::types::Value, _> = response.take(i);
        match taken {
            Ok(raw) => out.push(raw.into_json_value()),
            Err(e) if statement_was_not_executed(&e) => {
                skipped.get_or_insert_with(|| query_err(&e));
            }
            Err(e) => return Err(query_err(&e)),
        }
    }
    match skipped {
        Some(err) => Err(err),
        None => Ok(Value::Array(out)),
    }
}

/// Render a typed-CRUD or `LIVE SELECT` target as SurrealQL.
///
/// The target is data, never SurrealQL, by the same rule the query builder
/// applies ([`query_target`]): a table name renders through `quote_ident`,
/// a `table:key` is parsed as a `RecordID` whose `Display` quotes the key
/// so it cannot end the statement, keys SurrealQL would evaluate (arrays,
/// objects, ranges, generators) are refused, and so is anything else.
/// Surrounding whitespace is ignored.
pub(crate) fn render_target(target: &str) -> Result<String> {
    query_target(target.trim())
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
/// Rows are split out by [`response_rows`], the one rule the whole crate
/// uses: each statement's result spreads exactly one level, and the legacy
/// `{"result": …}` envelope is unwrapped at statement level only, so a row
/// that merely has a `result` field is a row.
pub(super) fn flatten_rows_typed<T: DeserializeOwned>(raw: &Value) -> Result<Vec<T>> {
    response_rows(raw)
        .into_iter()
        .map(|row| {
            serde_json::from_value(row).map_err(|e| SurqlError::Serialization {
                reason: e.to_string(),
            })
        })
        .collect()
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
