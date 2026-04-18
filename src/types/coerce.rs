//! Type coercion utilities for SurrealDB response data.
//!
//! Port of `surql/types/coerce.py`. Converts SurrealDB ISO datetime
//! strings into [`chrono::DateTime<Utc>`] values and provides a helper
//! for bulk-coercion across a `HashMap` representing a record.

use chrono::{DateTime, NaiveDateTime, TimeZone, Utc};
use serde_json::Value;

use crate::error::{Result, SurqlError};

/// Convert a SurrealDB ISO-8601 datetime string into [`DateTime<Utc>`].
///
/// Handles the formats:
///
/// - `2024-01-15T10:30:00Z`
/// - `2024-01-15T10:30:00+00:00`
/// - `2024-01-15T10:30:00.123456789Z` (nanoseconds truncated)
/// - `2024-01-15T10:30:00` (naive; coerced to UTC)
///
/// ## Examples
///
/// ```
/// use chrono::Datelike;
/// use surql::types::coerce_datetime;
///
/// let dt = coerce_datetime("2024-01-15T10:30:00Z").unwrap();
/// assert_eq!(dt.year(), 2024);
/// assert_eq!(dt.month(), 1);
/// ```
pub fn coerce_datetime(value: &str) -> Result<DateTime<Utc>> {
    // Try RFC 3339 (handles Z, offsets, fractional seconds)
    if let Ok(dt) = DateTime::parse_from_rfc3339(&truncate_fraction(value)) {
        return Ok(dt.with_timezone(&Utc));
    }

    // Try naive formats
    if let Ok(ndt) = NaiveDateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S%.f") {
        return Ok(Utc.from_utc_datetime(&ndt));
    }
    if let Ok(ndt) = NaiveDateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S") {
        return Ok(Utc.from_utc_datetime(&ndt));
    }

    Err(SurqlError::Validation {
        reason: format!("could not parse datetime string {value:?}"),
    })
}

/// Return a copy of `data` with the listed string fields parsed as UTC datetimes.
///
/// Fields that are missing, null, or already non-string are left unchanged.
/// Parse failures return [`SurqlError::Validation`].
///
/// ## Examples
///
/// ```
/// use serde_json::{json, Value};
/// use surql::types::coerce_record_datetimes;
///
/// let mut record = serde_json::Map::new();
/// record.insert("name".into(), json!("Alice"));
/// record.insert("created_at".into(), json!("2024-01-15T10:30:00Z"));
///
/// let out = coerce_record_datetimes(&record, &["created_at"]).unwrap();
/// assert!(matches!(out.get("created_at"), Some(Value::String(_))));
/// ```
///
/// The returned map stores coerced datetimes as RFC 3339 strings normalised
/// to UTC, leaving downstream consumers free to decode with `chrono` as
/// needed.
pub fn coerce_record_datetimes(
    data: &serde_json::Map<String, Value>,
    datetime_fields: &[&str],
) -> Result<serde_json::Map<String, Value>> {
    let mut out = data.clone();
    for field in datetime_fields {
        let Some(existing) = out.get(*field) else {
            continue;
        };
        if existing.is_null() {
            continue;
        }
        let Value::String(raw) = existing else {
            continue;
        };
        let dt = coerce_datetime(raw)?;
        out.insert((*field).to_owned(), Value::String(dt.to_rfc3339()));
    }
    Ok(out)
}

/// Truncate fractional seconds beyond 9 digits so chrono's RFC3339
/// parser accepts SurrealDB-style nanosecond precision.
fn truncate_fraction(value: &str) -> String {
    let Some(dot_idx) = value.find('.') else {
        return value.to_owned();
    };
    let bytes = value.as_bytes();
    let mut frac_end = dot_idx + 1;
    while frac_end < bytes.len() && bytes[frac_end].is_ascii_digit() {
        frac_end += 1;
    }
    let frac = &value[dot_idx + 1..frac_end];
    if frac.len() <= 9 {
        return value.to_owned();
    }
    let mut out = String::with_capacity(value.len());
    out.push_str(&value[..=dot_idx]);
    out.push_str(&frac[..9]);
    out.push_str(&value[frac_end..]);
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parses_z_suffix() {
        let dt = coerce_datetime("2024-01-15T10:30:00Z").unwrap();
        assert_eq!(dt.to_rfc3339(), "2024-01-15T10:30:00+00:00");
    }

    #[test]
    fn parses_offset() {
        let dt = coerce_datetime("2024-01-15T10:30:00+00:00").unwrap();
        assert_eq!(dt.to_rfc3339(), "2024-01-15T10:30:00+00:00");
    }

    #[test]
    fn parses_nanoseconds() {
        let dt = coerce_datetime("2024-01-15T10:30:00.123456789Z").unwrap();
        assert_eq!(dt.year(), 2024);
        // chrono DateTime::parse_from_rfc3339 supports up to 9 decimal digits
    }

    use chrono::Datelike;

    #[test]
    fn parses_naive_date() {
        let dt = coerce_datetime("2024-01-15T10:30:00").unwrap();
        assert_eq!(dt.to_rfc3339(), "2024-01-15T10:30:00+00:00");
    }

    #[test]
    fn rejects_garbage() {
        assert!(coerce_datetime("not-a-date").is_err());
    }

    #[test]
    fn coerces_datetime_field() {
        let mut m = serde_json::Map::new();
        m.insert("name".into(), json!("Alice"));
        m.insert("created_at".into(), json!("2024-01-15T10:30:00Z"));
        let out = coerce_record_datetimes(&m, &["created_at"]).unwrap();
        assert_eq!(
            out.get("created_at").unwrap().as_str().unwrap(),
            "2024-01-15T10:30:00+00:00"
        );
        assert_eq!(out.get("name").unwrap().as_str().unwrap(), "Alice");
    }

    #[test]
    fn skips_missing_and_null_fields() {
        let mut m = serde_json::Map::new();
        m.insert("deleted_at".into(), Value::Null);
        let out = coerce_record_datetimes(&m, &["deleted_at", "not_present"]).unwrap();
        assert!(out.get("deleted_at").unwrap().is_null());
    }
}
