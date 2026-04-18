//! Typed result wrappers and extraction helpers for SurrealDB responses.
//!
//! Port of `surql/query/results.py`.

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::error::{Result, SurqlError};

/// Generic query execution result.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QueryResult<T> {
    /// Query payload.
    pub data: T,
    /// Optional execution time reported by the server.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub time: Option<String>,
    /// Status string (defaults to `"OK"`).
    #[serde(default = "default_status")]
    pub status: String,
}

fn default_status() -> String {
    "OK".to_string()
}

/// Single-record wrapper.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RecordResult<T> {
    /// Inner record value.
    pub record: Option<T>,
    /// Whether the record existed at query time.
    #[serde(default = "default_true")]
    pub exists: bool,
}

fn default_true() -> bool {
    true
}

impl<T> RecordResult<T> {
    /// Unwrap the record, panicking on `None`.
    pub fn unwrap(self) -> T {
        self.record
            .expect("RecordResult::unwrap called on a None record")
    }

    /// Unwrap the record, or return [`SurqlError::Validation`] on `None`.
    pub fn try_unwrap(self) -> Result<T> {
        self.record.ok_or_else(|| SurqlError::Validation {
            reason: "Cannot unwrap None record".into(),
        })
    }

    /// Unwrap the record or return a supplied default.
    pub fn unwrap_or(self, default: T) -> T {
        self.record.unwrap_or(default)
    }
}

/// List-of-records wrapper with optional pagination hints.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ListResult<T> {
    /// Records returned.
    #[serde(default = "Vec::new")]
    pub records: Vec<T>,
    /// Total count (if known).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub total: Option<u64>,
    /// `LIMIT` used in the query.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<u64>,
    /// `OFFSET` used in the query.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub offset: Option<u64>,
    /// Whether more pages are available.
    #[serde(default)]
    pub has_more: bool,
}

impl<T> ListResult<T> {
    /// Number of records in this page.
    pub fn len(&self) -> usize {
        self.records.len()
    }

    /// Whether the page is empty.
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    /// Get a reference to the first record.
    pub fn first(&self) -> Option<&T> {
        self.records.first()
    }

    /// Get a reference to the last record.
    pub fn last(&self) -> Option<&T> {
        self.records.last()
    }

    /// Iterate by reference.
    pub fn iter(&self) -> std::slice::Iter<'_, T> {
        self.records.iter()
    }
}

impl<'a, T> IntoIterator for &'a ListResult<T> {
    type Item = &'a T;
    type IntoIter = std::slice::Iter<'a, T>;
    fn into_iter(self) -> Self::IntoIter {
        self.records.iter()
    }
}

impl<T> std::ops::Index<usize> for ListResult<T> {
    type Output = T;
    fn index(&self, idx: usize) -> &T {
        &self.records[idx]
    }
}

/// Count aggregation wrapper.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CountResult {
    /// Count value.
    pub count: i64,
}

/// Generic aggregation wrapper.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AggregateResult {
    /// Aggregated value.
    pub value: Value,
    /// Aggregation operation name (e.g. `"SUM"`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operation: Option<String>,
    /// Field aggregated (e.g. `"age"`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub field: Option<String>,
}

/// Pagination metadata.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PageInfo {
    /// Current page number (1-indexed).
    pub current_page: u64,
    /// Items per page.
    pub page_size: u64,
    /// Total pages available.
    pub total_pages: u64,
    /// Total items across all pages.
    pub total_items: u64,
    /// Whether a previous page exists.
    #[serde(default)]
    pub has_previous: bool,
    /// Whether a next page exists.
    #[serde(default)]
    pub has_next: bool,
}

/// Paginated result with page metadata.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PaginatedResult<T> {
    /// Items in the current page.
    #[serde(default = "Vec::new")]
    pub items: Vec<T>,
    /// Pagination metadata.
    pub page_info: PageInfo,
}

impl<T> PaginatedResult<T> {
    /// Number of items in the current page.
    pub fn len(&self) -> usize {
        self.items.len()
    }
    /// Whether the page is empty.
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }
    /// Iterate by reference.
    pub fn iter(&self) -> std::slice::Iter<'_, T> {
        self.items.iter()
    }
}

impl<'a, T> IntoIterator for &'a PaginatedResult<T> {
    type Item = &'a T;
    type IntoIter = std::slice::Iter<'a, T>;
    fn into_iter(self) -> Self::IntoIter {
        self.items.iter()
    }
}

// ---------------------------------------------------------------------------
// Constructor helpers
// ---------------------------------------------------------------------------

/// Build a [`QueryResult`] with status `"OK"`.
pub fn success<T>(data: T, time: Option<String>) -> QueryResult<T> {
    QueryResult {
        data,
        time,
        status: "OK".into(),
    }
}

/// Build a [`RecordResult`].
pub fn record<T>(rec: Option<T>, exists: bool) -> RecordResult<T> {
    RecordResult {
        record: rec,
        exists,
    }
}

/// Build a [`ListResult`] with `has_more` computed from the supplied
/// pagination inputs (mirrors the Python port's heuristic).
pub fn records<T>(
    items: Vec<T>,
    total: Option<u64>,
    limit: Option<u64>,
    offset: Option<u64>,
) -> ListResult<T> {
    let has_more = match (total, limit, offset) {
        (Some(t), Some(l), Some(o)) => o.saturating_add(l) < t,
        (None, Some(l), _) => items.len() as u64 == l,
        _ => false,
    };
    ListResult {
        records: items,
        total,
        limit,
        offset,
        has_more,
    }
}

/// Build a [`CountResult`].
pub fn count_result(value: i64) -> CountResult {
    CountResult { count: value }
}

/// Build an [`AggregateResult`].
pub fn aggregate(
    value: Value,
    operation: Option<String>,
    field: Option<String>,
) -> AggregateResult {
    AggregateResult {
        value,
        operation,
        field,
    }
}

/// Build a [`PaginatedResult`].
///
/// `page` is 1-indexed; `page_size` must be > 0 (else the returned
/// [`PageInfo::total_pages`] is zero).
pub fn paginated<T>(items: Vec<T>, page: u64, page_size: u64, total: u64) -> PaginatedResult<T> {
    let total_pages = if page_size == 0 {
        0
    } else {
        total.div_ceil(page_size)
    };
    let page_info = PageInfo {
        current_page: page,
        page_size,
        total_pages,
        total_items: total,
        has_previous: page > 1,
        has_next: page < total_pages,
    };
    PaginatedResult { items, page_info }
}

// ---------------------------------------------------------------------------
// Raw extraction
// ---------------------------------------------------------------------------

/// Extract the array of record dictionaries from a raw SurrealDB response.
///
/// Handles both the "nested" format returned by
/// [`db.query`](https://surrealdb.com/docs) -- `[{"result": [...]}]` --
/// and the "flat" format returned by `db.select` -- `[{...}, {...}]`.
pub fn extract_result(result: &Value) -> Vec<Map<String, Value>> {
    if let Value::Array(items) = result {
        if items.is_empty() {
            return Vec::new();
        }
        let is_nested = matches!(&items[0], Value::Object(o) if o.contains_key("result"));
        if is_nested {
            let mut out = Vec::new();
            for item in items {
                if let Value::Object(obj) = item {
                    if let Some(inner) = obj.get("result") {
                        push_value(&mut out, inner);
                    }
                }
            }
            return out;
        }
        return items
            .iter()
            .filter_map(|v| v.as_object().cloned())
            .collect();
    }
    if let Value::Object(obj) = result {
        if let Some(inner) = obj.get("result") {
            let mut out = Vec::new();
            push_value(&mut out, inner);
            return out;
        }
    }
    Vec::new()
}

fn push_value(out: &mut Vec<Map<String, Value>>, v: &Value) {
    match v {
        Value::Array(arr) => {
            for a in arr {
                if let Value::Object(o) = a {
                    out.push(o.clone());
                }
            }
        }
        Value::Null => {}
        Value::Object(o) => out.push(o.clone()),
        other => {
            let mut m = Map::new();
            m.insert("value".into(), other.clone());
            out.push(m);
        }
    }
}

/// Extract the first record from a raw response, or `None` when empty.
pub fn extract_one(result: &Value) -> Option<Map<String, Value>> {
    extract_result(result).into_iter().next()
}

/// Extract a scalar value from an aggregate response (e.g. `{count: 42}`).
/// Returns `default` when the result is empty or the key is missing.
pub fn extract_scalar(result: &Value, key: &str, default: Value) -> Value {
    extract_one(result)
        .and_then(|r| r.get(key).cloned())
        .unwrap_or(default)
}

/// Report whether the response contains at least one record.
pub fn has_results(result: &Value) -> bool {
    !extract_result(result).is_empty()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn record_result_unwrap() {
        let r = record(Some(42), true);
        assert_eq!(r.unwrap(), 42);
    }

    #[test]
    fn record_result_try_unwrap() {
        let none: RecordResult<i32> = record(None, false);
        assert!(none.try_unwrap().is_err());
    }

    #[test]
    fn record_result_unwrap_or() {
        let none: RecordResult<i32> = record(None, false);
        assert_eq!(none.unwrap_or(5), 5);
        let some = record(Some(1), true);
        assert_eq!(some.unwrap_or(5), 1);
    }

    #[test]
    fn list_result_helpers() {
        let lr = records(vec![1, 2, 3], Some(3), Some(10), Some(0));
        assert_eq!(lr.len(), 3);
        assert!(!lr.is_empty());
        assert_eq!(lr.first(), Some(&1));
        assert_eq!(lr.last(), Some(&3));
        assert_eq!(lr[1], 2);
        let sum: i32 = (&lr).into_iter().sum();
        assert_eq!(sum, 6);
    }

    #[test]
    fn records_computes_has_more_from_total() {
        let lr = records(vec![1, 2, 3], Some(20), Some(3), Some(0));
        assert!(lr.has_more);
        let done = records(vec![1, 2, 3], Some(3), Some(3), Some(0));
        assert!(!done.has_more);
    }

    #[test]
    fn records_computes_has_more_from_limit_only() {
        let lr = records(vec![1, 2, 3], None, Some(3), None);
        assert!(lr.has_more);
        let lr2 = records(vec![1, 2], None, Some(3), None);
        assert!(!lr2.has_more);
    }

    #[test]
    fn paginated_computes_pages() {
        let p = paginated(vec![1, 2, 3], 1, 10, 100);
        assert_eq!(p.page_info.current_page, 1);
        assert_eq!(p.page_info.page_size, 10);
        assert_eq!(p.page_info.total_pages, 10);
        assert!(!p.page_info.has_previous);
        assert!(p.page_info.has_next);
    }

    #[test]
    fn paginated_last_page_rounding() {
        let p = paginated::<i32>(vec![], 10, 10, 95);
        assert_eq!(p.page_info.total_pages, 10);
        assert_eq!(p.page_info.current_page, 10);
        assert!(!p.page_info.has_next);
        assert!(p.page_info.has_previous);
    }

    #[test]
    fn paginated_zero_page_size() {
        let p = paginated::<i32>(vec![], 1, 0, 100);
        assert_eq!(p.page_info.total_pages, 0);
    }

    #[test]
    fn extract_flat() {
        let v = json!([{"id": "user:123", "name": "Alice"}]);
        let out = extract_result(&v);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].get("name").unwrap(), &json!("Alice"));
    }

    #[test]
    fn extract_nested() {
        let v = json!([{"result": [{"id": "user:123", "name": "Alice"}]}]);
        let out = extract_result(&v);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].get("name").unwrap(), &json!("Alice"));
    }

    #[test]
    fn extract_nested_multiple() {
        let v = json!([
            {"result": [{"id": "user:1"}, {"id": "user:2"}]},
            {"result": [{"id": "user:3"}]}
        ]);
        let out = extract_result(&v);
        assert_eq!(out.len(), 3);
    }

    #[test]
    fn extract_empty() {
        let v = json!([]);
        assert!(extract_result(&v).is_empty());
    }

    #[test]
    fn extract_null() {
        let v = Value::Null;
        assert!(extract_result(&v).is_empty());
    }

    #[test]
    fn extract_object_with_result() {
        let v = json!({"result": [{"id": "u:1"}]});
        assert_eq!(extract_result(&v).len(), 1);
    }

    #[test]
    fn extract_one_first() {
        let v = json!([{"result": [{"id": "user:123", "name": "Alice"}]}]);
        let one = extract_one(&v).unwrap();
        assert_eq!(one.get("name").unwrap(), &json!("Alice"));
        assert!(extract_one(&json!([])).is_none());
    }

    #[test]
    fn extract_scalar_count() {
        let v = json!([{"result": [{"count": 42}]}]);
        assert_eq!(extract_scalar(&v, "count", json!(0)), json!(42));
        assert_eq!(extract_scalar(&json!([]), "count", json!(0)), json!(0));
        let v_missing = json!([{"id": "u:1"}]);
        assert_eq!(extract_scalar(&v_missing, "total", json!(0)), json!(0));
    }

    #[test]
    fn has_results_works() {
        assert!(has_results(&json!([{"result": [{"id": "u:1"}]}])));
        assert!(!has_results(&json!([])));
        assert!(has_results(&json!([{"id": "u:1"}])));
        assert!(!has_results(&json!([{"result": []}])));
    }

    #[test]
    fn success_wraps_data() {
        let r = success(vec![1, 2, 3], Some("12ms".into()));
        assert_eq!(r.status, "OK");
        assert_eq!(r.data, vec![1, 2, 3]);
        assert_eq!(r.time.as_deref(), Some("12ms"));
    }

    #[test]
    fn count_and_aggregate() {
        let c = count_result(42);
        assert_eq!(c.count, 42);
        let a = aggregate(json!(25.5), Some("AVG".into()), Some("age".into()));
        assert_eq!(a.value, json!(25.5));
    }
}
