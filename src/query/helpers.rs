//! Functional query-builder helpers and shared types.
//!
//! Port of `surql/query/helpers.py`. Provides standalone constructor
//! functions that each return a fresh [`Query`] pre-populated for a common
//! operation, plus the [`ReturnFormat`] and [`VectorDistanceType`] enums
//! shared with [`builder`](super::builder).

use std::collections::BTreeMap;
use std::fmt;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::Result;

use super::builder::Query;

/// Return format for `CREATE`, `UPDATE`, `UPSERT`, `DELETE` operations.
///
/// Controls what the server sends back after a mutation. Mirrors the
/// `RETURN ...` clause.
///
/// ## Examples
///
/// ```
/// use surql::query::helpers::ReturnFormat;
/// assert_eq!(ReturnFormat::Diff.to_surql(), "DIFF");
/// assert_eq!(ReturnFormat::None.to_surql(), "NONE");
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum ReturnFormat {
    /// `RETURN NONE`
    None,
    /// `RETURN DIFF`
    Diff,
    /// `RETURN FULL`
    Full,
    /// `RETURN BEFORE`
    Before,
    /// `RETURN AFTER`
    After,
}

impl ReturnFormat {
    /// Render as SurrealQL keyword (`NONE` / `DIFF` / `FULL` / `BEFORE` / `AFTER`).
    pub fn to_surql(self) -> &'static str {
        match self {
            Self::None => "NONE",
            Self::Diff => "DIFF",
            Self::Full => "FULL",
            Self::Before => "BEFORE",
            Self::After => "AFTER",
        }
    }
}

impl fmt::Display for ReturnFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.to_surql())
    }
}

/// Distance metric used by vector similarity operators and functions.
///
/// The uppercase spelling (via [`VectorDistanceType::to_surql`]) is used
/// inside the `<|k,METRIC|>` / `<|k,METRIC,threshold|>` MTREE operator, and
/// the lowercase spelling (via [`VectorDistanceType::as_func_suffix`]) is
/// used for `vector::similarity::<metric>(...)` function calls — matching
/// the Python port's behaviour.
///
/// ## Examples
///
/// ```
/// use surql::query::helpers::VectorDistanceType;
/// assert_eq!(VectorDistanceType::Cosine.to_surql(), "COSINE");
/// assert_eq!(VectorDistanceType::Cosine.as_func_suffix(), "cosine");
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum VectorDistanceType {
    /// Cosine similarity.
    Cosine,
    /// Euclidean distance.
    Euclidean,
    /// Manhattan (L1) distance.
    Manhattan,
    /// Minkowski distance.
    Minkowski,
    /// Chebyshev (L-infinity) distance.
    Chebyshev,
    /// Hamming distance.
    Hamming,
    /// Jaccard distance.
    Jaccard,
    /// Pearson correlation.
    Pearson,
    /// Mahalanobis distance.
    Mahalanobis,
}

impl VectorDistanceType {
    /// Uppercase keyword used inside the `<|k,METRIC|>` MTREE operator.
    pub fn to_surql(self) -> &'static str {
        match self {
            Self::Cosine => "COSINE",
            Self::Euclidean => "EUCLIDEAN",
            Self::Manhattan => "MANHATTAN",
            Self::Minkowski => "MINKOWSKI",
            Self::Chebyshev => "CHEBYSHEV",
            Self::Hamming => "HAMMING",
            Self::Jaccard => "JACCARD",
            Self::Pearson => "PEARSON",
            Self::Mahalanobis => "MAHALANOBIS",
        }
    }

    /// Lowercase suffix used for `vector::similarity::<suffix>(...)` calls.
    pub fn as_func_suffix(self) -> &'static str {
        match self {
            Self::Cosine => "cosine",
            Self::Euclidean => "euclidean",
            Self::Manhattan => "manhattan",
            Self::Minkowski => "minkowski",
            Self::Chebyshev => "chebyshev",
            Self::Hamming => "hamming",
            Self::Jaccard => "jaccard",
            Self::Pearson => "pearson",
            Self::Mahalanobis => "mahalanobis",
        }
    }
}

impl fmt::Display for VectorDistanceType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.to_surql())
    }
}

/// Convenience alias for the data map passed to `INSERT` / `UPDATE` /
/// `UPSERT` / `RELATE`.
///
/// Uses [`BTreeMap`] so that serialized key order is deterministic, which
/// matches the Python implementation's reliance on `dict` insertion order
/// for stable query output in tests.
pub type DataMap = BTreeMap<String, Value>;

// ---------------------------------------------------------------------------
// Standalone constructors
// ---------------------------------------------------------------------------

/// Create a `SELECT` query. Pass `None` for `SELECT *`.
///
/// ## Examples
///
/// ```
/// use surql::query::helpers::select;
///
/// let q = select(None);
/// assert_eq!(q.to_surql_or_panic_with_table("user"), "SELECT * FROM user");
/// ```
pub fn select(fields: Option<Vec<String>>) -> Query {
    Query::new().select(fields)
}

/// Add a `FROM <table>` clause to an existing query.
pub fn from_table(query: Query, table: impl Into<String>) -> Result<Query> {
    query.from_table(table)
}

/// Add a WHERE condition to an existing query. Accepts a string condition.
pub fn where_(query: Query, condition: impl Into<String>) -> Query {
    query.where_str(condition)
}

/// Add an `ORDER BY <field> <direction>` clause to an existing query.
///
/// `direction` must be `"ASC"` or `"DESC"` (case-insensitive).
pub fn order_by(
    query: Query,
    field: impl Into<String>,
    direction: impl Into<String>,
) -> Result<Query> {
    query.order_by(field, direction)
}

/// Add a `LIMIT n` clause to an existing query.
pub fn limit(query: Query, n: i64) -> Result<Query> {
    query.limit(n)
}

/// Add a `START n` (offset) clause to an existing query.
pub fn offset(query: Query, n: i64) -> Result<Query> {
    query.offset(n)
}

/// Create an `INSERT` query (renders as `CREATE <table> CONTENT {...}`).
pub fn insert(table: impl Into<String>, data: DataMap) -> Result<Query> {
    Query::new().insert(table, data)
}

/// Create an `UPDATE` query.
pub fn update(target: impl Into<String>, data: DataMap) -> Result<Query> {
    Query::new().update(target, data)
}

/// Create an `UPSERT` query.
pub fn upsert(target: impl Into<String>, data: DataMap) -> Result<Query> {
    Query::new().upsert(target, data)
}

/// Create a `DELETE` query.
pub fn delete(target: impl Into<String>) -> Result<Query> {
    Query::new().delete(target)
}

/// Create a `RELATE` query:
/// `RELATE <from>-><edge_table>-><to> [CONTENT {...}]`.
pub fn relate(
    edge_table: impl Into<String>,
    from_record: impl Into<String>,
    to_record: impl Into<String>,
    data: Option<DataMap>,
) -> Result<Query> {
    Query::new().relate(edge_table, from_record, to_record, data)
}

/// Create a vector similarity search query.
///
/// Convenience wrapper for `SELECT ... FROM <table> WHERE <field> <|k,METRIC|> [..]`.
pub fn vector_search_query(
    table: impl Into<String>,
    field: impl Into<String>,
    vector: Vec<f64>,
    k: i64,
    distance: VectorDistanceType,
    fields: Option<Vec<String>>,
    threshold: Option<f64>,
) -> Result<Query> {
    Query::new()
        .select(fields)
        .from_table(table)?
        .vector_search(field, vector, k, distance, threshold)
}

/// Create a vector similarity search query that also projects the score.
///
/// Combines [`Query::similarity_score`] with [`Query::vector_search`].
#[allow(clippy::too_many_arguments)]
pub fn similarity_search_query(
    table: impl Into<String>,
    field: impl Into<String>,
    vector: Vec<f64>,
    k: i64,
    distance: VectorDistanceType,
    threshold: Option<f64>,
    fields: Option<Vec<String>>,
    alias: impl Into<String>,
) -> Result<Query> {
    let target_field: String = field.into();
    Query::new()
        .select(fields)
        .from_table(table)?
        .similarity_score(&target_field, &vector, distance, alias)
        .vector_search(target_field, vector, k, distance, threshold)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn return_format_to_surql() {
        assert_eq!(ReturnFormat::None.to_surql(), "NONE");
        assert_eq!(ReturnFormat::Diff.to_surql(), "DIFF");
        assert_eq!(ReturnFormat::Full.to_surql(), "FULL");
        assert_eq!(ReturnFormat::Before.to_surql(), "BEFORE");
        assert_eq!(ReturnFormat::After.to_surql(), "AFTER");
    }

    #[test]
    fn return_format_display_matches_surql() {
        assert_eq!(ReturnFormat::Diff.to_string(), "DIFF");
    }

    #[test]
    fn vector_distance_uppercase() {
        assert_eq!(VectorDistanceType::Cosine.to_surql(), "COSINE");
        assert_eq!(VectorDistanceType::Euclidean.to_surql(), "EUCLIDEAN");
        assert_eq!(VectorDistanceType::Manhattan.to_surql(), "MANHATTAN");
        assert_eq!(VectorDistanceType::Minkowski.to_surql(), "MINKOWSKI");
        assert_eq!(VectorDistanceType::Chebyshev.to_surql(), "CHEBYSHEV");
        assert_eq!(VectorDistanceType::Hamming.to_surql(), "HAMMING");
        assert_eq!(VectorDistanceType::Jaccard.to_surql(), "JACCARD");
        assert_eq!(VectorDistanceType::Pearson.to_surql(), "PEARSON");
        assert_eq!(VectorDistanceType::Mahalanobis.to_surql(), "MAHALANOBIS");
    }

    #[test]
    fn vector_distance_func_suffix_is_lowercase() {
        assert_eq!(VectorDistanceType::Cosine.as_func_suffix(), "cosine");
        assert_eq!(VectorDistanceType::Euclidean.as_func_suffix(), "euclidean");
    }

    #[test]
    fn select_helper_is_star_by_default() {
        let q = select(None).from_table("user").unwrap();
        assert_eq!(q.to_surql().unwrap(), "SELECT * FROM user");
    }

    #[test]
    fn select_helper_projects_fields() {
        let q = select(Some(vec!["name".into(), "email".into()]))
            .from_table("user")
            .unwrap();
        assert_eq!(q.to_surql().unwrap(), "SELECT name, email FROM user");
    }

    #[test]
    fn from_table_helper_sets_table() {
        let q = from_table(select(None), "user").unwrap();
        assert_eq!(q.to_surql().unwrap(), "SELECT * FROM user");
    }

    #[test]
    fn where_helper_adds_condition() {
        let q = where_(select(None).from_table("user").unwrap(), "age > 18");
        assert_eq!(q.to_surql().unwrap(), "SELECT * FROM user WHERE (age > 18)");
    }

    #[test]
    fn order_by_helper() {
        let q = order_by(select(None).from_table("user").unwrap(), "name", "ASC").unwrap();
        assert_eq!(
            q.to_surql().unwrap(),
            "SELECT * FROM user ORDER BY name ASC"
        );
    }

    #[test]
    fn limit_helper() {
        let q = limit(select(None).from_table("user").unwrap(), 10).unwrap();
        assert_eq!(q.to_surql().unwrap(), "SELECT * FROM user LIMIT 10");
    }

    #[test]
    fn offset_helper_renders_start() {
        let q = offset(select(None).from_table("user").unwrap(), 20).unwrap();
        assert_eq!(q.to_surql().unwrap(), "SELECT * FROM user START 20");
    }

    #[test]
    fn insert_helper_constructs_query() {
        let mut data = DataMap::new();
        data.insert("name".into(), Value::String("Alice".into()));
        let q = insert("user", data).unwrap();
        let sql = q.to_surql().unwrap();
        assert!(sql.starts_with("CREATE user CONTENT"));
        assert!(sql.contains("name: 'Alice'"));
    }

    #[test]
    fn update_helper_constructs_query() {
        let mut data = DataMap::new();
        data.insert("status".into(), Value::String("active".into()));
        let q = update("user:alice", data).unwrap();
        assert_eq!(
            q.to_surql().unwrap(),
            "UPDATE user:alice SET status = 'active'"
        );
    }

    #[test]
    fn upsert_helper_constructs_query() {
        let mut data = DataMap::new();
        data.insert("status".into(), Value::String("active".into()));
        let q = upsert("user:alice", data).unwrap();
        assert_eq!(
            q.to_surql().unwrap(),
            "UPSERT user:alice CONTENT {status: 'active'}"
        );
    }

    #[test]
    fn delete_helper_constructs_query() {
        let q = delete("user:alice").unwrap();
        assert_eq!(q.to_surql().unwrap(), "DELETE user:alice");
    }

    #[test]
    fn relate_helper_constructs_query() {
        let q = relate("likes", "user:alice", "post:123", None).unwrap();
        assert_eq!(q.to_surql().unwrap(), "RELATE user:alice->likes->post:123");
    }

    #[test]
    fn vector_search_query_helper() {
        let q = vector_search_query(
            "documents",
            "embedding",
            vec![0.1, 0.2, 0.3],
            10,
            VectorDistanceType::Cosine,
            None,
            Some(0.7),
        )
        .unwrap();
        let sql = q.to_surql().unwrap();
        assert!(sql.starts_with("SELECT * FROM documents"));
        assert!(sql.contains("embedding <|10,COSINE,0.7|>"));
    }
}
