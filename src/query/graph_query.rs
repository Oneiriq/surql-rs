//! Fluent graph traversal builder ([`GraphQuery`]).
//!
//! Port of `surql/query/graph_query.py`. Follows the immutable-builder
//! convention used by [`Query`](crate::query::builder::Query): every
//! chainable method returns a fresh [`GraphQuery`] instance (via
//! `Clone` + field updates), so prior states remain reusable.
//!
//! ## Examples
//!
//! ```
//! use surql::query::graph_query::GraphQuery;
//!
//! let sql = GraphQuery::new("user:alice")
//!     .out("follows", None)
//!     .limit(10).unwrap()
//!     .to_surql().unwrap();
//! assert_eq!(sql, "SELECT * FROM user:alice->follows LIMIT 10");
//! ```

use crate::error::{Result, SurqlError};

#[cfg(feature = "client")]
use serde::de::DeserializeOwned;
#[cfg(feature = "client")]
use serde_json::Value;

#[cfg(feature = "client")]
use crate::connection::DatabaseClient;
#[cfg(feature = "client")]
use crate::query::executor::{extract_rows, flatten_rows};

/// Immutable fluent builder for graph traversal queries.
///
/// The builder accumulates arrow segments (`->edge[depth]` / `<-edge[depth]`
/// / `<->edge[depth]`), an optional target table, `WHERE` fragments,
/// projected fields, `FETCH` clauses, and a `LIMIT`. Call
/// [`GraphQuery::to_surql`] to render the SurrealQL, or
/// [`GraphQuery::execute`] / [`GraphQuery::fetch_typed`] /
/// [`GraphQuery::count`] / [`GraphQuery::exists`] to dispatch against a
/// [`DatabaseClient`].
///
/// All chain methods take `self` by value; use `.clone()` to fork a
/// partially-built query.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct GraphQuery {
    start: String,
    path: Vec<String>,
    conditions: Vec<String>,
    fields: Vec<String>,
    fetch: Vec<String>,
    limit_value: Option<i64>,
    target_table: Option<String>,
}

impl GraphQuery {
    /// Construct a new builder anchored at `start` (e.g. `"user:alice"`).
    pub fn new(start: impl Into<String>) -> Self {
        Self {
            start: start.into(),
            path: Vec::new(),
            conditions: Vec::new(),
            fields: Vec::new(),
            fetch: Vec::new(),
            limit_value: None,
            target_table: None,
        }
    }

    /// Append an outgoing arrow (`->edge[depth]`).
    pub fn out(mut self, edge: impl AsRef<str>, depth: Option<u32>) -> Self {
        let depth_str = depth.map_or(String::new(), |d| d.to_string());
        self.path.push(format!("->{}{depth_str}", edge.as_ref()));
        self
    }

    /// Append an incoming arrow (`<-edge[depth]`).
    ///
    /// Renamed from Python's `in_` to use Rust's raw-identifier syntax;
    /// semantics match `GraphQuery.in_` exactly.
    pub fn r#in(mut self, edge: impl AsRef<str>, depth: Option<u32>) -> Self {
        let depth_str = depth.map_or(String::new(), |d| d.to_string());
        self.path.push(format!("<-{}{depth_str}", edge.as_ref()));
        self
    }

    /// Append a bidirectional arrow (`<->edge[depth]`).
    pub fn both(mut self, edge: impl AsRef<str>, depth: Option<u32>) -> Self {
        let depth_str = depth.map_or(String::new(), |d| d.to_string());
        self.path.push(format!("<->{}{depth_str}", edge.as_ref()));
        self
    }

    /// Narrow the tail of the traversal to a specific target table. The
    /// corresponding SurrealQL is `->target_table` appended after the last
    /// edge hop.
    pub fn to(mut self, target: impl Into<String>) -> Self {
        self.target_table = Some(target.into());
        self
    }

    /// Append a `WHERE` condition. Multiple calls are combined with `AND`.
    pub fn r#where(mut self, condition: impl Into<String>) -> Self {
        self.conditions.push(condition.into());
        self
    }

    /// Project the given fields (`SELECT <fields> FROM ...`). Repeated
    /// calls extend the projection list.
    pub fn select<I, S>(mut self, fields: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.fields.extend(fields.into_iter().map(Into::into));
        self
    }

    /// Set `LIMIT`. Returns a validation error for negative values.
    pub fn limit(mut self, n: i64) -> Result<Self> {
        if n < 0 {
            return Err(SurqlError::Validation {
                reason: format!("Limit must be non-negative, got {n}"),
            });
        }
        self.limit_value = Some(n);
        Ok(self)
    }

    /// Append records to the `FETCH` clause (e.g. `FETCH author, tags`).
    pub fn fetch<I, S>(mut self, refs: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.fetch.extend(refs.into_iter().map(Into::into));
        self
    }

    /// Render the built query to SurrealQL.
    ///
    /// Returns a validation error when no traversal step has been added.
    pub fn to_surql(&self) -> Result<String> {
        if self.path.is_empty() {
            return Err(SurqlError::Validation {
                reason: "At least one traversal step (out, in, both) is required".to_string(),
            });
        }

        let fields_str = if self.fields.is_empty() {
            "*".to_string()
        } else {
            self.fields.join(", ")
        };

        let mut path_str = self.path.join("");
        if let Some(target) = &self.target_table {
            path_str.push_str("->");
            path_str.push_str(target);
        }

        let mut parts = vec![format!("SELECT {fields_str} FROM {}{path_str}", self.start)];

        if !self.conditions.is_empty() {
            let joined = self
                .conditions
                .iter()
                .map(|c| format!("({c})"))
                .collect::<Vec<_>>()
                .join(" AND ");
            parts.push(format!("WHERE {joined}"));
        }

        if !self.fetch.is_empty() {
            parts.push(format!("FETCH {}", self.fetch.join(", ")));
        }

        if let Some(n) = self.limit_value {
            parts.push(format!("LIMIT {n}"));
        }

        Ok(parts.join(" "))
    }

    /// Render a matching `SELECT count() FROM ... GROUP ALL` query.
    fn to_count_surql(&self) -> Result<String> {
        if self.path.is_empty() {
            return Err(SurqlError::Validation {
                reason: "At least one traversal step (out, in, both) is required".to_string(),
            });
        }

        let mut path_str = self.path.join("");
        if let Some(target) = &self.target_table {
            path_str.push_str("->");
            path_str.push_str(target);
        }

        let mut sql = format!("SELECT count() FROM {}{path_str}", self.start);
        if !self.conditions.is_empty() {
            let joined = self
                .conditions
                .iter()
                .map(|c| format!("({c})"))
                .collect::<Vec<_>>()
                .join(" AND ");
            sql.push_str(" WHERE ");
            sql.push_str(&joined);
        }
        sql.push_str(" GROUP ALL");
        Ok(sql)
    }

    /// Execute the rendered query and return raw JSON rows.
    #[cfg(feature = "client")]
    pub async fn execute(&self, client: &DatabaseClient) -> Result<Vec<Value>> {
        let surql = self.to_surql()?;
        let raw = client.query(&surql).await?;
        Ok(flatten_rows(&raw))
    }

    /// Execute the rendered query and deserialize each row into `T`.
    #[cfg(feature = "client")]
    pub async fn fetch_typed<T: DeserializeOwned>(
        &self,
        client: &DatabaseClient,
    ) -> Result<Vec<T>> {
        let surql = self.to_surql()?;
        let raw = client.query(&surql).await?;
        extract_rows::<T>(&raw)
    }

    /// Count matching rows via `SELECT count() ... GROUP ALL`.
    #[cfg(feature = "client")]
    pub async fn count(&self, client: &DatabaseClient) -> Result<i64> {
        let surql = self.to_count_surql()?;
        let raw = client.query(&surql).await?;
        let first = flatten_rows(&raw).into_iter().next();
        Ok(first
            .as_ref()
            .and_then(|r| r.get("count").and_then(Value::as_i64))
            .unwrap_or(0))
    }

    /// `true` when at least one row matches the query.
    #[cfg(feature = "client")]
    pub async fn exists(&self, client: &DatabaseClient) -> Result<bool> {
        Ok(self.count(client).await? > 0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn to_surql_requires_traversal_step() {
        let err = GraphQuery::new("user:alice").to_surql().unwrap_err();
        assert!(matches!(err, SurqlError::Validation { .. }));
    }

    #[test]
    fn out_renders_single_hop() {
        let sql = GraphQuery::new("user:alice")
            .out("follows", None)
            .to_surql()
            .unwrap();
        assert_eq!(sql, "SELECT * FROM user:alice->follows");
    }

    #[test]
    fn in_renders_incoming_with_depth() {
        let sql = GraphQuery::new("user:alice")
            .r#in("follows", Some(2))
            .to_surql()
            .unwrap();
        assert_eq!(sql, "SELECT * FROM user:alice<-follows2");
    }

    #[test]
    fn both_renders_bidirectional() {
        let sql = GraphQuery::new("user:alice")
            .both("knows", None)
            .to_surql()
            .unwrap();
        assert_eq!(sql, "SELECT * FROM user:alice<->knows");
    }

    #[test]
    fn to_target_table_appends_arrow_target() {
        let sql = GraphQuery::new("user:alice")
            .out("likes", None)
            .to("post")
            .to_surql()
            .unwrap();
        assert_eq!(sql, "SELECT * FROM user:alice->likes->post");
    }

    #[test]
    fn where_and_limit_compose() {
        let sql = GraphQuery::new("user:alice")
            .out("follows", None)
            .r#where("age > 18")
            .r#where("status = 'active'")
            .limit(10)
            .unwrap()
            .to_surql()
            .unwrap();
        assert_eq!(
            sql,
            "SELECT * FROM user:alice->follows WHERE (age > 18) AND (status = 'active') LIMIT 10"
        );
    }

    #[test]
    fn select_fields_projects_list() {
        let sql = GraphQuery::new("user:alice")
            .out("follows", None)
            .select(["id", "name"])
            .to_surql()
            .unwrap();
        assert_eq!(sql, "SELECT id, name FROM user:alice->follows");
    }

    #[test]
    fn fetch_appends_fetch_clause() {
        let sql = GraphQuery::new("user:alice")
            .out("likes", None)
            .fetch(["author"])
            .to_surql()
            .unwrap();
        assert_eq!(sql, "SELECT * FROM user:alice->likes FETCH author");
    }

    #[test]
    fn limit_rejects_negative_values() {
        let err = GraphQuery::new("user:alice")
            .out("follows", None)
            .limit(-1)
            .unwrap_err();
        assert!(matches!(err, SurqlError::Validation { .. }));
    }

    #[test]
    fn builder_is_immutable_across_forks() {
        let base = GraphQuery::new("user:alice").out("follows", None);
        let forked = base.clone().limit(5).unwrap();
        assert!(!base.to_surql().unwrap().contains("LIMIT"));
        assert!(forked.to_surql().unwrap().contains("LIMIT 5"));
    }

    #[test]
    fn count_surql_includes_group_all() {
        let sql = GraphQuery::new("user:alice")
            .out("follows", None)
            .to_count_surql()
            .unwrap();
        assert_eq!(sql, "SELECT count() FROM user:alice->follows GROUP ALL");
    }

    #[test]
    fn count_surql_with_where() {
        let sql = GraphQuery::new("user:alice")
            .out("follows", None)
            .r#where("age > 18")
            .to_count_surql()
            .unwrap();
        assert_eq!(
            sql,
            "SELECT count() FROM user:alice->follows WHERE (age > 18) GROUP ALL"
        );
    }
}
