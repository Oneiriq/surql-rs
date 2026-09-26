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
//!
//! // Two hops out, landing on `user` records.
//! let sql = GraphQuery::new("user:alice")
//!     .out("follows", Some(2))
//!     .to("user")
//!     .to_surql().unwrap();
//! assert_eq!(sql, "SELECT * FROM user:alice->follows->?->follows->user");
//! ```

use crate::error::{Result, SurqlError};

use super::validate::{render_target, validate_depth, validate_field_path, validate_identifier};

#[cfg(any(feature = "client", feature = "client-rustls", feature = "client-wasm"))]
use serde::de::DeserializeOwned;
#[cfg(any(feature = "client", feature = "client-rustls", feature = "client-wasm"))]
use serde_json::Value;

#[cfg(any(feature = "client", feature = "client-rustls", feature = "client-wasm"))]
use crate::connection::DatabaseClient;
#[cfg(any(feature = "client", feature = "client-rustls", feature = "client-wasm"))]
use crate::query::executor::{extract_rows, flatten_rows};

/// One traversal step: an arrow, an edge table, and an optional depth.
#[derive(Debug, Clone, PartialEq)]
struct Hop {
    arrow: &'static str,
    edge: String,
    depth: Option<u32>,
}

/// Immutable fluent builder for graph traversal queries.
///
/// The builder accumulates traversal steps (`out` / `in` / `both`), an
/// optional target table, `WHERE` fragments, projected fields, `FETCH`
/// clauses, and a `LIMIT`. Call [`GraphQuery::to_surql`] to render the
/// SurrealQL, or [`GraphQuery::execute`] / [`GraphQuery::fetch_typed`] /
/// [`GraphQuery::count`] / [`GraphQuery::exists`] to dispatch against a
/// [`DatabaseClient`].
///
/// Names are checked when the query is rendered: the start must be a table
/// or record id, edges and the target table identifiers, and projected and
/// fetched fields field paths (or `*`). `WHERE` fragments are raw SurrealQL.
///
/// All chain methods take `self` by value; use `.clone()` to fork a
/// partially-built query.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct GraphQuery {
    start: String,
    path: Vec<Hop>,
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

    fn hop(mut self, arrow: &'static str, edge: &str, depth: Option<u32>) -> Self {
        self.path.push(Hop {
            arrow,
            edge: edge.to_owned(),
            depth,
        });
        self
    }

    /// Append an outgoing step through `edge`.
    ///
    /// Without a depth this is the single hop `->edge`, which selects the
    /// edge records. With `Some(n)` it is exactly `n` hops through `edge`
    /// to the records at the far end, spelled out as `->edge->?` per hop
    /// (the form SurrealDB 3 accepts, as in the sibling ports); `n` must be
    /// in `1..=32`.
    pub fn out(self, edge: impl AsRef<str>, depth: Option<u32>) -> Self {
        self.hop("->", edge.as_ref(), depth)
    }

    /// Append an incoming step (`<-edge`, see [`GraphQuery::out`] for the
    /// depth).
    ///
    /// Renamed from Python's `in_` to use Rust's raw-identifier syntax;
    /// semantics match `GraphQuery.in_` exactly.
    pub fn r#in(self, edge: impl AsRef<str>, depth: Option<u32>) -> Self {
        self.hop("<-", edge.as_ref(), depth)
    }

    /// Append a bidirectional step (`<->edge`, see [`GraphQuery::out`] for
    /// the depth).
    pub fn both(self, edge: impl AsRef<str>, depth: Option<u32>) -> Self {
        self.hop("<->", edge.as_ref(), depth)
    }

    /// Narrow the tail of the traversal to a specific target table: the
    /// records at the far end of the last step, reached in that step's
    /// direction (`.r#in("follows", None).to("user")` renders
    /// `<-follows<-user`).
    pub fn to(mut self, target: impl Into<String>) -> Self {
        self.target_table = Some(target.into());
        self
    }

    /// Append a `WHERE` condition (raw SurrealQL). Multiple calls are
    /// combined with `AND`.
    pub fn r#where(mut self, condition: impl Into<String>) -> Self {
        self.conditions.push(condition.into());
        self
    }

    /// Project the given fields (`SELECT <fields> FROM ...`). Each must be a
    /// field path or `*`. Repeated calls extend the projection list.
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

    /// Append field paths to the `FETCH` clause (e.g. `FETCH author, tags`).
    pub fn fetch<I, S>(mut self, refs: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.fetch.extend(refs.into_iter().map(Into::into));
        self
    }

    /// Render `<start><steps>[<target>]`, checking every name.
    fn render_source(&self) -> Result<String> {
        let Some(last) = self.path.last() else {
            return Err(SurqlError::Validation {
                reason: "At least one traversal step (out, in, both) is required".to_string(),
            });
        };
        let mut source = render_target(&self.start)?;
        for hop in &self.path {
            validate_identifier(&hop.edge, "edge table name")?;
            let Hop { arrow, edge, depth } = hop;
            match *depth {
                None => {
                    source.push_str(arrow);
                    source.push_str(edge);
                }
                Some(depth) => {
                    validate_depth(depth)?;
                    let step = format!("{arrow}{edge}{arrow}?");
                    for _ in 0..depth {
                        source.push_str(&step);
                    }
                }
            }
        }
        if let Some(target) = &self.target_table {
            validate_identifier(target, "target table name")?;
            // A depth step already ends on its records (`->?`): the target
            // replaces the wildcard. A single hop ends on the edge, so the
            // target is one more arrow in the same direction.
            if last.depth.is_some() {
                source.pop();
            } else {
                source.push_str(last.arrow);
            }
            source.push_str(target);
        }
        Ok(source)
    }

    fn render_where(&self) -> Option<String> {
        (!self.conditions.is_empty()).then(|| {
            let joined = self
                .conditions
                .iter()
                .map(|c| format!("({c})"))
                .collect::<Vec<_>>()
                .join(" AND ");
            format!("WHERE {joined}")
        })
    }

    /// Render the built query to SurrealQL.
    ///
    /// Returns a validation error when no traversal step has been added, or
    /// when a name or depth is not acceptable.
    pub fn to_surql(&self) -> Result<String> {
        let source = self.render_source()?;

        let fields_str = if self.fields.is_empty() {
            "*".to_string()
        } else {
            for field in self.fields.iter().filter(|f| f.as_str() != "*") {
                validate_field_path(field, "projected field")?;
            }
            self.fields.join(", ")
        };

        let mut parts = vec![format!("SELECT {fields_str} FROM {source}")];
        parts.extend(self.render_where());

        // SurrealQL takes LIMIT before FETCH; the reverse is a parse error.
        if let Some(n) = self.limit_value {
            parts.push(format!("LIMIT {n}"));
        }

        if !self.fetch.is_empty() {
            for field in &self.fetch {
                validate_field_path(field, "fetch field")?;
            }
            parts.push(format!("FETCH {}", self.fetch.join(", ")));
        }

        Ok(parts.join(" "))
    }

    /// Render a matching `SELECT count() FROM ... GROUP ALL` query.
    #[cfg(any(feature = "client", feature = "client-rustls", feature = "client-wasm"))]
    fn to_count_surql(&self) -> Result<String> {
        let source = self.render_source()?;
        let mut parts = vec![format!("SELECT count() FROM {source}")];
        parts.extend(self.render_where());
        parts.push("GROUP ALL".to_owned());
        Ok(parts.join(" "))
    }

    /// Execute the rendered query and return raw JSON rows.
    #[cfg(any(feature = "client", feature = "client-rustls", feature = "client-wasm"))]
    pub async fn execute(&self, client: &DatabaseClient) -> Result<Vec<Value>> {
        let surql = self.to_surql()?;
        let raw = client.query(&surql).await?;
        Ok(flatten_rows(&raw))
    }

    /// Execute the rendered query and deserialize each row into `T`.
    #[cfg(any(feature = "client", feature = "client-rustls", feature = "client-wasm"))]
    pub async fn fetch_typed<T: DeserializeOwned>(
        &self,
        client: &DatabaseClient,
    ) -> Result<Vec<T>> {
        let surql = self.to_surql()?;
        let raw = client.query(&surql).await?;
        extract_rows::<T>(&raw)
    }

    /// Count matching rows via `SELECT count() ... GROUP ALL`.
    #[cfg(any(feature = "client", feature = "client-rustls", feature = "client-wasm"))]
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
    #[cfg(any(feature = "client", feature = "client-rustls", feature = "client-wasm"))]
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
        assert_eq!(sql, "SELECT * FROM user:alice<-follows<-?<-follows<-?");
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
    fn depth_unrolls_into_repeated_hops() {
        let sql = GraphQuery::new("user:alice")
            .out("follows", Some(2))
            .to_surql()
            .unwrap();
        assert_eq!(sql, "SELECT * FROM user:alice->follows->?->follows->?");
        let sql = GraphQuery::new("user:alice")
            .out("follows", Some(2))
            .to("user")
            .to_surql()
            .unwrap();
        assert_eq!(sql, "SELECT * FROM user:alice->follows->?->follows->user");
        assert!(GraphQuery::new("user:alice")
            .out("follows", Some(0))
            .to_surql()
            .is_err());
    }

    #[test]
    fn to_follows_the_direction_of_the_last_hop() {
        let sql = GraphQuery::new("user:alice")
            .r#in("follows", None)
            .to("user")
            .to_surql()
            .unwrap();
        assert_eq!(sql, "SELECT * FROM user:alice<-follows<-user");
    }

    #[test]
    fn limit_renders_before_fetch() {
        let sql = GraphQuery::new("user:alice")
            .out("likes", None)
            .fetch(["out"])
            .limit(1)
            .unwrap()
            .to_surql()
            .unwrap();
        assert_eq!(sql, "SELECT * FROM user:alice->likes LIMIT 1 FETCH out");
    }

    #[test]
    fn names_are_validated() {
        let hostile = "follows; DELETE user; --";
        assert!(GraphQuery::new("user:alice")
            .out(hostile, None)
            .to_surql()
            .is_err());
        assert!(GraphQuery::new("user:alice")
            .out("follows", None)
            .to(hostile)
            .to_surql()
            .is_err());
        assert!(GraphQuery::new("user:alice")
            .out("follows", None)
            .select([hostile])
            .to_surql()
            .is_err());
        assert!(GraphQuery::new("user:alice")
            .out("follows", None)
            .fetch([hostile])
            .to_surql()
            .is_err());
        assert_eq!(
            GraphQuery::new("user:a; DELETE user")
                .out("follows", None)
                .to_surql()
                .unwrap(),
            "SELECT * FROM user:⟨a; DELETE user⟩->follows"
        );
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
    fn depth_is_bounded() {
        assert!(GraphQuery::new("user:alice")
            .out("follows", Some(32))
            .to_surql()
            .is_ok());
        assert!(GraphQuery::new("user:alice")
            .out("follows", Some(33))
            .to_surql()
            .is_err());
    }

    #[test]
    fn mixed_steps_render_in_order() {
        let sql = GraphQuery::new("user:alice")
            .out("wrote", None)
            .both("tagged", Some(1))
            .to("topic")
            .to_surql()
            .unwrap();
        assert_eq!(sql, "SELECT * FROM user:alice->wrote<->tagged<->topic");
    }

    #[cfg(any(feature = "client", feature = "client-rustls", feature = "client-wasm"))]
    #[test]
    fn count_surql_includes_group_all() {
        let sql = GraphQuery::new("user:alice")
            .out("follows", None)
            .to_count_surql()
            .unwrap();
        assert_eq!(sql, "SELECT count() FROM user:alice->follows GROUP ALL");
    }

    #[cfg(any(feature = "client", feature = "client-rustls", feature = "client-wasm"))]
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
