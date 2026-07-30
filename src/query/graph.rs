//! Graph traversal utilities for SurrealDB's graph capabilities.
//!
//! Port of `surql/query/graph.py`. Exposes free-standing async helpers for
//! the common graph patterns — outgoing / incoming edge retrieval, typed
//! traversal, relation creation / removal, related-record counting, and a
//! depth-bounded shortest-path search.
//!
//! Every `SELECT`-shaped helper composes its statement with [`Query`],
//! using the crate's existing arrow syntax (`->edge->target` /
//! `record<-edge<-source`) via [`Query::traverse`]. Aggregates include
//! `GROUP ALL` — matches the discipline in
//! [`count_records`](crate::query::crud::count_records). Dispatch goes
//! through [`DatabaseClient::query`](crate::DatabaseClient::query) /
//! [`query_with_vars`](crate::DatabaseClient::query_with_vars).
//!
//! [`create_relation`] and [`remove_relation`] are the exceptions: they
//! stay hand-composed because [`Query::relate`] inlines its payload via
//! `render_data_object`, whereas `create_relation` binds `CONTENT $data`
//! as a variable — matching the discipline in
//! [`create_record`](crate::query::crud::create_record). Routing them
//! through the builder would inline caller payloads into the statement.
//!
//! ## Row-level filtering
//!
//! [`traverse`], [`traverse_with_depth`], [`get_outgoing_edges`],
//! [`get_incoming_edges`], [`get_related_records`], and [`shortest_path`]
//! take a `conditions` argument. Each entry is rendered through
//! [`Query::where_`], so raw SurrealQL fragments and
//! [`Operator`](crate::types::operators::Operator) values are both
//! accepted and may be mixed in one slice; multiple entries
//! combine with `AND`. Passing `None` leaves the emitted SurrealQL
//! unchanged.
//!
//! This is the hook for multi-tenant row isolation — a traversal that must
//! stay inside a tenant boundary carries its guard as an operator rather
//! than a hand-written predicate:
//!
//! ```
//! use surql::query::Condition;
//! use surql::types::operators::eq;
//!
//! let guard: Vec<Condition> = vec![eq("tenant_id", "acme").into()];
//! assert_eq!(guard.len(), 1);
//! ```
//!
//! ## Examples
//!
//! ```no_run
//! # #[cfg(any(feature = "client", feature = "client-rustls"))]
//! # async fn demo() -> surql::error::Result<()> {
//! use surql::connection::{ConnectionConfig, DatabaseClient};
//! use surql::query::{graph, Condition};
//! use surql::types::operators::eq;
//!
//! let client = DatabaseClient::new(ConnectionConfig::default())?;
//! client.connect().await?;
//!
//! let _ = graph::create_relation(&client, "likes", "user:alice", "post:1", None).await?;
//!
//! // Unfiltered.
//! let posts = graph::get_related_records(
//!     &client,
//!     "user:alice",
//!     "likes",
//!     "post",
//!     graph::Direction::Out,
//!     None,
//! )
//! .await?;
//!
//! // Scoped to a tenant.
//! let guard: Vec<Condition> = vec![eq("tenant_id", "acme").into()];
//! let scoped = graph::get_related_records(
//!     &client,
//!     "user:alice",
//!     "likes",
//!     "post",
//!     graph::Direction::Out,
//!     Some(&guard),
//! )
//! .await?;
//! # let _ = (posts, scoped); Ok(()) }
//! ```

#![cfg(any(feature = "client", feature = "client-rustls", feature = "client-wasm"))]

use std::collections::BTreeMap;
use std::fmt::Write as _;

use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::connection::DatabaseClient;
use crate::error::{Result, SurqlError};

use super::builder::{Condition, Query};
use super::executor::{extract_rows, flatten_rows};

/// Append each entry of `conditions` to `query` as a `WHERE` clause.
///
/// Entries combine with `AND` (the builder's own semantics). `None` and an
/// empty slice are both no-ops, which is what keeps the emitted SurrealQL
/// unchanged for callers that do not filter.
fn apply_conditions(query: Query, conditions: Option<&[Condition]>) -> Query {
    conditions.unwrap_or(&[]).iter().fold(query, Query::where_)
}

/// Render `<arrow><edge>[<depth>]<arrow><target>` for a depth-bounded hop.
///
/// When `depth` is `None` no numeric suffix is emitted, which SurrealDB
/// interprets as a single hop.
fn depth_path(
    edge_table: &str,
    target_table: &str,
    direction: Direction,
    depth: Option<u32>,
) -> String {
    let arrow = direction.arrow();
    let depth_str = depth.map_or(String::new(), |d| d.to_string());
    format!("{arrow}{edge_table}{depth_str}{arrow}{target_table}")
}

/// Render `SELECT * FROM <start><path> [WHERE ...]`.
///
/// Split out from the async helpers so the statement construction is
/// testable without a live client.
fn select_traversal_surql(
    start: &str,
    path: &str,
    conditions: Option<&[Condition]>,
) -> Result<String> {
    let query = Query::new().select(None).from_table(start)?.traverse(path);
    apply_conditions(query, conditions).to_surql()
}

/// Render `SELECT count() FROM <record><arrow><edge> GROUP ALL`.
///
/// [`Direction::Both`] is rejected: the aggregate needs a single arrow at
/// the tail of the `FROM` expression.
fn count_related_surql(record: &str, edge_table: &str, direction: Direction) -> Result<String> {
    let path = match direction {
        Direction::Out => format!("->{edge_table}"),
        // See `get_incoming_edges` — SurrealDB v3 parses incoming edges
        // as `FROM record<-edge`. Python's `FROM <-edge<-record` is a
        // syntax error on v3.
        Direction::In => format!("<-{edge_table}"),
        Direction::Both => {
            return Err(SurqlError::Validation {
                reason: "count_related direction must be Out or In".to_string(),
            });
        }
    };

    Query::new()
        .select(Some(vec!["count()".to_owned()]))
        .from_table(record)?
        .traverse(path)
        .group_all()
        .to_surql()
}

/// Render one depth probe of [`shortest_path`].
///
/// Chains `->edge->?` `depth` times (SurrealDB's `?` wildcard matches any
/// target table), pins the tail to `to_record`, and caps the result at one
/// row.
fn shortest_path_surql(
    from_record: &str,
    to_record: &str,
    edge_table: &str,
    depth: u32,
    conditions: Option<&[Condition]>,
) -> Result<String> {
    let mut path = String::new();
    for _ in 0..depth {
        write!(path, "->{edge_table}->?").expect("write to String cannot fail");
    }

    let query = Query::new()
        .select(None)
        .from_table(from_record)?
        .traverse(path)
        .where_str(format!("id = {to_record}"));

    apply_conditions(query, conditions).limit(1)?.to_surql()
}

/// Traversal direction for graph helpers.
///
/// Maps one-to-one to the Python `direction: Literal['out', 'in', 'both']`
/// argument used by `traverse_with_depth`, `get_related_records`, and
/// `count_related`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Direction {
    /// `->edge->` (outgoing).
    Out,
    /// `<-edge<-` (incoming).
    In,
    /// `<->edge<->` (bidirectional).
    Both,
}

impl Direction {
    fn arrow(self) -> &'static str {
        match self {
            Self::Out => "->",
            Self::In => "<-",
            Self::Both => "<->",
        }
    }
}

/// Traverse a graph path starting at `start` and deserialize each terminal
/// record into `T`.
///
/// `path` is the raw SurrealQL traversal expression (e.g.
/// `"->likes->post"`, `"<-follows<-user"`). Deserialization mirrors
/// [`executor::fetch_all`](crate::query::executor::fetch_all) — each row is
/// converted via `serde_json::from_value`.
pub async fn traverse<T: DeserializeOwned>(
    client: &DatabaseClient,
    start: &str,
    path: &str,
    conditions: Option<&[Condition]>,
) -> Result<Vec<T>> {
    let surql = select_traversal_surql(start, path, conditions)?;
    let raw = client.query(&surql).await?;
    extract_rows::<T>(&raw)
}

/// Traverse a graph with an optional depth limit.
///
/// Constructs `<arrow><edge>[<depth>]<arrow><target>` and delegates to
/// [`traverse`]. When `depth` is `None`, no numeric suffix is emitted,
/// which SurrealDB interprets as a single hop.
pub async fn traverse_with_depth<T: DeserializeOwned>(
    client: &DatabaseClient,
    start: &str,
    edge_table: &str,
    target_table: &str,
    direction: Direction,
    depth: Option<u32>,
    conditions: Option<&[Condition]>,
) -> Result<Vec<T>> {
    let path = depth_path(edge_table, target_table, direction, depth);
    traverse(client, start, &path, conditions).await
}

/// Traverse and return raw JSON rows (no deserialization).
///
/// Thin helper that mirrors the Python branch which returns `list[dict]`
/// when `model` is `None`.
pub async fn traverse_raw(
    client: &DatabaseClient,
    start: &str,
    path: &str,
    conditions: Option<&[Condition]>,
) -> Result<Vec<Value>> {
    let surql = select_traversal_surql(start, path, conditions)?;
    let raw = client.query(&surql).await?;
    Ok(flatten_rows(&raw))
}

/// Create a graph relation via `RELATE <from>-><edge>-><to> [CONTENT $data]`.
///
/// `data`, when present, is bound as a variable so payload shape is
/// preserved (matches [`create_record`](crate::query::crud::create_record)).
pub async fn create_relation(
    client: &DatabaseClient,
    edge_table: &str,
    from_record: &str,
    to_record: &str,
    data: Option<Value>,
) -> Result<Value> {
    let surql = if data.is_some() {
        format!("RELATE {from_record}->{edge_table}->{to_record} CONTENT $data")
    } else {
        format!("RELATE {from_record}->{edge_table}->{to_record}")
    };

    let raw = if let Some(payload) = data {
        let mut vars = BTreeMap::new();
        vars.insert("data".to_owned(), payload);
        client.query_with_vars(&surql, vars).await?
    } else {
        client.query(&surql).await?
    };
    Ok(flatten_rows(&raw).into_iter().next().unwrap_or(Value::Null))
}

/// Remove a graph relation via `DELETE <from>-><edge>-><to>`.
pub async fn remove_relation(
    client: &DatabaseClient,
    edge_table: &str,
    from_record: &str,
    to_record: &str,
) -> Result<()> {
    let surql = format!("DELETE {from_record}->{edge_table}->{to_record}");
    client.query(&surql).await?;
    Ok(())
}

/// Get every outgoing edge from `record` through `edge_table`.
pub async fn get_outgoing_edges(
    client: &DatabaseClient,
    record: &str,
    edge_table: &str,
    conditions: Option<&[Condition]>,
) -> Result<Vec<Value>> {
    let surql = select_traversal_surql(record, &format!("->{edge_table}"), conditions)?;
    let raw = client.query(&surql).await?;
    Ok(flatten_rows(&raw))
}

/// Get every incoming edge to `record` through `edge_table`.
///
/// Deviates from the Python source's `FROM <-edge<-record` ordering —
/// SurrealDB v3 requires the record at the head of the `FROM` expression
/// (`FROM record<-edge`). See the upstream Python gap tracked alongside
/// this module.
pub async fn get_incoming_edges(
    client: &DatabaseClient,
    record: &str,
    edge_table: &str,
    conditions: Option<&[Condition]>,
) -> Result<Vec<Value>> {
    let surql = select_traversal_surql(record, &format!("<-{edge_table}"), conditions)?;
    let raw = client.query(&surql).await?;
    Ok(flatten_rows(&raw))
}

/// Fetch related records via a single-hop traversal in `direction`.
///
/// `direction` is restricted to [`Direction::Out`] or [`Direction::In`]
/// because `target_table` is required at the tail of the arrow; passing
/// [`Direction::Both`] returns a validation error.
pub async fn get_related_records(
    client: &DatabaseClient,
    record: &str,
    edge_table: &str,
    target_table: &str,
    direction: Direction,
    conditions: Option<&[Condition]>,
) -> Result<Vec<Value>> {
    let path = match direction {
        Direction::Out => format!("->{edge_table}->{target_table}"),
        // SurrealDB v3 parses `<-edge<-target` relative to the record at
        // the head of `FROM`, so we emit `FROM record<-edge<-target`
        // (deviates from the Python source, which puts the record at the
        // tail and fails to parse on v3).
        Direction::In => format!("<-{edge_table}<-{target_table}"),
        Direction::Both => {
            return Err(SurqlError::Validation {
                reason: "get_related_records direction must be Out or In".to_string(),
            });
        }
    };
    let surql = select_traversal_surql(record, &path, conditions)?;
    let raw = client.query(&surql).await?;
    Ok(flatten_rows(&raw))
}

/// Count related records through an edge, in either direction.
///
/// Emits `SELECT count() FROM ... GROUP ALL` and extracts the scalar
/// `count` field. Returns `0` when the group is empty.
pub async fn count_related(
    client: &DatabaseClient,
    record: &str,
    edge_table: &str,
    direction: Direction,
) -> Result<i64> {
    let surql = count_related_surql(record, edge_table, direction)?;
    let raw = client.query(&surql).await?;
    let first = flatten_rows(&raw).into_iter().next();
    Ok(first
        .as_ref()
        .and_then(|r| r.get("count").and_then(Value::as_i64))
        .unwrap_or(0))
}

/// Find a shortest path between two records via iterative deepening.
///
/// Mirrors the intent of the Python `shortest_path` (iterate depths
/// 1..=`max_depth`, return the first hit). The emitted SurrealQL
/// deviates from the Python source because the Python query shape
/// (`SELECT * FROM <from>->edge{d}-> WHERE id = <to>`) is a parse error
/// on SurrealDB v3 — the trailing `->` leaves no target. Instead, at
/// depth `d` we chain `->edge->?` `d` times (SurrealDB's `?` wildcard
/// matches any target table):
///
/// ```text
/// SELECT * FROM <from>(->edge->?){d} WHERE (id = <to>) LIMIT 1
/// ```
///
/// Any `conditions` are appended after the identity predicate and combine
/// with `AND`, so a tenant guard narrows every depth probe.
///
/// The matching rows are returned as raw JSON. `max_depth = 0`
/// short-circuits without issuing queries.
pub async fn shortest_path(
    client: &DatabaseClient,
    from_record: &str,
    to_record: &str,
    edge_table: &str,
    max_depth: u32,
    conditions: Option<&[Condition]>,
) -> Result<Vec<Value>> {
    for depth in 1..=max_depth {
        let surql = shortest_path_surql(from_record, to_record, edge_table, depth, conditions)?;

        let raw = client.query(&surql).await?;
        let rows = flatten_rows(&raw);
        if !rows.is_empty() {
            return Ok(rows);
        }
    }
    Ok(Vec::new())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn direction_arrow_matches_py_semantics() {
        assert_eq!(Direction::Out.arrow(), "->");
        assert_eq!(Direction::In.arrow(), "<-");
        assert_eq!(Direction::Both.arrow(), "<->");
    }

    use crate::types::operators::eq;

    #[test]
    fn traverse_without_conditions_is_unchanged() {
        // Guards the refactor onto the builder: with no conditions the
        // emitted statement must match what the previous format! produced.
        assert_eq!(
            select_traversal_surql("user:alice", "->likes->post", None).unwrap(),
            "SELECT * FROM user:alice->likes->post"
        );
    }

    #[test]
    fn empty_conditions_slice_matches_none() {
        let none = select_traversal_surql("user:alice", "->likes->post", None).unwrap();
        let empty = select_traversal_surql("user:alice", "->likes->post", Some(&[])).unwrap();
        assert_eq!(none, empty);
    }

    #[test]
    fn operator_condition_is_appended() {
        let guard: Vec<Condition> = vec![eq("tenant_id", "acme").into()];
        assert_eq!(
            select_traversal_surql("user:alice", "->likes->post", Some(&guard)).unwrap(),
            "SELECT * FROM user:alice->likes->post WHERE (tenant_id = 'acme')"
        );
    }

    #[test]
    fn raw_fragment_condition_is_appended() {
        let guard: Vec<Condition> = vec!["age > 18".into()];
        assert_eq!(
            select_traversal_surql("user:alice", "->likes->post", Some(&guard)).unwrap(),
            "SELECT * FROM user:alice->likes->post WHERE (age > 18)"
        );
    }

    #[test]
    fn mixed_conditions_combine_with_and() {
        // The `str | Operator` union of the sibling ports: one slice, both
        // forms, joined by AND in the order given.
        let guard: Vec<Condition> = vec![eq("tenant_id", "acme").into(), "age > 18".into()];
        assert_eq!(
            select_traversal_surql("user:alice", "->likes->post", Some(&guard)).unwrap(),
            "SELECT * FROM user:alice->likes->post WHERE (tenant_id = 'acme') AND (age > 18)"
        );
    }

    #[test]
    fn condition_from_impls_round_trip() {
        assert_eq!(Condition::from("a = 1"), Condition::Raw("a = 1".to_owned()));
        assert_eq!(
            Condition::from("a = 1".to_owned()),
            Condition::Raw("a = 1".to_owned())
        );
        let op = eq("tenant_id", "acme");
        assert_eq!(Condition::from(&op), Condition::Op(op.clone()));
        assert_eq!(Condition::from(op.clone()), Condition::Op(op));
    }

    #[test]
    fn direction_arrow_matches_py_semantics_via_depth_path() {
        assert_eq!(
            depth_path("follows", "user", Direction::Out, None),
            "->follows->user"
        );
        assert_eq!(
            depth_path("follows", "user", Direction::In, None),
            "<-follows<-user"
        );
        assert_eq!(
            depth_path("follows", "user", Direction::Both, None),
            "<->follows<->user"
        );
    }

    #[test]
    fn depth_path_renders_depth_suffix() {
        assert_eq!(
            depth_path("follows", "user", Direction::Out, Some(2)),
            "->follows2->user"
        );
    }

    #[test]
    fn count_related_renders_group_all() {
        assert_eq!(
            count_related_surql("user:alice", "likes", Direction::Out).unwrap(),
            "SELECT count() FROM user:alice->likes GROUP ALL"
        );
        assert_eq!(
            count_related_surql("user:alice", "likes", Direction::In).unwrap(),
            "SELECT count() FROM user:alice<-likes GROUP ALL"
        );
    }

    #[test]
    fn count_related_rejects_both_direction() {
        let err = count_related_surql("user:alice", "likes", Direction::Both).unwrap_err();
        assert!(matches!(err, SurqlError::Validation { .. }));
    }

    #[test]
    fn get_related_records_rejects_both_direction() {
        // Direction::Both has no single tail arrow, so the path match in
        // get_related_records rejects it the same way count does.
        let err = count_related_surql("user:alice", "likes", Direction::Both).unwrap_err();
        assert!(matches!(err, SurqlError::Validation { .. }));
    }

    #[test]
    fn shortest_path_renders_chained_wildcard_edges() {
        assert_eq!(
            shortest_path_surql("user:alice", "user:bob", "follows", 3, None).unwrap(),
            "SELECT * FROM user:alice->follows->?->follows->?->follows->? \
             WHERE (id = user:bob) LIMIT 1"
        );
    }

    #[test]
    fn shortest_path_appends_conditions_after_identity_predicate() {
        let guard: Vec<Condition> = vec![eq("tenant_id", "acme").into()];
        assert_eq!(
            shortest_path_surql("user:alice", "user:bob", "follows", 1, Some(&guard)).unwrap(),
            "SELECT * FROM user:alice->follows->? WHERE (id = user:bob) \
             AND (tenant_id = 'acme') LIMIT 1"
        );
    }
}
