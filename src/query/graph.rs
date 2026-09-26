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
//! stay hand-composed because [`Query::relate`] inlines its payload as a
//! literal, whereas `create_relation` binds `CONTENT $data` as a variable —
//! matching the discipline in
//! [`create_record`](crate::query::crud::create_record).
//!
//! Every record argument is parsed and re-rendered as a record id (its key
//! escaped), and every edge and target table must be an identifier. The
//! `path` of [`traverse`] / [`traverse_raw`] is raw SurrealQL by design.
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

use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::connection::DatabaseClient;
use crate::error::{Result, SurqlError};

use super::builder::{Condition, Query};
use super::executor::{extract_rows, flatten_rows};
use super::validate::{
    parse_record, render_target, validate_depth, validate_identifier, MAX_GRAPH_DEPTH,
};

/// Append each entry of `conditions` to `query` as a `WHERE` clause.
///
/// Entries combine with `AND` (the builder's own semantics). `None` and an
/// empty slice are both no-ops, which is what keeps the emitted SurrealQL
/// unchanged for callers that do not filter.
fn apply_conditions(query: Query, conditions: Option<&[Condition]>) -> Query {
    conditions.unwrap_or(&[]).iter().fold(query, Query::where_)
}

/// Render the path of [`traverse_with_depth`]: `depth` hops through
/// `edge_table` ending on `target_table`.
///
/// One hop (`None` or `Some(1)`) is `<arrow><edge><arrow><target>`. Deeper
/// paths spell out each hop, with the `?` wildcard for the records in
/// between: `Some(2)` renders `->follows->?->follows->user`.
fn depth_path(
    edge_table: &str,
    target_table: &str,
    direction: Direction,
    depth: Option<u32>,
) -> Result<String> {
    validate_identifier(edge_table, "edge table name")?;
    validate_identifier(target_table, "target table name")?;
    let depth = depth.unwrap_or(1);
    validate_depth(depth)?;
    let arrow = direction.arrow();
    let hop = format!("{arrow}{edge_table}{arrow}?");
    let hops: String = (1..depth).map(|_| hop.as_str()).collect();
    Ok(format!("{hops}{arrow}{edge_table}{arrow}{target_table}"))
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
    validate_identifier(edge_table, "edge table name")?;
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
    validate_identifier(edge_table, "edge table name")?;
    validate_depth(depth)?;
    let to_record = parse_record(to_record)?;
    let hop = format!("->{edge_table}->?");
    let path: String = (0..depth).map(|_| hop.as_str()).collect();

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

/// Traverse exactly `depth` hops through `edge_table`, ending on
/// `target_table` records.
///
/// `None` is a single hop (`->edge->target`). `Some(n)` spells out `n`
/// hops, with the `?` wildcard for the records in between
/// (`->follows->?->follows->user` for `Some(2)`); `n` must be in `1..=32`.
/// Delegates to [`traverse`].
pub async fn traverse_with_depth<T: DeserializeOwned>(
    client: &DatabaseClient,
    start: &str,
    edge_table: &str,
    target_table: &str,
    direction: Direction,
    depth: Option<u32>,
    conditions: Option<&[Condition]>,
) -> Result<Vec<T>> {
    let path = depth_path(edge_table, target_table, direction, depth)?;
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

/// Render `<from>-><edge>-><to>` for the relation helpers.
fn relation_path(edge_table: &str, from_record: &str, to_record: &str) -> Result<String> {
    validate_identifier(edge_table, "edge table name")?;
    let from = render_target(from_record)?;
    let to = render_target(to_record)?;
    Ok(format!("{from}->{edge_table}->{to}"))
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
    let relation = relation_path(edge_table, from_record, to_record)?;
    let surql = if data.is_some() {
        format!("RELATE {relation} CONTENT $data")
    } else {
        format!("RELATE {relation}")
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
    let surql = format!(
        "DELETE {}",
        relation_path(edge_table, from_record, to_record)?
    );
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
    validate_identifier(edge_table, "edge table name")?;
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
    validate_identifier(edge_table, "edge table name")?;
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
    validate_identifier(edge_table, "edge table name")?;
    validate_identifier(target_table, "target table name")?;
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
/// short-circuits without issuing queries; a `max_depth` above 32 is a
/// validation error (each depth is one query spelling out every hop).
pub async fn shortest_path(
    client: &DatabaseClient,
    from_record: &str,
    to_record: &str,
    edge_table: &str,
    max_depth: u32,
    conditions: Option<&[Condition]>,
) -> Result<Vec<Value>> {
    if max_depth > MAX_GRAPH_DEPTH {
        return Err(SurqlError::Validation {
            reason: format!("shortest_path max_depth must be at most {MAX_GRAPH_DEPTH}"),
        });
    }
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
            depth_path("follows", "user", Direction::Out, None).unwrap(),
            "->follows->user"
        );
        assert_eq!(
            depth_path("follows", "user", Direction::In, None).unwrap(),
            "<-follows<-user"
        );
        assert_eq!(
            depth_path("follows", "user", Direction::Both, None).unwrap(),
            "<->follows<->user"
        );
    }

    #[test]
    fn depth_path_renders_depth_suffix() {
        assert_eq!(
            depth_path("follows", "user", Direction::Out, Some(2)).unwrap(),
            "->follows->?->follows->user"
        );
        assert_eq!(
            depth_path("follows", "user", Direction::In, Some(1)).unwrap(),
            "<-follows<-user"
        );
    }

    #[test]
    fn depth_path_validates_names_and_depth() {
        let out = Direction::Out;
        assert!(depth_path("follows", "user", out, Some(0)).is_err());
        assert!(depth_path("follows", "user", out, Some(MAX_GRAPH_DEPTH + 1)).is_err());
        assert!(depth_path("follows->user; DELETE", "user", out, None).is_err());
        assert!(depth_path("follows", "user WHERE true", out, None).is_err());
    }

    #[test]
    fn relation_path_escapes_records() {
        assert_eq!(
            relation_path("likes", "user:a", "post:b; DELETE post").unwrap(),
            "user:a->likes->post:⟨b; DELETE post⟩"
        );
        assert!(relation_path("likes; DELETE", "user:a", "post:b").is_err());
        assert!(relation_path("likes", "user; DELETE user", "post:b").is_err());
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
    fn shortest_path_target_cannot_extend_the_where_clause() {
        let sql =
            shortest_path_surql("user:alice", "user:bob OR true", "follows", 1, None).unwrap();
        assert_eq!(
            sql,
            "SELECT * FROM user:alice->follows->? WHERE (id = user:⟨bob OR true⟩) LIMIT 1"
        );
        assert!(
            shortest_path_surql("user:alice", "user:bob", "follows; DELETE user", 1, None).is_err()
        );
    }

    #[test]
    fn edge_names_are_validated() {
        assert!(
            count_related_surql("user:alice", "likes GROUP ALL; DELETE user", Direction::Out)
                .is_err()
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
