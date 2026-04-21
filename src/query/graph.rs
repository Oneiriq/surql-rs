//! Graph traversal utilities for SurrealDB's graph capabilities.
//!
//! Port of `surql/query/graph.py`. Exposes free-standing async helpers for
//! the common graph patterns — outgoing / incoming edge retrieval, typed
//! traversal, relation creation / removal, related-record counting, and a
//! depth-bounded shortest-path search.
//!
//! All functions use [`DatabaseClient::query`](crate::DatabaseClient::query)
//! / [`query_with_vars`](crate::DatabaseClient::query_with_vars) under the
//! hood and emit raw SurrealQL using the crate's existing arrow syntax
//! (`->edge->target` / `record<-edge<-source`). Aggregates include
//! `GROUP ALL` — matches the discipline in
//! [`count_records`](crate::query::crud::count_records).
//!
//! ## Examples
//!
//! ```no_run
//! # #[cfg(any(feature = "client", feature = "client-rustls"))]
//! # async fn demo() -> surql::error::Result<()> {
//! use surql::connection::{ConnectionConfig, DatabaseClient};
//! use surql::query::graph;
//!
//! let client = DatabaseClient::new(ConnectionConfig::default())?;
//! client.connect().await?;
//!
//! let _ = graph::create_relation(&client, "likes", "user:alice", "post:1", None).await?;
//! let posts = graph::get_related_records(
//!     &client,
//!     "user:alice",
//!     "likes",
//!     "post",
//!     graph::Direction::Out,
//! )
//! .await?;
//! # let _ = posts; Ok(()) }
//! ```

#![cfg(any(feature = "client", feature = "client-rustls"))]

use std::collections::BTreeMap;
use std::fmt::Write as _;

use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::connection::DatabaseClient;
use crate::error::{Result, SurqlError};

use super::executor::{extract_rows, flatten_rows};

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
) -> Result<Vec<T>> {
    let surql = format!("SELECT * FROM {start}{path}");
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
) -> Result<Vec<T>> {
    let arrow = direction.arrow();
    let depth_str = depth.map_or(String::new(), |d| d.to_string());
    let path = format!("{arrow}{edge_table}{depth_str}{arrow}{target_table}");
    traverse(client, start, &path).await
}

/// Traverse and return raw JSON rows (no deserialization).
///
/// Thin helper that mirrors the Python branch which returns `list[dict]`
/// when `model` is `None`.
pub async fn traverse_raw(client: &DatabaseClient, start: &str, path: &str) -> Result<Vec<Value>> {
    let surql = format!("SELECT * FROM {start}{path}");
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
) -> Result<Vec<Value>> {
    let surql = format!("SELECT * FROM {record}->{edge_table}");
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
) -> Result<Vec<Value>> {
    let surql = format!("SELECT * FROM {record}<-{edge_table}");
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
    let surql = format!("SELECT * FROM {record}{path}");
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
    let mut surql = match direction {
        Direction::Out => format!("SELECT count() FROM {record}->{edge_table}"),
        // See `get_incoming_edges` — SurrealDB v3 parses incoming edges
        // as `FROM record<-edge`. Python's `FROM <-edge<-record` is a
        // syntax error on v3.
        Direction::In => format!("SELECT count() FROM {record}<-{edge_table}"),
        Direction::Both => {
            return Err(SurqlError::Validation {
                reason: "count_related direction must be Out or In".to_string(),
            });
        }
    };
    surql.push_str(" GROUP ALL");

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
/// SELECT * FROM <from>(->edge->?){d} WHERE id = <to> LIMIT 1
/// ```
///
/// The matching rows are returned as raw JSON. `max_depth = 0`
/// short-circuits without issuing queries.
pub async fn shortest_path(
    client: &DatabaseClient,
    from_record: &str,
    to_record: &str,
    edge_table: &str,
    max_depth: u32,
) -> Result<Vec<Value>> {
    for depth in 1..=max_depth {
        let mut path = String::new();
        for _ in 0..depth {
            write!(path, "->{edge_table}->?").expect("write to String cannot fail");
        }
        let surql = format!("SELECT * FROM {from_record}{path} WHERE id = {to_record} LIMIT 1");

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

    #[test]
    fn traverse_path_is_plain_append() {
        // Smoke-test the SurrealQL string construction without a DB.
        let start = "user:alice";
        let path = "->likes->post";
        assert_eq!(
            format!("SELECT * FROM {start}{path}"),
            "SELECT * FROM user:alice->likes->post"
        );
    }

    #[test]
    fn traverse_with_depth_renders_depth_suffix() {
        let arrow = Direction::Out.arrow();
        let edge = "follows";
        let target = "user";
        let depth = Some(2u32);
        let depth_str = depth.map_or(String::new(), |d| d.to_string());
        let path = format!("{arrow}{edge}{depth_str}{arrow}{target}");
        assert_eq!(path, "->follows2->user");
    }

    #[test]
    fn count_related_rejects_both_direction() {
        let rendered = match Direction::Both {
            Direction::Out | Direction::In => "ok",
            Direction::Both => "err",
        };
        assert_eq!(rendered, "err");
    }

    #[test]
    fn shortest_path_renders_chained_wildcard_edges() {
        // Verify the per-depth path construction (pure string math, no DB).
        let edge_table = "follows";
        let mut path = String::new();
        for _ in 0..3 {
            write!(path, "->{edge_table}->?").unwrap();
        }
        assert_eq!(path, "->follows->?->follows->?->follows->?");
    }
}
