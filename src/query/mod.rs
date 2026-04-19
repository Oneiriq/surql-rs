//! Query construction and result handling.
//!
//! Port of `surql/query/` from `oneiriq-surql` (Python). Currently exposes:
//!
//! - [`builder`]: immutable [`Query`] builder.
//! - [`helpers`]: free functions that return preconfigured [`Query`]s.
//! - [`hints`]: query optimization hints ([`IndexHint`],
//!   [`ParallelHint`], [`TimeoutHint`],
//!   [`FetchHint`], [`ExplainHint`]).
//! - [`results`]: typed result wrappers and extraction helpers.
//! - [`batch`]: pure [`build_upsert_query`] /
//!   [`build_relate_query`] renderers plus async
//!   `*_many` helpers *(feature `client`)*.
//! - [`graph_query`]: fluent [`GraphQuery`] builder.
//! - [`executor`] *(feature `client`)*: async execution on top of
//!   [`DatabaseClient`](crate::DatabaseClient).
//! - [`crud`] *(feature `client`)*: JSON-in / JSON-out record CRUD helpers.
//! - [`typed`] *(feature `client`)*: serde-round-trip CRUD helpers.
//! - [`graph`] *(feature `client`)*: graph traversal + relation helpers.

pub mod batch;
pub mod builder;
#[cfg(feature = "client")]
pub mod crud;
#[cfg(feature = "client")]
pub mod executor;
pub mod expressions;
#[cfg(feature = "client")]
pub mod graph;
pub mod graph_query;
pub mod helpers;
pub mod hints;
pub mod results;
#[cfg(feature = "client")]
pub mod typed;

pub use batch::{build_relate_query, build_upsert_query, RelateItem};
pub use builder::{Operation, OrderField, Query, WhereCondition};
pub use expressions::{
    abs_, array_contains, array_length, as_, avg, cast, ceil, concat, count, count_all, count_if,
    field, floor, func, lower, math_abs, math_ceil, math_floor, math_max, math_mean, math_min,
    math_round, math_sum, max_, min_, raw, round_, string_concat, string_len, string_lower,
    string_upper, sum_, time_format, time_now, type_is, upper, value, ExprArg, Expression,
    ExpressionKind,
};
pub use graph_query::GraphQuery;
pub use helpers::{
    delete, from_table, insert, limit, offset, order_by, relate, select, similarity_search_query,
    update, upsert, vector_search_query, where_, DataMap, ReturnFormat, VectorDistanceType,
};
pub use hints::{
    merge_hints, render_hints, validate_hint, ExplainHint, FetchHint, FetchStrategy, HintRenderer,
    HintType, IndexHint, ParallelHint, QueryHint, TimeoutHint,
};
pub use results::{
    aggregate, count_result, extract_many, extract_one, extract_result, extract_scalar, has_result,
    has_results, paginated, record, records, success, AggregateResult, CountResult, ListResult,
    PageInfo, PaginatedResult, QueryResult, RecordResult,
};

// Aggregation entrypoint (sub-feature 4). Gated on the `client` feature
// because it needs a live [`DatabaseClient`] to dispatch the rendered query.
#[cfg(feature = "client")]
pub use crud::{aggregate_records, build_aggregate_query, AggregateOpts};
