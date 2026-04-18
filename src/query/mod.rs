//! Query construction and result handling.
//!
//! Port of `surql/query/` from `oneiriq-surql` (Python). Currently exposes:
//!
//! - [`builder`]: immutable [`Query`](builder::Query) builder.
//! - [`helpers`]: free functions that return preconfigured [`Query`](builder::Query)s.
//! - [`hints`]: query optimization hints ([`IndexHint`](hints::IndexHint),
//!   [`ParallelHint`](hints::ParallelHint), [`TimeoutHint`](hints::TimeoutHint),
//!   [`FetchHint`](hints::FetchHint), [`ExplainHint`](hints::ExplainHint)).
//! - [`results`]: typed result wrappers and extraction helpers.
//!
//! Subsequent increments add typed/batch/graph CRUD, and the async executor.

pub mod builder;
pub mod expressions;
pub mod helpers;
pub mod hints;
pub mod results;

pub use builder::{Operation, OrderField, Query, WhereCondition};
pub use expressions::{
    abs_, array_contains, array_length, as_, avg, cast, ceil, concat, count, field, floor, func,
    lower, math_max, math_mean, math_min, math_sum, max_, min_, raw, round_, sum_, time_format,
    time_now, type_is, upper, value, ExprArg, Expression, ExpressionKind,
};
pub use helpers::{
    delete, from_table, insert, limit, offset, order_by, relate, select, similarity_search_query,
    update, upsert, vector_search_query, where_, DataMap, ReturnFormat, VectorDistanceType,
};
pub use hints::{
    merge_hints, render_hints, validate_hint, ExplainHint, FetchHint, FetchStrategy, HintRenderer,
    HintType, IndexHint, ParallelHint, QueryHint, TimeoutHint,
};
pub use results::{
    aggregate, count_result, extract_one, extract_result, extract_scalar, has_results, paginated,
    record, records, success, AggregateResult, CountResult, ListResult, PageInfo, PaginatedResult,
    QueryResult, RecordResult,
};
