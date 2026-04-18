//! Query construction and result handling.
//!
//! Port of `surql/query/` from `oneiriq-surql` (Python). Currently exposes:
//!
//! - [`hints`]: query optimization hints ([`IndexHint`](hints::IndexHint),
//!   [`ParallelHint`](hints::ParallelHint), [`TimeoutHint`](hints::TimeoutHint),
//!   [`FetchHint`](hints::FetchHint), [`ExplainHint`](hints::ExplainHint)).
//! - [`results`]: typed result wrappers and extraction helpers.
//!
//! Subsequent increments add the immutable `Query` builder, expressions,
//! typed/batch/graph CRUD, and the async executor.

pub mod expressions;
pub mod hints;
pub mod results;

pub use expressions::{
    abs_, array_contains, array_length, as_, avg, cast, ceil, concat, count, field, floor, func,
    lower, math_max, math_mean, math_min, math_sum, max_, min_, raw, round_, sum_, time_format,
    time_now, type_is, upper, value, ExprArg, Expression, ExpressionKind,
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
