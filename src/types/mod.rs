//! Type-safe primitives for building SurrealDB queries.
//!
//! Port of `surql/types/` from `oneiriq-surql` (Python).

pub mod coerce;
pub mod operators;
pub mod record_id;
pub mod record_ref;
pub mod reserved;
pub mod surreal_fn;

pub use coerce::{coerce_datetime, coerce_record_datetimes};
pub use operators::{
    and_, contains, contains_all, contains_any, contains_not, eq, gt, gte, inside, is_not_null,
    is_null, lt, lte, ne, not_, not_inside, or_, type_record, type_thing, And, Contains,
    ContainsAll, ContainsAny, ContainsNot, Eq, Gt, Gte, Inside, IsNotNull, IsNull, Lt, Lte, Ne,
    Not, NotInside, Operator, OperatorExpr, Or,
};
pub use record_id::{RecordID, RecordIdValue};
pub use record_ref::{record_ref, RecordRef};
pub use reserved::{check_reserved_word, EDGE_ALLOWED_RESERVED, SURREAL_RESERVED_WORDS};
pub use surreal_fn::{surql_fn, SurrealFn};
