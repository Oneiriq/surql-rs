//! Query operators for building type-safe SurrealDB expressions.
//!
//! Port of `surql/types/operators.py`. Python subclasses are represented
//! here by a single [`Operator`] enum plus type aliases that reuse its
//! variants via the specific constructor helpers.

use serde_json::Value;

use super::escape::{is_identifier, quote_str};
use super::record_id::RecordIdValue;
use super::record_ref::record_ref;

use crate::query::expressions::Expression;

/// Trait implemented by every operator so they can all produce SurrealQL.
pub trait OperatorExpr {
    /// Render this operator as a SurrealQL expression.
    fn to_surql(&self) -> String;
}

/// A composed query expression.
#[derive(Debug, Clone, PartialEq)]
pub enum Operator {
    /// `field = value`
    Eq(Eq),
    /// `field != value`
    Ne(Ne),
    /// `field > value`
    Gt(Gt),
    /// `field >= value`
    Gte(Gte),
    /// `field < value`
    Lt(Lt),
    /// `field <= value`
    Lte(Lte),
    /// `field CONTAINS value`
    Contains(Contains),
    /// `field CONTAINSNOT value`
    ContainsNot(ContainsNot),
    /// `field CONTAINSALL [...]`
    ContainsAll(ContainsAll),
    /// `field CONTAINSANY [...]`
    ContainsAny(ContainsAny),
    /// `field INSIDE [...]`
    Inside(Inside),
    /// `field NOTINSIDE [...]`
    NotInside(NotInside),
    /// `field IS NULL`
    IsNull(IsNull),
    /// `field IS NOT NULL`
    IsNotNull(IsNotNull),
    /// `field IS NONE`
    IsNone(IsNone),
    /// `field IS NOT NONE`
    IsNotNone(IsNotNone),
    /// `(left) AND (right)`
    And(And),
    /// `(left) OR (right)`
    Or(Or),
    /// `NOT (operand)`
    Not(Not),
    /// A condition built from typed expressions, such as a comparison
    /// against a function call or a record reference (see [`eq_expr`]).
    Expr(Expression),
}

impl From<Expression> for Operator {
    fn from(expr: Expression) -> Self {
        Self::Expr(expr)
    }
}

impl OperatorExpr for Operator {
    fn to_surql(&self) -> String {
        match self {
            Self::Eq(x) => x.to_surql(),
            Self::Ne(x) => x.to_surql(),
            Self::Gt(x) => x.to_surql(),
            Self::Gte(x) => x.to_surql(),
            Self::Lt(x) => x.to_surql(),
            Self::Lte(x) => x.to_surql(),
            Self::Contains(x) => x.to_surql(),
            Self::ContainsNot(x) => x.to_surql(),
            Self::ContainsAll(x) => x.to_surql(),
            Self::ContainsAny(x) => x.to_surql(),
            Self::Inside(x) => x.to_surql(),
            Self::NotInside(x) => x.to_surql(),
            Self::IsNull(x) => x.to_surql(),
            Self::IsNotNull(x) => x.to_surql(),
            Self::IsNone(x) => x.to_surql(),
            Self::IsNotNone(x) => x.to_surql(),
            Self::And(x) => x.to_surql(),
            Self::Or(x) => x.to_surql(),
            Self::Not(x) => x.to_surql(),
            Self::Expr(x) => x.to_surql(),
        }
    }
}

macro_rules! binary_comparison {
    ($(#[$meta:meta])* $name:ident, $sql:literal) => {
        $(#[$meta])*
        #[derive(Debug, Clone, PartialEq)]
        pub struct $name {
            /// Field name.
            pub field: String,
            /// Right-hand value.
            pub value: Value,
        }

        impl $name {
            /// Construct a new operator.
            pub fn new(field: impl Into<String>, value: impl Into<Value>) -> Self {
                Self {
                    field: field.into(),
                    value: value.into(),
                }
            }
        }

        impl OperatorExpr for $name {
            fn to_surql(&self) -> String {
                format!("{} {} {}", self.field, $sql, quote_value(&self.value))
            }
        }
    };
}

binary_comparison!(
    /// `field = value`
    Eq,
    "="
);
binary_comparison!(
    /// `field != value`
    Ne,
    "!="
);
binary_comparison!(
    /// `field > value`
    Gt,
    ">"
);
binary_comparison!(
    /// `field >= value`
    Gte,
    ">="
);
binary_comparison!(
    /// `field < value`
    Lt,
    "<"
);
binary_comparison!(
    /// `field <= value`
    Lte,
    "<="
);
binary_comparison!(
    /// `field CONTAINS value`
    Contains,
    "CONTAINS"
);
binary_comparison!(
    /// `field CONTAINSNOT value`
    ContainsNot,
    "CONTAINSNOT"
);

macro_rules! array_comparison {
    ($(#[$meta:meta])* $name:ident, $sql:literal) => {
        $(#[$meta])*
        #[derive(Debug, Clone, PartialEq)]
        pub struct $name {
            /// Field name.
            pub field: String,
            /// Right-hand list of values.
            pub values: Vec<Value>,
        }

        impl $name {
            /// Construct a new operator.
            pub fn new(field: impl Into<String>, values: impl IntoIterator<Item = Value>) -> Self {
                Self {
                    field: field.into(),
                    values: values.into_iter().collect(),
                }
            }
        }

        impl OperatorExpr for $name {
            fn to_surql(&self) -> String {
                let rendered = self
                    .values
                    .iter()
                    .map(quote_value)
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("{} {} [{}]", self.field, $sql, rendered)
            }
        }
    };
}

array_comparison!(
    /// `field CONTAINSALL [...]`
    ContainsAll,
    "CONTAINSALL"
);
array_comparison!(
    /// `field CONTAINSANY [...]`
    ContainsAny,
    "CONTAINSANY"
);
array_comparison!(
    /// `field INSIDE [...]`
    Inside,
    "INSIDE"
);
array_comparison!(
    /// `field NOTINSIDE [...]`
    NotInside,
    "NOTINSIDE"
);

/// `field IS NULL`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct IsNull {
    /// Field name.
    pub field: String,
}

impl IsNull {
    /// Construct `IS NULL` for the given field.
    pub fn new(field: impl Into<String>) -> Self {
        Self {
            field: field.into(),
        }
    }
}

impl OperatorExpr for IsNull {
    fn to_surql(&self) -> String {
        format!("{} IS NULL", self.field)
    }
}

/// `field IS NOT NULL`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct IsNotNull {
    /// Field name.
    pub field: String,
}

impl IsNotNull {
    /// Construct `IS NOT NULL` for the given field.
    pub fn new(field: impl Into<String>) -> Self {
        Self {
            field: field.into(),
        }
    }
}

impl OperatorExpr for IsNotNull {
    fn to_surql(&self) -> String {
        format!("{} IS NOT NULL", self.field)
    }
}

/// `field IS NONE` — matches an absent field. SurrealDB distinguishes NONE
/// (the field is not set) from NULL (set to the null value); a field skipped
/// during serialization reads as NONE, so this is the correct guard for an
/// optional field that is simply unset.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct IsNone {
    /// Field name.
    pub field: String,
}

impl IsNone {
    /// Construct `IS NONE` for the given field.
    pub fn new(field: impl Into<String>) -> Self {
        Self {
            field: field.into(),
        }
    }
}

impl OperatorExpr for IsNone {
    fn to_surql(&self) -> String {
        format!("{} IS NONE", self.field)
    }
}

/// `field IS NOT NONE` — matches a field that is set (to any value, including
/// null). The complement of [`IsNone`].
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct IsNotNone {
    /// Field name.
    pub field: String,
}

impl IsNotNone {
    /// Construct `IS NOT NONE` for the given field.
    pub fn new(field: impl Into<String>) -> Self {
        Self {
            field: field.into(),
        }
    }
}

impl OperatorExpr for IsNotNone {
    fn to_surql(&self) -> String {
        format!("{} IS NOT NONE", self.field)
    }
}

/// Logical AND of two operators.
#[derive(Debug, Clone, PartialEq)]
pub struct And {
    /// Left operand.
    pub left: Box<Operator>,
    /// Right operand.
    pub right: Box<Operator>,
}

impl OperatorExpr for And {
    fn to_surql(&self) -> String {
        format!("({}) AND ({})", self.left.to_surql(), self.right.to_surql())
    }
}

/// Logical OR of two operators.
#[derive(Debug, Clone, PartialEq)]
pub struct Or {
    /// Left operand.
    pub left: Box<Operator>,
    /// Right operand.
    pub right: Box<Operator>,
}

impl OperatorExpr for Or {
    fn to_surql(&self) -> String {
        format!("({}) OR ({})", self.left.to_surql(), self.right.to_surql())
    }
}

/// Logical NOT.
#[derive(Debug, Clone, PartialEq)]
pub struct Not {
    /// Inner operator.
    pub operand: Box<Operator>,
}

impl OperatorExpr for Not {
    fn to_surql(&self) -> String {
        format!("NOT ({})", self.operand.to_surql())
    }
}

// ---------------------------------------------------------------------------
// Functional helpers (match the Python API: `eq`, `ne`, `and_`, ...).
// ---------------------------------------------------------------------------

/// Build an [`struct@Eq`] operator.
pub fn eq(field: impl Into<String>, value: impl Into<Value>) -> Operator {
    Operator::Eq(Eq::new(field, value))
}

/// Build a [`Ne`] operator.
pub fn ne(field: impl Into<String>, value: impl Into<Value>) -> Operator {
    Operator::Ne(Ne::new(field, value))
}

/// Build a [`Gt`] operator.
pub fn gt(field: impl Into<String>, value: impl Into<Value>) -> Operator {
    Operator::Gt(Gt::new(field, value))
}

/// Build a [`Gte`] operator.
pub fn gte(field: impl Into<String>, value: impl Into<Value>) -> Operator {
    Operator::Gte(Gte::new(field, value))
}

/// Build a [`Lt`] operator.
pub fn lt(field: impl Into<String>, value: impl Into<Value>) -> Operator {
    Operator::Lt(Lt::new(field, value))
}

/// Build an [`Lte`] operator.
pub fn lte(field: impl Into<String>, value: impl Into<Value>) -> Operator {
    Operator::Lte(Lte::new(field, value))
}

/// Build a [`Contains`] operator.
pub fn contains(field: impl Into<String>, value: impl Into<Value>) -> Operator {
    Operator::Contains(Contains::new(field, value))
}

/// Build a [`ContainsNot`] operator.
pub fn contains_not(field: impl Into<String>, value: impl Into<Value>) -> Operator {
    Operator::ContainsNot(ContainsNot::new(field, value))
}

/// Build a [`ContainsAll`] operator.
pub fn contains_all(field: impl Into<String>, values: impl IntoIterator<Item = Value>) -> Operator {
    Operator::ContainsAll(ContainsAll::new(field, values))
}

/// Build a [`ContainsAny`] operator.
pub fn contains_any(field: impl Into<String>, values: impl IntoIterator<Item = Value>) -> Operator {
    Operator::ContainsAny(ContainsAny::new(field, values))
}

/// Build an [`Inside`] operator.
pub fn inside(field: impl Into<String>, values: impl IntoIterator<Item = Value>) -> Operator {
    Operator::Inside(Inside::new(field, values))
}

/// Build a [`NotInside`] operator.
pub fn not_inside(field: impl Into<String>, values: impl IntoIterator<Item = Value>) -> Operator {
    Operator::NotInside(NotInside::new(field, values))
}

/// Build an [`IsNull`] operator.
pub fn is_null(field: impl Into<String>) -> Operator {
    Operator::IsNull(IsNull::new(field))
}

/// Build an [`IsNotNull`] operator.
pub fn is_not_null(field: impl Into<String>) -> Operator {
    Operator::IsNotNull(IsNotNull::new(field))
}

/// Build an [`IsNone`] operator (`field IS NONE` — the field is unset).
pub fn is_none(field: impl Into<String>) -> Operator {
    Operator::IsNone(IsNone::new(field))
}

/// Build an [`IsNotNone`] operator (`field IS NOT NONE` — the field is set).
pub fn is_not_none(field: impl Into<String>) -> Operator {
    Operator::IsNotNone(IsNotNone::new(field))
}

/// Combine two operators with logical AND.
pub fn and_(left: Operator, right: Operator) -> Operator {
    Operator::And(And {
        left: Box::new(left),
        right: Box::new(right),
    })
}

/// Combine two operators with logical OR.
pub fn or_(left: Operator, right: Operator) -> Operator {
    Operator::Or(Or {
        left: Box::new(left),
        right: Box::new(right),
    })
}

/// Negate an operator.
pub fn not_(operand: Operator) -> Operator {
    Operator::Not(Not {
        operand: Box::new(operand),
    })
}

// ---------------------------------------------------------------------------
// Expression-valued comparisons
// ---------------------------------------------------------------------------

macro_rules! expr_comparison {
    ($(#[$meta:meta])* $name:ident, $sql:literal) => {
        $(#[$meta])*
        pub fn $name(field: impl Into<String>, rhs: impl Into<Expression>) -> Operator {
            Operator::Expr(Expression::raw(format!(
                "{} {} {}",
                field.into(),
                $sql,
                rhs.into().to_surql()
            )))
        }
    };
}

expr_comparison!(
    /// `field = <expression>`: compare against a typed expression (a
    /// function call, a record reference, another field) instead of a JSON
    /// literal.
    ///
    /// ## Examples
    ///
    /// ```
    /// use surql::types::operators::{eq_expr, OperatorExpr};
    /// use surql::types::record_ref;
    ///
    /// let op = eq_expr("author", record_ref("user", "alice"));
    /// assert_eq!(op.to_surql(), "author = type::record('user', 'alice')");
    /// ```
    eq_expr,
    "="
);
expr_comparison!(
    /// `field != <expression>` (see [`eq_expr`]).
    ne_expr,
    "!="
);
expr_comparison!(
    /// `field > <expression>` (see [`eq_expr`]).
    gt_expr,
    ">"
);
expr_comparison!(
    /// `field >= <expression>` (see [`eq_expr`]).
    gte_expr,
    ">="
);
expr_comparison!(
    /// `field < <expression>` (see [`eq_expr`]).
    ///
    /// ## Examples
    ///
    /// ```
    /// use surql::query::expressions::time_now;
    /// use surql::types::operators::{lt_expr, OperatorExpr};
    ///
    /// assert_eq!(lt_expr("expires_at", time_now()).to_surql(), "expires_at < time::now()");
    /// ```
    lt_expr,
    "<"
);
expr_comparison!(
    /// `field <= <expression>` (see [`eq_expr`]).
    lte_expr,
    "<="
);

// ---------------------------------------------------------------------------
// Value quoting (mirrors Python's `_quote_value`).
// ---------------------------------------------------------------------------

/// Public wrapper around the internal `quote_value` helper for other
/// crate modules that need the same SurrealQL literal rendering.
pub fn quote_value_public(value: &Value) -> String {
    quote_value(value)
}

/// Quote a [`Value`] for inclusion in a SurrealQL expression.
///
/// - `null` becomes `NULL`.
/// - bool becomes `true`/`false`.
/// - numbers stringify directly, except an integer above `i64::MAX`, which
///   the engine's integer literal cannot hold: it renders as an exact
///   decimal (`18446744073709551615dec`).
/// - strings are single-quoted and escaped.
/// - arrays and objects render as literals, at any depth, with object keys
///   quoted by [`quote_object_key`].
///
/// A value nested more than [`MAX_INLINE_DEPTH`] arrays and objects deep is
/// the exception: the engine's parser refuses a literal nested 20 levels
/// deep ("Exceeded query recursion depth limit"), so such a value renders
/// as its JSON text in a string literal, decoded by the engine
/// (`encoding::json::decode('…')`, SurrealDB 3.1+). The parser then sees
/// one flat string whatever the depth, and the value arrives the same:
/// JSON strings stay strings, and an integer above `i64::MAX` is a decimal
/// either way.
///
/// A `Value` is always data: no shape of JSON renders as raw SurrealQL.
/// Function calls and record references reach a query only through typed
/// channels ([`SurrealFn`](super::SurrealFn) and
/// [`RecordRef`](super::RecordRef) converted into an [`Expression`], the
/// `*_expr` comparisons, `Query::set_expr`).
pub(crate) fn quote_value(value: &Value) -> String {
    if nesting_exceeds(value, MAX_INLINE_DEPTH) {
        // Serialising a `serde_json::Value` cannot fail.
        let json = serde_json::to_string(value).unwrap_or_default();
        return format!("encoding::json::decode({})", quote_str(&json));
    }
    quote_inline(value)
}

/// The deepest array / object nesting [`quote_value`] renders as a literal.
/// Below the parser's limit of 20 levels by enough to leave room for the
/// statement around the value (a `CONTENT` object, an `INSERT` list, the
/// parentheses of a `WHERE`).
pub const MAX_INLINE_DEPTH: usize = 16;

/// `true` when `value` nests arrays and objects more than `limit` deep.
fn nesting_exceeds(value: &Value, limit: usize) -> bool {
    let children: Box<dyn Iterator<Item = &Value>> = match value {
        Value::Array(items) => Box::new(items.iter()),
        Value::Object(fields) => Box::new(fields.values()),
        _ => return false,
    };
    limit == 0
        || children
            .into_iter()
            .any(|child| nesting_exceeds(child, limit - 1))
}

/// Render `value` as a literal, however deep.
fn quote_inline(value: &Value) -> String {
    match value {
        Value::Null => "NULL".to_string(),
        Value::Bool(b) => if *b { "true" } else { "false" }.to_string(),
        Value::Number(n) if n.is_u64() && !n.is_i64() => format!("{n}dec"),
        Value::Number(n) => n.to_string(),
        Value::String(s) => quote_str(s),
        Value::Array(arr) => {
            let inner = arr.iter().map(quote_inline).collect::<Vec<_>>().join(", ");
            format!("[{inner}]")
        }
        Value::Object(obj) => {
            let inner = obj
                .iter()
                .map(|(k, v)| format!("{}: {}", quote_object_key(k), quote_inline(v)))
                .collect::<Vec<_>>()
                .join(", ");
            format!("{{ {inner} }}")
        }
    }
}

/// Render `key` as the key of a SurrealQL object literal.
///
/// Bare when identifier-shaped (the engine's own object-key rule, which also
/// quotes `NaN` and `Infinity`), a single-quoted string otherwise. The
/// parser accepts any string literal as an object key, so no key can end the
/// literal early.
pub(crate) fn quote_object_key(key: &str) -> String {
    if is_identifier(key) && key != "NaN" && key != "Infinity" {
        key.to_owned()
    } else {
        quote_str(key)
    }
}

// ---------------------------------------------------------------------------
// type::record / type::thing first-class helpers
// ---------------------------------------------------------------------------

/// Build a `type::record('<table>', <id>)` expression.
///
/// Mirrors the ergonomics of the Python `type_record()` helper: callers pass
/// a table name and any [`RecordIdValue`]-convertible id, and receive an
/// [`Expression`] (tagged [`crate::query::expressions::ExpressionKind::Function`])
/// that can be embedded anywhere a target, value, or SurrealQL fragment is
/// accepted. The returned expression renders identically to
/// [`RecordRef::to_surql`].
///
/// ## Examples
///
/// ```
/// use surql::types::operators::type_record;
///
/// let target = type_record("task", "abc-123");
/// assert_eq!(target.to_surql(), "type::record('task', 'abc-123')");
///
/// let numeric = type_record("post", 42_i64);
/// assert_eq!(numeric.to_surql(), "type::record('post', 42)");
/// ```
pub fn type_record(table: impl Into<String>, record_id: impl Into<RecordIdValue>) -> Expression {
    Expression::function(record_ref(table, record_id).to_surql())
}

/// Alias of [`type_record`], kept for parity with the sibling ports.
///
/// SurrealDB 2 accepted `type::thing(...)`; SurrealDB 3 removed it and
/// rejects the statement at parse time ("did you maybe mean
/// `type::record`"). This helper therefore renders `type::record(...)`, the
/// same as [`type_record`].
///
/// ## Examples
///
/// ```
/// use surql::types::operators::type_thing;
///
/// let target = type_thing("user", "alice");
/// assert_eq!(target.to_surql(), "type::record('user', 'alice')");
///
/// let numeric = type_thing("post", 123_i64);
/// assert_eq!(numeric.to_surql(), "type::record('post', 123)");
/// ```
pub fn type_thing(table: impl Into<String>, record_id: impl Into<RecordIdValue>) -> Expression {
    type_record(table, record_id)
}

#[cfg(test)]
mod tests;
