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
/// - numbers stringify directly.
/// - strings are single-quoted and escaped.
/// - arrays and objects render as literals, at any depth, with object keys
///   quoted by [`quote_object_key`].
///
/// A `Value` is always data: no shape of JSON renders as raw SurrealQL.
/// Function calls and record references reach a query only through typed
/// channels ([`SurrealFn`](super::SurrealFn) and
/// [`RecordRef`](super::RecordRef) converted into an [`Expression`], the
/// `*_expr` comparisons, `Query::set_expr`).
pub(crate) fn quote_value(value: &Value) -> String {
    match value {
        Value::Null => "NULL".to_string(),
        Value::Bool(b) => if *b { "true" } else { "false" }.to_string(),
        Value::Number(n) => n.to_string(),
        Value::String(s) => quote_str(s),
        Value::Array(arr) => {
            let inner = arr.iter().map(quote_value).collect::<Vec<_>>().join(", ");
            format!("[{inner}]")
        }
        Value::Object(obj) => {
            let inner = obj
                .iter()
                .map(|(k, v)| format!("{}: {}", quote_object_key(k), quote_value(v)))
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
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn eq_renders() {
        assert_eq!(eq("name", "Alice").to_surql(), "name = 'Alice'");
    }

    #[test]
    fn ne_renders() {
        assert_eq!(ne("status", "deleted").to_surql(), "status != 'deleted'");
    }

    #[test]
    fn gt_renders_integer() {
        assert_eq!(gt("age", 18).to_surql(), "age > 18");
    }

    #[test]
    fn lt_renders_float() {
        assert_eq!(lt("price", 50.0).to_surql(), "price < 50.0");
    }

    #[test]
    fn gte_and_lte() {
        assert_eq!(gte("score", 100).to_surql(), "score >= 100");
        assert_eq!(lte("quantity", 10).to_surql(), "quantity <= 10");
    }

    #[test]
    fn contains_renders() {
        assert_eq!(
            contains("email", "@example.com").to_surql(),
            "email CONTAINS '@example.com'"
        );
    }

    #[test]
    fn contains_not_renders() {
        assert_eq!(
            contains_not("tags", "spam").to_surql(),
            "tags CONTAINSNOT 'spam'"
        );
    }

    #[test]
    fn contains_all_renders() {
        let op = contains_all("tags", [json!("python"), json!("database")]);
        assert_eq!(op.to_surql(), "tags CONTAINSALL ['python', 'database']");
    }

    #[test]
    fn contains_any_renders() {
        let op = contains_any("tags", [json!("python"), json!("javascript")]);
        assert_eq!(op.to_surql(), "tags CONTAINSANY ['python', 'javascript']");
    }

    #[test]
    fn inside_renders() {
        let op = inside("status", [json!("active"), json!("pending")]);
        assert_eq!(op.to_surql(), "status INSIDE ['active', 'pending']");
    }

    #[test]
    fn not_inside_renders() {
        let op = not_inside("status", [json!("deleted"), json!("archived")]);
        assert_eq!(op.to_surql(), "status NOTINSIDE ['deleted', 'archived']");
    }

    #[test]
    fn is_null_and_not_null() {
        assert_eq!(is_null("deleted_at").to_surql(), "deleted_at IS NULL");
        assert_eq!(
            is_not_null("created_at").to_surql(),
            "created_at IS NOT NULL"
        );
    }

    #[test]
    fn is_none_and_not_none() {
        // An absent (unset) field is NONE, not NULL -- the correct guard for an
        // optional field that was never written.
        assert_eq!(is_none("deleted_at").to_surql(), "deleted_at IS NONE");
        assert_eq!(
            is_not_none("consolidated_expert").to_surql(),
            "consolidated_expert IS NOT NONE"
        );
    }

    #[test]
    fn and_renders() {
        let op = and_(gt("age", 18), eq("status", "active"));
        assert_eq!(op.to_surql(), "(age > 18) AND (status = 'active')");
    }

    #[test]
    fn or_renders() {
        let op = or_(eq("type", "admin"), eq("type", "moderator"));
        assert_eq!(op.to_surql(), "(type = 'admin') OR (type = 'moderator')");
    }

    #[test]
    fn not_renders() {
        let op = not_(eq("status", "deleted"));
        assert_eq!(op.to_surql(), "NOT (status = 'deleted')");
    }

    #[test]
    fn null_quoted_as_keyword() {
        assert_eq!(
            eq("deleted_at", Value::Null).to_surql(),
            "deleted_at = NULL"
        );
    }

    #[test]
    fn bool_quoted_lowercase() {
        assert_eq!(eq("active", true).to_surql(), "active = true");
        assert_eq!(eq("active", false).to_surql(), "active = false");
    }

    #[test]
    fn string_escapes_single_quote() {
        assert_eq!(eq("name", "O'Brien").to_surql(), "name = 'O\\'Brien'");
    }

    #[test]
    fn string_escapes_backslash() {
        assert_eq!(eq("path", "a\\b").to_surql(), "path = 'a\\\\b'");
    }

    #[test]
    fn expression_shaped_data_renders_as_an_object_literal() {
        let hostile = json!({"body": {"expression": "1}; DELETE user; --"}});
        assert_eq!(
            quote_value(&hostile),
            "{ body: { expression: '1}; DELETE user; --' } }"
        );
        let harmless = json!({"expression": "1+1", "result": 2});
        assert_eq!(quote_value(&harmless), "{ expression: '1+1', result: 2 }");
    }

    #[test]
    fn record_ref_shaped_data_renders_as_an_object_literal() {
        let shaped = json!({"table": "user", "record_id": "alice"});
        assert_eq!(
            quote_value(&shaped),
            "{ record_id: 'alice', table: 'user' }"
        );
    }

    #[test]
    fn object_keys_are_quoted_when_not_identifiers() {
        assert_eq!(quote_value(&json!({"": 1})), "{ '': 1 }");
        assert_eq!(
            quote_value(&json!({"x: 1}; DELETE user; --": 1})),
            "{ 'x: 1}; DELETE user; --': 1 }"
        );
        assert_eq!(quote_value(&json!({"1a": 1})), "{ '1a': 1 }");
    }

    #[test]
    fn record_ref_escapes_the_table() {
        assert_eq!(
            record_ref("user', 'x'); DELETE user; --", "a").to_surql(),
            r"type::record('user\', \'x\'); DELETE user; --', 'a')"
        );
        assert_eq!(
            type_record("a'b", "c").to_surql(),
            r"type::record('a\'b', 'c')"
        );
    }

    #[test]
    fn surrealfn_renders_raw_only_through_the_typed_channel() {
        let now = super::super::surreal_fn::surql_fn("time::now", &[]);
        // Serialised into JSON it is data like any other object ...
        let as_json = serde_json::to_value(&now).unwrap();
        assert_eq!(
            eq("created_at", as_json).to_surql(),
            "created_at = { expression: 'time::now()' }"
        );
        // ... and only the typed comparison renders the call.
        assert_eq!(
            lt_expr("created_at", now).to_surql(),
            "created_at < time::now()"
        );
    }

    #[test]
    fn record_ref_renders_raw_only_through_the_typed_channel() {
        let rr = record_ref("user", "alice");
        let as_json = serde_json::to_value(&rr).unwrap();
        assert_eq!(
            eq("author", as_json).to_surql(),
            "author = { record_id: 'alice', table: 'user' }"
        );
        assert_eq!(
            eq_expr("author", rr).to_surql(),
            "author = type::record('user', 'alice')"
        );
    }

    #[test]
    fn expr_comparisons_compose_with_logical_operators() {
        use crate::query::expressions::{field, time_now};

        let op = and_(ne_expr("owner", field("author")), gte_expr("n", 1));
        assert_eq!(op.to_surql(), "(owner != author) AND (n >= 1)");
        assert_eq!(gt_expr("t", time_now()).to_surql(), "t > time::now()");
        assert_eq!(lte_expr("t", time_now()).to_surql(), "t <= time::now()");
        assert_eq!(Operator::from(Expression::raw("a = b")).to_surql(), "a = b");
    }

    #[test]
    fn type_record_string_id_renders() {
        assert_eq!(
            type_record("task", "abc-123").to_surql(),
            "type::record('task', 'abc-123')"
        );
    }

    #[test]
    fn type_record_int_id_renders() {
        assert_eq!(
            type_record("post", 42_i64).to_surql(),
            "type::record('post', 42)"
        );
    }

    #[test]
    fn type_record_escapes_single_quote() {
        assert_eq!(
            type_record("user", "o'brien").to_surql(),
            "type::record('user', 'o\\'brien')"
        );
    }

    #[test]
    fn type_record_is_function_expression() {
        let expr = type_record("task", "abc");
        assert_eq!(
            expr.kind,
            crate::query::expressions::ExpressionKind::Function
        );
    }

    #[test]
    fn type_thing_renders_the_v3_function() {
        // SurrealDB 3 rejects `type::thing(...)` at parse time.
        assert_eq!(
            type_thing("user", "alice").to_surql(),
            "type::record('user', 'alice')"
        );
        assert_eq!(
            type_thing("post", 123_i64).to_surql(),
            "type::record('post', 123)"
        );
    }

    #[test]
    fn type_thing_escapes_backslash() {
        assert_eq!(
            type_thing("path", "a\\b").to_surql(),
            "type::record('path', 'a\\\\b')"
        );
    }

    #[test]
    fn type_thing_is_function_expression() {
        let expr = type_thing("user", "alice");
        assert_eq!(
            expr.kind,
            crate::query::expressions::ExpressionKind::Function
        );
    }
}
