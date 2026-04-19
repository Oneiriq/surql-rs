//! Query operators for building type-safe SurrealDB expressions.
//!
//! Port of `surql/types/operators.py`. Python subclasses are represented
//! here by a single [`Operator`] enum plus type aliases that reuse its
//! variants via the specific constructor helpers.

use serde_json::Value;

use super::record_id::RecordIdValue;
use super::record_ref::{record_ref, RecordRef};
use super::surreal_fn::SurrealFn;

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
    /// `(left) AND (right)`
    And(And),
    /// `(left) OR (right)`
    Or(Or),
    /// `NOT (operand)`
    Not(Not),
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
            Self::And(x) => x.to_surql(),
            Self::Or(x) => x.to_surql(),
            Self::Not(x) => x.to_surql(),
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
/// - strings are single-quoted and escape `\` and `'`.
/// - [`SurrealFn`] and [`RecordRef`] encoded as JSON objects (via
///   `serde_json::to_value`) render their raw `to_surql()` expression.
pub(crate) fn quote_value(value: &Value) -> String {
    match value {
        Value::Null => "NULL".to_string(),
        Value::Bool(b) => if *b { "true" } else { "false" }.to_string(),
        Value::Number(n) => n.to_string(),
        Value::String(s) => {
            let escaped = s.replace('\\', "\\\\").replace('\'', "\\'");
            format!("'{escaped}'")
        }
        Value::Array(arr) => {
            let inner = arr.iter().map(quote_value).collect::<Vec<_>>().join(", ");
            format!("[{inner}]")
        }
        Value::Object(obj) => {
            // Detect `SurrealFn` / `RecordRef` shapes.
            if let Some(raw) = try_wrapped_raw(obj) {
                return raw;
            }
            let inner = obj
                .iter()
                .map(|(k, v)| format!("{}: {}", quote_key(k), quote_value(v)))
                .collect::<Vec<_>>()
                .join(", ");
            format!("{{ {inner} }}")
        }
    }
}

fn quote_key(key: &str) -> String {
    if key.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
        key.to_owned()
    } else {
        let escaped = key.replace('\\', "\\\\").replace('\'', "\\'");
        format!("'{escaped}'")
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

/// Build a `type::thing('<table>', <id>)` expression.
///
/// `type::thing` is the SurrealDB alias for `type::record`. This helper is
/// provided for parity with the SurrealQL function set; the rendered SurrealQL
/// uses `type::thing(...)` verbatim so query plans that expect the literal
/// `thing` function call continue to match.
///
/// ## Examples
///
/// ```
/// use surql::types::operators::type_thing;
///
/// let target = type_thing("user", "alice");
/// assert_eq!(target.to_surql(), "type::thing('user', 'alice')");
///
/// let numeric = type_thing("post", 123_i64);
/// assert_eq!(numeric.to_surql(), "type::thing('post', 123)");
/// ```
pub fn type_thing(table: impl Into<String>, record_id: impl Into<RecordIdValue>) -> Expression {
    let rendered = match record_id.into() {
        RecordIdValue::Int(n) => format!("type::thing('{}', {n})", table.into()),
        RecordIdValue::String(s) => {
            let escaped = s.replace('\\', "\\\\").replace('\'', "\\'");
            format!("type::thing('{}', '{escaped}')", table.into())
        }
    };
    Expression::function(rendered)
}

fn try_wrapped_raw(obj: &serde_json::Map<String, Value>) -> Option<String> {
    if let Ok(fnv) = serde_json::from_value::<SurrealFn>(Value::Object(obj.clone())) {
        return Some(fnv.to_surql());
    }
    if let Ok(rr) = serde_json::from_value::<RecordRef>(Value::Object(obj.clone())) {
        return Some(rr.to_surql());
    }
    None
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
    fn surrealfn_value_renders_raw() {
        let fnv =
            serde_json::to_value(super::super::surreal_fn::surql_fn("time::now", &[])).unwrap();
        assert_eq!(eq("created_at", fnv).to_surql(), "created_at = time::now()");
    }

    #[test]
    fn record_ref_value_renders_raw() {
        let rr =
            serde_json::to_value(super::super::record_ref::record_ref("user", "alice")).unwrap();
        assert_eq!(
            eq("author", rr).to_surql(),
            "author = type::record('user', 'alice')"
        );
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
    fn type_thing_string_id_renders() {
        assert_eq!(
            type_thing("user", "alice").to_surql(),
            "type::thing('user', 'alice')"
        );
    }

    #[test]
    fn type_thing_int_id_renders() {
        assert_eq!(
            type_thing("post", 123_i64).to_surql(),
            "type::thing('post', 123)"
        );
    }

    #[test]
    fn type_thing_escapes_backslash() {
        assert_eq!(
            type_thing("path", "a\\b").to_surql(),
            "type::thing('path', 'a\\\\b')"
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
