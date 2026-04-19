//! Query expression builders.
//!
//! Port of `surql/query/expressions.py`. Expressions are a typed string
//! wrapper for fragments that the query builder stitches together (field
//! references, quoted values, function calls, aliases, raw SurrealQL).

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::types::operators::quote_value_public;

/// A typed SurrealQL fragment.
///
/// Expressions store their rendered SurrealQL string. The [`kind`] tag
/// categorises the fragment (field reference, literal, function call, or
/// raw) and enables consumers to introspect without parsing the SQL.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Expression {
    /// Rendered SurrealQL.
    pub sql: String,
    /// Category of this expression.
    #[serde(default)]
    pub kind: ExpressionKind,
}

/// Category tag for an [`Expression`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExpressionKind {
    /// Plain / aliased / raw expression.
    #[default]
    Raw,
    /// Field reference (e.g. `user.name`).
    Field,
    /// Literal value (already quoted via [`quote_value_public`]).
    Value,
    /// Function call (e.g. `COUNT(*)`).
    Function,
}

impl Expression {
    /// Construct a raw expression from a pre-rendered SurrealQL string.
    pub fn raw(sql: impl Into<String>) -> Self {
        Self {
            sql: sql.into(),
            kind: ExpressionKind::Raw,
        }
    }

    /// Construct a field-reference expression.
    pub fn field(name: impl Into<String>) -> Self {
        Self {
            sql: name.into(),
            kind: ExpressionKind::Field,
        }
    }

    /// Construct a value expression (applies SurrealQL quoting).
    pub fn value(v: impl Into<Value>) -> Self {
        Self {
            sql: quote_value_public(&v.into()),
            kind: ExpressionKind::Value,
        }
    }

    /// Construct a function-call expression from a prerendered `"FN(...)"`.
    pub fn function(sql: impl Into<String>) -> Self {
        Self {
            sql: sql.into(),
            kind: ExpressionKind::Function,
        }
    }

    /// Render as SurrealQL.
    pub fn to_surql(&self) -> String {
        self.sql.clone()
    }
}

impl std::fmt::Display for Expression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.sql)
    }
}

// ---------------------------------------------------------------------------
// Top-level builders
// ---------------------------------------------------------------------------

/// Field reference: `field("user.name")` ⇒ `user.name`.
pub fn field(name: impl Into<String>) -> Expression {
    Expression::field(name)
}

/// Literal value (quoted with SurrealQL rules).
pub fn value(v: impl Into<Value>) -> Expression {
    Expression::value(v)
}

/// Raw SurrealQL (no quoting / escaping — use with caution).
pub fn raw(sql: impl Into<String>) -> Expression {
    Expression::raw(sql)
}

/// Function call: `func("UPPER", [field("name")])` ⇒ `UPPER(name)`.
///
/// `args` may be [`Expression`]s or anything convertible to a string via
/// `Into<String>` (the Python port accepts both).
pub fn func<A>(name: &str, args: impl IntoIterator<Item = A>) -> Expression
where
    A: Into<ExprArg>,
{
    let parts: Vec<String> = args
        .into_iter()
        .map(|a| match a.into() {
            ExprArg::Expr(e) => e.to_surql(),
            ExprArg::Str(s) => s,
        })
        .collect();
    Expression::function(format!("{name}({})", parts.join(", ")))
}

/// Argument wrapper for [`func`] / [`concat`] that accepts both
/// [`Expression`]s and raw strings.
#[derive(Debug, Clone)]
pub enum ExprArg {
    /// An already-built expression.
    Expr(Expression),
    /// A pre-rendered SurrealQL snippet.
    Str(String),
}

impl From<Expression> for ExprArg {
    fn from(e: Expression) -> Self {
        Self::Expr(e)
    }
}

impl From<&str> for ExprArg {
    fn from(s: &str) -> Self {
        Self::Str(s.to_owned())
    }
}

impl From<String> for ExprArg {
    fn from(s: String) -> Self {
        Self::Str(s)
    }
}

// ---------------------------------------------------------------------------
// Aggregate functions
// ---------------------------------------------------------------------------

/// `COUNT(*)` or `COUNT(field)`.
pub fn count(field_name: Option<&str>) -> Expression {
    Expression::function(format!("COUNT({})", field_name.unwrap_or("*")))
}

/// `SUM(field)`.
pub fn sum_(field_name: &str) -> Expression {
    Expression::function(format!("SUM({field_name})"))
}

/// `AVG(field)`.
pub fn avg(field_name: &str) -> Expression {
    Expression::function(format!("AVG({field_name})"))
}

/// `MIN(field)`.
pub fn min_(field_name: &str) -> Expression {
    Expression::function(format!("MIN({field_name})"))
}

/// `MAX(field)`.
pub fn max_(field_name: &str) -> Expression {
    Expression::function(format!("MAX({field_name})"))
}

// ---------------------------------------------------------------------------
// String functions
// ---------------------------------------------------------------------------

/// `string::uppercase(field)`.
pub fn upper(field_name: &str) -> Expression {
    Expression::function(format!("string::uppercase({field_name})"))
}

/// `string::lowercase(field)`.
pub fn lower(field_name: &str) -> Expression {
    Expression::function(format!("string::lowercase({field_name})"))
}

/// `string::concat(a, b, c, ...)`.
pub fn concat<A>(fields: impl IntoIterator<Item = A>) -> Expression
where
    A: Into<ExprArg>,
{
    let parts: Vec<String> = fields
        .into_iter()
        .map(|a| match a.into() {
            ExprArg::Expr(e) => e.to_surql(),
            ExprArg::Str(s) => s,
        })
        .collect();
    Expression::function(format!("string::concat({})", parts.join(", ")))
}

// ---------------------------------------------------------------------------
// Array functions
// ---------------------------------------------------------------------------

/// `array::len(field)`.
pub fn array_length(field_name: &str) -> Expression {
    Expression::function(format!("array::len({field_name})"))
}

/// `array::includes(field, value)`.
pub fn array_contains(field_name: &str, v: impl Into<Value>) -> Expression {
    Expression::function(format!(
        "array::includes({field_name}, {})",
        quote_value_public(&v.into())
    ))
}

// ---------------------------------------------------------------------------
// Math functions
// ---------------------------------------------------------------------------

/// `math::abs(field)`.
pub fn abs_(field_name: &str) -> Expression {
    Expression::function(format!("math::abs({field_name})"))
}

/// `math::ceil(field)`.
pub fn ceil(field_name: &str) -> Expression {
    Expression::function(format!("math::ceil({field_name})"))
}

/// `math::floor(field)`.
pub fn floor(field_name: &str) -> Expression {
    Expression::function(format!("math::floor({field_name})"))
}

/// `math::round(field, precision)`. `precision` defaults to 0 in the
/// Python port; this port requires an explicit value for clarity.
pub fn round_(field_name: &str, precision: i32) -> Expression {
    Expression::function(format!("math::round({field_name}, {precision})"))
}

/// `math::mean(field)`.
pub fn math_mean(field_name: &str) -> Expression {
    Expression::function(format!("math::mean({field_name})"))
}

/// `math::sum(field)`.
pub fn math_sum(field_name: &str) -> Expression {
    Expression::function(format!("math::sum({field_name})"))
}

/// `math::max(field)`.
pub fn math_max(field_name: &str) -> Expression {
    Expression::function(format!("math::max({field_name})"))
}

/// `math::min(field)`.
pub fn math_min(field_name: &str) -> Expression {
    Expression::function(format!("math::min({field_name})"))
}

// ---------------------------------------------------------------------------
// Time functions
// ---------------------------------------------------------------------------

/// `time::now()`.
pub fn time_now() -> Expression {
    Expression::function("time::now()".to_string())
}

/// `time::format(field, 'fmt')`.
pub fn time_format(field_name: &str, format_str: &str) -> Expression {
    Expression::function(format!(
        "time::format({field_name}, {})",
        quote_value_public(&Value::String(format_str.to_owned()))
    ))
}

// ---------------------------------------------------------------------------
// Type functions
// ---------------------------------------------------------------------------

/// `type::is::<type>(field)` (e.g. `type::is::string(name)`).
pub fn type_is(field_name: &str, type_name: &str) -> Expression {
    Expression::function(format!("type::is::{type_name}({field_name})"))
}

/// `<target_type>field` — SurrealQL cast syntax.
pub fn cast(field_name: &str, target_type: &str) -> Expression {
    Expression::raw(format!("<{target_type}>{field_name}"))
}

// ---------------------------------------------------------------------------
// Composition
// ---------------------------------------------------------------------------

/// Alias an expression: `as_(&count(None), "total")` ⇒ `COUNT(*) AS total`.
pub fn as_(expr: &Expression, alias: &str) -> Expression {
    Expression::raw(format!("{} AS {alias}", expr.to_surql()))
}

// ---------------------------------------------------------------------------
// Query-UX function factories (sub-feature 2): snake_case aliases that match
// the stable ports in surql-py and surql (TS). All return [`Expression`]s
// composable with `.select()`, `.set()`, `.where_()`, etc.
// ---------------------------------------------------------------------------

/// `math::abs(field)` - alias of [`abs_`] following the `math_*` naming
/// convention shared with the Python port.
pub fn math_abs(field_name: &str) -> Expression {
    Expression::function(format!("math::abs({field_name})"))
}

/// `math::ceil(field)` - snake_case alias of [`ceil`].
pub fn math_ceil(field_name: &str) -> Expression {
    Expression::function(format!("math::ceil({field_name})"))
}

/// `math::floor(field)` - snake_case alias of [`floor`].
pub fn math_floor(field_name: &str) -> Expression {
    Expression::function(format!("math::floor({field_name})"))
}

/// `math::round(field, precision)` - snake_case alias of [`round_`].
pub fn math_round(field_name: &str, precision: i32) -> Expression {
    Expression::function(format!("math::round({field_name}, {precision})"))
}

/// `string::len(field)` - reports the character length of a string field.
pub fn string_len(field_name: &str) -> Expression {
    Expression::function(format!("string::len({field_name})"))
}

/// `string::concat(a, b, c, ...)` - snake_case alias of [`concat`].
pub fn string_concat<A>(fields: impl IntoIterator<Item = A>) -> Expression
where
    A: Into<ExprArg>,
{
    concat(fields)
}

/// `string::lowercase(field)` - snake_case alias of [`lower`].
pub fn string_lower(field_name: &str) -> Expression {
    Expression::function(format!("string::lowercase({field_name})"))
}

/// `string::uppercase(field)` - snake_case alias of [`upper`].
pub fn string_upper(field_name: &str) -> Expression {
    Expression::function(format!("string::uppercase({field_name})"))
}

/// `count()` - zero-argument count aggregate.
///
/// Companion to the existing [`count`] helper (which accepts an optional
/// field name). Matches the `count()` shape used by the surql-py
/// query-UX API.
///
/// ## Examples
///
/// ```
/// use surql::query::expressions::count_all;
/// assert_eq!(count_all().to_surql(), "count()");
/// ```
pub fn count_all() -> Expression {
    Expression::function("count()".to_string())
}

/// `count(<condition>)` - count rows matching a boolean condition.
///
/// Accepts any [`ExprArg`] - an [`Expression`] (e.g. an [`Operator`](
/// crate::types::operators::Operator) rendered via `raw(op.to_surql())`)
/// or a bare SurrealQL fragment. Useful inside `SELECT` projections for
/// conditional aggregation.
///
/// ## Examples
///
/// ```
/// use surql::query::expressions::count_if;
/// assert_eq!(
///     count_if("age > 18").to_surql(),
///     "count(age > 18)",
/// );
/// ```
pub fn count_if<A>(condition: A) -> Expression
where
    A: Into<ExprArg>,
{
    let rendered = match condition.into() {
        ExprArg::Expr(e) => e.to_surql(),
        ExprArg::Str(s) => s,
    };
    Expression::function(format!("count({rendered})"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn field_and_value() {
        assert_eq!(field("user.name").to_surql(), "user.name");
        assert_eq!(value("Alice").to_surql(), "'Alice'");
        assert_eq!(value(42).to_surql(), "42");
        assert_eq!(value(true).to_surql(), "true");
    }

    #[test]
    fn count_renders() {
        assert_eq!(count(None).to_surql(), "COUNT(*)");
        assert_eq!(count(Some("id")).to_surql(), "COUNT(id)");
    }

    #[test]
    fn aggregate_functions() {
        assert_eq!(sum_("price").to_surql(), "SUM(price)");
        assert_eq!(avg("age").to_surql(), "AVG(age)");
        assert_eq!(min_("price").to_surql(), "MIN(price)");
        assert_eq!(max_("price").to_surql(), "MAX(price)");
    }

    #[test]
    fn math_native_aggregates() {
        assert_eq!(math_mean("score").to_surql(), "math::mean(score)");
        assert_eq!(math_sum("price").to_surql(), "math::sum(price)");
        assert_eq!(math_max("score").to_surql(), "math::max(score)");
        assert_eq!(math_min("price").to_surql(), "math::min(price)");
    }

    #[test]
    fn string_functions() {
        assert_eq!(upper("name").to_surql(), "string::uppercase(name)");
        assert_eq!(lower("email").to_surql(), "string::lowercase(email)");
        let c = concat::<ExprArg>([
            field("first_name").into(),
            value(" ").into(),
            field("last_name").into(),
        ]);
        assert_eq!(c.to_surql(), "string::concat(first_name, ' ', last_name)");
    }

    #[test]
    fn array_functions() {
        assert_eq!(array_length("tags").to_surql(), "array::len(tags)");
        assert_eq!(
            array_contains("tags", json!("python")).to_surql(),
            "array::includes(tags, 'python')"
        );
    }

    #[test]
    fn math_functions() {
        assert_eq!(abs_("temperature").to_surql(), "math::abs(temperature)");
        assert_eq!(ceil("price").to_surql(), "math::ceil(price)");
        assert_eq!(floor("price").to_surql(), "math::floor(price)");
        assert_eq!(round_("price", 2).to_surql(), "math::round(price, 2)");
    }

    #[test]
    fn time_functions() {
        assert_eq!(time_now().to_surql(), "time::now()");
        assert_eq!(
            time_format("created_at", "%Y-%m-%d").to_surql(),
            "time::format(created_at, '%Y-%m-%d')"
        );
    }

    #[test]
    fn type_functions() {
        assert_eq!(
            type_is("value", "string").to_surql(),
            "type::is::string(value)"
        );
        assert_eq!(cast("id", "string").to_surql(), "<string>id");
    }

    #[test]
    fn func_accepts_mixed_args() {
        let c = func::<ExprArg>(
            "CONCAT",
            [field("first").into(), "' '".into(), field("last").into()],
        );
        assert_eq!(c.to_surql(), "CONCAT(first, ' ', last)");
    }

    #[test]
    fn as_aliases_expressions() {
        assert_eq!(as_(&count(None), "total").to_surql(), "COUNT(*) AS total");
        let inner = concat::<ExprArg>([field("first").into(), field("last").into()]);
        assert_eq!(
            as_(&inner, "full_name").to_surql(),
            "string::concat(first, last) AS full_name"
        );
    }

    #[test]
    fn raw_passes_through() {
        assert_eq!(raw("time::now()").to_surql(), "time::now()");
    }

    #[test]
    fn kind_tag_reflects_constructor() {
        assert_eq!(field("x").kind, ExpressionKind::Field);
        assert_eq!(value(1).kind, ExpressionKind::Value);
        assert_eq!(count(None).kind, ExpressionKind::Function);
        assert_eq!(raw("x").kind, ExpressionKind::Raw);
    }

    #[test]
    fn display_matches_to_surql() {
        let e = count(None);
        assert_eq!(format!("{e}"), e.to_surql());
    }

    // -----------------------------------------------------------------------
    // Sub-feature 2: query-UX function factories
    // -----------------------------------------------------------------------

    #[test]
    fn math_snake_case_aliases() {
        assert_eq!(math_abs("t").to_surql(), "math::abs(t)");
        assert_eq!(math_ceil("p").to_surql(), "math::ceil(p)");
        assert_eq!(math_floor("p").to_surql(), "math::floor(p)");
        assert_eq!(math_round("p", 2).to_surql(), "math::round(p, 2)");
    }

    #[test]
    fn string_snake_case_aliases() {
        assert_eq!(string_len("name").to_surql(), "string::len(name)");
        assert_eq!(string_lower("e").to_surql(), "string::lowercase(e)");
        assert_eq!(string_upper("n").to_surql(), "string::uppercase(n)");
        let joined = string_concat::<ExprArg>([
            field("first").into(),
            value(" ").into(),
            field("last").into(),
        ]);
        assert_eq!(joined.to_surql(), "string::concat(first, ' ', last)");
    }

    #[test]
    fn count_all_zero_arg() {
        assert_eq!(count_all().to_surql(), "count()");
    }

    #[test]
    fn count_if_accepts_string_condition() {
        assert_eq!(count_if("age > 18").to_surql(), "count(age > 18)");
    }

    #[test]
    fn count_if_accepts_expression() {
        let e = raw("status = 'active'");
        assert_eq!(count_if(e).to_surql(), "count(status = 'active')");
    }

    #[test]
    fn new_factories_are_function_kind() {
        assert_eq!(math_abs("x").kind, ExpressionKind::Function);
        assert_eq!(string_len("x").kind, ExpressionKind::Function);
        assert_eq!(count_all().kind, ExpressionKind::Function);
        assert_eq!(count_if("x = 1").kind, ExpressionKind::Function);
    }
}
