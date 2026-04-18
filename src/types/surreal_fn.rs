//! Raw SurrealDB function wrapper.
//!
//! Port of `surql/types/surreal_fn.py`. Used as a value in CREATE/UPDATE/UPSERT
//! operations when the caller wants the expression emitted verbatim rather
//! than parameterised as a string literal.

use serde::{Deserialize, Serialize};

/// Value wrapper for a raw SurrealDB function call.
///
/// Renders as raw SurrealQL when embedded in query bodies.
///
/// ## Examples
///
/// ```
/// use surql::types::{surql_fn, SurrealFn};
///
/// let now = surql_fn("time::now", &[]);
/// assert_eq!(now.to_surql(), "time::now()");
///
/// let fmt = surql_fn("time::format", &["created_at", "%Y-%m-%d"]);
/// assert_eq!(fmt.to_surql(), "time::format(created_at, %Y-%m-%d)");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SurrealFn {
    /// Fully-rendered function expression (e.g. `time::now()`).
    pub expression: String,
}

impl SurrealFn {
    /// Construct a new [`SurrealFn`] from a pre-rendered expression string.
    pub fn new(expression: impl Into<String>) -> Self {
        Self {
            expression: expression.into(),
        }
    }

    /// Render the function call as raw SurrealQL.
    pub fn to_surql(&self) -> String {
        self.expression.clone()
    }
}

impl std::fmt::Display for SurrealFn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.expression)
    }
}

/// Create a SurrealDB function call value.
///
/// Arguments are rendered verbatim via `Display`, joined with `, `. When
/// `args` is empty the call renders with just empty parentheses.
pub fn surql_fn(name: &str, args: &[&str]) -> SurrealFn {
    if args.is_empty() {
        SurrealFn::new(format!("{name}()"))
    } else {
        SurrealFn::new(format!("{name}({})", args.join(", ")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_args() {
        assert_eq!(surql_fn("time::now", &[]).to_surql(), "time::now()");
    }

    #[test]
    fn with_args() {
        assert_eq!(
            surql_fn("time::format", &["created_at", "%Y-%m-%d"]).to_surql(),
            "time::format(created_at, %Y-%m-%d)"
        );
    }

    #[test]
    fn single_arg() {
        assert_eq!(
            surql_fn("math::sum", &["scores"]).to_surql(),
            "math::sum(scores)"
        );
    }

    #[test]
    fn display_matches_to_surql() {
        let f = surql_fn("time::now", &[]);
        assert_eq!(format!("{f}"), f.to_surql());
    }

    #[test]
    fn serde_roundtrip() {
        let f = SurrealFn::new("time::now()");
        let json = serde_json::to_string(&f).unwrap();
        let back: SurrealFn = serde_json::from_str(&json).unwrap();
        assert_eq!(f, back);
    }
}
