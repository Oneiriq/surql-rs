//! `DEFINE PARAM` parser.
//!
//! Extracts [`ParamDefinition`] values from the `params` map of an
//! `INFO FOR DB` response. The engine echoes
//! `DEFINE PARAM $P VALUE 'hello' COMMENT 'a param' PERMISSIONS FULL`, so the
//! `VALUE` body runs up to whichever of `COMMENT` / `PERMISSIONS` comes
//! first — found outside quotes and brackets, because a value may itself
//! contain either word (`VALUE { comment: 'x' }`).

use super::function::TAIL_CLAUSES;
use super::scan::{clause, clauses, find_keyword, string_literal, unquote_ident, Shape};
use crate::schema::param::ParamDefinition;

/// Parse one `DEFINE PARAM` statement.
///
/// `name` is the `INFO FOR DB` key (already without the `$`); the name inside
/// the statement is preferred when present. Returns `None` when the
/// definition is empty or carries no `VALUE`.
pub fn parse_param(name: &str, definition: &str) -> Option<ParamDefinition> {
    if definition.is_empty() {
        return None;
    }
    let at = find_keyword(definition, "VALUE")?;
    let head = definition.get(..at)?;
    let rest = definition.get(at..)?;
    let keywords: Vec<(&str, Shape)> = std::iter::once(("VALUE", Shape::Expr))
        .chain(TAIL_CLAUSES.iter().copied())
        .collect();
    let found = clauses(rest, &keywords);
    let value = clause(&found, "VALUE").filter(|v| !v.is_empty())?;

    let declared = head
        .rsplit_once('$')
        .map(|(_, n)| unquote_ident(n.trim()))
        .filter(|n| !n.is_empty());

    let mut param = ParamDefinition::new(declared.unwrap_or_else(|| name.to_string()), value);
    param.comment = clause(&found, "COMMENT").and_then(string_literal);
    param.permissions = clause(&found, "PERMISSIONS")
        .filter(|p| !p.is_empty())
        .map(str::to_string);
    Some(param)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_or_valueless_definitions_are_none() {
        assert!(parse_param("p", "").is_none());
        assert!(parse_param("p", "DEFINE PARAM $p").is_none());
        assert!(parse_param("p", "DEFINE PARAM $p VALUE ").is_none());
    }

    #[test]
    fn engine_echo_with_every_clause() {
        let p = parse_param(
            "P1",
            "DEFINE PARAM $P1 VALUE 'hello' COMMENT 'a param' PERMISSIONS FULL",
        )
        .expect("param");
        assert_eq!(p.name, "P1");
        assert_eq!(p.value, "'hello'");
        assert_eq!(p.comment.as_deref(), Some("a param"));
        assert_eq!(p.permissions.as_deref(), Some("FULL"));
    }

    #[test]
    fn engine_echo_of_a_numeric_value() {
        let p =
            parse_param("P2", "DEFINE PARAM $P2 VALUE 42 PERMISSIONS WHERE $auth").expect("param");
        assert_eq!(p.value, "42");
        assert_eq!(p.permissions.as_deref(), Some("WHERE $auth"));
        assert!(p.comment.is_none());
    }

    /// A value that happens to contain a clause keyword must survive intact.
    #[test]
    fn a_quoted_keyword_in_the_value_is_not_a_clause_boundary() {
        let p = parse_param(
            "P",
            "DEFINE PARAM $P VALUE 'leave a comment about permissions' PERMISSIONS FULL",
        )
        .expect("param");
        assert_eq!(p.value, "'leave a comment about permissions'");
        assert_eq!(p.permissions.as_deref(), Some("FULL"));
    }

    #[test]
    fn an_object_value_with_clause_named_keys_survives() {
        // Exact 3.0.5 echo.
        let p = parse_param(
            "obj",
            "DEFINE PARAM $obj VALUE { comment: 'x', permissions: 'y' } COMMENT 'o' \
             PERMISSIONS FULL",
        )
        .expect("param");
        assert_eq!(p.value, "{ comment: 'x', permissions: 'y' }");
        assert_eq!(p.comment.as_deref(), Some("o"));
        assert_eq!(p.permissions.as_deref(), Some("FULL"));
        let p = parse_param(
            "s",
            "DEFINE PARAM $s VALUE \"it's\" PERMISSIONS WHERE $auth.admin = true",
        )
        .expect("param");
        assert_eq!(p.value, "\"it's\"");
        assert_eq!(p.permissions.as_deref(), Some("WHERE $auth.admin = true"));
    }

    #[test]
    fn a_bare_statement_keeps_its_value() {
        let p = parse_param("P", "DEFINE PARAM $P VALUE [1, 2, 3];").expect("param");
        assert_eq!(p.value, "[1, 2, 3]");
        assert!(p.permissions.is_none());
    }

    #[test]
    fn the_map_key_is_used_when_the_statement_has_no_sigil() {
        let p = parse_param("fallback", "DEFINE PARAM VALUE 1").expect("param");
        assert_eq!(p.name, "fallback");
    }

    #[test]
    fn round_trips_through_the_renderer_after_normalising() {
        let code = crate::schema::param_schema("APP", "'oneiriq'")
            .comment("display name")
            .build()
            .unwrap();
        let parsed = parse_param(
            "APP",
            code.normalized().to_surql().unwrap().trim_end_matches(';'),
        )
        .expect("param");
        assert_eq!(parsed.normalized(), code.normalized());
    }
}
