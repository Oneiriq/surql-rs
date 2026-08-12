//! `DEFINE PARAM` parser.
//!
//! Extracts [`ParamDefinition`] values from the `params` map of an
//! `INFO FOR DB` response. The engine echoes
//! `DEFINE PARAM $P VALUE 'hello' COMMENT 'a param' PERMISSIONS FULL`, so the
//! `VALUE` body runs up to whichever of `COMMENT` / `PERMISSIONS` comes
//! first — found outside quotes, because a value may itself contain either
//! word.

use super::find_keyword_unquoted;
use super::function::{extract_permissions, extract_quoted_after};
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
    let at = find_keyword_unquoted(definition, "VALUE")?;
    let head = &definition[..at];
    let tail = &definition[at + "VALUE".len()..];

    let mut value_end = tail.len();
    for keyword in ["COMMENT", "PERMISSIONS"] {
        if let Some(at) = find_keyword_unquoted(tail, keyword) {
            value_end = value_end.min(at);
        }
    }
    let value = tail[..value_end].trim().trim_end_matches(';').trim();
    if value.is_empty() {
        return None;
    }

    let declared = head
        .rsplit('$')
        .next()
        .map(|n| n.trim().to_string())
        .filter(|n| !n.is_empty() && n.chars().all(|c| c.is_alphanumeric() || c == '_'));

    let mut param = ParamDefinition::new(declared.unwrap_or_else(|| name.to_string()), value);
    param.comment = extract_quoted_after(&tail[value_end..], "COMMENT");
    param.permissions = extract_permissions(&tail[value_end..]);
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
