//! `DEFINE FUNCTION` parser.
//!
//! Extracts [`FunctionDefinition`] values from the `functions` map of an
//! `INFO FOR DB` response. The engine echoes the canonical form —
//! `DEFINE FUNCTION fn::greet($name: string) -> string { RETURN 'hi ' + $name }
//! COMMENT 'greeter' PERMISSIONS FULL` — so the argument list is read
//! depth-aware (a generic like `array<record<x>>` carries its own commas) and
//! the body is taken from the outermost brace pair.

use super::find_keyword_unquoted as find_keyword;
use crate::schema::function::{FunctionArg, FunctionDefinition};

/// Parse one `DEFINE FUNCTION` statement.
///
/// `name` is the `INFO FOR DB` key (already without the `fn::` prefix); the
/// name inside the statement is preferred when present. Returns `None` when
/// the definition is empty or has no `{ body }`.
pub fn parse_function(name: &str, definition: &str) -> Option<FunctionDefinition> {
    if definition.is_empty() {
        return None;
    }
    let (body, before, after) = split_body(definition)?;

    let (declared_name, args) = parse_signature(before)?;
    let returns = before
        .rfind("->")
        .map(|at| before[at + 2..].trim().to_string())
        .filter(|r| !r.is_empty());

    let mut function = FunctionDefinition::new(
        if declared_name.is_empty() {
            name.to_string()
        } else {
            declared_name
        },
        body,
    );
    function.args = args;
    function.returns = returns;
    function.comment = extract_quoted_after(after, "COMMENT");
    function.permissions = extract_permissions(after);
    Some(function)
}

/// Split at the outermost `{ ... }`, returning the body and the text on
/// either side of it.
fn split_body(definition: &str) -> Option<(String, &str, &str)> {
    let bytes = definition.as_bytes();
    let open = definition.find('{')?;
    let mut depth = 0i32;
    for (i, b) in bytes.iter().enumerate().skip(open) {
        match b {
            b'{' => depth += 1,
            b'}' => {
                depth -= 1;
                if depth == 0 {
                    return Some((
                        definition[open + 1..i].trim().to_string(),
                        &definition[..open],
                        &definition[i + 1..],
                    ));
                }
            }
            _ => {}
        }
    }
    None
}

/// Read the `fn::<name>(<args>)` signature out of the text before the body.
fn parse_signature(before: &str) -> Option<(String, Vec<FunctionArg>)> {
    let open = before.find('(')?;
    let close = matching_paren(before, open)?;
    let head = before[..open].trim();
    let name = head
        .rsplit_once("fn::")
        .map(|(_, n)| n.trim().to_string())
        .unwrap_or_default();
    let args = split_top_level(&before[open + 1..close])
        .into_iter()
        .filter_map(|arg| {
            let (raw_name, arg_type) = arg.split_once(':')?;
            Some(FunctionArg::new(
                raw_name.trim().trim_start_matches('$'),
                arg_type.trim(),
            ))
        })
        .collect();
    Some((name, args))
}

/// Index of the `)` closing the `(` at `open`.
fn matching_paren(text: &str, open: usize) -> Option<usize> {
    let mut depth = 0i32;
    for (i, b) in text.as_bytes().iter().enumerate().skip(open) {
        match b {
            b'(' => depth += 1,
            b')' => {
                depth -= 1;
                if depth == 0 {
                    return Some(i);
                }
            }
            _ => {}
        }
    }
    None
}

/// Split an argument list on top-level commas, so `array<record<x>>` and
/// nested generics survive.
fn split_top_level(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut depth = 0i32;
    let mut current = String::new();
    for c in text.chars() {
        match c {
            '<' | '(' | '[' | '{' => depth += 1,
            '>' | ')' | ']' | '}' => depth -= 1,
            ',' if depth == 0 => {
                push_trimmed(&mut out, &current);
                current.clear();
                continue;
            }
            _ => {}
        }
        current.push(c);
    }
    push_trimmed(&mut out, &current);
    out
}

fn push_trimmed(out: &mut Vec<String>, value: &str) {
    let trimmed = value.trim();
    if !trimmed.is_empty() {
        out.push(trimmed.to_string());
    }
}

/// Read the quoted operand of `keyword` from the tail of a statement.
pub(crate) fn extract_quoted_after(tail: &str, keyword: &str) -> Option<String> {
    let at = find_keyword(tail, keyword)?;
    let rest = tail[at + keyword.len()..].trim_start();
    let quote = rest.chars().next().filter(|c| *c == '\'' || *c == '"')?;
    let body = &rest[quote.len_utf8()..];
    let end = body.find(quote)?;
    Some(body[..end].to_string())
}

/// Read the `PERMISSIONS` clause body from the tail of a statement.
pub(crate) fn extract_permissions(tail: &str) -> Option<String> {
    let at = find_keyword(tail, "PERMISSIONS")?;
    let mut rest = tail[at + "PERMISSIONS".len()..].trim();
    if let Some(at) = find_keyword(rest, "COMMENT") {
        rest = rest[..at].trim();
    }
    let rest = rest.trim_end_matches(';').trim();
    if rest.is_empty() {
        None
    } else {
        Some(rest.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_or_bodyless_definitions_are_none() {
        assert!(parse_function("f", "").is_none());
        assert!(parse_function("f", "DEFINE FUNCTION fn::f()").is_none());
    }

    #[test]
    fn engine_echo_with_every_clause() {
        let f = parse_function(
            "greet",
            "DEFINE FUNCTION fn::greet($name: string) -> string { RETURN 'hi ' + $name } \
             COMMENT 'greeter' PERMISSIONS FULL",
        )
        .expect("function");
        assert_eq!(f.name, "greet");
        assert_eq!(f.args, vec![FunctionArg::new("name", "string")]);
        assert_eq!(f.returns.as_deref(), Some("string"));
        assert_eq!(f.body, "RETURN 'hi ' + $name");
        assert_eq!(f.comment.as_deref(), Some("greeter"));
        assert_eq!(f.permissions.as_deref(), Some("FULL"));
    }

    #[test]
    fn engine_echo_of_a_none_union_argument() {
        let f = parse_function(
            "pkg::nested",
            "DEFINE FUNCTION fn::pkg::nested($a: int, $b: none | int) { RETURN $a } \
             PERMISSIONS FULL",
        )
        .expect("function");
        assert_eq!(f.name, "pkg::nested");
        assert_eq!(
            f.args,
            vec![
                FunctionArg::new("a", "int"),
                FunctionArg::new("b", "none | int"),
            ]
        );
        assert!(f.returns.is_none());
    }

    #[test]
    fn a_where_permission_keeps_its_expression() {
        let f = parse_function(
            "noargs",
            "DEFINE FUNCTION fn::noargs() { RETURN 1 } PERMISSIONS WHERE $auth",
        )
        .expect("function");
        assert!(f.args.is_empty());
        assert_eq!(f.permissions.as_deref(), Some("WHERE $auth"));
    }

    #[test]
    fn a_generic_argument_type_is_not_split_at_its_commas() {
        let f = parse_function(
            "f",
            "DEFINE FUNCTION fn::f($a: array<record<x>>, $b: int) { RETURN $b }",
        )
        .expect("function");
        assert_eq!(
            f.args,
            vec![
                FunctionArg::new("a", "array<record<x>>"),
                FunctionArg::new("b", "int"),
            ]
        );
    }

    #[test]
    fn a_nested_block_body_keeps_its_braces() {
        let f = parse_function(
            "f",
            "DEFINE FUNCTION fn::f() { IF $a { RETURN 1 } ELSE { RETURN 2 } } PERMISSIONS FULL",
        )
        .expect("function");
        assert_eq!(f.body, "IF $a { RETURN 1 } ELSE { RETURN 2 }");
        assert_eq!(f.permissions.as_deref(), Some("FULL"));
    }

    #[test]
    fn the_map_key_is_used_when_the_statement_has_no_name() {
        let f = parse_function("fallback", "DEFINE FUNCTION () { RETURN 1 }").expect("function");
        assert_eq!(f.name, "fallback");
    }

    #[test]
    fn round_trips_through_the_renderer_after_normalising() {
        let code = crate::schema::function_schema("greet", "RETURN 'hi ' + $name;")
            .arg("name", "option<string>")
            .returns("string")
            .comment("greeter")
            .build()
            .unwrap();
        let parsed = parse_function(
            "greet",
            code.normalized().to_surql().unwrap().trim_end_matches(';'),
        )
        .expect("function");
        assert_eq!(parsed.normalized(), code.normalized());
    }
}
