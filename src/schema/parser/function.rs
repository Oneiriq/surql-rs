//! `DEFINE FUNCTION` parser.
//!
//! Extracts [`FunctionDefinition`] values from the `functions` map of an
//! `INFO FOR DB` response. The engine echoes the canonical form —
//! `DEFINE FUNCTION fn::greet($name: string) -> string { RETURN 'hi ' + $name }
//! COMMENT 'greeter' PERMISSIONS FULL` — so the argument list is read
//! depth-aware (a generic like `array<record<x>>` carries its own commas) and
//! the body is taken from the outermost brace pair, skipping any brace inside
//! a string (`RETURN '{' + $x`).

use super::scan::{clause, clauses, matching_close, string_literal, unquote_ident, Shape};
use crate::schema::function::{FunctionArg, FunctionDefinition};

/// The clauses that can follow a function body or a param value.
pub(super) const TAIL_CLAUSES: &[(&str, Shape)] =
    &[("COMMENT", Shape::Str), ("PERMISSIONS", Shape::Expr)];

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
        .and_then(|at| before.get(at + 2..))
        .map(|r| r.trim().to_string())
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
    let (comment, permissions) = read_tail(after);
    function.comment = comment;
    function.permissions = permissions;
    Some(function)
}

/// Split at the outermost `{ ... }`, returning the body and the text on
/// either side of it. Braces inside quoted text do not count.
fn split_body(definition: &str) -> Option<(String, &str, &str)> {
    let open = definition.find('{')?;
    let close = matching_close(definition, open)?;
    Some((
        definition.get(open + 1..close)?.trim().to_string(),
        definition.get(..open)?,
        definition.get(close + 1..)?,
    ))
}

/// Read the `fn::<name>(<args>)` signature out of the text before the body.
fn parse_signature(before: &str) -> Option<(String, Vec<FunctionArg>)> {
    let open = before.find('(')?;
    let close = matching_close(before, open)?;
    let head = before.get(..open)?.trim();
    let name = head
        .rsplit_once("fn::")
        .map(|(_, n)| {
            n.split("::")
                .map(unquote_ident)
                .collect::<Vec<_>>()
                .join("::")
        })
        .unwrap_or_default();
    let args = split_args(before.get(open + 1..close)?)
        .into_iter()
        .filter_map(|arg| {
            let (raw_name, arg_type) = arg.split_once(':')?;
            Some(FunctionArg::new(
                unquote_ident(raw_name.trim().trim_start_matches('$')),
                arg_type.trim(),
            ))
        })
        .collect();
    Some((name, args))
}

/// Split an argument list on top-level commas, so `array<record<x>>` and
/// nested generics survive.
fn split_args(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut depth = 0usize;
    let mut current = String::new();
    for c in text.chars() {
        match c {
            '<' | '(' | '[' | '{' => depth += 1,
            '>' | ')' | ']' | '}' => depth = depth.saturating_sub(1),
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

/// Read the `COMMENT` (unescaped) and `PERMISSIONS` clauses that follow a
/// function body.
fn read_tail(tail: &str) -> (Option<String>, Option<String>) {
    let found = clauses(tail, TAIL_CLAUSES);
    let comment = clause(&found, "COMMENT").and_then(string_literal);
    let permissions = clause(&found, "PERMISSIONS")
        .filter(|p| !p.is_empty())
        .map(str::to_string);
    (comment, permissions)
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
    fn braces_inside_strings_do_not_end_the_body() {
        // Exact 3.0.5 echoes.
        let f = parse_function(
            "strip",
            "DEFINE FUNCTION fn::strip($x: string) -> string { RETURN string::replace($x, '}', \
             '') } COMMENT \"user's greeting\" PERMISSIONS WHERE $auth.admin = true",
        )
        .expect("function");
        assert_eq!(f.body, "RETURN string::replace($x, '}', '')");
        assert_eq!(f.comment.as_deref(), Some("user's greeting"));
        assert_eq!(f.permissions.as_deref(), Some("WHERE $auth.admin = true"));
        let f = parse_function(
            "open",
            r"DEFINE FUNCTION fn::open($x: string) { RETURN '{' + $x } COMMENT 'line1\nline2' PERMISSIONS FULL",
        )
        .expect("function");
        assert_eq!(f.body, "RETURN '{' + $x");
        assert_eq!(f.comment.as_deref(), Some("line1\nline2"));
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
