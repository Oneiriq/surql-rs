//! Table-level `PERMISSIONS` clause parser.
//!
//! Extracts the per-action `PERMISSIONS` rules from a `DEFINE TABLE`
//! statement string. Used by [`super::parse_table_info`] and
//! [`super::parse_edge_info`] so that both tables and edges round-trip
//! the permissions clause through `INFO FOR DB` → parser → `diff_*`.
//!
//! ## Shapes recognised
//!
//! - **Trivial postures** — `PERMISSIONS NONE` / `PERMISSIONS FULL` →
//!   `None`. The code-side helpers have no representation for the
//!   default-deny / default-allow defaults, so returning `None` matches.
//! - **Expanded form** — `PERMISSIONS FOR select WHERE r1 FOR create
//!   WHERE r2 ...` → `{select: r1, create: r2, ...}`.
//! - **Comma-joined form** — SurrealDB v3 collapses runs of identical
//!   rules to `PERMISSIONS FOR select, create, update, delete WHERE r`.
//!   The rule is exploded across every named action.
//! - **Mixed forms** — some actions grouped via comma, others split into
//!   separate `FOR ... WHERE ...` clauses — also work because every
//!   `FOR <list> WHERE <rule>` clause is parsed independently and the
//!   action list inside each is comma-split.
//!
//! Returns `None` when no `PERMISSIONS` clause is present in the
//! definition. The 1.6.2 / 1.6.3 surql-py releases established this
//! same return convention; the Rust port mirrors it exactly.

use std::collections::BTreeMap;
use std::sync::OnceLock;

use regex::Regex;

use super::regex_case_insensitive;

/// Match a `PERMISSIONS` clause body up to the trailing `;` / end of
/// string. Captures the clause body in group 1.
fn table_permissions_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| regex_case_insensitive(r"\bPERMISSIONS\b([\s\S]*?)(?:\s*;\s*$|\s*$)"))
}

/// Match one `FOR <action-list> WHERE <rule>` clause. The action list
/// (group 1) is a comma-separated set of `select|create|update|delete`;
/// the rule (group 2) follows `WHERE` and runs to the end of the input
/// chunk it was applied to. The Rust `regex` crate does not support
/// lookahead, so the clause-body parser ([`parse_table_permissions`])
/// splits the `PERMISSIONS` body on `FOR` boundaries manually and feeds
/// each resulting chunk to this regex; that way every match's rule
/// portion is naturally bounded by the chunk and no lookahead is needed.
fn permission_clause_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        regex_case_insensitive(
            r"^\s*((?:select|create|update|delete)(?:\s*,\s*(?:select|create|update|delete))*)\s+WHERE\s+([\s\S]*?)\s*$",
        )
    })
}

/// Word-boundary-anchored match for the literal `FOR` keyword. Used to
/// split a `PERMISSIONS` body into per-action chunks before
/// [`permission_clause_regex`] is applied.
fn for_keyword_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| regex_case_insensitive(r"\bFOR\b"))
}

/// Extract the table-level `PERMISSIONS` clause from a `DEFINE TABLE`
/// statement string into a per-action rule map.
///
/// Returns `None` for an empty input, for a definition that has no
/// `PERMISSIONS` keyword, or for the trivial `NONE` / `FULL` postures —
/// the code-side helpers have no representation for those, so `None`
/// matches whatever the consumer's `EdgeDefinition.permissions` /
/// `TableDefinition.permissions` field looks like when no per-action
/// rule was supplied.
///
/// ## Examples
///
/// ```ignore
/// // Crate-private: not part of the public API surface.
/// use surql::schema::parser::parse_table_permissions;
///
/// let perms = parse_table_permissions(
///     "DEFINE TABLE community PERMISSIONS \
///      FOR select WHERE $auth.id != NONE \
///      FOR create WHERE $auth.id = owner"
/// );
/// assert_eq!(perms.unwrap().get("select").map(String::as_str), Some("$auth.id != NONE"));
/// ```
pub fn parse_table_permissions(definition: &str) -> Option<BTreeMap<String, String>> {
    if definition.is_empty() {
        return None;
    }

    let perm_match = table_permissions_regex().captures(definition)?;
    let body = perm_match.get(1)?.as_str().trim();
    if body.is_empty() {
        return None;
    }
    let upper = body.to_ascii_uppercase();
    if upper == "NONE" || upper == "FULL" {
        return None;
    }

    // Split the body on `FOR` boundaries so each chunk is a single
    // `<action-list> WHERE <rule>` clause. Rust's `regex` crate does
    // not support lookahead, so the alternative — a single regex with
    // `(?=\s+FOR ...)` to bound each rule — won't compile. This split
    // gives the same per-clause bounding without the lookahead.
    let mut rules: BTreeMap<String, String> = BTreeMap::new();
    let chunks: Vec<&str> = for_keyword_regex().split(body).collect();
    for chunk in chunks {
        let trimmed = chunk.trim().trim_end_matches(';').trim();
        if trimmed.is_empty() {
            continue;
        }
        let Some(caps) = permission_clause_regex().captures(trimmed) else {
            continue;
        };
        let Some(actions) = caps.get(1) else { continue };
        let Some(rule) = caps.get(2) else { continue };
        let rule_str = rule.as_str().trim().to_string();
        for raw in actions.as_str().split(',') {
            let action = raw.trim().to_ascii_lowercase();
            if matches!(action.as_str(), "select" | "create" | "update" | "delete") {
                rules.insert(action, rule_str.clone());
            }
        }
    }

    if rules.is_empty() {
        None
    } else {
        Some(rules)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn returns_none_for_empty_string() {
        assert_eq!(parse_table_permissions(""), None);
    }

    #[test]
    fn returns_none_when_no_permissions_clause() {
        assert_eq!(
            parse_table_permissions("DEFINE TABLE community SCHEMAFULL"),
            None,
        );
    }

    #[test]
    fn returns_none_for_trivial_none_or_full() {
        assert_eq!(
            parse_table_permissions("DEFINE TABLE x PERMISSIONS NONE"),
            None,
        );
        assert_eq!(
            parse_table_permissions("DEFINE TABLE x PERMISSIONS FULL"),
            None,
        );
    }

    #[test]
    fn parses_expanded_per_action_form() {
        let perms = parse_table_permissions(
            "DEFINE TABLE x SCHEMAFULL PERMISSIONS \
             FOR select WHERE $auth.id != NONE \
             FOR create WHERE $auth.id = owner",
        )
        .unwrap();
        assert_eq!(
            perms.get("select").map(String::as_str),
            Some("$auth.id != NONE")
        );
        assert_eq!(
            perms.get("create").map(String::as_str),
            Some("$auth.id = owner")
        );
        assert!(!perms.contains_key("update"));
        assert!(!perms.contains_key("delete"));
    }

    #[test]
    fn parses_v3_comma_joined_form() {
        // SurrealDB v3 collapses runs of identical rules into
        // `FOR select, create, update, delete WHERE r` — every consumer
        // with shared rules saw a false-positive MODIFY_PERMISSIONS diff
        // until 1.6.3 taught the parser to explode the list.
        let perms = parse_table_permissions(
            "DEFINE TABLE x PERMISSIONS \
             FOR select, create, update, delete WHERE tenant = $auth.tenant",
        )
        .unwrap();
        assert_eq!(perms.len(), 4);
        for action in ["select", "create", "update", "delete"] {
            assert_eq!(
                perms.get(action).map(String::as_str),
                Some("tenant = $auth.tenant"),
                "action {action} should carry the shared rule"
            );
        }
    }

    #[test]
    fn parses_mixed_forms() {
        // Some actions grouped via comma, others split into separate
        // clauses — every `FOR <list> WHERE <rule>` is independent.
        let perms = parse_table_permissions(
            "DEFINE TABLE x PERMISSIONS \
             FOR select, create WHERE shared \
             FOR update WHERE owned \
             FOR delete WHERE admin",
        )
        .unwrap();
        assert_eq!(perms.get("select").map(String::as_str), Some("shared"));
        assert_eq!(perms.get("create").map(String::as_str), Some("shared"));
        assert_eq!(perms.get("update").map(String::as_str), Some("owned"));
        assert_eq!(perms.get("delete").map(String::as_str), Some("admin"));
    }
}
