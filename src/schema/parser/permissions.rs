//! `PERMISSIONS` clause parser for tables, edges, and fields.
//!
//! Reads the per-action rules the engine echoes back into the map shape the
//! code side declares ([`TableDefinition::permissions`],
//! [`EdgeDefinition::permissions`], [`FieldDefinition::permissions`]):
//! action name to a `WHERE` expression, or to the literal `"NONE"` /
//! `"FULL"` for the two fixed postures.
//!
//! ## What the engine echoes
//!
//! - `PERMISSIONS NONE` / `PERMISSIONS FULL` when every action agrees.
//! - Otherwise `FOR <actions> <NONE | FULL | WHERE expr>` lines joined with
//!   `", "`, actions that share a rule grouped into one line:
//!   `PERMISSIONS FOR select WHERE tenant = $auth.tenant, FOR create, update,
//!   delete NONE`.
//! - `delete` is left out of that list when it is `FULL` (the engine shares
//!   the printer with fields, which have no `delete`), so an echo naming
//!   `select`, `create`, and `update` but not `delete` means `delete FULL`.
//!
//! ## What the parser returns
//!
//! Only actions that differ from the engine default are kept: `NONE` for
//! tables and edges (a `DEFINE TABLE` without `PERMISSIONS` denies
//! everything), `FULL` for fields. A definition at the default reads back as
//! `None`, which is what a code-side definition without permissions holds,
//! so the two compare equal. `FULL` and `NONE` are kept as those words, so a
//! table opened with `PERMISSIONS FULL` no longer reads as undeclared.
//!
//! [`TableDefinition::permissions`]: crate::schema::TableDefinition::permissions
//! [`EdgeDefinition::permissions`]: crate::schema::EdgeDefinition::permissions
//! [`FieldDefinition::permissions`]: crate::schema::FieldDefinition::permissions

use std::collections::BTreeMap;

use super::scan::{find_keyword_from, tokens};
use super::table::read_table;

/// One action's permission.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Rule {
    None,
    Full,
    Where(String),
}

impl Rule {
    fn into_value(self) -> String {
        match self {
            Self::None => "NONE".to_string(),
            Self::Full => "FULL".to_string(),
            Self::Where(expr) => expr,
        }
    }
}

/// Which object the clause belongs to: decides the actions it may name and
/// the posture an unnamed action keeps.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Owner {
    /// Tables and edges: four actions, default `NONE`.
    Table,
    /// Fields: no `delete`, default `FULL`.
    Field,
}

impl Owner {
    fn actions(self) -> &'static [&'static str] {
        match self {
            Self::Table => &["select", "create", "update", "delete"],
            Self::Field => &["select", "create", "update"],
        }
    }

    fn default_rule(self) -> Rule {
        match self {
            Self::Table => Rule::None,
            Self::Field => Rule::Full,
        }
    }
}

/// One `FOR <actions> <rule>` line.
struct Line {
    actions: Vec<String>,
    rule: Rule,
}

/// Parse the `FOR …` lines of a clause body. Returns the lines and whether
/// they were joined the engine's way, with `", "`.
fn parse_lines(body: &str) -> (Vec<Line>, bool) {
    let mut starts = Vec::new();
    let mut from = 0;
    while let Some(at) = find_keyword_from(body, "FOR", from) {
        starts.push(at);
        from = at + "FOR".len();
    }
    let mut comma_joined = true;
    let mut lines = Vec::new();
    for (n, &start) in starts.iter().enumerate() {
        let end = starts.get(n + 1).copied().unwrap_or(body.len());
        let chunk = body.get(start + "FOR".len()..end).unwrap_or("").trim();
        if n + 1 < starts.len() && !chunk.ends_with(',') {
            comma_joined = false;
        }
        if let Some(line) = parse_line(chunk.trim_end_matches(',').trim_end()) {
            lines.push(line);
        }
    }
    (lines, comma_joined)
}

/// Parse `select, create WHERE <expr>` (the text after `FOR`).
fn parse_line(chunk: &str) -> Option<Line> {
    let mut actions = Vec::new();
    for token in tokens(chunk) {
        let word = token.text.trim_end_matches(',');
        let rule = if word.eq_ignore_ascii_case("NONE") {
            Some(Rule::None)
        } else if word.eq_ignore_ascii_case("FULL") {
            Some(Rule::Full)
        } else if word.eq_ignore_ascii_case("WHERE") {
            let expr = chunk.get(token.end..).unwrap_or("").trim();
            Some(Rule::Where(expr.to_string()))
        } else {
            None
        };
        if let Some(rule) = rule {
            return (!actions.is_empty()).then_some(Line { actions, rule });
        }
        actions.extend(
            word.split(',')
                .map(|a| a.trim().to_ascii_lowercase())
                .filter(|a| !a.is_empty()),
        );
    }
    None
}

/// Parse a `PERMISSIONS` clause body (the text after the keyword) for
/// `owner`. `None` when every action keeps the owner's default.
pub(super) fn parse_permissions_body(body: &str, owner: Owner) -> Option<BTreeMap<String, String>> {
    let body = body.trim().trim_end_matches(';').trim_end();
    let actions = owner.actions();
    let mut rules: BTreeMap<&str, Rule> = BTreeMap::new();
    if body.eq_ignore_ascii_case("NONE") || body.eq_ignore_ascii_case("FULL") {
        let rule = if body.eq_ignore_ascii_case("NONE") {
            Rule::None
        } else {
            Rule::Full
        };
        for action in actions {
            rules.insert(*action, rule.clone());
        }
    } else {
        let (lines, comma_joined) = parse_lines(body);
        let echo_shaped = comma_joined || lines.len() == 1;
        for line in lines {
            for action in &line.actions {
                if let Some(known) = actions.iter().find(|a| **a == action.as_str()) {
                    rules.insert(*known, line.rule.clone());
                }
            }
        }
        let names_the_rest = ["select", "create", "update"]
            .iter()
            .all(|a| rules.contains_key(*a));
        if owner == Owner::Table && echo_shaped && names_the_rest && !rules.contains_key("delete") {
            rules.insert("delete", Rule::Full);
        }
    }
    let default = owner.default_rule();
    let out: BTreeMap<String, String> = rules
        .into_iter()
        .filter(|(_, rule)| *rule != default)
        .map(|(action, rule)| (action.to_string(), rule.into_value()))
        .collect();
    (!out.is_empty()).then_some(out)
}

/// Extract the table-level `PERMISSIONS` clause from a `DEFINE TABLE`
/// statement string into a per-action rule map.
///
/// Returns `None` for an empty input, for a definition without a
/// `PERMISSIONS` clause, and for one at the table default (`NONE` for every
/// action), which is what a code-side definition without permissions holds.
/// Every other action carries its `WHERE` expression, or `"FULL"` /
/// `"NONE"` for the fixed postures, so `PERMISSIONS FULL` reads back as
/// `FULL` on all four actions rather than as undeclared.
///
/// ## Examples
///
/// ```
/// use surql::schema::parser::parse_table_permissions;
///
/// let perms = parse_table_permissions(
///     "DEFINE TABLE community TYPE NORMAL SCHEMAFULL PERMISSIONS \
///      FOR select WHERE $auth.id != NONE, FOR create, update, delete NONE",
/// )
/// .unwrap();
/// assert_eq!(perms.get("select").map(String::as_str), Some("$auth.id != NONE"));
/// assert!(!perms.contains_key("create"));
/// ```
pub fn parse_table_permissions(definition: &str) -> Option<BTreeMap<String, String>> {
    let body = read_table(definition).permissions?;
    parse_permissions_body(body, Owner::Table)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn map(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect()
    }

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
    fn none_is_the_default_and_full_is_explicit() {
        assert_eq!(
            parse_table_permissions("DEFINE TABLE x PERMISSIONS NONE"),
            None,
        );
        assert_eq!(
            parse_table_permissions("DEFINE TABLE x TYPE ANY DROP SCHEMALESS PERMISSIONS FULL"),
            Some(map(&[
                ("create", "FULL"),
                ("delete", "FULL"),
                ("select", "FULL"),
                ("update", "FULL"),
            ])),
        );
    }

    #[test]
    fn the_engine_separator_is_not_part_of_the_rule() {
        // Exact 3.0.5 echo: the old parser read the rule as
        // `tenant = $auth.tenant,` and dropped the NONE line.
        let perms = parse_table_permissions(
            "DEFINE TABLE user TYPE NORMAL SCHEMAFULL PERMISSIONS FOR select WHERE \
             tenant = $auth.tenant, FOR create, update, delete NONE",
        );
        assert_eq!(perms, Some(map(&[("select", "tenant = $auth.tenant")])));
    }

    #[test]
    fn a_missing_delete_in_the_echo_means_full() {
        // 3.0.5 echo of `FOR select WHERE a = 1 FOR delete FULL`: the engine
        // leaves a FULL delete out of the list.
        let perms = parse_table_permissions(
            "DEFINE TABLE t2 TYPE NORMAL SCHEMAFULL COMMENT 'no changefeed needed' \
             PERMISSIONS FOR select WHERE a = 1, FOR create, update NONE",
        );
        assert_eq!(perms, Some(map(&[("delete", "FULL"), ("select", "a = 1")])));
    }

    #[test]
    fn quoted_separators_and_keywords_stay_in_the_rule() {
        let perms = parse_table_permissions(
            "DEFINE TABLE t4 TYPE NORMAL SCHEMAFULL COMMENT \"it's\" CHANGEFEED 1d PERMISSIONS \
             FOR select, create WHERE x = 'FOR y, z', FOR update NONE, FOR delete WHERE true",
        );
        assert_eq!(
            perms,
            Some(map(&[
                ("create", "x = 'FOR y, z'"),
                ("delete", "true"),
                ("select", "x = 'FOR y, z'"),
            ])),
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
            perms,
            map(&[
                ("create", "$auth.id = owner"),
                ("select", "$auth.id != NONE")
            ])
        );
    }

    #[test]
    fn parses_v3_comma_joined_form() {
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

    #[test]
    fn a_rendered_statement_without_delete_keeps_the_default() {
        // Space-joined lines are this crate's own rendering, not the echo:
        // an unnamed delete keeps the table default.
        let perms = parse_table_permissions(
            "DEFINE TABLE x SCHEMAFULL PERMISSIONS FOR create WHERE a FOR select WHERE b \
             FOR update WHERE c",
        )
        .unwrap();
        assert!(!perms.contains_key("delete"));
    }

    #[test]
    fn field_bodies_drop_the_full_default() {
        assert_eq!(parse_permissions_body("FULL", Owner::Field), None);
        assert_eq!(
            parse_permissions_body(
                "FOR select WHERE $auth.admin, FOR create, update FULL",
                Owner::Field
            ),
            Some(map(&[("select", "$auth.admin")])),
        );
        assert_eq!(
            parse_permissions_body("NONE", Owner::Field),
            Some(map(&[
                ("create", "NONE"),
                ("select", "NONE"),
                ("update", "NONE")
            ])),
        );
    }
}
