//! `DEFINE EVENT` parser.
//!
//! Extracts [`EventDefinition`] values (the `WHEN ... THEN ...` pair)
//! from SurrealDB `INFO FOR TABLE` responses. Split out of the
//! monolithic `parser.rs` so each submodule stays under the 1000-LOC
//! budget; see parent [`super`] for the public entry points.
//!
//! The engine echoes `DEFINE EVENT <name> ON <table> [ASYNC …] WHEN <cond>
//! THEN <action> [COMMENT …]`, with a block action printed as
//! `{ a; b; }`. The action is read back as the block's body, which is what
//! [`EventDefinition::to_surql`] wraps in braces again.

use super::scan::{clause, clauses, define_head, strip_group, Shape};
use crate::schema::table::EventDefinition;

const EVENT_CLAUSES: &[(&str, Shape)] = &[
    ("WHEN", Shape::Expr),
    ("THEN", Shape::Expr),
    ("COMMENT", Shape::Str),
];

// --- Public parsers ----------------------------------------------------------

/// Parse every entry of an `ev` / `events` map.
pub fn parse_events(ev: &std::collections::BTreeMap<String, String>) -> Vec<EventDefinition> {
    ev.iter()
        .filter_map(|(name, def)| parse_event(name, def))
        .collect()
}

/// Parse one `DEFINE EVENT` statement.
///
/// A braced action (`THEN { UPDATE s SET n += 1; DELETE tmp; }`) reads back
/// as its body without the braces or the trailing `;`, so rendering it again
/// yields the same block.
///
/// Returns `None` when the condition or action cannot be located.
pub fn parse_event(name: &str, definition: &str) -> Option<EventDefinition> {
    if definition.trim().is_empty() {
        return None;
    }
    let body = define_head(definition, "EVENT", true).map_or(definition, |head| head.rest);
    let found = clauses(body, EVENT_CLAUSES);
    let condition = clause(&found, "WHEN").filter(|c| !c.is_empty())?;
    let action = clause(&found, "THEN").filter(|a| !a.is_empty())?;
    let action =
        strip_group(action, '{').map_or(action, |inner| inner.trim_end_matches(';').trim_end());
    Some(EventDefinition {
        name: name.to_string(),
        condition: condition.to_string(),
        action: action.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_block_keeps_every_statement_and_its_inner_braces() {
        let ev = parse_event(
            "ev",
            "DEFINE EVENT ev ON research_paper WHEN $event = 'CREATE' THEN { UPDATE s SET \
             n += 1; IF x { DELETE tmp }; }",
        )
        .unwrap();
        assert_eq!(ev.condition, "$event = 'CREATE'");
        assert_eq!(ev.action, "UPDATE s SET n += 1; IF x { DELETE tmp }");
    }

    #[test]
    fn keywords_in_the_action_and_a_trailing_comment_are_handled() {
        let ev = parse_event(
            "e1",
            "DEFINE EVENT e1 ON t4 WHEN true THEN { CREATE log SET note = 'WHEN THEN' } \
             COMMENT 'THEN x'",
        )
        .unwrap();
        assert_eq!(ev.condition, "true");
        assert_eq!(ev.action, "CREATE log SET note = 'WHEN THEN'");
    }

    #[test]
    fn a_parenthesised_action_is_kept_as_is() {
        let ev = parse_event(
            "ev2",
            "DEFINE EVENT ev2 ON research_paper WHEN true THEN (CREATE log)",
        )
        .unwrap();
        assert_eq!(ev.action, "(CREATE log)");
    }

    #[test]
    fn the_rendered_block_round_trips() {
        let code = EventDefinition::new("n", "true", "UPDATE s SET n += 1; DELETE tmp");
        let parsed = parse_event("n", code.to_surql("t").trim_end_matches(';')).unwrap();
        assert_eq!(parsed, code);
    }
}
