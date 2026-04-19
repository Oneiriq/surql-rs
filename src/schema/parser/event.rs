//! `DEFINE EVENT` parser.
//!
//! Extracts [`EventDefinition`] values (the `WHEN ... THEN ...` pair)
//! from SurrealDB `INFO FOR TABLE` responses. Split out of the
//! monolithic `parser.rs` so each submodule stays under the 1000-LOC
//! budget; see parent [`super`] for the public entry points.

use std::sync::OnceLock;

use regex::Regex;

use super::regex_case_insensitive;
use crate::schema::table::EventDefinition;

// --- Regex accessors ---------------------------------------------------------

fn when_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| regex_case_insensitive(r"(?s)WHEN\s+(.+?)\s+THEN"))
}

fn then_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| regex_case_insensitive(r"(?s)THEN\s+(?:\{(.+?)\}|(.+?))(?:\s*;|\s*$)"))
}

// --- Public parsers ----------------------------------------------------------

/// Parse every entry of an `ev` / `events` map.
pub fn parse_events(ev: &std::collections::BTreeMap<String, String>) -> Vec<EventDefinition> {
    ev.iter()
        .filter_map(|(name, def)| parse_event(name, def))
        .collect()
}

/// Parse one `DEFINE EVENT` statement.
///
/// Returns `None` when the condition or action cannot be located.
pub fn parse_event(name: &str, definition: &str) -> Option<EventDefinition> {
    if definition.is_empty() {
        return None;
    }
    let condition = extract_event_condition(definition)?;
    let action = extract_event_action(definition)?;
    Some(EventDefinition {
        name: name.to_string(),
        condition,
        action,
    })
}

// --- Event extractors --------------------------------------------------------

fn extract_event_condition(definition: &str) -> Option<String> {
    when_regex()
        .captures(definition)
        .and_then(|c| c.get(1))
        .map(|m| m.as_str().trim().to_string())
}

fn extract_event_action(definition: &str) -> Option<String> {
    let caps = then_regex().captures(definition)?;
    if let Some(m) = caps.get(1) {
        return Some(m.as_str().trim().to_string());
    }
    caps.get(2).map(|m| m.as_str().trim().to_string())
}
