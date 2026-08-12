//! `DEFINE SEQUENCE` parser.
//!
//! Extracts [`SequenceDefinition`] values from the `sequences` map of an
//! `INFO FOR DB` response, mirroring [`super::bucket`]. The engine always
//! echoes `BATCH` and `START`, so the parser and the renderer agree on the
//! defaults rather than one of them omitting them.

use std::sync::OnceLock;

use regex::Regex;

use super::regex_case_insensitive;
use crate::schema::sequence::{SequenceDefinition, DEFAULT_BATCH, DEFAULT_START};

fn batch_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| regex_case_insensitive(r"\bBATCH\s+(\d+)"))
}

fn start_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| regex_case_insensitive(r"\bSTART\s+(-?\d+)"))
}

fn timeout_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| regex_case_insensitive(r"\bTIMEOUT\s+(\S+?)\s*(?:;|$)"))
}

/// Parse one `DEFINE SEQUENCE` statement.
///
/// Returns `None` when the definition is empty. A missing `BATCH` / `START`
/// falls back to the engine defaults.
pub fn parse_sequence(name: &str, definition: &str) -> Option<SequenceDefinition> {
    if definition.is_empty() {
        return None;
    }
    let batch = batch_regex()
        .captures(definition)
        .and_then(|c| c.get(1))
        .and_then(|m| m.as_str().parse().ok())
        .unwrap_or(DEFAULT_BATCH);
    let start = start_regex()
        .captures(definition)
        .and_then(|c| c.get(1))
        .and_then(|m| m.as_str().parse().ok())
        .unwrap_or(DEFAULT_START);
    let timeout = timeout_regex()
        .captures(definition)
        .and_then(|c| c.get(1))
        .map(|m| m.as_str().to_string());

    let mut sequence = SequenceDefinition::new(name)
        .with_batch(batch)
        .with_start(start);
    sequence.timeout = timeout;
    Some(sequence)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_definition_is_none() {
        assert!(parse_sequence("s", "").is_none());
    }

    #[test]
    fn engine_echo_of_a_bare_sequence() {
        let s = parse_sequence("bare", "DEFINE SEQUENCE bare BATCH 1000 START 0").unwrap();
        assert_eq!(s, SequenceDefinition::new("bare"));
    }

    #[test]
    fn engine_echo_with_every_clause() {
        let s = parse_sequence("s", "DEFINE SEQUENCE s BATCH 500 START 10 TIMEOUT 5s").unwrap();
        assert_eq!(s.batch, 500);
        assert_eq!(s.start, 10);
        assert_eq!(s.timeout.as_deref(), Some("5s"));
    }

    #[test]
    fn a_negative_start_round_trips() {
        let s = parse_sequence("s", "DEFINE SEQUENCE s BATCH 10 START -5").unwrap();
        assert_eq!(s.start, -5);
    }

    #[test]
    fn missing_clauses_fall_back_to_the_engine_defaults() {
        let s = parse_sequence("s", "DEFINE SEQUENCE s").unwrap();
        assert_eq!(s.batch, DEFAULT_BATCH);
        assert_eq!(s.start, DEFAULT_START);
        assert!(s.timeout.is_none());
    }

    #[test]
    fn round_trips_through_the_renderer() {
        let code = SequenceDefinition::new("s")
            .with_batch(7)
            .with_timeout("2s");
        let parsed = parse_sequence("s", code.to_surql().unwrap().trim_end_matches(';')).unwrap();
        assert_eq!(parsed, code);
    }
}
