//! `DEFINE SEQUENCE` parser.
//!
//! Extracts [`SequenceDefinition`] values from the `sequences` map of an
//! `INFO FOR DB` response, mirroring [`super::bucket`]. The engine always
//! echoes `BATCH` and `START`, so the parser and the renderer agree on the
//! defaults rather than one of them omitting them.

use super::scan::{define_head, tokens};
use crate::schema::sequence::{SequenceDefinition, DEFAULT_BATCH, DEFAULT_START};

/// Parse one `DEFINE SEQUENCE` statement.
///
/// Returns `None` when the definition is empty. A missing `BATCH` / `START`
/// falls back to the engine defaults.
pub fn parse_sequence(name: &str, definition: &str) -> Option<SequenceDefinition> {
    if definition.is_empty() {
        return None;
    }
    let rest = define_head(definition, "SEQUENCE", false).map_or(definition, |head| head.rest);
    let toks = tokens(rest);
    let value_of = |keyword: &str| {
        toks.windows(2)
            .find(|pair| pair.first().is_some_and(|t| t.is(keyword)))
            .and_then(|pair| pair.get(1))
            .map(|t| t.text.trim_end_matches(';'))
    };
    let batch = value_of("BATCH")
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_BATCH);
    let start = value_of("START")
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_START);

    let mut sequence = SequenceDefinition::new(name)
        .with_batch(batch)
        .with_start(start);
    sequence.timeout = value_of("TIMEOUT").map(str::to_string);
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
