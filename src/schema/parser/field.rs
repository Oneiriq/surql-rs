//! `DEFINE FIELD` parser.
//!
//! Extracts [`FieldDefinition`] values from the SurrealDB
//! `INFO FOR TABLE` response strings. Split out of the monolithic
//! `parser.rs` so each parser submodule stays under the repo's 1000-LOC
//! budget; see parent [`super`] for the public entry points.

use std::sync::OnceLock;

use regex::Regex;

use super::regex_case_insensitive;
use crate::schema::fields::{FieldDefinition, FieldType};
use crate::schema::reference::ReferenceAction;

// --- Regex accessors ---------------------------------------------------------

pub(super) fn type_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| regex_case_insensitive(r"\bTYPE\s+(\w+)"))
}

fn readonly_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| regex_case_insensitive(r"\bREADONLY\b"))
}

fn flexible_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| regex_case_insensitive(r"\bFLEXIBLE\b"))
}

fn record_target_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| regex_case_insensitive(r"record\s*<\s*(\w+)\s*>"))
}

/// Matches the `REFERENCE` marker and its optional `ON DELETE <action>` tail.
/// A bare `REFERENCE` means `ON DELETE IGNORE`, which is also what the engine
/// echoes back from `INFO FOR TABLE`.
fn reference_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| regex_case_insensitive(r"\bREFERENCE\b(?:\s+ON\s+DELETE\s+(\w+))?"))
}

/// Matches `TYPE option<inner>` where `inner` is a bare type word or a
/// single-level generic like `record<blob>` — exactly the shapes the
/// emitter produces. The inner text is captured for type resolution.
fn option_type_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| regex_case_insensitive(r"\bTYPE\s+option\s*<\s*(\w+(?:\s*<\s*\w+\s*>)?)\s*>"))
}

/// Matches the engine's echo form for optional fields: `TYPE none | inner`.
/// The 3.x server reports `option<T>` this way in `INFO FOR TABLE`.
fn none_union_type_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| regex_case_insensitive(r"\bTYPE\s+none\s*\|\s*(\w+(?:\s*<\s*\w+\s*>)?)"))
}

// --- Public parsers ----------------------------------------------------------

/// Parse every entry of a `fd` / `fields` map.
///
/// Entries that fail to parse are skipped; success entries land in the
/// returned vector in the iteration order of the underlying map.
pub fn parse_fields(fd: &std::collections::BTreeMap<String, String>) -> Vec<FieldDefinition> {
    fd.iter()
        .filter_map(|(name, def)| parse_field(name, def))
        .collect()
}

/// Resolve the `REFERENCE` clause. A bare `REFERENCE` and an explicit
/// `REFERENCE ON DELETE IGNORE` both yield [`ReferenceAction::Ignore`].
fn extract_reference(definition: &str) -> Option<ReferenceAction> {
    let caps = reference_regex().captures(definition)?;
    Some(
        caps.get(1)
            .and_then(|m| ReferenceAction::from_keyword(m.as_str()))
            .unwrap_or(ReferenceAction::Ignore),
    )
}

fn extract_computed(definition: &str) -> Option<String> {
    extract_clause(definition, "COMPUTED", &terminators_excluding("COMPUTED"))
}

/// Parse one `DEFINE FIELD` statement.
///
/// Returns `None` when the definition string is empty.
pub fn parse_field(name: &str, definition: &str) -> Option<FieldDefinition> {
    if definition.is_empty() {
        return None;
    }
    let (field_type, nullable) = extract_field_type(definition);
    Some(FieldDefinition {
        name: name.to_string(),
        field_type,
        assertion: extract_assertion(definition),
        default: extract_default(definition),
        value: extract_value(definition),
        permissions: None,
        readonly: extract_readonly(definition),
        flexible: extract_flexible(definition),
        target_table: extract_target_table(definition),
        nullable,
        reference: extract_reference(definition),
        computed: extract_computed(definition),
    })
}

// --- Field extractors --------------------------------------------------------

/// Resolve the field type and whether it is `option<...>`-wrapped.
///
/// `option<inner>` is checked first — the plain `TYPE \w+` regex would
/// capture the word `option` and fall through to [`FieldType::Any`],
/// which would break the code/database round-trip and make migration
/// diffing flap on every nullable field.
fn extract_field_type(definition: &str) -> (FieldType, bool) {
    if let Some(caps) = none_union_type_regex().captures(definition) {
        let inner = caps[1].to_ascii_lowercase();
        let word = inner.split('<').next().unwrap_or("").trim().to_string();
        return (field_type_from_word(&word), true);
    }
    if let Some(caps) = option_type_regex().captures(definition) {
        let inner = caps[1].to_ascii_lowercase();
        let word = inner.split('<').next().unwrap_or("").trim().to_string();
        return (field_type_from_word(&word), true);
    }
    let Some(caps) = type_regex().captures(definition) else {
        return (FieldType::Any, false);
    };
    let Some(m) = caps.get(1) else {
        return (FieldType::Any, false);
    };
    (
        field_type_from_word(&m.as_str().to_ascii_lowercase()),
        false,
    )
}

fn field_type_from_word(word: &str) -> FieldType {
    match word {
        "string" => FieldType::String,
        "int" => FieldType::Int,
        "float" => FieldType::Float,
        "bool" => FieldType::Bool,
        "datetime" => FieldType::Datetime,
        "duration" => FieldType::Duration,
        "decimal" => FieldType::Decimal,
        "number" => FieldType::Number,
        "object" => FieldType::Object,
        "array" => FieldType::Array,
        "record" => FieldType::Record,
        "geometry" => FieldType::Geometry,
        "file" => FieldType::File,
        "bytes" => FieldType::Bytes,
        _ => FieldType::Any,
    }
}

/// Extract the target table from a `record<table>` TYPE clause, if present.
fn extract_target_table(definition: &str) -> Option<String> {
    record_target_regex()
        .captures(definition)
        .map(|caps| caps[1].to_string())
}

/// Locate the case-insensitive keyword `kw` in `text` only at word boundaries
/// (ASCII boundaries). Returns the byte offset at which the keyword starts.
///
/// When `require_whitespace_left` is true, the keyword must be preceded by
/// whitespace or sit at byte 0 (a `$`-prefixed identifier like `$value` does
/// not satisfy this, and therefore will not be mis-identified as a clause
/// terminator).
fn find_keyword(text: &str, kw: &str, require_whitespace_left: bool) -> Option<usize> {
    let text_upper = text.to_ascii_uppercase();
    let kw_upper = kw.to_ascii_uppercase();
    let bytes = text_upper.as_bytes();
    let needle = kw_upper.as_bytes();
    if needle.is_empty() {
        return None;
    }
    let mut i = 0;
    while i + needle.len() <= bytes.len() {
        if bytes[i..i + needle.len()] == *needle {
            // `$` blocks a match: `$value` must never read as the
            // VALUE keyword, or every ASSERT mentioning it grows a
            // phantom VALUE clause.
            let left_ok = if require_whitespace_left {
                i == 0 || bytes[i - 1].is_ascii_whitespace()
            } else {
                i == 0 || (!is_ident_byte(bytes[i - 1]) && bytes[i - 1] != b'$')
            };
            let right_ok =
                i + needle.len() == bytes.len() || !is_ident_byte(bytes[i + needle.len()]);
            if left_ok && right_ok {
                return Some(i);
            }
        }
        i += 1;
    }
    None
}

fn is_ident_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

/// Extract the body of a `KEYWORD <body> [TERMINATOR | ;]` clause.
///
/// `terminators` lists other keywords that would end the clause; any such
/// occurrence after the `keyword` anchor truncates the body. A trailing
/// semicolon is always stripped.
fn extract_clause(definition: &str, keyword: &str, terminators: &[&str]) -> Option<String> {
    let start = find_keyword(definition, keyword, false)?;
    let after_kw = start + keyword.len();
    // Require at least one whitespace after the keyword (matches `\s+`).
    let rest_start = definition[after_kw..]
        .find(|c: char| !c.is_whitespace())
        .map(|off| after_kw + off)?;
    // Ensure we actually consumed whitespace between the keyword and the body.
    if rest_start == after_kw {
        return None;
    }
    let tail = &definition[rest_start..];

    let mut end = tail.len();
    for term in terminators {
        if let Some(pos) = find_keyword(tail, term, true) {
            if pos < end {
                end = pos;
            }
        }
    }
    if let Some(pos) = tail.find(';') {
        if pos < end {
            end = pos;
        }
    }

    let body = tail[..end].trim();
    if body.is_empty() {
        return None;
    }
    Some(body.to_string())
}

/// Clause keywords that can follow any of `ASSERT` / `DEFAULT` / `VALUE` /
/// `COMPUTED` and therefore terminate its body. `REFERENCE` is in the list
/// because the engine echoes it *after* `ASSERT`
/// (`... ASSERT true REFERENCE ON DELETE REJECT ...`), which would otherwise
/// swallow the whole reference clause into the assertion.
const CLAUSE_TERMINATORS: &[&str] = &[
    "ASSERT",
    "DEFAULT",
    "VALUE",
    "COMPUTED",
    "READONLY",
    "FLEXIBLE",
    "REFERENCE",
    "PERMISSIONS",
    "COMMENT",
];

/// [`CLAUSE_TERMINATORS`] minus the clause being extracted.
fn terminators_excluding(keyword: &str) -> Vec<&'static str> {
    CLAUSE_TERMINATORS
        .iter()
        .copied()
        .filter(|k| *k != keyword)
        .collect()
}

fn extract_assertion(definition: &str) -> Option<String> {
    extract_clause(definition, "ASSERT", &terminators_excluding("ASSERT"))
}

fn extract_default(definition: &str) -> Option<String> {
    extract_clause(definition, "DEFAULT", &terminators_excluding("DEFAULT"))
}

fn extract_value(definition: &str) -> Option<String> {
    extract_clause(definition, "VALUE", &terminators_excluding("VALUE"))
}

fn extract_readonly(definition: &str) -> bool {
    readonly_regex().is_match(definition)
}

fn extract_flexible(definition: &str) -> bool {
    flexible_regex().is_match(definition)
}

#[cfg(test)]
mod echo_tests {
    use crate::schema::fields::FieldType;
    use crate::schema::reference::ReferenceAction;

    /// The engine echoes a bare `REFERENCE` with its default action spelled
    /// out; the renderer does the same, so the pair compares equal.
    #[test]
    fn engine_echo_reference_round_trips() {
        let field = super::parse_field(
            "author",
            "DEFINE FIELD author ON comment TYPE none | record<person> \
             REFERENCE ON DELETE CASCADE PERMISSIONS FULL",
        )
        .unwrap();
        assert_eq!(field.field_type, FieldType::Record);
        assert!(field.nullable);
        assert_eq!(field.target_table.as_deref(), Some("person"));
        assert_eq!(field.reference, Some(ReferenceAction::Cascade));
        assert_eq!(
            field.to_surql("comment"),
            "DEFINE FIELD author ON TABLE comment TYPE option<record<person>> \
             REFERENCE ON DELETE CASCADE;"
        );
    }

    #[test]
    fn bare_reference_reads_as_the_ignore_default() {
        let field = super::parse_field(
            "f",
            "DEFINE FIELD f ON comment TYPE record<person> REFERENCE",
        )
        .unwrap();
        assert_eq!(field.reference, Some(ReferenceAction::Ignore));
    }

    #[test]
    fn array_of_record_reference_round_trips() {
        let field = super::parse_field(
            "c",
            "DEFINE FIELD c ON comment TYPE array<record<person>> \
             REFERENCE ON DELETE UNSET PERMISSIONS FULL",
        )
        .unwrap();
        assert_eq!(field.field_type, FieldType::Array);
        assert_eq!(field.target_table.as_deref(), Some("person"));
        assert_eq!(field.reference, Some(ReferenceAction::Unset));
        assert_eq!(
            field.to_surql("comment"),
            "DEFINE FIELD c ON TABLE comment TYPE array<record<person>> \
             REFERENCE ON DELETE UNSET;"
        );
    }

    /// The engine emits `ASSERT <expr> REFERENCE ...`, so the assertion body
    /// must stop at the `REFERENCE` keyword.
    #[test]
    fn reference_after_assert_does_not_leak_into_the_assertion() {
        let field = super::parse_field(
            "b",
            "DEFINE FIELD b ON comment TYPE none | record<person> ASSERT true \
             REFERENCE ON DELETE REJECT PERMISSIONS FULL",
        )
        .unwrap();
        assert_eq!(field.assertion.as_deref(), Some("true"));
        assert_eq!(field.reference, Some(ReferenceAction::Reject));
    }

    #[test]
    fn computed_field_round_trips_without_a_type() {
        let field = super::parse_field(
            "comments",
            "DEFINE FIELD comments ON person COMPUTED <~comment PERMISSIONS FULL",
        )
        .unwrap();
        assert_eq!(field.field_type, FieldType::Any);
        assert_eq!(field.computed.as_deref(), Some("<~comment"));
        assert!(field.reference.is_none());
        assert_eq!(
            field.to_surql("person"),
            "DEFINE FIELD comments ON TABLE person COMPUTED <~comment;"
        );
    }

    #[test]
    fn computed_field_keeps_a_declared_type() {
        let field = super::parse_field(
            "c1",
            "DEFINE FIELD c1 ON comment TYPE none | string COMPUTED 'x' PERMISSIONS FULL",
        )
        .unwrap();
        assert_eq!(field.field_type, FieldType::String);
        assert!(field.nullable);
        assert_eq!(field.computed.as_deref(), Some("'x'"));
        assert_eq!(
            field.to_surql("comment"),
            "DEFINE FIELD c1 ON TABLE comment TYPE option<string> COMPUTED 'x';"
        );
    }

    #[test]
    fn a_plain_field_gains_no_reference_or_computed() {
        let field =
            super::parse_field("text", "DEFINE FIELD text ON comment TYPE none | string").unwrap();
        assert!(field.reference.is_none());
        assert!(field.computed.is_none());
    }

    #[test]
    fn engine_echo_none_union_parses_as_nullable() {
        let field = super::parse_field(
            "expires_at",
            "DEFINE FIELD expires_at ON access_grant TYPE none | datetime PERMISSIONS FULL",
        )
        .unwrap();
        assert_eq!(field.field_type, FieldType::Datetime);
        assert!(field.nullable);

        let field = super::parse_field(
            "file",
            "DEFINE FIELD file ON access_grant TYPE none | record<file> PERMISSIONS FULL",
        )
        .unwrap();
        assert_eq!(field.field_type, FieldType::Record);
        assert!(field.nullable);
        assert_eq!(field.target_table.as_deref(), Some("file"));
    }

    #[test]
    fn dollar_value_never_reads_as_the_value_keyword() {
        let field = super::parse_field(
            "op",
            "DEFINE FIELD op ON access_grant TYPE string DEFAULT 'get' ASSERT $value INSIDE ['x'] PERMISSIONS FULL",
        )
        .unwrap();
        assert!(field.value.is_none(), "{:?}", field.value);
        assert!(field.assertion.as_deref().unwrap().contains("INSIDE"));
    }
}
