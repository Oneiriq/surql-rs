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

// --- Regex accessors ---------------------------------------------------------

pub(super) fn type_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| regex_case_insensitive(r"TYPE\s+(\w+)"))
}

fn readonly_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| regex_case_insensitive(r"\bREADONLY\b"))
}

fn flexible_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| regex_case_insensitive(r"\bFLEXIBLE\b"))
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

/// Parse one `DEFINE FIELD` statement.
///
/// Returns `None` when the definition string is empty.
pub fn parse_field(name: &str, definition: &str) -> Option<FieldDefinition> {
    if definition.is_empty() {
        return None;
    }
    Some(FieldDefinition {
        name: name.to_string(),
        field_type: extract_field_type(definition),
        assertion: extract_assertion(definition),
        default: extract_default(definition),
        value: extract_value(definition),
        permissions: None,
        readonly: extract_readonly(definition),
        flexible: extract_flexible(definition),
    })
}

// --- Field extractors --------------------------------------------------------

fn extract_field_type(definition: &str) -> FieldType {
    let Some(caps) = type_regex().captures(definition) else {
        return FieldType::Any;
    };
    let Some(m) = caps.get(1) else {
        return FieldType::Any;
    };
    match m.as_str().to_ascii_lowercase().as_str() {
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
        _ => FieldType::Any,
    }
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
            let left_ok = if require_whitespace_left {
                i == 0 || bytes[i - 1].is_ascii_whitespace()
            } else {
                i == 0 || !is_ident_byte(bytes[i - 1])
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

fn extract_assertion(definition: &str) -> Option<String> {
    extract_clause(
        definition,
        "ASSERT",
        &["DEFAULT", "VALUE", "READONLY", "FLEXIBLE", "PERMISSIONS"],
    )
}

fn extract_default(definition: &str) -> Option<String> {
    extract_clause(
        definition,
        "DEFAULT",
        &["VALUE", "READONLY", "FLEXIBLE", "PERMISSIONS", "ASSERT"],
    )
}

fn extract_value(definition: &str) -> Option<String> {
    extract_clause(
        definition,
        "VALUE",
        &["DEFAULT", "READONLY", "FLEXIBLE", "PERMISSIONS", "ASSERT"],
    )
}

fn extract_readonly(definition: &str) -> bool {
    readonly_regex().is_match(definition)
}

fn extract_flexible(definition: &str) -> bool {
    flexible_regex().is_match(definition)
}
