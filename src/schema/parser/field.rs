//! `DEFINE FIELD` parser.
//!
//! Extracts [`FieldDefinition`] values from the SurrealDB
//! `INFO FOR TABLE` response strings. Split out of the monolithic
//! `parser.rs` so each parser submodule stays under the repo's 1000-LOC
//! budget; see parent [`super`] for the public entry points.

use super::permissions::{parse_permissions_body, Owner};
use super::scan::{clause, clauses, define_head, unquote_ident, Clause, Shape};
use crate::schema::fields::{FieldDefinition, FieldType};
use crate::schema::reference::ReferenceAction;

/// The clauses of a `DEFINE FIELD` statement. The engine echoes them as
/// `TYPE … [FLEXIBLE] [DEFAULT …] [READONLY] [VALUE …] [ASSERT …]
/// [COMPUTED …] [REFERENCE …] [COMMENT …] PERMISSIONS …`; this crate renders
/// a different order, and both are read the same way.
const FIELD_CLAUSES: &[(&str, Shape)] = &[
    ("TYPE", Shape::Expr),
    ("FLEXIBLE", Shape::Flag),
    ("DEFAULT", Shape::Expr),
    ("READONLY", Shape::Flag),
    ("VALUE", Shape::Expr),
    ("ASSERT", Shape::Expr),
    ("COMPUTED", Shape::Expr),
    ("REFERENCE", Shape::Flag),
    ("COMMENT", Shape::Str),
    ("PERMISSIONS", Shape::Expr),
];

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
fn extract_reference(found: &[Clause<'_>]) -> Option<ReferenceAction> {
    let body = clause(found, "REFERENCE")?;
    let action = body
        .split_whitespace()
        .collect::<Vec<_>>()
        .get(2)
        .and_then(|word| ReferenceAction::from_keyword(word));
    Some(action.unwrap_or(ReferenceAction::Ignore))
}

/// An expression clause's body, `None` when absent or empty.
fn expression(found: &[Clause<'_>], keyword: &str) -> Option<String> {
    clause(found, keyword)
        .filter(|body| !body.is_empty())
        .map(str::to_string)
}

/// Parse one `DEFINE FIELD` statement.
///
/// Clauses are read after the `DEFINE FIELD <name> ON <table>` head and
/// outside quotes and brackets, so a field named `default`, a default of
/// `'no comment'`, or an assertion over `(SELECT VALUE name FROM tag)`
/// all keep their meaning. Field `PERMISSIONS` read back into the same
/// per-action map [`FieldDefinition::permissions`] renders from, keeping
/// only actions that differ from the field default (`FULL`).
///
/// Returns `None` when the definition string is empty.
pub fn parse_field(name: &str, definition: &str) -> Option<FieldDefinition> {
    if definition.trim().is_empty() {
        return None;
    }
    let body = define_head(definition, "FIELD", true).map_or(definition, |head| head.rest);
    let found = clauses(body, FIELD_CLAUSES);
    let has = |keyword: &str| clause(&found, keyword).is_some();
    let kind = clause(&found, "TYPE").map(parse_kind).unwrap_or_default();
    Some(FieldDefinition {
        name: name.to_string(),
        field_type: kind.field_type,
        assertion: expression(&found, "ASSERT"),
        default: expression(&found, "DEFAULT"),
        value: expression(&found, "VALUE"),
        permissions: clause(&found, "PERMISSIONS")
            .and_then(|perms| parse_permissions_body(perms, Owner::Field)),
        readonly: has("READONLY"),
        flexible: has("FLEXIBLE"),
        target_table: kind.target_table,
        nullable: kind.nullable,
        reference: extract_reference(&found),
        computed: expression(&found, "COMPUTED"),
    })
}

// --- Field type --------------------------------------------------------------

/// What a `TYPE` clause says about a field.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Kind {
    field_type: FieldType,
    nullable: bool,
    target_table: Option<String>,
}

impl Default for Kind {
    fn default() -> Self {
        Self {
            field_type: FieldType::Any,
            nullable: false,
            target_table: None,
        }
    }
}

/// Split a type at top-level `|`, ignoring the `|` inside `record<a | b>`.
fn split_union(text: &str) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut depth = 0usize;
    let mut start = 0;
    for (at, c) in text.char_indices() {
        match c {
            '<' | '(' | '[' | '{' => depth += 1,
            '>' | ')' | ']' | '}' => depth = depth.saturating_sub(1),
            '|' if depth == 0 => {
                parts.push(text.get(start..at).unwrap_or("").trim());
                start = at + 1;
            }
            _ => {}
        }
    }
    parts.push(text.get(start..).unwrap_or("").trim());
    parts
}

/// The text between a generic's outer `<` and its last `>`.
fn generic_inner(text: &str) -> Option<&str> {
    let open = text.find('<')?;
    let close = text.rfind('>')?;
    text.get(open + 1..close).map(str::trim)
}

/// Resolve a `TYPE` clause body: the base type, whether it accepts `NONE`,
/// and the linked table of a `record<t>` / `array<record<t>>`.
///
/// The engine echoes `option<T>` as `none | T`; the code side renders
/// `option<T>`. Both read as nullable `T`, nested generics included
/// (`option<array<record<t>>>` is a nullable array linked to `t`). A union of
/// several non-`none` types has no [`FieldType`] and reads as
/// [`FieldType::Any`].
fn parse_kind(text: &str) -> Kind {
    let mut nullable = false;
    let mut members: Vec<&str> = split_union(text);
    let mut rounds = 0;
    loop {
        members.retain(|m| {
            let none = m.eq_ignore_ascii_case("none");
            nullable |= none;
            !none && !m.is_empty()
        });
        let unwrapped = match members.as_slice() {
            [only] if starts_with_word(only, "option") => generic_inner(only),
            _ => None,
        };
        match unwrapped {
            Some(inner) if rounds < 8 => {
                nullable = true;
                members = split_union(inner);
                rounds += 1;
            }
            _ => break,
        }
    }
    let [only] = members.as_slice() else {
        return Kind {
            nullable,
            ..Kind::default()
        };
    };
    let base = only
        .split('<')
        .next()
        .unwrap_or("")
        .trim()
        .to_ascii_lowercase();
    let field_type = field_type_from_word(&base);
    let target_table = match field_type {
        FieldType::Record => generic_inner(only).and_then(record_targets),
        FieldType::Array => generic_inner(only)
            .and_then(|inner| split_generic_args(inner).into_iter().next())
            .filter(|item| starts_with_word(item, "record"))
            .and_then(generic_inner)
            .and_then(record_targets),
        _ => None,
    };
    Kind {
        field_type,
        nullable,
        target_table,
    }
}

/// `true` when `text` starts with the word `word` followed by `<`.
fn starts_with_word(text: &str, word: &str) -> bool {
    text.get(..word.len())
        .is_some_and(|head| head.eq_ignore_ascii_case(word))
        && text
            .get(word.len()..)
            .is_some_and(|rest| rest.trim_start().starts_with('<'))
}

/// Split generic arguments (`record<t>, 10`) at top-level commas.
fn split_generic_args(text: &str) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut depth = 0usize;
    let mut start = 0;
    for (at, c) in text.char_indices() {
        match c {
            '<' => depth += 1,
            '>' => depth = depth.saturating_sub(1),
            ',' if depth == 0 => {
                parts.push(text.get(start..at).unwrap_or("").trim());
                start = at + 1;
            }
            _ => {}
        }
    }
    parts.push(text.get(start..).unwrap_or("").trim());
    parts
}

/// The tables of a `record<a | b>`, unquoted and joined the way they render.
fn record_targets(inner: &str) -> Option<String> {
    let names: Vec<String> = inner
        .split('|')
        .map(|name| unquote_ident(name.trim()))
        .filter(|name| !name.is_empty())
        .collect();
    (!names.is_empty()).then(|| names.join(" | "))
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

#[cfg(test)]
mod echo_tests {
    use std::collections::BTreeMap;

    use crate::schema::fields::FieldType;
    use crate::schema::reference::ReferenceAction;

    #[test]
    fn a_field_named_after_a_clause_keyword_reads_its_real_clauses() {
        let f = super::parse_field(
            "reference",
            "DEFINE FIELD reference ON invoice TYPE string PERMISSIONS FULL",
        )
        .unwrap();
        assert!(f.reference.is_none());
        assert_eq!(f.field_type, FieldType::String);
        let f = super::parse_field(
            "default",
            "DEFINE FIELD default ON card TYPE bool DEFAULT false",
        )
        .unwrap();
        assert_eq!(f.default.as_deref(), Some("false"));
        let f = super::parse_field(
            "value",
            "DEFINE FIELD `value` ON user TYPE int VALUE 1 PERMISSIONS FULL",
        )
        .unwrap();
        assert_eq!(f.value.as_deref(), Some("1"));
    }

    #[test]
    fn nested_and_quoted_keywords_stay_in_their_clause() {
        let f = super::parse_field(
            "h",
            "DEFINE FIELD h ON t4 TYPE string DEFAULT 'no comment' ASSERT $value INSIDE \
             (SELECT VALUE name FROM tag) COMMENT 'c' PERMISSIONS FOR select NONE, \
             FOR create, update FULL",
        )
        .unwrap();
        assert_eq!(f.default.as_deref(), Some("'no comment'"));
        assert_eq!(
            f.assertion.as_deref(),
            Some("$value INSIDE (SELECT VALUE name FROM tag)")
        );
        assert!(f.value.is_none());
        let f = super::parse_field(
            "i",
            "DEFINE FIELD i ON t4 TYPE string READONLY ASSERT $value INSIDE ['readonly', 'x'] \
             PERMISSIONS FULL",
        )
        .unwrap();
        assert!(f.readonly);
        let f = super::parse_field(
            "i",
            "DEFINE FIELD i ON t4 TYPE string ASSERT $value INSIDE ['readonly', 'x']",
        )
        .unwrap();
        assert!(!f.readonly);
    }

    #[test]
    fn field_permissions_read_back_without_the_full_default() {
        let f = super::parse_field(
            "ssn",
            "DEFINE FIELD ssn ON user TYPE string PERMISSIONS FOR select WHERE $auth.admin, \
             FOR create, update FULL",
        )
        .unwrap();
        let expected: BTreeMap<String, String> =
            [("select".to_string(), "$auth.admin".to_string())].into();
        assert_eq!(f.permissions, Some(expected));
        let f =
            super::parse_field("x", "DEFINE FIELD x ON t TYPE string PERMISSIONS FULL").unwrap();
        assert!(f.permissions.is_none());
    }

    #[test]
    fn nested_option_generics_read_as_nullable_links() {
        let f = super::parse_field("f", "DEFINE FIELD f ON t TYPE option<array<record<user>>>")
            .unwrap();
        assert_eq!(f.field_type, FieldType::Array);
        assert!(f.nullable);
        assert_eq!(f.target_table.as_deref(), Some("user"));
        let f = super::parse_field(
            "f",
            "DEFINE FIELD f ON t4 TYPE none | array<record<user>> PERMISSIONS FULL",
        )
        .unwrap();
        assert_eq!(f.field_type, FieldType::Array);
        assert!(f.nullable);
        assert_eq!(f.target_table.as_deref(), Some("user"));
        let f = super::parse_field("g", "DEFINE FIELD g ON t4 TYPE array<string> | int").unwrap();
        assert_eq!(f.field_type, FieldType::Any);
        let f = super::parse_field("j", "DEFINE FIELD j ON t4 TYPE record<user | post>").unwrap();
        assert_eq!(f.target_table.as_deref(), Some("user | post"));
    }

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
