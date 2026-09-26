//! Type-safe `RecordID` wrapper for SurrealDB `table:id` identifiers.
//!
//! Port of `surql/types/record_id.py`. Supports angle-bracket syntax for
//! complex IDs (containing dots, dashes, colons, etc.) and integer IDs.

use std::fmt;
use std::marker::PhantomData;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::escape::{is_identifier, quote_record_key, unescape};
use crate::error::{Result, SurqlError};

/// Value held by a [`RecordID`].
///
/// Matches Python's `id: str | int`; integer IDs never require angle brackets.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum RecordIdValue {
    /// String-valued identifier.
    String(String),
    /// 64-bit signed integer identifier.
    Int(i64),
}

impl fmt::Display for RecordIdValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::String(s) => f.write_str(s),
            Self::Int(n) => write!(f, "{n}"),
        }
    }
}

impl From<&str> for RecordIdValue {
    fn from(s: &str) -> Self {
        Self::String(s.to_owned())
    }
}

impl From<String> for RecordIdValue {
    fn from(s: String) -> Self {
        Self::String(s)
    }
}

impl From<i64> for RecordIdValue {
    fn from(n: i64) -> Self {
        Self::Int(n)
    }
}

impl From<i32> for RecordIdValue {
    fn from(n: i32) -> Self {
        Self::Int(i64::from(n))
    }
}

/// Type-safe wrapper for a SurrealDB record id (`table:id` format).
///
/// The generic parameter `T` is a phantom tag for compile-time typing of
/// record targets (e.g. `RecordID::<User>::new("user", "alice")`) and does
/// not affect runtime layout.
///
/// ## v3 escape syntax
///
/// SurrealDB v3 expects record-id keys to match either the bare identifier
/// shape `[A-Za-z_][A-Za-z0-9_]*` or to be a pure integer literal. Anything
/// else — dots, hyphens, colons, leading-digit-with-letters like `1abc` —
/// must be wrapped in **unicode** angle brackets `⟨ … ⟩` (U+27E8 / U+27E9).
/// ASCII `<` / `>` is rejected by the v3 parser. `RecordID::Display` auto-
/// wraps the id in unicode brackets whenever it does not match a safe
/// shape; [`RecordID::parse`] accepts both bracket forms on input.
///
/// ## Examples
///
/// ```
/// use surql::types::RecordID;
///
/// let id = RecordID::<()>::new("user", "alice").unwrap();
/// assert_eq!(id.to_string(), "user:alice");
///
/// // Complex ids are auto-wrapped in unicode angle brackets.
/// let complex = RecordID::<()>::new("outlet", "alaskabeacon.com").unwrap();
/// assert_eq!(complex.to_string(), "outlet:⟨alaskabeacon.com⟩");
///
/// // Leading-digit-with-letters now wraps too — pre-0.2.5 the lax regex
/// // emitted these bare and v3 rejected the record with `Unexpected token`.
/// let leading_digit = RecordID::<()>::new("chunk", "1abc").unwrap();
/// assert_eq!(leading_digit.to_string(), "chunk:⟨1abc⟩");
///
/// // A digit-only string id is bracketed so it keeps naming the string
/// // key; the integer id renders bare.
/// let digit_string = RecordID::<()>::new("post", "123").unwrap();
/// assert_eq!(digit_string.to_string(), "post:⟨123⟩");
/// let integer = RecordID::<()>::new("post", 123_i64).unwrap();
/// assert_eq!(integer.to_string(), "post:123");
///
/// let parsed = RecordID::<()>::parse("post:123").unwrap();
/// assert_eq!(parsed.table(), "post");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RecordID<T = ()> {
    table: String,
    id: RecordIdValue,
    _phantom: PhantomData<fn() -> T>,
}

/// The text between `open` and `close` when `s` is exactly wrapped by them.
fn delimited(s: &str, open: char, close: char) -> Option<&str> {
    s.strip_prefix(open)?.strip_suffix(close)
}

/// Strip SurrealDB v3 wire-format angle brackets from a record-id-shaped
/// string.
///
/// SurrealDB v3 wraps record-id keys that contain anything other than
/// `[A-Za-z_][A-Za-z0-9_]*` or pure digits in unicode angle brackets
/// `⟨ … ⟩` (U+27E8 / U+27E9). Downstream callers that want the bare
/// `table:id` shape — for use in API responses, log lines, or string-keyed
/// lookups — previously had to call
/// `value.replace('⟨', "").replace('⟩', "")` themselves at every boundary;
/// this helper centralises that strip.
///
/// Both bracket forms are accepted on input: the v3 unicode brackets
/// (`⟨ … ⟩`) and the legacy ASCII brackets (`< … >`). `None` is returned
/// untouched so the helper is safe to apply unconditionally.
///
/// ## Examples
///
/// ```
/// use surql::types::strip_brackets;
///
/// assert_eq!(
///     strip_brackets(Some("outlet:⟨alaska.com⟩")),
///     Some("outlet:alaska.com".to_string()),
/// );
/// assert_eq!(
///     strip_brackets(Some("plan_chunk:⟨demo-plan-ff3d5981⟩")),
///     Some("plan_chunk:demo-plan-ff3d5981".to_string()),
/// );
/// // Bare ids pass through untouched.
/// assert_eq!(
///     strip_brackets(Some("user:alice")),
///     Some("user:alice".to_string()),
/// );
/// // Legacy ASCII brackets are also stripped.
/// assert_eq!(
///     strip_brackets(Some("outlet:<legacy.com>")),
///     Some("outlet:legacy.com".to_string()),
/// );
/// // `None` is preserved.
/// assert_eq!(strip_brackets(None), None);
/// ```
pub fn strip_brackets(value: Option<&str>) -> Option<String> {
    value.map(|s| {
        s.chars()
            .filter(|c| !matches!(c, '⟨' | '⟩' | '<' | '>'))
            .collect()
    })
}

impl<T> RecordID<T> {
    /// Build a new [`RecordID`] after validating the table name.
    ///
    /// Returns [`SurqlError::Validation`] if the table name is empty or
    /// contains characters other than `[A-Za-z0-9_]` (and must not start
    /// with a digit).
    pub fn new(table: impl Into<String>, id: impl Into<RecordIdValue>) -> Result<Self> {
        let table = table.into();
        Self::validate_table(&table)?;
        Ok(Self {
            table,
            id: id.into(),
            _phantom: PhantomData,
        })
    }

    fn validate_table(name: &str) -> Result<()> {
        if name.is_empty() {
            return Err(SurqlError::Validation {
                reason: "Table name cannot be empty".into(),
            });
        }
        if !is_identifier(name) {
            return Err(SurqlError::Validation {
                reason: format!(
                    "Invalid table name: {name:?}. Must contain only alphanumeric \
                     characters and underscores, and cannot start with a digit"
                ),
            });
        }
        Ok(())
    }

    /// Parse a string of the form `table:id`, `table:⟨id⟩`, or `` table:`id` ``.
    ///
    /// A bare integer-looking id is parsed into [`RecordIdValue::Int`], as the
    /// engine does. A quoted id always stays a [`RecordIdValue::String`], so
    /// `post:⟨123⟩` names the string key `"123"`, not the integer key `123`;
    /// escapes inside the quotes (`\\`, `` \` ``) are reversed. The legacy
    /// ASCII `table:<id>` form is accepted and taken literally.
    pub fn parse(input: &str) -> Result<Self> {
        let Some((table, id_str)) = input.split_once(':') else {
            return Err(SurqlError::Validation {
                reason: format!("Invalid record ID format: {input}. Expected format: table:id"),
            });
        };

        let table = table.trim();
        let id_str = id_str.trim();

        if table.is_empty() {
            return Err(SurqlError::Validation {
                reason: format!("Invalid record ID: table name cannot be empty in {input:?}"),
            });
        }
        if id_str.is_empty() {
            return Err(SurqlError::Validation {
                reason: format!("Invalid record ID: id cannot be empty in {input:?}"),
            });
        }

        // The unicode `⟨ … ⟩` and backtick forms are what `Display` and the
        // engine emit; ASCII `< … >` comes from older serialisers
        // (surql-py < 1.5.11) and never carried escapes.
        let id = if let Some(inner) =
            delimited(id_str, '⟨', '⟩').or_else(|| delimited(id_str, '`', '`'))
        {
            RecordIdValue::String(unescape(inner))
        } else if let Some(inner) = delimited(id_str, '<', '>') {
            RecordIdValue::String(inner.to_owned())
        } else {
            id_str.parse::<i64>().map_or_else(
                |_| RecordIdValue::String(id_str.to_owned()),
                RecordIdValue::Int,
            )
        };

        Self::new(table, id)
    }

    /// The table name portion of the id.
    pub fn table(&self) -> &str {
        &self.table
    }

    /// The id value portion.
    pub fn id(&self) -> &RecordIdValue {
        &self.id
    }

    /// Render as a SurrealQL record id literal.
    pub fn to_surql(&self) -> String {
        self.to_string()
    }
}

impl<T> fmt::Display for RecordID<T> {
    /// Render the record id in SurrealQL `table:id` form.
    ///
    /// Integer ids render bare. String ids render through
    /// [`quote_record_key`](super::escape::quote_record_key): bare when
    /// identifier-shaped, wrapped in unicode angle brackets (U+27E8 / U+27E9)
    /// otherwise, and backtick-quoted when the id itself contains `⟩`. A
    /// digit-only string id is bracketed so it keeps naming the string key.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.id {
            RecordIdValue::Int(n) => write!(f, "{}:{n}", self.table),
            RecordIdValue::String(s) => write!(f, "{}:{}", self.table, quote_record_key(s)),
        }
    }
}

impl<T> Serialize for RecordID<T> {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.collect_str(self)
    }
}

impl<'de, T> Deserialize<'de> for RecordID<T> {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Self::parse(&s).map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn string_id_renders_simple() {
        let id = RecordID::<()>::new("user", "alice").unwrap();
        assert_eq!(id.to_string(), "user:alice");
    }

    #[test]
    fn complex_id_uses_unicode_angle_brackets() {
        // SurrealDB v3 rejects ASCII `<>` with
        // `Unexpected token '<', expected a record-id key` — emit the
        // unicode escape syntax `⟨ … ⟩` (U+27E8 / U+27E9) instead.
        let id = RecordID::<()>::new("outlet", "alaskabeacon.com").unwrap();
        assert_eq!(id.to_string(), "outlet:⟨alaskabeacon.com⟩");
    }

    #[test]
    fn leading_digit_with_letters_wraps_in_unicode_brackets() {
        // Pre-0.2.5 the simple_id_pattern was `[A-Za-z0-9_]+`, which
        // accepted `1abc` bare and produced the v3-rejected literal
        // `chunk:1abc`. The 0.2.5 identifier_id_pattern is
        // `[A-Za-z_][A-Za-z0-9_]*`, so leading-digit-with-letters auto-
        // wraps.
        let id = RecordID::<()>::new("chunk", "1abc").unwrap();
        assert_eq!(id.to_string(), "chunk:⟨1abc⟩");
    }

    #[test]
    fn pure_digit_string_id_stays_a_string_key() {
        // `post:123` is the integer key 123 and `post:⟨123⟩` the string key
        // "123": two different records. A string id must keep naming the
        // string key, and `post:007` would even name the integer 7.
        let id = RecordID::<()>::new("post", "123").unwrap();
        assert_eq!(id.to_string(), "post:⟨123⟩");
        let id = RecordID::<()>::new("post", "007").unwrap();
        assert_eq!(id.to_string(), "post:⟨007⟩");
    }

    #[test]
    fn closing_bracket_in_id_cannot_end_the_key() {
        let id = RecordID::<()>::new("user", "x⟩; DELETE user; --").unwrap();
        assert_eq!(id.to_string(), "user:`x⟩; DELETE user; --`");
    }

    #[test]
    fn backslash_in_id_is_escaped() {
        let id = RecordID::<()>::new("user", r"a\b").unwrap();
        assert_eq!(id.to_string(), r"user:⟨a\\b⟩");
    }

    #[test]
    fn parse_bracketed_digits_keeps_the_string_key() {
        let id = RecordID::<()>::parse("post:⟨123⟩").unwrap();
        assert!(matches!(id.id(), RecordIdValue::String(s) if s == "123"));
        let id = RecordID::<()>::parse("post:`123`").unwrap();
        assert!(matches!(id.id(), RecordIdValue::String(s) if s == "123"));
    }

    #[test]
    fn parse_reverses_escapes() {
        let id = RecordID::<()>::parse(r"user:`x⟩\`y\\z`").unwrap();
        assert!(matches!(id.id(), RecordIdValue::String(s) if s == r"x⟩`y\z"));
    }

    #[test]
    fn display_then_parse_round_trips() {
        let ids = [
            "alice", "123", "-5", "007", "a-b", "a:b", "x⟩y", r"a\b", "a`b", "é", " ", "⟨",
        ];
        for raw in ids {
            let id = RecordID::<()>::new("t", raw).unwrap();
            let back = RecordID::<()>::parse(&id.to_string()).unwrap();
            assert_eq!(back, id, "{id}");
        }
        let id = RecordID::<()>::new("t", -5_i64).unwrap();
        assert_eq!(RecordID::<()>::parse(&id.to_string()).unwrap(), id);
    }

    #[test]
    fn integer_id_never_brackets() {
        let id = RecordID::<()>::new("post", 123i64).unwrap();
        assert_eq!(id.to_string(), "post:123");
    }

    #[test]
    fn parse_simple_string() {
        let id = RecordID::<()>::parse("user:alice").unwrap();
        assert_eq!(id.table(), "user");
        assert!(matches!(id.id(), RecordIdValue::String(s) if s == "alice"));
    }

    #[test]
    fn parse_integer_id() {
        let id = RecordID::<()>::parse("post:42").unwrap();
        assert!(matches!(id.id(), RecordIdValue::Int(42)));
    }

    #[test]
    fn parse_ascii_angle_brackets_strips_them() {
        // Legacy ASCII brackets — accepted on input for backwards-compat
        // with older surql-py / surql wire formats.
        let id = RecordID::<()>::parse("outlet:<alaskabeacon.com>").unwrap();
        assert_eq!(id.table(), "outlet");
        assert!(matches!(id.id(), RecordIdValue::String(s) if s == "alaskabeacon.com"));
    }

    #[test]
    fn parse_unicode_angle_brackets_strips_them() {
        // The form RecordID::Display emits — must round-trip through
        // parse without losing the original id.
        let id = RecordID::<()>::parse("outlet:⟨alaskabeacon.com⟩").unwrap();
        assert_eq!(id.table(), "outlet");
        assert!(matches!(id.id(), RecordIdValue::String(s) if s == "alaskabeacon.com"));
    }

    #[test]
    fn real_world_plan_chunk_id_round_trip() {
        // Hyphenated composite id from a real builder-graph workload.
        // The 0.2.5 release pins the end-to-end round trip so consumers
        // can drop their own .replace('⟨', "").replace('⟩', "") calls.
        let rid = RecordID::<()>::parse("plan_chunk:⟨demo-plan-ff3d5981⟩").unwrap();
        assert!(matches!(rid.id(), RecordIdValue::String(s) if s == "demo-plan-ff3d5981"));
        assert_eq!(rid.to_string(), "plan_chunk:⟨demo-plan-ff3d5981⟩");
    }

    #[test]
    fn parse_rejects_missing_colon() {
        assert!(RecordID::<()>::parse("user").is_err());
    }

    #[test]
    fn parse_rejects_empty_table() {
        assert!(RecordID::<()>::parse(":id").is_err());
    }

    #[test]
    fn parse_rejects_empty_id() {
        assert!(RecordID::<()>::parse("user:").is_err());
    }

    #[test]
    fn new_rejects_empty_table() {
        assert!(RecordID::<()>::new("", "alice").is_err());
    }

    #[test]
    fn new_rejects_hyphen_in_table() {
        assert!(RecordID::<()>::new("user-name", "alice").is_err());
    }

    #[test]
    fn new_rejects_leading_digit() {
        assert!(RecordID::<()>::new("1user", "alice").is_err());
    }

    #[test]
    fn serde_json_roundtrip() {
        let id = RecordID::<()>::new("user", "alice").unwrap();
        let json = serde_json::to_string(&id).unwrap();
        assert_eq!(json, "\"user:alice\"");
        let back: RecordID<()> = serde_json::from_str(&json).unwrap();
        assert_eq!(back, id);
    }

    #[test]
    fn serde_json_roundtrip_complex() {
        let id = RecordID::<()>::new("outlet", "alaskabeacon.com").unwrap();
        let json = serde_json::to_string(&id).unwrap();
        // serde_json emits the unicode characters literally (UTF-8); the
        // serialised form is the v3-escape syntax `⟨ … ⟩`.
        assert_eq!(json, "\"outlet:⟨alaskabeacon.com⟩\"");
        let back: RecordID<()> = serde_json::from_str(&json).unwrap();
        assert_eq!(back, id);
    }

    // ---- strip_brackets ------------------------------------------------------

    #[test]
    fn strip_brackets_unicode_form() {
        assert_eq!(
            strip_brackets(Some("outlet:⟨alaska.com⟩")),
            Some("outlet:alaska.com".to_string()),
        );
    }

    #[test]
    fn strip_brackets_hyphenated_id() {
        assert_eq!(
            strip_brackets(Some("plan_chunk:⟨demo-plan-ff3d5981⟩")),
            Some("plan_chunk:demo-plan-ff3d5981".to_string()),
        );
    }

    #[test]
    fn strip_brackets_bare_id_is_untouched() {
        assert_eq!(
            strip_brackets(Some("user:alice")),
            Some("user:alice".to_string()),
        );
    }

    #[test]
    fn strip_brackets_legacy_ascii_form() {
        assert_eq!(
            strip_brackets(Some("outlet:<legacy.com>")),
            Some("outlet:legacy.com".to_string()),
        );
    }

    #[test]
    fn strip_brackets_passes_none_through() {
        assert_eq!(strip_brackets(None), None);
    }

    #[test]
    fn strip_brackets_handles_empty_string() {
        assert_eq!(strip_brackets(Some("")), Some(String::new()));
    }

    #[test]
    fn strip_brackets_strips_both_forms_in_one_input() {
        // Pathological mixed input — both shapes appear. The helper just
        // nukes any bracket character; it does not validate the structure.
        assert_eq!(
            strip_brackets(Some("a:⟨x⟩;b:<y>")),
            Some("a:x;b:y".to_string()),
        );
    }

    #[test]
    fn strip_brackets_handles_composite_id() {
        // SurrealDB v3 lets the id portion itself contain colons inside
        // brackets. Stripping exposes the composite shape.
        assert_eq!(
            strip_brackets(Some("community:⟨BFS:lakewood⟩")),
            Some("community:BFS:lakewood".to_string()),
        );
    }
}
