//! Type-safe `RecordID` wrapper for SurrealDB `table:id` identifiers.
//!
//! Port of `surql/types/record_id.py`. Supports angle-bracket syntax for
//! complex IDs (containing dots, dashes, colons, etc.) and integer IDs.

use std::fmt;
use std::marker::PhantomData;
use std::sync::OnceLock;

use regex::Regex;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

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
/// ## Examples
///
/// ```
/// use surql::types::RecordID;
///
/// let id = RecordID::<()>::new("user", "alice").unwrap();
/// assert_eq!(id.to_string(), "user:alice");
///
/// let complex = RecordID::<()>::new("outlet", "alaskabeacon.com").unwrap();
/// assert_eq!(complex.to_string(), "outlet:<alaskabeacon.com>");
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

fn table_pattern() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"^[A-Za-z_][A-Za-z0-9_]*$").expect("valid regex"))
}

fn simple_id_pattern() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"^[A-Za-z0-9_]+$").expect("valid regex"))
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
        if !table_pattern().is_match(name) {
            return Err(SurqlError::Validation {
                reason: format!(
                    "Invalid table name: {name:?}. Must contain only alphanumeric \
                     characters and underscores, and cannot start with a digit"
                ),
            });
        }
        Ok(())
    }

    /// Parse a string of the form `table:id` or `table:<id>`.
    ///
    /// Integer-looking ids are parsed into [`RecordIdValue::Int`]; everything
    /// else stays a [`RecordIdValue::String`]. Angle brackets around the id
    /// (used for complex identifiers in SurrealQL) are stripped on parse.
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

        // Strip angle brackets if present.
        let stripped = if id_str.starts_with('<') && id_str.ends_with('>') && id_str.len() >= 2 {
            &id_str[1..id_str.len() - 1]
        } else {
            id_str
        };

        let id = stripped.parse::<i64>().map_or_else(
            |_| RecordIdValue::String(stripped.to_owned()),
            RecordIdValue::Int,
        );

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

    fn needs_angle_brackets(&self) -> bool {
        match &self.id {
            RecordIdValue::Int(_) => false,
            RecordIdValue::String(s) => !simple_id_pattern().is_match(s),
        }
    }
}

impl<T> fmt::Display for RecordID<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.needs_angle_brackets() {
            write!(f, "{}:<{}>", self.table, self.id)
        } else {
            write!(f, "{}:{}", self.table, self.id)
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
    fn complex_id_uses_angle_brackets() {
        let id = RecordID::<()>::new("outlet", "alaskabeacon.com").unwrap();
        assert_eq!(id.to_string(), "outlet:<alaskabeacon.com>");
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
    fn parse_angle_brackets_strips_them() {
        let id = RecordID::<()>::parse("outlet:<alaskabeacon.com>").unwrap();
        assert_eq!(id.table(), "outlet");
        assert!(matches!(id.id(), RecordIdValue::String(s) if s == "alaskabeacon.com"));
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
        assert_eq!(json, "\"outlet:<alaskabeacon.com>\"");
        let back: RecordID<()> = serde_json::from_str(&json).unwrap();
        assert_eq!(back, id);
    }
}
