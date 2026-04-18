//! `type::record()` reference helper.
//!
//! Port of `surql/types/record_ref.py`. Generates `type::record(table, id)`
//! SurrealQL expressions usable as raw values inside CREATE/UPDATE/UPSERT.

use serde::{Deserialize, Serialize};

use super::record_id::RecordIdValue;

/// Reference to a SurrealDB record via `type::record()`.
///
/// When this value is embedded in a query body (rather than as a parameter)
/// it renders verbatim rather than being quoted as a string.
///
/// ## Examples
///
/// ```
/// use surql::types::{record_ref, RecordRef};
///
/// let r = record_ref("user", "alice");
/// assert_eq!(r.to_surql(), "type::record('user', 'alice')");
///
/// let num = record_ref("post", 123i64);
/// assert_eq!(num.to_surql(), "type::record('post', 123)");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RecordRef {
    /// Target table name.
    pub table: String,
    /// Target record id.
    #[serde(with = "record_id_value_serde")]
    pub record_id: RecordIdValue,
}

impl RecordRef {
    /// Render as a `type::record()` SurrealQL call.
    pub fn to_surql(&self) -> String {
        match &self.record_id {
            RecordIdValue::Int(n) => format!("type::record('{}', {})", self.table, n),
            RecordIdValue::String(s) => {
                let escaped = s.replace('\\', "\\\\").replace('\'', "\\'");
                format!("type::record('{}', '{escaped}')", self.table)
            }
        }
    }
}

impl std::fmt::Display for RecordRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.to_surql())
    }
}

/// Builder for [`RecordRef`].
pub fn record_ref(table: impl Into<String>, record_id: impl Into<RecordIdValue>) -> RecordRef {
    RecordRef {
        table: table.into(),
        record_id: record_id.into(),
    }
}

mod record_id_value_serde {
    use super::RecordIdValue;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    #[derive(Serialize, Deserialize)]
    #[serde(untagged)]
    enum Wire {
        Int(i64),
        Str(String),
    }

    pub fn serialize<S>(v: &RecordIdValue, s: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match v {
            RecordIdValue::Int(n) => Wire::Int(*n).serialize(s),
            RecordIdValue::String(st) => Wire::Str(st.clone()).serialize(s),
        }
    }

    pub fn deserialize<'de, D>(d: D) -> Result<RecordIdValue, D::Error>
    where
        D: Deserializer<'de>,
    {
        match Wire::deserialize(d)? {
            Wire::Int(n) => Ok(RecordIdValue::Int(n)),
            Wire::Str(s) => Ok(RecordIdValue::String(s)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_string_id() {
        assert_eq!(
            record_ref("user", "alice").to_surql(),
            "type::record('user', 'alice')"
        );
    }

    #[test]
    fn renders_int_id() {
        assert_eq!(
            record_ref("post", 123i64).to_surql(),
            "type::record('post', 123)"
        );
    }

    #[test]
    fn escapes_single_quote() {
        assert_eq!(
            record_ref("user", "o'brien").to_surql(),
            "type::record('user', 'o\\'brien')"
        );
    }

    #[test]
    fn escapes_backslash() {
        assert_eq!(
            record_ref("path", "a\\b").to_surql(),
            "type::record('path', 'a\\\\b')"
        );
    }

    #[test]
    fn display_matches_to_surql() {
        let r = record_ref("user", "alice");
        assert_eq!(format!("{r}"), r.to_surql());
    }

    #[test]
    fn serde_string_roundtrip() {
        let r = record_ref("user", "alice");
        let json = serde_json::to_string(&r).unwrap();
        let back: RecordRef = serde_json::from_str(&json).unwrap();
        assert_eq!(r, back);
    }

    #[test]
    fn serde_int_roundtrip() {
        let r = record_ref("post", 42i64);
        let json = serde_json::to_string(&r).unwrap();
        let back: RecordRef = serde_json::from_str(&json).unwrap();
        assert_eq!(r, back);
    }
}
