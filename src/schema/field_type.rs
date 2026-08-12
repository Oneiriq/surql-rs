//! The [`FieldType`] keyword set accepted by `DEFINE FIELD`.
//!
//! Split out of [`super::fields`] so that module stays under the repo's
//! 1000-LOC budget. `FieldType` is re-exported from `schema::fields`, so
//! existing paths keep resolving.

use serde::{Deserialize, Serialize};

/// SurrealDB field types supported by `DEFINE FIELD`.
///
/// Container element types are carried separately by
/// [`FieldDefinition::target_table`](super::fields::FieldDefinition::target_table),
/// which turns [`Self::Record`] into `record<table>` and [`Self::Array`] into
/// `array<record<table>>`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum FieldType {
    /// `string`
    String,
    /// `int`
    Int,
    /// `float`
    Float,
    /// `bool`
    Bool,
    /// `datetime`
    Datetime,
    /// `duration`
    Duration,
    /// `decimal`
    Decimal,
    /// `number`
    Number,
    /// `object`
    Object,
    /// `array`
    Array,
    /// `record`
    Record,
    /// `geometry`
    Geometry,
    /// `file` — a SurrealDB v3 file pointer into a bucket
    /// (see [`crate::schema::bucket`]).
    File,
    /// `bytes` — raw binary data.
    Bytes,
    /// `any`
    Any,
}

impl FieldType {
    /// Render the type as SurrealQL keyword.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::String => "string",
            Self::Int => "int",
            Self::Float => "float",
            Self::Bool => "bool",
            Self::Datetime => "datetime",
            Self::Duration => "duration",
            Self::Decimal => "decimal",
            Self::Number => "number",
            Self::Object => "object",
            Self::Array => "array",
            Self::Record => "record",
            Self::Geometry => "geometry",
            Self::File => "file",
            Self::Bytes => "bytes",
            Self::Any => "any",
        }
    }
}

impl std::fmt::Display for FieldType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn as_str_matches_lowercase() {
        assert_eq!(FieldType::String.as_str(), "string");
        assert_eq!(FieldType::Datetime.as_str(), "datetime");
        assert_eq!(FieldType::Any.as_str(), "any");
    }

    #[test]
    fn file_and_bytes_as_str() {
        assert_eq!(FieldType::File.as_str(), "file");
        assert_eq!(FieldType::Bytes.as_str(), "bytes");
    }

    #[test]
    fn file_bytes_serde_roundtrip() {
        for ft in [FieldType::File, FieldType::Bytes] {
            let json = serde_json::to_string(&ft).unwrap();
            let back: FieldType = serde_json::from_str(&json).unwrap();
            assert_eq!(ft, back);
        }
        assert_eq!(serde_json::to_string(&FieldType::File).unwrap(), "\"file\"");
        assert_eq!(
            serde_json::to_string(&FieldType::Bytes).unwrap(),
            "\"bytes\""
        );
    }

    #[test]
    fn display_matches_as_str() {
        assert_eq!(format!("{}", FieldType::Int), "int");
    }

    #[test]
    fn serializes_lowercase() {
        let json = serde_json::to_string(&FieldType::Datetime).unwrap();
        assert_eq!(json, "\"datetime\"");
    }

    #[test]
    fn deserializes_lowercase() {
        let ft: FieldType = serde_json::from_str("\"bool\"").unwrap();
        assert_eq!(ft, FieldType::Bool);
    }
}
