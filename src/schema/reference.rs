//! Record-reference tracking for `DEFINE FIELD` (SurrealDB v3).
//!
//! A field marked `REFERENCE` makes the engine record the incoming link, so
//! the referenced record can be walked backwards with the `<~` operator (see
//! [`Query::reverse_traverse`](crate::query::builder::Query::reverse_traverse))
//! and so deleting the target can propagate into the referencing record.
//!
//! The complementary half is [`FieldDefinition::computed`], which stores the
//! reverse lookup as a field: `DEFINE FIELD comments ON person COMPUTED
//! <~comment`.
//!
//! ## What the engine accepts
//!
//! `REFERENCE` is only valid on a **top-level** field whose type is a record
//! or a container of records. Verified against v3.0.5, which rejects:
//!
//! - nested paths — `Cannot use the REFERENCE keyword on nested field
//!   'metadata.comics'`;
//! - union types — `Cannot use the REFERENCE keyword with TYPE
//!   array<record<x>> | string`.
//!
//! [`validate_reference_target`] enforces the first rule and the
//! record-typed rule; this crate cannot express a union type at all, so the
//! second is unreachable from a [`FieldDefinition`].

use serde::{Deserialize, Serialize};

use crate::error::{Result, SurqlError};

use super::fields::FieldType;

/// What the engine does to a referencing field when the record it points at
/// is deleted.
///
/// Renders as the `ON DELETE <action>` tail of the `REFERENCE` clause.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum ReferenceAction {
    /// Leave the dangling reference in place. The engine default, and what a
    /// bare `REFERENCE` is stored as.
    Ignore,
    /// Refuse the delete while any reference remains.
    Reject,
    /// Delete the referencing record too.
    Cascade,
    /// Clear the reference, or drop the entry from a referencing array.
    Unset,
}

impl ReferenceAction {
    /// Render as the SurrealQL keyword.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ignore => "IGNORE",
            Self::Reject => "REJECT",
            Self::Cascade => "CASCADE",
            Self::Unset => "UNSET",
        }
    }

    /// Parse a SurrealQL keyword, case-insensitively. Unknown words yield
    /// `None`.
    pub fn from_keyword(word: &str) -> Option<Self> {
        match word.to_ascii_uppercase().as_str() {
            "IGNORE" => Some(Self::Ignore),
            "REJECT" => Some(Self::Reject),
            "CASCADE" => Some(Self::Cascade),
            "UNSET" => Some(Self::Unset),
            _ => None,
        }
    }
}

impl std::fmt::Display for ReferenceAction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Render the ` REFERENCE ON DELETE <action>` clause.
///
/// The action is always spelled out, including the `IGNORE` default, because
/// that is the form `INFO FOR TABLE` echoes back — emitting a bare
/// `REFERENCE` would diff against the database on every reconcile.
pub(super) fn render_reference_clause(action: ReferenceAction) -> String {
    format!(" REFERENCE ON DELETE {}", action.as_str())
}

/// Validate that a field can carry `REFERENCE`.
///
/// Returns [`SurqlError::Validation`] for a nested field name or for a type
/// that is not a record / array-of-record link.
pub(super) fn validate_reference_target(
    name: &str,
    field_type: FieldType,
    target_table: Option<&str>,
) -> Result<()> {
    if name.contains('.') {
        return Err(SurqlError::Validation {
            reason: format!(
                "Field {name:?} cannot use REFERENCE: reference tracking only \
                 works on top-level fields"
            ),
        });
    }
    if !matches!(field_type, FieldType::Record | FieldType::Array) || target_table.is_none() {
        return Err(SurqlError::Validation {
            reason: format!(
                "Field {name:?} cannot use REFERENCE: the type must be \
                 record<table> or array<record<table>>"
            ),
        });
    }
    Ok(())
}

/// Validate that `COMPUTED` is not combined with a clause the engine rejects.
///
/// v3.0.5 refuses `Cannot use the READONLY keyword with COMPUTED`, and a
/// stored `VALUE` makes no sense beside a value recomputed on every read.
pub(super) fn validate_computed(
    name: &str,
    readonly: bool,
    value: Option<&str>,
    default: Option<&str>,
) -> Result<()> {
    if readonly {
        return Err(SurqlError::Validation {
            reason: format!("Field {name:?} cannot combine COMPUTED with READONLY"),
        });
    }
    if value.is_some() {
        return Err(SurqlError::Validation {
            reason: format!("Field {name:?} cannot combine COMPUTED with VALUE"),
        });
    }
    if default.is_some() {
        return Err(SurqlError::Validation {
            reason: format!("Field {name:?} cannot combine COMPUTED with DEFAULT"),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::fields::{
        array_field, field, record_field, reverse_reference_field, string_field, FieldDefinition,
    };

    #[test]
    fn action_keywords_round_trip() {
        for action in [
            ReferenceAction::Ignore,
            ReferenceAction::Reject,
            ReferenceAction::Cascade,
            ReferenceAction::Unset,
        ] {
            assert_eq!(ReferenceAction::from_keyword(action.as_str()), Some(action));
            assert_eq!(action.to_string(), action.as_str());
        }
        assert_eq!(
            ReferenceAction::from_keyword("cascade"),
            Some(ReferenceAction::Cascade)
        );
        assert!(ReferenceAction::from_keyword("BOGUS").is_none());
    }

    #[test]
    fn action_serialises_uppercase() {
        let json = serde_json::to_string(&ReferenceAction::Unset).unwrap();
        assert_eq!(json, "\"UNSET\"");
        let back: ReferenceAction = serde_json::from_str(&json).unwrap();
        assert_eq!(back, ReferenceAction::Unset);
    }

    #[test]
    fn record_reference_renders_on_delete() {
        let (f, _) = record_field("author", Some("person"))
            .nullable(true)
            .reference(ReferenceAction::Cascade)
            .build()
            .unwrap();
        assert_eq!(
            f.to_surql("comment"),
            "DEFINE FIELD author ON TABLE comment TYPE option<record<person>> \
             REFERENCE ON DELETE CASCADE;"
        );
    }

    #[test]
    fn bare_reference_spells_out_the_engine_default() {
        let (f, _) = record_field("author", Some("person"))
            .reference(ReferenceAction::Ignore)
            .build()
            .unwrap();
        assert!(f.to_surql("comment").contains("REFERENCE ON DELETE IGNORE"));
    }

    #[test]
    fn array_of_record_reference_renders() {
        let (f, _) = array_field("comics")
            .target_table("comic_book")
            .nullable(true)
            .reference(ReferenceAction::Unset)
            .build()
            .unwrap();
        assert_eq!(
            f.to_surql("person"),
            "DEFINE FIELD comics ON TABLE person TYPE option<array<record<comic_book>>> \
             REFERENCE ON DELETE UNSET;"
        );
    }

    #[test]
    fn overwrite_form_carries_the_reference_clause() {
        let (f, _) = record_field("author", Some("person"))
            .reference(ReferenceAction::Reject)
            .build()
            .unwrap();
        let sql = f.to_surql_overwrite("comment");
        assert!(sql.starts_with("DEFINE FIELD OVERWRITE author ON TABLE comment"));
        assert!(sql.contains("REFERENCE ON DELETE REJECT"));
    }

    #[test]
    fn nested_reference_field_is_rejected() {
        let err = record_field("metadata.comics", Some("comic_book"))
            .reference(ReferenceAction::Ignore)
            .build()
            .unwrap_err();
        assert!(err.to_string().contains("top-level"), "{err}");
    }

    #[test]
    fn reference_on_a_non_record_type_is_rejected() {
        let err = string_field("title")
            .reference(ReferenceAction::Ignore)
            .build()
            .unwrap_err();
        assert!(err.to_string().contains("record<table>"), "{err}");
    }

    #[test]
    fn reference_on_an_untargeted_record_is_rejected() {
        let err = record_field("author", None)
            .reference(ReferenceAction::Ignore)
            .build()
            .unwrap_err();
        assert!(err.to_string().contains("record<table>"), "{err}");
    }

    #[test]
    fn reverse_reference_field_renders_computed_without_a_type() {
        let (f, _) = reverse_reference_field("comments", "comment")
            .build()
            .unwrap();
        assert_eq!(
            f.to_surql("person"),
            "DEFINE FIELD comments ON TABLE person COMPUTED <~comment;"
        );
    }

    #[test]
    fn computed_with_a_declared_type_keeps_the_type_clause() {
        let (f, _) = field("accessed_at", FieldType::Datetime)
            .computed("time::now()")
            .build()
            .unwrap();
        assert_eq!(
            f.to_surql("person"),
            "DEFINE FIELD accessed_at ON TABLE person TYPE datetime COMPUTED time::now();"
        );
    }

    #[test]
    fn computed_rejects_readonly_value_and_default() {
        assert!(validate_computed("x", true, None, None).is_err());
        assert!(validate_computed("x", false, Some("1"), None).is_err());
        assert!(validate_computed("x", false, None, Some("1")).is_err());
        assert!(validate_computed("x", false, None, None).is_ok());
    }

    #[test]
    fn reference_and_computed_survive_serde() {
        let (f, _) = record_field("author", Some("person"))
            .reference(ReferenceAction::Cascade)
            .build()
            .unwrap();
        let json = serde_json::to_string(&f).unwrap();
        let back: FieldDefinition = serde_json::from_str(&json).unwrap();
        assert_eq!(f, back);
        assert_eq!(back.reference, Some(ReferenceAction::Cascade));
    }

    #[test]
    fn definitions_without_references_deserialize_from_older_snapshots() {
        let json = r#"{"name":"email","type":"string"}"#;
        let f: FieldDefinition = serde_json::from_str(json).unwrap();
        assert!(f.reference.is_none());
        assert!(f.computed.is_none());
    }
}
