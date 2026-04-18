//! Field schema definitions.
//!
//! Port of `surql/schema/fields.py`. Provides the [`FieldType`] enum,
//! [`FieldDefinition`] struct, and a family of builder helpers that construct
//! immutable field descriptors used by table and edge schemas.
//!
//! Each [`FieldDefinition`] renders a SurrealQL `DEFINE FIELD` statement via
//! [`FieldDefinition::to_surql`].

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::sync::OnceLock;

use regex::Regex;
use serde::{Deserialize, Serialize};

use crate::error::{Result, SurqlError};
use crate::types::check_reserved_word;

fn field_name_part_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"^[a-zA-Z_][a-zA-Z0-9_]*$").expect("valid regex"))
}

/// SurrealDB field types supported by `DEFINE FIELD`.
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
            Self::Any => "any",
        }
    }
}

impl std::fmt::Display for FieldType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Immutable field definition for table schemas.
///
/// Represents a single field in a SurrealDB table schema along with its
/// constraints, defaults, and permissions.
///
/// ## Examples
///
/// ```
/// use surql::schema::{FieldDefinition, FieldType};
///
/// let email = FieldDefinition::new("email", FieldType::String);
/// assert_eq!(email.to_surql("user"), "DEFINE FIELD email ON TABLE user TYPE string;");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FieldDefinition {
    /// Field name (supports dot notation for nested fields).
    pub name: String,
    /// Field type.
    #[serde(rename = "type")]
    pub field_type: FieldType,
    /// Optional SurrealQL assertion expression.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub assertion: Option<String>,
    /// Optional default value expression.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub default: Option<String>,
    /// Optional computed-value expression.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub value: Option<String>,
    /// Optional per-action permission rules keyed by action name.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub permissions: Option<BTreeMap<String, String>>,
    /// Whether the field is read-only after creation.
    #[serde(default)]
    pub readonly: bool,
    /// Whether the field allows flexible schema.
    #[serde(default)]
    pub flexible: bool,
}

impl FieldDefinition {
    /// Construct a new [`FieldDefinition`] with only the required members.
    ///
    /// Other members default to empty/false and can be set via chainable
    /// `with_*` setters.
    pub fn new(name: impl Into<String>, field_type: FieldType) -> Self {
        Self {
            name: name.into(),
            field_type,
            assertion: None,
            default: None,
            value: None,
            permissions: None,
            readonly: false,
            flexible: false,
        }
    }

    /// Set the assertion expression.
    pub fn with_assertion(mut self, assertion: impl Into<String>) -> Self {
        self.assertion = Some(assertion.into());
        self
    }

    /// Set the default value expression.
    pub fn with_default(mut self, default: impl Into<String>) -> Self {
        self.default = Some(default.into());
        self
    }

    /// Set the computed-value expression.
    pub fn with_value(mut self, value: impl Into<String>) -> Self {
        self.value = Some(value.into());
        self
    }

    /// Attach per-action permissions.
    pub fn with_permissions<I, K, V>(mut self, permissions: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        self.permissions = Some(
            permissions
                .into_iter()
                .map(|(k, v)| (k.into(), v.into()))
                .collect(),
        );
        self
    }

    /// Mark the field as read-only.
    pub fn readonly(mut self, readonly: bool) -> Self {
        self.readonly = readonly;
        self
    }

    /// Mark the field as flexible.
    pub fn flexible(mut self, flexible: bool) -> Self {
        self.flexible = flexible;
        self
    }

    /// Validate the field definition against SurrealDB identifier rules.
    ///
    /// Returns [`SurqlError::Validation`] for an empty name, empty segments,
    /// or segments that contain invalid characters.
    pub fn validate(&self) -> Result<()> {
        validate_field_name(&self.name)
    }

    /// Render the `DEFINE FIELD` statement for this field on the given table.
    ///
    /// ## Examples
    ///
    /// ```
    /// use surql::schema::{FieldDefinition, FieldType};
    ///
    /// let f = FieldDefinition::new("email", FieldType::String)
    ///     .with_assertion("string::is::email($value)");
    /// assert_eq!(
    ///     f.to_surql("user"),
    ///     "DEFINE FIELD email ON TABLE user TYPE string ASSERT string::is::email($value);",
    /// );
    /// ```
    pub fn to_surql(&self, table: &str) -> String {
        self.to_surql_with_options(table, false)
    }

    /// Render with optional `IF NOT EXISTS` clause.
    pub fn to_surql_with_options(&self, table: &str, if_not_exists: bool) -> String {
        let ine = if if_not_exists { " IF NOT EXISTS" } else { "" };
        let mut sql = format!(
            "DEFINE FIELD{ine} {name} ON TABLE {table} TYPE {ty}",
            ine = ine,
            name = self.name,
            table = table,
            ty = self.field_type.as_str(),
        );
        if let Some(assertion) = &self.assertion {
            write!(sql, " ASSERT {}", assertion).expect("writing to String cannot fail");
        }
        if let Some(default) = &self.default {
            write!(sql, " DEFAULT {}", default).expect("writing to String cannot fail");
        }
        if let Some(value) = &self.value {
            write!(sql, " VALUE {}", value).expect("writing to String cannot fail");
        }
        if self.readonly {
            sql.push_str(" READONLY");
        }
        if self.flexible {
            sql.push_str(" FLEXIBLE");
        }
        sql.push(';');
        sql
    }
}

/// Validate a field name against SurrealDB identifier rules.
///
/// Supports dot-notation for nested fields (for example `address.city`). Each
/// segment must match `[a-zA-Z_][a-zA-Z0-9_]*`.
///
/// ## Examples
///
/// ```
/// use surql::schema::fields::validate_field_name;
///
/// assert!(validate_field_name("email").is_ok());
/// assert!(validate_field_name("address.city").is_ok());
/// assert!(validate_field_name("").is_err());
/// assert!(validate_field_name("1bad").is_err());
/// ```
pub fn validate_field_name(name: &str) -> Result<()> {
    if name.is_empty() {
        return Err(SurqlError::Validation {
            reason: "Field name cannot be empty".into(),
        });
    }
    let regex = field_name_part_regex();
    for part in name.split('.') {
        if part.is_empty() {
            return Err(SurqlError::Validation {
                reason: format!("Invalid field name {name:?}: empty segment"),
            });
        }
        if !regex.is_match(part) {
            return Err(SurqlError::Validation {
                reason: format!(
                    "Invalid field name {name:?}: segment {part:?} must contain only \
                     alphanumeric characters and underscores, and cannot start with a digit"
                ),
            });
        }
    }
    Ok(())
}

/// Build a [`FieldDefinition`] with named parameters, mirroring
/// `surql.schema.fields.field`.
///
/// The field name is validated eagerly; reserved-word collisions surface as
/// an optional warning message returned alongside the definition so the
/// caller can relay it through `tracing::warn!` or their own logger.
///
/// ## Examples
///
/// ```
/// use surql::schema::fields::{field, FieldType};
///
/// let (f, warning) = field("name", FieldType::String).build().unwrap();
/// assert_eq!(f.field_type, FieldType::String);
/// assert!(warning.is_none());
/// ```
pub fn field(name: impl Into<String>, field_type: FieldType) -> FieldBuilder {
    FieldBuilder::new(name.into(), field_type)
}

/// Chainable builder used by [`field`] and the typed helpers.
#[derive(Debug, Clone)]
pub struct FieldBuilder {
    inner: FieldDefinition,
}

impl FieldBuilder {
    fn new(name: String, field_type: FieldType) -> Self {
        Self {
            inner: FieldDefinition::new(name, field_type),
        }
    }

    /// Set the assertion expression.
    pub fn assertion(mut self, assertion: impl Into<String>) -> Self {
        self.inner.assertion = Some(assertion.into());
        self
    }

    /// Set the default value expression.
    pub fn default(mut self, default: impl Into<String>) -> Self {
        self.inner.default = Some(default.into());
        self
    }

    /// Set the computed-value expression.
    pub fn value(mut self, value: impl Into<String>) -> Self {
        self.inner.value = Some(value.into());
        self
    }

    /// Attach per-action permissions.
    pub fn permissions<I, K, V>(mut self, permissions: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        self.inner.permissions = Some(
            permissions
                .into_iter()
                .map(|(k, v)| (k.into(), v.into()))
                .collect(),
        );
        self
    }

    /// Set the read-only flag.
    pub fn readonly(mut self, readonly: bool) -> Self {
        self.inner.readonly = readonly;
        self
    }

    /// Set the flexible flag.
    pub fn flexible(mut self, flexible: bool) -> Self {
        self.inner.flexible = flexible;
        self
    }

    /// Finalise the builder, returning the field and an optional reserved-word
    /// warning message for the caller to log.
    pub fn build(self) -> Result<(FieldDefinition, Option<String>)> {
        self.inner.validate()?;
        let warning = check_reserved_word(&self.inner.name, false);
        Ok((self.inner, warning))
    }

    /// Finalise the builder and discard any reserved-word warning.
    pub fn build_unchecked(self) -> Result<FieldDefinition> {
        self.inner.validate()?;
        Ok(self.inner)
    }
}

/// Convenience constructor for a `string` field.
pub fn string_field(name: impl Into<String>) -> FieldBuilder {
    field(name, FieldType::String)
}

/// Convenience constructor for an `int` field.
pub fn int_field(name: impl Into<String>) -> FieldBuilder {
    field(name, FieldType::Int)
}

/// Convenience constructor for a `float` field.
pub fn float_field(name: impl Into<String>) -> FieldBuilder {
    field(name, FieldType::Float)
}

/// Convenience constructor for a `bool` field.
pub fn bool_field(name: impl Into<String>) -> FieldBuilder {
    field(name, FieldType::Bool)
}

/// Convenience constructor for a `datetime` field.
pub fn datetime_field(name: impl Into<String>) -> FieldBuilder {
    field(name, FieldType::Datetime)
}

/// Convenience constructor for an `array` field.
pub fn array_field(name: impl Into<String>) -> FieldBuilder {
    field(name, FieldType::Array)
}

/// Convenience constructor for an `object` field.
///
/// Objects default to `flexible = true` to match `surql.schema.fields.object_field`.
pub fn object_field(name: impl Into<String>) -> FieldBuilder {
    field(name, FieldType::Object).flexible(true)
}

/// Convenience constructor for a `record` field.
///
/// When `table` is `Some`, an assertion is attached that constrains the
/// referenced record table. An explicit `assertion` chained afterwards is
/// composed using `AND` (mirroring the Python behaviour).
pub fn record_field(name: impl Into<String>, table: Option<&str>) -> FieldBuilder {
    let mut builder = field(name, FieldType::Record);
    if let Some(target) = table {
        builder.inner.assertion = Some(format!("$value.table = \"{target}\""));
    }
    builder
}

/// Convenience constructor for a computed field.
///
/// Computed fields are always read-only; the Python implementation hard-codes
/// `readonly=True`, so this helper does the same.
pub fn computed_field(
    name: impl Into<String>,
    value: impl Into<String>,
    field_type: FieldType,
) -> FieldBuilder {
    field(name, field_type).value(value).readonly(true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn field_type_as_str_matches_lowercase() {
        assert_eq!(FieldType::String.as_str(), "string");
        assert_eq!(FieldType::Datetime.as_str(), "datetime");
        assert_eq!(FieldType::Any.as_str(), "any");
    }

    #[test]
    fn field_type_display_matches_as_str() {
        assert_eq!(format!("{}", FieldType::Int), "int");
    }

    #[test]
    fn field_type_serializes_lowercase() {
        let json = serde_json::to_string(&FieldType::Datetime).unwrap();
        assert_eq!(json, "\"datetime\"");
    }

    #[test]
    fn field_type_deserializes_lowercase() {
        let ft: FieldType = serde_json::from_str("\"bool\"").unwrap();
        assert_eq!(ft, FieldType::Bool);
    }

    #[test]
    fn new_sets_defaults() {
        let f = FieldDefinition::new("email", FieldType::String);
        assert_eq!(f.name, "email");
        assert_eq!(f.field_type, FieldType::String);
        assert!(f.assertion.is_none());
        assert!(!f.readonly);
        assert!(!f.flexible);
    }

    #[test]
    fn to_surql_minimal() {
        let f = FieldDefinition::new("email", FieldType::String);
        assert_eq!(
            f.to_surql("user"),
            "DEFINE FIELD email ON TABLE user TYPE string;"
        );
    }

    #[test]
    fn to_surql_with_assertion() {
        let f = FieldDefinition::new("email", FieldType::String)
            .with_assertion("string::is::email($value)");
        assert_eq!(
            f.to_surql("user"),
            "DEFINE FIELD email ON TABLE user TYPE string ASSERT string::is::email($value);"
        );
    }

    #[test]
    fn to_surql_with_default() {
        let f = FieldDefinition::new("created_at", FieldType::Datetime).with_default("time::now()");
        assert_eq!(
            f.to_surql("event"),
            "DEFINE FIELD created_at ON TABLE event TYPE datetime DEFAULT time::now();"
        );
    }

    #[test]
    fn to_surql_readonly_flexible() {
        let f = FieldDefinition::new("meta", FieldType::Object)
            .readonly(true)
            .flexible(true);
        assert_eq!(
            f.to_surql("user"),
            "DEFINE FIELD meta ON TABLE user TYPE object READONLY FLEXIBLE;"
        );
    }

    #[test]
    fn to_surql_with_value_expression() {
        let f = FieldDefinition::new("full", FieldType::String).with_value("string::concat(a,b)");
        assert!(f.to_surql("t").contains("VALUE string::concat(a,b)"));
    }

    #[test]
    fn to_surql_if_not_exists() {
        let f = FieldDefinition::new("name", FieldType::String);
        assert_eq!(
            f.to_surql_with_options("user", true),
            "DEFINE FIELD IF NOT EXISTS name ON TABLE user TYPE string;"
        );
    }

    #[test]
    fn validate_rejects_empty_name() {
        let f = FieldDefinition::new("", FieldType::String);
        assert!(f.validate().is_err());
    }

    #[test]
    fn validate_rejects_bad_leading_digit() {
        let f = FieldDefinition::new("1bad", FieldType::String);
        assert!(f.validate().is_err());
    }

    #[test]
    fn validate_allows_dot_nested() {
        let f = FieldDefinition::new("address.city", FieldType::String);
        assert!(f.validate().is_ok());
    }

    #[test]
    fn validate_rejects_empty_segment() {
        let f = FieldDefinition::new("address..city", FieldType::String);
        assert!(f.validate().is_err());
    }

    #[test]
    fn builder_string_field() {
        let (f, _) = string_field("email").build().unwrap();
        assert_eq!(f.field_type, FieldType::String);
    }

    #[test]
    fn builder_int_field_with_assertion() {
        let (f, _) = int_field("age").assertion("$value >= 0").build().unwrap();
        assert_eq!(f.field_type, FieldType::Int);
        assert_eq!(f.assertion.as_deref(), Some("$value >= 0"));
    }

    #[test]
    fn builder_float_field() {
        let (f, _) = float_field("price").build().unwrap();
        assert_eq!(f.field_type, FieldType::Float);
    }

    #[test]
    fn builder_bool_field_with_default() {
        let (f, _) = bool_field("active").default("true").build().unwrap();
        assert_eq!(f.field_type, FieldType::Bool);
        assert_eq!(f.default.as_deref(), Some("true"));
    }

    #[test]
    fn builder_datetime_field_readonly() {
        let (f, _) = datetime_field("created_at")
            .default("time::now()")
            .readonly(true)
            .build()
            .unwrap();
        assert!(f.readonly);
        assert_eq!(f.default.as_deref(), Some("time::now()"));
    }

    #[test]
    fn builder_array_field() {
        let (f, _) = array_field("tags").default("[]").build().unwrap();
        assert_eq!(f.field_type, FieldType::Array);
    }

    #[test]
    fn builder_object_field_defaults_flexible() {
        let (f, _) = object_field("metadata").build().unwrap();
        assert_eq!(f.field_type, FieldType::Object);
        assert!(f.flexible);
    }

    #[test]
    fn builder_record_field_with_table() {
        let (f, _) = record_field("author", Some("user")).build().unwrap();
        assert_eq!(f.field_type, FieldType::Record);
        assert_eq!(f.assertion.as_deref(), Some(r#"$value.table = "user""#),);
    }

    #[test]
    fn builder_record_field_no_table() {
        let (f, _) = record_field("link", None).build().unwrap();
        assert!(f.assertion.is_none());
    }

    #[test]
    fn builder_computed_field_is_readonly() {
        let (f, _) = computed_field("full", "a + b", FieldType::String)
            .build()
            .unwrap();
        assert!(f.readonly);
        assert_eq!(f.value.as_deref(), Some("a + b"));
    }

    #[test]
    fn builder_rejects_invalid_name() {
        let err = string_field("1bad").build().unwrap_err();
        assert!(matches!(err, SurqlError::Validation { .. }));
    }

    #[test]
    fn builder_flags_reserved_word() {
        let (_f, warning) = string_field("select").build().unwrap();
        assert!(warning.is_some());
    }

    #[test]
    fn builder_permissions_are_stored() {
        let (f, _) = string_field("name")
            .permissions([("select", "true")])
            .build()
            .unwrap();
        assert_eq!(
            f.permissions
                .as_ref()
                .unwrap()
                .get("select")
                .map(String::as_str),
            Some("true")
        );
    }

    #[test]
    fn validate_field_name_helper() {
        assert!(validate_field_name("ok").is_ok());
        assert!(validate_field_name("ok.nested").is_ok());
        assert!(validate_field_name("").is_err());
        assert!(validate_field_name("bad seg").is_err());
    }
}
