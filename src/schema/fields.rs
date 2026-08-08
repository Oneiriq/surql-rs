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

pub use super::field_type::FieldType;

use super::reference::{
    render_reference_clause, validate_computed, validate_reference_target, ReferenceAction,
};

fn field_name_part_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"^[a-zA-Z_][a-zA-Z0-9_]*$").expect("valid regex"))
}

/// Regex matching the canonical `type::record("<table>", $value)` coercion.
fn type_record_coercion_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(
            r#"^type::record\s*\(\s*["']([a-zA-Z_][a-zA-Z0-9_]*)["']\s*,\s*\$value\s*\)\s*\z"#,
        )
        .expect("valid regex")
    })
}

/// If `value` is the canonical record coercion expression, return the target
/// table. `type::record("plan", $value)` yields `Some("plan")`. Returns `None`
/// for anything else, including more complex VALUE expressions.
fn detect_target_table_from_value(value: &str) -> Option<String> {
    type_record_coercion_regex()
        .captures(value.trim())
        .map(|caps| caps[1].to_string())
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
    /// The table a link points at. On a RECORD field this renders
    /// `TYPE record<{target_table}>` instead of bare `record`; on an ARRAY
    /// field it renders `TYPE array<record<{target_table}>>`, the shape
    /// reference tracking needs for a to-many link.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub target_table: Option<String>,
    /// Whether the field accepts `NONE`, rendering `TYPE option<{inner}>`.
    ///
    /// SurrealDB v3 SCHEMAFULL tables reject `NONE` for a plain-typed
    /// column; wrapping the type in `option<...>` is how a column opts into
    /// being unset. Mirrors `nullable=True` in the Python port (1.5.8+) and
    /// the TS port's option-wrapped emission. Defaults to `false`, which
    /// keeps rendering byte-identical for existing definitions and lets
    /// snapshots written before this field existed deserialize cleanly.
    #[serde(default)]
    pub nullable: bool,
    /// Reference tracking (`REFERENCE ON DELETE <action>`). See
    /// [`crate::schema::reference`] for what the engine accepts.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub reference: Option<ReferenceAction>,
    /// Expression recomputed on every read (`COMPUTED <expr>`), as opposed to
    /// the stored [`Self::value`]. This is where a `<~table` reverse-reference
    /// lookup lives.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub computed: Option<String>,
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
            target_table: None,
            nullable: false,
            reference: None,
            computed: None,
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

    /// Set the record target table, rendering `TYPE record<table>`.
    pub fn with_target_table(mut self, table: impl Into<String>) -> Self {
        self.target_table = Some(table.into());
        self
    }

    /// Mark the field as accepting `NONE`, rendering `TYPE option<{inner}>`.
    pub fn with_nullable(mut self, nullable: bool) -> Self {
        self.nullable = nullable;
        self
    }

    /// Track incoming links to this field (`REFERENCE ON DELETE <action>`).
    pub fn with_reference(mut self, action: ReferenceAction) -> Self {
        self.reference = Some(action);
        self
    }

    /// Set the `COMPUTED` expression, recomputed on every read.
    pub fn with_computed(mut self, expression: impl Into<String>) -> Self {
        self.computed = Some(expression.into());
        self
    }

    /// Validate the field definition against SurrealDB identifier rules,
    /// plus the `REFERENCE` and `COMPUTED` restrictions the engine enforces.
    ///
    /// Returns [`SurqlError::Validation`] for an empty name, empty segments,
    /// or segments that contain invalid characters.
    pub fn validate(&self) -> Result<()> {
        validate_field_name(&self.name)?;
        if self.reference.is_some() {
            validate_reference_target(&self.name, self.field_type, self.target_table.as_deref())?;
        }
        if self.computed.is_some() {
            validate_computed(
                &self.name,
                self.readonly,
                self.value.as_deref(),
                self.default.as_deref(),
            )?;
        }
        Ok(())
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
        self.render_guard(table, if if_not_exists { " IF NOT EXISTS" } else { "" })
    }

    /// Render with `OVERWRITE`, replacing an existing definition while
    /// leaving stored data untouched. What schema evolution applies
    /// when a stored definition no longer matches the code.
    pub fn to_surql_overwrite(&self, table: &str) -> String {
        self.render_guard(table, " OVERWRITE")
    }

    fn render_guard(&self, table: &str, ine: &str) -> String {
        let (type_clause, drop_value) = self.resolve_type_clause();
        let type_clause = if self.nullable {
            format!("option<{type_clause}>")
        } else {
            type_clause
        };
        let mut sql = format!(
            "DEFINE FIELD{ine} {name} ON TABLE {table}",
            ine = ine,
            name = self.name,
            table = table,
        );
        if !self.omits_type_clause() {
            write!(sql, " TYPE {type_clause}").expect("writing to String cannot fail");
        }
        // SurrealDB v3 requires FLEXIBLE immediately after the TYPE
        // clause; rendering it after READONLY (this crate's previous
        // trailing position) is a parse error: "FLEXIBLE must be
        // specified after TYPE". Verified against v3.0.5.
        if self.flexible {
            sql.push_str(" FLEXIBLE");
        }
        if let Some(action) = self.reference {
            sql.push_str(&render_reference_clause(action));
        }
        if let Some(computed) = &self.computed {
            write!(sql, " COMPUTED {computed}").expect("writing to String cannot fail");
        }
        if let Some(assertion) = &self.assertion {
            write!(sql, " ASSERT {}", assertion).expect("writing to String cannot fail");
        }
        if let Some(default) = &self.default {
            write!(sql, " DEFAULT {}", default).expect("writing to String cannot fail");
        }
        if let Some(value) = &self.value {
            if !drop_value {
                write!(sql, " VALUE {}", value).expect("writing to String cannot fail");
            }
        }
        if self.readonly {
            sql.push_str(" READONLY");
        }
        sql.push(';');
        sql
    }

    /// A `COMPUTED` field with no declared type renders no `TYPE` clause,
    /// which is how the engine stores `DEFINE FIELD x ON t COMPUTED <~y`.
    /// Any explicit type (including `option<...>`) is emitted as usual.
    fn omits_type_clause(&self) -> bool {
        self.computed.is_some()
            && self.field_type == FieldType::Any
            && !self.nullable
            && self.target_table.is_none()
    }

    /// Resolve the `TYPE` clause, honoring a `target_table` by emitting
    /// `record<target>` for a RECORD field and `array<record<target>>` for an
    /// ARRAY field. The returned boolean indicates whether a redundant
    /// `type::record("target", $value)` VALUE coercion should be dropped.
    fn resolve_type_clause(&self) -> (String, bool) {
        let Some(target) = self.target_table.as_deref() else {
            return (self.field_type.as_str().to_string(), false);
        };
        match self.field_type {
            FieldType::Record => {
                let drop_value = self
                    .value
                    .as_deref()
                    .and_then(detect_target_table_from_value)
                    .as_deref()
                    == Some(target);
                (format!("record<{target}>"), drop_value)
            }
            FieldType::Array => (format!("array<record<{target}>>"), false),
            _ => (self.field_type.as_str().to_string(), false),
        }
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

    /// Set the record target table, rendering `TYPE record<table>`.
    pub fn target_table(mut self, table: impl Into<String>) -> Self {
        self.inner.target_table = Some(table.into());
        self
    }

    /// Mark the field as accepting `NONE`, rendering `TYPE option<{inner}>`.
    ///
    /// Composes with every other builder option, including record targets:
    ///
    /// ```
    /// use surql::schema::fields::{datetime_field, record_field};
    ///
    /// let (f, _) = datetime_field("deleted_at").nullable(true).build().unwrap();
    /// assert_eq!(
    ///     f.to_surql("file"),
    ///     "DEFINE FIELD deleted_at ON TABLE file TYPE option<datetime>;",
    /// );
    ///
    /// let (link, _) = record_field("prior", Some("file_version"))
    ///     .nullable(true)
    ///     .build()
    ///     .unwrap();
    /// assert_eq!(
    ///     link.to_surql("file_version"),
    ///     "DEFINE FIELD prior ON TABLE file_version TYPE option<record<file_version>>;",
    /// );
    /// ```
    pub fn nullable(mut self, nullable: bool) -> Self {
        self.inner.nullable = nullable;
        self
    }

    /// Track incoming links to this field (`REFERENCE ON DELETE <action>`).
    ///
    /// Only valid on a top-level `record<table>` / `array<record<table>>`
    /// field; [`build`](Self::build) rejects anything else.
    pub fn reference(mut self, action: ReferenceAction) -> Self {
        self.inner.reference = Some(action);
        self
    }

    /// Set the `COMPUTED` expression, recomputed on every read.
    pub fn computed(mut self, expression: impl Into<String>) -> Self {
        self.inner.computed = Some(expression.into());
        self
    }

    /// Finalise the builder, returning the field and an optional reserved-word
    /// warning message for the caller to log.
    pub fn build(mut self) -> Result<(FieldDefinition, Option<String>)> {
        self.finalize_record_target();
        self.inner.validate()?;
        let warning = check_reserved_word(&self.inner.name, false);
        Ok((self.inner, warning))
    }

    /// Finalise the builder and discard any reserved-word warning.
    pub fn build_unchecked(mut self) -> Result<FieldDefinition> {
        self.finalize_record_target();
        self.inner.validate()?;
        Ok(self.inner)
    }

    /// Mirror `surql.schema.fields.field`: lift a canonical
    /// `type::record("X", $value)` coercion on a RECORD field into
    /// `target_table`, then drop the now-redundant VALUE coercion.
    fn finalize_record_target(&mut self) {
        if self.inner.field_type != FieldType::Record {
            return;
        }
        if self.inner.target_table.is_none() {
            if let Some(detected) = self
                .inner
                .value
                .as_deref()
                .and_then(detect_target_table_from_value)
            {
                self.inner.target_table = Some(detected);
            }
        }
        let redundant = matches!(
            (self.inner.target_table.as_deref(), self.inner.value.as_deref()),
            (Some(target), Some(value))
                if detect_target_table_from_value(value).as_deref() == Some(target)
        );
        if redundant {
            self.inner.value = None;
        }
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
/// When `table` is `Some`, the target table is recorded so the field renders
/// `TYPE record<table>` (the typed form SurrealDB introspection expects).
pub fn record_field(name: impl Into<String>, table: Option<&str>) -> FieldBuilder {
    let mut builder = field(name, FieldType::Record);
    if let Some(target) = table {
        builder.inner.target_table = Some(target.to_string());
    }
    builder
}

/// Convenience constructor for a stored computed field (`VALUE` + `READONLY`).
///
/// The Python implementation hard-codes `readonly=True`, so this helper does
/// the same. For a value the engine recomputes on every read, use
/// [`FieldBuilder::computed`] instead.
pub fn computed_field(
    name: impl Into<String>,
    value: impl Into<String>,
    field_type: FieldType,
) -> FieldBuilder {
    field(name, field_type).value(value).readonly(true)
}

/// Convenience constructor for the reverse half of a record reference:
/// `DEFINE FIELD {name} ON TABLE {t} COMPUTED <~{source}`.
///
/// `source` is the table whose `REFERENCE` field points back at this one. The
/// field is untyped, matching how the engine stores it.
///
/// ## Examples
///
/// ```
/// use surql::schema::reverse_reference_field;
///
/// let (f, _) = reverse_reference_field("comments", "comment").build().unwrap();
/// assert_eq!(
///     f.to_surql("person"),
///     "DEFINE FIELD comments ON TABLE person COMPUTED <~comment;",
/// );
/// ```
pub fn reverse_reference_field(name: impl Into<String>, source: &str) -> FieldBuilder {
    field(name, FieldType::Any).computed(format!("<~{source}"))
}

/// Convenience constructor for a `file` field.
///
/// A `file` field stores a SurrealDB v3 file pointer (`f"bucket:/key"`) into
/// an object-storage bucket defined via [`crate::schema::bucket`]. Pair it
/// with the runtime file API on
/// [`DatabaseClient::bucket`](crate::connection::DatabaseClient) to populate
/// the referenced object.
pub fn file_field(name: impl Into<String>) -> FieldBuilder {
    field(name, FieldType::File)
}

/// Convenience constructor for a `bytes` field (raw binary data).
pub fn bytes_field(name: impl Into<String>) -> FieldBuilder {
    field(name, FieldType::Bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nullable_wraps_type_in_option() {
        let (f, _) = datetime_field("deleted_at").nullable(true).build().unwrap();
        assert_eq!(
            f.to_surql("file"),
            "DEFINE FIELD deleted_at ON TABLE file TYPE option<datetime>;",
        );
    }

    #[test]
    fn nullable_wraps_record_target() {
        let (f, _) = record_field("prior", Some("file_version"))
            .nullable(true)
            .build()
            .unwrap();
        assert_eq!(
            f.to_surql("file_version"),
            "DEFINE FIELD prior ON TABLE file_version TYPE option<record<file_version>>;",
        );
    }

    #[test]
    fn nullable_composes_with_other_clauses() {
        let (f, _) = string_field("lock_mode")
            .nullable(true)
            .assertion("$value INSIDE ['governance', 'compliance']")
            .build()
            .unwrap();
        assert_eq!(
            f.to_surql("blob"),
            "DEFINE FIELD lock_mode ON TABLE blob TYPE option<string> \
             ASSERT $value INSIDE ['governance', 'compliance'];",
        );
    }

    #[test]
    fn non_nullable_rendering_is_unchanged() {
        // Guards the default path: byte-identical to pre-nullable output.
        let f = FieldDefinition::new("email", FieldType::String);
        assert_eq!(
            f.to_surql("user"),
            "DEFINE FIELD email ON TABLE user TYPE string;",
        );
        assert!(!f.nullable);
    }

    #[test]
    fn builder_file_field_emits_type_file() {
        let (f, _) = file_field("avatar").build().unwrap();
        assert_eq!(f.field_type, FieldType::File);
        assert_eq!(
            f.to_surql("user"),
            "DEFINE FIELD avatar ON TABLE user TYPE file;"
        );
    }

    #[test]
    fn builder_bytes_field_emits_type_bytes() {
        let (f, _) = bytes_field("blob").build().unwrap();
        assert_eq!(f.field_type, FieldType::Bytes);
        assert_eq!(
            f.to_surql("doc"),
            "DEFINE FIELD blob ON TABLE doc TYPE bytes;"
        );
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
        // FLEXIBLE immediately after TYPE is the only ordering the v3
        // parser accepts alongside READONLY; the previous trailing
        // position was a parse error on a live server.
        let f = FieldDefinition::new("meta", FieldType::Object)
            .readonly(true)
            .flexible(true);
        assert_eq!(
            f.to_surql("user"),
            "DEFINE FIELD meta ON TABLE user TYPE object FLEXIBLE READONLY;"
        );
    }

    #[test]
    fn to_surql_flexible_composes_with_option_and_default() {
        let f = FieldDefinition::new("metadata", FieldType::Object)
            .flexible(true)
            .with_nullable(true)
            .with_default("{}");
        assert_eq!(
            f.to_surql("file"),
            "DEFINE FIELD metadata ON TABLE file TYPE option<object> FLEXIBLE DEFAULT {};"
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
        assert_eq!(f.target_table.as_deref(), Some("user"));
        assert_eq!(
            f.to_surql("post"),
            "DEFINE FIELD author ON TABLE post TYPE record<user>;"
        );
    }

    #[test]
    fn builder_record_field_no_table() {
        let (f, _) = record_field("link", None).build().unwrap();
        assert!(f.assertion.is_none());
        assert!(f.target_table.is_none());
    }

    #[test]
    fn detect_target_table_matches_canonical_coercion() {
        assert_eq!(
            detect_target_table_from_value("type::record(\"plan\", $value)").as_deref(),
            Some("plan")
        );
        assert_eq!(
            detect_target_table_from_value("type::record('user', $value)").as_deref(),
            Some("user")
        );
        assert!(detect_target_table_from_value("$value.id").is_none());
        assert!(detect_target_table_from_value("type::record(\"a\", $other)").is_none());
    }

    #[test]
    fn explicit_target_table_renders_typed_record() {
        let (f, _) = field("author", FieldType::Record)
            .target_table("user")
            .build()
            .unwrap();
        assert_eq!(f.target_table.as_deref(), Some("user"));
        assert_eq!(
            f.to_surql("post"),
            "DEFINE FIELD author ON TABLE post TYPE record<user>;"
        );
    }

    #[test]
    fn value_coercion_lifts_target_table_and_drops_value() {
        let (f, _) = field("workspace_id", FieldType::Record)
            .value("type::record(\"workspace\", $value)")
            .build()
            .unwrap();
        assert_eq!(f.target_table.as_deref(), Some("workspace"));
        assert!(f.value.is_none());
        assert_eq!(
            f.to_surql("task"),
            "DEFINE FIELD workspace_id ON TABLE task TYPE record<workspace>;"
        );
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
