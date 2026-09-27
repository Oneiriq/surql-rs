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

use serde::{Deserialize, Serialize};

use crate::error::{Result, SurqlError};
use crate::types::check_reserved_word;
use crate::types::escape::{is_identifier, quote_ident};

pub use super::field_type::FieldType;

use super::permissions::{render_permissions_clause, validate_permissions, FIELD_ACTIONS};
use super::reference::{
    render_reference_clause, validate_computed, validate_reference_target, ReferenceAction,
};

/// If `value` is the canonical record coercion expression, return the target
/// table. `type::record("plan", $value)` yields `Some("plan")`. Returns `None`
/// for anything else, including more complex VALUE expressions.
fn detect_target_table_from_value(value: &str) -> Option<String> {
    let rest = value
        .trim()
        .strip_prefix("type::record")?
        .trim_start()
        .strip_prefix('(')?
        .trim_start()
        .strip_prefix(['"', '\''])?;
    let end = rest.find(['"', '\''])?;
    let table = rest.get(..end)?;
    let tail = rest
        .get(end + 1..)?
        .trim_start()
        .strip_prefix(',')?
        .trim_start()
        .strip_prefix("$value")?
        .trim_start()
        .strip_prefix(')')?;
    (is_identifier(table) && tail.trim().is_empty()).then(|| table.to_string())
}

/// Render a field path (`address.city`, `tags.*`, `tags[*]`) for a
/// statement, backtick-quoting any name segment the engine would not read
/// as a plain name (a reserved word, a name with a `-`).
///
/// A segment carrying brackets or an expression is left as written.
pub(crate) fn render_field_path(path: &str) -> String {
    path.split('.')
        .map(|segment| {
            let (base, suffix) = segment
                .find('[')
                .and_then(|at| Some((segment.get(..at)?, segment.get(at..)?)))
                .unwrap_or((segment, ""));
            let plain = !base.is_empty()
                && base != "*"
                && !base.contains(['(', ')', '`', '\'', '"', '$', ' ', '⟨']);
            if plain {
                format!("{}{suffix}", quote_ident(base))
            } else {
                segment.to_string()
            }
        })
        .collect::<Vec<_>>()
        .join(".")
}

/// Render the tables of a `record<a | b>` link, each quoted as a name.
pub(crate) fn render_table_list(tables: &str) -> String {
    tables
        .split('|')
        .map(|table| quote_ident(table.trim()))
        .collect::<Vec<_>>()
        .join(" | ")
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
// The bools mirror independent DDL flags (READONLY, FLEXIBLE, INLINE, the
// `option<...>` wrapper); folding them into enums would only rename them.
#[allow(clippy::struct_excessive_bools)]
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
    /// Optional per-action permission rules keyed by action (`select`,
    /// `create`, `update`; fields have no `delete`), rendered as the field's
    /// `PERMISSIONS FOR <action> WHERE <rule>` clause. A rule of `"NONE"` or
    /// `"FULL"` renders that posture instead of a `WHERE`. Actions left out
    /// keep the engine's field default, `FULL`.
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
    /// A SurrealQL type the [`FieldType`] keywords cannot spell, rendered
    /// verbatim as the `TYPE` clause (inside `option<...>` when
    /// [`Self::nullable`]): a union (`array<string> | int`), a literal
    /// (`'draft' | 'published'`), or a typed container (`array<string, 5>`,
    /// `set<int>`, `geometry<point>`). `field_type` is [`FieldType::Any`]
    /// when it is set. Set it with [`Self::with_custom_type`].
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub custom_type: Option<String>,
    /// Whether the field's value is kept in the edge's adjacency entries
    /// (`INLINE`, SurrealDB 3.3+), so a traversal can filter on it without
    /// fetching the edge record. Only a top-level, non-`COMPUTED` field of a
    /// relation table that is not lightweight may set it.
    #[serde(default)]
    pub inline: bool,
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
            custom_type: None,
            inline: false,
        }
    }

    /// Keep the field's value in the edge's adjacency entries (`INLINE`,
    /// SurrealDB 3.3+), for a top-level field of a relation table.
    pub fn with_inline(mut self, inline: bool) -> Self {
        self.inline = inline;
        self
    }

    /// Type the field with a SurrealQL type the [`FieldType`] keywords
    /// cannot spell (see [`Self::custom_type`]), such as a union. Sets
    /// `field_type` to [`FieldType::Any`] and clears `target_table`; use
    /// [`Self::with_nullable`] rather than writing `option<...>` or
    /// `none | ...` here.
    ///
    /// ## Examples
    ///
    /// ```
    /// use surql::schema::{FieldDefinition, FieldType};
    ///
    /// let status = FieldDefinition::new("status", FieldType::Any)
    ///     .with_custom_type("'draft' | 'published'");
    /// assert_eq!(
    ///     status.to_surql("post"),
    ///     "DEFINE FIELD status ON TABLE post TYPE 'draft' | 'published';"
    /// );
    /// ```
    pub fn with_custom_type(mut self, ty: impl Into<String>) -> Self {
        self.custom_type = Some(ty.into());
        self.field_type = FieldType::Any;
        self.target_table = None;
        self
    }

    /// The `TYPE` clause this field renders, `option<...>` included, or
    /// `None` for a `COMPUTED` field that declares no type.
    pub fn type_clause(&self) -> Option<String> {
        if self.omits_type_clause() {
            return None;
        }
        let (ty, _) = self.resolve_type_clause();
        Some(if self.nullable {
            format!("option<{ty}>")
        } else {
            ty
        })
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
    /// plus the `REFERENCE`, `COMPUTED`, and `PERMISSIONS` restrictions the
    /// engine enforces.
    ///
    /// Returns [`SurqlError::Validation`] for an empty name, empty segments,
    /// segments that contain invalid characters, or a permission naming an
    /// action other than `select`, `create`, or `update`.
    pub fn validate(&self) -> Result<()> {
        validate_field_name(&self.name)?;
        validate_permissions(
            &format!("Field {:?}", self.name),
            self.permissions.as_ref(),
            FIELD_ACTIONS,
        )?;
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
        if self.inline && (self.computed.is_some() || self.name.contains(['.', '['])) {
            return Err(SurqlError::Validation {
                reason: format!(
                    "Field {:?}: INLINE takes a top-level field that is not COMPUTED",
                    self.name
                ),
            });
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
        let (_, drop_value) = self.resolve_type_clause();
        let mut sql = format!(
            "DEFINE FIELD{ine} {name} ON TABLE {table}",
            ine = ine,
            name = render_field_path(&self.name),
            table = quote_ident(table),
        );
        if let Some(type_clause) = self.type_clause() {
            let _ = write!(sql, " TYPE {type_clause}");
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
            let _ = write!(sql, " COMPUTED {computed}");
        }
        if let Some(assertion) = &self.assertion {
            let _ = write!(sql, " ASSERT {assertion}");
        }
        if let Some(default) = &self.default {
            let _ = write!(sql, " DEFAULT {default}");
        }
        if let Some(value) = &self.value {
            if !drop_value {
                let _ = write!(sql, " VALUE {value}");
            }
        }
        if self.readonly {
            sql.push_str(" READONLY");
        }
        if self.inline {
            sql.push_str(" INLINE");
        }
        // Field permissions: without this clause the field would get the
        // engine default, FULL, whatever the definition declares.
        sql.push_str(&render_permissions_clause(self.permissions.as_ref()));
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
            && self.custom_type.is_none()
    }

    /// Resolve the `TYPE` clause, honoring a `custom_type` verbatim and a
    /// `target_table` by emitting `record<target>` for a RECORD field and
    /// `array<record<target>>` for an ARRAY field. The returned boolean
    /// indicates whether a redundant `type::record("target", $value)` VALUE
    /// coercion should be dropped.
    fn resolve_type_clause(&self) -> (String, bool) {
        if let Some(custom) = &self.custom_type {
            return (custom.clone(), false);
        }
        let Some(target) = self.target_table.as_deref() else {
            return (self.field_type.as_str().to_string(), false);
        };
        let targets = render_table_list(target);
        match self.field_type {
            FieldType::Record => {
                let drop_value = self
                    .value
                    .as_deref()
                    .and_then(detect_target_table_from_value)
                    .as_deref()
                    == Some(target);
                (format!("record<{targets}>"), drop_value)
            }
            FieldType::Array => (format!("array<record<{targets}>>"), false),
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
    for part in name.split('.') {
        if part.is_empty() {
            return Err(SurqlError::Validation {
                reason: format!("Invalid field name {name:?}: empty segment"),
            });
        }
        if !is_identifier(part) {
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

    /// Attach per-action permissions, rendered as the field's `PERMISSIONS`
    /// clause (see [`FieldDefinition::permissions`]).
    ///
    /// ```
    /// use surql::schema::string_field;
    ///
    /// let (ssn, _) = string_field("ssn")
    ///     .permissions([("select", "$auth.admin = true")])
    ///     .build()
    ///     .unwrap();
    /// assert_eq!(
    ///     ssn.to_surql("user"),
    ///     "DEFINE FIELD ssn ON TABLE user TYPE string \
    ///      PERMISSIONS FOR select WHERE $auth.admin = true;",
    /// );
    /// ```
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
    ///
    /// Adding this to a field that already has rows tracks NOTHING for
    /// them: the engine registers a reference on value change only, and
    /// even a self-assignment does not count. Run
    /// [`reference_backfill_sql`](super::reference_backfill_sql) after
    /// the DDL, or take it from the schema diff, which carries it for
    /// exactly this case.
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
mod tests;
