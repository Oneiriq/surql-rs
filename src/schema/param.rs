//! Database-level parameters (`DEFINE PARAM $<name>`).
//!
//! A param is a named constant every query in the database can read as
//! `$<name>` without binding it. Mirrors [`crate::schema::bucket`]: a value
//! object that renders its own `DEFINE` / `REMOVE` DDL, with the inverse in
//! [`crate::schema::parser`].
//!
//! Like [`crate::schema::function`], an omitted `PERMISSIONS` clause comes
//! back from `INFO FOR DB` as `PERMISSIONS FULL`, so
//! [`ParamDefinition::normalized`] fills it in before comparison.

use std::fmt::Write as _;

use serde::{Deserialize, Serialize};

use crate::error::{Result, SurqlError};

use super::function::DEFAULT_PERMISSIONS;
use super::sequence::guard_keyword;

/// Immutable `DEFINE PARAM` schema definition.
///
/// `name` never carries the leading `$`; it is added when rendering and
/// stripped when parsing, matching how `INFO FOR DB` keys the entry.
///
/// ## Examples
///
/// ```
/// use surql::schema::param_schema;
///
/// let p = param_schema("APP_NAME", "'oneiriq'").build().unwrap();
/// assert_eq!(
///     p.to_surql().unwrap(),
///     "DEFINE PARAM $APP_NAME VALUE 'oneiriq';"
/// );
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ParamDefinition {
    /// Param name without the leading `$`.
    pub name: String,
    /// The value expression, passed to the engine verbatim.
    pub value: String,
    /// Optional `PERMISSIONS` clause body (`FULL`, `NONE`, or
    /// `WHERE <expr>`). `None` means the engine default, `FULL`.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub permissions: Option<String>,
    /// Optional human-readable comment.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub comment: Option<String>,
}

impl ParamDefinition {
    /// Construct a param with a value expression.
    pub fn new(name: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            value: value.into(),
            permissions: None,
            comment: None,
        }
    }

    /// Set the `PERMISSIONS` clause body.
    pub fn with_permissions(mut self, permissions: impl Into<String>) -> Self {
        self.permissions = Some(permissions.into());
        self
    }

    /// Set the comment.
    pub fn with_comment(mut self, comment: impl Into<String>) -> Self {
        self.comment = Some(comment.into());
        self
    }

    /// Validate the definition.
    ///
    /// Returns [`SurqlError::Validation`] for an empty name or value.
    pub fn validate(&self) -> Result<()> {
        if self.name.is_empty() {
            return Err(SurqlError::Validation {
                reason: "Param name cannot be empty".into(),
            });
        }
        if self.value.trim().is_empty() {
            return Err(SurqlError::Validation {
                reason: format!("Param {:?} must have a value", self.name),
            });
        }
        Ok(())
    }

    /// Rewrite into the form the engine stores, so a code-side definition and
    /// the database's echo of it compare equal: whitespace collapsed and the
    /// default `PERMISSIONS` spelled out.
    pub fn normalized(&self) -> Self {
        Self {
            name: self.name.clone(),
            value: normalize_whitespace(&self.value),
            permissions: Some(
                self.permissions
                    .as_deref()
                    .map_or_else(|| DEFAULT_PERMISSIONS.to_string(), normalize_whitespace),
            ),
            comment: self.comment.clone(),
        }
    }

    /// Render the `DEFINE PARAM` statement.
    pub fn to_surql(&self) -> Result<String> {
        self.to_surql_with_options(false, false)
    }

    /// Render with optional `IF NOT EXISTS` or `OVERWRITE` guards (mutually
    /// exclusive in SurrealQL; `OVERWRITE` wins, matching the server).
    pub fn to_surql_with_options(&self, if_not_exists: bool, overwrite: bool) -> Result<String> {
        self.validate()?;
        let mut sql = format!(
            "DEFINE PARAM {guard}${name} VALUE {value}",
            guard = guard_keyword(if_not_exists, overwrite),
            name = self.name,
            value = self.value.trim(),
        );
        if let Some(comment) = &self.comment {
            write!(sql, " COMMENT '{comment}'").expect("writing to String cannot fail");
        }
        if let Some(permissions) = &self.permissions {
            write!(sql, " PERMISSIONS {permissions}").expect("writing to String cannot fail");
        }
        sql.push(';');
        Ok(sql)
    }

    /// Render the `OVERWRITE` form, which replaces a stored definition.
    pub fn to_surql_overwrite(&self) -> Result<String> {
        self.to_surql_with_options(false, true)
    }

    /// Render a `REMOVE PARAM` statement for this param.
    pub fn to_remove_surql(&self) -> String {
        Self::remove_surql(&self.name)
    }

    /// Render a `REMOVE PARAM` statement for a param by name (with or without
    /// the leading `$`).
    pub fn remove_surql(name: &str) -> String {
        format!("REMOVE PARAM IF EXISTS ${};", name.trim_start_matches('$'))
    }
}

fn normalize_whitespace(text: &str) -> String {
    text.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// Builder for a [`ParamDefinition`].
#[derive(Debug, Clone)]
pub struct ParamSchemaBuilder {
    inner: ParamDefinition,
}

impl ParamSchemaBuilder {
    /// Set the `PERMISSIONS` clause body.
    pub fn permissions(mut self, permissions: impl Into<String>) -> Self {
        self.inner.permissions = Some(permissions.into());
        self
    }

    /// Set the comment.
    pub fn comment(mut self, comment: impl Into<String>) -> Self {
        self.inner.comment = Some(comment.into());
        self
    }

    /// Finalise the builder, validating the definition.
    pub fn build(self) -> Result<ParamDefinition> {
        self.inner.validate()?;
        Ok(self.inner)
    }
}

/// Functional constructor for a [`ParamDefinition`].
///
/// `name` is given without the leading `$`; `value` is a SurrealQL
/// expression, so a string constant needs its own quotes.
pub fn param_schema(name: impl Into<String>, value: impl Into<String>) -> ParamSchemaBuilder {
    ParamSchemaBuilder {
        inner: ParamDefinition::new(name, value),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn minimal_param_renders() {
        let p = ParamDefinition::new("RATE", "0.25");
        assert_eq!(p.to_surql().unwrap(), "DEFINE PARAM $RATE VALUE 0.25;");
    }

    #[test]
    fn comment_and_permissions_render_in_order() {
        let p = param_schema("APP", "'oneiriq'")
            .comment("display name")
            .permissions("WHERE $auth")
            .build()
            .unwrap();
        assert_eq!(
            p.to_surql().unwrap(),
            "DEFINE PARAM $APP VALUE 'oneiriq' COMMENT 'display name' PERMISSIONS WHERE $auth;"
        );
    }

    #[test]
    fn guards_render() {
        let p = ParamDefinition::new("G", "1");
        assert!(p
            .to_surql_with_options(true, false)
            .unwrap()
            .starts_with("DEFINE PARAM IF NOT EXISTS $G"));
        assert!(p
            .to_surql_overwrite()
            .unwrap()
            .starts_with("DEFINE PARAM OVERWRITE $G"));
    }

    #[test]
    fn remove_statement_tolerates_the_sigil() {
        assert_eq!(
            ParamDefinition::remove_surql("P"),
            "REMOVE PARAM IF EXISTS $P;"
        );
        assert_eq!(
            ParamDefinition::remove_surql("$P"),
            "REMOVE PARAM IF EXISTS $P;"
        );
        assert_eq!(
            ParamDefinition::new("P", "1").to_remove_surql(),
            "REMOVE PARAM IF EXISTS $P;"
        );
    }

    #[test]
    fn normalize_fills_in_the_default_permissions_and_collapses_whitespace() {
        let code = ParamDefinition::new("P", "  'hello'  ");
        let echoed = ParamDefinition::new("P", "'hello'").with_permissions("FULL");
        assert_eq!(code.normalized(), echoed.normalized());
        assert_eq!(code.normalized().permissions.as_deref(), Some("FULL"));
    }

    #[test]
    fn validate_rejects_empty_pieces() {
        assert!(ParamDefinition::new("", "1").validate().is_err());
        assert!(ParamDefinition::new("P", "  ").validate().is_err());
        assert!(param_schema("", "1").build().is_err());
    }

    #[test]
    fn serde_roundtrip() {
        let p = param_schema("P", "1")
            .comment("c")
            .permissions("NONE")
            .build()
            .unwrap();
        let json = serde_json::to_string(&p).unwrap();
        let back: ParamDefinition = serde_json::from_str(&json).unwrap();
        assert_eq!(p, back);
    }
}
