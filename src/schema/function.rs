//! Custom functions (`DEFINE FUNCTION fn::<name>`).
//!
//! A stored function runs server-side and is called as `fn::name(...)`.
//! Mirrors [`crate::schema::bucket`]: a value object that renders its own
//! `DEFINE` / `REMOVE` DDL, with the inverse in [`crate::schema::parser`].
//!
//! ## Canonical form
//!
//! The engine rewrites what it stores: `option<T>` becomes `none | T`, a
//! trailing `;` is dropped from the body, and an omitted `PERMISSIONS`
//! clause is echoed back as `PERMISSIONS FULL`. [`FunctionDefinition::normalized`]
//! applies the same rewrites so a code-side definition and the database's
//! echo of it compare equal instead of diffing on spelling alone.

use std::fmt::Write as _;

use serde::{Deserialize, Serialize};

use crate::error::{Result, SurqlError};

use super::sequence::guard_keyword;

/// The engine's default permission posture for a function.
pub const DEFAULT_PERMISSIONS: &str = "FULL";

/// One declared argument: `$name: type`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FunctionArg {
    /// Argument name, without the leading `$`.
    pub name: String,
    /// SurrealQL type, passed through verbatim.
    #[serde(rename = "type")]
    pub arg_type: String,
}

impl FunctionArg {
    /// Declare an argument.
    pub fn new(name: impl Into<String>, arg_type: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            arg_type: arg_type.into(),
        }
    }

    /// Render as `$name: type`.
    pub fn to_surql(&self) -> String {
        format!("${}: {}", self.name, self.arg_type)
    }
}

/// Immutable `DEFINE FUNCTION` schema definition.
///
/// `name` never carries the `fn::` prefix; it is added when rendering and
/// stripped when parsing, matching how `INFO FOR DB` keys the entry.
///
/// ## Examples
///
/// ```
/// use surql::schema::{function_schema, FunctionArg};
///
/// let f = function_schema("greet", "RETURN 'hi ' + $name")
///     .arg("name", "string")
///     .returns("string")
///     .build()
///     .unwrap();
/// assert_eq!(
///     f.to_surql().unwrap(),
///     "DEFINE FUNCTION fn::greet($name: string) -> string { RETURN 'hi ' + $name };"
/// );
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FunctionDefinition {
    /// Function name without the `fn::` prefix. May be namespaced
    /// (`pkg::nested`).
    pub name: String,
    /// Declared arguments, in order.
    #[serde(default)]
    pub args: Vec<FunctionArg>,
    /// Optional declared return type.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub returns: Option<String>,
    /// Body, without the surrounding braces.
    pub body: String,
    /// Optional `PERMISSIONS` clause body (`FULL`, `NONE`, or
    /// `WHERE <expr>`). `None` means the engine default, `FULL`.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub permissions: Option<String>,
    /// Optional human-readable comment.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub comment: Option<String>,
}

impl FunctionDefinition {
    /// Construct a function with a body and no arguments.
    pub fn new(name: impl Into<String>, body: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            args: Vec::new(),
            returns: None,
            body: body.into(),
            permissions: None,
            comment: None,
        }
    }

    /// Append an argument.
    pub fn with_arg(mut self, name: impl Into<String>, arg_type: impl Into<String>) -> Self {
        self.args.push(FunctionArg::new(name, arg_type));
        self
    }

    /// Set the declared return type.
    pub fn with_returns(mut self, returns: impl Into<String>) -> Self {
        self.returns = Some(returns.into());
        self
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
    /// Returns [`SurqlError::Validation`] for an empty name, an empty body, or
    /// an argument declared with no name or type.
    pub fn validate(&self) -> Result<()> {
        if self.name.is_empty() {
            return Err(SurqlError::Validation {
                reason: "Function name cannot be empty".into(),
            });
        }
        if self.body.trim().is_empty() {
            return Err(SurqlError::Validation {
                reason: format!("Function {:?} must have a body", self.name),
            });
        }
        for arg in &self.args {
            if arg.name.is_empty() || arg.arg_type.trim().is_empty() {
                return Err(SurqlError::Validation {
                    reason: format!(
                        "Function {:?} has an argument without a name or type",
                        self.name
                    ),
                });
            }
        }
        Ok(())
    }

    /// Rewrite into the form the engine stores, so a code-side definition and
    /// the database's echo of it compare equal.
    ///
    /// Types lose their `option<...>` spelling in favour of `none | ...`, the
    /// body is whitespace-normalised without its trailing `;`, and an unset
    /// `PERMISSIONS` becomes the [`DEFAULT_PERMISSIONS`] the engine reports.
    pub fn normalized(&self) -> Self {
        Self {
            name: self.name.clone(),
            args: self
                .args
                .iter()
                .map(|a| FunctionArg::new(a.name.clone(), normalize_type(&a.arg_type)))
                .collect(),
            returns: self.returns.as_deref().map(normalize_type),
            body: normalize_body(&self.body),
            permissions: Some(
                self.permissions
                    .as_deref()
                    .map_or_else(|| DEFAULT_PERMISSIONS.to_string(), normalize_whitespace),
            ),
            comment: self.comment.clone(),
        }
    }

    /// Render the `DEFINE FUNCTION` statement.
    pub fn to_surql(&self) -> Result<String> {
        self.to_surql_with_options(false, false)
    }

    /// Render with optional `IF NOT EXISTS` or `OVERWRITE` guards (mutually
    /// exclusive in SurrealQL; `OVERWRITE` wins, matching the server).
    pub fn to_surql_with_options(&self, if_not_exists: bool, overwrite: bool) -> Result<String> {
        self.validate()?;
        let args: Vec<String> = self.args.iter().map(FunctionArg::to_surql).collect();
        let mut sql = format!(
            "DEFINE FUNCTION {guard}fn::{name}({args})",
            guard = guard_keyword(if_not_exists, overwrite),
            name = self.name,
            args = args.join(", "),
        );
        if let Some(returns) = &self.returns {
            write!(sql, " -> {returns}").expect("writing to String cannot fail");
        }
        write!(sql, " {{ {} }}", self.body.trim()).expect("writing to String cannot fail");
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

    /// Render a `REMOVE FUNCTION` statement for this function.
    pub fn to_remove_surql(&self) -> String {
        Self::remove_surql(&self.name)
    }

    /// Render a `REMOVE FUNCTION` statement for a function by name (with or
    /// without the `fn::` prefix).
    pub fn remove_surql(name: &str) -> String {
        let bare = name.strip_prefix("fn::").unwrap_or(name);
        format!("REMOVE FUNCTION IF EXISTS fn::{bare};")
    }
}

/// Rewrite `option<T>` as the `none | T` union the engine stores.
fn normalize_type(declared: &str) -> String {
    let trimmed = declared.trim();
    let inner = trimmed
        .strip_prefix("option<")
        .or_else(|| trimmed.strip_prefix("OPTION<"))
        .and_then(|rest| rest.strip_suffix('>'));
    match inner {
        Some(inner) => format!("none | {}", normalize_type(inner)),
        None => normalize_whitespace(trimmed),
    }
}

/// Trim the body and drop the trailing `;` the engine strips.
fn normalize_body(body: &str) -> String {
    normalize_whitespace(body.trim().trim_end_matches(';'))
}

fn normalize_whitespace(text: &str) -> String {
    text.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// Builder for a [`FunctionDefinition`].
#[derive(Debug, Clone)]
pub struct FunctionSchemaBuilder {
    inner: FunctionDefinition,
}

impl FunctionSchemaBuilder {
    /// Append an argument.
    pub fn arg(mut self, name: impl Into<String>, arg_type: impl Into<String>) -> Self {
        self.inner.args.push(FunctionArg::new(name, arg_type));
        self
    }

    /// Set the declared return type.
    pub fn returns(mut self, returns: impl Into<String>) -> Self {
        self.inner.returns = Some(returns.into());
        self
    }

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
    pub fn build(self) -> Result<FunctionDefinition> {
        self.inner.validate()?;
        Ok(self.inner)
    }
}

/// Functional constructor for a [`FunctionDefinition`].
///
/// `name` is given without the `fn::` prefix.
pub fn function_schema(name: impl Into<String>, body: impl Into<String>) -> FunctionSchemaBuilder {
    FunctionSchemaBuilder {
        inner: FunctionDefinition::new(name, body),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn minimal_function_renders() {
        let f = FunctionDefinition::new("noargs", "RETURN 1");
        assert_eq!(
            f.to_surql().unwrap(),
            "DEFINE FUNCTION fn::noargs() { RETURN 1 };"
        );
    }

    #[test]
    fn args_return_comment_and_permissions_render_in_order() {
        let f = function_schema("greet", "RETURN 'hi ' + $name")
            .arg("name", "string")
            .arg("loud", "option<bool>")
            .returns("string")
            .comment("greeter")
            .permissions("WHERE $auth")
            .build()
            .unwrap();
        assert_eq!(
            f.to_surql().unwrap(),
            "DEFINE FUNCTION fn::greet($name: string, $loud: option<bool>) -> string \
             { RETURN 'hi ' + $name } COMMENT 'greeter' PERMISSIONS WHERE $auth;"
        );
    }

    #[test]
    fn a_namespaced_name_keeps_its_path() {
        let f = FunctionDefinition::new("pkg::nested", "RETURN $a");
        assert!(f
            .to_surql()
            .unwrap()
            .starts_with("DEFINE FUNCTION fn::pkg::nested("));
    }

    #[test]
    fn guards_render() {
        let f = FunctionDefinition::new("g", "RETURN 1");
        assert!(f
            .to_surql_with_options(true, false)
            .unwrap()
            .starts_with("DEFINE FUNCTION IF NOT EXISTS fn::g"));
        assert!(f
            .to_surql_overwrite()
            .unwrap()
            .starts_with("DEFINE FUNCTION OVERWRITE fn::g"));
    }

    #[test]
    fn remove_statement_tolerates_the_prefix() {
        assert_eq!(
            FunctionDefinition::remove_surql("greet"),
            "REMOVE FUNCTION IF EXISTS fn::greet;"
        );
        assert_eq!(
            FunctionDefinition::remove_surql("fn::greet"),
            "REMOVE FUNCTION IF EXISTS fn::greet;"
        );
        assert_eq!(
            FunctionDefinition::new("greet", "RETURN 1").to_remove_surql(),
            "REMOVE FUNCTION IF EXISTS fn::greet;"
        );
    }

    #[test]
    fn normalize_rewrites_option_types() {
        assert_eq!(normalize_type("option<int>"), "none | int");
        assert_eq!(normalize_type("option<option<int>>"), "none | none | int");
        assert_eq!(normalize_type("  int  "), "int");
    }

    #[test]
    fn normalize_matches_the_engine_echo() {
        let code = function_schema("greet", "RETURN 'hi ' + $name;")
            .arg("name", "option<string>")
            .returns("option<string>")
            .build()
            .unwrap();
        let echoed = function_schema("greet", "RETURN 'hi '   +   $name")
            .arg("name", "none | string")
            .returns("none | string")
            .permissions("FULL")
            .build()
            .unwrap();
        assert_eq!(code.normalized(), echoed.normalized());
    }

    #[test]
    fn normalize_fills_in_the_default_permissions() {
        let f = FunctionDefinition::new("f", "RETURN 1");
        assert_eq!(f.normalized().permissions.as_deref(), Some("FULL"));
    }

    #[test]
    fn validate_rejects_empty_pieces() {
        assert!(FunctionDefinition::new("", "RETURN 1").validate().is_err());
        assert!(FunctionDefinition::new("f", "  ").validate().is_err());
        assert!(FunctionDefinition::new("f", "RETURN 1")
            .with_arg("", "int")
            .validate()
            .is_err());
        assert!(FunctionDefinition::new("f", "RETURN 1")
            .with_arg("a", " ")
            .validate()
            .is_err());
        assert!(function_schema("", "RETURN 1").build().is_err());
    }

    #[test]
    fn serde_roundtrip() {
        let f = function_schema("f", "RETURN 1")
            .arg("a", "int")
            .returns("int")
            .comment("c")
            .permissions("NONE")
            .build()
            .unwrap();
        let json = serde_json::to_string(&f).unwrap();
        let back: FunctionDefinition = serde_json::from_str(&json).unwrap();
        assert_eq!(f, back);
    }
}
