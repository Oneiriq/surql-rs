//! Object-storage bucket schema definitions (SurrealDB v3 files / buckets).
//!
//! Buckets are the object-storage primitive introduced in SurrealDB v3: a
//! named container, backed by an in-memory / local-filesystem / S3 store,
//! into which files are written and from which they are read via the
//! `f"bucket:/key"` file-pointer syntax (see [`crate::types::FileRef`] and the
//! runtime API on
//! [`DatabaseClient::bucket`](crate::connection::DatabaseClient)).
//!
//! This module is the schema-definition (code-first) side: it models a
//! [`BucketDefinition`] and renders the `DEFINE BUCKET` / `ALTER BUCKET` /
//! `REMOVE BUCKET` DDL, mirroring [`crate::schema::access`]. The inverse
//! (parsing `DEFINE BUCKET` back out of `INFO FOR DB`) lives in
//! [`crate::schema::parser`].
//!
//! ## Backends
//!
//! The `backend` string is passed verbatim to SurrealDB:
//!
//! - `"memory"` — non-persistent, in the database process memory.
//! - `"file:/some/dir"` — local filesystem (the path must appear in the
//!   server's `SURREAL_BUCKET_FOLDER_ALLOWLIST`).
//! - `"s3://bucket-name"` — an S3-compatible object store.
//!
//! ## Experimental feature
//!
//! Buckets require the server to be started with the
//! `SURREAL_CAPS_ALLOW_EXPERIMENTAL=files` environment variable (the feature
//! is hidden and not enabled by `--allow-all`; the `--allow-experimental
//! files` flag form is broken — see the crate README's files/buckets section).
//! The DDL string generation here is independent of that switch; only live
//! execution needs it.

use std::fmt::Write as _;

use serde::{Deserialize, Serialize};

use crate::error::{Result, SurqlError};
use crate::types::escape::{quote_ident, quote_str};

/// The engine's permission posture for a bucket defined without one.
pub const DEFAULT_BUCKET_PERMISSIONS: &str = "FULL";

/// Render a string literal: double-quoted as this module has always
/// rendered it when the text needs no escaping, the escaped single-quoted
/// form from [`quote_str`] otherwise, so a `"`, `\`, or newline in a backend
/// path or comment can never end the literal early.
fn quote_literal(text: &str) -> String {
    let plain = !text.contains(['"', '\\', '\0', '\r', '\t', '\n', '\x08', '\x0C']);
    if plain {
        format!("\"{text}\"")
    } else {
        quote_str(text)
    }
}

/// Immutable `DEFINE BUCKET` schema definition.
///
/// Models a SurrealDB v3 object-storage bucket. Construct one with
/// [`bucket_schema`], [`memory_bucket`], or [`file_bucket`] and render the DDL
/// with [`BucketDefinition::to_surql`].
///
/// ## Examples
///
/// ```
/// use surql::schema::memory_bucket;
///
/// let b = memory_bucket("avatars");
/// assert_eq!(
///     b.to_surql().unwrap(),
///     "DEFINE BUCKET avatars BACKEND \"memory\";"
/// );
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BucketDefinition {
    /// Bucket name.
    pub name: String,
    /// Storage backend URL (`memory` / `file:/path` / `s3://...`).
    pub backend: String,
    /// Whether the bucket rejects writes (`READONLY`).
    #[serde(default)]
    pub readonly: bool,
    /// The `PERMISSIONS` clause body: `NONE`, `FULL`, or `WHERE <expr>`.
    ///
    /// A bucket has one permission covering every file operation, not the
    /// per-action map a table has; `FOR select WHERE ...` is a parse error.
    /// `None` is the engine default, [`DEFAULT_BUCKET_PERMISSIONS`].
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub permissions: Option<String>,
    /// Optional human-readable comment.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub comment: Option<String>,
}

impl BucketDefinition {
    /// Construct a new [`BucketDefinition`] with the given name and backend.
    pub fn new(name: impl Into<String>, backend: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            backend: backend.into(),
            readonly: false,
            permissions: None,
            comment: None,
        }
    }

    /// Mark the bucket read-only (or clear the flag).
    pub fn with_readonly(mut self, readonly: bool) -> Self {
        self.readonly = readonly;
        self
    }

    /// Set the `PERMISSIONS` clause body: `NONE`, `FULL`, or
    /// `WHERE <expr>`.
    pub fn with_permissions(mut self, permissions: impl Into<String>) -> Self {
        self.permissions = Some(permissions.into());
        self
    }

    /// Set the comment.
    pub fn with_comment(mut self, comment: impl Into<String>) -> Self {
        self.comment = Some(comment.into());
        self
    }

    /// Validate the bucket definition.
    ///
    /// Returns [`SurqlError::Validation`] when the name or backend is empty,
    /// or when the permissions are not `NONE`, `FULL`, or `WHERE <expr>`.
    pub fn validate(&self) -> Result<()> {
        if self.name.is_empty() {
            return Err(SurqlError::Validation {
                reason: "Bucket name cannot be empty".into(),
            });
        }
        if self.backend.is_empty() {
            return Err(SurqlError::Validation {
                reason: format!("Bucket {:?} must have a backend", self.name),
            });
        }
        if let Some(permissions) = &self.permissions {
            let body = permissions.trim();
            let keyword = body.split_whitespace().next().unwrap_or("");
            let fixed = body.eq_ignore_ascii_case("NONE") || body.eq_ignore_ascii_case("FULL");
            let rule = keyword.eq_ignore_ascii_case("WHERE") && body.len() > keyword.len();
            if !fixed && !rule {
                return Err(SurqlError::Validation {
                    reason: format!(
                        "Bucket {:?}: permissions must be NONE, FULL, or WHERE <expr>, got {body:?}",
                        self.name
                    ),
                });
            }
        }
        Ok(())
    }

    /// The rendered ` PERMISSIONS <body>` clause, or an empty string.
    fn permissions_clause(&self) -> String {
        self.permissions
            .as_deref()
            .map(|p| format!(" PERMISSIONS {}", p.trim()))
            .unwrap_or_default()
    }

    /// Render the `DEFINE BUCKET` statement.
    ///
    /// Validates the definition first; returns an error if validation fails.
    pub fn to_surql(&self) -> Result<String> {
        self.to_surql_with_options(false, false)
    }

    /// Render the `DEFINE BUCKET` statement with optional `IF NOT EXISTS` or
    /// `OVERWRITE` guards (the two are mutually exclusive in SurrealQL; when
    /// both are passed, `OVERWRITE` wins, matching the server precedence).
    ///
    /// Validates the definition first.
    pub fn to_surql_with_options(&self, if_not_exists: bool, overwrite: bool) -> Result<String> {
        self.validate()?;
        let guard = if overwrite {
            "OVERWRITE "
        } else if if_not_exists {
            "IF NOT EXISTS "
        } else {
            ""
        };
        let mut sql = format!(
            "DEFINE BUCKET {guard}{name} BACKEND {backend}",
            guard = guard,
            name = quote_ident(&self.name),
            backend = quote_literal(&self.backend),
        );
        if self.readonly {
            sql.push_str(" READONLY");
        }
        sql.push_str(&self.permissions_clause());
        if let Some(comment) = &self.comment {
            let _ = write!(sql, " COMMENT {}", quote_literal(comment));
        }
        sql.push(';');
        Ok(sql)
    }

    /// Render a `REMOVE BUCKET` statement for this bucket.
    pub fn to_remove_surql(&self) -> String {
        Self::remove_surql(&self.name)
    }

    /// Render a `REMOVE BUCKET` statement for a bucket by name.
    pub fn remove_surql(name: &str) -> String {
        format!("REMOVE BUCKET {};", quote_ident(name))
    }

    /// Render an `ALTER BUCKET` statement that turns `from` into `self`.
    ///
    /// Only the fields that differ are emitted. A backend present on `from`
    /// but absent on `self` is impossible (backend is required), so the
    /// `DROP BACKEND` clause is never produced here; a `None` comment on
    /// `self` paired with a `Some` comment on `from` renders `DROP COMMENT`.
    /// Read-only transitions render `READONLY` / `DROP READONLY`. A
    /// permission change re-emits the `PERMISSIONS` clause; clearing it
    /// restores the engine default, [`DEFAULT_BUCKET_PERMISSIONS`].
    ///
    /// `if_exists` adds the `IF EXISTS` guard for idempotent re-application.
    pub fn to_alter_surql(&self, from: &BucketDefinition, if_exists: bool) -> String {
        let guard = if if_exists { "IF EXISTS " } else { "" };
        let mut sql = format!(
            "ALTER BUCKET {guard}{name}",
            guard = guard,
            name = quote_ident(&self.name)
        );

        if self.readonly != from.readonly {
            if self.readonly {
                sql.push_str(" READONLY");
            } else {
                sql.push_str(" DROP READONLY");
            }
        }

        if self.backend != from.backend {
            let _ = write!(sql, " BACKEND {}", quote_literal(&self.backend));
        }

        if self.permissions != from.permissions {
            let clause = self.permissions_clause();
            if clause.is_empty() {
                let _ = write!(sql, " PERMISSIONS {DEFAULT_BUCKET_PERMISSIONS}");
            } else {
                sql.push_str(&clause);
            }
        }

        if self.comment != from.comment {
            match &self.comment {
                Some(comment) => {
                    let _ = write!(sql, " COMMENT {}", quote_literal(comment));
                }
                None => sql.push_str(" DROP COMMENT"),
            }
        }

        sql.push(';');
        sql
    }
}

/// Builder for a [`BucketDefinition`].
///
/// Mirrors [`crate::schema::access::AccessSchemaBuilder`]: chain the setters
/// and call [`BucketSchemaBuilder::build`] to validate and finalise.
#[derive(Debug, Clone)]
pub struct BucketSchemaBuilder {
    inner: BucketDefinition,
}

impl BucketSchemaBuilder {
    /// Mark the bucket read-only.
    pub fn readonly(mut self, readonly: bool) -> Self {
        self.inner.readonly = readonly;
        self
    }

    /// Set the `PERMISSIONS` clause body: `NONE`, `FULL`, or
    /// `WHERE <expr>`.
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
    pub fn build(self) -> Result<BucketDefinition> {
        self.inner.validate()?;
        Ok(self.inner)
    }
}

/// Functional constructor for a [`BucketDefinition`] with an explicit backend.
///
/// The returned builder is validated on [`BucketSchemaBuilder::build`].
///
/// ## Examples
///
/// ```
/// use surql::schema::bucket_schema;
///
/// let b = bucket_schema("uploads", "s3://my-bucket")
///     .readonly(true)
///     .comment("read-only mirror")
///     .build()
///     .unwrap();
/// let sql = b.to_surql().unwrap();
/// assert!(sql.contains("BACKEND \"s3://my-bucket\""));
/// assert!(sql.contains("READONLY"));
/// ```
pub fn bucket_schema(name: impl Into<String>, backend: impl Into<String>) -> BucketSchemaBuilder {
    BucketSchemaBuilder {
        inner: BucketDefinition::new(name, backend),
    }
}

/// Convenience constructor for an in-memory (`BACKEND "memory"`) bucket.
pub fn memory_bucket(name: impl Into<String>) -> BucketDefinition {
    BucketDefinition::new(name, "memory")
}

/// Convenience constructor for a local-filesystem (`BACKEND "file:/<path>"`)
/// bucket.
///
/// `path` is the directory under which files are stored; it is prefixed with
/// `file:` to form the backend URL. The directory must be present in the
/// server's `SURREAL_BUCKET_FOLDER_ALLOWLIST`.
///
/// ## Examples
///
/// ```
/// use surql::schema::file_bucket;
///
/// let b = file_bucket("docs", "/var/data/docs");
/// assert_eq!(
///     b.to_surql().unwrap(),
///     "DEFINE BUCKET docs BACKEND \"file:/var/data/docs\";"
/// );
/// ```
pub fn file_bucket(name: impl Into<String>, path: impl AsRef<str>) -> BucketDefinition {
    let path = path.as_ref();
    let backend = if let Some(stripped) = path.strip_prefix("file:") {
        format!("file:{stripped}")
    } else {
        format!("file:{path}")
    };
    BucketDefinition::new(name, backend)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn memory_bucket_minimal_surql() {
        let b = memory_bucket("avatars");
        assert_eq!(
            b.to_surql().unwrap(),
            "DEFINE BUCKET avatars BACKEND \"memory\";"
        );
    }

    #[test]
    fn file_bucket_prefixes_backend() {
        let b = file_bucket("docs", "/var/data/docs");
        assert_eq!(
            b.to_surql().unwrap(),
            "DEFINE BUCKET docs BACKEND \"file:/var/data/docs\";"
        );
    }

    #[test]
    fn file_bucket_does_not_double_prefix() {
        let b = file_bucket("docs", "file:/var/data/docs");
        assert_eq!(b.backend, "file:/var/data/docs");
    }

    #[test]
    fn readonly_renders() {
        let b = memory_bucket("ro").with_readonly(true);
        assert_eq!(
            b.to_surql().unwrap(),
            "DEFINE BUCKET ro BACKEND \"memory\" READONLY;"
        );
    }

    #[test]
    fn comment_renders() {
        let b = memory_bucket("c").with_comment("hello");
        let sql = b.to_surql().unwrap();
        assert!(sql.ends_with("COMMENT \"hello\";"));
    }

    #[test]
    fn permissions_render_as_one_posture() {
        // A bucket has a single permission; the per-action `FOR select`
        // form this used to render is a parse error.
        let b = bucket_schema("p", "memory")
            .permissions("WHERE $auth.id != NONE")
            .build()
            .unwrap();
        assert_eq!(
            b.to_surql().unwrap(),
            "DEFINE BUCKET p BACKEND \"memory\" PERMISSIONS WHERE $auth.id != NONE;"
        );
        assert!(bucket_schema("p", "memory")
            .permissions("FOR select WHERE true")
            .build()
            .is_err());
        assert!(bucket_schema("p", "memory")
            .permissions("NONE")
            .build()
            .is_ok());
    }

    #[test]
    fn literals_that_need_escaping_cannot_break_out() {
        let b = memory_bucket("c").with_comment("x\"; REMOVE TABLE user; --");
        assert_eq!(
            b.to_surql().unwrap(),
            r#"DEFINE BUCKET c BACKEND "memory" COMMENT 'x"; REMOVE TABLE user; --';"#
        );
        let b = file_bucket("d", r"C:\data");
        assert_eq!(
            b.to_surql().unwrap(),
            r"DEFINE BUCKET d BACKEND 'file:C:\\data';"
        );
        let b = memory_bucket("it's").with_comment("user's files");
        assert_eq!(
            b.to_surql().unwrap(),
            "DEFINE BUCKET `it's` BACKEND \"memory\" COMMENT \"user's files\";"
        );
        assert_eq!(
            BucketDefinition::remove_surql("a-b"),
            "REMOVE BUCKET `a-b`;"
        );
    }

    #[test]
    fn if_not_exists_guard() {
        let b = memory_bucket("g");
        let sql = b.to_surql_with_options(true, false).unwrap();
        assert!(sql.starts_with("DEFINE BUCKET IF NOT EXISTS g BACKEND"));
    }

    #[test]
    fn overwrite_guard_wins_over_if_not_exists() {
        let b = memory_bucket("g");
        let sql = b.to_surql_with_options(true, true).unwrap();
        assert!(sql.starts_with("DEFINE BUCKET OVERWRITE g BACKEND"));
    }

    #[test]
    fn s3_backend_full_statement() {
        let b = bucket_schema("uploads", "s3://my-bucket")
            .readonly(true)
            .comment("mirror")
            .build()
            .unwrap();
        assert_eq!(
            b.to_surql().unwrap(),
            "DEFINE BUCKET uploads BACKEND \"s3://my-bucket\" READONLY COMMENT \"mirror\";"
        );
    }

    #[test]
    fn remove_surql() {
        assert_eq!(
            BucketDefinition::remove_surql("avatars"),
            "REMOVE BUCKET avatars;"
        );
        assert_eq!(
            memory_bucket("avatars").to_remove_surql(),
            "REMOVE BUCKET avatars;"
        );
    }

    #[test]
    fn alter_sets_readonly() {
        let from = memory_bucket("b");
        let to = memory_bucket("b").with_readonly(true);
        assert_eq!(to.to_alter_surql(&from, false), "ALTER BUCKET b READONLY;");
    }

    #[test]
    fn alter_drops_readonly() {
        let from = memory_bucket("b").with_readonly(true);
        let to = memory_bucket("b");
        assert_eq!(
            to.to_alter_surql(&from, false),
            "ALTER BUCKET b DROP READONLY;"
        );
    }

    #[test]
    fn alter_changes_backend() {
        let from = memory_bucket("b");
        let to = BucketDefinition::new("b", "s3://x");
        assert_eq!(
            to.to_alter_surql(&from, false),
            "ALTER BUCKET b BACKEND \"s3://x\";"
        );
    }

    #[test]
    fn alter_drops_comment() {
        let from = memory_bucket("b").with_comment("old");
        let to = memory_bucket("b");
        assert_eq!(
            to.to_alter_surql(&from, false),
            "ALTER BUCKET b DROP COMMENT;"
        );
    }

    #[test]
    fn alter_sets_comment() {
        let from = memory_bucket("b");
        let to = memory_bucket("b").with_comment("new");
        assert_eq!(
            to.to_alter_surql(&from, false),
            "ALTER BUCKET b COMMENT \"new\";"
        );
    }

    #[test]
    fn alter_if_exists_guard() {
        let from = memory_bucket("b");
        let to = memory_bucket("b").with_readonly(true);
        assert_eq!(
            to.to_alter_surql(&from, true),
            "ALTER BUCKET IF EXISTS b READONLY;"
        );
    }

    #[test]
    fn alter_replaces_permissions() {
        let from = memory_bucket("b");
        let to = memory_bucket("b").with_permissions("WHERE true");
        let sql = to.to_alter_surql(&from, false);
        assert!(sql.contains("PERMISSIONS WHERE true"));
    }

    #[test]
    fn alter_clears_permissions_to_the_default() {
        let from = memory_bucket("b").with_permissions("WHERE true");
        let to = memory_bucket("b");
        let sql = to.to_alter_surql(&from, false);
        assert!(sql.contains("PERMISSIONS FULL"));
    }

    #[test]
    fn alter_no_change_is_bare_statement() {
        let b = memory_bucket("b").with_comment("same").with_readonly(true);
        assert_eq!(b.to_alter_surql(&b.clone(), false), "ALTER BUCKET b;");
    }

    #[test]
    fn validate_rejects_empty_name() {
        let mut b = memory_bucket("b");
        b.name = String::new();
        assert!(b.validate().is_err());
    }

    #[test]
    fn validate_rejects_empty_backend() {
        let mut b = memory_bucket("b");
        b.backend = String::new();
        assert!(b.validate().is_err());
    }

    #[test]
    fn builder_requires_valid() {
        assert!(bucket_schema("", "memory").build().is_err());
    }

    #[test]
    fn serde_roundtrip() {
        let b = bucket_schema("p", "memory")
            .readonly(true)
            .permissions("WHERE true")
            .comment("c")
            .build()
            .unwrap();
        let json = serde_json::to_string(&b).unwrap();
        let back: BucketDefinition = serde_json::from_str(&json).unwrap();
        assert_eq!(b, back);
    }

    #[test]
    fn clone_and_eq() {
        let b = memory_bucket("b");
        assert_eq!(b.clone(), b);
    }
}
