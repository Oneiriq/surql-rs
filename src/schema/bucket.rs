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

use std::collections::BTreeMap;
use std::fmt::Write as _;

use serde::{Deserialize, Serialize};

use crate::error::{Result, SurqlError};

/// Render a per-action `PERMISSIONS` clause body shared by tables and buckets.
///
/// Produces `FOR <action> WHERE <rule> ...` (space-joined), the only valid
/// inline placement. Returns an empty string when `permissions` is `None` or
/// empty so callers can append it unconditionally.
fn render_permissions_clause(permissions: Option<&BTreeMap<String, String>>) -> String {
    match permissions {
        Some(perms) if !perms.is_empty() => {
            let clauses: Vec<String> = perms
                .iter()
                .map(|(action, rule)| format!("FOR {action} WHERE {rule}"))
                .collect();
            format!(" PERMISSIONS {}", clauses.join(" "))
        }
        _ => String::new(),
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
    /// Per-action permission rules keyed by action name, rendered inline as a
    /// `PERMISSIONS FOR <action> WHERE <rule>` clause — the same shape as
    /// [`TableDefinition::permissions`](crate::schema::TableDefinition).
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub permissions: Option<BTreeMap<String, String>>,
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

    /// Set the comment.
    pub fn with_comment(mut self, comment: impl Into<String>) -> Self {
        self.comment = Some(comment.into());
        self
    }

    /// Validate the bucket definition.
    ///
    /// Returns [`SurqlError::Validation`] when the name or backend is empty.
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
        Ok(())
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
            "DEFINE BUCKET {guard}{name} BACKEND \"{backend}\"",
            guard = guard,
            name = self.name,
            backend = self.backend,
        );
        if self.readonly {
            sql.push_str(" READONLY");
        }
        sql.push_str(&render_permissions_clause(self.permissions.as_ref()));
        if let Some(comment) = &self.comment {
            write!(sql, " COMMENT \"{comment}\"").expect("writing to String cannot fail");
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
        format!("REMOVE BUCKET {name};")
    }

    /// Render an `ALTER BUCKET` statement that turns `from` into `self`.
    ///
    /// Only the fields that differ are emitted. A backend present on `from`
    /// but absent on `self` is impossible (backend is required), so the
    /// `DROP BACKEND` clause is never produced here; a `None` comment on
    /// `self` paired with a `Some` comment on `from` renders `DROP COMMENT`.
    /// Read-only transitions render `READONLY` / `DROP READONLY`. Permission
    /// changes re-emit the full `PERMISSIONS` clause (SurrealQL replaces the
    /// whole posture).
    ///
    /// `if_exists` adds the `IF EXISTS` guard for idempotent re-application.
    pub fn to_alter_surql(&self, from: &BucketDefinition, if_exists: bool) -> String {
        let guard = if if_exists { "IF EXISTS " } else { "" };
        let mut sql = format!(
            "ALTER BUCKET {guard}{name}",
            guard = guard,
            name = self.name
        );

        if self.readonly != from.readonly {
            if self.readonly {
                sql.push_str(" READONLY");
            } else {
                sql.push_str(" DROP READONLY");
            }
        }

        if self.backend != from.backend {
            write!(sql, " BACKEND \"{}\"", self.backend).expect("writing to String cannot fail");
        }

        if self.permissions != from.permissions {
            let clause = render_permissions_clause(self.permissions.as_ref());
            if clause.is_empty() {
                // Clearing all per-action rules: re-assert the default-deny
                // posture explicitly so the ALTER is not a no-op.
                sql.push_str(" PERMISSIONS NONE");
            } else {
                sql.push_str(&clause);
            }
        }

        if self.comment != from.comment {
            match &self.comment {
                Some(comment) => {
                    write!(sql, " COMMENT \"{comment}\"").expect("writing to String cannot fail");
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
    fn permissions_render_inline() {
        let b = bucket_schema("p", "memory")
            .permissions([("select", "$auth.id != NONE"), ("create", "$auth.admin")])
            .build()
            .unwrap();
        let sql = b.to_surql().unwrap();
        assert!(sql.contains("PERMISSIONS FOR"));
        assert!(sql.contains("FOR select WHERE $auth.id != NONE"));
        assert!(sql.contains("FOR create WHERE $auth.admin"));
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
        let to = memory_bucket("b").with_permissions([("select", "true")]);
        let sql = to.to_alter_surql(&from, false);
        assert!(sql.contains("PERMISSIONS FOR select WHERE true"));
    }

    #[test]
    fn alter_clears_permissions_to_none() {
        let from = memory_bucket("b").with_permissions([("select", "true")]);
        let to = memory_bucket("b");
        let sql = to.to_alter_surql(&from, false);
        assert!(sql.contains("PERMISSIONS NONE"));
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
            .permissions([("select", "true")])
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
