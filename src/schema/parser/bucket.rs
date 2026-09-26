//! `DEFINE BUCKET` parser.
//!
//! Extracts [`BucketDefinition`] values from SurrealDB `INFO FOR DB`
//! responses so buckets round-trip through `INFO FOR DB` → parser →
//! `diff_buckets`, mirroring [`super::access`]. Split out of the monolithic
//! `parser.rs` so each submodule stays under the 1000-LOC budget; see parent
//! [`super`] for the public entry points.
//!
//! The engine echoes `DEFINE BUCKET <name> [READONLY] BACKEND '<url>'
//! PERMISSIONS <NONE | FULL | WHERE expr> [COMMENT '<text>']`: a bucket has a
//! single permission, not a per-action list.

use super::scan::{clause, clauses, define_head, string_literal, Shape};
use crate::schema::bucket::{BucketDefinition, DEFAULT_BUCKET_PERMISSIONS};

const BUCKET_CLAUSES: &[(&str, Shape)] = &[
    ("READONLY", Shape::Flag),
    ("BACKEND", Shape::Expr),
    ("PERMISSIONS", Shape::Expr),
    ("COMMENT", Shape::Str),
];

// --- Public parser -----------------------------------------------------------

/// Parse one `DEFINE BUCKET` statement into a [`BucketDefinition`].
///
/// The engine always echoes a `PERMISSIONS` clause; its default,
/// [`DEFAULT_BUCKET_PERMISSIONS`], reads back as `None`, which is what a
/// definition that never set one holds. String literals are unescaped.
///
/// Returns `None` when the definition is empty or has no `BACKEND` clause
/// (the backend is required, so a definition without one is not a usable
/// bucket).
pub fn parse_bucket(name: &str, definition: &str) -> Option<BucketDefinition> {
    if definition.is_empty() {
        return None;
    }
    let body = define_head(definition, "BUCKET", false).map_or(definition, |head| head.rest);
    let found = clauses(body, BUCKET_CLAUSES);
    let backend = clause(&found, "BACKEND").filter(|b| !b.is_empty())?;
    let backend = string_literal(backend).unwrap_or_else(|| backend.to_string());

    let mut bucket = BucketDefinition::new(name, backend);
    bucket.readonly = clause(&found, "READONLY").is_some();
    bucket.permissions = clause(&found, "PERMISSIONS")
        .filter(|p| !p.is_empty() && !p.eq_ignore_ascii_case(DEFAULT_BUCKET_PERMISSIONS))
        .map(str::to_string);
    bucket.comment = clause(&found, "COMMENT").and_then(string_literal);
    Some(bucket)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_definition_is_none() {
        assert!(parse_bucket("b", "").is_none());
    }

    #[test]
    fn missing_backend_is_none() {
        assert!(parse_bucket("b", "DEFINE BUCKET b").is_none());
    }

    #[test]
    fn parses_minimal_memory_bucket() {
        let b = parse_bucket("avatars", "DEFINE BUCKET avatars BACKEND \"memory\"").unwrap();
        assert_eq!(b.name, "avatars");
        assert_eq!(b.backend, "memory");
        assert!(!b.readonly);
        assert!(b.permissions.is_none());
        assert!(b.comment.is_none());
    }

    #[test]
    fn parses_readonly() {
        let b = parse_bucket("b", "DEFINE BUCKET b BACKEND \"memory\" READONLY").unwrap();
        assert!(b.readonly);
    }

    #[test]
    fn parses_comment() {
        let b = parse_bucket(
            "b",
            "DEFINE BUCKET b BACKEND \"memory\" COMMENT \"hello world\"",
        )
        .unwrap();
        assert_eq!(b.comment.as_deref(), Some("hello world"));
    }

    #[test]
    fn parses_s3_backend() {
        let b = parse_bucket("u", "DEFINE BUCKET u BACKEND \"s3://my-bucket\"").unwrap();
        assert_eq!(b.backend, "s3://my-bucket");
    }

    #[test]
    fn parses_file_backend() {
        let b = parse_bucket("d", "DEFINE BUCKET d BACKEND \"file:/var/data\"").unwrap();
        assert_eq!(b.backend, "file:/var/data");
    }

    #[test]
    fn parses_the_engine_echo_with_its_single_permission() {
        let b = parse_bucket(
            "p",
            "DEFINE BUCKET p READONLY BACKEND 'memory' PERMISSIONS WHERE $auth.id != NONE \
             COMMENT 'x'",
        )
        .unwrap();
        assert!(b.readonly);
        assert_eq!(b.permissions.as_deref(), Some("WHERE $auth.id != NONE"));
        assert_eq!(b.comment.as_deref(), Some("x"));
        let b = parse_bucket("p", "DEFINE BUCKET p BACKEND 'memory' PERMISSIONS FULL").unwrap();
        assert!(b.permissions.is_none());
    }

    #[test]
    fn escaped_literals_are_unescaped() {
        let b = parse_bucket(
            "d",
            r"DEFINE BUCKET d BACKEND 'file:C:\\data' PERMISSIONS FULL COMMENT 'a\nb'",
        )
        .unwrap();
        assert_eq!(b.backend, r"file:C:\data");
        assert_eq!(b.comment.as_deref(), Some("a\nb"));
    }

    #[test]
    fn parses_bare_backend() {
        let b = parse_bucket("b", "DEFINE BUCKET b BACKEND memory").unwrap();
        assert_eq!(b.backend, "memory");
    }
}
