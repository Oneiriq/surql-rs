//! `DEFINE BUCKET` parser.
//!
//! Extracts [`BucketDefinition`] values from SurrealDB `INFO FOR DB`
//! responses so buckets round-trip through `INFO FOR DB` → parser →
//! `diff_buckets`, mirroring [`super::access`]. Split out of the monolithic
//! `parser.rs` so each submodule stays under the 1000-LOC budget; see parent
//! [`super`] for the public entry points.

use std::sync::OnceLock;

use regex::Regex;

use super::permissions::parse_table_permissions;
use super::regex_case_insensitive;
use crate::schema::bucket::BucketDefinition;

// --- Regex accessors ---------------------------------------------------------

fn backend_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    // Accept both quoted (`BACKEND "memory"`) and bare (`BACKEND memory`)
    // forms; SurrealDB echoes the quoted form but tolerate either on input.
    RE.get_or_init(|| regex_case_insensitive(r#"BACKEND\s+(?:"([^"]*)"|'([^']*)'|(\S+))"#))
}

fn readonly_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| regex_case_insensitive(r"\bREADONLY\b"))
}

fn comment_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| regex_case_insensitive(r#"(?s)COMMENT\s+(?:"([^"]*)"|'([^']*)')"#))
}

// --- Public parser -----------------------------------------------------------

/// Parse one `DEFINE BUCKET` statement into a [`BucketDefinition`].
///
/// Returns `None` when the definition is empty or has no `BACKEND` clause
/// (the backend is required, so a definition without one is not a usable
/// bucket).
pub fn parse_bucket(name: &str, definition: &str) -> Option<BucketDefinition> {
    if definition.is_empty() {
        return None;
    }
    let backend = extract_backend(definition)?;

    let mut bucket = BucketDefinition::new(name, backend);
    bucket.readonly = readonly_regex().is_match(definition);
    bucket.permissions = parse_table_permissions(definition);
    bucket.comment = extract_comment(definition);
    Some(bucket)
}

// --- Extractors --------------------------------------------------------------

fn extract_backend(definition: &str) -> Option<String> {
    let caps = backend_regex().captures(definition)?;
    caps.get(1)
        .or_else(|| caps.get(2))
        .or_else(|| caps.get(3))
        .map(|m| m.as_str().to_string())
}

fn extract_comment(definition: &str) -> Option<String> {
    let caps = comment_regex().captures(definition)?;
    caps.get(1)
        .or_else(|| caps.get(2))
        .map(|m| m.as_str().to_string())
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
    fn parses_permissions() {
        let b = parse_bucket(
            "p",
            "DEFINE BUCKET p BACKEND \"memory\" PERMISSIONS FOR select WHERE $auth.id != NONE",
        )
        .unwrap();
        let perms = b.permissions.expect("permissions parsed");
        assert_eq!(
            perms.get("select").map(String::as_str),
            Some("$auth.id != NONE")
        );
    }

    #[test]
    fn parses_bare_backend() {
        let b = parse_bucket("b", "DEFINE BUCKET b BACKEND memory").unwrap();
        assert_eq!(b.backend, "memory");
    }
}
