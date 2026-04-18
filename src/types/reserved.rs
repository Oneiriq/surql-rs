//! Reserved-word validation for SurrealDB field names.
//!
//! Port of `surql/types/reserved.py`. Performs case-insensitive checks
//! against a canonical set of SurrealDB reserved words and returns a
//! warning message when a chosen name would collide.

use std::sync::OnceLock;

/// Full list of SurrealDB reserved words.
///
/// Exposed as a read-only slice. For set-style membership use
/// [`is_reserved_word`] or [`check_reserved_word`].
pub const SURREAL_RESERVED_WORDS: &[&str] = &[
    "select",
    "from",
    "where",
    "group",
    "order",
    "limit",
    "start",
    "fetch",
    "timeout",
    "parallel",
    "value",
    "content",
    "set",
    "create",
    "update",
    "delete",
    "relate",
    "insert",
    "define",
    "remove",
    "begin",
    "commit",
    "cancel",
    "return",
    "let",
    "if",
    "else",
    "then",
    "end",
    "for",
    "break",
    "continue",
    "throw",
    "none",
    "null",
    "true",
    "false",
    "and",
    "or",
    "not",
    "is",
    "contains",
    "inside",
    "outside",
    "intersects",
    "type",
    "table",
    "field",
    "index",
    "event",
    "namespace",
    "database",
    "scope",
    "token",
    "info",
    "live",
    "kill",
    "sleep",
    "use",
    "in",
    "out",
];

/// Edge-allowed reserved words. `in`/`out` are permitted on edge schemas.
pub const EDGE_ALLOWED_RESERVED: &[&str] = &["in", "out"];

fn reserved_set() -> &'static std::collections::HashSet<&'static str> {
    static SET: OnceLock<std::collections::HashSet<&'static str>> = OnceLock::new();
    SET.get_or_init(|| SURREAL_RESERVED_WORDS.iter().copied().collect())
}

fn edge_allowed_set() -> &'static std::collections::HashSet<&'static str> {
    static SET: OnceLock<std::collections::HashSet<&'static str>> = OnceLock::new();
    SET.get_or_init(|| EDGE_ALLOWED_RESERVED.iter().copied().collect())
}

/// Return `true` if `name` (case-insensitive, leaf of a dot path) is reserved.
pub fn is_reserved_word(name: &str) -> bool {
    let leaf = leaf_segment(name).to_ascii_lowercase();
    reserved_set().contains(leaf.as_str())
}

/// Check whether `name` collides with a SurrealDB reserved word.
///
/// Returns `Some(warning)` when the name is reserved, or `None` when safe.
/// Dot-notation names have only their leaf segment checked, matching Python
/// behaviour.
///
/// ## Examples
///
/// ```
/// use surql::types::check_reserved_word;
///
/// assert!(check_reserved_word("select", false).is_some());
/// assert!(check_reserved_word("user.name", false).is_none());
/// assert!(check_reserved_word("user.select", false).is_some());
/// // edge_allowed_fields permits `in`/`out`
/// assert!(check_reserved_word("in", true).is_none());
/// assert!(check_reserved_word("in", false).is_some());
/// ```
pub fn check_reserved_word(name: &str, allow_edge_fields: bool) -> Option<String> {
    let leaf = leaf_segment(name);
    let lower = leaf.to_ascii_lowercase();

    if !reserved_set().contains(lower.as_str()) {
        return None;
    }

    if allow_edge_fields && edge_allowed_set().contains(lower.as_str()) {
        return None;
    }

    Some(format!(
        "Field name {name:?} collides with SurrealDB reserved word {lower:?}. \
         This may cause unexpected query behavior."
    ))
}

fn leaf_segment(name: &str) -> &str {
    name.rsplit('.').next().unwrap_or(name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reserved_word_detected_case_insensitive() {
        assert!(is_reserved_word("SELECT"));
        assert!(is_reserved_word("select"));
        assert!(is_reserved_word("Where"));
    }

    #[test]
    fn safe_names_not_reserved() {
        assert!(!is_reserved_word("username"));
        assert!(!is_reserved_word("user_name"));
        assert!(!is_reserved_word("email"));
    }

    #[test]
    fn dot_notation_checks_leaf() {
        // leaf = `name`, not reserved
        assert!(check_reserved_word("user.name", false).is_none());
        // leaf = `select`, reserved
        assert!(check_reserved_word("user.select", false).is_some());
    }

    #[test]
    fn edge_allowed_words_pass_with_flag() {
        assert!(check_reserved_word("in", true).is_none());
        assert!(check_reserved_word("out", true).is_none());
        assert!(check_reserved_word("in", false).is_some());
    }

    #[test]
    fn warning_mentions_reserved_name() {
        let msg = check_reserved_word("select", false).unwrap();
        assert!(msg.contains("select"));
        assert!(msg.contains("reserved word"));
    }
}
