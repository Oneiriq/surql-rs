//! `DEFINE TABLE ... AS SELECT` (view) parser.
//!
//! Reconstructs a [`ViewDefinition`] from the `DEFINE TABLE` statement the
//! engine echoes in `INFO FOR DB`, which looks like
//!
//! ```text
//! DEFINE TABLE stats TYPE NORMAL SCHEMALESS
//!   AS SELECT count() AS total, author FROM comment
//!   WHERE n > 2 GROUP BY author PERMISSIONS NONE
//! ```
//!
//! Splitting is depth-aware so a projection such as `math::max([a, b])` is
//! not torn apart at the comma inside it.

use crate::schema::view::{ViewDefinition, ViewGroup};

/// Parse the `AS SELECT` body out of a `DEFINE TABLE` statement.
///
/// Returns `None` for a table that is not a view.
pub fn parse_view(definition: &str) -> Option<ViewDefinition> {
    let body = clause_after(definition, "AS SELECT")?;
    let body = truncate_at_clause(&body);
    let body = body.trim().trim_end_matches(';').trim();

    let (projections, rest) = split_at_keyword(body, "FROM")?;
    let (tables, rest) = match split_at_keyword(&rest, "WHERE") {
        Some((tables, after)) => (tables, Some(("WHERE", after))),
        None => match split_at_keyword(&rest, "GROUP") {
            Some((tables, after)) => (tables, Some(("GROUP", after))),
            None => (rest.clone(), None),
        },
    };

    let mut view = ViewDefinition::new(split_top_level(&projections), split_top_level(&tables));
    match rest {
        Some(("WHERE", after)) => {
            let (condition, group) = match split_at_keyword(&after, "GROUP") {
                Some((condition, group)) => (condition, Some(group)),
                None => (after, None),
            };
            if !condition.trim().is_empty() {
                view = view.with_condition(condition.trim());
            }
            if let Some(group) = group {
                view.group = parse_group(&group);
            }
        }
        Some((_, after)) => view.group = parse_group(&after),
        None => {}
    }
    Some(view)
}

/// Read the `GROUP` operand: `ALL` or a field list.
fn parse_group(rest: &str) -> Option<ViewGroup> {
    let rest = rest.trim();
    if rest.is_empty() {
        return None;
    }
    if rest.eq_ignore_ascii_case("ALL") {
        return Some(ViewGroup::All);
    }
    let fields = rest
        .strip_prefix("BY ")
        .or_else(|| rest.strip_prefix("by "));
    let fields = split_top_level(fields.unwrap_or(rest));
    if fields.is_empty() {
        None
    } else {
        Some(ViewGroup::By(fields))
    }
}

/// Everything after the first case-insensitive occurrence of `keyword`.
fn clause_after(text: &str, keyword: &str) -> Option<String> {
    let at = find_keyword(text, keyword)?;
    Some(text[at + keyword.len()..].to_string())
}

/// Split `text` at the first top-level occurrence of `keyword`, returning the
/// text before it and the text after it.
fn split_at_keyword(text: &str, keyword: &str) -> Option<(String, String)> {
    let at = find_keyword(text, keyword)?;
    Some((
        text[..at].trim().to_string(),
        text[at + keyword.len()..].trim().to_string(),
    ))
}

/// Cut the `AS SELECT` body at the first clause that can follow it.
///
/// `COMMENT` only counts when a quoted string follows: `comment` is also a
/// perfectly ordinary table name, and a view selecting `FROM comment` must
/// not be truncated at its own source.
fn truncate_at_clause(text: &str) -> String {
    let mut end = text.len();
    for keyword in ["PERMISSIONS", "CHANGEFEED"] {
        if let Some(at) = find_keyword(text, keyword) {
            end = end.min(at);
        }
    }
    let mut from = 0;
    while let Some(at) = find_keyword_from(text, "COMMENT", from) {
        let after = text[at + "COMMENT".len()..].trim_start();
        if after.starts_with('\'') || after.starts_with('"') {
            end = end.min(at);
            break;
        }
        from = at + "COMMENT".len();
    }
    text[..end].to_string()
}

/// Locate `keyword` case-insensitively at a word boundary and outside any
/// bracket, parenthesis, or quoted run.
fn find_keyword(text: &str, keyword: &str) -> Option<usize> {
    find_keyword_from(text, keyword, 0)
}

/// [`find_keyword`] starting the scan at byte offset `from`.
fn find_keyword_from(text: &str, keyword: &str, from: usize) -> Option<usize> {
    let haystack = text.to_ascii_uppercase();
    let needle = keyword.to_ascii_uppercase();
    let bytes = haystack.as_bytes();
    let needle = needle.as_bytes();
    let mut depth = 0i32;
    let mut quote: Option<u8> = None;
    let mut i = from;
    while i < bytes.len() {
        let b = bytes[i];
        match quote {
            Some(q) => {
                if b == q {
                    quote = None;
                }
                i += 1;
                continue;
            }
            None => match b {
                b'\'' | b'"' => {
                    quote = Some(b);
                    i += 1;
                    continue;
                }
                b'(' | b'[' | b'{' => depth += 1,
                b')' | b']' | b'}' => depth -= 1,
                _ => {}
            },
        }
        if depth == 0 && i + needle.len() <= bytes.len() && bytes[i..i + needle.len()] == *needle {
            let left_ok = i == 0 || !is_word_byte(bytes[i - 1]);
            let right_ok =
                i + needle.len() == bytes.len() || !is_word_byte(bytes[i + needle.len()]);
            if left_ok && right_ok {
                return Some(i);
            }
        }
        i += 1;
    }
    None
}

fn is_word_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

/// Split a comma-separated list, ignoring commas nested in brackets or
/// quotes.
fn split_top_level(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut depth = 0i32;
    let mut quote: Option<char> = None;
    let mut current = String::new();
    for c in text.chars() {
        match quote {
            Some(q) => {
                current.push(c);
                if c == q {
                    quote = None;
                }
                continue;
            }
            None => match c {
                '\'' | '"' => quote = Some(c),
                '(' | '[' | '{' => depth += 1,
                ')' | ']' | '}' => depth -= 1,
                ',' if depth == 0 => {
                    push_trimmed(&mut out, &current);
                    current.clear();
                    continue;
                }
                _ => {}
            },
        }
        current.push(c);
    }
    push_trimmed(&mut out, &current);
    out
}

fn push_trimmed(out: &mut Vec<String>, value: &str) {
    let trimmed = value.trim();
    if !trimmed.is_empty() {
        out.push(trimmed.to_string());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_plain_table_is_not_a_view() {
        assert!(parse_view("DEFINE TABLE user TYPE NORMAL SCHEMAFULL PERMISSIONS NONE").is_none());
        assert!(parse_view("").is_none());
    }

    #[test]
    fn engine_echo_with_group_by() {
        let view = parse_view(
            "DEFINE TABLE v1 TYPE NORMAL SCHEMAFULL AS SELECT count() AS total, author \
             FROM comment GROUP BY author PERMISSIONS NONE",
        )
        .expect("view");
        assert_eq!(view.projections, ["count() AS total", "author"]);
        assert_eq!(view.tables, ["comment"]);
        assert!(view.condition.is_none());
        assert_eq!(view.group, Some(ViewGroup::by(["author"])));
    }

    #[test]
    fn engine_echo_with_where_and_group_all() {
        let view = parse_view(
            "DEFINE TABLE v2 TYPE NORMAL SCHEMALESS AS SELECT count() AS c FROM comment \
             WHERE n > 2 GROUP ALL PERMISSIONS NONE",
        )
        .expect("view");
        assert_eq!(view.projections, ["count() AS c"]);
        assert_eq!(view.condition.as_deref(), Some("n > 2"));
        assert_eq!(view.group, Some(ViewGroup::All));
    }

    #[test]
    fn engine_echo_with_several_sources() {
        let view = parse_view(
            "DEFINE TABLE multi TYPE ANY SCHEMALESS AS SELECT id FROM comment, person \
             PERMISSIONS NONE",
        )
        .expect("view");
        assert_eq!(view.tables, ["comment", "person"]);
        assert!(view.group.is_none());
    }

    #[test]
    fn a_where_without_a_group_keeps_the_whole_predicate() {
        let view = parse_view("DEFINE TABLE v SCHEMALESS AS SELECT id FROM comment WHERE n > 2;")
            .expect("view");
        assert_eq!(view.condition.as_deref(), Some("n > 2"));
        assert!(view.group.is_none());
    }

    #[test]
    fn a_changefeed_after_the_view_does_not_leak_in() {
        let view = parse_view(
            "DEFINE TABLE v TYPE NORMAL SCHEMALESS AS SELECT id FROM comment CHANGEFEED 1d \
             PERMISSIONS NONE",
        )
        .expect("view");
        assert_eq!(view.tables, ["comment"]);
        assert!(view.condition.is_none());
    }

    #[test]
    fn commas_inside_a_projection_are_not_split_points() {
        let view = parse_view(
            "DEFINE TABLE v SCHEMALESS AS SELECT math::max([a, b]) AS top, author FROM comment",
        )
        .expect("view");
        assert_eq!(view.projections, ["math::max([a, b]) AS top", "author"]);
    }

    #[test]
    fn a_quoted_keyword_is_not_a_clause_boundary() {
        let view = parse_view(
            "DEFINE TABLE v SCHEMALESS AS SELECT id FROM comment WHERE tag = 'group by'",
        )
        .expect("view");
        assert_eq!(view.condition.as_deref(), Some("tag = 'group by'"));
        assert!(view.group.is_none());
    }

    #[test]
    fn a_source_table_named_comment_is_not_a_comment_clause() {
        let view = parse_view(
            "DEFINE TABLE v TYPE NORMAL SCHEMALESS AS SELECT id FROM comment PERMISSIONS NONE",
        )
        .expect("view");
        assert_eq!(view.tables, ["comment"]);
    }

    #[test]
    fn a_real_comment_clause_is_a_boundary() {
        let view = parse_view(
            "DEFINE TABLE v SCHEMALESS AS SELECT id FROM comment COMMENT 'per-author rollup'",
        )
        .expect("view");
        assert_eq!(view.tables, ["comment"]);
        assert!(view.condition.is_none());
    }

    #[test]
    fn a_view_round_trips_through_its_own_renderer() {
        let statement = "DEFINE TABLE v TYPE NORMAL SCHEMALESS AS SELECT count() AS total, author \
                         FROM comment WHERE n > 2 GROUP BY author PERMISSIONS NONE";
        let view = parse_view(statement).expect("view");
        assert_eq!(
            view.to_clause(),
            " AS SELECT count() AS total, author FROM comment WHERE n > 2 GROUP BY author"
        );
    }
}
