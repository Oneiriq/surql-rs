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

use super::scan::{find_keyword, split_top_level as split_at};
use super::table::read_table;
use crate::schema::view::{ViewDefinition, ViewGroup};

/// Parse the `AS SELECT` body out of a `DEFINE TABLE` statement.
///
/// Returns `None` for a table that is not a view.
pub fn parse_view(definition: &str) -> Option<ViewDefinition> {
    let body = read_table(definition).view?;
    let body = body
        .get("SELECT".len()..)
        .unwrap_or("")
        .trim()
        .trim_end_matches(';')
        .trim();

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

/// Split `text` at the first top-level occurrence of `keyword`, returning the
/// text before it and the text after it.
fn split_at_keyword(text: &str, keyword: &str) -> Option<(String, String)> {
    let at = find_keyword(text, keyword)?;
    Some((
        text.get(..at)?.trim().to_string(),
        text.get(at + keyword.len()..)?.trim().to_string(),
    ))
}

/// Split a comma-separated list, ignoring commas nested in brackets or
/// quotes.
fn split_top_level(text: &str) -> Vec<String> {
    split_at(text, ',')
        .into_iter()
        .map(str::trim)
        .filter(|item| !item.is_empty())
        .map(str::to_string)
        .collect()
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
