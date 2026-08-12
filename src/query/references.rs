//! Reverse-reference (`<~`) query helpers.
//!
//! The read half of [`DEFINE FIELD ... REFERENCE`](crate::schema::reference):
//! `<~<table>` walks incoming links back to the records pointing at the
//! selected row, and `<~<table>.{ a, b }` narrows what comes back.
//!
//! [`Query::reverse_traverse`](crate::query::builder::Query::reverse_traverse)
//! is the builder-side entry point; the pure renderer lives here so it is
//! testable without a query.

use crate::error::Result;

use super::builder::Query;

/// Render a `<~<table>[.{ f, ... }] AS <alias>` projection item.
///
/// An empty `fields` slice renders the whole-record form, same as `None`.
pub fn reverse_reference_projection(table: &str, fields: Option<&[&str]>, alias: &str) -> String {
    let projection = match fields {
        Some(fields) if !fields.is_empty() => format!(".{{ {} }}", fields.join(", ")),
        _ => String::new(),
    };
    format!("<~{table}{projection} AS {alias}")
}

/// Build `SELECT *, <~<source> AS <alias> FROM <table>`.
///
/// The convenience form of
/// [`Query::reverse_traverse`](crate::query::builder::Query::reverse_traverse)
/// for the common "list everything that points at me" read.
///
/// ## Examples
///
/// ```
/// use surql::query::references::reverse_reference_query;
///
/// let q = reverse_reference_query("comic_book", "person", None, "owners").unwrap();
/// assert_eq!(
///     q.to_surql().unwrap(),
///     "SELECT *, <~person AS owners FROM comic_book",
/// );
/// ```
pub fn reverse_reference_query(
    table: &str,
    source: &str,
    fields: Option<&[&str]>,
    alias: &str,
) -> Result<Query> {
    Ok(Query::new()
        .select(None)
        .from_table(table)?
        .reverse_traverse(source, fields, alias))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn projection_without_fields_is_the_whole_record() {
        assert_eq!(
            reverse_reference_projection("person", None, "owners"),
            "<~person AS owners"
        );
    }

    #[test]
    fn projection_with_fields_destructures() {
        assert_eq!(
            reverse_reference_projection("person", Some(&["id", "name"]), "owners"),
            "<~person.{ id, name } AS owners"
        );
    }

    #[test]
    fn empty_field_slice_falls_back_to_the_whole_record() {
        assert_eq!(
            reverse_reference_projection("person", Some(&[]), "owners"),
            "<~person AS owners"
        );
    }

    #[test]
    fn query_helper_renders_a_select() {
        let q = reverse_reference_query("comic_book", "person", Some(&["id"]), "owners").unwrap();
        assert_eq!(
            q.to_surql().unwrap(),
            "SELECT *, <~person.{ id } AS owners FROM comic_book"
        );
    }

    #[test]
    fn query_helper_rejects_an_invalid_table() {
        assert!(reverse_reference_query("1bad", "person", None, "owners").is_err());
    }

    #[test]
    fn traversal_composes_with_an_explicit_projection() {
        let q = Query::new()
            .select(Some(vec!["title".into()]))
            .from_table("comic_book")
            .unwrap()
            .reverse_traverse("person", None, "owners");
        assert_eq!(
            q.to_surql().unwrap(),
            "SELECT title, <~person AS owners FROM comic_book"
        );
    }
}
