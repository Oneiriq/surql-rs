//! The SurrealQL the diff renders: removals, the permissions-only
//! `ALTER TABLE`, and the edge shape the canonical renderer refuses, with
//! every name quoted the way the `DEFINE` renderers quote it. Definitions
//! otherwise go through their canonical renderers.

use std::collections::BTreeMap;

use crate::schema::edge::EdgeDefinition;
use crate::schema::fields::{render_field_path, render_table_list, FieldDefinition};
use crate::schema::permissions::render_permissions_clause;
use crate::schema::table::{render_inline_caps, EventDefinition, IndexDefinition};
use crate::types::escape::quote_ident;

/// Render an edge's `DEFINE TABLE` statement, in its `OVERWRITE` form when
/// `overwrite` is set.
///
/// The canonical renderer refuses a `RELATION` edge that names only one
/// endpoint (or none). The engine accepts that shape and constrains just the
/// side that is named, so it renders here instead of vanishing from the diff.
pub(super) fn edge_define_sql(edge: &EdgeDefinition, overwrite: bool) -> String {
    let canonical = if overwrite {
        edge.to_surql_overwrite()
    } else {
        edge.to_surql()
    };
    canonical.unwrap_or_else(|_| {
        let guard = if overwrite { " OVERWRITE" } else { "" };
        let mut sql = format!(
            "DEFINE TABLE{guard} {} TYPE RELATION",
            quote_ident(&edge.name)
        );
        if let Some(from) = edge.from_table.as_deref() {
            sql.push_str(" FROM ");
            sql.push_str(&render_table_list(from));
        }
        if let Some(to) = edge.to_table.as_deref() {
            sql.push_str(" TO ");
            sql.push_str(&render_table_list(to));
        }
        sql.push_str(edge.relation_flags());
        sql.push_str(&render_inline_caps(
            edge.inline_edges,
            edge.inline_references,
        ));
        sql.push_str(&render_permissions_clause(edge.permissions.as_ref()));
        sql.push(';');
        sql
    })
}

/// Render a permission map as an `ALTER TABLE ... PERMISSIONS` statement.
///
/// `ALTER` replaces the table's whole permission set and nothing else, so the
/// table's mode, type, endpoints, and fields survive. `DEFINE TABLE` could not
/// carry a permissions-only change: the plain form fails on a table that
/// exists, and the `OVERWRITE` form resets every clause it does not repeat.
/// An absent or empty map is the engine's default for a table, `NONE`.
pub(super) fn render_permission_statements(
    table: &str,
    perms: Option<&BTreeMap<String, String>>,
) -> String {
    let clause = render_permissions_clause(perms);
    let clause = if clause.is_empty() {
        " PERMISSIONS NONE".to_owned()
    } else {
        clause
    };
    format!("ALTER TABLE {}{clause};", quote_ident(table))
}

/// `REMOVE TABLE <name>;`, the name quoted as the `DEFINE` renderers quote it.
pub(super) fn remove_table_sql(name: &str) -> String {
    format!("REMOVE TABLE {};", quote_ident(name))
}

/// `REMOVE FIELD <path> ON TABLE <table>;`
pub(super) fn remove_field_sql(table: &str, field: &str) -> String {
    format!(
        "REMOVE FIELD {} ON TABLE {};",
        render_field_path(field),
        quote_ident(table)
    )
}

/// `REMOVE INDEX <name> ON TABLE <table>;`
pub(super) fn remove_index_sql(table: &str, index: &str) -> String {
    format!(
        "REMOVE INDEX {} ON TABLE {};",
        quote_ident(index),
        quote_ident(table)
    )
}

/// `REMOVE EVENT <name> ON TABLE <table>;`
pub(super) fn remove_event_sql(table: &str, event: &str) -> String {
    format!(
        "REMOVE EVENT {} ON TABLE {};",
        quote_ident(event),
        quote_ident(table)
    )
}

/// Render a field's `DEFINE FIELD` statement through the canonical renderer,
/// so clause ordering (FLEXIBLE after TYPE, VALUE placement) has exactly one
/// implementation.
pub(super) fn field_to_sql(table: &str, field: &FieldDefinition) -> String {
    field.to_surql(table)
}

/// Render an index's `DEFINE INDEX` statement, in its `OVERWRITE` form when
/// `overwrite` is set, through the canonical renderer. Every direction of
/// the diff goes through here, so an index is always re-created as the kind
/// it was, with the tail the engine echoes and its names quoted.
pub(super) fn index_to_sql(table: &str, idx: &IndexDefinition, overwrite: bool) -> String {
    if overwrite {
        idx.to_surql_overwrite(table)
    } else {
        idx.to_surql(table)
    }
}

/// Render an event's `DEFINE EVENT` statement, in its `OVERWRITE` form when
/// `overwrite` is set, through the canonical renderer, which keeps a
/// multi-statement action inside one `{ ... }` block.
pub(super) fn event_to_sql(table: &str, ev: &EventDefinition, overwrite: bool) -> String {
    if overwrite {
        ev.to_surql_overwrite(table)
    } else {
        ev.to_surql(table)
    }
}
