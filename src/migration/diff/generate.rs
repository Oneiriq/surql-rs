//! Generators: the [`SchemaDiff`] for each added, dropped, or changed
//! definition, its forward and backward SurrealQL rendered through
//! [`super::render`].

use std::collections::BTreeMap;

use super::render::{
    edge_define_sql, event_to_sql, field_to_sql, index_to_sql, remove_event_sql, remove_field_sql,
    remove_index_sql, remove_table_sql, render_permission_statements,
};
use super::validate::validate_default_value;
use crate::migration::models::{DiffOperation, SchemaDiff};
use crate::schema::edge::EdgeDefinition;
use crate::schema::fields::{render_field_path, FieldDefinition};
use crate::schema::table::{EventDefinition, IndexDefinition, TableDefinition};
use crate::types::escape::quote_ident;

pub(super) fn generate_add_table_diffs(table: &TableDefinition) -> Vec<SchemaDiff> {
    // The canonical renderer carries mode AND permissions in the one
    // statement; a separate permissions statement would re-define the
    // table it just created.
    let forward_sql = table.to_surql();
    let backward_sql = remove_table_sql(&table.name);
    let mut out = vec![SchemaDiff {
        operation: DiffOperation::AddTable,
        table: table.name.clone(),
        field: None,
        index: None,
        event: None,
        bucket: None,
        analyzer: None,
        object: None,
        description: format!("Add table {}", table.name),
        forward_sql,
        backward_sql,
        details: BTreeMap::new(),
    }];
    for field in &table.fields {
        out.push(generate_add_field_diff(&table.name, field));
    }
    for idx in &table.indexes {
        out.push(generate_add_index_diff(&table.name, idx));
    }
    for ev in &table.events {
        out.push(generate_add_event_diff(&table.name, ev));
    }
    out
}

pub(super) fn generate_drop_table_diffs(table: &TableDefinition) -> Vec<SchemaDiff> {
    // REMOVE TABLE takes the fields, indexes, and events with it, so the
    // rollback re-creates all of them, not just the table's shell.
    let mut restore = vec![table.to_surql()];
    restore.extend(member_statements(
        &table.name,
        &table.fields,
        &table.indexes,
        &table.events,
    ));
    vec![SchemaDiff {
        operation: DiffOperation::DropTable,
        table: table.name.clone(),
        field: None,
        index: None,
        event: None,
        bucket: None,
        analyzer: None,
        object: None,
        description: format!("Drop table {}", table.name),
        forward_sql: remove_table_sql(&table.name),
        backward_sql: restore.join("\n"),
        details: BTreeMap::new(),
    }]
}

/// The statements that define a table's fields, indexes, and events, as the
/// add diffs render them.
fn member_statements(
    table: &str,
    fields: &[FieldDefinition],
    indexes: &[IndexDefinition],
    events: &[EventDefinition],
) -> Vec<String> {
    fields
        .iter()
        .map(|field| field_to_sql(table, field))
        .chain(indexes.iter().map(|idx| index_to_sql(table, idx, false)))
        .chain(events.iter().map(|ev| event_to_sql(table, ev, false)))
        .collect()
}

/// A field's type for a diff's details: its custom type, or the keyword.
fn type_name(field: &FieldDefinition) -> String {
    field
        .custom_type
        .clone()
        .unwrap_or_else(|| field.field_type.as_str().to_string())
}

pub(super) fn generate_add_field_diff(table: &str, field: &FieldDefinition) -> SchemaDiff {
    let mut forward_sql = field_to_sql(table, field);
    if let Some(default) = field.default.as_deref() {
        // Best-effort backfill: failures to validate default surface as a
        // skipped backfill rather than a panic (matches conservative Python
        // path — though Python raises, Rust returns a safe render because
        // this function is infallible by contract).
        if validate_default_value(default).is_ok() {
            let backfill = format!(
                "UPDATE {table} SET {name} = {default} WHERE {name} IS NONE;",
                table = quote_ident(table),
                name = render_field_path(&field.name),
            );
            forward_sql.push('\n');
            forward_sql.push_str(&backfill);
        }
    }
    let backward_sql = remove_field_sql(table, &field.name);
    let mut details = BTreeMap::new();
    details.insert(
        "type".to_string(),
        serde_json::Value::String(type_name(field)),
    );
    SchemaDiff {
        operation: DiffOperation::AddField,
        table: table.to_string(),
        field: Some(field.name.clone()),
        index: None,
        event: None,
        bucket: None,
        analyzer: None,
        object: None,
        description: format!("Add field {} to {}", field.name, table),
        forward_sql,
        backward_sql,
        details,
    }
}

pub(super) fn generate_drop_field_diff(table: &str, field: &FieldDefinition) -> SchemaDiff {
    let forward_sql = remove_field_sql(table, &field.name);
    let backward_sql = field_to_sql(table, field);
    SchemaDiff {
        operation: DiffOperation::DropField,
        table: table.to_string(),
        field: Some(field.name.clone()),
        index: None,
        event: None,
        bucket: None,
        analyzer: None,
        object: None,
        description: format!("Drop field {} from {}", field.name, table),
        forward_sql,
        backward_sql,
        details: BTreeMap::new(),
    }
}

pub(super) fn generate_modify_field_diff(
    table: &str,
    old_field: &FieldDefinition,
    new_field: &FieldDefinition,
) -> SchemaDiff {
    // Plain DEFINE fails on an existing field; the replace form is
    // what a modification means.
    let forward_sql = new_field.to_surql_overwrite(table);
    let backward_sql = old_field.to_surql_overwrite(table);
    let mut details = BTreeMap::new();
    details.insert(
        "old_type".into(),
        serde_json::Value::String(type_name(old_field)),
    );
    details.insert(
        "new_type".into(),
        serde_json::Value::String(type_name(new_field)),
    );
    let mut description = format!("Modify field {} in {}", new_field.name, table);
    // Gaining REFERENCE is the one field change whose DDL alone leaves
    // the database lying: the engine backfills nothing, so every row
    // that already held a value stays invisible to `<~` until it is
    // rewritten (see [`crate::schema::reference_backfill_sql`]). The
    // rewrite rides `details` rather than `forward_sql` because it is
    // DML an application's own events may refuse, so a live reconciler
    // must choose where it runs; the migration generator, whose files
    // a person reviews, includes it right after the DDL.
    if old_field.reference.is_none() && new_field.reference.is_some() {
        // The Err arm is unreachable for schema-borne names, which
        // were validated at definition time; a name the validator
        // refuses could not have rendered the DDL above either.
        if let Ok(backfill) = crate::schema::reference_backfill_sql(table, &new_field.name) {
            details.insert(
                "reference_backfill_sql".into(),
                serde_json::Value::String(backfill),
            );
            description.push_str(" (gains REFERENCE: existing rows need the backfill rewrite)");
        }
    }
    SchemaDiff {
        operation: DiffOperation::ModifyField,
        table: table.to_string(),
        field: Some(new_field.name.clone()),
        index: None,
        event: None,
        bucket: None,
        analyzer: None,
        object: None,
        description,
        forward_sql,
        backward_sql,
        details,
    }
}

pub(super) fn generate_add_index_diff(table: &str, idx: &IndexDefinition) -> SchemaDiff {
    let forward_sql = index_to_sql(table, idx, false);
    let backward_sql = remove_index_sql(table, &idx.name);
    SchemaDiff {
        operation: DiffOperation::AddIndex,
        table: table.to_string(),
        field: None,
        index: Some(idx.name.clone()),
        event: None,
        bucket: None,
        analyzer: None,
        object: None,
        description: format!("Add index {} to {}", idx.name, table),
        forward_sql,
        backward_sql,
        details: BTreeMap::new(),
    }
}

pub(super) fn generate_drop_index_diff(table: &str, idx: &IndexDefinition) -> SchemaDiff {
    let forward_sql = remove_index_sql(table, &idx.name);
    // The same renderer as the add, so a UNIQUE (or full-text, or vector)
    // index comes back as the kind it was.
    let backward_sql = index_to_sql(table, idx, false);
    SchemaDiff {
        operation: DiffOperation::DropIndex,
        table: table.to_string(),
        field: None,
        index: Some(idx.name.clone()),
        event: None,
        bucket: None,
        analyzer: None,
        object: None,
        description: format!("Drop index {} from {}", idx.name, table),
        forward_sql,
        backward_sql,
        details: BTreeMap::new(),
    }
}

/// An index whose definition changed, re-defined whole in both directions
/// and reported as [`DiffOperation::ModifyIndex`].
pub(super) fn generate_modify_index_diff(
    table: &str,
    old_idx: &IndexDefinition,
    new_idx: &IndexDefinition,
) -> SchemaDiff {
    SchemaDiff {
        operation: DiffOperation::ModifyIndex,
        table: table.to_string(),
        field: None,
        index: Some(new_idx.name.clone()),
        event: None,
        bucket: None,
        analyzer: None,
        object: None,
        description: format!("Modify index {} on {}", new_idx.name, table),
        forward_sql: index_to_sql(table, new_idx, true),
        backward_sql: index_to_sql(table, old_idx, true),
        details: BTreeMap::new(),
    }
}

pub(super) fn generate_add_event_diff(table: &str, ev: &EventDefinition) -> SchemaDiff {
    let forward_sql = event_to_sql(table, ev, false);
    let backward_sql = remove_event_sql(table, &ev.name);
    SchemaDiff {
        operation: DiffOperation::AddEvent,
        table: table.to_string(),
        field: None,
        index: None,
        event: Some(ev.name.clone()),
        bucket: None,
        analyzer: None,
        object: None,
        description: format!("Add event {} to {}", ev.name, table),
        forward_sql,
        backward_sql,
        details: BTreeMap::new(),
    }
}

pub(super) fn generate_drop_event_diff(table: &str, ev: &EventDefinition) -> SchemaDiff {
    let forward_sql = remove_event_sql(table, &ev.name);
    let backward_sql = event_to_sql(table, ev, false);
    SchemaDiff {
        operation: DiffOperation::DropEvent,
        table: table.to_string(),
        field: None,
        index: None,
        event: Some(ev.name.clone()),
        bucket: None,
        analyzer: None,
        object: None,
        description: format!("Drop event {} from {}", ev.name, table),
        forward_sql,
        backward_sql,
        details: BTreeMap::new(),
    }
}

/// An event whose `WHEN` or `THEN` changed, re-defined whole in both
/// directions and reported as [`DiffOperation::ModifyEvent`].
pub(super) fn generate_modify_event_diff(
    table: &str,
    old_ev: &EventDefinition,
    new_ev: &EventDefinition,
) -> SchemaDiff {
    SchemaDiff {
        operation: DiffOperation::ModifyEvent,
        table: table.to_string(),
        field: None,
        index: None,
        event: Some(new_ev.name.clone()),
        bucket: None,
        analyzer: None,
        object: None,
        description: format!("Modify event {} on {}", new_ev.name, table),
        forward_sql: event_to_sql(table, new_ev, true),
        backward_sql: event_to_sql(table, old_ev, true),
        details: BTreeMap::new(),
    }
}

pub(super) fn generate_modify_permissions_diff(
    table: &str,
    new_permissions: Option<&BTreeMap<String, String>>,
    old_permissions: Option<&BTreeMap<String, String>>,
) -> SchemaDiff {
    let forward_sql = render_permission_statements(table, new_permissions);
    let backward_sql = render_permission_statements(table, old_permissions);
    SchemaDiff {
        operation: DiffOperation::ModifyPermissions,
        table: table.to_string(),
        field: None,
        index: None,
        event: None,
        bucket: None,
        analyzer: None,
        object: None,
        description: format!("Modify permissions for {table}"),
        forward_sql,
        backward_sql,
        details: BTreeMap::new(),
    }
}

pub(super) fn generate_add_edge_diffs(edge: &EdgeDefinition) -> Vec<SchemaDiff> {
    // One statement carries the mode, the endpoints, and the permissions. A
    // separate permissions statement would re-define the table it just
    // created, and its OVERWRITE form would reset `TYPE RELATION`.
    let forward_sql = edge_define_sql(edge, false);
    let backward_sql = remove_table_sql(&edge.name);

    let mut out = vec![SchemaDiff {
        operation: DiffOperation::AddTable,
        table: edge.name.clone(),
        field: None,
        index: None,
        event: None,
        bucket: None,
        analyzer: None,
        object: None,
        description: format!("Add edge {}", edge.name),
        forward_sql,
        backward_sql,
        details: BTreeMap::new(),
    }];
    for field in &edge.fields {
        out.push(generate_add_field_diff(&edge.name, field));
    }
    for idx in &edge.indexes {
        out.push(generate_add_index_diff(&edge.name, idx));
    }
    for ev in &edge.events {
        out.push(generate_add_event_diff(&edge.name, ev));
    }
    out
}

pub(super) fn generate_drop_edge_diffs(edge: &EdgeDefinition) -> Vec<SchemaDiff> {
    // As for a table: the rollback re-creates the edge and everything the
    // REMOVE took with it.
    let mut restore = vec![edge_define_sql(edge, false)];
    restore.extend(member_statements(
        &edge.name,
        &edge.fields,
        &edge.indexes,
        &edge.events,
    ));
    vec![SchemaDiff {
        operation: DiffOperation::DropTable,
        table: edge.name.clone(),
        field: None,
        index: None,
        event: None,
        bucket: None,
        analyzer: None,
        object: None,
        description: format!("Drop edge {}", edge.name),
        forward_sql: remove_table_sql(&edge.name),
        backward_sql: restore.join("\n"),
        details: BTreeMap::new(),
    }]
}
