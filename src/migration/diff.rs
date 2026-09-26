//! Schema diffing engine.
//!
//! Port of `surql/migration/diff.py`. Compares two schema snapshots
//! (code-side vs database-side, or two code versions) and produces a list
//! of [`SchemaDiff`] entries describing every additive, destructive, or
//! modifying schema change.
//!
//! ## Public API
//!
//! The public entrypoints are free functions that operate on slices of
//! schema definitions and return a [`Vec<SchemaDiff>`]:
//!
//! - [`diff_tables`] — compare two sets of [`TableDefinition`]s.
//! - [`diff_fields`] — compare two sets of [`FieldDefinition`]s for a table.
//! - [`diff_indexes`] — compare two sets of [`IndexDefinition`]s for a table.
//! - [`diff_events`] — compare two sets of [`EventDefinition`]s for a table.
//! - [`diff_permissions`] — compare two permission maps for a table.
//! - [`diff_edges`] — compare two sets of [`EdgeDefinition`]s.
//! - [`diff_buckets`] / [`diff_analyzers`] — re-exported from
//!   [`crate::migration::diff_objects`], which holds every database-level
//!   object diff.
//! - [`diff_schemas`] — aggregate diff across full [`SchemaSnapshot`]s.
//!
//! The comparisons behind them are public too, for callers (the schema
//! validator among them) that need the same notion of "unchanged":
//! [`fields_equal`], [`indexes_equal`], [`events_equal`],
//! [`permissions_equal`] with its [`table_permissions_equal`] and
//! [`field_permissions_equal`] forms, [`expr_eq`], and
//! [`normalize_expression`].
//!
//! ## Deviation from Python
//!
//! In the Python implementation the per-category diff helpers take a single
//! pair of objects (`old`, `new`). The Rust port exposes slice-based
//! signatures that internally compute the pair-wise comparison by name.
//! The old pair-wise helpers are preserved as `diff_*_pair` functions for
//! callers that want the fine-grained semantics (for example the migration
//! generator). Functions that render SurrealQL require an explicit `table`
//! parameter because the field / index / event / permission slices do not
//! carry table context on their own.
//!
//! ## Expression normalisation
//!
//! Field expressions (`assertion`, `default`, `value`) are compared using
//! whitespace-normalised equality so that cosmetic reformatting by the
//! database server does not produce spurious diffs.
//!
//! ## Layout
//!
//! This file holds the snapshot type and the walks that pair definitions up
//! by name. The rest lives in submodules, re-exported here: the comparisons
//! (`equality`), expression normalisation (`normalize`), the diff
//! generators (`generate`), the statements the diff renders itself
//! (`render`), and the expression safety checks (`validate`).

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

pub use crate::migration::diff_objects::{diff_analyzers, diff_buckets};
use crate::migration::diff_objects::{diff_functions, diff_params, diff_sequences};
use crate::migration::models::{DiffOperation, SchemaDiff};
use crate::schema::bucket::BucketDefinition;
use crate::schema::edge::{EdgeDefinition, EdgeMode};
use crate::schema::fields::FieldDefinition;
use crate::schema::function::FunctionDefinition;
use crate::schema::param::ParamDefinition;
use crate::schema::sequence::SequenceDefinition;
use crate::schema::table::{EventDefinition, IndexDefinition, TableDefinition};
use crate::schema::view::ViewDefinition;

mod equality;
mod generate;
mod normalize;
mod render;
mod validate;

pub use equality::{
    events_equal, field_permissions_equal, fields_equal, indexes_equal, permissions_equal,
    table_permissions_equal,
};
use generate::{
    generate_add_edge_diffs, generate_add_event_diff, generate_add_field_diff,
    generate_add_index_diff, generate_add_table_diffs, generate_drop_edge_diffs,
    generate_drop_event_diff, generate_drop_field_diff, generate_drop_index_diff,
    generate_drop_table_diffs, generate_modify_event_diff, generate_modify_field_diff,
    generate_modify_index_diff, generate_modify_permissions_diff,
};
pub use normalize::{expr_eq, normalize_expression};
use render::edge_define_sql;
pub use validate::{validate_default_value, validate_event_expression};

/// Full schema snapshot passed to [`diff_schemas`].
///
/// Pairs table definitions with edge definitions to support a single-call
/// diff across all objects in a schema. Order is preserved as supplied but
/// comparisons are name-based so the input order does not affect output.
///
/// ## Examples
///
/// ```
/// use surql::migration::diff::SchemaSnapshot;
/// use surql::schema::table::table_schema;
///
/// let snapshot = SchemaSnapshot {
///     tables: vec![table_schema("user")],
///     edges: vec![],
///     ..Default::default()
/// };
/// assert_eq!(snapshot.tables.len(), 1);
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaSnapshot {
    /// All tables known to this snapshot (in discovery order).
    #[serde(default)]
    pub tables: Vec<TableDefinition>,
    /// All edges known to this snapshot (in discovery order).
    #[serde(default)]
    pub edges: Vec<EdgeDefinition>,
    /// All object-storage buckets known to this snapshot (in discovery
    /// order). Defaults to empty so older snapshots without a `buckets` key
    /// still deserialise.
    #[serde(default)]
    pub buckets: Vec<BucketDefinition>,
    /// All text analyzers known to this snapshot (in discovery
    /// order). Defaults to empty so older snapshots still
    /// deserialise.
    #[serde(default)]
    pub analyzers: Vec<crate::schema::analyzer::AnalyzerDefinition>,
    /// All ID sequences known to this snapshot (in discovery order).
    /// Defaults to empty so older snapshots still deserialise.
    #[serde(default)]
    pub sequences: Vec<SequenceDefinition>,
    /// All custom functions known to this snapshot (in discovery order).
    /// Defaults to empty so older snapshots still deserialise.
    #[serde(default)]
    pub functions: Vec<FunctionDefinition>,
    /// All database-level params known to this snapshot (in discovery order).
    /// Defaults to empty so older snapshots still deserialise.
    #[serde(default)]
    pub params: Vec<ParamDefinition>,
}

impl SchemaSnapshot {
    /// Construct an empty snapshot.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Convenience constructor from tables + edges iterators (no buckets).
    pub fn from_parts<T, E>(tables: T, edges: E) -> Self
    where
        T: IntoIterator<Item = TableDefinition>,
        E: IntoIterator<Item = EdgeDefinition>,
    {
        Self {
            tables: tables.into_iter().collect(),
            edges: edges.into_iter().collect(),
            ..Default::default()
        }
    }

    /// Convenience constructor from tables + edges + buckets iterators.
    pub fn from_all_parts<T, E, B>(tables: T, edges: E, buckets: B) -> Self
    where
        T: IntoIterator<Item = TableDefinition>,
        E: IntoIterator<Item = EdgeDefinition>,
        B: IntoIterator<Item = BucketDefinition>,
    {
        Self {
            tables: tables.into_iter().collect(),
            edges: edges.into_iter().collect(),
            buckets: buckets.into_iter().collect(),
            ..Default::default()
        }
    }
}
// ---------------------------------------------------------------------------
// Public slice-based API
// ---------------------------------------------------------------------------

/// Compare two slices of tables and produce every required schema change.
///
/// Tables present in `code` but not in `db` are added (with all contained
/// fields/indexes/events/permissions). Tables present in `db` but not in
/// `code` are dropped. Tables present in both are recursively compared.
///
/// ## Examples
///
/// ```
/// use surql::migration::diff::diff_tables;
/// use surql::schema::table::table_schema;
///
/// let code = vec![table_schema("user")];
/// let db: Vec<_> = vec![];
/// let diffs = diff_tables(&code, &db);
/// assert_eq!(diffs.len(), 1);
/// ```
#[must_use]
pub fn diff_tables(code: &[TableDefinition], db: &[TableDefinition]) -> Vec<SchemaDiff> {
    let code_map = index_by_name(code, |t| t.name.as_str());
    let db_map = index_by_name(db, |t| t.name.as_str());
    let mut out: Vec<SchemaDiff> = Vec::new();

    // The maps iterate in name order, so the output is stable whatever
    // order the slices came in.
    // Added tables — present in code, absent in db.
    for (name, table) in &code_map {
        if !db_map.contains_key(name) {
            out.extend(generate_add_table_diffs(table));
        }
    }
    // Dropped tables — present in db, absent in code.
    for (name, table) in &db_map {
        if !code_map.contains_key(name) {
            out.extend(generate_drop_table_diffs(table));
        }
    }
    // Modified tables — present in both, diff recursively.
    for (name, code_table) in &code_map {
        if let Some(db_table) = db_map.get(name) {
            out.extend(diff_table_pair_inner(code_table, db_table));
        }
    }
    out
}

/// Compare two field slices for the named table.
///
/// Added fields appear first (in `code` order), followed by dropped fields
/// (in `db` order), followed by modified fields (in `code` order).
///
/// ## Examples
///
/// ```
/// use surql::migration::diff::diff_fields;
/// use surql::schema::fields::{FieldDefinition, FieldType};
///
/// let code = vec![FieldDefinition::new("email", FieldType::String)];
/// let db: Vec<FieldDefinition> = vec![];
/// let diffs = diff_fields("user", &code, &db);
/// assert_eq!(diffs.len(), 1);
/// ```
#[must_use]
pub fn diff_fields(
    table: &str,
    code: &[FieldDefinition],
    db: &[FieldDefinition],
) -> Vec<SchemaDiff> {
    let code_map = index_by_name(code, |f| f.name.as_str());
    let db_map = index_by_name(db, |f| f.name.as_str());
    let mut out: Vec<SchemaDiff> = Vec::new();

    for f in code {
        if !db_map.contains_key(f.name.as_str()) {
            out.push(generate_add_field_diff(table, f));
        }
    }
    for f in db {
        if !code_map.contains_key(f.name.as_str()) {
            // The engine defines array-child fields (`name.*`) on its
            // own beside a declared array parent; those are engine
            // bookkeeping, never removals.
            if let Some(parent) = f.name.strip_suffix(".*") {
                if code_map.contains_key(parent) {
                    continue;
                }
            }
            out.push(generate_drop_field_diff(table, f));
        }
    }
    for f in code {
        if let Some(db_field) = db_map.get(f.name.as_str()) {
            if !fields_equal(f, db_field) {
                out.push(generate_modify_field_diff(table, db_field, f));
            }
        }
    }
    out
}

/// Compare two index slices for the named table.
///
/// Indexes only in `code` are added and indexes only in `db` dropped. An
/// index in both whose definition differs (see [`indexes_equal`]) is
/// re-defined with `DEFINE INDEX OVERWRITE` in both directions, reported as
/// [`DiffOperation::ModifyTable`] with [`SchemaDiff::index`] naming it.
#[must_use]
pub fn diff_indexes(
    table: &str,
    code: &[IndexDefinition],
    db: &[IndexDefinition],
) -> Vec<SchemaDiff> {
    let code_map = index_by_name(code, |i| i.name.as_str());
    let db_map = index_by_name(db, |i| i.name.as_str());
    let mut out: Vec<SchemaDiff> = Vec::new();

    for idx in code {
        if !db_map.contains_key(idx.name.as_str()) {
            out.push(generate_add_index_diff(table, idx));
        }
    }
    for idx in db {
        if !code_map.contains_key(idx.name.as_str()) {
            out.push(generate_drop_index_diff(table, idx));
        }
    }
    for idx in code {
        if let Some(db_idx) = db_map.get(idx.name.as_str()) {
            if !indexes_equal(idx, db_idx) {
                out.push(generate_modify_index_diff(table, db_idx, idx));
            }
        }
    }
    out
}

/// Compare two event slices for the named table.
///
/// Events only in `code` are added and events only in `db` dropped. An event
/// in both whose `WHEN` or `THEN` differs (see [`events_equal`]) is
/// re-defined with `DEFINE EVENT OVERWRITE` in both directions, reported as
/// [`DiffOperation::ModifyTable`] with [`SchemaDiff::event`] naming it.
#[must_use]
pub fn diff_events(
    table: &str,
    code: &[EventDefinition],
    db: &[EventDefinition],
) -> Vec<SchemaDiff> {
    let code_map = index_by_name(code, |e| e.name.as_str());
    let db_map = index_by_name(db, |e| e.name.as_str());
    let mut out: Vec<SchemaDiff> = Vec::new();

    for ev in code {
        if !db_map.contains_key(ev.name.as_str()) {
            out.push(generate_add_event_diff(table, ev));
        }
    }
    for ev in db {
        if !code_map.contains_key(ev.name.as_str()) {
            out.push(generate_drop_event_diff(table, ev));
        }
    }
    for ev in code {
        if let Some(db_ev) = db_map.get(ev.name.as_str()) {
            if !events_equal(ev, db_ev) {
                out.push(generate_modify_event_diff(table, db_ev, ev));
            }
        }
    }
    out
}

/// Compare two permission maps for the named table.
///
/// Emits at most one [`SchemaDiff`] describing the delta. If the maps are
/// equal, returns an empty vector. Both directions render
/// `ALTER TABLE <table> PERMISSIONS ...`, which replaces the permission set
/// and leaves every other clause of the table alone; an absent map renders
/// `PERMISSIONS NONE`, the engine's default for a table.
///
/// [`diff_tables`] and [`diff_edges`] carry permission changes as the full
/// `DEFINE TABLE OVERWRITE` statement instead, since they have the whole
/// definition to hand.
#[must_use]
pub fn diff_permissions(
    table: &str,
    code: Option<&BTreeMap<String, String>>,
    db: Option<&BTreeMap<String, String>>,
) -> Vec<SchemaDiff> {
    if table_permissions_equal(code, db) {
        return Vec::new();
    }
    vec![generate_modify_permissions_diff(table, code, db)]
}

/// Compare two edge slices.
///
/// Same high-level behaviour as [`diff_tables`]: added edges produce add
/// diffs for the edge and all of its contained objects; dropped edges
/// produce drop diffs; edges present in both are recursively compared on
/// fields/indexes/events/permissions. A change to an edge's own shape (its
/// mode, or a relation's `FROM` / `TO` table) re-defines it with
/// `DEFINE TABLE OVERWRITE`, reported as [`DiffOperation::ModifyTable`].
#[must_use]
pub fn diff_edges(code: &[EdgeDefinition], db: &[EdgeDefinition]) -> Vec<SchemaDiff> {
    let code_map = index_by_name(code, |e| e.name.as_str());
    let db_map = index_by_name(db, |e| e.name.as_str());
    let mut out: Vec<SchemaDiff> = Vec::new();

    for (name, edge) in &code_map {
        if !db_map.contains_key(name) {
            out.extend(generate_add_edge_diffs(edge));
        }
    }
    for (name, edge) in &db_map {
        if !code_map.contains_key(name) {
            out.extend(generate_drop_edge_diffs(edge));
        }
    }
    for (name, code_edge) in &code_map {
        if let Some(db_edge) = db_map.get(name) {
            out.extend(diff_edge_pair_inner(code_edge, db_edge));
        }
    }
    out
}

/// Diff two complete snapshots and return every change required to make
/// `db` look like `code`.
///
/// The diffs come in the order a migration can apply them, and a rollback
/// (which runs the backward statements in reverse) can undo them:
///
/// 1. functions, params, sequences, analyzers, and buckets being added or
///    changed, so the tables that use them find them defined: a full-text
///    index cannot build over existing rows until its analyzer exists, and
///    a field backfill can call a function or read a param;
/// 2. every table and edge being dropped, before anything is defined, so a
///    name that turns from an edge into a table (or back) is free again;
/// 3. the remaining table changes, then the remaining edge changes;
/// 4. the database-level objects being dropped, last and in the reverse
///    kind order, once nothing that used them is left (the engine refuses
///    to remove an analyzer a full-text index still names).
#[must_use]
pub fn diff_schemas(code: &SchemaSnapshot, db: &SchemaSnapshot) -> Vec<SchemaDiff> {
    let objects = [
        diff_functions(&code.functions, &db.functions),
        diff_params(&code.params, &db.params),
        diff_sequences(&code.sequences, &db.sequences),
        diff_analyzers(&code.analyzers, &db.analyzers),
        diff_buckets(&code.buckets, &db.buckets),
    ];
    let mut out = Vec::new();
    let mut object_drops = Vec::new();
    for diffs in objects {
        let (drops, rest): (Vec<SchemaDiff>, Vec<SchemaDiff>) =
            diffs.into_iter().partition(|d| is_object_drop(d.operation));
        out.extend(rest);
        object_drops.push(drops);
    }
    let (table_drops, table_rest): (Vec<SchemaDiff>, Vec<SchemaDiff>) =
        diff_tables(&code.tables, &db.tables)
            .into_iter()
            .chain(diff_edges(&code.edges, &db.edges))
            .partition(|d| d.operation == DiffOperation::DropTable);
    out.extend(table_drops);
    out.extend(table_rest);
    out.extend(object_drops.into_iter().rev().flatten());
    out
}

/// Whether `operation` removes a database-level object.
fn is_object_drop(operation: DiffOperation) -> bool {
    matches!(
        operation,
        DiffOperation::DropFunction
            | DiffOperation::DropParam
            | DiffOperation::DropSequence
            | DiffOperation::DropAnalyzer
            | DiffOperation::DropBucket
    )
}

// ---------------------------------------------------------------------------
// Pair-wise helpers (preserved for migration-generator callers and tests)
// ---------------------------------------------------------------------------

/// Compare a single pair of tables, handling add/drop/modify.
///
/// Matches the semantics of the Python `diff_tables(old, new)` helper: pass
/// `None` for "table does not exist on that side".
#[must_use]
pub fn diff_table_pair(
    code: Option<&TableDefinition>,
    db: Option<&TableDefinition>,
) -> Vec<SchemaDiff> {
    match (code, db) {
        (Some(code), None) => generate_add_table_diffs(code),
        (None, Some(db)) => generate_drop_table_diffs(db),
        (Some(code), Some(db)) => diff_table_pair_inner(code, db),
        (None, None) => Vec::new(),
    }
}

/// Compare a single pair of edges, handling add/drop/modify.
#[must_use]
pub fn diff_edge_pair(
    code: Option<&EdgeDefinition>,
    db: Option<&EdgeDefinition>,
) -> Vec<SchemaDiff> {
    match (code, db) {
        (Some(code), None) => generate_add_edge_diffs(code),
        (None, Some(db)) => generate_drop_edge_diffs(db),
        (Some(code), Some(db)) => diff_edge_pair_inner(code, db),
        (None, None) => Vec::new(),
    }
}

fn diff_table_pair_inner(code: &TableDefinition, db: &TableDefinition) -> Vec<SchemaDiff> {
    let mut out = diff_fields(&code.name, &code.fields, &db.fields);
    out.extend(diff_indexes(&code.name, &code.indexes, &db.indexes));
    out.extend(diff_events(&code.name, &code.events, &db.events));
    out.extend(diff_table_body(code, db));
    if !table_permissions_equal(code.permissions.as_ref(), db.permissions.as_ref()) {
        // The full definition replaces: a permissions-only DEFINE
        // TABLE would silently reset the table's mode.
        out.push(modify_permissions_full(
            &code.name,
            code.to_surql_overwrite(),
            db.to_surql_overwrite(),
        ));
    }
    out
}

fn diff_edge_pair_inner(code: &EdgeDefinition, db: &EdgeDefinition) -> Vec<SchemaDiff> {
    let mut out = Vec::new();
    if edge_shape(code) != edge_shape(db) {
        out.push(SchemaDiff {
            operation: DiffOperation::ModifyTable,
            table: code.name.clone(),
            field: None,
            index: None,
            event: None,
            bucket: None,
            analyzer: None,
            object: None,
            description: format!("Modify edge {}", code.name),
            forward_sql: edge_define_sql(code, true),
            backward_sql: edge_define_sql(db, true),
            details: BTreeMap::new(),
        });
    }
    out.extend(diff_fields(&code.name, &code.fields, &db.fields));
    out.extend(diff_indexes(&code.name, &code.indexes, &db.indexes));
    out.extend(diff_events(&code.name, &code.events, &db.events));
    if !table_permissions_equal(code.permissions.as_ref(), db.permissions.as_ref()) {
        out.push(modify_permissions_full(
            &code.name,
            edge_define_sql(code, true),
            edge_define_sql(db, true),
        ));
    }
    out
}

/// What an edge's own `DEFINE TABLE` says about its shape: the mode and, for
/// a relation, the two endpoint tables. A non-relation edge renders no
/// endpoints, so any it carries are not part of its shape.
fn edge_shape(edge: &EdgeDefinition) -> (EdgeMode, Option<String>, Option<String>) {
    match edge.mode {
        EdgeMode::Relation => (
            edge.mode,
            edge.from_table.as_deref().map(normalize_expression),
            edge.to_table.as_deref().map(normalize_expression),
        ),
        mode => (mode, None, None),
    }
}

/// Compare the parts of a `DEFINE TABLE` statement that belong to the table
/// itself rather than to a field, index, event, or permission rule: the
/// change feed and the `AS SELECT` view body.
///
/// The table mode is deliberately left out. It is carried by the same
/// `OVERWRITE` statement, so a change to it rides along with any of the other
/// diffs; making it its own trigger would change long-standing behaviour for
/// every schema that leaves the mode implicit.
fn diff_table_body(code: &TableDefinition, db: &TableDefinition) -> Vec<SchemaDiff> {
    let changefeed_changed = code.changefeed != db.changefeed;
    // Views compare on the rendered clause with whitespace normalised: the
    // engine reformats a projection or predicate as it likes, and a diff on
    // the spacing alone would re-apply the view on every reconcile.
    let view_changed = !expr_eq(
        code.view.as_ref().map(ViewDefinition::to_clause).as_deref(),
        db.view.as_ref().map(ViewDefinition::to_clause).as_deref(),
    );
    if !changefeed_changed && !view_changed {
        return Vec::new();
    }
    let what = if view_changed { "view" } else { "change feed" };
    vec![SchemaDiff {
        operation: DiffOperation::ModifyTable,
        table: code.name.clone(),
        field: None,
        index: None,
        event: None,
        bucket: None,
        analyzer: None,
        object: None,
        description: format!("Modify {what} on {}", code.name),
        // The full definition replaces: a bare CHANGEFEED or AS SELECT
        // statement would silently reset the table's mode and permissions.
        forward_sql: code.to_surql_overwrite(),
        backward_sql: db.to_surql_overwrite(),
        details: BTreeMap::new(),
    }]
}

/// A permissions change carried as the owning definition's full
/// `OVERWRITE` form.
fn modify_permissions_full(table: &str, forward_sql: String, backward_sql: String) -> SchemaDiff {
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

/// Key `items` by name. The map iterates in name order, which is what makes
/// every diff's output order independent of the input order; a later
/// duplicate name replaces an earlier one.
pub(super) fn index_by_name<'a, T, F>(items: &'a [T], key: F) -> BTreeMap<&'a str, &'a T>
where
    F: Fn(&'a T) -> &'a str,
{
    items.iter().map(|item| (key(item), item)).collect()
}

#[cfg(test)]
mod tests;
