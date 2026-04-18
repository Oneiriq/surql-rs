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
//! - [`diff_schemas`] — aggregate diff across full [`SchemaSnapshot`]s.
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

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::error::{Result, SurqlError};
use crate::migration::models::{DiffOperation, SchemaDiff};
use crate::schema::edge::{EdgeDefinition, EdgeMode};
use crate::schema::fields::FieldDefinition;
use crate::schema::table::{
    EventDefinition, HnswDistanceType, IndexDefinition, IndexType, MTreeDistanceType,
    MTreeVectorType, TableDefinition,
};

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
}

impl SchemaSnapshot {
    /// Construct an empty snapshot.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Convenience constructor from two iterators.
    pub fn from_parts<T, E>(tables: T, edges: E) -> Self
    where
        T: IntoIterator<Item = TableDefinition>,
        E: IntoIterator<Item = EdgeDefinition>,
    {
        Self {
            tables: tables.into_iter().collect(),
            edges: edges.into_iter().collect(),
        }
    }
}

/// Regex characters treated as safe in a default-value expression.
///
/// Preserved verbatim from the Python implementation to keep the validation
/// behaviour identical across runtimes.
const SAFE_DEFAULT_PATTERN: &str = concat!(
    r"^(",
    r"[a-zA-Z_][a-zA-Z0-9_]*(?:::[a-zA-Z_][a-zA-Z0-9_]*)*\([^;]*\)",
    r"|-?\d+(?:\.\d+)?",
    r"|true|false",
    r"|NONE|NULL",
    r"|'(?:[^'\\]|\\.)*'",
    r"|\$[a-zA-Z_][a-zA-Z0-9_]*",
    r")$",
);

fn safe_default_regex() -> &'static regex::Regex {
    static RE: std::sync::OnceLock<regex::Regex> = std::sync::OnceLock::new();
    RE.get_or_init(|| regex::Regex::new(SAFE_DEFAULT_PATTERN).expect("valid regex"))
}

/// Validate that an event expression has no injection patterns.
///
/// Mirrors `_validate_event_expression` in Python: rejects statement
/// separators (`;`) and SQL comments (`--`).
///
/// # Errors
///
/// Returns [`SurqlError::Validation`] when the expression contains a
/// banned pattern.
pub fn validate_event_expression(expr: &str, label: &str) -> Result<()> {
    let stripped = expr.trim();
    if stripped.contains("; ") || stripped.contains(";--") || stripped.ends_with(';') {
        return Err(SurqlError::Validation {
            reason: format!(
                "Unsafe event {label}: {expr:?}. Event {label}s must not contain statement separators."
            ),
        });
    }
    if stripped.contains("--") {
        return Err(SurqlError::Validation {
            reason: format!(
                "Unsafe event {label}: {expr:?}. Event {label}s must not contain SQL comments."
            ),
        });
    }
    Ok(())
}

/// Validate that a default-value expression is one of the allowlisted forms.
///
/// # Errors
///
/// Returns [`SurqlError::Validation`] when the expression does not match
/// the safe-default pattern.
pub fn validate_default_value(default: &str) -> Result<()> {
    if !safe_default_regex().is_match(default.trim()) {
        return Err(SurqlError::Validation {
            reason: format!(
                "Unsafe default value expression: {default:?}. \
                 Defaults must be function calls, literals, or parameter references."
            ),
        });
    }
    Ok(())
}

/// Normalise whitespace in an expression for semantic equality comparison.
///
/// Collapses runs of whitespace to a single space and trims the ends. Used
/// when comparing field expressions — a database server may reformat
/// expressions when echoing them back.
#[must_use]
pub fn normalize_expression(expr: &str) -> String {
    let mut out = String::with_capacity(expr.len());
    let mut in_space = false;
    for ch in expr.trim().chars() {
        if ch.is_whitespace() {
            if !in_space {
                out.push(' ');
                in_space = true;
            }
        } else {
            out.push(ch);
            in_space = false;
        }
    }
    out
}

fn expr_eq(a: Option<&str>, b: Option<&str>) -> bool {
    match (a, b) {
        (None, None) => true,
        (Some(x), Some(y)) => normalize_expression(x) == normalize_expression(y),
        _ => false,
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

    // Added tables — present in code, absent in db.
    for name in sorted_keys(&code_map) {
        if !db_map.contains_key(name) {
            out.extend(generate_add_table_diffs(code_map[name]));
        }
    }
    // Dropped tables — present in db, absent in code.
    for name in sorted_keys(&db_map) {
        if !code_map.contains_key(name) {
            out.extend(generate_drop_table_diffs(db_map[name]));
        }
    }
    // Modified tables — present in both, diff recursively.
    for name in sorted_keys(&code_map) {
        if let Some(db_table) = db_map.get(name) {
            let code_table = code_map[name];
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
    out
}

/// Compare two event slices for the named table.
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
    out
}

/// Compare two permission maps for the named table.
///
/// Emits at most one [`SchemaDiff`] describing the delta. If the maps are
/// equal, returns an empty vector.
#[must_use]
pub fn diff_permissions(
    table: &str,
    code: Option<&BTreeMap<String, String>>,
    db: Option<&BTreeMap<String, String>>,
) -> Vec<SchemaDiff> {
    if permissions_equal(code, db) {
        return Vec::new();
    }
    vec![generate_modify_permissions_diff(table, code, db)]
}

/// Compare two edge slices.
///
/// Same high-level behaviour as [`diff_tables`]: added edges produce add
/// diffs for the edge and all of its contained objects; dropped edges
/// produce drop diffs; edges present in both are recursively compared on
/// fields/indexes/events/permissions.
#[must_use]
pub fn diff_edges(code: &[EdgeDefinition], db: &[EdgeDefinition]) -> Vec<SchemaDiff> {
    let code_map = index_by_name(code, |e| e.name.as_str());
    let db_map = index_by_name(db, |e| e.name.as_str());
    let mut out: Vec<SchemaDiff> = Vec::new();

    for name in sorted_keys(&code_map) {
        if !db_map.contains_key(name) {
            out.extend(generate_add_edge_diffs(code_map[name]));
        }
    }
    for name in sorted_keys(&db_map) {
        if !code_map.contains_key(name) {
            out.extend(generate_drop_edge_diffs(db_map[name]));
        }
    }
    for name in sorted_keys(&code_map) {
        if let Some(db_edge) = db_map.get(name) {
            let code_edge = code_map[name];
            out.extend(diff_edge_pair_inner(code_edge, db_edge));
        }
    }
    out
}

/// Diff two complete snapshots and return every change required to make
/// `db` look like `code`.
///
/// The returned diffs are ordered: tables first, then edges.
#[must_use]
pub fn diff_schemas(code: &SchemaSnapshot, db: &SchemaSnapshot) -> Vec<SchemaDiff> {
    let mut out = diff_tables(&code.tables, &db.tables);
    out.extend(diff_edges(&code.edges, &db.edges));
    out
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
    out.extend(diff_permissions(
        &code.name,
        code.permissions.as_ref(),
        db.permissions.as_ref(),
    ));
    out
}

fn diff_edge_pair_inner(code: &EdgeDefinition, db: &EdgeDefinition) -> Vec<SchemaDiff> {
    let mut out = diff_fields(&code.name, &code.fields, &db.fields);
    out.extend(diff_indexes(&code.name, &code.indexes, &db.indexes));
    out.extend(diff_events(&code.name, &code.events, &db.events));
    out.extend(diff_permissions(
        &code.name,
        code.permissions.as_ref(),
        db.permissions.as_ref(),
    ));
    out
}

// ---------------------------------------------------------------------------
// Generator helpers (pure functions, rendered into SurrealQL text)
// ---------------------------------------------------------------------------

fn generate_add_table_diffs(table: &TableDefinition) -> Vec<SchemaDiff> {
    let forward_sql = format!("DEFINE TABLE {} {};", table.name, table.mode.as_str());
    let backward_sql = format!("REMOVE TABLE {};", table.name);
    let mut out = vec![SchemaDiff {
        operation: DiffOperation::AddTable,
        table: table.name.clone(),
        field: None,
        index: None,
        event: None,
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
    if let Some(perms) = table.permissions.as_ref() {
        if !perms.is_empty() {
            out.push(generate_modify_permissions_diff(
                &table.name,
                Some(perms),
                None,
            ));
        }
    }
    out
}

fn generate_drop_table_diffs(table: &TableDefinition) -> Vec<SchemaDiff> {
    vec![SchemaDiff {
        operation: DiffOperation::DropTable,
        table: table.name.clone(),
        field: None,
        index: None,
        event: None,
        description: format!("Drop table {}", table.name),
        forward_sql: format!("REMOVE TABLE {};", table.name),
        backward_sql: format!("DEFINE TABLE {} {};", table.name, table.mode.as_str()),
        details: BTreeMap::new(),
    }]
}

fn generate_add_field_diff(table: &str, field: &FieldDefinition) -> SchemaDiff {
    let mut forward_sql = field_to_sql(table, field);
    if let Some(default) = field.default.as_deref() {
        // Best-effort backfill: failures to validate default surface as a
        // skipped backfill rather than a panic (matches conservative Python
        // path — though Python raises, Rust returns a safe render because
        // this function is infallible by contract).
        if validate_default_value(default).is_ok() {
            let backfill = format!(
                "UPDATE {table} SET {name} = {default} WHERE {name} IS NONE;",
                name = field.name,
            );
            forward_sql.push('\n');
            forward_sql.push_str(&backfill);
        }
    }
    let backward_sql = format!("REMOVE FIELD {} ON TABLE {};", field.name, table);
    let mut details = BTreeMap::new();
    details.insert(
        "type".to_string(),
        serde_json::Value::String(field.field_type.as_str().into()),
    );
    SchemaDiff {
        operation: DiffOperation::AddField,
        table: table.to_string(),
        field: Some(field.name.clone()),
        index: None,
        event: None,
        description: format!("Add field {} to {}", field.name, table),
        forward_sql,
        backward_sql,
        details,
    }
}

fn generate_drop_field_diff(table: &str, field: &FieldDefinition) -> SchemaDiff {
    let forward_sql = format!("REMOVE FIELD {} ON TABLE {};", field.name, table);
    let backward_sql = field_to_sql(table, field);
    SchemaDiff {
        operation: DiffOperation::DropField,
        table: table.to_string(),
        field: Some(field.name.clone()),
        index: None,
        event: None,
        description: format!("Drop field {} from {}", field.name, table),
        forward_sql,
        backward_sql,
        details: BTreeMap::new(),
    }
}

fn generate_modify_field_diff(
    table: &str,
    old_field: &FieldDefinition,
    new_field: &FieldDefinition,
) -> SchemaDiff {
    let forward_sql = field_to_sql(table, new_field);
    let backward_sql = field_to_sql(table, old_field);
    let mut details = BTreeMap::new();
    details.insert(
        "old_type".into(),
        serde_json::Value::String(old_field.field_type.as_str().into()),
    );
    details.insert(
        "new_type".into(),
        serde_json::Value::String(new_field.field_type.as_str().into()),
    );
    SchemaDiff {
        operation: DiffOperation::ModifyField,
        table: table.to_string(),
        field: Some(new_field.name.clone()),
        index: None,
        event: None,
        description: format!("Modify field {} in {}", new_field.name, table),
        forward_sql,
        backward_sql,
        details,
    }
}

fn generate_add_index_diff(table: &str, idx: &IndexDefinition) -> SchemaDiff {
    let forward_sql = match idx.index_type {
        IndexType::Mtree => mtree_index_to_sql(table, idx),
        IndexType::Hnsw => hnsw_index_to_sql(table, idx),
        _ => {
            let columns = idx.columns.join(", ");
            let mut sql = format!(
                "DEFINE INDEX {name} ON TABLE {table} COLUMNS {columns}",
                name = idx.name
            );
            if idx.index_type.as_str() != "INDEX" {
                sql.push(' ');
                sql.push_str(idx.index_type.as_str());
            }
            sql.push(';');
            sql
        }
    };
    let backward_sql = format!("REMOVE INDEX {} ON TABLE {};", idx.name, table);
    SchemaDiff {
        operation: DiffOperation::AddIndex,
        table: table.to_string(),
        field: None,
        index: Some(idx.name.clone()),
        event: None,
        description: format!("Add index {} to {}", idx.name, table),
        forward_sql,
        backward_sql,
        details: BTreeMap::new(),
    }
}

fn generate_drop_index_diff(table: &str, idx: &IndexDefinition) -> SchemaDiff {
    let forward_sql = format!("REMOVE INDEX {} ON TABLE {};", idx.name, table);
    let backward_sql = match idx.index_type {
        IndexType::Mtree => mtree_index_to_sql(table, idx),
        IndexType::Hnsw => hnsw_index_to_sql(table, idx),
        _ => {
            let columns = idx.columns.join(", ");
            format!(
                "DEFINE INDEX {name} ON TABLE {table} COLUMNS {columns};",
                name = idx.name
            )
        }
    };
    SchemaDiff {
        operation: DiffOperation::DropIndex,
        table: table.to_string(),
        field: None,
        index: Some(idx.name.clone()),
        event: None,
        description: format!("Drop index {} from {}", idx.name, table),
        forward_sql,
        backward_sql,
        details: BTreeMap::new(),
    }
}

fn generate_add_event_diff(table: &str, ev: &EventDefinition) -> SchemaDiff {
    // Best-effort validation; on failure we still emit the diff so callers
    // can surface the unsafe SQL via dry-run, matching the "infallible diff
    // constructor" contract of this module.
    let _ = validate_event_expression(&ev.condition, "condition");
    let _ = validate_event_expression(&ev.action, "action");
    let forward_sql = format!(
        "DEFINE EVENT {name} ON TABLE {table} WHEN {cond} THEN {{ {act} }};",
        name = ev.name,
        cond = ev.condition,
        act = ev.action,
    );
    let backward_sql = format!("REMOVE EVENT {} ON TABLE {};", ev.name, table);
    SchemaDiff {
        operation: DiffOperation::AddEvent,
        table: table.to_string(),
        field: None,
        index: None,
        event: Some(ev.name.clone()),
        description: format!("Add event {} to {}", ev.name, table),
        forward_sql,
        backward_sql,
        details: BTreeMap::new(),
    }
}

fn generate_drop_event_diff(table: &str, ev: &EventDefinition) -> SchemaDiff {
    let _ = validate_event_expression(&ev.condition, "condition");
    let _ = validate_event_expression(&ev.action, "action");
    let forward_sql = format!("REMOVE EVENT {} ON TABLE {};", ev.name, table);
    let backward_sql = format!(
        "DEFINE EVENT {name} ON TABLE {table} WHEN {cond} THEN {{ {act} }};",
        name = ev.name,
        cond = ev.condition,
        act = ev.action,
    );
    SchemaDiff {
        operation: DiffOperation::DropEvent,
        table: table.to_string(),
        field: None,
        index: None,
        event: Some(ev.name.clone()),
        description: format!("Drop event {} from {}", ev.name, table),
        forward_sql,
        backward_sql,
        details: BTreeMap::new(),
    }
}

fn generate_modify_permissions_diff(
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
        description: format!("Modify permissions for {table}"),
        forward_sql,
        backward_sql,
        details: BTreeMap::new(),
    }
}

fn generate_add_edge_diffs(edge: &EdgeDefinition) -> Vec<SchemaDiff> {
    let mut forward_sql = match edge.mode {
        EdgeMode::Relation => {
            let mut s = format!("DEFINE TABLE {} TYPE RELATION", edge.name);
            if let Some(from) = edge.from_table.as_deref() {
                s.push_str(" FROM ");
                s.push_str(from);
            }
            if let Some(to) = edge.to_table.as_deref() {
                s.push_str(" TO ");
                s.push_str(to);
            }
            s
        }
        EdgeMode::Schemafull => format!("DEFINE TABLE {} SCHEMAFULL", edge.name),
        EdgeMode::Schemaless => format!("DEFINE TABLE {} SCHEMALESS", edge.name),
    };
    forward_sql.push(';');
    let backward_sql = format!("REMOVE TABLE {};", edge.name);

    let mut out = vec![SchemaDiff {
        operation: DiffOperation::AddTable,
        table: edge.name.clone(),
        field: None,
        index: None,
        event: None,
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
    if let Some(perms) = edge.permissions.as_ref() {
        if !perms.is_empty() {
            out.push(generate_modify_permissions_diff(
                &edge.name,
                Some(perms),
                None,
            ));
        }
    }
    out
}

fn generate_drop_edge_diffs(edge: &EdgeDefinition) -> Vec<SchemaDiff> {
    vec![SchemaDiff {
        operation: DiffOperation::DropTable,
        table: edge.name.clone(),
        field: None,
        index: None,
        event: None,
        description: format!("Drop edge {}", edge.name),
        forward_sql: format!("REMOVE TABLE {};", edge.name),
        backward_sql: String::new(),
        details: BTreeMap::new(),
    }]
}

fn render_permission_statements(table: &str, perms: Option<&BTreeMap<String, String>>) -> String {
    let Some(perms) = perms else {
        return String::new();
    };
    if perms.is_empty() {
        return String::new();
    }
    let mut parts: Vec<String> = Vec::with_capacity(perms.len());
    for (action, condition) in perms {
        parts.push(format!(
            "DEFINE FIELD PERMISSIONS FOR {} ON TABLE {table} WHERE {condition};",
            action.to_uppercase()
        ));
    }
    parts.join(" ")
}

fn field_to_sql(table: &str, field: &FieldDefinition) -> String {
    let mut sql = format!(
        "DEFINE FIELD {name} ON TABLE {table} TYPE {ty}",
        name = field.name,
        ty = field.field_type.as_str(),
    );
    if let Some(a) = field.assertion.as_deref() {
        sql.push_str(" ASSERT ");
        sql.push_str(a);
    }
    if let Some(d) = field.default.as_deref() {
        sql.push_str(" DEFAULT ");
        sql.push_str(d);
    }
    if let Some(v) = field.value.as_deref() {
        sql.push_str(" VALUE ");
        sql.push_str(v);
    }
    if field.readonly {
        sql.push_str(" READONLY");
    }
    if field.flexible {
        sql.push_str(" FLEXIBLE");
    }
    sql.push(';');
    sql
}

fn fields_equal(a: &FieldDefinition, b: &FieldDefinition) -> bool {
    a.name == b.name
        && a.field_type == b.field_type
        && a.readonly == b.readonly
        && a.flexible == b.flexible
        && expr_eq(a.assertion.as_deref(), b.assertion.as_deref())
        && expr_eq(a.default.as_deref(), b.default.as_deref())
        && expr_eq(a.value.as_deref(), b.value.as_deref())
}

fn permissions_equal(
    a: Option<&BTreeMap<String, String>>,
    b: Option<&BTreeMap<String, String>>,
) -> bool {
    match (a, b) {
        (None, None) => true,
        (Some(x), Some(y)) => {
            if x.len() != y.len() {
                return false;
            }
            for (k, vx) in x {
                let Some(vy) = y.get(k) else { return false };
                if normalize_expression(vx) != normalize_expression(vy) {
                    return false;
                }
            }
            true
        }
        (Some(m), None) | (None, Some(m)) => m.is_empty(),
    }
}

fn mtree_index_to_sql(table: &str, idx: &IndexDefinition) -> String {
    let field = idx.columns.first().map_or("", String::as_str);
    let dim = idx.dimension.unwrap_or(0);
    let distance = idx.distance.unwrap_or(MTreeDistanceType::Euclidean);
    let vtype = idx.vector_type.unwrap_or(MTreeVectorType::F64);
    let mut sql = format!(
        "DEFINE INDEX {name} ON TABLE {table} COLUMNS {field} MTREE DIMENSION {dim}",
        name = idx.name,
    );
    sql.push_str(" DIST ");
    sql.push_str(distance.as_str());
    sql.push_str(" TYPE ");
    sql.push_str(vtype.as_str());
    sql.push(';');
    sql
}

fn hnsw_index_to_sql(table: &str, idx: &IndexDefinition) -> String {
    let field = idx.columns.first().map_or("", String::as_str);
    let dim = idx.dimension.unwrap_or(0);
    let distance = idx.hnsw_distance.unwrap_or(HnswDistanceType::Euclidean);
    let vtype = idx.vector_type.unwrap_or(MTreeVectorType::F64);
    let mut sql = format!(
        "DEFINE INDEX {name} ON TABLE {table} COLUMNS {field} HNSW DIMENSION {dim}",
        name = idx.name,
    );
    sql.push_str(" DIST ");
    sql.push_str(distance.as_str());
    sql.push_str(" TYPE ");
    sql.push_str(vtype.as_str());
    if let Some(efc) = idx.efc {
        use std::fmt::Write as _;
        let _ = write!(sql, " EFC {efc}");
    }
    if let Some(m) = idx.m {
        use std::fmt::Write as _;
        let _ = write!(sql, " M {m}");
    }
    sql.push(';');
    sql
}

fn index_by_name<'a, T, F>(items: &'a [T], key: F) -> BTreeMap<&'a str, &'a T>
where
    F: Fn(&'a T) -> &'a str,
{
    let mut map = BTreeMap::new();
    for item in items {
        map.insert(key(item), item);
    }
    map
}

fn sorted_keys<'a, V>(map: &'a BTreeMap<&'a str, V>) -> Vec<&'a str> {
    // BTreeMap iterates in key order already, so we just need to collect
    // the keys into a concrete vector to avoid holding the borrow across
    // the map while iterating mutably elsewhere.
    let mut keys: Vec<&str> = map.keys().copied().collect();
    keys.sort_unstable();
    // dedupe is not needed — a BTreeMap cannot have duplicates — but keep
    // a stable Vec<&str> interface.
    let set: BTreeSet<&str> = keys.into_iter().collect();
    set.into_iter().collect()
}

// Silence clippy::missing_fields_in_debug warnings from older toolchains:
// all public types here derive Debug explicitly.

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::edge::{EdgeDefinition, EdgeMode};
    use crate::schema::fields::{FieldDefinition, FieldType};
    use crate::schema::table::{
        event, hnsw_index, index, mtree_index, table_schema, unique_index, HnswDistanceType,
        IndexDefinition, IndexType, MTreeDistanceType, MTreeVectorType, TableMode,
    };

    fn tbl(name: &str) -> TableDefinition {
        table_schema(name)
    }

    fn f(name: &str, ty: FieldType) -> FieldDefinition {
        FieldDefinition::new(name, ty)
    }

    // ----- normalize_expression -----

    #[test]
    fn normalize_expression_collapses_runs_of_whitespace() {
        assert_eq!(normalize_expression("a   b\tc\n d"), "a b c d");
    }

    #[test]
    fn normalize_expression_trims_edges() {
        assert_eq!(normalize_expression("  hello world  "), "hello world");
    }

    #[test]
    fn normalize_expression_empty_is_empty() {
        assert_eq!(normalize_expression("   "), "");
    }

    // ----- validate_event_expression -----

    #[test]
    fn validate_event_expression_allows_safe() {
        assert!(validate_event_expression("$event = \"CREATE\"", "condition").is_ok());
        assert!(validate_event_expression("$before.a != $after.a", "condition").is_ok());
        assert!(validate_event_expression("true", "condition").is_ok());
        assert!(validate_event_expression("CREATE log SET u = 1", "action").is_ok());
    }

    #[test]
    fn validate_event_expression_rejects_statement_separator() {
        assert!(validate_event_expression("a; DROP b", "condition").is_err());
    }

    #[test]
    fn validate_event_expression_rejects_trailing_semicolon() {
        assert!(validate_event_expression("a;", "condition").is_err());
    }

    #[test]
    fn validate_event_expression_rejects_comment() {
        assert!(validate_event_expression("a -- b", "condition").is_err());
    }

    #[test]
    fn validate_event_expression_rejects_semicolon_comment() {
        assert!(validate_event_expression("a;--b", "condition").is_err());
    }

    // ----- validate_default_value -----

    #[test]
    fn validate_default_value_accepts_literals() {
        assert!(validate_default_value("42").is_ok());
        assert!(validate_default_value("-1").is_ok());
        assert!(validate_default_value("3.14").is_ok());
        assert!(validate_default_value("true").is_ok());
        assert!(validate_default_value("false").is_ok());
        assert!(validate_default_value("NONE").is_ok());
        assert!(validate_default_value("NULL").is_ok());
        assert!(validate_default_value("'hello'").is_ok());
        assert!(validate_default_value("time::now()").is_ok());
        assert!(validate_default_value("$auth").is_ok());
    }

    #[test]
    fn validate_default_value_rejects_unsafe() {
        assert!(validate_default_value("a; DROP TABLE u").is_err());
        assert!(validate_default_value("SELECT * FROM u").is_err());
    }

    // ----- diff_tables: ADD -----

    #[test]
    fn diff_tables_adds_new_table() {
        let code = vec![tbl("user")];
        let db: Vec<TableDefinition> = vec![];
        let diffs = diff_tables(&code, &db);
        assert_eq!(diffs.len(), 1);
        assert_eq!(diffs[0].operation, DiffOperation::AddTable);
        assert_eq!(diffs[0].table, "user");
        assert!(diffs[0].forward_sql.starts_with("DEFINE TABLE user"));
        assert_eq!(diffs[0].backward_sql, "REMOVE TABLE user;");
    }

    #[test]
    fn diff_tables_adds_new_table_with_field_and_index() {
        let code_table = tbl("user")
            .with_fields([f("email", FieldType::String)])
            .with_indexes([unique_index("email_idx", ["email"])]);
        let diffs = diff_tables(&[code_table], &[]);
        // 1 table + 1 field + 1 index = 3 diffs.
        assert_eq!(diffs.len(), 3);
        assert_eq!(diffs[0].operation, DiffOperation::AddTable);
        assert!(diffs.iter().any(|d| d.operation == DiffOperation::AddField));
        assert!(diffs.iter().any(|d| d.operation == DiffOperation::AddIndex));
    }

    #[test]
    fn diff_tables_adds_table_with_event_and_perms() {
        let code_table = tbl("user")
            .with_events([event("on_upd", "true", "RETURN 1")])
            .with_permissions([("select", "true")]);
        let diffs = diff_tables(&[code_table], &[]);
        // table + event + perms
        assert_eq!(diffs.len(), 3);
        assert!(diffs.iter().any(|d| d.operation == DiffOperation::AddEvent));
        assert!(diffs
            .iter()
            .any(|d| d.operation == DiffOperation::ModifyPermissions));
    }

    // ----- diff_tables: DROP -----

    #[test]
    fn diff_tables_drops_missing_table() {
        let db = vec![tbl("old").with_mode(TableMode::Schemaless)];
        let diffs = diff_tables(&[], &db);
        assert_eq!(diffs.len(), 1);
        assert_eq!(diffs[0].operation, DiffOperation::DropTable);
        assert_eq!(diffs[0].forward_sql, "REMOVE TABLE old;");
        assert_eq!(diffs[0].backward_sql, "DEFINE TABLE old SCHEMALESS;");
    }

    // ----- diff_tables: MODIFY (no-op when identical) -----

    #[test]
    fn diff_tables_identical_produces_no_diff() {
        let a = tbl("user").with_fields([f("email", FieldType::String)]);
        let diffs = diff_tables(std::slice::from_ref(&a), std::slice::from_ref(&a));
        assert!(diffs.is_empty());
    }

    // ----- diff_fields: ADD / DROP / MODIFY -----

    #[test]
    fn diff_fields_detects_added() {
        let code = vec![f("email", FieldType::String)];
        let diffs = diff_fields("user", &code, &[]);
        assert_eq!(diffs.len(), 1);
        assert_eq!(diffs[0].operation, DiffOperation::AddField);
        assert_eq!(diffs[0].field.as_deref(), Some("email"));
        assert!(diffs[0].forward_sql.contains("DEFINE FIELD email"));
        assert!(diffs[0].backward_sql.contains("REMOVE FIELD email"));
    }

    #[test]
    fn diff_fields_detects_dropped() {
        let db = vec![f("old", FieldType::String)];
        let diffs = diff_fields("user", &[], &db);
        assert_eq!(diffs.len(), 1);
        assert_eq!(diffs[0].operation, DiffOperation::DropField);
        assert!(diffs[0].forward_sql.contains("REMOVE FIELD old"));
        assert!(diffs[0].backward_sql.contains("DEFINE FIELD old"));
    }

    #[test]
    fn diff_fields_detects_modified_type() {
        let code = vec![f("age", FieldType::Int)];
        let db = vec![f("age", FieldType::String)];
        let diffs = diff_fields("user", &code, &db);
        assert_eq!(diffs.len(), 1);
        assert_eq!(diffs[0].operation, DiffOperation::ModifyField);
        assert_eq!(
            diffs[0].details.get("old_type"),
            Some(&serde_json::json!("string"))
        );
        assert_eq!(
            diffs[0].details.get("new_type"),
            Some(&serde_json::json!("int"))
        );
    }

    #[test]
    fn diff_fields_identical_yields_nothing() {
        let a = vec![f("x", FieldType::Int)];
        assert!(diff_fields("t", &a, &a).is_empty());
    }

    #[test]
    fn diff_fields_whitespace_different_assertion_is_not_a_diff() {
        let code = vec![f("x", FieldType::Int).with_assertion("$value  > 0")];
        let db = vec![f("x", FieldType::Int).with_assertion("$value > 0")];
        assert!(diff_fields("t", &code, &db).is_empty());
    }

    #[test]
    fn diff_fields_modify_detects_assertion_semantic_change() {
        let code = vec![f("x", FieldType::Int).with_assertion("$value > 0")];
        let db = vec![f("x", FieldType::Int).with_assertion("$value >= 0")];
        let diffs = diff_fields("t", &code, &db);
        assert_eq!(diffs.len(), 1);
        assert_eq!(diffs[0].operation, DiffOperation::ModifyField);
    }

    #[test]
    fn diff_fields_add_with_default_emits_backfill() {
        let code = vec![f("age", FieldType::Int).with_default("0")];
        let diffs = diff_fields("user", &code, &[]);
        assert!(diffs[0].forward_sql.contains("DEFAULT 0"));
        assert!(diffs[0]
            .forward_sql
            .contains("UPDATE user SET age = 0 WHERE age IS NONE;"));
    }

    #[test]
    fn diff_fields_add_with_unsafe_default_skips_backfill() {
        let code = vec![f("age", FieldType::Int).with_default("DROP TABLE x")];
        let diffs = diff_fields("user", &code, &[]);
        assert!(!diffs[0].forward_sql.contains("UPDATE"));
    }

    #[test]
    fn diff_fields_readonly_toggle_is_a_modify() {
        let code = vec![f("x", FieldType::Int).readonly(true)];
        let db = vec![f("x", FieldType::Int)];
        let diffs = diff_fields("t", &code, &db);
        assert_eq!(diffs.len(), 1);
        assert_eq!(diffs[0].operation, DiffOperation::ModifyField);
    }

    // ----- diff_indexes -----

    #[test]
    fn diff_indexes_detects_added_standard() {
        let code = vec![index("title_idx", ["title"])];
        let diffs = diff_indexes("post", &code, &[]);
        assert_eq!(diffs.len(), 1);
        assert_eq!(diffs[0].operation, DiffOperation::AddIndex);
        assert_eq!(
            diffs[0].forward_sql,
            "DEFINE INDEX title_idx ON TABLE post COLUMNS title;"
        );
    }

    #[test]
    fn diff_indexes_detects_added_unique() {
        let code = vec![unique_index("email_idx", ["email"])];
        let diffs = diff_indexes("user", &code, &[]);
        assert!(diffs[0].forward_sql.contains("UNIQUE"));
    }

    #[test]
    fn diff_indexes_detects_dropped() {
        let db = vec![index("old_idx", ["x"])];
        let diffs = diff_indexes("t", &[], &db);
        assert_eq!(diffs.len(), 1);
        assert_eq!(diffs[0].operation, DiffOperation::DropIndex);
        assert!(diffs[0].forward_sql.contains("REMOVE INDEX old_idx"));
        assert!(diffs[0].backward_sql.contains("DEFINE INDEX old_idx"));
    }

    #[test]
    fn diff_indexes_identical_yields_nothing() {
        let a = vec![index("x", ["a"])];
        assert!(diff_indexes("t", &a, &a).is_empty());
    }

    #[test]
    fn diff_indexes_added_mtree() {
        let idx = mtree_index(
            "e_idx",
            "embedding",
            1536,
            MTreeDistanceType::Cosine,
            MTreeVectorType::F32,
        );
        let diffs = diff_indexes("doc", &[idx], &[]);
        assert_eq!(diffs.len(), 1);
        assert!(diffs[0].forward_sql.contains("MTREE DIMENSION 1536"));
        assert!(diffs[0].forward_sql.contains("DIST COSINE"));
        assert!(diffs[0].forward_sql.contains("TYPE F32"));
    }

    #[test]
    fn diff_indexes_dropped_mtree_recreates_in_backward() {
        let idx = mtree_index(
            "e_idx",
            "embedding",
            8,
            MTreeDistanceType::Euclidean,
            MTreeVectorType::F64,
        );
        let diffs = diff_indexes("doc", &[], &[idx]);
        assert!(diffs[0].forward_sql.starts_with("REMOVE INDEX e_idx"));
        assert!(diffs[0].backward_sql.contains("MTREE DIMENSION 8"));
    }

    #[test]
    fn diff_indexes_added_hnsw() {
        let idx = hnsw_index(
            "h_idx",
            "v",
            64,
            HnswDistanceType::Cosine,
            MTreeVectorType::F32,
            Some(200),
            Some(16),
        );
        let diffs = diff_indexes("doc", &[idx], &[]);
        let sql = &diffs[0].forward_sql;
        assert!(sql.contains("HNSW DIMENSION 64"));
        assert!(sql.contains("DIST COSINE"));
        assert!(sql.contains("EFC 200"));
        assert!(sql.contains("M 16"));
    }

    #[test]
    fn diff_indexes_added_hnsw_without_tuning() {
        let idx = hnsw_index(
            "h_idx",
            "v",
            64,
            HnswDistanceType::Euclidean,
            MTreeVectorType::F64,
            None,
            None,
        );
        let diffs = diff_indexes("doc", &[idx], &[]);
        let sql = &diffs[0].forward_sql;
        assert!(!sql.contains("EFC"));
    }

    #[test]
    fn diff_indexes_search_index_emits_search_keyword() {
        let idx = IndexDefinition::new("s_idx", ["body"]).with_type(IndexType::Search);
        let diffs = diff_indexes("post", &[idx], &[]);
        assert!(diffs[0].forward_sql.contains("SEARCH"));
    }

    // ----- diff_events -----

    #[test]
    fn diff_events_detects_added() {
        let ev = event("on_upd", "true", "RETURN 1");
        let diffs = diff_events("t", &[ev], &[]);
        assert_eq!(diffs.len(), 1);
        assert_eq!(diffs[0].operation, DiffOperation::AddEvent);
        assert_eq!(diffs[0].event.as_deref(), Some("on_upd"));
        assert!(diffs[0].forward_sql.contains("DEFINE EVENT on_upd"));
    }

    #[test]
    fn diff_events_detects_dropped() {
        let ev = event("on_upd", "true", "RETURN 1");
        let diffs = diff_events("t", &[], &[ev]);
        assert_eq!(diffs.len(), 1);
        assert_eq!(diffs[0].operation, DiffOperation::DropEvent);
        assert!(diffs[0].forward_sql.starts_with("REMOVE EVENT on_upd"));
    }

    #[test]
    fn diff_events_identical_yields_nothing() {
        let ev = event("on_upd", "true", "RETURN 1");
        let a = vec![ev];
        assert!(diff_events("t", &a, &a).is_empty());
    }

    // ----- diff_permissions -----

    #[test]
    fn diff_permissions_added() {
        let mut new_perms = BTreeMap::new();
        new_perms.insert("select".into(), "true".into());
        let diffs = diff_permissions("t", Some(&new_perms), None);
        assert_eq!(diffs.len(), 1);
        assert_eq!(diffs[0].operation, DiffOperation::ModifyPermissions);
        assert!(diffs[0].forward_sql.contains("FOR SELECT"));
        assert_eq!(diffs[0].backward_sql, "");
    }

    #[test]
    fn diff_permissions_removed_roundtrip() {
        let mut old_perms = BTreeMap::new();
        old_perms.insert("select".into(), "$auth.id = id".into());
        let diffs = diff_permissions("t", None, Some(&old_perms));
        assert_eq!(diffs.len(), 1);
        assert_eq!(diffs[0].forward_sql, "");
        assert!(diffs[0].backward_sql.contains("$auth.id = id"));
    }

    #[test]
    fn diff_permissions_modified_carries_old_in_backward() {
        let mut old_perms = BTreeMap::new();
        old_perms.insert("select".into(), "$auth.id = id".into());
        let mut new_perms = BTreeMap::new();
        new_perms.insert("select".into(), "true".into());

        let diffs = diff_permissions("t", Some(&new_perms), Some(&old_perms));
        assert_eq!(diffs.len(), 1);
        assert!(diffs[0].forward_sql.contains("true"));
        assert!(diffs[0].backward_sql.contains("$auth.id = id"));
    }

    #[test]
    fn diff_permissions_identical_yields_nothing() {
        let mut p = BTreeMap::new();
        p.insert("select".into(), "true".into());
        assert!(diff_permissions("t", Some(&p), Some(&p)).is_empty());
    }

    #[test]
    fn diff_permissions_whitespace_variance_is_equal() {
        let mut code = BTreeMap::new();
        code.insert("select".into(), "$auth.id  =  id".into());
        let mut db = BTreeMap::new();
        db.insert("select".into(), "$auth.id = id".into());
        assert!(diff_permissions("t", Some(&code), Some(&db)).is_empty());
    }

    #[test]
    fn diff_permissions_none_and_empty_are_equal() {
        let empty: BTreeMap<String, String> = BTreeMap::new();
        assert!(diff_permissions("t", Some(&empty), None).is_empty());
        assert!(diff_permissions("t", None, Some(&empty)).is_empty());
    }

    // ----- diff_edges: ADD / DROP / MODIFY -----

    fn relation_edge(name: &str) -> EdgeDefinition {
        EdgeDefinition::new(name)
            .with_mode(EdgeMode::Relation)
            .with_from_table("user")
            .with_to_table("post")
    }

    #[test]
    fn diff_edges_detects_added_relation() {
        let code = vec![relation_edge("likes")];
        let diffs = diff_edges(&code, &[]);
        assert!(!diffs.is_empty());
        assert_eq!(diffs[0].operation, DiffOperation::AddTable);
        assert!(diffs[0].forward_sql.contains("TYPE RELATION"));
        assert!(diffs[0].forward_sql.contains("FROM user"));
        assert!(diffs[0].forward_sql.contains("TO post"));
    }

    #[test]
    fn diff_edges_detects_added_schemafull() {
        let code = vec![EdgeDefinition::new("rel").with_mode(EdgeMode::Schemafull)];
        let diffs = diff_edges(&code, &[]);
        assert_eq!(diffs.len(), 1);
        assert!(diffs[0].forward_sql.contains("SCHEMAFULL"));
    }

    #[test]
    fn diff_edges_detects_dropped() {
        let db = vec![relation_edge("likes")];
        let diffs = diff_edges(&[], &db);
        assert_eq!(diffs.len(), 1);
        assert_eq!(diffs[0].operation, DiffOperation::DropTable);
        assert!(diffs[0].forward_sql.starts_with("REMOVE TABLE likes"));
    }

    #[test]
    fn diff_edges_field_added() {
        let old = relation_edge("likes");
        let new = relation_edge("likes").with_fields([f("weight", FieldType::Int)]);
        let diffs = diff_edges(&[new], &[old]);
        assert_eq!(diffs.len(), 1);
        assert_eq!(diffs[0].operation, DiffOperation::AddField);
        assert_eq!(diffs[0].field.as_deref(), Some("weight"));
    }

    #[test]
    fn diff_edges_field_removed() {
        let old = relation_edge("likes").with_fields([f("weight", FieldType::Int)]);
        let new = relation_edge("likes");
        let diffs = diff_edges(&[new], &[old]);
        assert_eq!(diffs.len(), 1);
        assert_eq!(diffs[0].operation, DiffOperation::DropField);
    }

    #[test]
    fn diff_edges_field_modified() {
        let old = relation_edge("likes").with_fields([f("weight", FieldType::Int)]);
        let new = relation_edge("likes").with_fields([f("weight", FieldType::Float)]);
        let diffs = diff_edges(&[new], &[old]);
        assert_eq!(diffs.len(), 1);
        assert_eq!(diffs[0].operation, DiffOperation::ModifyField);
    }

    #[test]
    fn diff_edges_index_and_event_and_perms() {
        let old = relation_edge("likes");
        let new = relation_edge("likes")
            .with_indexes([index("w_idx", ["weight"])])
            .with_events([event("on_like", "true", "RETURN 1")])
            .with_permissions([("select", "true")]);
        let diffs = diff_edges(&[new], &[old]);
        let ops: BTreeSet<DiffOperation> = diffs.iter().map(|d| d.operation).collect();
        assert!(ops.contains(&DiffOperation::AddIndex));
        assert!(ops.contains(&DiffOperation::AddEvent));
        assert!(ops.contains(&DiffOperation::ModifyPermissions));
    }

    #[test]
    fn diff_edges_identical_yields_nothing() {
        let e = relation_edge("likes").with_fields([f("weight", FieldType::Int)]);
        assert!(diff_edges(std::slice::from_ref(&e), std::slice::from_ref(&e)).is_empty());
    }

    // ----- diff_schemas aggregator -----

    #[test]
    fn diff_schemas_empty_snapshots_are_equal() {
        let a = SchemaSnapshot::default();
        let b = SchemaSnapshot::default();
        assert!(diff_schemas(&a, &b).is_empty());
    }

    #[test]
    fn diff_schemas_add_tables_and_edges() {
        let code = SchemaSnapshot::from_parts([tbl("user")], [relation_edge("likes")]);
        let db = SchemaSnapshot::default();
        let diffs = diff_schemas(&code, &db);
        let ops: Vec<DiffOperation> = diffs.iter().map(|d| d.operation).collect();
        // At least one AddTable for the user table and one AddTable for the edge.
        assert!(
            ops.iter()
                .filter(|o| **o == DiffOperation::AddTable)
                .count()
                >= 2
        );
    }

    #[test]
    fn diff_schemas_drops_removed_items() {
        let code = SchemaSnapshot::default();
        let db = SchemaSnapshot::from_parts([tbl("old")], [relation_edge("old_rel")]);
        let diffs = diff_schemas(&code, &db);
        let drops = diffs
            .iter()
            .filter(|d| d.operation == DiffOperation::DropTable)
            .count();
        assert_eq!(drops, 2);
    }

    #[test]
    fn diff_schemas_handles_mixed_add_drop_modify() {
        let shared = tbl("user").with_fields([f("email", FieldType::String)]);
        let shared_modified = tbl("user").with_fields([f("email", FieldType::Int)]);
        let code = SchemaSnapshot::from_parts([tbl("new"), shared_modified], []);
        let db = SchemaSnapshot::from_parts([shared, tbl("obsolete")], []);
        let diffs = diff_schemas(&code, &db);
        let ops: BTreeSet<DiffOperation> = diffs.iter().map(|d| d.operation).collect();
        assert!(ops.contains(&DiffOperation::AddTable));
        assert!(ops.contains(&DiffOperation::DropTable));
        assert!(ops.contains(&DiffOperation::ModifyField));
    }

    // ----- pair-wise helpers -----

    #[test]
    fn diff_table_pair_add_is_same_as_slice_form() {
        let t = tbl("user");
        let pair = diff_table_pair(Some(&t), None);
        let slice = diff_tables(std::slice::from_ref(&t), &[]);
        assert_eq!(pair, slice);
    }

    #[test]
    fn diff_table_pair_drop_is_same_as_slice_form() {
        let t = tbl("user");
        let pair = diff_table_pair(None, Some(&t));
        let slice = diff_tables(&[], std::slice::from_ref(&t));
        assert_eq!(pair, slice);
    }

    #[test]
    fn diff_table_pair_none_none_is_empty() {
        assert!(diff_table_pair(None, None).is_empty());
    }

    #[test]
    fn diff_edge_pair_none_none_is_empty() {
        assert!(diff_edge_pair(None, None).is_empty());
    }

    #[test]
    fn diff_edge_pair_add_matches_slice_form() {
        let e = relation_edge("likes");
        let pair = diff_edge_pair(Some(&e), None);
        let slice = diff_edges(std::slice::from_ref(&e), &[]);
        assert_eq!(pair, slice);
    }

    // ----- round-trip & details shape -----

    #[test]
    fn modify_field_details_contains_both_types() {
        let code = vec![f("n", FieldType::Int)];
        let db = vec![f("n", FieldType::Float)];
        let diffs = diff_fields("t", &code, &db);
        assert_eq!(diffs.len(), 1);
        let d = &diffs[0];
        assert_eq!(d.details.get("old_type"), Some(&serde_json::json!("float")));
        assert_eq!(d.details.get("new_type"), Some(&serde_json::json!("int")));
    }

    #[test]
    fn add_field_details_contains_type() {
        let code = vec![f("age", FieldType::Int)];
        let diffs = diff_fields("u", &code, &[]);
        assert_eq!(
            diffs[0].details.get("type"),
            Some(&serde_json::json!("int"))
        );
    }

    #[test]
    fn diff_permissions_multiple_entries_render_space_separated() {
        let mut code = BTreeMap::new();
        code.insert("select".into(), "true".into());
        code.insert("create".into(), "true".into());
        let diffs = diff_permissions("t", Some(&code), None);
        let fwd = &diffs[0].forward_sql;
        // Two separate DEFINE FIELD PERMISSIONS statements.
        assert_eq!(fwd.matches("DEFINE FIELD PERMISSIONS").count(), 2);
    }

    #[test]
    fn event_action_is_wrapped_in_braces() {
        let ev = event("e", "true", "RETURN 1");
        let diffs = diff_events("t", &[ev], &[]);
        assert!(diffs[0].forward_sql.contains("THEN { RETURN 1 }"));
    }

    #[test]
    fn modify_field_preserves_name_as_context() {
        let code = vec![f("email", FieldType::String)];
        let db = vec![f("email", FieldType::Int)];
        let diffs = diff_fields("user", &code, &db);
        assert_eq!(diffs[0].table, "user");
        assert_eq!(diffs[0].field.as_deref(), Some("email"));
    }

    // ----- snapshot round-trip -----

    #[test]
    fn snapshot_serde_roundtrip() {
        let snap = SchemaSnapshot::from_parts([tbl("user")], [relation_edge("likes")]);
        let j = serde_json::to_string(&snap).unwrap();
        let back: SchemaSnapshot = serde_json::from_str(&j).unwrap();
        assert_eq!(snap, back);
    }

    #[test]
    fn snapshot_default_is_empty() {
        let s = SchemaSnapshot::default();
        assert!(s.tables.is_empty());
        assert!(s.edges.is_empty());
    }

    #[test]
    fn snapshot_new_matches_default() {
        assert_eq!(SchemaSnapshot::new(), SchemaSnapshot::default());
    }

    // ----- sorted_keys / index_by_name are tested indirectly via diff_* -----

    #[test]
    fn diff_tables_sort_stable_across_multiple_adds_drops() {
        let code = vec![tbl("a"), tbl("c")];
        let db = vec![tbl("b"), tbl("d")];
        let diffs = diff_tables(&code, &db);
        let adds: Vec<&str> = diffs
            .iter()
            .filter(|d| d.operation == DiffOperation::AddTable)
            .map(|d| d.table.as_str())
            .collect();
        let drops: Vec<&str> = diffs
            .iter()
            .filter(|d| d.operation == DiffOperation::DropTable)
            .map(|d| d.table.as_str())
            .collect();
        assert_eq!(adds, vec!["a", "c"]);
        assert_eq!(drops, vec!["b", "d"]);
    }

    #[test]
    fn field_expr_comparison_treats_value_whitespace() {
        let a = vec![f("x", FieldType::String).with_value("a  +  b")];
        let b = vec![f("x", FieldType::String).with_value("a + b")];
        assert!(diff_fields("t", &a, &b).is_empty());
    }

    #[test]
    fn field_expr_comparison_treats_default_whitespace() {
        let a = vec![f("x", FieldType::Int).with_default("42  ")];
        let b = vec![f("x", FieldType::Int).with_default("42")];
        assert!(diff_fields("t", &a, &b).is_empty());
    }
}
