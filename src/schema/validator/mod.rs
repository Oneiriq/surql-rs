//! Schema validation: compare code-defined schemas against database-observed
//! schemas.
//!
//! Port of `surql/schema/validator.py`. This module performs cross-schema
//! validation and produces a list of [`ValidationResult`] entries describing
//! the differences between the two sides. Each result carries a
//! [`ValidationSeverity`] (`ERROR` / `WARNING` / `INFO`), the table (and
//! optionally field) it applies to, a human-readable message, and the
//! conflicting values on each side.
//!
//! Every attribute a definition can render is compared: table mode, `DROP`,
//! view body, change feed and permissions; field type, nullability, record
//! target, `REFERENCE`, `COMPUTED`, `ASSERT` / `DEFAULT` / `VALUE`,
//! `READONLY` / `FLEXIBLE` and permissions; index kind, columns (in order)
//! and the per-kind options; event `WHEN` / `THEN` bodies; and for edges the
//! mode, `FROM` / `TO` endpoints, permissions, fields, indexes and events.
//! Differences the engine introduces on its own are folded away before
//! comparing: expression reformatting (see [`normalize_expression`]),
//! implicit defaults (field permissions `FULL`, table permissions `NONE`,
//! HNSW `EFC 150 M 12`, the DISKANN tuning defaults, the `ascii` full-text
//! analyzer) and the `<array>.*` child fields the engine defines beside a
//! typed array.
//!
//! Unlike the Python source, async database fetching lives outside of the
//! pure-schema layer: [`validate_schema`] takes both code and database
//! table/edge maps as arguments. Callers are expected to produce the `db_*`
//! maps by querying `INFO FOR DB` and then `INFO FOR TABLE` for every table
//! (`INFO FOR DB` alone carries no fields, indexes or events), parsing the
//! results with [`parse_table_full`](crate::schema::parser::parse_table_full)
//! and [`parse_edge_info`](crate::schema::parser::parse_edge_info).
//!
//! ## Examples
//!
//! ```
//! use std::collections::HashMap;
//!
//! use surql::schema::validator::{validate_schema, ValidationSeverity};
//! use surql::schema::{table_schema, TableDefinition, TableMode};
//!
//! let mut code = HashMap::new();
//! code.insert("user".to_string(), table_schema("user"));
//!
//! let db: HashMap<String, TableDefinition> = HashMap::new();
//!
//! let results = validate_schema(&code, &db, None, None);
//! assert_eq!(results.len(), 1);
//! assert_eq!(results[0].severity, ValidationSeverity::Error);
//! ```

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::hash::BuildHasher;

use serde::{Deserialize, Serialize};

use super::edge::{EdgeDefinition, EdgeMode};
use super::table::{TableDefinition, TableMode};
use super::view::ViewDefinition;

mod events;
mod fields;
mod indexes;
mod normalize;
mod permissions;

pub use fields::validate_field;
pub use indexes::validate_index;
pub use normalize::normalize_expression;

use events::compare_events;
use fields::compare_fields;
use indexes::compare_indexes;
use normalize::expr_eq;
use permissions::{compare_permissions, TABLE_PERMISSIONS};

/// Severity classification for a [`ValidationResult`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ValidationSeverity {
    /// Schema drift requiring migration.
    Error,
    /// Non-critical difference worth surfacing.
    Warning,
    /// Informational message only.
    Info,
}

impl ValidationSeverity {
    /// Render the severity as a lowercase keyword (`error` / `warning` / `info`).
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Error => "error",
            Self::Warning => "warning",
            Self::Info => "info",
        }
    }

    /// Render the severity as its uppercase tag (`ERROR` / `WARNING` / `INFO`).
    pub fn as_upper_str(self) -> &'static str {
        match self {
            Self::Error => "ERROR",
            Self::Warning => "WARNING",
            Self::Info => "INFO",
        }
    }
}

impl std::fmt::Display for ValidationSeverity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Single schema validation finding.
///
/// Mirrors the Python `ValidationResult` dataclass. Fields are public and the
/// struct is immutable by convention (the Python port marks it `frozen=True`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ValidationResult {
    /// Severity classification.
    pub severity: ValidationSeverity,
    /// Name of the affected table.
    pub table: String,
    /// Optional field (or pseudo-field like `index:foo` / `event:foo`).
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub field: Option<String>,
    /// Human-readable description of the mismatch.
    pub message: String,
    /// Value on the code side, if relevant.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub code_value: Option<String>,
    /// Value on the database side, if relevant.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub db_value: Option<String>,
}

impl ValidationResult {
    /// Construct a new [`ValidationResult`].
    pub fn new(
        severity: ValidationSeverity,
        table: impl Into<String>,
        field: Option<String>,
        message: impl Into<String>,
        code_value: Option<String>,
        db_value: Option<String>,
    ) -> Self {
        Self {
            severity,
            table: table.into(),
            field,
            message: message.into(),
            code_value,
            db_value,
        }
    }
}

impl std::fmt::Display for ValidationResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[{}] {}", self.severity.as_upper_str(), self.table)?;
        if let Some(field) = &self.field {
            write!(f, ".{field}")?;
        }
        write!(f, ": {}", self.message)?;
        if self.code_value.is_some() || self.db_value.is_some() {
            let code = self.code_value.as_deref().unwrap_or("None");
            let db = self.db_value.as_deref().unwrap_or("None");
            write!(f, " (code: {code}, db: {db})")?;
        }
        Ok(())
    }
}

/// Shorthand for the "defined here, missing there" pair every presence
/// check reports.
fn presence(
    severity: ValidationSeverity,
    table: &str,
    field: Option<String>,
    message: &str,
    in_code: bool,
) -> ValidationResult {
    let (code, db) = if in_code {
        ("exists", "missing")
    } else {
        ("missing", "exists")
    };
    ValidationResult::new(
        severity,
        table,
        field,
        message,
        Some(code.into()),
        Some(db.into()),
    )
}

/// Borrow a name-keyed map in name order, whatever its hasher.
fn by_name<T, S: BuildHasher>(map: &HashMap<String, T, S>) -> BTreeMap<&str, &T> {
    map.iter().map(|(k, v)| (k.as_str(), v)).collect()
}

// -----------------------------------------------------------------------------
// Main validation entry point
// -----------------------------------------------------------------------------

/// Compare a set of code-defined schemas against a set of database-observed
/// schemas.
///
/// Callers are expected to have fetched the `db_tables` / `db_edges` maps up
/// front — this function is pure and synchronous. Each map must hold the
/// complete definitions (fields, indexes, events) that `INFO FOR TABLE`
/// reports; the fieldless tables `INFO FOR DB` alone yields would make every
/// code-side field look missing.
///
/// Edges are validated only when both `code_edges` and `db_edges` are
/// `Some`: `None` means "not supplied", never "the database has no edges".
/// Tables and edges share one namespace in SurrealDB, so a name defined as a
/// table on one side and as an edge on the other is reported once, under the
/// side that owns it, rather than as a missing table plus an extra edge. A
/// code edge the database holds as a plain table (as `INFO FOR DB` reports a
/// non-`RELATION` edge) is compared as an edge. The edge maps take the same
/// hasher as the table maps on their side, which keeps a bare `None`
/// inferable.
///
/// Returns the aggregated list of [`ValidationResult`] entries in a
/// deterministic (name-sorted) order. An empty vector means no difference
/// was found in any compared attribute.
pub fn validate_schema<S1: BuildHasher, S2: BuildHasher>(
    code_tables: &HashMap<String, TableDefinition, S1>,
    db_tables: &HashMap<String, TableDefinition, S2>,
    code_edges: Option<&HashMap<String, EdgeDefinition, S1>>,
    db_edges: Option<&HashMap<String, EdgeDefinition, S2>>,
) -> Vec<ValidationResult> {
    let code_tables = by_name(code_tables);
    let db_tables = by_name(db_tables);
    let code_edges = code_edges.map(by_name);
    let db_edges = db_edges.map(by_name);

    let code_edge_names: BTreeSet<&str> = code_edges
        .as_ref()
        .map(|m| m.keys().copied().collect())
        .unwrap_or_default();
    let db_edge_names: BTreeSet<&str> = db_edges
        .as_ref()
        .map(|m| m.keys().copied().collect())
        .unwrap_or_default();

    let mut results = compare_tables(&code_tables, &db_tables, &code_edge_names, &db_edge_names);
    if let (Some(code_edges), Some(db_edges)) = (code_edges, db_edges) {
        let code_table_names: BTreeSet<&str> = code_tables.keys().copied().collect();
        results.extend(compare_edges(
            &code_edges,
            &db_edges,
            &code_table_names,
            &db_tables,
        ));
    }
    results
}

// -----------------------------------------------------------------------------
// Table validation
// -----------------------------------------------------------------------------

/// Validate every table across the two maps (missing, extra, and matching).
pub fn validate_tables<S1: BuildHasher, S2: BuildHasher>(
    code_tables: &HashMap<String, TableDefinition, S1>,
    db_tables: &HashMap<String, TableDefinition, S2>,
) -> Vec<ValidationResult> {
    compare_tables(
        &by_name(code_tables),
        &by_name(db_tables),
        &BTreeSet::new(),
        &BTreeSet::new(),
    )
}

fn compare_tables(
    code: &BTreeMap<&str, &TableDefinition>,
    db: &BTreeMap<&str, &TableDefinition>,
    code_edge_names: &BTreeSet<&str>,
    db_edge_names: &BTreeSet<&str>,
) -> Vec<ValidationResult> {
    let mut results = Vec::new();

    for name in code.keys().filter(|n| !db.contains_key(*n)) {
        if db_edge_names.contains(name) {
            results.push(ValidationResult::new(
                ValidationSeverity::Error,
                *name,
                None,
                "Table defined in code but the database defines it as an edge (TYPE RELATION)",
                Some("table".into()),
                Some("edge".into()),
            ));
        } else {
            results.push(presence(
                ValidationSeverity::Error,
                name,
                None,
                "Table defined in code but missing from database",
                true,
            ));
        }
    }

    // A database table named like a code edge is compared as that edge.
    for name in db
        .keys()
        .filter(|n| !code.contains_key(*n) && !code_edge_names.contains(*n))
    {
        results.push(presence(
            ValidationSeverity::Warning,
            name,
            None,
            "Table exists in database but not defined in code",
            false,
        ));
    }

    for (name, code_table) in code {
        if let Some(db_table) = db.get(name) {
            results.extend(validate_table(code_table, db_table));
        }
    }

    results
}

/// Validate a single table: mode, `DROP` flag, view body, change feed,
/// permissions, fields, indexes, and events.
pub fn validate_table(
    code_table: &TableDefinition,
    db_table: &TableDefinition,
) -> Vec<ValidationResult> {
    let table = code_table.name.as_str();
    let mut results = Vec::new();

    if code_table.mode != db_table.mode {
        results.push(ValidationResult::new(
            ValidationSeverity::Error,
            table,
            None,
            "Table mode mismatch",
            Some(code_table.mode.as_str().to_string()),
            Some(db_table.mode.as_str().to_string()),
        ));
    }

    if code_table.drop != db_table.drop {
        results.push(ValidationResult::new(
            ValidationSeverity::Error,
            table,
            None,
            "Table DROP flag mismatch",
            Some(code_table.drop.to_string()),
            Some(db_table.drop.to_string()),
        ));
    }

    let code_view = code_table.view.as_ref().map(ViewDefinition::to_clause);
    let db_view = db_table.view.as_ref().map(ViewDefinition::to_clause);
    if !expr_eq(code_view.as_deref(), db_view.as_deref()) {
        results.push(ValidationResult::new(
            ValidationSeverity::Error,
            table,
            None,
            "Table view (AS SELECT) mismatch",
            code_view.map(|v| v.trim().to_string()),
            db_view.map(|v| v.trim().to_string()),
        ));
    }

    if code_table.changefeed != db_table.changefeed {
        let clause = |t: &TableDefinition| {
            t.changefeed
                .as_ref()
                .map(|cf| cf.to_clause().trim().to_string())
        };
        results.push(ValidationResult::new(
            ValidationSeverity::Warning,
            table,
            None,
            "Table change feed mismatch",
            clause(code_table),
            clause(db_table),
        ));
    }

    results.extend(compare_permissions(
        table,
        None,
        "Table permissions mismatch",
        code_table.permissions.as_ref(),
        db_table.permissions.as_ref(),
        &TABLE_PERMISSIONS,
    ));
    results.extend(compare_fields(table, &code_table.fields, &db_table.fields));
    results.extend(compare_indexes(
        table,
        &code_table.indexes,
        &db_table.indexes,
    ));
    results.extend(compare_events(table, &code_table.events, &db_table.events));
    results
}

// -----------------------------------------------------------------------------
// Edge validation
// -----------------------------------------------------------------------------

/// Validate every edge definition against its database counterpart.
///
/// `db_edges` holds the edges as the parser produces them
/// ([`parse_edge_info`](crate::schema::parser::parse_edge_info)). Edges on
/// either side only are reported (missing as `ERROR`, extra as `WARNING`),
/// edges on both sides are compared with [`validate_edge`].
pub fn validate_edges<S1: BuildHasher, S2: BuildHasher>(
    code_edges: &HashMap<String, EdgeDefinition, S1>,
    db_edges: &HashMap<String, EdgeDefinition, S2>,
) -> Vec<ValidationResult> {
    compare_edges(
        &by_name(code_edges),
        &by_name(db_edges),
        &BTreeSet::new(),
        &BTreeMap::new(),
    )
}

/// The edge a plain table stands for, used when the database holds a code
/// edge as an ordinary (non-`RELATION`) table.
fn edge_from_table(table: &TableDefinition) -> EdgeDefinition {
    EdgeDefinition {
        name: table.name.clone(),
        mode: match table.mode {
            TableMode::Schemafull => EdgeMode::Schemafull,
            TableMode::Schemaless | TableMode::Drop => EdgeMode::Schemaless,
        },
        from_table: None,
        to_table: None,
        fields: table.fields.clone(),
        indexes: table.indexes.clone(),
        events: table.events.clone(),
        permissions: table.permissions.clone(),
    }
}

fn compare_edges(
    code: &BTreeMap<&str, &EdgeDefinition>,
    db: &BTreeMap<&str, &EdgeDefinition>,
    code_table_names: &BTreeSet<&str>,
    db_tables: &BTreeMap<&str, &TableDefinition>,
) -> Vec<ValidationResult> {
    let mut results = Vec::new();

    for (name, code_edge) in code.iter().filter(|(n, _)| !db.contains_key(*n)) {
        match db_tables.get(name) {
            Some(db_table) => results.extend(validate_edge(code_edge, &edge_from_table(db_table))),
            None => results.push(presence(
                ValidationSeverity::Error,
                name,
                None,
                "Edge defined in code but missing from database",
                true,
            )),
        }
    }

    // A database edge named like a code table was reported with the tables.
    for name in db
        .keys()
        .filter(|n| !code.contains_key(*n) && !code_table_names.contains(*n))
    {
        results.push(presence(
            ValidationSeverity::Warning,
            name,
            None,
            "Edge exists in database but not defined in code",
            false,
        ));
    }

    for (name, code_edge) in code {
        if let Some(db_edge) = db.get(name) {
            results.extend(validate_edge(code_edge, db_edge));
        }
    }

    results
}

fn compare_endpoint(
    edge: &str,
    clause: &str,
    code: Option<&str>,
    db: Option<&str>,
) -> Option<ValidationResult> {
    // Differing tables are drift; a constraint on one side only is reported
    // softer, since an unconstrained relation still accepts the code's links.
    let severity = match (code, db) {
        (Some(c), Some(d)) if c != d => ValidationSeverity::Error,
        (Some(_), None) | (None, Some(_)) => ValidationSeverity::Warning,
        _ => return None,
    };
    Some(ValidationResult::new(
        severity,
        edge,
        None,
        format!("Edge {clause} table mismatch"),
        code.map(str::to_string),
        db.map(str::to_string),
    ))
}

/// Validate a single edge definition against its database counterpart.
///
/// Mirrors [`validate_table`] for edges: mode, the `FROM` / `TO` endpoints
/// of a `RELATION` edge, permissions, fields, indexes, and events. Only
/// differences are reported; an edge that matches yields an empty vector.
pub fn validate_edge(
    code_edge: &EdgeDefinition,
    db_edge: &EdgeDefinition,
) -> Vec<ValidationResult> {
    let edge = code_edge.name.as_str();
    let mut results = Vec::new();

    if code_edge.mode != db_edge.mode {
        results.push(ValidationResult::new(
            ValidationSeverity::Error,
            edge,
            None,
            "Edge mode mismatch",
            Some(code_edge.mode.as_str().to_string()),
            Some(db_edge.mode.as_str().to_string()),
        ));
    }

    if code_edge.mode == EdgeMode::Relation || db_edge.mode == EdgeMode::Relation {
        results.extend(compare_endpoint(
            edge,
            "FROM",
            code_edge.from_table.as_deref(),
            db_edge.from_table.as_deref(),
        ));
        results.extend(compare_endpoint(
            edge,
            "TO",
            code_edge.to_table.as_deref(),
            db_edge.to_table.as_deref(),
        ));
    }

    results.extend(compare_permissions(
        edge,
        None,
        "Edge permissions mismatch",
        code_edge.permissions.as_ref(),
        db_edge.permissions.as_ref(),
        &TABLE_PERMISSIONS,
    ));
    results.extend(compare_fields(edge, &code_edge.fields, &db_edge.fields));
    results.extend(compare_indexes(edge, &code_edge.indexes, &db_edge.indexes));
    results.extend(compare_events(edge, &code_edge.events, &db_edge.events));
    results
}

#[cfg(test)]
mod tests;
#[cfg(test)]
mod tests_drift;
