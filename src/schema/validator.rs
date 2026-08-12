//! Schema validation: compare code-defined schemas against database-observed
//! schemas.
//!
//! Port of `surql/schema/validator.py`. This module performs cross-schema
//! validation and produces a list of [`ValidationResult`] entries describing
//! every difference between the two sides. Each result carries a
//! [`ValidationSeverity`] (`ERROR` / `WARNING` / `INFO`), the table (and
//! optionally field) it applies to, a human-readable message, and the
//! conflicting values on each side.
//!
//! Unlike the Python source, async database fetching lives outside of the
//! pure-schema layer: [`validate_schema`] takes both code and database
//! table/edge maps as arguments. Callers are expected to produce the `db_*`
//! maps — typically by querying `INFO FOR DB` / `INFO FOR TABLE` and parsing
//! the results via the schema parser.
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

use std::collections::{BTreeSet, HashMap};

use serde::{Deserialize, Serialize};

use super::edge::{EdgeDefinition, EdgeMode};
use super::fields::FieldDefinition;
use super::table::{IndexDefinition, IndexType, TableDefinition};

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
            write!(f, ".{}", field)?;
        }
        write!(f, ": {}", self.message)?;
        if self.code_value.is_some() || self.db_value.is_some() {
            let code = self.code_value.as_deref().unwrap_or("None");
            let db = self.db_value.as_deref().unwrap_or("None");
            write!(f, " (code: {}, db: {})", code, db)?;
        }
        Ok(())
    }
}

// -----------------------------------------------------------------------------
// Main validation entry point
// -----------------------------------------------------------------------------

/// Compare a set of code-defined schemas against a set of database-observed
/// schemas.
///
/// Callers are expected to have fetched the `db_tables` / `db_edges` maps up
/// front — this function is pure and synchronous.
///
/// Returns the aggregated list of [`ValidationResult`] entries. An empty
/// vector means the schemas match.
#[allow(clippy::implicit_hasher)]
pub fn validate_schema(
    code_tables: &HashMap<String, TableDefinition>,
    db_tables: &HashMap<String, TableDefinition>,
    code_edges: Option<&HashMap<String, EdgeDefinition>>,
    db_edges: Option<&HashMap<String, TableDefinition>>,
) -> Vec<ValidationResult> {
    let mut results = Vec::new();

    results.extend(validate_tables(code_tables, db_tables));

    if let Some(code_edges) = code_edges {
        let empty_edges: HashMap<String, TableDefinition> = HashMap::new();
        let db_edges = db_edges.unwrap_or(&empty_edges);
        results.extend(validate_edges(code_edges, db_edges));
    }

    results
}

// -----------------------------------------------------------------------------
// Table validation
// -----------------------------------------------------------------------------

/// Validate every table across the two maps (missing, extra, and matching).
#[allow(clippy::implicit_hasher)]
pub fn validate_tables(
    code_tables: &HashMap<String, TableDefinition>,
    db_tables: &HashMap<String, TableDefinition>,
) -> Vec<ValidationResult> {
    let mut results = Vec::new();

    let code_names: BTreeSet<&String> = code_tables.keys().collect();
    let db_names: BTreeSet<&String> = db_tables.keys().collect();

    // Missing tables — in code but not in DB.
    for name in code_names.difference(&db_names) {
        results.push(ValidationResult::new(
            ValidationSeverity::Error,
            (*name).clone(),
            None,
            "Table defined in code but missing from database",
            Some("exists".into()),
            Some("missing".into()),
        ));
    }

    // Extra tables — in DB but not in code.
    for name in db_names.difference(&code_names) {
        results.push(ValidationResult::new(
            ValidationSeverity::Warning,
            (*name).clone(),
            None,
            "Table exists in database but not defined in code",
            Some("missing".into()),
            Some("exists".into()),
        ));
    }

    // Matching tables.
    for name in code_names.intersection(&db_names) {
        let code_table = &code_tables[*name];
        let db_table = &db_tables[*name];
        results.extend(validate_table(code_table, db_table));
    }

    results
}

/// Validate a single table (mode, fields, indexes, events).
pub fn validate_table(
    code_table: &TableDefinition,
    db_table: &TableDefinition,
) -> Vec<ValidationResult> {
    let mut results = Vec::new();
    results.extend(validate_table_mode(code_table, db_table));
    results.extend(validate_fields(code_table, db_table));
    results.extend(validate_indexes(code_table, db_table));
    results.extend(validate_events(code_table, db_table));
    results
}

fn validate_table_mode(
    code_table: &TableDefinition,
    db_table: &TableDefinition,
) -> Vec<ValidationResult> {
    if code_table.mode == db_table.mode {
        return Vec::new();
    }
    vec![ValidationResult::new(
        ValidationSeverity::Error,
        &code_table.name,
        None,
        "Table mode mismatch",
        Some(code_table.mode.as_str().to_string()),
        Some(db_table.mode.as_str().to_string()),
    )]
}

// -----------------------------------------------------------------------------
// Field validation
// -----------------------------------------------------------------------------

fn field_map(fields: &[FieldDefinition]) -> HashMap<&str, &FieldDefinition> {
    fields.iter().map(|f| (f.name.as_str(), f)).collect()
}

fn validate_fields(
    code_table: &TableDefinition,
    db_table: &TableDefinition,
) -> Vec<ValidationResult> {
    let mut results = Vec::new();
    let table_name = &code_table.name;

    let code_fields = field_map(&code_table.fields);
    let db_fields = field_map(&db_table.fields);

    let code_names: BTreeSet<&str> = code_fields.keys().copied().collect();
    let db_names: BTreeSet<&str> = db_fields.keys().copied().collect();

    for name in code_names.difference(&db_names) {
        results.push(ValidationResult::new(
            ValidationSeverity::Error,
            table_name,
            Some((*name).to_string()),
            "Field defined in code but missing from database",
            Some("exists".into()),
            Some("missing".into()),
        ));
    }

    for name in db_names.difference(&code_names) {
        results.push(ValidationResult::new(
            ValidationSeverity::Warning,
            table_name,
            Some((*name).to_string()),
            "Field exists in database but not defined in code",
            Some("missing".into()),
            Some("exists".into()),
        ));
    }

    for name in code_names.intersection(&db_names) {
        let code_field = code_fields[name];
        let db_field = db_fields[name];
        results.extend(validate_field(table_name, code_field, db_field));
    }

    results
}

/// Validate a single field across code and database definitions.
pub fn validate_field(
    table_name: &str,
    code_field: &FieldDefinition,
    db_field: &FieldDefinition,
) -> Vec<ValidationResult> {
    let mut results = Vec::new();

    if code_field.field_type != db_field.field_type {
        results.push(ValidationResult::new(
            ValidationSeverity::Error,
            table_name,
            Some(code_field.name.clone()),
            "Field type mismatch",
            Some(code_field.field_type.as_str().to_string()),
            Some(db_field.field_type.as_str().to_string()),
        ));
    }

    if normalize_expression(code_field.assertion.as_deref())
        != normalize_expression(db_field.assertion.as_deref())
    {
        results.push(ValidationResult::new(
            ValidationSeverity::Warning,
            table_name,
            Some(code_field.name.clone()),
            "Field assertion mismatch",
            code_field.assertion.clone(),
            db_field.assertion.clone(),
        ));
    }

    if normalize_expression(code_field.default.as_deref())
        != normalize_expression(db_field.default.as_deref())
    {
        results.push(ValidationResult::new(
            ValidationSeverity::Warning,
            table_name,
            Some(code_field.name.clone()),
            "Field default value mismatch",
            code_field.default.clone(),
            db_field.default.clone(),
        ));
    }

    if normalize_expression(code_field.value.as_deref())
        != normalize_expression(db_field.value.as_deref())
    {
        results.push(ValidationResult::new(
            ValidationSeverity::Warning,
            table_name,
            Some(code_field.name.clone()),
            "Field computed value mismatch",
            code_field.value.clone(),
            db_field.value.clone(),
        ));
    }

    if code_field.readonly != db_field.readonly {
        results.push(ValidationResult::new(
            ValidationSeverity::Info,
            table_name,
            Some(code_field.name.clone()),
            "Field readonly flag mismatch",
            Some(code_field.readonly.to_string()),
            Some(db_field.readonly.to_string()),
        ));
    }

    if code_field.flexible != db_field.flexible {
        results.push(ValidationResult::new(
            ValidationSeverity::Info,
            table_name,
            Some(code_field.name.clone()),
            "Field flexible flag mismatch",
            Some(code_field.flexible.to_string()),
            Some(db_field.flexible.to_string()),
        ));
    }

    results
}

// -----------------------------------------------------------------------------
// Index validation
// -----------------------------------------------------------------------------

fn index_map(indexes: &[IndexDefinition]) -> HashMap<&str, &IndexDefinition> {
    indexes.iter().map(|i| (i.name.as_str(), i)).collect()
}

fn validate_indexes(
    code_table: &TableDefinition,
    db_table: &TableDefinition,
) -> Vec<ValidationResult> {
    let mut results = Vec::new();
    let table_name = &code_table.name;

    let code_indexes = index_map(&code_table.indexes);
    let db_indexes = index_map(&db_table.indexes);

    let code_names: BTreeSet<&str> = code_indexes.keys().copied().collect();
    let db_names: BTreeSet<&str> = db_indexes.keys().copied().collect();

    for name in code_names.difference(&db_names) {
        results.push(ValidationResult::new(
            ValidationSeverity::Error,
            table_name,
            Some(format!("index:{}", name)),
            "Index defined in code but missing from database",
            Some("exists".into()),
            Some("missing".into()),
        ));
    }

    for name in db_names.difference(&code_names) {
        results.push(ValidationResult::new(
            ValidationSeverity::Warning,
            table_name,
            Some(format!("index:{}", name)),
            "Index exists in database but not defined in code",
            Some("missing".into()),
            Some("exists".into()),
        ));
    }

    for name in code_names.intersection(&db_names) {
        let code_index = code_indexes[name];
        let db_index = db_indexes[name];
        results.extend(validate_index(table_name, code_index, db_index));
    }

    results
}

/// Validate a single index across code and database definitions.
pub fn validate_index(
    table_name: &str,
    code_index: &IndexDefinition,
    db_index: &IndexDefinition,
) -> Vec<ValidationResult> {
    let mut results = Vec::new();
    let index_field = format!("index:{}", code_index.name);

    if code_index.index_type != db_index.index_type {
        results.push(ValidationResult::new(
            ValidationSeverity::Error,
            table_name,
            Some(index_field.clone()),
            "Index type mismatch",
            Some(code_index.index_type.as_str().to_string()),
            Some(db_index.index_type.as_str().to_string()),
        ));
    }

    let mut code_cols = code_index.columns.clone();
    code_cols.sort();
    let mut db_cols = db_index.columns.clone();
    db_cols.sort();
    if code_cols != db_cols {
        results.push(ValidationResult::new(
            ValidationSeverity::Error,
            table_name,
            Some(index_field.clone()),
            "Index columns mismatch",
            Some(code_index.columns.join(",")),
            Some(db_index.columns.join(",")),
        ));
    }

    if code_index.index_type == IndexType::Mtree {
        results.extend(validate_mtree_index(table_name, code_index, db_index));
    }
    if code_index.index_type == IndexType::Hnsw {
        results.extend(validate_hnsw_index(table_name, code_index, db_index));
    }

    results
}

fn validate_mtree_index(
    table_name: &str,
    code_index: &IndexDefinition,
    db_index: &IndexDefinition,
) -> Vec<ValidationResult> {
    let mut results = Vec::new();
    let index_field = format!("index:{}", code_index.name);

    if code_index.dimension != db_index.dimension {
        results.push(ValidationResult::new(
            ValidationSeverity::Error,
            table_name,
            Some(index_field.clone()),
            "MTREE index dimension mismatch",
            code_index.dimension.map(|d| d.to_string()),
            db_index.dimension.map(|d| d.to_string()),
        ));
    }

    if code_index.distance != db_index.distance {
        results.push(ValidationResult::new(
            ValidationSeverity::Warning,
            table_name,
            Some(index_field.clone()),
            "MTREE index distance metric mismatch",
            code_index.distance.map(|d| d.as_str().to_string()),
            db_index.distance.map(|d| d.as_str().to_string()),
        ));
    }

    if code_index.vector_type != db_index.vector_type {
        results.push(ValidationResult::new(
            ValidationSeverity::Warning,
            table_name,
            Some(index_field),
            "MTREE index vector type mismatch",
            code_index.vector_type.map(|v| v.as_str().to_string()),
            db_index.vector_type.map(|v| v.as_str().to_string()),
        ));
    }

    results
}

fn validate_hnsw_index(
    table_name: &str,
    code_index: &IndexDefinition,
    db_index: &IndexDefinition,
) -> Vec<ValidationResult> {
    let mut results = Vec::new();
    let index_field = format!("index:{}", code_index.name);

    if code_index.dimension != db_index.dimension {
        results.push(ValidationResult::new(
            ValidationSeverity::Error,
            table_name,
            Some(index_field.clone()),
            "HNSW index dimension mismatch",
            code_index.dimension.map(|d| d.to_string()),
            db_index.dimension.map(|d| d.to_string()),
        ));
    }

    if code_index.hnsw_distance != db_index.hnsw_distance {
        results.push(ValidationResult::new(
            ValidationSeverity::Warning,
            table_name,
            Some(index_field.clone()),
            "HNSW index distance metric mismatch",
            code_index.hnsw_distance.map(|d| d.as_str().to_string()),
            db_index.hnsw_distance.map(|d| d.as_str().to_string()),
        ));
    }

    if code_index.vector_type != db_index.vector_type {
        results.push(ValidationResult::new(
            ValidationSeverity::Warning,
            table_name,
            Some(index_field.clone()),
            "HNSW index vector type mismatch",
            code_index.vector_type.map(|v| v.as_str().to_string()),
            db_index.vector_type.map(|v| v.as_str().to_string()),
        ));
    }

    if code_index.efc != db_index.efc {
        results.push(ValidationResult::new(
            ValidationSeverity::Warning,
            table_name,
            Some(index_field.clone()),
            "HNSW index EFC mismatch",
            code_index.efc.map(|e| e.to_string()),
            db_index.efc.map(|e| e.to_string()),
        ));
    }

    if code_index.m != db_index.m {
        results.push(ValidationResult::new(
            ValidationSeverity::Warning,
            table_name,
            Some(index_field),
            "HNSW index M mismatch",
            code_index.m.map(|m| m.to_string()),
            db_index.m.map(|m| m.to_string()),
        ));
    }

    results
}

// -----------------------------------------------------------------------------
// Event validation
// -----------------------------------------------------------------------------

fn validate_events(
    code_table: &TableDefinition,
    db_table: &TableDefinition,
) -> Vec<ValidationResult> {
    let mut results = Vec::new();
    let table_name = &code_table.name;

    let code_events: BTreeSet<&str> = code_table.events.iter().map(|e| e.name.as_str()).collect();
    let db_events: BTreeSet<&str> = db_table.events.iter().map(|e| e.name.as_str()).collect();

    for name in code_events.difference(&db_events) {
        results.push(ValidationResult::new(
            ValidationSeverity::Error,
            table_name,
            Some(format!("event:{}", name)),
            "Event defined in code but missing from database",
            Some("exists".into()),
            Some("missing".into()),
        ));
    }

    for name in db_events.difference(&code_events) {
        results.push(ValidationResult::new(
            ValidationSeverity::Warning,
            table_name,
            Some(format!("event:{}", name)),
            "Event exists in database but not defined in code",
            Some("missing".into()),
            Some("exists".into()),
        ));
    }

    results
}

// -----------------------------------------------------------------------------
// Edge validation
// -----------------------------------------------------------------------------

/// Validate every edge definition against its corresponding database table.
#[allow(clippy::implicit_hasher)]
pub fn validate_edges(
    code_edges: &HashMap<String, EdgeDefinition>,
    db_edges: &HashMap<String, TableDefinition>,
) -> Vec<ValidationResult> {
    let mut results = Vec::new();

    let code_names: BTreeSet<&String> = code_edges.keys().collect();
    let db_names: BTreeSet<&String> = db_edges.keys().collect();

    for name in code_names.difference(&db_names) {
        results.push(ValidationResult::new(
            ValidationSeverity::Error,
            (*name).clone(),
            None,
            "Edge defined in code but missing from database",
            Some("exists".into()),
            Some("missing".into()),
        ));
    }

    for name in code_names.intersection(&db_names) {
        let code_edge = &code_edges[*name];
        let db_edge = &db_edges[*name];
        results.extend(validate_edge(code_edge, db_edge));
    }

    results
}

/// Validate a single edge definition against its database table representation.
pub fn validate_edge(
    code_edge: &EdgeDefinition,
    db_edge: &TableDefinition,
) -> Vec<ValidationResult> {
    let mut results = Vec::new();
    let edge_name = &code_edge.name;

    if code_edge.mode == EdgeMode::Relation {
        results.push(ValidationResult::new(
            ValidationSeverity::Info,
            edge_name,
            None,
            format!("Edge mode: {}", code_edge.mode.as_str()),
            Some(code_edge.mode.as_str().to_string()),
            Some(db_edge.mode.as_str().to_string()),
        ));
    }

    let code_fields = field_map(&code_edge.fields);
    let db_fields = field_map(&db_edge.fields);

    for (name, code_field) in &code_fields {
        if let Some(db_field) = db_fields.get(name) {
            results.extend(validate_field(edge_name, code_field, db_field));
        } else {
            results.push(ValidationResult::new(
                ValidationSeverity::Error,
                edge_name,
                Some((*name).to_string()),
                "Edge field missing from database",
                Some("exists".into()),
                Some("missing".into()),
            ));
        }
    }

    let code_indexes = index_map(&code_edge.indexes);
    let db_indexes = index_map(&db_edge.indexes);

    for (name, code_index) in &code_indexes {
        if let Some(db_index) = db_indexes.get(name) {
            results.extend(validate_index(edge_name, code_index, db_index));
        } else {
            results.push(ValidationResult::new(
                ValidationSeverity::Error,
                edge_name,
                Some(format!("index:{}", name)),
                "Edge index missing from database",
                Some("exists".into()),
                Some("missing".into()),
            ));
        }
    }

    results
}

// -----------------------------------------------------------------------------
// Utility
// -----------------------------------------------------------------------------

/// Normalize a SurrealQL expression for comparison purposes.
///
/// Collapses consecutive whitespace to a single space and trims the result.
/// Returns `None` for `None` inputs or expressions that become empty after
/// normalization.
pub fn normalize_expression(expr: Option<&str>) -> Option<String> {
    let expr = expr?;
    let normalized = expr.split_whitespace().collect::<Vec<_>>().join(" ");
    if normalized.is_empty() {
        None
    } else {
        Some(normalized)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::fields::{FieldDefinition, FieldType};
    use crate::schema::table::{
        event, mtree_index, table_schema, EventDefinition, IndexDefinition, IndexType,
        MTreeDistanceType, MTreeVectorType, TableDefinition, TableMode,
    };

    fn user_with_name() -> TableDefinition {
        table_schema("user").with_fields([FieldDefinition::new("name", FieldType::String)])
    }

    // -- ValidationSeverity ----------------------------------------------------

    #[test]
    fn severity_error_value() {
        assert_eq!(ValidationSeverity::Error.as_str(), "error");
    }

    #[test]
    fn severity_warning_value() {
        assert_eq!(ValidationSeverity::Warning.as_str(), "warning");
    }

    #[test]
    fn severity_info_value() {
        assert_eq!(ValidationSeverity::Info.as_str(), "info");
    }

    #[test]
    fn severity_display_is_lowercase() {
        assert_eq!(format!("{}", ValidationSeverity::Error), "error");
    }

    #[test]
    fn severity_upper_tags() {
        assert_eq!(ValidationSeverity::Error.as_upper_str(), "ERROR");
        assert_eq!(ValidationSeverity::Warning.as_upper_str(), "WARNING");
        assert_eq!(ValidationSeverity::Info.as_upper_str(), "INFO");
    }

    // -- ValidationResult ------------------------------------------------------

    #[test]
    fn validation_result_creation_basic() {
        let r = ValidationResult::new(
            ValidationSeverity::Error,
            "user",
            Some("email".into()),
            "Field type mismatch",
            Some("string".into()),
            Some("int".into()),
        );
        assert_eq!(r.severity, ValidationSeverity::Error);
        assert_eq!(r.table, "user");
        assert_eq!(r.field.as_deref(), Some("email"));
        assert_eq!(r.message, "Field type mismatch");
        assert_eq!(r.code_value.as_deref(), Some("string"));
        assert_eq!(r.db_value.as_deref(), Some("int"));
    }

    #[test]
    fn validation_result_none_field() {
        let r = ValidationResult::new(
            ValidationSeverity::Error,
            "user",
            None,
            "Table missing",
            Some("exists".into()),
            Some("missing".into()),
        );
        assert!(r.field.is_none());
    }

    #[test]
    fn validation_result_none_values() {
        let r = ValidationResult::new(
            ValidationSeverity::Info,
            "user",
            Some("name".into()),
            "info",
            None,
            None,
        );
        assert!(r.code_value.is_none());
        assert!(r.db_value.is_none());
    }

    #[test]
    fn validation_result_display_with_field() {
        let r = ValidationResult::new(
            ValidationSeverity::Error,
            "user",
            Some("email".into()),
            "Field type mismatch",
            Some("string".into()),
            Some("int".into()),
        );
        let s = r.to_string();
        assert!(s.contains("[ERROR]"));
        assert!(s.contains("user.email"));
        assert!(s.contains("Field type mismatch"));
        assert!(s.contains("code: string"));
        assert!(s.contains("db: int"));
    }

    #[test]
    fn validation_result_display_without_field() {
        let r = ValidationResult::new(
            ValidationSeverity::Warning,
            "post",
            None,
            "Table missing",
            Some("missing".into()),
            Some("exists".into()),
        );
        let s = r.to_string();
        assert!(s.contains("[WARNING]"));
        assert!(s.contains("post"));
        assert!(!s.contains("post."));
    }

    #[test]
    fn validation_result_display_without_values() {
        let r = ValidationResult::new(
            ValidationSeverity::Info,
            "user",
            Some("name".into()),
            "Some info",
            None,
            None,
        );
        let s = r.to_string();
        assert!(s.contains("[INFO]"));
        assert!(!s.contains("code:"));
    }

    // -- Missing tables --------------------------------------------------------

    #[test]
    fn table_missing_from_database() {
        let mut code = HashMap::new();
        code.insert("user".into(), user_with_name());
        let db = HashMap::new();

        let results = validate_schema(&code, &db, None, None);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].severity, ValidationSeverity::Error);
        assert_eq!(results[0].table, "user");
        assert!(results[0].message.contains("missing from database"));
    }

    #[test]
    fn multiple_tables_missing() {
        let mut code = HashMap::new();
        code.insert("user".into(), table_schema("user"));
        code.insert("post".into(), table_schema("post"));
        let db = HashMap::new();

        let results = validate_schema(&code, &db, None, None);
        assert_eq!(results.len(), 2);
        let names: BTreeSet<&str> = results.iter().map(|r| r.table.as_str()).collect();
        assert!(names.contains("user"));
        assert!(names.contains("post"));
    }

    // -- Extra tables ----------------------------------------------------------

    #[test]
    fn table_in_database_not_in_code() {
        let code: HashMap<String, TableDefinition> = HashMap::new();
        let mut db = HashMap::new();
        db.insert("legacy_table".into(), table_schema("legacy_table"));

        let results = validate_schema(&code, &db, None, None);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].severity, ValidationSeverity::Warning);
        assert_eq!(results[0].table, "legacy_table");
        assert!(results[0].message.contains("not defined in code"));
    }

    // -- Matching schemas ------------------------------------------------------

    #[test]
    fn schemas_match_returns_empty() {
        let mut code = HashMap::new();
        code.insert("user".into(), user_with_name());
        let mut db = HashMap::new();
        db.insert("user".into(), user_with_name());

        let results = validate_schema(&code, &db, None, None);
        assert!(results.is_empty());
    }

    // -- Field mismatches ------------------------------------------------------

    #[test]
    fn field_type_mismatch() {
        let code_table =
            table_schema("user").with_fields([FieldDefinition::new("age", FieldType::Int)]);
        let db_table =
            table_schema("user").with_fields([FieldDefinition::new("age", FieldType::String)]);

        let mut code = HashMap::new();
        code.insert("user".into(), code_table);
        let mut db = HashMap::new();
        db.insert("user".into(), db_table);

        let results = validate_schema(&code, &db, None, None);
        let mismatches: Vec<_> = results
            .iter()
            .filter(|r| r.message.to_lowercase().contains("type mismatch"))
            .collect();
        assert!(!mismatches.is_empty());
        assert_eq!(mismatches[0].severity, ValidationSeverity::Error);
    }

    #[test]
    fn field_missing_from_database() {
        let code_table = table_schema("user").with_fields([
            FieldDefinition::new("name", FieldType::String),
            FieldDefinition::new("email", FieldType::String),
        ]);
        let db_table =
            table_schema("user").with_fields([FieldDefinition::new("name", FieldType::String)]);

        let mut code = HashMap::new();
        code.insert("user".into(), code_table);
        let mut db = HashMap::new();
        db.insert("user".into(), db_table);

        let results = validate_schema(&code, &db, None, None);
        let missing: Vec<_> = results
            .iter()
            .filter(|r| r.field.as_deref() == Some("email"))
            .collect();
        assert_eq!(missing.len(), 1);
        assert_eq!(missing[0].severity, ValidationSeverity::Error);
        assert!(missing[0].message.contains("missing from database"));
    }

    #[test]
    fn extra_field_in_database() {
        let code_table =
            table_schema("user").with_fields([FieldDefinition::new("name", FieldType::String)]);
        let db_table = table_schema("user").with_fields([
            FieldDefinition::new("name", FieldType::String),
            FieldDefinition::new("legacy_field", FieldType::Int),
        ]);

        let mut code = HashMap::new();
        code.insert("user".into(), code_table);
        let mut db = HashMap::new();
        db.insert("user".into(), db_table);

        let results = validate_schema(&code, &db, None, None);
        let extra: Vec<_> = results
            .iter()
            .filter(|r| r.field.as_deref() == Some("legacy_field"))
            .collect();
        assert_eq!(extra.len(), 1);
        assert_eq!(extra[0].severity, ValidationSeverity::Warning);
        assert!(extra[0].message.contains("not defined in code"));
    }

    #[test]
    fn field_assertion_mismatch() {
        let code_table =
            table_schema("user").with_fields([FieldDefinition::new("email", FieldType::String)
                .with_assertion("string::is::email($value)")]);
        let db_table =
            table_schema("user")
                .with_fields([FieldDefinition::new("email", FieldType::String)
                    .with_assertion("$value != NONE")]);

        let mut code = HashMap::new();
        code.insert("user".into(), code_table);
        let mut db = HashMap::new();
        db.insert("user".into(), db_table);

        let results = validate_schema(&code, &db, None, None);
        let assertions: Vec<_> = results
            .iter()
            .filter(|r| r.message.to_lowercase().contains("assertion"))
            .collect();
        assert!(!assertions.is_empty());
        assert_eq!(assertions[0].severity, ValidationSeverity::Warning);
    }

    #[test]
    fn field_default_mismatch() {
        let code_table = table_schema("t")
            .with_fields([FieldDefinition::new("x", FieldType::Int).with_default("0")]);
        let db_table = table_schema("t")
            .with_fields([FieldDefinition::new("x", FieldType::Int).with_default("1")]);

        let results = validate_field("t", &code_table.fields[0], &db_table.fields[0]);
        assert!(results
            .iter()
            .any(|r| r.message.contains("default value mismatch")));
    }

    #[test]
    fn field_value_mismatch() {
        let code_table = table_schema("t")
            .with_fields([FieldDefinition::new("x", FieldType::Int).with_value("1 + 1")]);
        let db_table = table_schema("t")
            .with_fields([FieldDefinition::new("x", FieldType::Int).with_value("2 + 2")]);

        let results = validate_field("t", &code_table.fields[0], &db_table.fields[0]);
        assert!(results
            .iter()
            .any(|r| r.message.contains("computed value mismatch")));
    }

    #[test]
    fn field_readonly_mismatch_is_info() {
        let code_field = FieldDefinition::new("x", FieldType::Int).readonly(true);
        let db_field = FieldDefinition::new("x", FieldType::Int).readonly(false);
        let r = validate_field("t", &code_field, &db_field);
        let msg = r.iter().find(|r| r.message.contains("readonly")).unwrap();
        assert_eq!(msg.severity, ValidationSeverity::Info);
    }

    #[test]
    fn field_flexible_mismatch_is_info() {
        let code_field = FieldDefinition::new("x", FieldType::Object).flexible(true);
        let db_field = FieldDefinition::new("x", FieldType::Object).flexible(false);
        let r = validate_field("t", &code_field, &db_field);
        let msg = r.iter().find(|r| r.message.contains("flexible")).unwrap();
        assert_eq!(msg.severity, ValidationSeverity::Info);
    }

    #[test]
    fn field_assertion_whitespace_normalized() {
        let code_field =
            FieldDefinition::new("x", FieldType::String).with_assertion("$value  !=  NONE");
        let db_field =
            FieldDefinition::new("x", FieldType::String).with_assertion("$value != NONE");
        let r = validate_field("t", &code_field, &db_field);
        assert!(!r.iter().any(|r| r.message.contains("assertion")));
    }

    // -- Index mismatches ------------------------------------------------------

    #[test]
    fn index_missing_from_database() {
        let code_table =
            table_schema("user").with_indexes([
                IndexDefinition::new("email_idx", ["email"]).with_type(IndexType::Unique)
            ]);
        let db_table = table_schema("user");

        let mut code = HashMap::new();
        code.insert("user".into(), code_table);
        let mut db = HashMap::new();
        db.insert("user".into(), db_table);

        let results = validate_schema(&code, &db, None, None);
        let missing: Vec<_> = results
            .iter()
            .filter(|r| r.field.as_deref() == Some("index:email_idx"))
            .collect();
        assert_eq!(missing.len(), 1);
        assert_eq!(missing[0].severity, ValidationSeverity::Error);
        assert!(missing[0].message.contains("missing from database"));
    }

    #[test]
    fn extra_index_in_database() {
        let code_table = table_schema("user");
        let db_table =
            table_schema("user").with_indexes([IndexDefinition::new("legacy_idx", ["legacy"])]);

        let mut code = HashMap::new();
        code.insert("user".into(), code_table);
        let mut db = HashMap::new();
        db.insert("user".into(), db_table);

        let results = validate_schema(&code, &db, None, None);
        let extra: Vec<_> = results
            .iter()
            .filter(|r| r.field.as_deref() == Some("index:legacy_idx"))
            .collect();
        assert_eq!(extra.len(), 1);
        assert_eq!(extra[0].severity, ValidationSeverity::Warning);
    }

    #[test]
    fn index_type_mismatch() {
        let code_table =
            table_schema("user").with_indexes([
                IndexDefinition::new("email_idx", ["email"]).with_type(IndexType::Unique)
            ]);
        let db_table =
            table_schema("user").with_indexes([IndexDefinition::new("email_idx", ["email"])]);

        let mut code = HashMap::new();
        code.insert("user".into(), code_table);
        let mut db = HashMap::new();
        db.insert("user".into(), db_table);

        let results = validate_schema(&code, &db, None, None);
        let mismatches: Vec<_> = results
            .iter()
            .filter(|r| r.message.contains("Index type mismatch"))
            .collect();
        assert!(!mismatches.is_empty());
        assert_eq!(mismatches[0].severity, ValidationSeverity::Error);
    }

    #[test]
    fn index_columns_mismatch() {
        let code_table = table_schema("user").with_indexes([IndexDefinition::new(
            "name_idx",
            ["first_name", "last_name"],
        )]);
        let db_table =
            table_schema("user").with_indexes([IndexDefinition::new("name_idx", ["first_name"])]);

        let mut code = HashMap::new();
        code.insert("user".into(), code_table);
        let mut db = HashMap::new();
        db.insert("user".into(), db_table);

        let results = validate_schema(&code, &db, None, None);
        let mismatches: Vec<_> = results
            .iter()
            .filter(|r| r.message.contains("columns mismatch"))
            .collect();
        assert!(!mismatches.is_empty());
        assert_eq!(mismatches[0].severity, ValidationSeverity::Error);
    }

    #[test]
    fn index_columns_order_insensitive() {
        let code_table =
            table_schema("user").with_indexes([IndexDefinition::new("composite", ["a", "b"])]);
        let db_table =
            table_schema("user").with_indexes([IndexDefinition::new("composite", ["b", "a"])]);

        let mut code = HashMap::new();
        code.insert("user".into(), code_table);
        let mut db = HashMap::new();
        db.insert("user".into(), db_table);

        let results = validate_schema(&code, &db, None, None);
        assert!(!results
            .iter()
            .any(|r| r.message.contains("columns mismatch")));
    }

    #[test]
    fn mtree_index_dimension_mismatch() {
        let code_table = table_schema("document").with_indexes([mtree_index(
            "vec_idx",
            "embedding",
            1024,
            MTreeDistanceType::Cosine,
            MTreeVectorType::F32,
        )]);
        let db_table = table_schema("document").with_indexes([mtree_index(
            "vec_idx",
            "embedding",
            768,
            MTreeDistanceType::Cosine,
            MTreeVectorType::F32,
        )]);

        let mut code = HashMap::new();
        code.insert("document".into(), code_table);
        let mut db = HashMap::new();
        db.insert("document".into(), db_table);

        let results = validate_schema(&code, &db, None, None);
        let dims: Vec<_> = results
            .iter()
            .filter(|r| r.message.contains("dimension mismatch"))
            .collect();
        assert!(!dims.is_empty());
        assert_eq!(dims[0].severity, ValidationSeverity::Error);
    }

    #[test]
    fn mtree_index_distance_mismatch_is_warning() {
        let code_idx = mtree_index(
            "v",
            "emb",
            32,
            MTreeDistanceType::Cosine,
            MTreeVectorType::F32,
        );
        let db_idx = mtree_index(
            "v",
            "emb",
            32,
            MTreeDistanceType::Euclidean,
            MTreeVectorType::F32,
        );
        let results = validate_index("t", &code_idx, &db_idx);
        let msg = results
            .iter()
            .find(|r| r.message.contains("distance metric mismatch"))
            .unwrap();
        assert_eq!(msg.severity, ValidationSeverity::Warning);
    }

    #[test]
    fn mtree_index_vector_type_mismatch_is_warning() {
        let code_idx = mtree_index(
            "v",
            "emb",
            32,
            MTreeDistanceType::Cosine,
            MTreeVectorType::F32,
        );
        let db_idx = mtree_index(
            "v",
            "emb",
            32,
            MTreeDistanceType::Cosine,
            MTreeVectorType::F64,
        );
        let results = validate_index("t", &code_idx, &db_idx);
        let msg = results
            .iter()
            .find(|r| r.message.contains("vector type mismatch"))
            .unwrap();
        assert_eq!(msg.severity, ValidationSeverity::Warning);
    }

    #[test]
    fn hnsw_index_dimension_mismatch() {
        use crate::schema::table::{hnsw_index, HnswDistanceType};
        let code_idx = hnsw_index(
            "v",
            "emb",
            128,
            HnswDistanceType::Cosine,
            MTreeVectorType::F32,
            None,
            None,
        );
        let db_idx = hnsw_index(
            "v",
            "emb",
            64,
            HnswDistanceType::Cosine,
            MTreeVectorType::F32,
            None,
            None,
        );
        let results = validate_index("t", &code_idx, &db_idx);
        let msg = results
            .iter()
            .find(|r| r.message.contains("HNSW index dimension mismatch"))
            .unwrap();
        assert_eq!(msg.severity, ValidationSeverity::Error);
    }

    #[test]
    fn hnsw_index_efc_m_mismatches() {
        use crate::schema::table::{hnsw_index, HnswDistanceType};
        let code_idx = hnsw_index(
            "v",
            "emb",
            64,
            HnswDistanceType::Cosine,
            MTreeVectorType::F32,
            Some(200),
            Some(16),
        );
        let db_idx = hnsw_index(
            "v",
            "emb",
            64,
            HnswDistanceType::Cosine,
            MTreeVectorType::F32,
            Some(400),
            Some(32),
        );
        let results = validate_index("t", &code_idx, &db_idx);
        assert!(results
            .iter()
            .any(|r| r.message.contains("HNSW index EFC mismatch")));
        assert!(results
            .iter()
            .any(|r| r.message.contains("HNSW index M mismatch")));
    }

    #[test]
    fn hnsw_index_distance_vector_type_mismatches() {
        use crate::schema::table::{hnsw_index, HnswDistanceType};
        let code_idx = hnsw_index(
            "v",
            "emb",
            64,
            HnswDistanceType::Cosine,
            MTreeVectorType::F32,
            None,
            None,
        );
        let db_idx = hnsw_index(
            "v",
            "emb",
            64,
            HnswDistanceType::Euclidean,
            MTreeVectorType::F64,
            None,
            None,
        );
        let results = validate_index("t", &code_idx, &db_idx);
        assert!(results
            .iter()
            .any(|r| r.message.contains("HNSW index distance metric mismatch")));
        assert!(results
            .iter()
            .any(|r| r.message.contains("HNSW index vector type mismatch")));
    }

    // -- Table mode mismatch --------------------------------------------------

    #[test]
    fn table_mode_mismatch_schemafull_vs_schemaless() {
        let code_table = table_schema("user").with_mode(TableMode::Schemafull);
        let db_table = table_schema("user").with_mode(TableMode::Schemaless);

        let mut code = HashMap::new();
        code.insert("user".into(), code_table);
        let mut db = HashMap::new();
        db.insert("user".into(), db_table);

        let results = validate_schema(&code, &db, None, None);
        let modes: Vec<_> = results
            .iter()
            .filter(|r| r.message.to_lowercase().contains("mode mismatch"))
            .collect();
        assert!(!modes.is_empty());
        assert_eq!(modes[0].severity, ValidationSeverity::Error);
    }

    // -- Event mismatches ------------------------------------------------------

    #[test]
    fn event_missing_from_database() {
        let code_table = table_schema("user").with_events([event("e", "true", "RETURN 1")]);
        let db_table = table_schema("user");

        let mut code = HashMap::new();
        code.insert("user".into(), code_table);
        let mut db = HashMap::new();
        db.insert("user".into(), db_table);

        let results = validate_schema(&code, &db, None, None);
        let missing: Vec<_> = results
            .iter()
            .filter(|r| r.field.as_deref() == Some("event:e"))
            .collect();
        assert_eq!(missing.len(), 1);
        assert_eq!(missing[0].severity, ValidationSeverity::Error);
    }

    #[test]
    fn extra_event_in_database() {
        let code_table = table_schema("user");
        let db_table =
            table_schema("user").with_events([EventDefinition::new("e", "true", "RETURN 1")]);

        let mut code = HashMap::new();
        code.insert("user".into(), code_table);
        let mut db = HashMap::new();
        db.insert("user".into(), db_table);

        let results = validate_schema(&code, &db, None, None);
        let extra: Vec<_> = results
            .iter()
            .filter(|r| r.field.as_deref() == Some("event:e"))
            .collect();
        assert_eq!(extra.len(), 1);
        assert_eq!(extra[0].severity, ValidationSeverity::Warning);
    }

    // -- Edge validation -------------------------------------------------------

    #[test]
    fn edge_missing_from_database() {
        use crate::schema::edge::typed_edge;

        let mut code_edges = HashMap::new();
        code_edges.insert("likes".into(), typed_edge("likes", "user", "post"));
        let db_edges: HashMap<String, TableDefinition> = HashMap::new();

        let results = validate_schema(
            &HashMap::new(),
            &HashMap::new(),
            Some(&code_edges),
            Some(&db_edges),
        );
        let missing: Vec<_> = results
            .iter()
            .filter(|r| r.table == "likes" && r.message.to_lowercase().contains("missing"))
            .collect();
        assert!(!missing.is_empty());
        assert_eq!(missing[0].severity, ValidationSeverity::Error);
    }

    #[test]
    fn edge_field_mismatch() {
        use crate::schema::edge::typed_edge;

        let code_edge = typed_edge("likes", "user", "post")
            .with_fields([FieldDefinition::new("weight", FieldType::Int)]);
        let db_edge = table_schema("likes").with_mode(TableMode::Schemafull);

        let mut code_edges = HashMap::new();
        code_edges.insert("likes".into(), code_edge);
        let mut db_edges = HashMap::new();
        db_edges.insert("likes".into(), db_edge);

        let results = validate_schema(
            &HashMap::new(),
            &HashMap::new(),
            Some(&code_edges),
            Some(&db_edges),
        );
        let field_issues: Vec<_> = results
            .iter()
            .filter(|r| r.field.as_deref() == Some("weight"))
            .collect();
        assert!(!field_issues.is_empty());
    }

    #[test]
    fn edge_field_type_mismatch_via_validate_field() {
        use crate::schema::edge::typed_edge;

        let code_edge = typed_edge("r", "user", "post")
            .with_fields([FieldDefinition::new("w", FieldType::Int)]);
        let db_edge = table_schema("r").with_fields([FieldDefinition::new("w", FieldType::String)]);

        let mut code_edges = HashMap::new();
        code_edges.insert("r".into(), code_edge);
        let mut db_edges = HashMap::new();
        db_edges.insert("r".into(), db_edge);

        let results = validate_schema(
            &HashMap::new(),
            &HashMap::new(),
            Some(&code_edges),
            Some(&db_edges),
        );
        assert!(results
            .iter()
            .any(|r| r.message.contains("Field type mismatch")));
    }

    #[test]
    fn edge_index_missing_from_database() {
        use crate::schema::edge::typed_edge;

        let code_edge =
            typed_edge("r", "user", "post").with_indexes([IndexDefinition::new("idx", ["w"])]);
        let db_edge = table_schema("r");

        let mut code_edges = HashMap::new();
        code_edges.insert("r".into(), code_edge);
        let mut db_edges = HashMap::new();
        db_edges.insert("r".into(), db_edge);

        let results = validate_schema(
            &HashMap::new(),
            &HashMap::new(),
            Some(&code_edges),
            Some(&db_edges),
        );
        assert!(results
            .iter()
            .any(|r| r.field.as_deref() == Some("index:idx")
                && r.message.contains("missing from database")));
    }

    #[test]
    fn edge_mode_info_emitted_for_relation() {
        use crate::schema::edge::typed_edge;

        let code_edge = typed_edge("r", "user", "post");
        let db_edge = table_schema("r");

        let mut code_edges = HashMap::new();
        code_edges.insert("r".into(), code_edge);
        let mut db_edges = HashMap::new();
        db_edges.insert("r".into(), db_edge);

        let results = validate_schema(
            &HashMap::new(),
            &HashMap::new(),
            Some(&code_edges),
            Some(&db_edges),
        );
        assert!(
            results
                .iter()
                .any(|r| r.severity == ValidationSeverity::Info
                    && r.message.starts_with("Edge mode:"))
        );
    }

    // -- normalize_expression --------------------------------------------------

    #[test]
    fn normalize_expression_none() {
        assert_eq!(normalize_expression(None), None);
    }

    #[test]
    fn normalize_expression_empty() {
        assert_eq!(normalize_expression(Some("   ")), None);
    }

    #[test]
    fn normalize_expression_collapses_whitespace() {
        assert_eq!(
            normalize_expression(Some("  $value   !=    NONE  ")).as_deref(),
            Some("$value != NONE")
        );
    }
}
