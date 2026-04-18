//! Table schema definitions.
//!
//! Port of `surql/schema/table.py`. Exposes the [`TableDefinition`] value
//! object together with the supporting enums ([`TableMode`], [`IndexType`],
//! [`MTreeDistanceType`], [`HnswDistanceType`], [`MTreeVectorType`]) and the
//! [`IndexDefinition`] / [`EventDefinition`] structs. Each definition renders
//! the corresponding `DEFINE` statement via `to_surql`.

use std::collections::BTreeMap;
use std::fmt::Write as _;

use serde::{Deserialize, Serialize};

use crate::error::{Result, SurqlError};

use super::fields::FieldDefinition;

/// Table schema mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum TableMode {
    /// Strict schema — fields must be declared up-front.
    Schemafull,
    /// Flexible schema — fields are added on write.
    Schemaless,
    /// Drop mode — server treats writes as no-ops.
    Drop,
}

impl TableMode {
    /// Render as SurrealQL keyword (`SCHEMAFULL` / `SCHEMALESS` / `DROP`).
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Schemafull => "SCHEMAFULL",
            Self::Schemaless => "SCHEMALESS",
            Self::Drop => "DROP",
        }
    }
}

impl std::fmt::Display for TableMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Index type supported by `DEFINE INDEX`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum IndexType {
    /// UNIQUE index.
    Unique,
    /// Full-text SEARCH index (with ASCII analyzer by default).
    Search,
    /// Plain b-tree style index.
    Standard,
    /// MTREE vector similarity index.
    Mtree,
    /// HNSW vector similarity index.
    Hnsw,
}

impl IndexType {
    /// Render as SurrealQL keyword (matching the Python enum values).
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Unique => "UNIQUE",
            Self::Search => "SEARCH",
            Self::Standard => "INDEX",
            Self::Mtree => "MTREE",
            Self::Hnsw => "HNSW",
        }
    }
}

impl std::fmt::Display for IndexType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Distance metric for MTREE vector indexes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum MTreeDistanceType {
    /// Cosine distance.
    Cosine,
    /// Euclidean (L2) distance.
    Euclidean,
    /// Manhattan (L1) distance.
    Manhattan,
    /// Minkowski distance.
    Minkowski,
}

impl MTreeDistanceType {
    /// Render as SurrealQL keyword.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Cosine => "COSINE",
            Self::Euclidean => "EUCLIDEAN",
            Self::Manhattan => "MANHATTAN",
            Self::Minkowski => "MINKOWSKI",
        }
    }
}

impl std::fmt::Display for MTreeDistanceType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Distance metric for HNSW vector indexes (superset of [`MTreeDistanceType`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum HnswDistanceType {
    /// Chebyshev distance.
    Chebyshev,
    /// Cosine distance.
    Cosine,
    /// Euclidean distance.
    Euclidean,
    /// Hamming distance.
    Hamming,
    /// Jaccard distance.
    Jaccard,
    /// Manhattan distance.
    Manhattan,
    /// Minkowski distance.
    Minkowski,
    /// Pearson correlation distance.
    Pearson,
}

impl HnswDistanceType {
    /// Render as SurrealQL keyword.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Chebyshev => "CHEBYSHEV",
            Self::Cosine => "COSINE",
            Self::Euclidean => "EUCLIDEAN",
            Self::Hamming => "HAMMING",
            Self::Jaccard => "JACCARD",
            Self::Manhattan => "MANHATTAN",
            Self::Minkowski => "MINKOWSKI",
            Self::Pearson => "PEARSON",
        }
    }
}

impl std::fmt::Display for HnswDistanceType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Numeric type for vector components in MTREE/HNSW indexes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum MTreeVectorType {
    /// 64-bit float.
    F64,
    /// 32-bit float.
    F32,
    /// 64-bit integer.
    I64,
    /// 32-bit integer.
    I32,
    /// 16-bit integer.
    I16,
}

impl MTreeVectorType {
    /// Render as SurrealQL keyword.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::F64 => "F64",
            Self::F32 => "F32",
            Self::I64 => "I64",
            Self::I32 => "I32",
            Self::I16 => "I16",
        }
    }
}

impl std::fmt::Display for MTreeVectorType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Immutable index definition describing one or more columns of a table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexDefinition {
    /// Index name.
    pub name: String,
    /// Columns participating in the index.
    pub columns: Vec<String>,
    /// Index kind.
    #[serde(rename = "type", default = "IndexDefinition::default_type")]
    pub index_type: IndexType,
    /// MTREE/HNSW dimension.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub dimension: Option<u32>,
    /// MTREE distance metric.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub distance: Option<MTreeDistanceType>,
    /// MTREE/HNSW vector component type.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub vector_type: Option<MTreeVectorType>,
    /// HNSW-specific distance metric.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub hnsw_distance: Option<HnswDistanceType>,
    /// HNSW exploration factor during construction.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub efc: Option<u32>,
    /// HNSW maximum bidirectional links per node.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub m: Option<u32>,
}

impl IndexDefinition {
    fn default_type() -> IndexType {
        IndexType::Standard
    }

    /// Build a minimal [`IndexDefinition`] with only name and columns.
    pub fn new<I, S>(name: impl Into<String>, columns: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self {
            name: name.into(),
            columns: columns.into_iter().map(Into::into).collect(),
            index_type: IndexType::Standard,
            dimension: None,
            distance: None,
            vector_type: None,
            hnsw_distance: None,
            efc: None,
            m: None,
        }
    }

    /// Set the index kind.
    pub fn with_type(mut self, index_type: IndexType) -> Self {
        self.index_type = index_type;
        self
    }

    /// Validate the index definition.
    ///
    /// Returns [`SurqlError::Validation`] when the name or column list is
    /// empty, or when vector-index fields are missing required members.
    pub fn validate(&self) -> Result<()> {
        if self.name.is_empty() {
            return Err(SurqlError::Validation {
                reason: "Index name cannot be empty".into(),
            });
        }
        if self.columns.is_empty() {
            return Err(SurqlError::Validation {
                reason: format!("Index {:?} must have at least one column", self.name),
            });
        }
        if matches!(self.index_type, IndexType::Mtree | IndexType::Hnsw)
            && self.dimension.is_none()
        {
            return Err(SurqlError::Validation {
                reason: format!("Vector index {:?} requires a dimension", self.name),
            });
        }
        Ok(())
    }

    /// Render the `DEFINE INDEX` statement for this index on the given table.
    pub fn to_surql(&self, table: &str) -> String {
        self.to_surql_with_options(table, false)
    }

    /// Render with optional `IF NOT EXISTS` clause.
    pub fn to_surql_with_options(&self, table: &str, if_not_exists: bool) -> String {
        let ine = if if_not_exists { " IF NOT EXISTS" } else { "" };
        match self.index_type {
            IndexType::Mtree => {
                let field = self.columns.first().map_or("", String::as_str);
                let dim = self.dimension.unwrap_or(0);
                let mut sql = format!(
                    "DEFINE INDEX{ine} {name} ON TABLE {table} COLUMNS {field} MTREE DIMENSION {dim}",
                    ine = ine,
                    name = self.name,
                    table = table,
                    field = field,
                    dim = dim,
                );
                if let Some(d) = self.distance {
                    write!(sql, " DIST {}", d.as_str()).expect("writing to String cannot fail");
                }
                if let Some(vt) = self.vector_type {
                    write!(sql, " TYPE {}", vt.as_str()).expect("writing to String cannot fail");
                }
                sql.push(';');
                sql
            }
            IndexType::Hnsw => {
                let field = self.columns.first().map_or("", String::as_str);
                let dim = self.dimension.unwrap_or(0);
                let mut sql = format!(
                    "DEFINE INDEX{ine} {name} ON TABLE {table} COLUMNS {field} HNSW DIMENSION {dim}",
                    ine = ine,
                    name = self.name,
                    table = table,
                    field = field,
                    dim = dim,
                );
                if let Some(d) = self.hnsw_distance {
                    write!(sql, " DIST {}", d.as_str()).expect("writing to String cannot fail");
                }
                if let Some(vt) = self.vector_type {
                    write!(sql, " TYPE {}", vt.as_str()).expect("writing to String cannot fail");
                }
                if let Some(efc) = self.efc {
                    write!(sql, " EFC {efc}").expect("writing to String cannot fail");
                }
                if let Some(m) = self.m {
                    write!(sql, " M {m}").expect("writing to String cannot fail");
                }
                sql.push(';');
                sql
            }
            _ => {
                let columns = self.columns.join(", ");
                let mut sql = format!(
                    "DEFINE INDEX{ine} {name} ON TABLE {table} COLUMNS {columns}",
                    ine = ine,
                    name = self.name,
                    table = table,
                    columns = columns,
                );
                match self.index_type {
                    IndexType::Unique => sql.push_str(" UNIQUE"),
                    IndexType::Search => sql.push_str(" SEARCH ANALYZER ascii"),
                    _ => {}
                }
                sql.push(';');
                sql
            }
        }
    }
}

/// Immutable event definition (`DEFINE EVENT`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventDefinition {
    /// Event name.
    pub name: String,
    /// SurrealQL `WHEN` condition expression.
    pub condition: String,
    /// SurrealQL `THEN` action.
    pub action: String,
}

impl EventDefinition {
    /// Construct a new [`EventDefinition`].
    pub fn new(
        name: impl Into<String>,
        condition: impl Into<String>,
        action: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            condition: condition.into(),
            action: action.into(),
        }
    }

    /// Validate that the event is not missing required pieces.
    pub fn validate(&self) -> Result<()> {
        if self.name.is_empty() {
            return Err(SurqlError::Validation {
                reason: "Event name cannot be empty".into(),
            });
        }
        if self.condition.is_empty() {
            return Err(SurqlError::Validation {
                reason: format!("Event {:?} must have a condition", self.name),
            });
        }
        if self.action.is_empty() {
            return Err(SurqlError::Validation {
                reason: format!("Event {:?} must have an action", self.name),
            });
        }
        Ok(())
    }

    /// Render the `DEFINE EVENT` statement.
    pub fn to_surql(&self, table: &str) -> String {
        self.to_surql_with_options(table, false)
    }

    /// Render with optional `IF NOT EXISTS` clause.
    pub fn to_surql_with_options(&self, table: &str, if_not_exists: bool) -> String {
        let ine = if if_not_exists { " IF NOT EXISTS" } else { "" };
        format!(
            "DEFINE EVENT{ine} {name} ON TABLE {table} WHEN {cond} THEN {act};",
            ine = ine,
            name = self.name,
            table = table,
            cond = self.condition,
            act = self.action,
        )
    }
}

/// Immutable table schema.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableDefinition {
    /// Table name.
    pub name: String,
    /// Schema mode.
    #[serde(default = "TableDefinition::default_mode")]
    pub mode: TableMode,
    /// Field definitions.
    #[serde(default)]
    pub fields: Vec<FieldDefinition>,
    /// Index definitions.
    #[serde(default)]
    pub indexes: Vec<IndexDefinition>,
    /// Event definitions.
    #[serde(default)]
    pub events: Vec<EventDefinition>,
    /// Per-action permissions map.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub permissions: Option<BTreeMap<String, String>>,
    /// Whether this table is marked for deletion.
    #[serde(default)]
    pub drop: bool,
}

impl TableDefinition {
    fn default_mode() -> TableMode {
        TableMode::Schemafull
    }

    /// Construct a new [`TableDefinition`] in `SCHEMAFULL` mode.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            mode: TableMode::Schemafull,
            fields: Vec::new(),
            indexes: Vec::new(),
            events: Vec::new(),
            permissions: None,
            drop: false,
        }
    }

    /// Set the schema mode.
    pub fn with_mode(mut self, mode: TableMode) -> Self {
        self.mode = mode;
        self
    }

    /// Append field definitions.
    pub fn with_fields<I>(mut self, fields: I) -> Self
    where
        I: IntoIterator<Item = FieldDefinition>,
    {
        self.fields.extend(fields);
        self
    }

    /// Append index definitions.
    pub fn with_indexes<I>(mut self, indexes: I) -> Self
    where
        I: IntoIterator<Item = IndexDefinition>,
    {
        self.indexes.extend(indexes);
        self
    }

    /// Append event definitions.
    pub fn with_events<I>(mut self, events: I) -> Self
    where
        I: IntoIterator<Item = EventDefinition>,
    {
        self.events.extend(events);
        self
    }

    /// Replace per-action permissions.
    pub fn with_permissions<I, K, V>(mut self, permissions: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        self.permissions = Some(
            permissions
                .into_iter()
                .map(|(k, v)| (k.into(), v.into()))
                .collect(),
        );
        self
    }

    /// Mark the table for deletion.
    pub fn with_drop(mut self, drop: bool) -> Self {
        self.drop = drop;
        self
    }

    /// Validate the table and its contained definitions.
    pub fn validate(&self) -> Result<()> {
        if self.name.is_empty() {
            return Err(SurqlError::Validation {
                reason: "Table name cannot be empty".into(),
            });
        }
        for field in &self.fields {
            field.validate()?;
        }
        for index in &self.indexes {
            index.validate()?;
        }
        for event in &self.events {
            event.validate()?;
        }
        Ok(())
    }

    /// Render just the `DEFINE TABLE` statement.
    pub fn to_surql(&self) -> String {
        self.to_surql_with_options(false)
    }

    /// Render the `DEFINE TABLE` statement with optional `IF NOT EXISTS`.
    pub fn to_surql_with_options(&self, if_not_exists: bool) -> String {
        let ine = if if_not_exists { " IF NOT EXISTS" } else { "" };
        format!(
            "DEFINE TABLE{ine} {name} {mode};",
            ine = ine,
            name = self.name,
            mode = self.mode.as_str(),
        )
    }

    /// Render every statement required to create this table.
    ///
    /// Returns the `DEFINE TABLE` line followed by each contained field,
    /// index, event, and permission statement.
    pub fn to_surql_all(&self) -> Vec<String> {
        self.to_surql_all_with_options(false)
    }

    /// Render every statement with optional `IF NOT EXISTS`.
    pub fn to_surql_all_with_options(&self, if_not_exists: bool) -> Vec<String> {
        let mut out =
            Vec::with_capacity(1 + self.fields.len() + self.indexes.len() + self.events.len());
        out.push(self.to_surql_with_options(if_not_exists));
        for field in &self.fields {
            out.push(field.to_surql_with_options(&self.name, if_not_exists));
        }
        for index in &self.indexes {
            out.push(index.to_surql_with_options(&self.name, if_not_exists));
        }
        for event in &self.events {
            out.push(event.to_surql_with_options(&self.name, if_not_exists));
        }
        if let Some(perms) = &self.permissions {
            for (action, rule) in perms {
                out.push(format!(
                    "DEFINE FIELD PERMISSIONS FOR {action} ON TABLE {name} WHERE {rule};",
                    action = action.to_uppercase(),
                    name = self.name,
                    rule = rule,
                ));
            }
        }
        out
    }
}

/// Functional constructor mirroring `surql.schema.table.table_schema`.
pub fn table_schema(name: impl Into<String>) -> TableDefinition {
    TableDefinition::new(name)
}

/// Build a standard index.
pub fn index<I, S>(name: impl Into<String>, columns: I) -> IndexDefinition
where
    I: IntoIterator<Item = S>,
    S: Into<String>,
{
    IndexDefinition::new(name, columns)
}

/// Build a `UNIQUE` index.
pub fn unique_index<I, S>(name: impl Into<String>, columns: I) -> IndexDefinition
where
    I: IntoIterator<Item = S>,
    S: Into<String>,
{
    IndexDefinition::new(name, columns).with_type(IndexType::Unique)
}

/// Build a full-text `SEARCH` index.
pub fn search_index<I, S>(name: impl Into<String>, columns: I) -> IndexDefinition
where
    I: IntoIterator<Item = S>,
    S: Into<String>,
{
    IndexDefinition::new(name, columns).with_type(IndexType::Search)
}

/// Build an MTREE vector index.
pub fn mtree_index(
    name: impl Into<String>,
    column: impl Into<String>,
    dimension: u32,
    distance: MTreeDistanceType,
    vector_type: MTreeVectorType,
) -> IndexDefinition {
    IndexDefinition {
        name: name.into(),
        columns: vec![column.into()],
        index_type: IndexType::Mtree,
        dimension: Some(dimension),
        distance: Some(distance),
        vector_type: Some(vector_type),
        hnsw_distance: None,
        efc: None,
        m: None,
    }
}

/// Build an HNSW vector index.
///
/// `efc` and `m` are optional tuning parameters; when omitted, the server
/// defaults are used.
pub fn hnsw_index(
    name: impl Into<String>,
    column: impl Into<String>,
    dimension: u32,
    distance: HnswDistanceType,
    vector_type: MTreeVectorType,
    efc: Option<u32>,
    m: Option<u32>,
) -> IndexDefinition {
    IndexDefinition {
        name: name.into(),
        columns: vec![column.into()],
        index_type: IndexType::Hnsw,
        dimension: Some(dimension),
        distance: None,
        vector_type: Some(vector_type),
        hnsw_distance: Some(distance),
        efc,
        m,
    }
}

/// Build an [`EventDefinition`].
pub fn event(
    name: impl Into<String>,
    condition: impl Into<String>,
    action: impl Into<String>,
) -> EventDefinition {
    EventDefinition::new(name, condition, action)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::fields::{int_field, string_field};

    #[test]
    fn table_mode_strings() {
        assert_eq!(TableMode::Schemafull.as_str(), "SCHEMAFULL");
        assert_eq!(TableMode::Schemaless.as_str(), "SCHEMALESS");
        assert_eq!(TableMode::Drop.as_str(), "DROP");
    }

    #[test]
    fn table_mode_display() {
        assert_eq!(format!("{}", TableMode::Schemafull), "SCHEMAFULL");
    }

    #[test]
    fn table_mode_serializes_uppercase() {
        let json = serde_json::to_string(&TableMode::Schemaless).unwrap();
        assert_eq!(json, "\"SCHEMALESS\"");
    }

    #[test]
    fn index_type_strings() {
        assert_eq!(IndexType::Unique.as_str(), "UNIQUE");
        assert_eq!(IndexType::Standard.as_str(), "INDEX");
        assert_eq!(IndexType::Mtree.as_str(), "MTREE");
        assert_eq!(IndexType::Hnsw.as_str(), "HNSW");
    }

    #[test]
    fn mtree_distance_display() {
        assert_eq!(format!("{}", MTreeDistanceType::Cosine), "COSINE");
    }

    #[test]
    fn hnsw_distance_display() {
        assert_eq!(format!("{}", HnswDistanceType::Chebyshev), "CHEBYSHEV");
    }

    #[test]
    fn mtree_vector_type_display() {
        assert_eq!(format!("{}", MTreeVectorType::F32), "F32");
    }

    #[test]
    fn table_to_surql_schemafull() {
        let t = table_schema("user");
        assert_eq!(t.to_surql(), "DEFINE TABLE user SCHEMAFULL;");
    }

    #[test]
    fn table_to_surql_schemaless() {
        let t = table_schema("log").with_mode(TableMode::Schemaless);
        assert_eq!(t.to_surql(), "DEFINE TABLE log SCHEMALESS;");
    }

    #[test]
    fn table_to_surql_if_not_exists() {
        let t = table_schema("user");
        assert_eq!(
            t.to_surql_with_options(true),
            "DEFINE TABLE IF NOT EXISTS user SCHEMAFULL;"
        );
    }

    #[test]
    fn table_to_surql_all_includes_fields() {
        let t = table_schema("user").with_fields([
            string_field("name").build_unchecked().unwrap(),
            int_field("age").build_unchecked().unwrap(),
        ]);
        let stmts = t.to_surql_all();
        assert_eq!(stmts[0], "DEFINE TABLE user SCHEMAFULL;");
        assert!(stmts
            .iter()
            .any(|s| s.contains("DEFINE FIELD name ON TABLE user TYPE string")));
        assert!(stmts
            .iter()
            .any(|s| s.contains("DEFINE FIELD age ON TABLE user TYPE int")));
    }

    #[test]
    fn table_to_surql_all_includes_unique_index() {
        let t = table_schema("user").with_indexes([unique_index("email_idx", ["email"])]);
        let stmts = t.to_surql_all();
        assert!(stmts
            .iter()
            .any(|s| s == "DEFINE INDEX email_idx ON TABLE user COLUMNS email UNIQUE;"));
    }

    #[test]
    fn table_to_surql_all_includes_event() {
        let t = table_schema("user").with_events([event(
            "email_changed",
            "$before.email != $after.email",
            "CREATE audit_log",
        )]);
        let stmts = t.to_surql_all();
        assert!(stmts
            .iter()
            .any(|s| s.starts_with("DEFINE EVENT email_changed ON TABLE user")));
    }

    #[test]
    fn table_permissions_render_upper() {
        let t = table_schema("user").with_permissions([("select", "$auth.id = id")]);
        let stmts = t.to_surql_all();
        assert!(stmts
            .iter()
            .any(|s| s.contains("FOR SELECT") && s.contains("$auth.id = id")));
    }

    #[test]
    fn index_new_defaults_to_standard() {
        let idx = index("title_idx", ["title"]);
        assert_eq!(idx.index_type, IndexType::Standard);
    }

    #[test]
    fn unique_index_to_surql() {
        let idx = unique_index("email_idx", ["email"]);
        assert_eq!(
            idx.to_surql("user"),
            "DEFINE INDEX email_idx ON TABLE user COLUMNS email UNIQUE;"
        );
    }

    #[test]
    fn standard_index_to_surql() {
        let idx = index("title_idx", ["title"]);
        assert_eq!(
            idx.to_surql("post"),
            "DEFINE INDEX title_idx ON TABLE post COLUMNS title;"
        );
    }

    #[test]
    fn search_index_to_surql() {
        let idx = search_index("content_search", ["title", "content"]);
        assert_eq!(
            idx.to_surql("post"),
            "DEFINE INDEX content_search ON TABLE post COLUMNS title, content SEARCH ANALYZER ascii;"
        );
    }

    #[test]
    fn mtree_index_to_surql() {
        let idx = mtree_index(
            "embedding_idx",
            "embedding",
            1536,
            MTreeDistanceType::Cosine,
            MTreeVectorType::F32,
        );
        let sql = idx.to_surql("doc");
        assert!(sql.contains(
            "DEFINE INDEX embedding_idx ON TABLE doc COLUMNS embedding MTREE DIMENSION 1536"
        ));
        assert!(sql.contains("DIST COSINE"));
        assert!(sql.contains("TYPE F32"));
    }

    #[test]
    fn hnsw_index_to_surql_with_efc_m() {
        let idx = hnsw_index(
            "feat_idx",
            "features",
            128,
            HnswDistanceType::Cosine,
            MTreeVectorType::F32,
            Some(500),
            Some(16),
        );
        let sql = idx.to_surql("doc");
        assert!(sql.contains("HNSW DIMENSION 128"));
        assert!(sql.contains("DIST COSINE"));
        assert!(sql.contains("TYPE F32"));
        assert!(sql.contains("EFC 500"));
        assert!(sql.contains("M 16"));
    }

    #[test]
    fn hnsw_index_without_efc_m_omits_them() {
        let idx = hnsw_index(
            "feat_idx",
            "features",
            64,
            HnswDistanceType::Euclidean,
            MTreeVectorType::F64,
            None,
            None,
        );
        let sql = idx.to_surql("doc");
        assert!(!sql.contains("EFC"));
        assert!(!sql.contains("M 12"));
    }

    #[test]
    fn index_to_surql_if_not_exists() {
        let idx = unique_index("email_idx", ["email"]);
        assert_eq!(
            idx.to_surql_with_options("user", true),
            "DEFINE INDEX IF NOT EXISTS email_idx ON TABLE user COLUMNS email UNIQUE;"
        );
    }

    #[test]
    fn event_to_surql() {
        let ev = event(
            "email_changed",
            "$before.email != $after.email",
            "CREATE audit_log SET user = $value.id",
        );
        assert_eq!(
            ev.to_surql("user"),
            "DEFINE EVENT email_changed ON TABLE user WHEN $before.email != $after.email \
             THEN CREATE audit_log SET user = $value.id;"
        );
    }

    #[test]
    fn event_to_surql_if_not_exists() {
        let ev = event("n", "true", "do");
        assert!(ev
            .to_surql_with_options("t", true)
            .starts_with("DEFINE EVENT IF NOT EXISTS n ON TABLE t"));
    }

    #[test]
    fn event_validate_rejects_empty() {
        assert!(event("", "c", "a").validate().is_err());
        assert!(event("n", "", "a").validate().is_err());
        assert!(event("n", "c", "").validate().is_err());
    }

    #[test]
    fn index_validate_rejects_empty_name() {
        let mut idx = unique_index("x", ["a"]);
        idx.name = String::new();
        assert!(idx.validate().is_err());
    }

    #[test]
    fn index_validate_rejects_empty_columns() {
        let idx = IndexDefinition::new("x", Vec::<String>::new()).with_type(IndexType::Unique);
        assert!(idx.validate().is_err());
    }

    #[test]
    fn index_validate_mtree_requires_dimension() {
        let mut idx = IndexDefinition::new("x", ["v"]).with_type(IndexType::Mtree);
        assert!(idx.validate().is_err());
        idx.dimension = Some(64);
        assert!(idx.validate().is_ok());
    }

    #[test]
    fn index_validate_hnsw_requires_dimension() {
        let idx = IndexDefinition::new("x", ["v"]).with_type(IndexType::Hnsw);
        assert!(idx.validate().is_err());
    }

    #[test]
    fn table_validate_rejects_empty_name() {
        assert!(table_schema("").validate().is_err());
    }

    #[test]
    fn table_validate_propagates_field_errors() {
        let t = table_schema("user").with_fields([FieldDefinition::new(
            "1bad",
            crate::schema::fields::FieldType::String,
        )]);
        assert!(t.validate().is_err());
    }

    #[test]
    fn table_statement_order_defines_table_first() {
        let t = table_schema("user")
            .with_fields([string_field("name").build_unchecked().unwrap()])
            .with_indexes([unique_index("name_idx", ["name"])]);
        let stmts = t.to_surql_all();
        assert!(stmts[0].starts_with("DEFINE TABLE"));
    }

    #[test]
    fn minimal_table_returns_single_statement() {
        let t = table_schema("empty");
        assert_eq!(t.to_surql_all().len(), 1);
    }

    #[test]
    fn table_definition_clone_eq() {
        let t1 = table_schema("user").with_mode(TableMode::Schemafull);
        let t2 = t1.clone();
        assert_eq!(t1, t2);
    }
}
