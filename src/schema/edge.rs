//! Edge (graph relationship) schema definitions.
//!
//! Port of `surql/schema/edge.py`. Provides the [`EdgeDefinition`] value
//! object plus the [`EdgeMode`] enum and helper constructors.
//!
//! Two modes are supported:
//!
//! - [`EdgeMode::Relation`] — modern `DEFINE TABLE ... TYPE RELATION` edges
//!   with automatic `in`/`out` fields.
//! - [`EdgeMode::Schemafull`] / [`EdgeMode::Schemaless`] — traditional table
//!   layout with explicit `in`/`out` record fields.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::error::{Result, SurqlError};

use super::fields::FieldDefinition;
use super::table::{EventDefinition, IndexDefinition};

/// Edge table mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum EdgeMode {
    /// Modern `TYPE RELATION` edges with automatic `in` / `out` fields.
    Relation,
    /// Traditional schemafull layout with explicit `in` / `out` fields.
    Schemafull,
    /// Flexible layout.
    Schemaless,
}

impl EdgeMode {
    /// Render as SurrealQL keyword.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Relation => "RELATION",
            Self::Schemafull => "SCHEMAFULL",
            Self::Schemaless => "SCHEMALESS",
        }
    }
}

impl std::fmt::Display for EdgeMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Immutable edge (graph relationship) definition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EdgeDefinition {
    /// Edge table name.
    pub name: String,
    /// Edge mode.
    #[serde(default = "EdgeDefinition::default_mode")]
    pub mode: EdgeMode,
    /// Source table constraint for `RELATION` mode.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub from_table: Option<String>,
    /// Target table constraint for `RELATION` mode.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub to_table: Option<String>,
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
}

impl EdgeDefinition {
    fn default_mode() -> EdgeMode {
        EdgeMode::Relation
    }

    /// Construct a new [`EdgeDefinition`] in `RELATION` mode.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            mode: EdgeMode::Relation,
            from_table: None,
            to_table: None,
            fields: Vec::new(),
            indexes: Vec::new(),
            events: Vec::new(),
            permissions: None,
        }
    }

    /// Set the edge mode.
    pub fn with_mode(mut self, mode: EdgeMode) -> Self {
        self.mode = mode;
        self
    }

    /// Set the source-table constraint.
    pub fn with_from_table(mut self, from_table: impl Into<String>) -> Self {
        self.from_table = Some(from_table.into());
        self
    }

    /// Set the target-table constraint.
    pub fn with_to_table(mut self, to_table: impl Into<String>) -> Self {
        self.to_table = Some(to_table.into());
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

    /// Validate the edge.
    ///
    /// Returns [`SurqlError::Validation`] for an empty name, missing
    /// `from_table`/`to_table` in `RELATION` mode, or invalid fields.
    pub fn validate(&self) -> Result<()> {
        if self.name.is_empty() {
            return Err(SurqlError::Validation {
                reason: "Edge name cannot be empty".into(),
            });
        }
        if self.mode == EdgeMode::Relation && (self.from_table.is_none() || self.to_table.is_none())
        {
            return Err(SurqlError::Validation {
                reason: format!(
                    "Edge {:?} with RELATION mode requires both from_table and to_table",
                    self.name
                ),
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

    /// Render the `DEFINE TABLE` statement for the edge.
    pub fn to_surql(&self) -> Result<String> {
        self.to_surql_with_options(false)
    }

    /// Render the `DEFINE TABLE` statement with optional `IF NOT EXISTS`.
    pub fn to_surql_with_options(&self, if_not_exists: bool) -> Result<String> {
        let ine = if if_not_exists { " IF NOT EXISTS" } else { "" };
        match self.mode {
            EdgeMode::Relation => {
                let from = self
                    .from_table
                    .as_deref()
                    .ok_or_else(|| SurqlError::Validation {
                        reason: format!(
                            "Edge {:?} with RELATION mode requires both from_table and to_table",
                            self.name
                        ),
                    })?;
                let to = self
                    .to_table
                    .as_deref()
                    .ok_or_else(|| SurqlError::Validation {
                        reason: format!(
                            "Edge {:?} with RELATION mode requires both from_table and to_table",
                            self.name
                        ),
                    })?;
                Ok(format!(
                    "DEFINE TABLE{ine} {name} TYPE RELATION FROM {from} TO {to};",
                    ine = ine,
                    name = self.name,
                    from = from,
                    to = to,
                ))
            }
            EdgeMode::Schemafull => Ok(format!(
                "DEFINE TABLE{ine} {name} SCHEMAFULL;",
                ine = ine,
                name = self.name,
            )),
            EdgeMode::Schemaless => Ok(format!(
                "DEFINE TABLE{ine} {name} SCHEMALESS;",
                ine = ine,
                name = self.name,
            )),
        }
    }

    /// Render every statement required to create this edge.
    pub fn to_surql_all(&self) -> Result<Vec<String>> {
        self.to_surql_all_with_options(false)
    }

    /// Render every statement with optional `IF NOT EXISTS`.
    pub fn to_surql_all_with_options(&self, if_not_exists: bool) -> Result<Vec<String>> {
        let mut out =
            Vec::with_capacity(1 + self.fields.len() + self.indexes.len() + self.events.len());
        out.push(self.to_surql_with_options(if_not_exists)?);
        for field in &self.fields {
            out.push(field.to_surql_with_options(&self.name, if_not_exists));
        }
        for index in &self.indexes {
            out.push(index.to_surql_with_options(&self.name, if_not_exists));
        }
        for event in &self.events {
            out.push(event.to_surql_with_options(&self.name, if_not_exists));
        }
        Ok(out)
    }
}

/// Functional constructor mirroring `surql.schema.edge.edge_schema`.
pub fn edge_schema(name: impl Into<String>) -> EdgeDefinition {
    EdgeDefinition::new(name)
}

/// Convenience constructor for a typed edge with specific source and target tables.
pub fn typed_edge(
    name: impl Into<String>,
    from_table: impl Into<String>,
    to_table: impl Into<String>,
) -> EdgeDefinition {
    EdgeDefinition::new(name)
        .with_from_table(from_table)
        .with_to_table(to_table)
}

/// Convenience constructor for a bidirectional (self-referential) edge.
pub fn bidirectional_edge(name: impl Into<String>, table: impl Into<String>) -> EdgeDefinition {
    let table = table.into();
    EdgeDefinition::new(name)
        .with_from_table(table.clone())
        .with_to_table(table)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::fields::{datetime_field, int_field};

    #[test]
    fn edge_mode_strings() {
        assert_eq!(EdgeMode::Relation.as_str(), "RELATION");
        assert_eq!(EdgeMode::Schemafull.as_str(), "SCHEMAFULL");
        assert_eq!(EdgeMode::Schemaless.as_str(), "SCHEMALESS");
    }

    #[test]
    fn edge_mode_display() {
        assert_eq!(format!("{}", EdgeMode::Relation), "RELATION");
    }

    #[test]
    fn edge_mode_serializes_uppercase() {
        let json = serde_json::to_string(&EdgeMode::Schemafull).unwrap();
        assert_eq!(json, "\"SCHEMAFULL\"");
    }

    #[test]
    fn typed_edge_renders_relation() {
        let e = typed_edge("likes", "user", "post");
        assert_eq!(
            e.to_surql().unwrap(),
            "DEFINE TABLE likes TYPE RELATION FROM user TO post;"
        );
    }

    #[test]
    fn bidirectional_edge_uses_same_table() {
        let e = bidirectional_edge("follows", "user");
        assert_eq!(
            e.to_surql().unwrap(),
            "DEFINE TABLE follows TYPE RELATION FROM user TO user;"
        );
    }

    #[test]
    fn schemafull_edge_renders() {
        let e = edge_schema("entity_relation").with_mode(EdgeMode::Schemafull);
        assert_eq!(
            e.to_surql().unwrap(),
            "DEFINE TABLE entity_relation SCHEMAFULL;"
        );
    }

    #[test]
    fn schemaless_edge_renders() {
        let e = edge_schema("loose").with_mode(EdgeMode::Schemaless);
        assert_eq!(e.to_surql().unwrap(), "DEFINE TABLE loose SCHEMALESS;");
    }

    #[test]
    fn relation_edge_missing_from_is_error() {
        let e = edge_schema("likes").with_to_table("post");
        assert!(e.to_surql().is_err());
    }

    #[test]
    fn relation_edge_missing_to_is_error() {
        let e = edge_schema("likes").with_from_table("user");
        assert!(e.to_surql().is_err());
    }

    #[test]
    fn relation_edge_missing_both_is_error() {
        let e = edge_schema("likes");
        assert!(e.to_surql().is_err());
    }

    #[test]
    fn schemafull_edge_does_not_require_tables() {
        let e = edge_schema("r").with_mode(EdgeMode::Schemafull);
        assert!(e.to_surql().is_ok());
    }

    #[test]
    fn edge_to_surql_all_includes_fields() {
        let e = typed_edge("likes", "user", "post").with_fields([
            datetime_field("created_at")
                .default("time::now()")
                .build_unchecked()
                .unwrap(),
            int_field("weight").default("1").build_unchecked().unwrap(),
        ]);
        let stmts = e.to_surql_all().unwrap();
        assert_eq!(
            stmts[0],
            "DEFINE TABLE likes TYPE RELATION FROM user TO post;"
        );
        assert!(stmts
            .iter()
            .any(|s| s.contains("DEFINE FIELD created_at ON TABLE likes TYPE datetime")));
        assert!(stmts
            .iter()
            .any(|s| s.contains("DEFINE FIELD weight ON TABLE likes TYPE int")));
    }

    #[test]
    fn edge_to_surql_if_not_exists() {
        let e = typed_edge("likes", "user", "post");
        assert_eq!(
            e.to_surql_with_options(true).unwrap(),
            "DEFINE TABLE IF NOT EXISTS likes TYPE RELATION FROM user TO post;"
        );
    }

    #[test]
    fn edge_validate_rejects_empty_name() {
        assert!(edge_schema("").validate().is_err());
    }

    #[test]
    fn edge_validate_requires_relation_endpoints() {
        assert!(edge_schema("likes").validate().is_err());
        assert!(typed_edge("likes", "user", "post").validate().is_ok());
    }

    #[test]
    fn edge_validate_propagates_field_errors() {
        let e = typed_edge("likes", "user", "post").with_fields([FieldDefinition::new(
            "1bad",
            crate::schema::fields::FieldType::String,
        )]);
        assert!(e.validate().is_err());
    }

    #[test]
    fn edge_statement_order_table_first() {
        let e = typed_edge("follows", "user", "user").with_fields([datetime_field("since")
            .default("time::now()")
            .build_unchecked()
            .unwrap()]);
        let stmts = e.to_surql_all().unwrap();
        assert!(stmts[0].starts_with("DEFINE TABLE"));
    }

    #[test]
    fn edge_permissions_setter() {
        let e =
            typed_edge("follows", "user", "user").with_permissions([("create", "$auth.id = in")]);
        assert!(e.permissions.as_ref().unwrap().contains_key("create"));
    }

    #[test]
    fn edge_clone_eq() {
        let e1 = typed_edge("likes", "user", "post");
        let e2 = e1.clone();
        assert_eq!(e1, e2);
    }
}
