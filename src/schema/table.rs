//! Table schema definitions.
//!
//! Port of `surql/schema/table.py`. Exposes the [`TableDefinition`] value
//! object together with [`TableMode`] and [`EventDefinition`]. Each
//! definition renders the corresponding `DEFINE` statement via `to_surql`.
//!
//! `DEFINE INDEX` lives in [`super::index`] and is re-exported here, so
//! `schema::table::IndexDefinition` and friends keep resolving.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::error::{Result, SurqlError};

use super::changefeed::ChangeFeed;
use super::fields::FieldDefinition;

pub use super::index::{
    bm25_index, hnsw_index, index, mtree_index, search_index, unique_index, HnswDistanceType,
    IndexDefinition, IndexType, MTreeDistanceType, MTreeVectorType,
};

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
        self.render_guard(table, if if_not_exists { " IF NOT EXISTS" } else { "" })
    }

    /// Render with `OVERWRITE`, replacing an existing definition while
    /// leaving stored data untouched. What schema evolution applies
    /// when a stored definition no longer matches the code.
    pub fn to_surql_overwrite(&self, table: &str) -> String {
        self.render_guard(table, " OVERWRITE")
    }

    fn render_guard(&self, table: &str, ine: &str) -> String {
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
    /// Mutation-log retention (`CHANGEFEED <duration> [INCLUDE ORIGINAL]`),
    /// read back with [`crate::query::changes`].
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub changefeed: Option<ChangeFeed>,
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
            changefeed: None,
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

    /// Retain a mutation log for this table (`CHANGEFEED <duration>`).
    pub fn with_changefeed(mut self, changefeed: ChangeFeed) -> Self {
        self.changefeed = Some(changefeed);
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
    ///
    /// Table-level `PERMISSIONS` are rendered as part of this statement
    /// (`... PERMISSIONS FOR select WHERE ... FOR create WHERE ...`), which is
    /// the only valid placement for table permissions in SurrealQL.
    pub fn to_surql_with_options(&self, if_not_exists: bool) -> String {
        self.render_guard(if if_not_exists { " IF NOT EXISTS" } else { "" })
    }

    /// Render with `OVERWRITE`, replacing an existing definition while
    /// leaving stored data untouched. What schema evolution applies
    /// when a stored definition no longer matches the code.
    pub fn to_surql_overwrite(&self) -> String {
        self.render_guard(" OVERWRITE")
    }

    fn render_guard(&self, ine: &str) -> String {
        let perms = match &self.permissions {
            Some(perms) if !perms.is_empty() => {
                let clauses: Vec<String> = perms
                    .iter()
                    .map(|(action, rule)| format!("FOR {action} WHERE {rule}"))
                    .collect();
                format!(" PERMISSIONS {}", clauses.join(" "))
            }
            _ => String::new(),
        };
        // SurrealQL order: mode, then CHANGEFEED, then PERMISSIONS.
        let changefeed = self
            .changefeed
            .as_ref()
            .map(ChangeFeed::to_clause)
            .unwrap_or_default();
        format!(
            "DEFINE TABLE{ine} {name} {mode}{changefeed}{perms};",
            ine = ine,
            name = self.name,
            mode = self.mode.as_str(),
            changefeed = changefeed,
            perms = perms,
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
        // Table-level PERMISSIONS are rendered inline on the `DEFINE TABLE`
        // statement (see `to_surql_with_options`), not as separate statements.
        out
    }
}

/// Functional constructor mirroring `surql.schema.table.table_schema`.
pub fn table_schema(name: impl Into<String>) -> TableDefinition {
    TableDefinition::new(name)
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
    fn table_permissions_render_on_define_table() {
        let t = table_schema("user")
            .with_permissions([("select", "$auth.id = id"), ("create", "$auth.id = id")]);
        let define = t.to_surql_with_options(false);
        // Permissions live inside the DEFINE TABLE statement, not a bogus
        // `DEFINE FIELD PERMISSIONS ...` line.
        assert!(define.starts_with("DEFINE TABLE user"));
        assert!(define.contains("PERMISSIONS FOR"));
        assert!(define.contains("FOR select WHERE $auth.id = id"));
        assert!(define.contains("FOR create WHERE $auth.id = id"));
        assert!(define.ends_with(';'));
        // And there is no separate malformed statement.
        let stmts = t.to_surql_all();
        assert!(!stmts.iter().any(|s| s.contains("DEFINE FIELD PERMISSIONS")));
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
