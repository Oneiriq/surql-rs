//! Migration data models and types.
//!
//! Port of `surql/migration/models.py`. This module defines the core data
//! structures for the migration system, including migration metadata,
//! state tracking, and execution plans.
//!
//! ## Deviation from Python
//!
//! In Python, a [`Migration`] embeds two `Callable[[], list[str]]` objects
//! (`up` / `down`) loaded dynamically from a `.py` file via `importlib`.
//! Rust cannot execute arbitrary Python at runtime, so migrations are
//! represented as pure data: both [`Migration::up`] and [`Migration::down`]
//! are `Vec<String>` of SurrealQL statements parsed from the migration
//! file (see [`crate::migration::discovery`] for the file format).

use std::collections::BTreeMap;
use std::path::PathBuf;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// State of a migration in the execution lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MigrationState {
    /// The migration has not yet been applied.
    Pending,
    /// The migration has been applied successfully.
    Applied,
    /// The migration failed while being applied.
    Failed,
}

impl MigrationState {
    /// Render the state as a lowercase string (matches Python `.value`).
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Applied => "applied",
            Self::Failed => "failed",
        }
    }
}

impl std::fmt::Display for MigrationState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Direction of migration execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MigrationDirection {
    /// Forward migration (apply).
    Up,
    /// Backward migration (rollback).
    Down,
}

impl MigrationDirection {
    /// Render the direction as a lowercase string.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Up => "up",
            Self::Down => "down",
        }
    }
}

impl std::fmt::Display for MigrationDirection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Immutable migration definition.
///
/// Represents a single migration file with its metadata and SurrealQL
/// statements for both the forward (`up`) and backward (`down`) directions.
///
/// Unlike the Python original, the `up` and `down` directions are stored
/// as plain `Vec<String>` (pre-parsed SurrealQL statements) rather than
/// callables, because migration files in the Rust port are flat text
/// (see the module-level deviation note).
///
/// ## Examples
///
/// ```
/// use std::path::PathBuf;
/// use surql::migration::Migration;
///
/// let m = Migration {
///     version: "20260102_120000".into(),
///     description: "Create user table".into(),
///     path: PathBuf::from("migrations/20260102_120000_create_user.surql"),
///     up: vec!["DEFINE TABLE user SCHEMAFULL;".into()],
///     down: vec!["REMOVE TABLE user;".into()],
///     checksum: Some("abc123".into()),
///     depends_on: vec![],
/// };
/// assert_eq!(m.version, "20260102_120000");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Migration {
    /// Migration version (timestamp-based, e.g. `YYYYMMDD_HHMMSS`).
    pub version: String,
    /// Human-readable description of what the migration does.
    pub description: String,
    /// Path to the migration file on disk.
    pub path: PathBuf,
    /// SurrealQL statements for the forward (`up`) direction.
    pub up: Vec<String>,
    /// SurrealQL statements for the backward (`down`) direction.
    pub down: Vec<String>,
    /// Content checksum (SHA-256 hex) of the source file, if computed.
    pub checksum: Option<String>,
    /// Versions of other migrations this migration depends on.
    #[serde(default)]
    pub depends_on: Vec<String>,
}

/// Migration history record stored in the database.
///
/// Represents a migration that has been applied to the database.
///
/// ## Examples
///
/// ```
/// use chrono::Utc;
/// use surql::migration::MigrationHistory;
///
/// let h = MigrationHistory {
///     version: "20260102_120000".into(),
///     description: "Create user table".into(),
///     applied_at: Utc::now(),
///     checksum: "abc123".into(),
///     execution_time_ms: Some(42),
/// };
/// assert_eq!(h.version, "20260102_120000");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MigrationHistory {
    /// Migration version.
    pub version: String,
    /// Migration description.
    pub description: String,
    /// Timestamp at which the migration was applied.
    pub applied_at: DateTime<Utc>,
    /// Content checksum (SHA-256 hex) captured at apply time.
    pub checksum: String,
    /// Wall-clock execution time in milliseconds, if measured.
    pub execution_time_ms: Option<u64>,
}

/// Execution plan for a set of migrations.
///
/// Represents the ordered list of migrations to execute and their direction.
///
/// ## Examples
///
/// ```
/// use surql::migration::{MigrationDirection, MigrationPlan};
///
/// let plan = MigrationPlan {
///     migrations: vec![],
///     direction: MigrationDirection::Up,
/// };
/// assert!(plan.is_empty());
/// assert_eq!(plan.count(), 0);
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MigrationPlan {
    /// Ordered list of migrations to execute (sorted by version).
    pub migrations: Vec<Migration>,
    /// Execution direction (up or down).
    pub direction: MigrationDirection,
}

impl MigrationPlan {
    /// Number of migrations in the plan.
    pub fn count(&self) -> usize {
        self.migrations.len()
    }

    /// `true` if the plan has no migrations.
    pub fn is_empty(&self) -> bool {
        self.migrations.is_empty()
    }
}

/// Metadata for a migration file.
///
/// This is the data structure expected in the `-- @metadata` section
/// of a migration file.
///
/// ## Examples
///
/// ```
/// use surql::migration::MigrationMetadata;
///
/// let meta = MigrationMetadata {
///     version: "20260102_120000".into(),
///     description: "Create user table".into(),
///     author: MigrationMetadata::default_author(),
///     depends_on: vec![],
/// };
/// assert_eq!(meta.author, "surql");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MigrationMetadata {
    /// Migration version.
    pub version: String,
    /// Human-readable description.
    pub description: String,
    /// Author string (defaults to `"surql"`).
    #[serde(default = "MigrationMetadata::default_author")]
    pub author: String,
    /// Versions of other migrations this one depends on.
    #[serde(default)]
    pub depends_on: Vec<String>,
}

impl MigrationMetadata {
    /// Default author string used when the migration file omits `author`.
    pub fn default_author() -> String {
        "surql".to_string()
    }
}

/// Status information for a migration.
///
/// Combines a migration definition with its current runtime state.
///
/// ## Examples
///
/// ```
/// use std::path::PathBuf;
/// use surql::migration::{Migration, MigrationState, MigrationStatus};
///
/// let m = Migration {
///     version: "v1".into(),
///     description: "demo".into(),
///     path: PathBuf::from("v1.surql"),
///     up: vec![],
///     down: vec![],
///     checksum: None,
///     depends_on: vec![],
/// };
/// let s = MigrationStatus {
///     migration: m,
///     state: MigrationState::Pending,
///     applied_at: None,
///     error: None,
/// };
/// assert_eq!(s.state, MigrationState::Pending);
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MigrationStatus {
    /// Underlying migration definition.
    pub migration: Migration,
    /// Current state of the migration.
    pub state: MigrationState,
    /// Timestamp at which the migration was applied, if any.
    pub applied_at: Option<DateTime<Utc>>,
    /// Error message describing the failure, if any.
    pub error: Option<String>,
}

/// Type of schema change operation captured by a [`SchemaDiff`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DiffOperation {
    /// A new table was added.
    AddTable,
    /// An existing table was removed.
    DropTable,
    /// A new field was added to an existing table.
    AddField,
    /// An existing field was removed.
    DropField,
    /// An existing field had its type or constraints changed.
    ModifyField,
    /// A new index was added.
    AddIndex,
    /// An existing index was removed.
    DropIndex,
    /// A new event was added.
    AddEvent,
    /// An existing event was removed.
    DropEvent,
    /// Permissions were modified on a table or field.
    ModifyPermissions,
}

impl DiffOperation {
    /// Render the operation as its snake-case string form.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::AddTable => "add_table",
            Self::DropTable => "drop_table",
            Self::AddField => "add_field",
            Self::DropField => "drop_field",
            Self::ModifyField => "modify_field",
            Self::AddIndex => "add_index",
            Self::DropIndex => "drop_index",
            Self::AddEvent => "add_event",
            Self::DropEvent => "drop_event",
            Self::ModifyPermissions => "modify_permissions",
        }
    }
}

impl std::fmt::Display for DiffOperation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Represents a difference between two schema versions.
///
/// ## Examples
///
/// ```
/// use surql::migration::{DiffOperation, SchemaDiff};
///
/// let diff = SchemaDiff {
///     operation: DiffOperation::AddTable,
///     table: "user".into(),
///     field: None,
///     index: None,
///     event: None,
///     description: "Add user table".into(),
///     forward_sql: "DEFINE TABLE user SCHEMAFULL;".into(),
///     backward_sql: "REMOVE TABLE user;".into(),
///     details: Default::default(),
/// };
/// assert_eq!(diff.operation, DiffOperation::AddTable);
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaDiff {
    /// The kind of schema change.
    pub operation: DiffOperation,
    /// Table name affected by the change.
    pub table: String,
    /// Field name, if the change targets a field.
    pub field: Option<String>,
    /// Index name, if the change targets an index.
    pub index: Option<String>,
    /// Event name, if the change targets an event.
    pub event: Option<String>,
    /// Human-readable description.
    pub description: String,
    /// SurrealQL that applies the change (forward).
    pub forward_sql: String,
    /// SurrealQL that reverts the change (backward).
    pub backward_sql: String,
    /// Extra operation-specific details.
    #[serde(default)]
    pub details: BTreeMap<String, serde_json::Value>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn migration_state_as_str_values() {
        assert_eq!(MigrationState::Pending.as_str(), "pending");
        assert_eq!(MigrationState::Applied.as_str(), "applied");
        assert_eq!(MigrationState::Failed.as_str(), "failed");
    }

    #[test]
    fn migration_state_display_matches_as_str() {
        assert_eq!(MigrationState::Pending.to_string(), "pending");
        assert_eq!(MigrationState::Applied.to_string(), "applied");
        assert_eq!(MigrationState::Failed.to_string(), "failed");
    }

    #[test]
    fn migration_direction_as_str_values() {
        assert_eq!(MigrationDirection::Up.as_str(), "up");
        assert_eq!(MigrationDirection::Down.as_str(), "down");
    }

    #[test]
    fn migration_direction_display_matches_as_str() {
        assert_eq!(MigrationDirection::Up.to_string(), "up");
        assert_eq!(MigrationDirection::Down.to_string(), "down");
    }

    #[test]
    fn migration_state_serde_roundtrip() {
        let states = [
            MigrationState::Pending,
            MigrationState::Applied,
            MigrationState::Failed,
        ];
        for s in states {
            let j = serde_json::to_string(&s).unwrap();
            let back: MigrationState = serde_json::from_str(&j).unwrap();
            assert_eq!(s, back);
        }
    }

    #[test]
    fn migration_state_serializes_lowercase() {
        let j = serde_json::to_string(&MigrationState::Applied).unwrap();
        assert_eq!(j, "\"applied\"");
    }

    #[test]
    fn migration_direction_serializes_lowercase() {
        let j = serde_json::to_string(&MigrationDirection::Down).unwrap();
        assert_eq!(j, "\"down\"");
    }

    fn sample_migration(version: &str) -> Migration {
        Migration {
            version: version.to_string(),
            description: "test migration".into(),
            path: PathBuf::from(format!("migrations/{version}_test.surql")),
            up: vec!["DEFINE TABLE t SCHEMAFULL;".into()],
            down: vec!["REMOVE TABLE t;".into()],
            checksum: Some("deadbeef".into()),
            depends_on: vec![],
        }
    }

    #[test]
    fn migration_fields_are_populated() {
        let m = sample_migration("20260102_120000");
        assert_eq!(m.version, "20260102_120000");
        assert_eq!(m.description, "test migration");
        assert_eq!(m.up.len(), 1);
        assert_eq!(m.down.len(), 1);
        assert_eq!(m.checksum.as_deref(), Some("deadbeef"));
        assert!(m.depends_on.is_empty());
    }

    #[test]
    fn migration_serde_roundtrip() {
        let m = sample_migration("20260102_120000");
        let j = serde_json::to_string(&m).unwrap();
        let back: Migration = serde_json::from_str(&j).unwrap();
        assert_eq!(m, back);
    }

    #[test]
    fn migration_serde_missing_depends_on_defaults_empty() {
        let j = r#"{
            "version": "v1",
            "description": "d",
            "path": "p.surql",
            "up": [],
            "down": [],
            "checksum": null
        }"#;
        let m: Migration = serde_json::from_str(j).unwrap();
        assert!(m.depends_on.is_empty());
    }

    #[test]
    fn migration_history_serde_roundtrip() {
        let h = MigrationHistory {
            version: "20260102_120000".into(),
            description: "test".into(),
            applied_at: DateTime::parse_from_rfc3339("2026-01-02T12:00:00Z")
                .unwrap()
                .with_timezone(&Utc),
            checksum: "abc".into(),
            execution_time_ms: Some(100),
        };
        let j = serde_json::to_string(&h).unwrap();
        let back: MigrationHistory = serde_json::from_str(&j).unwrap();
        assert_eq!(h, back);
    }

    #[test]
    fn migration_history_execution_time_optional() {
        let h = MigrationHistory {
            version: "v1".into(),
            description: "d".into(),
            applied_at: Utc::now(),
            checksum: "c".into(),
            execution_time_ms: None,
        };
        let j = serde_json::to_string(&h).unwrap();
        let back: MigrationHistory = serde_json::from_str(&j).unwrap();
        assert_eq!(h, back);
        assert!(back.execution_time_ms.is_none());
    }

    #[test]
    fn migration_plan_empty_and_count() {
        let plan = MigrationPlan {
            migrations: vec![],
            direction: MigrationDirection::Up,
        };
        assert!(plan.is_empty());
        assert_eq!(plan.count(), 0);
    }

    #[test]
    fn migration_plan_non_empty() {
        let plan = MigrationPlan {
            migrations: vec![sample_migration("v1"), sample_migration("v2")],
            direction: MigrationDirection::Down,
        };
        assert!(!plan.is_empty());
        assert_eq!(plan.count(), 2);
        assert_eq!(plan.direction, MigrationDirection::Down);
    }

    #[test]
    fn migration_plan_serde_roundtrip() {
        let plan = MigrationPlan {
            migrations: vec![sample_migration("v1")],
            direction: MigrationDirection::Up,
        };
        let j = serde_json::to_string(&plan).unwrap();
        let back: MigrationPlan = serde_json::from_str(&j).unwrap();
        assert_eq!(plan, back);
    }

    #[test]
    fn migration_metadata_default_author() {
        assert_eq!(MigrationMetadata::default_author(), "surql");
    }

    #[test]
    fn migration_metadata_serde_defaults() {
        let j = r#"{"version":"v1","description":"d"}"#;
        let meta: MigrationMetadata = serde_json::from_str(j).unwrap();
        assert_eq!(meta.author, "surql");
        assert!(meta.depends_on.is_empty());
    }

    #[test]
    fn migration_metadata_serde_roundtrip() {
        let meta = MigrationMetadata {
            version: "v1".into(),
            description: "d".into(),
            author: "alice".into(),
            depends_on: vec!["v0".into()],
        };
        let j = serde_json::to_string(&meta).unwrap();
        let back: MigrationMetadata = serde_json::from_str(&j).unwrap();
        assert_eq!(meta, back);
    }

    #[test]
    fn migration_status_fields() {
        let m = sample_migration("v1");
        let s = MigrationStatus {
            migration: m.clone(),
            state: MigrationState::Applied,
            applied_at: Some(Utc::now()),
            error: None,
        };
        assert_eq!(s.migration, m);
        assert_eq!(s.state, MigrationState::Applied);
        assert!(s.applied_at.is_some());
        assert!(s.error.is_none());
    }

    #[test]
    fn migration_status_failure_captures_error() {
        let s = MigrationStatus {
            migration: sample_migration("v1"),
            state: MigrationState::Failed,
            applied_at: None,
            error: Some("syntax error".into()),
        };
        assert_eq!(s.state, MigrationState::Failed);
        assert_eq!(s.error.as_deref(), Some("syntax error"));
    }

    #[test]
    fn migration_status_serde_roundtrip() {
        let s = MigrationStatus {
            migration: sample_migration("v1"),
            state: MigrationState::Pending,
            applied_at: None,
            error: None,
        };
        let j = serde_json::to_string(&s).unwrap();
        let back: MigrationStatus = serde_json::from_str(&j).unwrap();
        assert_eq!(s, back);
    }

    #[test]
    fn diff_operation_as_str_values() {
        assert_eq!(DiffOperation::AddTable.as_str(), "add_table");
        assert_eq!(DiffOperation::DropTable.as_str(), "drop_table");
        assert_eq!(DiffOperation::AddField.as_str(), "add_field");
        assert_eq!(DiffOperation::DropField.as_str(), "drop_field");
        assert_eq!(DiffOperation::ModifyField.as_str(), "modify_field");
        assert_eq!(DiffOperation::AddIndex.as_str(), "add_index");
        assert_eq!(DiffOperation::DropIndex.as_str(), "drop_index");
        assert_eq!(DiffOperation::AddEvent.as_str(), "add_event");
        assert_eq!(DiffOperation::DropEvent.as_str(), "drop_event");
        assert_eq!(
            DiffOperation::ModifyPermissions.as_str(),
            "modify_permissions"
        );
    }

    #[test]
    fn diff_operation_display_matches_as_str() {
        assert_eq!(DiffOperation::AddTable.to_string(), "add_table");
        assert_eq!(
            DiffOperation::ModifyPermissions.to_string(),
            "modify_permissions"
        );
    }

    #[test]
    fn diff_operation_serializes_snake_case() {
        let j = serde_json::to_string(&DiffOperation::ModifyPermissions).unwrap();
        assert_eq!(j, "\"modify_permissions\"");
    }

    #[test]
    fn diff_operation_serde_roundtrip_all() {
        let ops = [
            DiffOperation::AddTable,
            DiffOperation::DropTable,
            DiffOperation::AddField,
            DiffOperation::DropField,
            DiffOperation::ModifyField,
            DiffOperation::AddIndex,
            DiffOperation::DropIndex,
            DiffOperation::AddEvent,
            DiffOperation::DropEvent,
            DiffOperation::ModifyPermissions,
        ];
        for op in ops {
            let j = serde_json::to_string(&op).unwrap();
            let back: DiffOperation = serde_json::from_str(&j).unwrap();
            assert_eq!(op, back);
        }
    }

    #[test]
    fn schema_diff_basic_construction() {
        let diff = SchemaDiff {
            operation: DiffOperation::AddTable,
            table: "user".into(),
            field: None,
            index: None,
            event: None,
            description: "Add user table".into(),
            forward_sql: "DEFINE TABLE user SCHEMAFULL;".into(),
            backward_sql: "REMOVE TABLE user;".into(),
            details: BTreeMap::new(),
        };
        assert_eq!(diff.operation, DiffOperation::AddTable);
        assert_eq!(diff.table, "user");
        assert!(diff.field.is_none());
    }

    #[test]
    fn schema_diff_with_field() {
        let diff = SchemaDiff {
            operation: DiffOperation::AddField,
            table: "user".into(),
            field: Some("email".into()),
            index: None,
            event: None,
            description: "Add email field".into(),
            forward_sql: "DEFINE FIELD email ON TABLE user TYPE string;".into(),
            backward_sql: "REMOVE FIELD email ON TABLE user;".into(),
            details: BTreeMap::new(),
        };
        assert_eq!(diff.field.as_deref(), Some("email"));
    }

    #[test]
    fn schema_diff_serde_roundtrip() {
        let mut details = BTreeMap::new();
        details.insert("old_type".to_string(), serde_json::json!("string"));
        details.insert("new_type".to_string(), serde_json::json!("int"));
        let diff = SchemaDiff {
            operation: DiffOperation::ModifyField,
            table: "user".into(),
            field: Some("age".into()),
            index: None,
            event: None,
            description: "change age".into(),
            forward_sql: "DEFINE FIELD age ON TABLE user TYPE int;".into(),
            backward_sql: "DEFINE FIELD age ON TABLE user TYPE string;".into(),
            details,
        };
        let j = serde_json::to_string(&diff).unwrap();
        let back: SchemaDiff = serde_json::from_str(&j).unwrap();
        assert_eq!(diff, back);
    }

    #[test]
    fn schema_diff_serde_missing_details_defaults_empty() {
        let j = r#"{
            "operation": "add_table",
            "table": "t",
            "field": null,
            "index": null,
            "event": null,
            "description": "d",
            "forward_sql": "f",
            "backward_sql": "b"
        }"#;
        let diff: SchemaDiff = serde_json::from_str(j).unwrap();
        assert!(diff.details.is_empty());
    }
}
