//! Migration subsystem.
//!
//! Port of `surql/migration/` from `oneiriq-surql` (Python). This module
//! currently covers the pure data model (tracked in [`models`]) and
//! filesystem-level discovery/loading of migration files (tracked in
//! [`discovery`]).
//!
//! Additional submodules cover [`executor`] (client-gated),
//! [`history`] (client-gated), [`rollback`] (client-gated),
//! [`squash`] (pure, always on), and [`watcher`] (feature-gated behind
//! `watcher`). Git hook integration lives in [`hooks`] (including the
//! auto-snapshot toggle quartet); snapshot versioning in [`versioning`].
//!
//! ## Migration file format
//!
//! Unlike the Python implementation — which imports `.py` migration
//! modules at runtime — the Rust port stores migrations as plain `.surql`
//! files with section markers:
//!
//! ```surql,ignore
//! -- @metadata
//! -- version: 20260102_120000
//! -- description: Create user table
//! -- author: surql
//! -- depends_on: [20260101_000000_init]
//! -- @up
//! DEFINE TABLE user SCHEMAFULL;
//! -- @down
//! REMOVE TABLE user;
//! ```
//!
//! See [`discovery`] for the exact grammar.

pub mod diff;
pub mod discovery;
#[cfg(any(feature = "client", feature = "client-rustls"))]
pub mod executor;
pub mod generator;
#[cfg(any(feature = "client", feature = "client-rustls"))]
pub mod history;
pub mod hooks;
pub mod models;
#[cfg(any(feature = "client", feature = "client-rustls"))]
pub mod rollback;
pub mod squash;
pub mod versioning;
#[cfg(feature = "watcher")]
pub mod watcher;

pub use diff::{
    diff_edge_pair, diff_edges, diff_events, diff_fields, diff_indexes, diff_permissions,
    diff_schemas, diff_table_pair, diff_tables, normalize_expression, validate_default_value,
    validate_event_expression, SchemaSnapshot,
};
pub use discovery::{
    discover_migrations, get_description_from_filename, get_version_from_filename, load_migration,
    validate_migration_name,
};
pub use generator::{
    create_blank_migration, generate_initial_migration, generate_migration,
    generate_migration_from_diffs,
};
pub use hooks::{
    check_schema_drift, check_schema_drift_from_snapshots, create_snapshot_on_migration,
    default_schema_filter, disable_auto_snapshots, enable_auto_snapshots,
    generate_precommit_config, get_staged_schema_files, is_auto_snapshot_enabled,
    registry_to_snapshot, severity_for_operation, versioned_to_snapshot, DriftIssue, DriftReport,
    DriftSeverity, SnapshotHooks,
};
pub use models::{
    DiffOperation, Migration, MigrationDirection, MigrationHistory, MigrationMetadata,
    MigrationPlan, MigrationState, MigrationStatus, SchemaDiff,
};
pub use squash::{
    filter_migrations_by_version, generate_squashed_migration_content, optimize_statements,
    squash_migrations, validate_squash_safety, SquashError, SquashOptions, SquashResult,
    SquashSeverity, SquashWarning,
};
pub use versioning::{
    compare_snapshots, create_snapshot, list_snapshots, load_snapshot, store_snapshot,
    SnapshotComparison, VersionGraph, VersionNode, VersionedSnapshot, VersionedSnapshotBuilder,
};
#[cfg(feature = "watcher")]
pub use watcher::{is_schema_file, SchemaWatcher, WatcherConfig};

#[cfg(any(feature = "client", feature = "client-rustls"))]
pub use executor::{
    create_migration_plan, execute_migration, execute_migration_plan,
    get_applied_migrations_ordered, get_migration_status, get_pending_migrations, migrate_down,
    migrate_up, validate_migrations, version_is_applied, MigrateUpOptions, MigrationStatusReport,
};
#[cfg(any(feature = "client", feature = "client-rustls"))]
pub use history::{
    auto_snapshot_after_apply, create_migration_table, ensure_migration_table,
    get_applied_migrations, get_migration_history, is_migration_applied, record_migration,
    remove_migration_record, MIGRATION_TABLE_NAME,
};
#[cfg(any(feature = "client", feature = "client-rustls"))]
pub use rollback::{
    analyze_rollback_safety, create_rollback_plan, execute_rollback, plan_rollback_to_version,
    RollbackIssue, RollbackPlan, RollbackResult, RollbackSafety,
};
