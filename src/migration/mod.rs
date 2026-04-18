//! Migration subsystem.
//!
//! Port of `surql/migration/` from `oneiriq-surql` (Python). This module
//! currently covers the pure data model (tracked in [`models`]) and
//! filesystem-level discovery/loading of migration files (tracked in
//! [`discovery`]).
//!
//! Additional submodules (`executor`, `history`, `hooks`, `rollback`,
//! `squash`, `versioning`, `watcher`) will land in follow-up PRs.
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
pub mod generator;
pub mod models;

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
pub use models::{
    DiffOperation, Migration, MigrationDirection, MigrationHistory, MigrationMetadata,
    MigrationPlan, MigrationState, MigrationStatus, SchemaDiff,
};
