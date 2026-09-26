//! # surql
//!
//! Code-first database toolkit for SurrealDB in Rust.
//!
//! Rust port of [`oneiriq-surql`](https://github.com/Oneiriq/surql-py) (Python) and
//! [`@oneiriq/surql`](https://github.com/Oneiriq/surql) (TypeScript).
//! Target: 1:1 feature parity.
//!
//! ## Modules
//!
//! - [`error`]: [`SurqlError`] and [`Result`].
//! - [`types`]: Type-safe wrappers ([`RecordID`](types::RecordID),
//!   [`RecordRef`](types::RecordRef), [`SurrealFn`](types::SurrealFn),
//!   operators, reserved-word checks, datetime coercion).
//! - [`connection`]: Connection [`ConnectionConfig`](connection::ConnectionConfig)
//!   and credential types ([`RootCredentials`](connection::RootCredentials),
//!   [`NamespaceCredentials`](connection::NamespaceCredentials),
//!   [`DatabaseCredentials`](connection::DatabaseCredentials),
//!   [`ScopeCredentials`](connection::ScopeCredentials)).
//! - [`schema`]: Schema definition layer —
//!   [`FieldDefinition`](schema::FieldDefinition),
//!   [`TableDefinition`](schema::TableDefinition),
//!   [`EdgeDefinition`](schema::EdgeDefinition), and
//!   [`AccessDefinition`](schema::AccessDefinition).
//! - [`migration`]: Migration data model ([`Migration`](migration::Migration),
//!   [`MigrationHistory`](migration::MigrationHistory),
//!   [`MigrationPlan`](migration::MigrationPlan),
//!   [`MigrationState`](migration::MigrationState),
//!   [`MigrationDirection`](migration::MigrationDirection),
//!   [`SchemaDiff`](migration::SchemaDiff)) and filesystem-level discovery
//!   ([`discover_migrations`](migration::discover_migrations),
//!   [`load_migration`](migration::load_migration)).
//!
//! - [`orchestration`] *(feature-gated: `orchestration`)*: Multi-database
//!   migration orchestration — [`EnvironmentConfig`](orchestration::EnvironmentConfig),
//!   [`EnvironmentRegistry`](orchestration::EnvironmentRegistry),
//!   [`MigrationCoordinator`](orchestration::MigrationCoordinator),
//!   [`HealthCheck`](orchestration::HealthCheck), and deployment
//!   strategies ([`SequentialStrategy`](orchestration::SequentialStrategy),
//!   [`ParallelStrategy`](orchestration::ParallelStrategy),
//!   [`RollingStrategy`](orchestration::RollingStrategy),
//!   [`CanaryStrategy`](orchestration::CanaryStrategy)).

// Lint levels live in Cargo.toml's `[lints]` table. On top of them, shipped
// code may not panic: the crate's own unit tests (`cfg(test)`) may.
#![cfg_attr(
    not(test),
    warn(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::unreachable,
        clippy::todo,
        clippy::unimplemented
    )
)]

#[cfg(feature = "cache")]
pub mod cache;
#[cfg(feature = "cli")]
pub mod cli;
pub mod connection;
pub mod error;
pub mod migration;
#[cfg(feature = "orchestration")]
pub mod orchestration;
pub mod query;
pub mod schema;
#[cfg(feature = "settings")]
pub mod settings;
pub mod types;

pub use error::{Result, SurqlError};

#[cfg(any(feature = "client", feature = "client-rustls", feature = "client-wasm"))]
pub use connection::DatabaseClient;

// Convenience re-exports for the first-class `type::record` helpers.
pub use types::operators::{type_record, type_thing};

// Result-extraction helpers hoisted into the crate root for ergonomic
// `use surql::{extract_one, extract_scalar, extract_many, has_result};` usage.
pub use query::results::{extract_many, extract_one, extract_scalar, has_result};
