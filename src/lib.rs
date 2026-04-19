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
//! - [`error`]: [`SurqlError`](error::SurqlError) and [`Result`](error::Result).
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

#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![allow(clippy::module_name_repetitions)]
#![allow(clippy::must_use_candidate)]
#![allow(clippy::missing_errors_doc)]
#![allow(clippy::missing_panics_doc)]
#![allow(clippy::doc_markdown)]
#![allow(clippy::uninlined_format_args)]
#![allow(clippy::return_self_not_must_use)]
#![deny(missing_docs)]
#![forbid(unsafe_code)]

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

#[cfg(feature = "client")]
pub use connection::DatabaseClient;
