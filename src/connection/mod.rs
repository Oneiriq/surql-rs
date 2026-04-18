//! Connection-level types for the `surql` crate.
//!
//! Port of `surql/connection/` from `oneiriq-surql` (Python). This module
//! currently exposes the pure data types (configuration, credentials)
//! needed to describe how to talk to SurrealDB; the runtime
//! [`DatabaseClient`] and [`AuthManager`] live behind the `client` cargo
//! feature and land in a follow-up increment.
//!
//! [`DatabaseClient`]: crate::connection::DatabaseClient "stub"
//! [`AuthManager`]: crate::connection::AuthManager "stub"

pub mod auth;
pub mod config;

pub use auth::{
    AuthType, DatabaseCredentials, NamespaceCredentials, RootCredentials, ScopeCredentials,
    TokenAuth,
};
pub use config::{ConnectionConfig, NamedConnectionConfig, Protocol};
