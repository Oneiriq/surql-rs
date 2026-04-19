//! Connection-level types for the `surql` crate.
//!
//! Port of `surql/connection/` from `oneiriq-surql` (Python). The
//! pure data types ([`ConnectionConfig`], credentials, etc.) are always
//! available. The runtime [`DatabaseClient`], [`Transaction`], and
//! [`LiveQuery`] live behind the `client` cargo feature.

pub mod auth;
#[cfg(feature = "client")]
pub mod auth_manager;
#[cfg(feature = "client")]
pub mod client;
pub mod config;
#[cfg(feature = "client")]
pub mod context;
#[cfg(feature = "client")]
pub mod registry;
#[cfg(feature = "client")]
pub mod streaming;
#[cfg(feature = "client")]
pub mod transaction;

pub use auth::{
    AuthType, Credentials, DatabaseCredentials, NamespaceCredentials, RootCredentials,
    ScopeCredentials, TokenAuth,
};
pub use config::{ConnectionConfig, NamedConnectionConfig, Protocol};

#[cfg(feature = "client")]
pub use auth_manager::{AuthManager, TokenState};
#[cfg(feature = "client")]
pub use client::DatabaseClient;
#[cfg(feature = "client")]
pub use context::{clear_db, connection_override, connection_scope, get_db, has_db, set_db};
#[cfg(feature = "client")]
pub use registry::{get_registry, set_registry, ConnectionRegistry};
#[cfg(feature = "client")]
pub use streaming::LiveQuery;
#[cfg(feature = "client")]
pub use transaction::{Transaction, TransactionState};
