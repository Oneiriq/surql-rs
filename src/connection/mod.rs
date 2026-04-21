//! Connection-level types for the `surql` crate.
//!
//! Port of `surql/connection/` from `oneiriq-surql` (Python). The
//! pure data types ([`ConnectionConfig`], credentials, etc.) are always
//! available. The runtime [`DatabaseClient`], [`Transaction`], and
//! [`LiveQuery`] live behind the `client` cargo feature, as do the
//! context / registry / auth-manager / streaming-manager facilities.

pub mod auth;
#[cfg(any(feature = "client", feature = "client-rustls"))]
pub mod auth_manager;
#[cfg(any(feature = "client", feature = "client-rustls"))]
pub mod client;
pub mod config;
#[cfg(any(feature = "client", feature = "client-rustls"))]
pub mod context;
#[cfg(any(feature = "client", feature = "client-rustls"))]
pub mod registry;
#[cfg(any(feature = "client", feature = "client-rustls"))]
pub mod streaming;
#[cfg(any(feature = "client", feature = "client-rustls"))]
pub mod transaction;

pub use auth::{
    AuthType, Credentials, DatabaseCredentials, NamespaceCredentials, RootCredentials,
    ScopeCredentials, TokenAuth,
};
pub use config::{ConnectionConfig, NamedConnectionConfig, Protocol};

#[cfg(any(feature = "client", feature = "client-rustls"))]
pub use auth_manager::{AuthManager, TokenState};
#[cfg(any(feature = "client", feature = "client-rustls"))]
pub use client::DatabaseClient;
#[cfg(any(feature = "client", feature = "client-rustls"))]
pub use context::{clear_db, connection_override, connection_scope, get_db, has_db, set_db};
#[cfg(any(feature = "client", feature = "client-rustls"))]
pub use registry::{get_registry, set_registry, ConnectionRegistry};
#[cfg(any(feature = "client", feature = "client-rustls"))]
pub use streaming::{LiveQuery, StreamingManager, SubscriptionId};
#[cfg(any(feature = "client", feature = "client-rustls"))]
pub use transaction::{Transaction, TransactionState};
