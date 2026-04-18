//! Error types for the `surql` crate.
//!
//! Follows a strict no-panic policy: every fallible operation returns
//! [`Result<T, SurqlError>`]. The [`SurqlError`] enum unifies every error
//! kind the library can produce. Per-subsystem error variants mirror the
//! Python (`oneiriq-surql`) exception hierarchy.
//!
//! ## Examples
//!
//! ```
//! use surql::error::{Context, Result, SurqlError};
//!
//! fn parse_id(raw: &str) -> Result<(String, String)> {
//!     let (table, id) = raw.split_once(':').ok_or_else(|| {
//!         SurqlError::Validation {
//!             reason: format!("expected table:id, got {raw:?}"),
//!         }
//!     })?;
//!     Ok((table.to_owned(), id.to_owned()))
//! }
//!
//! let outcome = parse_id("user").context("parsing record id").unwrap_err();
//! assert!(outcome.to_string().contains("parsing record id"));
//! ```

use std::fmt;

/// Convenient alias for results produced by this crate.
pub type Result<T> = std::result::Result<T, SurqlError>;

/// Unified error type for the `surql` crate.
///
/// Each variant corresponds to one subsystem. Variants can be wrapped with
/// additional context via [`Context::context`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SurqlError {
    /// General database error (analogue of Python `DatabaseError`).
    Database {
        /// Human-readable explanation.
        reason: String,
    },
    /// Connection failed, timed out, or was closed unexpectedly.
    Connection {
        /// Human-readable explanation.
        reason: String,
    },
    /// A query failed at the database or during result decoding.
    Query {
        /// Human-readable explanation.
        reason: String,
    },
    /// A transaction could not be started, committed, or rolled back.
    Transaction {
        /// Human-readable explanation.
        reason: String,
    },
    /// Ambient connection context was missing or misconfigured.
    Context {
        /// Human-readable explanation.
        reason: String,
    },
    /// Named-connection registry lookup or registration failed.
    Registry {
        /// Human-readable explanation.
        reason: String,
    },
    /// Live/streaming query error.
    Streaming {
        /// Human-readable explanation.
        reason: String,
    },
    /// Input failed validation (invalid identifier, malformed id, etc.).
    Validation {
        /// Human-readable explanation.
        reason: String,
    },
    /// Schema parser could not understand the schema text or response.
    SchemaParse {
        /// Human-readable explanation.
        reason: String,
    },
    /// Error while discovering migration files on disk.
    MigrationDiscovery {
        /// Human-readable explanation.
        reason: String,
    },
    /// Error while loading an individual migration.
    MigrationLoad {
        /// Human-readable explanation.
        reason: String,
    },
    /// Error while generating a migration from a schema diff.
    MigrationGeneration {
        /// Human-readable explanation.
        reason: String,
    },
    /// Error while executing a migration against the database.
    MigrationExecution {
        /// Human-readable explanation.
        reason: String,
    },
    /// Error while reading or writing migration history.
    MigrationHistory {
        /// Human-readable explanation.
        reason: String,
    },
    /// Error while squashing migrations.
    MigrationSquash {
        /// Human-readable explanation.
        reason: String,
    },
    /// Multi-environment orchestration failed.
    Orchestration {
        /// Human-readable explanation.
        reason: String,
    },
    /// JSON encode or decode failure.
    Serialization {
        /// Human-readable explanation.
        reason: String,
    },
    /// Filesystem / generic I/O error.
    Io {
        /// Human-readable explanation.
        reason: String,
    },
    /// An existing [`SurqlError`] with extra context prepended.
    WithContext {
        /// Underlying error.
        source: Box<SurqlError>,
        /// Context description added at the call site.
        context: String,
    },
}

impl fmt::Display for SurqlError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Database { reason } => write!(f, "database error: {reason}"),
            Self::Connection { reason } => write!(f, "connection error: {reason}"),
            Self::Query { reason } => write!(f, "query error: {reason}"),
            Self::Transaction { reason } => write!(f, "transaction error: {reason}"),
            Self::Context { reason } => write!(f, "context error: {reason}"),
            Self::Registry { reason } => write!(f, "registry error: {reason}"),
            Self::Streaming { reason } => write!(f, "streaming error: {reason}"),
            Self::Validation { reason } => write!(f, "validation error: {reason}"),
            Self::SchemaParse { reason } => write!(f, "schema parse error: {reason}"),
            Self::MigrationDiscovery { reason } => {
                write!(f, "migration discovery error: {reason}")
            }
            Self::MigrationLoad { reason } => write!(f, "migration load error: {reason}"),
            Self::MigrationGeneration { reason } => {
                write!(f, "migration generation error: {reason}")
            }
            Self::MigrationExecution { reason } => {
                write!(f, "migration execution error: {reason}")
            }
            Self::MigrationHistory { reason } => write!(f, "migration history error: {reason}"),
            Self::MigrationSquash { reason } => write!(f, "migration squash error: {reason}"),
            Self::Orchestration { reason } => write!(f, "orchestration error: {reason}"),
            Self::Serialization { reason } => write!(f, "serialization error: {reason}"),
            Self::Io { reason } => write!(f, "io error: {reason}"),
            Self::WithContext { source, context } => write!(f, "{context}: {source}"),
        }
    }
}

impl std::error::Error for SurqlError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::WithContext { source, .. } => Some(source.as_ref()),
            _ => None,
        }
    }
}

impl From<std::io::Error> for SurqlError {
    fn from(err: std::io::Error) -> Self {
        Self::Io {
            reason: err.to_string(),
        }
    }
}

impl From<serde_json::Error> for SurqlError {
    fn from(err: serde_json::Error) -> Self {
        Self::Serialization {
            reason: err.to_string(),
        }
    }
}

/// Extension trait for attaching contextual messages to a [`Result`].
///
/// Mirrors the `Context` trait used in `oniq`; the attached message is
/// formatted as `"{context}: {source}"` in [`SurqlError::WithContext`].
///
/// ## Examples
///
/// ```
/// use surql::error::{Context, SurqlError};
///
/// let result: Result<(), SurqlError> = Err(SurqlError::Query {
///     reason: "syntax near SELECT".into(),
/// });
/// let wrapped = result.context("loading user").unwrap_err();
/// assert!(wrapped.to_string().starts_with("loading user: query error"));
/// ```
pub trait Context<T> {
    /// Attach the given context to this result's error (no-op on `Ok`).
    fn context(self, context: impl Into<String>) -> Result<T>;
}

impl<T, E> Context<T> for std::result::Result<T, E>
where
    E: Into<SurqlError>,
{
    fn context(self, context: impl Into<String>) -> Result<T> {
        self.map_err(|e| SurqlError::WithContext {
            source: Box::new(e.into()),
            context: context.into(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_includes_reason() {
        let err = SurqlError::Query {
            reason: "missing table".into(),
        };
        assert_eq!(err.to_string(), "query error: missing table");
    }

    #[test]
    fn context_wraps_error() {
        let base: Result<()> = Err(SurqlError::Connection {
            reason: "refused".into(),
        });
        let wrapped = base.context("dialing surrealdb").unwrap_err();
        assert_eq!(
            wrapped.to_string(),
            "dialing surrealdb: connection error: refused"
        );
    }

    #[test]
    fn context_is_noop_on_ok() {
        let ok: Result<u32> = Ok(1);
        assert_eq!(ok.context("should not fire").unwrap(), 1);
    }

    #[test]
    fn from_serde_json_error() {
        let json_err = serde_json::from_str::<serde_json::Value>("not json").unwrap_err();
        let err: SurqlError = json_err.into();
        assert!(matches!(err, SurqlError::Serialization { .. }));
    }

    #[test]
    fn from_io_error() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "missing");
        let err: SurqlError = io_err.into();
        assert!(matches!(err, SurqlError::Io { .. }));
    }

    #[test]
    fn source_chain_is_reported() {
        let err = SurqlError::WithContext {
            source: Box::new(SurqlError::Validation {
                reason: "bad".into(),
            }),
            context: "outer".into(),
        };
        let source = std::error::Error::source(&err);
        assert!(source.is_some());
    }
}
