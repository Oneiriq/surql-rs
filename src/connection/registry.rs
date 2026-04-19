//! Named-connection registry.
//!
//! Port of `surql/connection/registry.py`. The Python module exposes a
//! single module-level `_registry` singleton. The Rust equivalent lives
//! behind a process-wide [`OnceLock`] (see [`get_registry`]); tests can
//! swap in a dedicated registry with [`set_registry`].
//!
//! The registry owns `Arc<DatabaseClient>` values (not owned clients) so
//! callers can share handles with [`crate::connection::context`].

use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

use tokio::sync::RwLock;

use crate::connection::client::DatabaseClient;
use crate::connection::config::ConnectionConfig;
use crate::error::{Result, SurqlError};

/// Registry of named database connections.
///
/// Clone-cheap: internally refcounted.
#[derive(Debug, Clone, Default)]
pub struct ConnectionRegistry {
    inner: Arc<RegistryInner>,
}

#[derive(Debug, Default)]
struct RegistryInner {
    connections: RwLock<HashMap<String, Arc<DatabaseClient>>>,
    configs: RwLock<HashMap<String, ConnectionConfig>>,
    default_name: RwLock<Option<String>>,
}

impl ConnectionRegistry {
    /// Create an empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a new named connection, optionally opening it immediately.
    ///
    /// Mirrors py's `register(name, config, connect=True, set_default=False)`.
    /// The first registered connection is automatically promoted to
    /// default if `set_default` is not set.
    ///
    /// # Errors
    ///
    /// - [`SurqlError::Registry`] when `name` is already registered.
    /// - [`SurqlError::Connection`] when `connect` is `true` and the
    ///   underlying `DatabaseClient::connect` fails.
    pub async fn register(
        &self,
        name: impl Into<String>,
        config: ConnectionConfig,
        connect: bool,
        set_default: bool,
    ) -> Result<Arc<DatabaseClient>> {
        let name = name.into();
        {
            let conns = self.inner.connections.read().await;
            if conns.contains_key(&name) {
                return Err(SurqlError::Registry {
                    reason: format!("connection {name:?} already registered"),
                });
            }
        }

        let client = Arc::new(DatabaseClient::new(config.clone())?);
        if connect {
            client.connect().await?;
        }

        // Re-check under write lock to avoid TOCTOU races against a concurrent register.
        let mut conns = self.inner.connections.write().await;
        if conns.contains_key(&name) {
            return Err(SurqlError::Registry {
                reason: format!("connection {name:?} already registered"),
            });
        }
        conns.insert(name.clone(), client.clone());

        let mut configs = self.inner.configs.write().await;
        configs.insert(name.clone(), config);

        let mut default = self.inner.default_name.write().await;
        if set_default || default.is_none() {
            *default = Some(name);
        }

        Ok(client)
    }

    /// Remove a named connection, optionally disconnecting it first.
    ///
    /// If the removed connection was the default, the new default is
    /// chosen from the remaining connections (first by iteration order),
    /// or cleared if none remain.
    ///
    /// # Errors
    ///
    /// Returns [`SurqlError::Registry`] when `name` is not registered.
    pub async fn unregister(&self, name: &str, disconnect: bool) -> Result<()> {
        let mut conns = self.inner.connections.write().await;
        let Some(client) = conns.remove(name) else {
            return Err(SurqlError::Registry {
                reason: format!("connection {name:?} not found"),
            });
        };

        if disconnect && client.is_connected() {
            // Drop the lock before awaiting so we don't hold the write guard across .await.
            drop(conns);
            let _ = client.disconnect().await;
            conns = self.inner.connections.write().await;
        }

        let mut configs = self.inner.configs.write().await;
        configs.remove(name);

        let mut default = self.inner.default_name.write().await;
        if default.as_deref() == Some(name) {
            *default = conns.keys().next().cloned();
        }

        Ok(())
    }

    /// Fetch a registered connection by name, or the default when
    /// `name` is `None`.
    ///
    /// # Errors
    ///
    /// Returns [`SurqlError::Registry`] when no connection matches (or
    /// no default is set for the `None` case).
    pub async fn get(&self, name: Option<&str>) -> Result<Arc<DatabaseClient>> {
        let conns = self.inner.connections.read().await;
        let lookup = match name {
            Some(n) => n.to_owned(),
            None => self
                .inner
                .default_name
                .read()
                .await
                .clone()
                .ok_or_else(|| SurqlError::Registry {
                    reason: "no default connection set".into(),
                })?,
        };
        conns.get(&lookup).cloned().ok_or(SurqlError::Registry {
            reason: format!("connection {lookup:?} not found"),
        })
    }

    /// Fetch the stored [`ConnectionConfig`] for a registered connection.
    ///
    /// # Errors
    ///
    /// Returns [`SurqlError::Registry`] when no connection matches.
    pub async fn get_config(&self, name: Option<&str>) -> Result<ConnectionConfig> {
        let configs = self.inner.configs.read().await;
        let lookup = match name {
            Some(n) => n.to_owned(),
            None => self
                .inner
                .default_name
                .read()
                .await
                .clone()
                .ok_or_else(|| SurqlError::Registry {
                    reason: "no default connection set".into(),
                })?,
        };
        configs.get(&lookup).cloned().ok_or(SurqlError::Registry {
            reason: format!("connection {lookup:?} not found"),
        })
    }

    /// Promote a registered connection to default.
    ///
    /// # Errors
    ///
    /// Returns [`SurqlError::Registry`] when `name` is not registered.
    pub async fn set_default(&self, name: &str) -> Result<()> {
        let conns = self.inner.connections.read().await;
        if !conns.contains_key(name) {
            return Err(SurqlError::Registry {
                reason: format!("connection {name:?} not found"),
            });
        }
        *self.inner.default_name.write().await = Some(name.to_owned());
        Ok(())
    }

    /// List every registered connection name.
    pub async fn list(&self) -> Vec<String> {
        self.inner
            .connections
            .read()
            .await
            .keys()
            .cloned()
            .collect()
    }

    /// Return the current default connection name (if any).
    pub async fn default_name(&self) -> Option<String> {
        self.inner.default_name.read().await.clone()
    }

    /// Disconnect every registered connection (keeping them registered).
    pub async fn disconnect_all(&self) {
        let snapshot: Vec<Arc<DatabaseClient>> = self
            .inner
            .connections
            .read()
            .await
            .values()
            .cloned()
            .collect();
        for client in snapshot {
            if client.is_connected() {
                let _ = client.disconnect().await;
            }
        }
    }

    /// Disconnect and remove every registered connection.
    pub async fn clear(&self) {
        self.disconnect_all().await;
        self.inner.connections.write().await.clear();
        self.inner.configs.write().await.clear();
        *self.inner.default_name.write().await = None;
    }
}

/// Handle to the process-wide [`ConnectionRegistry`].
///
/// The first call initialises a fresh registry; subsequent calls return
/// a clone of the same handle.
pub fn get_registry() -> ConnectionRegistry {
    global().clone()
}

/// Replace the process-wide registry.
///
/// Primarily useful in tests. No-op if the global is already
/// initialised **and** equal to the supplied instance (by `Arc`
/// identity); otherwise it swaps the inner-most slot.
///
/// # Errors
///
/// Returns [`SurqlError::Registry`] if the global has already been
/// initialised with a different instance and cannot be overwritten
/// (the `OnceLock` can only be set once per process).
pub fn set_registry(registry: ConnectionRegistry) -> Result<()> {
    GLOBAL.set(registry).map_err(|_| SurqlError::Registry {
        reason: "global registry is already initialised".into(),
    })
}

static GLOBAL: OnceLock<ConnectionRegistry> = OnceLock::new();

fn global() -> &'static ConnectionRegistry {
    GLOBAL.get_or_init(ConnectionRegistry::new)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_config(db: &str) -> ConnectionConfig {
        ConnectionConfig::builder()
            .url("ws://localhost:8000")
            .namespace("test")
            .database(db)
            .build()
            .expect("valid config")
    }

    #[tokio::test]
    async fn register_without_connect_stores_client() {
        let r = ConnectionRegistry::new();
        let client = r
            .register("primary", make_config("a"), false, false)
            .await
            .expect("register");
        assert!(!client.is_connected());

        let fetched = r.get(Some("primary")).await.expect("fetch");
        assert!(Arc::ptr_eq(&client, &fetched));

        let default_fetched = r.get(None).await.expect("default fetch");
        assert!(Arc::ptr_eq(&client, &default_fetched));
        assert_eq!(r.default_name().await.as_deref(), Some("primary"));
    }

    #[tokio::test]
    async fn duplicate_register_rejects() {
        let r = ConnectionRegistry::new();
        r.register("primary", make_config("a"), false, false)
            .await
            .expect("first");
        let err = r
            .register("primary", make_config("a"), false, false)
            .await
            .unwrap_err();
        assert!(matches!(err, SurqlError::Registry { .. }));
    }

    #[tokio::test]
    async fn unregister_rotates_default() {
        let r = ConnectionRegistry::new();
        r.register("a", make_config("a"), false, false)
            .await
            .unwrap();
        r.register("b", make_config("b"), false, false)
            .await
            .unwrap();
        assert_eq!(r.default_name().await.as_deref(), Some("a"));
        r.unregister("a", false).await.unwrap();
        assert_eq!(r.default_name().await.as_deref(), Some("b"));
        r.unregister("b", false).await.unwrap();
        assert!(r.default_name().await.is_none());

        let err = r.get(None).await.unwrap_err();
        assert!(matches!(err, SurqlError::Registry { .. }));
    }

    #[tokio::test]
    async fn unregister_missing_errors() {
        let r = ConnectionRegistry::new();
        let err = r.unregister("ghost", false).await.unwrap_err();
        assert!(matches!(err, SurqlError::Registry { .. }));
    }

    #[tokio::test]
    async fn set_default_requires_known_name() {
        let r = ConnectionRegistry::new();
        let err = r.set_default("ghost").await.unwrap_err();
        assert!(matches!(err, SurqlError::Registry { .. }));
    }

    #[tokio::test]
    async fn clear_empties_state() {
        let r = ConnectionRegistry::new();
        r.register("a", make_config("a"), false, false)
            .await
            .unwrap();
        r.register("b", make_config("b"), false, false)
            .await
            .unwrap();
        r.clear().await;
        assert!(r.list().await.is_empty());
        assert!(r.default_name().await.is_none());
    }

    #[tokio::test]
    async fn list_returns_every_registered_name() {
        let r = ConnectionRegistry::new();
        r.register("a", make_config("a"), false, false)
            .await
            .unwrap();
        r.register("b", make_config("b"), false, false)
            .await
            .unwrap();
        let mut names = r.list().await;
        names.sort();
        assert_eq!(names, vec!["a".to_owned(), "b".to_owned()]);
    }

    #[tokio::test]
    async fn set_default_promotes_named_connection() {
        let r = ConnectionRegistry::new();
        r.register("a", make_config("a"), false, false)
            .await
            .unwrap();
        r.register("b", make_config("b"), false, false)
            .await
            .unwrap();
        r.set_default("b").await.unwrap();
        assert_eq!(r.default_name().await.as_deref(), Some("b"));
    }
}
