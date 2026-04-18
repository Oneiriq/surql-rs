//! Async SurrealDB client wrapper.
//!
//! Port of `surql/connection/client.py`. Wraps
//! [`surrealdb::Surreal<surrealdb::engine::any::Any>`], which picks the
//! underlying engine (WebSocket, HTTP, in-memory, file, `SurrealKV`) from
//! the URL at runtime. Retry logic, connection timeout, and
//! auth-level dispatch mirror the Python client one-for-one.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use serde::{de::DeserializeOwned, Serialize};
use serde_json::Value;
use surrealdb::engine::any::Any;
use surrealdb::opt::auth::{
    Database as SdkDatabase, Jwt, Namespace as SdkNamespace, Record as SdkRecord, Root as SdkRoot,
};
use surrealdb::Surreal;
use tokio::sync::RwLock;
use tokio::time::sleep;

use crate::connection::auth::{AuthType, Credentials, ScopeCredentials, TokenAuth};
use crate::connection::config::ConnectionConfig;
use crate::error::{Result, SurqlError};

/// Async SurrealDB client with connection + retry management.
///
/// This is a thin wrapper over [`surrealdb::Surreal`] bound to the
/// dynamic [`Any`] engine. All methods are `async` and cancellation-safe
/// at the tokio level.
///
/// The client is `Clone`-able: every clone shares the same underlying
/// connection (the `surrealdb` SDK holds its own `Arc`).
#[derive(Debug, Clone)]
pub struct DatabaseClient {
    config: ConnectionConfig,
    inner: Surreal<Any>,
    connected: Arc<RwLock<bool>>,
}

impl DatabaseClient {
    /// Build a new client. Does **not** open a network connection; call
    /// [`DatabaseClient::connect`] for that.
    pub fn new(config: ConnectionConfig) -> Result<Self> {
        config.validate()?;
        Ok(Self {
            config,
            inner: Surreal::init(),
            connected: Arc::new(RwLock::new(false)),
        })
    }

    /// Borrow the underlying configuration.
    pub fn config(&self) -> &ConnectionConfig {
        &self.config
    }

    /// Borrow the underlying SurrealDB SDK handle (advanced usage).
    pub fn inner(&self) -> &Surreal<Any> {
        &self.inner
    }

    /// Return `true` if [`DatabaseClient::connect`] has completed successfully.
    pub fn is_connected(&self) -> bool {
        self.connected.try_read().is_ok_and(|g| *g)
    }

    /// Establish the connection and select the configured namespace / database.
    ///
    /// Retries with exponential backoff up to
    /// [`ConnectionConfig::retry_max_attempts`] times; each attempt is
    /// bounded by [`ConnectionConfig::timeout`].
    pub async fn connect(&self) -> Result<()> {
        // Reconnect is idempotent: disconnect any previous session first.
        if *self.connected.read().await {
            self.disconnect().await.ok();
        }

        let attempts = self.config.retry_max_attempts().max(1);
        let mut last_err: Option<SurqlError> = None;

        for attempt in 1..=attempts {
            match self.connect_once().await {
                Ok(()) => {
                    *self.connected.write().await = true;
                    return Ok(());
                }
                Err(err) => {
                    last_err = Some(err);
                    if attempt < attempts {
                        let wait = self.backoff_for(attempt);
                        sleep(wait).await;
                    }
                }
            }
        }

        Err(last_err.unwrap_or_else(|| SurqlError::Connection {
            reason: format!("connection failed after {attempts} attempts"),
        }))
    }

    /// Close the underlying connection. Safe to call even if not connected.
    pub async fn disconnect(&self) -> Result<()> {
        {
            let mut guard = self.connected.write().await;
            if !*guard {
                return Ok(());
            }
            *guard = false;
        }
        // The SDK exposes `invalidate` to clear auth, but there is no
        // explicit disconnect on `Surreal<Any>` beyond dropping the
        // handle. We invalidate the session so subsequent calls fail
        // cleanly.
        self.inner.invalidate().await.ok();
        Ok(())
    }

    /// Sign in using one of the four auth levels.
    pub async fn signin<C: Credentials + ?Sized>(&self, creds: &C) -> Result<TokenAuth> {
        self.require_connected()?;
        let payload = creds.to_signin_payload();
        let jwt = match creds.auth_type() {
            AuthType::Root => {
                let username = payload_str(&payload, "username")?;
                let password = payload_str(&payload, "password")?;
                self.inner
                    .signin(SdkRoot {
                        username: &username,
                        password: &password,
                    })
                    .await
                    .map_err(|e| connection_err(&e))?
            }
            AuthType::Namespace => {
                let namespace = payload_str(&payload, "namespace")?;
                let username = payload_str(&payload, "username")?;
                let password = payload_str(&payload, "password")?;
                self.inner
                    .signin(SdkNamespace {
                        namespace: &namespace,
                        username: &username,
                        password: &password,
                    })
                    .await
                    .map_err(|e| connection_err(&e))?
            }
            AuthType::Database => {
                let namespace = payload_str(&payload, "namespace")?;
                let database = payload_str(&payload, "database")?;
                let username = payload_str(&payload, "username")?;
                let password = payload_str(&payload, "password")?;
                self.inner
                    .signin(SdkDatabase {
                        namespace: &namespace,
                        database: &database,
                        username: &username,
                        password: &password,
                    })
                    .await
                    .map_err(|e| connection_err(&e))?
            }
            AuthType::Scope => {
                let namespace = payload_str(&payload, "namespace")?;
                let database = payload_str(&payload, "database")?;
                let access = payload_str(&payload, "access")?;
                // Everything else is scope-defined vars.
                let mut params = serde_json::Map::new();
                for (k, v) in &payload {
                    if !matches!(k.as_str(), "namespace" | "database" | "access") {
                        params.insert(k.clone(), v.clone());
                    }
                }
                self.inner
                    .signin(SdkRecord {
                        namespace: &namespace,
                        database: &database,
                        access: &access,
                        params,
                    })
                    .await
                    .map_err(|e| connection_err(&e))?
            }
        };
        Ok(TokenAuth::new(jwt.into_insecure_token()))
    }

    /// Sign up a scope user (record access).
    pub async fn signup(&self, creds: &ScopeCredentials) -> Result<TokenAuth> {
        self.require_connected()?;
        let mut params = serde_json::Map::new();
        for (k, v) in &creds.variables {
            params.insert(k.clone(), v.clone());
        }
        let jwt = self
            .inner
            .signup(SdkRecord {
                namespace: &creds.namespace,
                database: &creds.database,
                access: &creds.access,
                params,
            })
            .await
            .map_err(|e| connection_err(&e))?;
        Ok(TokenAuth::new(jwt.into_insecure_token()))
    }

    /// Authenticate using a previously-issued JWT.
    pub async fn authenticate(&self, token: &str) -> Result<()> {
        self.require_connected()?;
        self.inner
            .authenticate(Jwt::from(token))
            .await
            .map_err(|e| connection_err(&e))?;
        Ok(())
    }

    /// Invalidate the current session.
    pub async fn invalidate(&self) -> Result<()> {
        self.require_connected()?;
        self.inner
            .invalidate()
            .await
            .map_err(|e| connection_err(&e))?;
        Ok(())
    }

    /// Execute a raw SurrealQL query and return every statement's result
    /// as a JSON array (one entry per statement).
    pub async fn query(&self, surql: &str) -> Result<Value> {
        self.query_with_vars(surql, BTreeMap::new()).await
    }

    /// Execute a raw SurrealQL query with bound variables.
    pub async fn query_with_vars(
        &self,
        surql: &str,
        vars: BTreeMap<String, Value>,
    ) -> Result<Value> {
        self.require_connected()?;
        let mut builder = self.inner.query(surql.to_owned());
        for (k, v) in vars {
            builder = builder.bind((k, v));
        }
        let mut response = builder.await.map_err(|e| query_err(&e))?;
        let count = response.num_statements();
        let mut out = Vec::with_capacity(count);
        for i in 0..count {
            // Take the raw SurrealDB `Value` (which preserves Record IDs,
            // Durations, etc.) and convert to `serde_json::Value` via
            // the SDK's built-in JSON mapping. Trying to deserialize
            // directly into `serde_json::Value` fails because SurrealDB
            // uses tagged enums on the wire.
            let raw: surrealdb::Value = response.take(i).map_err(|e| query_err(&e))?;
            out.push(surreal_value_to_json(raw));
        }
        Ok(Value::Array(out))
    }

    /// Typed `SELECT` against a table or record ID (`"user"` / `"user:alice"`).
    pub async fn select<T: DeserializeOwned>(&self, target: &str) -> Result<Vec<T>> {
        self.require_connected()?;
        let (table, id) = split_target(target);
        let out: Vec<T> = if let Some(id) = id {
            let single: Option<T> = self
                .inner
                .select((table.to_owned(), id.to_owned()))
                .await
                .map_err(|e| query_err(&e))?;
            single.into_iter().collect()
        } else {
            self.inner
                .select(table.to_owned())
                .await
                .map_err(|e| query_err(&e))?
        };
        Ok(out)
    }

    /// Typed `CREATE`. Returns the created record.
    pub async fn create<T>(&self, target: &str, data: T) -> Result<T>
    where
        T: Serialize + DeserializeOwned + Send + Sync + 'static,
    {
        self.require_connected()?;
        let (table, id) = split_target(target);
        let result: Option<T> = if let Some(id) = id {
            self.inner
                .create((table.to_owned(), id.to_owned()))
                .content(data)
                .await
                .map_err(|e| query_err(&e))?
        } else {
            // Table-level create on a target without an id returns `Option<T>`.
            self.inner
                .create(table.to_owned())
                .content(data)
                .await
                .map_err(|e| query_err(&e))?
        };
        result.ok_or_else(|| SurqlError::Query {
            reason: format!("CREATE on {target} returned no record"),
        })
    }

    /// Typed `UPDATE`. Returns the updated record.
    pub async fn update<T>(&self, target: &str, data: T) -> Result<T>
    where
        T: Serialize + DeserializeOwned + Send + Sync + 'static,
    {
        self.require_connected()?;
        let (table, id) = split_target(target);
        let result: Option<T> = if let Some(id) = id {
            self.inner
                .update((table.to_owned(), id.to_owned()))
                .content(data)
                .await
                .map_err(|e| query_err(&e))?
        } else {
            let mut list: Vec<T> = self
                .inner
                .update(table.to_owned())
                .content(data)
                .await
                .map_err(|e| query_err(&e))?;
            if list.is_empty() {
                None
            } else {
                Some(list.remove(0))
            }
        };
        result.ok_or_else(|| SurqlError::Query {
            reason: format!("UPDATE on {target} returned no record"),
        })
    }

    /// Typed `MERGE`. Returns the merged record.
    ///
    /// The input (`D`) is a partial patch; the output (`T`) is the full
    /// merged record. Pass a `serde_json::Value` or a dedicated patch
    /// struct for `D`.
    pub async fn merge<D, T>(&self, target: &str, data: D) -> Result<T>
    where
        D: Serialize + Send + Sync + 'static,
        T: DeserializeOwned + Send + Sync + 'static,
    {
        self.require_connected()?;
        let (table, id) = split_target(target);
        let result: Option<T> = if let Some(id) = id {
            self.inner
                .update((table.to_owned(), id.to_owned()))
                .merge(data)
                .await
                .map_err(|e| query_err(&e))?
        } else {
            let mut list: Vec<T> = self
                .inner
                .update(table.to_owned())
                .merge(data)
                .await
                .map_err(|e| query_err(&e))?;
            if list.is_empty() {
                None
            } else {
                Some(list.remove(0))
            }
        };
        result.ok_or_else(|| SurqlError::Query {
            reason: format!("MERGE on {target} returned no record"),
        })
    }

    /// Typed `DELETE`. Returns the deleted records.
    pub async fn delete<T: DeserializeOwned>(&self, target: &str) -> Result<Vec<T>> {
        self.require_connected()?;
        let (table, id) = split_target(target);
        let out: Vec<T> = if let Some(id) = id {
            let deleted: Option<T> = self
                .inner
                .delete((table.to_owned(), id.to_owned()))
                .await
                .map_err(|e| query_err(&e))?;
            deleted.into_iter().collect()
        } else {
            self.inner
                .delete(table.to_owned())
                .await
                .map_err(|e| query_err(&e))?
        };
        Ok(out)
    }

    /// Server-side health check (wraps `Surreal::health`).
    pub async fn health(&self) -> Result<bool> {
        self.require_connected()?;
        match self.inner.health().await {
            Ok(()) => Ok(true),
            Err(_) => Ok(false),
        }
    }

    // -- internal ----------------------------------------------------------

    async fn connect_once(&self) -> Result<()> {
        let timeout = Duration::from_secs_f64(self.config.timeout().max(0.1));

        tokio::time::timeout(timeout, self.inner.connect(self.config.url().to_owned()))
            .await
            .map_err(|_| SurqlError::Connection {
                reason: format!("connect timed out after {timeout:?}"),
            })?
            .map_err(|e| connection_err(&e))?;

        if let (Some(user), Some(pass)) = (self.config.username(), self.config.password()) {
            self.inner
                .signin(SdkRoot {
                    username: user,
                    password: pass,
                })
                .await
                .map_err(|e| connection_err(&e))?;
        }

        self.inner
            .use_ns(self.config.namespace().to_owned())
            .use_db(self.config.database().to_owned())
            .await
            .map_err(|e| connection_err(&e))?;

        Ok(())
    }

    fn backoff_for(&self, attempt: u32) -> Duration {
        let min = self.config.retry_min_wait();
        let max = self.config.retry_max_wait();
        let mult = self.config.retry_multiplier();
        let exp = f64::from(attempt.saturating_sub(1));
        let secs = (min * mult.powf(exp)).clamp(min, max);
        Duration::from_secs_f64(secs)
    }

    fn require_connected(&self) -> Result<()> {
        if self.is_connected() {
            Ok(())
        } else {
            Err(SurqlError::Connection {
                reason: "client is not connected to database".into(),
            })
        }
    }
}

impl From<surrealdb::Error> for SurqlError {
    fn from(err: surrealdb::Error) -> Self {
        // Prefer the underlying string; SurrealDB's Display already
        // covers both `Api` and `Db` variants.
        classify_surrealdb_error(&err, err.to_string())
    }
}

fn classify_surrealdb_error(err: &surrealdb::Error, msg: String) -> SurqlError {
    match err {
        surrealdb::Error::Api(api) => {
            let api_msg = api.to_string();
            let lowered = api_msg.to_lowercase();
            if lowered.contains("connection")
                || lowered.contains("not connected")
                || lowered.contains("connect")
                || lowered.contains("websocket")
                || lowered.contains("timed out")
            {
                SurqlError::Connection { reason: msg }
            } else if lowered.contains("transaction") {
                SurqlError::Transaction { reason: msg }
            } else {
                SurqlError::Query { reason: msg }
            }
        }
        surrealdb::Error::Db(_) => SurqlError::Database { reason: msg },
    }
}

pub(crate) fn connection_err(err: &surrealdb::Error) -> SurqlError {
    SurqlError::Connection {
        reason: err.to_string(),
    }
}

pub(crate) fn query_err(err: &surrealdb::Error) -> SurqlError {
    match err {
        surrealdb::Error::Api(_) => SurqlError::Query {
            reason: err.to_string(),
        },
        surrealdb::Error::Db(_) => SurqlError::Database {
            reason: err.to_string(),
        },
    }
}

fn surreal_value_to_json(value: surrealdb::Value) -> Value {
    // `surrealdb::Value` is a thin wrapper over the core `Value`, which
    // implements `From<CoreValue> for serde_json::Value`.
    Value::from(value.into_inner())
}

fn payload_str(map: &serde_json::Map<String, Value>, key: &str) -> Result<String> {
    match map.get(key) {
        Some(Value::String(s)) => Ok(s.clone()),
        Some(_) => Err(SurqlError::Validation {
            reason: format!("credential field {key:?} must be a string"),
        }),
        None => Err(SurqlError::Validation {
            reason: format!("credential field {key:?} is missing"),
        }),
    }
}

fn split_target(target: &str) -> (&str, Option<&str>) {
    match target.split_once(':') {
        Some((table, id)) if !table.is_empty() && !id.is_empty() => (table, Some(id)),
        _ => (target, None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connection::auth::RootCredentials;

    #[test]
    fn new_validates_config() {
        let cfg = ConnectionConfig::default();
        let client = DatabaseClient::new(cfg).expect("valid default config");
        assert!(!client.is_connected());
    }

    #[test]
    fn new_rejects_invalid_config() {
        let bad = ConnectionConfig {
            db_url: "ftp://nope".into(),
            ..Default::default()
        };
        assert!(DatabaseClient::new(bad).is_err());
    }

    #[test]
    fn split_target_detects_record_id() {
        assert_eq!(split_target("user"), ("user", None));
        assert_eq!(split_target("user:alice"), ("user", Some("alice")));
        assert_eq!(split_target(":alice"), (":alice", None));
        assert_eq!(split_target("user:"), ("user:", None));
    }

    #[test]
    fn payload_str_round_trip() {
        let creds = RootCredentials::new("root", "secret");
        let m = creds.to_signin_payload();
        assert_eq!(payload_str(&m, "username").unwrap(), "root");
        assert_eq!(payload_str(&m, "password").unwrap(), "secret");
        assert!(payload_str(&m, "missing").is_err());
    }

    #[tokio::test]
    async fn disconnect_when_never_connected_is_ok() {
        let client = DatabaseClient::new(ConnectionConfig::default()).unwrap();
        client.disconnect().await.unwrap();
        assert!(!client.is_connected());
    }

    #[tokio::test]
    async fn operations_fail_when_not_connected() {
        let client = DatabaseClient::new(ConnectionConfig::default()).unwrap();
        let err = client.query("INFO FOR DB").await.unwrap_err();
        assert!(matches!(err, SurqlError::Connection { .. }));
    }

    #[test]
    fn backoff_respects_bounds() {
        let cfg = ConnectionConfig {
            db_retry_min_wait: 0.5,
            db_retry_max_wait: 4.0,
            db_retry_multiplier: 2.0,
            ..Default::default()
        };
        let client = DatabaseClient::new(cfg).unwrap();
        let a1 = client.backoff_for(1);
        let a5 = client.backoff_for(5);
        assert!(a1 >= Duration::from_secs_f64(0.5));
        assert!(a5 <= Duration::from_secs_f64(4.0));
    }

    #[test]
    fn surrealdb_error_maps_to_surql_error() {
        // We can't easily construct a real surrealdb::Error here, but we
        // can assert the mapper accepts a constructed Db variant.
        let core = surrealdb::error::Db::Thrown("boom".into());
        let err: SurqlError = surrealdb::Error::from(core).into();
        assert!(matches!(err, SurqlError::Database { .. }));
    }
}
