//! Async SurrealDB client wrapper.
//!
//! Port of `surql/connection/client.py`. Wraps
//! [`surrealdb::Surreal<surrealdb::engine::any::Any>`], which picks the
//! underlying engine (WebSocket, HTTP, in-memory, file, `SurrealKV`) from
//! the URL at runtime. Retry logic, connection timeout, and
//! auth-level dispatch mirror the Python client one-for-one.
//!
//! Targets the `surrealdb` crate 3.x line, which removed the
//! top-level `api::` module in favour of `engine::`, replaced the
//! opaque `Jwt` return on signin with a structured `Token`, and made
//! the `SurrealValue` trait the typed-call envelope. For the typed
//! CRUD helpers exposed by [`DatabaseClient`] we intentionally round
//! through raw SurrealQL + `serde_json::Value` so callers only need
//! `serde::Serialize + serde::de::DeserializeOwned` bounds on their
//! types (not `SurrealValue`).

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use serde::{de::DeserializeOwned, Serialize};
use serde_json::Value;
use surrealdb::engine::any::Any;
use surrealdb::opt::auth::{
    Database as SdkDatabase, Namespace as SdkNamespace, Record as SdkRecord, Root as SdkRoot, Token,
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
        let token = match creds.auth_type() {
            AuthType::Root => {
                let username = payload_str(&payload, "username")?;
                let password = payload_str(&payload, "password")?;
                self.inner
                    .signin(SdkRoot { username, password })
                    .await
                    .map_err(|e| connection_err(&e))?
            }
            AuthType::Namespace => {
                let namespace = payload_str(&payload, "namespace")?;
                let username = payload_str(&payload, "username")?;
                let password = payload_str(&payload, "password")?;
                self.inner
                    .signin(SdkNamespace {
                        namespace,
                        username,
                        password,
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
                        namespace,
                        database,
                        username,
                        password,
                    })
                    .await
                    .map_err(|e| connection_err(&e))?
            }
            AuthType::Scope => {
                let namespace = payload_str(&payload, "namespace")?;
                let database = payload_str(&payload, "database")?;
                let access = payload_str(&payload, "access")?;
                // Everything else is scope-defined vars. In v3 the
                // `Record` credential is generic over `P: SurrealValue`;
                // `serde_json::Value` implements it, so we bundle the
                // remaining credential fields into a JSON object.
                let mut params = serde_json::Map::new();
                for (k, v) in &payload {
                    if !matches!(k.as_str(), "namespace" | "database" | "access") {
                        params.insert(k.clone(), v.clone());
                    }
                }
                self.inner
                    .signin(SdkRecord {
                        namespace,
                        database,
                        access,
                        params: Value::Object(params),
                    })
                    .await
                    .map_err(|e| connection_err(&e))?
            }
        };
        Ok(TokenAuth::new(token.access.into_insecure_token()))
    }

    /// Sign up a scope user (record access).
    pub async fn signup(&self, creds: &ScopeCredentials) -> Result<TokenAuth> {
        self.require_connected()?;
        let mut params = serde_json::Map::new();
        for (k, v) in &creds.variables {
            params.insert(k.clone(), v.clone());
        }
        let token = self
            .inner
            .signup(SdkRecord {
                namespace: creds.namespace.clone(),
                database: creds.database.clone(),
                access: creds.access.clone(),
                params: Value::Object(params),
            })
            .await
            .map_err(|e| connection_err(&e))?;
        Ok(TokenAuth::new(token.access.into_insecure_token()))
    }

    /// Authenticate using a previously-issued JWT.
    pub async fn authenticate(&self, token: &str) -> Result<()> {
        self.require_connected()?;
        self.inner
            .authenticate(Token::from(token))
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
            // In 3.x the `bind` input must implement `SurrealValue`;
            // `(String, serde_json::Value)` qualifies because both
            // components do (and tuples are encoded as 2-element
            // arrays which `into_variables` unpacks as key/value
            // chunks).
            builder = builder.bind((k, v));
        }
        let mut response = builder.await.map_err(|e| query_err(&e))?;
        let count = response.num_statements();
        let mut out = Vec::with_capacity(count);
        for i in 0..count {
            // `IndexedResults::take(usize)` in 3.x only accepts
            // `surrealdb::types::Value` / `Vec<T>` / `Option<T>` for
            // index-based retrieval. Take the core `Value` (which
            // preserves record IDs, durations, decimals, etc.) and
            // downgrade to `serde_json::Value` via
            // `into_json_value`.
            let raw: surrealdb::types::Value = response.take(i).map_err(|e| query_err(&e))?;
            out.push(raw.into_json_value());
        }
        Ok(Value::Array(out))
    }

    /// Typed `SELECT` against a table or record ID (`"user"` / `"user:alice"`).
    ///
    /// Internally routes through raw SurrealQL + `serde_json::Value`
    /// so callers only need `serde::de::DeserializeOwned`; the 3.x
    /// SDK's typed `select` would force a `SurrealValue` bound on
    /// `T`, which would be a breaking change for existing users.
    pub async fn select<T: DeserializeOwned>(&self, target: &str) -> Result<Vec<T>> {
        self.require_connected()?;
        let surql = format!("SELECT * FROM {target};");
        let raw = self.query(&surql).await?;
        flatten_rows_typed(&raw)
    }

    /// Typed `CREATE`. Returns the created record.
    pub async fn create<T>(&self, target: &str, data: T) -> Result<T>
    where
        T: Serialize + DeserializeOwned + Send + Sync + 'static,
    {
        self.require_connected()?;
        let content = serde_json::to_value(&data).map_err(|e| SurqlError::Serialization {
            reason: e.to_string(),
        })?;
        let mut vars: BTreeMap<String, Value> = BTreeMap::new();
        vars.insert("data".into(), content);
        let surql = format!("CREATE {target} CONTENT $data;");
        let raw = self.query_with_vars(&surql, vars).await?;
        first_row_typed(&raw)?.ok_or_else(|| SurqlError::Query {
            reason: format!("CREATE on {target} returned no record"),
        })
    }

    /// Typed `UPDATE`. Returns the updated record.
    pub async fn update<T>(&self, target: &str, data: T) -> Result<T>
    where
        T: Serialize + DeserializeOwned + Send + Sync + 'static,
    {
        self.require_connected()?;
        let content = serde_json::to_value(&data).map_err(|e| SurqlError::Serialization {
            reason: e.to_string(),
        })?;
        let mut vars: BTreeMap<String, Value> = BTreeMap::new();
        vars.insert("data".into(), content);
        let surql = format!("UPDATE {target} CONTENT $data;");
        let raw = self.query_with_vars(&surql, vars).await?;
        first_row_typed(&raw)?.ok_or_else(|| SurqlError::Query {
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
        let patch = serde_json::to_value(&data).map_err(|e| SurqlError::Serialization {
            reason: e.to_string(),
        })?;
        let mut vars: BTreeMap<String, Value> = BTreeMap::new();
        vars.insert("patch".into(), patch);
        let surql = format!("UPDATE {target} MERGE $patch;");
        let raw = self.query_with_vars(&surql, vars).await?;
        first_row_typed(&raw)?.ok_or_else(|| SurqlError::Query {
            reason: format!("MERGE on {target} returned no record"),
        })
    }

    /// Typed `DELETE`. Returns the deleted records.
    pub async fn delete<T: DeserializeOwned>(&self, target: &str) -> Result<Vec<T>> {
        self.require_connected()?;
        let surql = format!("DELETE {target} RETURN BEFORE;");
        let raw = self.query(&surql).await?;
        flatten_rows_typed(&raw)
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
                    username: user.to_owned(),
                    password: pass.to_owned(),
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
        // 3.x unifies `Error` into a single struct with a `kind_str()`
        // discriminator and a human-readable message. Map the relevant
        // kinds onto the richer `SurqlError` taxonomy; fall back to a
        // substring match on the message for anything not yet modelled
        // in the typed details.
        classify_surrealdb_error(&err, err.to_string())
    }
}

fn classify_surrealdb_error(err: &surrealdb::Error, msg: String) -> SurqlError {
    if err.is_connection() {
        return SurqlError::Connection { reason: msg };
    }
    if err.is_query() || err.is_not_found() || err.is_not_allowed() || err.is_thrown() {
        return SurqlError::Query { reason: msg };
    }
    if err.is_serialization() {
        return SurqlError::Serialization { reason: msg };
    }
    let lowered = msg.to_lowercase();
    if lowered.contains("transaction") {
        return SurqlError::Transaction { reason: msg };
    }
    if lowered.contains("connect")
        || lowered.contains("not connected")
        || lowered.contains("websocket")
        || lowered.contains("timed out")
        || lowered.contains("subprotocol")
    {
        return SurqlError::Connection { reason: msg };
    }
    SurqlError::Database { reason: msg }
}

pub(crate) fn connection_err(err: &surrealdb::Error) -> SurqlError {
    SurqlError::Connection {
        reason: err.to_string(),
    }
}

pub(crate) fn query_err(err: &surrealdb::Error) -> SurqlError {
    classify_surrealdb_error(err, err.to_string())
}

/// Flatten every row in the raw `query()` response into a typed vector.
fn flatten_rows_typed<T: DeserializeOwned>(raw: &Value) -> Result<Vec<T>> {
    let mut out: Vec<T> = Vec::new();
    collect_rows(raw, &mut out)?;
    Ok(out)
}

fn collect_rows<T: DeserializeOwned>(value: &Value, out: &mut Vec<T>) -> Result<()> {
    match value {
        Value::Null => Ok(()),
        Value::Array(items) => {
            for item in items {
                collect_rows(item, out)?;
            }
            Ok(())
        }
        Value::Object(obj) => {
            if let Some(inner) = obj.get("result") {
                return collect_rows(inner, out);
            }
            let row: T = serde_json::from_value(Value::Object(obj.clone())).map_err(|e| {
                SurqlError::Serialization {
                    reason: e.to_string(),
                }
            })?;
            out.push(row);
            Ok(())
        }
        other => {
            let row: T =
                serde_json::from_value(other.clone()).map_err(|e| SurqlError::Serialization {
                    reason: e.to_string(),
                })?;
            out.push(row);
            Ok(())
        }
    }
}

fn first_row_typed<T: DeserializeOwned>(raw: &Value) -> Result<Option<T>> {
    let mut rows: Vec<T> = flatten_rows_typed(raw)?;
    Ok(if rows.is_empty() {
        None
    } else {
        Some(rows.remove(0))
    })
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
    fn flatten_rows_typed_handles_wrapped_and_flat_shapes() {
        #[derive(serde::Deserialize, Debug, PartialEq)]
        struct Row {
            name: String,
        }
        let wrapped = serde_json::json!([
            { "result": [{ "name": "alice" }, { "name": "bob" }] }
        ]);
        let rows: Vec<Row> = flatten_rows_typed(&wrapped).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].name, "alice");

        let flat = serde_json::json!([[{ "name": "carol" }]]);
        let rows: Vec<Row> = flatten_rows_typed(&flat).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].name, "carol");
    }

    #[test]
    fn first_row_typed_returns_none_for_empty_array() {
        #[derive(serde::Deserialize, Debug)]
        struct Row {
            #[allow(dead_code)]
            name: String,
        }
        let raw = serde_json::json!([[]]);
        let row: Option<Row> = first_row_typed(&raw).unwrap();
        assert!(row.is_none());
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
        // In 3.x `surrealdb::Error` is a single struct with typed
        // variants exposed via predicate methods. Use the public
        // constructor helpers to synthesise representative cases and
        // assert they map onto the expected `SurqlError` variants.
        let thrown: SurqlError = surrealdb::Error::thrown("boom".into()).into();
        assert!(matches!(thrown, SurqlError::Query { .. }));

        let connection: SurqlError = surrealdb::Error::connection("down".into(), None).into();
        assert!(matches!(connection, SurqlError::Connection { .. }));

        let internal: SurqlError = surrealdb::Error::internal("boom".into()).into();
        assert!(matches!(internal, SurqlError::Database { .. }));
    }
}
