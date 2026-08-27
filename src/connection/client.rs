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
use surrealdb::opt::Config as SdkConfig;
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
/// The client is `Clone`-able, and every clone shares ONE engine
/// session: the inner SDK handle rides an `Arc`, because the SDK's
/// own `Clone` mints a session per clone and sends session lifecycle
/// events the remote router can lose under concurrency, which
/// surfaces as `Session not found` on requests. A service that
/// clones its client per request wants one shared session; code that
/// needs an independent session says so through
/// [`DatabaseClient::caller_session`] or an explicit
/// `client.inner().clone()`.
#[derive(Debug, Clone)]
pub struct DatabaseClient {
    config: ConnectionConfig,
    inner: Arc<Surreal<Any>>,
    connected: Arc<RwLock<bool>>,
    /// Whether the underlying SDK engine has been connected. The SDK connects
    /// a handle once and rejects a second `connect` ("Already connected"), so
    /// a retry after a partially-successful attempt (engine up, signin or
    /// namespace selection failed) must skip the engine connect and resume at
    /// the step that actually failed -- otherwise every retry dies on the
    /// re-connect and its error masks the real one.
    engine_connected: Arc<RwLock<bool>>,
    /// Whether an expired engine session may be re-established from the
    /// config credentials. On by default: for a client whose authority IS
    /// the config (the root/owner service shape), a replay reproduces
    /// exactly the session it held. Off for [`DatabaseClient::caller_session`]
    /// clones, whose authority is a caller's token that only the caller
    /// layer may renew -- replaying config credentials there would swap the
    /// caller's identity for the service's.
    replay_expired_session: bool,
}

impl DatabaseClient {
    /// Build a new client. Does **not** open a network connection; call
    /// [`DatabaseClient::connect`] for that.
    pub fn new(config: ConnectionConfig) -> Result<Self> {
        config.validate()?;
        Ok(Self {
            config,
            inner: Arc::new(Surreal::init()),
            connected: Arc::new(RwLock::new(false)),
            engine_connected: Arc::new(RwLock::new(false)),
            replay_expired_session: true,
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
    /// bounded by [`ConnectionConfig::timeout`]. The underlying engine
    /// connects at most once per handle; a retry -- or a reconnect on an
    /// already-connected client -- resumes at the step that failed
    /// (credential signin, namespace selection), so the error that surfaces
    /// is the real failure, never the SDK's "Already connected" rejection.
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

    /// Open an independent engine session over the same connection
    /// and bind it to a caller identity.
    ///
    /// A cloned SDK handle is its own engine session, so the returned
    /// client authenticates the token without touching this client's
    /// session, and both run side by side on one connection. The
    /// engine then evaluates `PERMISSIONS` clauses against the caller
    /// session while this client keeps its own authority. The session
    /// ends when the returned client drops. Namespace and database
    /// come from the token's claims, which is why none are selected
    /// here.
    ///
    /// The token must come from a `DEFINE ACCESS ... TYPE RECORD`
    /// method and carry an `id` claim: the engine binds a record
    /// identity only then, and a session without one holds system
    /// authority that `PERMISSIONS` clauses do not filter. This
    /// method verifies the binding and refuses otherwise, so a wrong
    /// token kind errors here instead of yielding an unfiltered
    /// session. Enforcement does not depend on the engine holding
    /// credentials: a record session is constrained even on an open
    /// engine, where only anonymous sessions act as owner.
    pub async fn caller_session(&self, token: &str) -> Result<DatabaseClient> {
        self.require_connected()?;
        let session = DatabaseClient {
            config: self.config.clone(),
            // An explicit SDK-level clone: THIS is the one place a
            // fresh engine session is wanted.
            inner: Arc::new((*self.inner).clone()),
            // Fresh flags: disconnecting or invalidating the caller
            // session must leave the parent client's state alone.
            connected: Arc::new(RwLock::new(true)),
            engine_connected: Arc::new(RwLock::new(true)),
            // Never replay config credentials on a caller-bound session:
            // the whole point of this session is that it holds the
            // CALLER's authority, and a service-credential replay would
            // silently hand it the service's.
            replay_expired_session: false,
        };
        session
            .inner
            .authenticate(Token::from(token))
            .await
            .map_err(|e| connection_err(&e))?;
        let auth = session.query("RETURN $auth;").await?;
        let bound = auth.get(0).is_some_and(|v| !v.is_null());
        if !bound {
            return Err(SurqlError::Connection {
                reason: "the engine bound no record identity to the session: the token \
                         is not from a record access method or lacks an `id` claim"
                    .to_owned(),
            });
        }
        Ok(session)
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
    ///
    /// A long-lived connection's authenticated session can expire
    /// server-side while the socket stays healthy, after which every
    /// request fails "The session has expired" until something
    /// re-authenticates (observed in production as a service erroring
    /// on all traffic until restarted). Where this client's authority
    /// is the config's own credentials, that something is this method:
    /// the session is re-established and the statement retried, once.
    pub async fn query_with_vars(
        &self,
        surql: &str,
        vars: BTreeMap<String, Value>,
    ) -> Result<Value> {
        self.require_connected()?;
        // Cloned up front only where a replay is possible, because the
        // retry needs the variables after the first attempt consumed them.
        let retry_vars = self.can_replay_session().then(|| vars.clone());
        match self.run_json_query(surql, vars).await {
            Err(err) if retry_vars.is_some() && err_says_session_expired(&err) => {
                self.replay_session().await?;
                self.run_json_query(surql, retry_vars.unwrap_or_default())
                    .await
            }
            other => other,
        }
    }

    async fn run_json_query(&self, surql: &str, vars: BTreeMap<String, Value>) -> Result<Value> {
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

    /// Execute a raw SurrealQL query, binding native
    /// [`surrealdb::types::Value`] variables.
    ///
    /// This is the binary-safe sibling of [`query_with_vars`]. The JSON path
    /// cannot carry raw bytes — `serde_json::Value` has no byte-string variant,
    /// so a `Vec<u8>` would round-trip as a JSON array of numbers and arrive
    /// server-side as an `array<int>`, not a `bytes` value. Binding a
    /// [`surrealdb::types::Value::Bytes`](surrealdb::types::Value) directly
    /// preserves the `bytes` type, which is what the file `put` API needs.
    ///
    /// Each statement's result is returned as one entry of a JSON array, the
    /// same shape as [`query_with_vars`].
    ///
    /// [`query_with_vars`]: DatabaseClient::query_with_vars
    pub async fn query_with_surreal_vars(
        &self,
        surql: &str,
        vars: BTreeMap<String, surrealdb::types::Value>,
    ) -> Result<Value> {
        self.require_connected()?;
        // Same expired-session replay as `query_with_vars`. The clone can
        // carry `Value::Bytes` payloads, so it is taken only where a
        // replay is actually possible.
        let retry_vars = self.can_replay_session().then(|| vars.clone());
        match self.run_surreal_query(surql, vars).await {
            Err(err) if retry_vars.is_some() && err_says_session_expired(&err) => {
                self.replay_session().await?;
                self.run_surreal_query(surql, retry_vars.unwrap_or_default())
                    .await
            }
            other => other,
        }
    }

    async fn run_surreal_query(
        &self,
        surql: &str,
        vars: BTreeMap<String, surrealdb::types::Value>,
    ) -> Result<Value> {
        let mut builder = self.inner.query(surql.to_owned());
        for (k, v) in vars {
            // `(String, surrealdb::types::Value)` implements `SurrealValue`
            // (both components do), so it binds as a 2-element key/value chunk
            // exactly like the JSON path — but the value keeps its native
            // type, including `Value::Bytes`.
            builder = builder.bind((k, v));
        }
        let mut response = builder.await.map_err(|e| query_err(&e))?;
        let count = response.num_statements();
        let mut out = Vec::with_capacity(count);
        for i in 0..count {
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

    /// True when a failed operation may be retried on a fresh session: the
    /// config holds credentials, so a replay reproduces exactly the
    /// authority this client was built with, and this client is not a
    /// caller session.
    fn can_replay_session(&self) -> bool {
        self.replay_expired_session
            && self.config.username().is_some()
            && self.config.password().is_some()
    }

    /// Re-establish the configured session on the live engine.
    /// [`DatabaseClient::connect_once`] with the engine already up is
    /// exactly that: credential signin plus namespace selection, no
    /// engine reconnect and no flag transitions, so concurrent requests
    /// on other clones never observe a disconnected client.
    async fn replay_session(&self) -> Result<()> {
        self.connect_once().await
    }

    async fn connect_once(&self) -> Result<()> {
        let timeout = Duration::from_secs_f64(self.config.timeout().max(0.1));

        // Connect the engine at most once per handle (the write lock also
        // serialises concurrent connectors). On later attempts -- a retry
        // after signin or namespace selection failed, or a reconnect -- the
        // engine is already up, so resume at the step that failed instead of
        // letting the SDK's "Already connected" rejection mask the real
        // error. The SDK reporting "already connected" itself just means the
        // engine is up by another path; treat it as such, not as a failure.
        {
            let mut engine_up = self.engine_connected.write().await;
            if !*engine_up {
                // Credentials also reach the engine at build time. An
                // embedded datastore built without a root user treats
                // every anonymous session as owner, so locking the
                // engine needs the user to exist before the first
                // session. Remote engines ignore the endpoint config
                // and authenticate through the signin below.
                let connect = match (self.config.username(), self.config.password()) {
                    (Some(user), Some(pass)) => self.inner.connect((
                        self.config.url().to_owned(),
                        SdkConfig::new().user(SdkRoot {
                            username: user.to_owned(),
                            password: pass.to_owned(),
                        }),
                    )),
                    _ => self.inner.connect(self.config.url().to_owned()),
                };
                match tokio::time::timeout(timeout, connect).await {
                    Err(_) => {
                        return Err(SurqlError::Connection {
                            reason: format!("connect timed out after {timeout:?}"),
                        })
                    }
                    Ok(Err(e)) if !sdk_says_already_connected(&e) => {
                        return Err(connection_err(&e))
                    }
                    Ok(_) => {}
                }
                *engine_up = true;
            }
        }

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

/// The SDK's rejection of a second `connect` on an already-connected handle.
/// (A message match, like [`classify_surrealdb_error`]'s fallbacks: the 3.x
/// error type does not discriminate this case.)
fn sdk_says_already_connected(err: &surrealdb::Error) -> bool {
    err.to_string().to_lowercase().contains("already connected")
}

/// The engine's refusal of a request whose authenticated session has
/// expired. (A message match on the mapped error, like
/// [`sdk_says_already_connected`]: the 3.x error type folds this case
/// into the query kind.)
fn err_says_session_expired(err: &SurqlError) -> bool {
    err.to_string()
        .to_lowercase()
        .contains("session has expired")
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

    /// Regression: a connect attempt that gets the engine up but fails a
    /// later step (here: root signin) used to die on the SDK's "Already
    /// connected" rejection on every retry, masking the real error behind
    /// it. Credentials now initialise embedded engines at build time, so
    /// the failing-signin state is constructed directly: the engine comes
    /// up without the config's credentials, the way an external engine
    /// with different credentials would look.
    #[tokio::test]
    async fn retries_surface_the_failing_step_not_already_connected() {
        let cfg = ConnectionConfig::builder()
            .url("mem://")
            .namespace("t")
            .database("t")
            .username("root")
            .password("wrong")
            .retry_max_attempts(3)
            .retry_min_wait(0.1)
            .retry_max_wait(1.0)
            .build()
            .unwrap();
        let client = DatabaseClient::new(cfg).unwrap();
        client.inner.connect("mem://".to_owned()).await.unwrap();
        *client.engine_connected.write().await = true;
        let err = client.connect().await.unwrap_err();
        let msg = err.to_string().to_lowercase();
        assert!(
            !msg.contains("already connected"),
            "the signin failure must surface, not the engine re-connect: {err}"
        );
        assert!(!client.is_connected());
    }

    /// Regression: reconnecting an already-connected client used to fail the
    /// same way (the entry disconnect only invalidates; the engine stays up,
    /// so the fresh `connect` died on "Already connected").
    #[tokio::test]
    async fn reconnect_on_the_same_client_succeeds() {
        let cfg = ConnectionConfig::builder()
            .url("mem://")
            .namespace("t")
            .database("t")
            .retry_max_attempts(1)
            .build()
            .unwrap();
        let client = DatabaseClient::new(cfg).unwrap();
        client.connect().await.unwrap();
        assert!(client.is_connected());
        client.connect().await.unwrap();
        assert!(client.is_connected(), "reconnect lands back in service");
        // And the session still works.
        client.query("INFO FOR DB").await.unwrap();
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

    /// A long-lived session the engine expires must heal in place on a
    /// client whose authority is the config credentials: sign in as a
    /// user whose sessions live one second, let it expire, and the next
    /// query must succeed by replaying the configured root session --
    /// the production incident (every request failing "The session has
    /// expired" until a restart) in miniature. On runs where the engine
    /// declines to enforce the expiry (embedded engines are not
    /// deterministic about it) the query succeeds directly, so the test
    /// can never false-fail; the runs that do enforce it exercise the
    /// whole replay path.
    #[tokio::test]
    async fn expired_session_heals_on_a_config_credentialed_client() {
        let cfg = ConnectionConfig::builder()
            .url("mem://")
            .namespace("t")
            .database("t")
            .username("root")
            .password("root")
            .retry_max_attempts(1)
            .build()
            .unwrap();
        let client = DatabaseClient::new(cfg).unwrap();
        client.connect().await.unwrap();
        client
            .query("DEFINE USER brief ON ROOT PASSWORD 'pw' ROLES OWNER DURATION FOR SESSION 1s;")
            .await
            .unwrap();
        client
            .signin(&RootCredentials::new("brief", "pw"))
            .await
            .unwrap();
        sleep(Duration::from_millis(2500)).await;
        client
            .query("INFO FOR DB")
            .await
            .expect("the expired session replays the config credentials and retries");
    }

    /// The replay guard is the security boundary, and it is pure logic:
    /// only a client whose authority IS the config credentials may
    /// replay them. Asserting the guard through the engine proved
    /// untestable -- whether an embedded engine refuses or quietly
    /// downgrades an expired session is not deterministic across runs --
    /// so the truth table is pinned here directly, private-field access
    /// standing in for [`DatabaseClient::caller_session`]'s construction
    /// (which is the one production source of `replay_expired_session:
    /// false`).
    #[test]
    fn replay_guard_truth_table() {
        let with_creds = ConnectionConfig::builder()
            .url("mem://")
            .namespace("t")
            .database("t")
            .username("root")
            .password("root")
            .build()
            .unwrap();
        let without_creds = ConnectionConfig::builder()
            .url("mem://")
            .namespace("t")
            .database("t")
            .build()
            .unwrap();

        let service = DatabaseClient::new(with_creds.clone()).unwrap();
        assert!(
            service.can_replay_session(),
            "config credentials + primary client: the one shape that replays"
        );

        let caller_shaped = DatabaseClient {
            replay_expired_session: false,
            ..DatabaseClient::new(with_creds).unwrap()
        };
        assert!(
            !caller_shaped.can_replay_session(),
            "a caller session never replays, even with config credentials present"
        );

        let anonymous = DatabaseClient::new(without_creds).unwrap();
        assert!(
            !anonymous.can_replay_session(),
            "no config credentials: nothing safe to replay"
        );
    }

    #[test]
    fn session_expiry_matcher_reads_the_mapped_error() {
        let expired = SurqlError::Query {
            reason: "The session has expired".into(),
        };
        assert!(err_says_session_expired(&expired));
        let other = SurqlError::Query {
            reason: "There was a problem with the database".into(),
        };
        assert!(!err_says_session_expired(&other));
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
