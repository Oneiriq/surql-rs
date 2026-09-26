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
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use serde::{de::DeserializeOwned, Serialize};
use serde_json::Value;
use surrealdb::engine::any::Any;
use surrealdb::opt::auth::{
    Database as SdkDatabase, Namespace as SdkNamespace, Record as SdkRecord, Root as SdkRoot, Token,
};
use surrealdb::opt::Config as SdkConfig;
use surrealdb::types::SurrealValue;
use surrealdb::{IndexedResults, Surreal};
use tokio::sync::RwLock;
use tokio::time::sleep;

use crate::connection::auth::{AuthType, Credentials, ScopeCredentials, TokenAuth};
use crate::connection::config::ConnectionConfig;
use crate::error::{Result, SurqlError};

mod convert;
mod errors;
#[cfg(test)]
mod tests;

pub(crate) use convert::render_target;
use convert::{first_row_typed, flatten_rows_typed, payload_str, seconds, statement_results};
use errors::{connection_err, query_err, request_says_session_expired, sdk_says_already_connected};

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
///
/// `Debug` output shows the config with its secrets redacted and leaves
/// the SDK handle out.
#[derive(Clone)]
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
    /// Whether the shared engine session currently holds the authority
    /// [`DatabaseClient::connect`] established from the config. Only then
    /// may an expired session be re-established from the config
    /// credentials: the replay reproduces exactly the session it held.
    /// Shared by every clone because the clones share the one session;
    /// set only by `connect`, cleared by `signin`, `signup`,
    /// `authenticate`, `invalidate` and `disconnect`, each of which gives
    /// the session some other identity (or none). Replaying the config
    /// credentials after any of them would swap that identity for the
    /// service's own.
    config_authority: Arc<AtomicBool>,
    /// Whether this client is a [`DatabaseClient::caller_session`], whose
    /// authority is a caller's token that only the caller layer may renew.
    /// Such a client never replays and refuses `connect`, which would sign
    /// it in with the config credentials.
    caller_bound: bool,
}

impl fmt::Debug for DatabaseClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DatabaseClient")
            .field("config", &self.config)
            .field("connected", &self.is_connected())
            .field("caller_session", &self.caller_bound)
            .finish_non_exhaustive()
    }
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
            config_authority: Arc::new(AtomicBool::new(false)),
            caller_bound: false,
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
    ///
    /// A successful connect is what lets the client heal an expired
    /// session from the config credentials (see
    /// [`DatabaseClient::query_with_vars`]).
    ///
    /// # Errors
    ///
    /// Returns [`SurqlError::Connection`] on a
    /// [`DatabaseClient::caller_session`]: connecting signs the session in
    /// with the config credentials, which would hand the caller the
    /// service's authority.
    pub async fn connect(&self) -> Result<()> {
        if self.caller_bound {
            return Err(SurqlError::Connection {
                reason: "a caller session cannot connect: it would sign the caller's session \
                         in with the config credentials"
                    .into(),
            });
        }
        // Reconnect is idempotent: disconnect any previous session first.
        if *self.connected.read().await {
            self.disconnect().await.ok();
        }

        let attempts = self.config.retry_max_attempts().max(1);
        let mut last_err: Option<SurqlError> = None;

        for attempt in 1..=attempts {
            match self.connect_once().await {
                Ok(()) => {
                    self.config_authority.store(true, Ordering::SeqCst);
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
        self.leave_config_authority();
        // The SDK exposes `invalidate` to clear auth, but there is no
        // explicit disconnect on `Surreal<Any>` beyond dropping the
        // handle. We invalidate the session so subsequent calls fail
        // cleanly.
        self.inner.invalidate().await.ok();
        Ok(())
    }

    /// Sign in using one of the four auth levels.
    ///
    /// The shared session takes the signed-in identity, so from here on
    /// an expired session surfaces its error instead of being replayed
    /// from the config credentials, until the next
    /// [`DatabaseClient::connect`].
    pub async fn signin<C: Credentials + ?Sized>(&self, creds: &C) -> Result<TokenAuth> {
        self.require_connected()?;
        self.leave_config_authority();
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
    ///
    /// Like [`DatabaseClient::signin`], this gives the shared session the
    /// new user's identity and ends config-credential replay until the next
    /// [`DatabaseClient::connect`].
    pub async fn signup(&self, creds: &ScopeCredentials) -> Result<TokenAuth> {
        self.require_connected()?;
        self.leave_config_authority();
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
    ///
    /// Like [`DatabaseClient::signin`], this gives the shared session the
    /// token's identity and ends config-credential replay until the next
    /// [`DatabaseClient::connect`].
    pub async fn authenticate(&self, token: &str) -> Result<()> {
        self.require_connected()?;
        self.leave_config_authority();
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
    ///
    /// The returned client never re-establishes an expired session from
    /// the config credentials, and its [`DatabaseClient::connect`] refuses:
    /// either would replace the caller's identity with the service's.
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
            // The session holds the CALLER's authority, never the
            // config's, and being caller-bound keeps it that way: no
            // replay, no connect.
            config_authority: Arc::new(AtomicBool::new(false)),
            caller_bound: true,
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
    ///
    /// The session is left unauthenticated, so config-credential replay
    /// ends until the next [`DatabaseClient::connect`].
    pub async fn invalidate(&self) -> Result<()> {
        self.require_connected()?;
        self.leave_config_authority();
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
    /// on all traffic until restarted). Where the session holds the
    /// authority [`DatabaseClient::connect`] established from the config
    /// credentials, that something is this method: the session is
    /// re-established and the request retried, once.
    ///
    /// Only the engine's refusal of the whole request counts as expiry.
    /// The engine checks the session before it runs any statement, so the
    /// retry cannot repeat a write; an error raised by one statement of
    /// the request (a `THROW`, a failed `ASSERT`) is never retried,
    /// whatever its message says.
    pub async fn query_with_vars(
        &self,
        surql: &str,
        vars: BTreeMap<String, Value>,
    ) -> Result<Value> {
        self.run_query(surql, vars).await
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
    /// same shape as [`query_with_vars`], and an expired session heals the
    /// same way.
    ///
    /// [`query_with_vars`]: DatabaseClient::query_with_vars
    pub async fn query_with_surreal_vars(
        &self,
        surql: &str,
        vars: BTreeMap<String, surrealdb::types::Value>,
    ) -> Result<Value> {
        self.run_query(surql, vars).await
    }

    /// The query funnel behind both public variants: send, heal an expired
    /// session once where that is safe, then unpack every statement.
    async fn run_query<V>(&self, surql: &str, vars: BTreeMap<String, V>) -> Result<Value>
    where
        V: SurrealValue + Clone,
    {
        self.require_connected()?;
        // Cloned up front only where a replay is possible, because the
        // retry needs the variables after the first attempt consumed them
        // (and they can carry `Value::Bytes` payloads).
        let retry_vars = self.can_replay_session().then(|| vars.clone());
        let response = match self.send_query(surql, vars).await {
            Err(err) if request_says_session_expired(&err) => {
                // Re-checked after the failure: a signin on another clone
                // while the request was in flight changed whose session
                // this is.
                let Some(vars) = retry_vars.filter(|_| self.can_replay_session()) else {
                    return Err(query_err(&err));
                };
                self.replay_session().await?;
                self.send_query(surql, vars).await
            }
            other => other,
        }
        .map_err(|e| query_err(&e))?;
        statement_results(response)
    }

    /// Send one request. `Err` here is the engine refusing the request as
    /// a whole; per-statement errors stay inside the returned results.
    async fn send_query<V: SurrealValue>(
        &self,
        surql: &str,
        vars: BTreeMap<String, V>,
    ) -> std::result::Result<IndexedResults, surrealdb::Error> {
        let mut builder = self.inner.query(surql.to_owned());
        for (k, v) in vars {
            // In 3.x the `bind` input must implement `SurrealValue`;
            // `(String, V)` qualifies because both components do (and
            // tuples are encoded as 2-element arrays which
            // `into_variables` unpacks as key/value chunks). A native
            // `surrealdb::types::Value` keeps its type, including
            // `Value::Bytes`.
            builder = builder.bind((k, v));
        }
        builder.await
    }

    /// Typed `SELECT` against a table or record ID (`"user"` / `"user:alice"`).
    ///
    /// Internally routes through raw SurrealQL + `serde_json::Value`
    /// so callers only need `serde::de::DeserializeOwned`; the 3.x
    /// SDK's typed `select` would force a `SurrealValue` bound on
    /// `T`, which would be a breaking change for existing users.
    ///
    /// # Targets
    ///
    /// This and the other typed CRUD methods take `target` as either a
    /// table name (`"user"`) or a `table:key` record id (`"user:alice"`),
    /// never as SurrealQL. A record key is always read as a literal string
    /// key (an integer when it is all digits; `⟨…⟩` and backtick quoting
    /// are understood), so a key built from user input cannot end the
    /// statement: `"user:x; REMOVE TABLE user"` names the record whose key
    /// is `x; REMOVE TABLE user`. Record-key expressions (`user:ulid()`,
    /// ranges, array or object keys) are not supported here; write the
    /// statement with [`DatabaseClient::query_with_vars`] instead.
    ///
    /// # Errors
    ///
    /// [`SurqlError::Validation`] when `target` is neither a table name nor
    /// a `table:key` record id.
    pub async fn select<T: DeserializeOwned>(&self, target: &str) -> Result<Vec<T>> {
        self.require_connected()?;
        let surql = format!("SELECT * FROM {};", render_target(target)?);
        let raw = self.query(&surql).await?;
        flatten_rows_typed(&raw)
    }

    /// Typed `CREATE`. Returns the created record.
    ///
    /// `target` follows the rules on [`DatabaseClient::select`].
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
        let surql = format!("CREATE {} CONTENT $data;", render_target(target)?);
        let raw = self.query_with_vars(&surql, vars).await?;
        first_row_typed(&raw)?.ok_or_else(|| SurqlError::Query {
            reason: format!("CREATE on {target} returned no record"),
        })
    }

    /// Typed `UPDATE`. Returns the updated record.
    ///
    /// `target` follows the rules on [`DatabaseClient::select`].
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
        let surql = format!("UPDATE {} CONTENT $data;", render_target(target)?);
        let raw = self.query_with_vars(&surql, vars).await?;
        first_row_typed(&raw)?.ok_or_else(|| SurqlError::Query {
            reason: format!("UPDATE on {target} returned no record"),
        })
    }

    /// Typed `MERGE`. Returns the merged record.
    ///
    /// The input (`D`) is a partial patch; the output (`T`) is the full
    /// merged record. Pass a `serde_json::Value` or a dedicated patch
    /// struct for `D`. `target` follows the rules on
    /// [`DatabaseClient::select`].
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
        let surql = format!("UPDATE {} MERGE $patch;", render_target(target)?);
        let raw = self.query_with_vars(&surql, vars).await?;
        first_row_typed(&raw)?.ok_or_else(|| SurqlError::Query {
            reason: format!("MERGE on {target} returned no record"),
        })
    }

    /// Typed `DELETE`. Returns the deleted records.
    ///
    /// `target` follows the rules on [`DatabaseClient::select`].
    pub async fn delete<T: DeserializeOwned>(&self, target: &str) -> Result<Vec<T>> {
        self.require_connected()?;
        let surql = format!("DELETE {} RETURN BEFORE;", render_target(target)?);
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
    /// session holds the authority `connect` established, the config holds
    /// credentials, so a replay reproduces exactly that authority, and this
    /// client is not a caller session.
    fn can_replay_session(&self) -> bool {
        !self.caller_bound
            && self.config_authority.load(Ordering::SeqCst)
            && self.config.username().is_some()
            && self.config.password().is_some()
    }

    /// Record that the shared session no longer holds the config's
    /// authority. Called BEFORE the identity-changing request, so a request
    /// that fails half-way leaves replay off rather than on.
    fn leave_config_authority(&self) {
        self.config_authority.store(false, Ordering::SeqCst);
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
        let timeout = seconds(self.config.timeout().max(0.1))?;

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
        // Validation bounds every input, so this only falls back on a
        // config mutated past `validate` (the fields are public).
        seconds(secs).unwrap_or(Duration::from_secs(1))
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
