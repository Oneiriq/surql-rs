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
use surrealdb::types::{AuthError, ConnectionError, NotAllowedError, SurrealValue};
use surrealdb::{IndexedResults, Surreal};
use tokio::sync::RwLock;
use tokio::time::sleep;

use crate::connection::auth::{AuthType, Credentials, ScopeCredentials, TokenAuth};
use crate::connection::config::ConnectionConfig;
use crate::error::{Result, SurqlError};
use crate::types::escape::{is_identifier, quote_ident};
use crate::types::RecordID;

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
/// Read from the structured details, with a message match for SDK paths
/// that report it without them.
fn sdk_says_already_connected(err: &surrealdb::Error) -> bool {
    matches!(
        err.connection_details(),
        Some(ConnectionError::AlreadyConnected)
    ) || err.to_string().to_lowercase().contains("already connected")
}

/// The engine's refusal of a whole request because its authenticated
/// session has expired.
///
/// Only ever asked of the error a request as a whole failed with, never of
/// a per-statement result: a statement error carries user data (a `THROW`
/// message, the value a failed `ASSERT` rejected), so reading one as expiry
/// would let a stored value trigger a replay. The engine reports expiry as
/// a structured not-allowed error; the fallback, for a peer that sends the
/// kind without the details, is an exact match on the engine's fixed
/// message, never a substring.
fn request_says_session_expired(err: &surrealdb::Error) -> bool {
    let structured = matches!(
        err.not_allowed_details(),
        Some(NotAllowedError::Auth(AuthError::SessionExpired))
    );
    let fixed_message = (err.is_not_allowed() || err.is_internal())
        && err
            .message()
            .trim()
            .eq_ignore_ascii_case("the session has expired");
    structured || fixed_message
}

pub(crate) fn query_err(err: &surrealdb::Error) -> SurqlError {
    classify_surrealdb_error(err, err.to_string())
}

/// Unpack every statement of a response into one JSON array entry each.
/// A statement's error fails the whole call (and is never retried).
fn statement_results(mut response: IndexedResults) -> Result<Value> {
    let count = response.num_statements();
    let mut out = Vec::with_capacity(count);
    for i in 0..count {
        // `IndexedResults::take(usize)` in 3.x only accepts
        // `surrealdb::types::Value` / `Vec<T>` / `Option<T>` for
        // index-based retrieval. Take the core `Value` (which
        // preserves record IDs, durations, decimals, etc.) and
        // downgrade to `serde_json::Value` via `into_json_value`.
        let raw: surrealdb::types::Value = response.take(i).map_err(|e| query_err(&e))?;
        out.push(raw.into_json_value());
    }
    Ok(Value::Array(out))
}

/// Render a typed-CRUD or `LIVE SELECT` target as SurrealQL.
///
/// The target is data, never SurrealQL: a table name renders through
/// [`quote_ident`], anything with a `:` is parsed as a [`RecordID`], whose
/// `Display` quotes the key so it cannot end the statement, and anything
/// else is refused.
pub(crate) fn render_target(target: &str) -> Result<String> {
    let target = target.trim();
    if is_identifier(target) {
        return Ok(quote_ident(target));
    }
    if target.contains(':') {
        return RecordID::<()>::parse(target).map(|id| id.to_string());
    }
    Err(SurqlError::Validation {
        reason: format!("invalid target {target:?}: expected a table name or a table:id record id"),
    })
}

/// A config duration in seconds as a [`Duration`], refusing what
/// `Duration` cannot hold instead of panicking.
fn seconds(secs: f64) -> Result<Duration> {
    Duration::try_from_secs_f64(secs).map_err(|e| SurqlError::Validation {
        reason: format!("invalid duration of {secs} seconds: {e}"),
    })
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
    let rows: Vec<T> = flatten_rows_typed(raw)?;
    Ok(rows.into_iter().next())
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
    use crate::connection::auth_manager::AuthManager;

    #[test]
    fn new_validates_config() {
        let cfg = ConnectionConfig::default();
        let client = DatabaseClient::new(cfg).expect("valid default config");
        assert!(!client.is_connected());
    }

    /// Regression: the derived `Debug` printed the config's password (and
    /// the SDK handle); a client is routinely logged.
    #[test]
    fn debug_redacts_the_config_secrets() {
        let client = DatabaseClient::new(ConnectionConfig {
            db_url: "ws://svc:urlsecret@db.example/rpc".into(),
            db_user: Some("svc".into()),
            db_pass: Some("hunter2".into()),
            ..Default::default()
        })
        .unwrap();
        let shown = format!("{client:?}");
        assert!(!shown.contains("hunter2"), "{shown}");
        assert!(!shown.contains("urlsecret"), "{shown}");
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
        let raw = serde_json::json!([[]]);
        let row: Option<Value> = first_row_typed(&raw).unwrap();
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
    /// client whose session holds the config credentials' authority: give
    /// the config's own user one-second sessions, reconnect to start one,
    /// let it expire, and the next query must succeed by replaying the
    /// configured session -- the production incident (every request
    /// failing "The session has expired" until a restart) in miniature.
    /// On runs where the embedded engine declines to enforce the expiry
    /// the query succeeds directly, so the test can never false-fail; the
    /// runs that do enforce it exercise the whole replay path (the ws
    /// integration suite exercises it deterministically).
    #[tokio::test]
    async fn expired_session_heals_on_a_config_credentialed_client() {
        let client = DatabaseClient::new(root_mem_config("heal")).unwrap();
        client.connect().await.unwrap();
        client
            .query(
                "DEFINE USER OVERWRITE root ON ROOT PASSWORD 'root' ROLES OWNER \
                 DURATION FOR SESSION 1s;",
            )
            .await
            .unwrap();
        client.connect().await.unwrap();
        sleep(Duration::from_millis(2500)).await;
        client
            .query("INFO FOR DB")
            .await
            .expect("the expired session replays the config credentials and retries");
    }

    /// The replay guard is the security boundary: only a session whose
    /// authority IS the config credentials may have them replayed. The
    /// truth table runs every transition through the real methods on an
    /// embedded engine, so the guard is pinned to what `connect`,
    /// `signin`, `signup`, `authenticate`, `invalidate`, `disconnect` and
    /// `caller_session` actually do, not to a hand-set field.
    #[tokio::test]
    async fn replay_guard_truth_table() {
        let service = DatabaseClient::new(root_mem_config("truth")).unwrap();
        assert!(
            !service.can_replay_session(),
            "never connected: no session to heal"
        );

        service.connect().await.unwrap();
        assert!(
            service.can_replay_session(),
            "config credentials + connect: the one shape that replays"
        );
        assert!(
            service.clone().can_replay_session(),
            "clones share the session, so they share the authority"
        );

        let token = service
            .signin(&RootCredentials::new("root", "root"))
            .await
            .unwrap();
        assert!(
            !service.can_replay_session(),
            "signin gives the session another identity"
        );
        service.connect().await.unwrap();
        assert!(service.can_replay_session(), "connect restores it");

        let clone = service.clone();
        clone.authenticate(&token.token).await.unwrap();
        assert!(
            !service.can_replay_session(),
            "authenticate on ANY clone changes the shared session"
        );
        service.connect().await.unwrap();

        define_member_access(&service).await;
        let member = service
            .signup(&ScopeCredentials::new("truth", "truth", "member").with("name", "m"))
            .await
            .unwrap();
        assert!(!service.can_replay_session(), "signup, likewise");
        service.connect().await.unwrap();

        service.invalidate().await.unwrap();
        assert!(!service.can_replay_session(), "invalidate, likewise");
        service.connect().await.unwrap();

        let caller = service.caller_session(&member.token).await.unwrap();
        assert!(
            !caller.can_replay_session(),
            "a caller session never replays, even with config credentials present"
        );
        assert!(
            service.can_replay_session(),
            "a caller session is its own session: the parent keeps its authority"
        );

        service.disconnect().await.unwrap();
        assert!(!service.can_replay_session(), "disconnect ends it");

        let anonymous = DatabaseClient::new(ConnectionConfig {
            db_user: None,
            db_pass: None,
            ..root_mem_config("truth_anon")
        })
        .unwrap();
        anonymous.connect().await.unwrap();
        assert!(
            !anonymous.can_replay_session(),
            "no config credentials: nothing safe to replay"
        );
    }

    #[test]
    fn session_expiry_is_read_from_the_request_error_only() {
        let structured = surrealdb::Error::not_allowed(
            "The session has expired".into(),
            NotAllowedError::Auth(AuthError::SessionExpired),
        );
        assert!(request_says_session_expired(&structured));
        let unstructured = surrealdb::Error::not_allowed("The session has expired".into(), None);
        assert!(request_says_session_expired(&unstructured));
        // A statement's own error text is never read as expiry.
        let thrown = surrealdb::Error::thrown("The session has expired".into());
        assert!(!request_says_session_expired(&thrown));
        let embedded = surrealdb::Error::not_allowed(
            "Found 'The session has expired' for field `note`".into(),
            None,
        );
        assert!(!request_says_session_expired(&embedded));
    }

    #[test]
    fn render_target_accepts_tables_and_record_ids_only() {
        assert_eq!(render_target("user").unwrap(), "user");
        assert_eq!(render_target(" user ").unwrap(), "user");
        assert_eq!(render_target("select").unwrap(), "`select`");
        assert_eq!(render_target("user:alice").unwrap(), "user:alice");
        assert_eq!(render_target("post:42").unwrap(), "post:42");
        assert_eq!(render_target("user:⟨a-b⟩").unwrap(), "user:⟨a-b⟩");
        assert_eq!(
            render_target("user:x; REMOVE TABLE user").unwrap(),
            "user:⟨x; REMOVE TABLE user⟩"
        );
        assert_eq!(
            render_target("user:x⟩; REMOVE TABLE user").unwrap(),
            "user:`x⟩; REMOVE TABLE user`"
        );
        for bad in [
            "",
            "user; REMOVE TABLE user",
            "user WHERE true",
            "1user:a",
            ":a",
        ] {
            assert!(
                matches!(render_target(bad), Err(SurqlError::Validation { .. })),
                "{bad:?}"
            );
        }
    }

    #[test]
    fn seconds_refuses_what_duration_cannot_hold() {
        assert_eq!(seconds(1.5).unwrap(), Duration::from_millis(1500));
        for bad in [f64::INFINITY, f64::NAN, 1e300, -1.0] {
            assert!(seconds(bad).is_err(), "{bad}");
        }
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

    fn root_mem_config(ns: &str) -> ConnectionConfig {
        ConnectionConfig::builder()
            .url("mem://")
            .namespace(ns)
            .database(ns)
            .username("root")
            .password("root")
            .retry_max_attempts(1)
            .build()
            .unwrap()
    }

    /// A record access method whose sessions live one second, plus a
    /// table only system users may read.
    async fn define_member_access(client: &DatabaseClient) {
        client
            .query(
                "DEFINE ACCESS member ON DATABASE TYPE RECORD \
                 SIGNUP (CREATE member SET name = $name) \
                 SIGNIN (SELECT * FROM member WHERE name = $name) \
                 DURATION FOR SESSION 1s; \
                 DEFINE TABLE secret SCHEMALESS PERMISSIONS NONE; \
                 CREATE secret SET v = 1;",
            )
            .await
            .unwrap();
    }

    /// Regression: the replay used to be a fixed per-client flag, so a
    /// root-configured service that signed its shared session in as a
    /// record user had that user's expired session silently replaced by
    /// ROOT, which then ran the user's statement unfiltered. Whatever
    /// the engine does with the expiry, the statement must never see a
    /// row that `PERMISSIONS NONE` hides from the record user.
    #[tokio::test]
    async fn expired_record_session_never_replays_as_config_root() {
        let client = DatabaseClient::new(root_mem_config("escalate")).unwrap();
        client.connect().await.unwrap();
        define_member_access(&client).await;
        AuthManager::new()
            .signup(
                &client,
                &ScopeCredentials::new("escalate", "escalate", "member").with("name", "m"),
            )
            .await
            .unwrap();
        sleep(Duration::from_millis(2500)).await;
        if let Ok(rows) = client.query("SELECT * FROM secret;").await {
            assert_eq!(
                rows,
                serde_json::json!([[]]),
                "the record user's statement ran with root authority"
            );
        }
    }

    /// Regression: `connect` on a caller session signed it in with the
    /// config credentials, handing the caller the service's authority.
    #[tokio::test]
    async fn connect_refuses_on_a_caller_session() {
        let root = DatabaseClient::new(root_mem_config("callerconnect")).unwrap();
        root.connect().await.unwrap();
        define_member_access(&root).await;
        let token = root
            .signup(
                &ScopeCredentials::new("callerconnect", "callerconnect", "member")
                    .with("name", "m"),
            )
            .await
            .unwrap();
        // The signup switched the shared session; put root back.
        root.connect().await.unwrap();
        let caller = root.caller_session(&token.token).await.unwrap();
        assert!(caller.connect().await.is_err());
        let seen = caller.query("SELECT * FROM secret;").await.unwrap();
        assert_eq!(seen, serde_json::json!([[]]));
    }

    /// Regression: the expiry check matched a substring of ANY error,
    /// including a per-statement error that carries user data, and the
    /// replay then re-ran every statement of the request.
    #[tokio::test]
    async fn statement_errors_never_trigger_a_replay() {
        let client = DatabaseClient::new(root_mem_config("noreplay")).unwrap();
        client.connect().await.unwrap();
        let err = client
            .query("CREATE counter SET n = 1; THROW 'The session has expired';")
            .await
            .unwrap_err();
        assert!(err.to_string().contains("session has expired"), "{err}");
        let count = client
            .query("SELECT count() FROM counter GROUP ALL;")
            .await
            .unwrap();
        assert_eq!(count, serde_json::json!([[{ "count": 1 }]]));
    }

    /// Regression: typed CRUD targets were spliced into the statement
    /// verbatim, so a record key taken from user input ended the
    /// statement and ran another.
    #[tokio::test]
    async fn crud_targets_cannot_inject_statements() {
        let client = DatabaseClient::new(root_mem_config("inject")).unwrap();
        client.connect().await.unwrap();
        client.query("CREATE user:a SET n = 1;").await.unwrap();
        let _ = client.select::<Value>("user:x; REMOVE TABLE user").await;
        let _ = client.delete::<Value>("user:x; REMOVE TABLE user").await;
        let rows: Vec<Value> = client.select("user").await.unwrap();
        assert_eq!(rows.len(), 1, "the injected REMOVE TABLE ran");
    }
}
