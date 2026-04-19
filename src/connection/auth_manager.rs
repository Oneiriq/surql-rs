//! Async authentication orchestrator.
//!
//! Port of the `AuthManager` class in `surql/connection/auth.py`. Keeps
//! an in-memory handle on the last-issued token and the auth level that
//! produced it, exposes `signin` / `signup` / `authenticate` / `invalidate`
//! wrappers that delegate to [`DatabaseClient`], and carries a stub
//! [`AuthManager::refresh`] for SDK parity. The v3 `surrealdb` SDK does
//! not yet expose a dedicated refresh entry point; the stub re-runs
//! [`DatabaseClient::authenticate`] with the cached token so callers can
//! drive periodic keep-alive without reaching into the SDK directly.

use std::sync::Arc;

use tokio::sync::Mutex;

use crate::connection::auth::{AuthType, Credentials, ScopeCredentials, TokenAuth};
use crate::connection::client::DatabaseClient;
use crate::error::{Result, SurqlError};

/// Snapshot of the last successful authentication.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TokenState {
    /// Token returned by the SDK on signin / signup.
    pub token: TokenAuth,
    /// Auth level that produced the token.
    pub auth_type: AuthType,
}

/// Async orchestrator around [`DatabaseClient`] auth primitives.
///
/// `Clone` is cheap — internally refcounted so CLI / orchestration code
/// can share a single manager across tasks.
#[derive(Debug, Clone, Default)]
pub struct AuthManager {
    inner: Arc<AuthManagerInner>,
}

#[derive(Debug, Default)]
struct AuthManagerInner {
    state: Mutex<Option<TokenState>>,
}

impl AuthManager {
    /// Construct a new manager with no cached token.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sign in `client` with `creds`, caching the returned token.
    ///
    /// # Errors
    ///
    /// Propagates any [`DatabaseClient::signin`] error.
    pub async fn signin<C: Credentials + ?Sized>(
        &self,
        client: &DatabaseClient,
        creds: &C,
    ) -> Result<TokenAuth> {
        let auth_type = creds.auth_type();
        let token = client.signin(creds).await?;
        *self.inner.state.lock().await = Some(TokenState {
            token: token.clone(),
            auth_type,
        });
        Ok(token)
    }

    /// Sign up a scope user and cache the returned token.
    ///
    /// # Errors
    ///
    /// Propagates any [`DatabaseClient::signup`] error.
    pub async fn signup(
        &self,
        client: &DatabaseClient,
        creds: &ScopeCredentials,
    ) -> Result<TokenAuth> {
        let token = client.signup(creds).await?;
        *self.inner.state.lock().await = Some(TokenState {
            token: token.clone(),
            auth_type: AuthType::Scope,
        });
        Ok(token)
    }

    /// Authenticate with an existing JWT, caching it as the current token.
    ///
    /// The cached [`TokenState::auth_type`] defaults to the current
    /// value if one is cached; otherwise it is set to [`AuthType::Scope`]
    /// because Record-access tokens are the most common source of
    /// externally-held JWTs.
    ///
    /// # Errors
    ///
    /// Propagates any [`DatabaseClient::authenticate`] error.
    pub async fn authenticate(&self, client: &DatabaseClient, token: &str) -> Result<()> {
        client.authenticate(token).await?;
        let mut slot = self.inner.state.lock().await;
        let preserved = slot.as_ref().map_or(AuthType::Scope, |s| s.auth_type);
        *slot = Some(TokenState {
            token: TokenAuth::new(token.to_owned()),
            auth_type: preserved,
        });
        Ok(())
    }

    /// Invalidate the current session and drop the cached token.
    ///
    /// # Errors
    ///
    /// Propagates any [`DatabaseClient::invalidate`] error.
    pub async fn invalidate(&self, client: &DatabaseClient) -> Result<()> {
        client.invalidate().await?;
        *self.inner.state.lock().await = None;
        Ok(())
    }

    /// Re-apply the cached token against `client`.
    ///
    /// The v3 `surrealdb` SDK has no dedicated refresh endpoint. This
    /// method therefore acts as a keep-alive: it calls
    /// [`DatabaseClient::authenticate`] with the current cached token so
    /// reconnects (after e.g. a socket drop) pick the session back up.
    /// Returns the preserved [`TokenAuth`].
    ///
    /// # Errors
    ///
    /// - [`SurqlError::Context`] when no token is cached.
    /// - Propagates [`DatabaseClient::authenticate`] errors.
    pub async fn refresh(&self, client: &DatabaseClient) -> Result<TokenAuth> {
        let cached = self
            .current_token()
            .await
            .ok_or_else(|| SurqlError::Context {
                reason: "no cached token to refresh".into(),
            })?;
        client.authenticate(&cached.token).await?;
        Ok(cached)
    }

    /// Snapshot the currently-cached token, if any.
    pub async fn current_token(&self) -> Option<TokenAuth> {
        self.inner
            .state
            .lock()
            .await
            .as_ref()
            .map(|s| s.token.clone())
    }

    /// Snapshot the current auth level, if any.
    pub async fn auth_type(&self) -> Option<AuthType> {
        self.inner.state.lock().await.as_ref().map(|s| s.auth_type)
    }

    /// Return `true` if a token is currently cached.
    pub async fn is_authenticated(&self) -> bool {
        self.inner.state.lock().await.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connection::config::ConnectionConfig;

    #[tokio::test]
    async fn default_manager_is_empty() {
        let am = AuthManager::new();
        assert!(!am.is_authenticated().await);
        assert!(am.current_token().await.is_none());
        assert!(am.auth_type().await.is_none());
    }

    #[tokio::test]
    async fn refresh_without_token_errors() {
        let am = AuthManager::new();
        let client =
            DatabaseClient::new(ConnectionConfig::default()).expect("default config is valid");
        let err = am.refresh(&client).await.unwrap_err();
        assert!(matches!(err, SurqlError::Context { .. }));
    }

    #[tokio::test]
    async fn signin_against_disconnected_client_errors() {
        // Without a live server we can still prove the manager forwards
        // errors: the underlying client refuses when not connected.
        use crate::connection::auth::RootCredentials;
        let am = AuthManager::new();
        let client =
            DatabaseClient::new(ConnectionConfig::default()).expect("default config is valid");
        let err = am
            .signin(&client, &RootCredentials::new("root", "root"))
            .await
            .unwrap_err();
        assert!(matches!(err, SurqlError::Connection { .. }));
        assert!(!am.is_authenticated().await);
    }
}
